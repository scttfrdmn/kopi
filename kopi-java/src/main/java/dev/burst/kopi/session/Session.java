package dev.burst.kopi.session;

import dev.burst.kopi.KopiException;
import dev.burst.kopi.MapOptions;
import dev.burst.kopi.PartialException;
import dev.burst.kopi.config.Config;
import dev.burst.kopi.serialize.ResultPayload;
import dev.burst.kopi.serialize.TaskPayload;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.*;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeVpcsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Core orchestration for a kopi burst session.
 *
 * <p>Implements the full burst protocol:
 * <ol>
 *   <li>Generate session ID and chunk items
 *   <li>Upload task files to S3 concurrently
 *   <li>Write manifest
 *   <li>Launch ECS Fargate tasks
 *   <li>Poll S3 status files every 2 s
 *   <li>Download results
 *   <li>Fire-and-forget cleanup
 *   <li>Return ordered results or throw {@link PartialException}
 * </ol>
 */
public final class Session {

    private static final Logger LOG = LoggerFactory.getLogger(Session.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String LIBRARY_VERSION = "0.1.0";

    // Concurrency limit for S3 uploads/downloads
    private static final int S3_CONCURRENCY = 20;

    private Session() {}

    // ---------- Public entry point ----------

    /**
     * Runs a complete burst session end-to-end.
     *
     * @param cfg        resolved configuration
     * @param items      serialized input items
     * @param fnName     registered function name
     * @param resultType output class for deserialization
     * @param opts       map options (workers, CPU, memory, etc.)
     * @param <U>        result type
     * @return ordered list of results
     * @throws KopiException on failure
     */
    public static <U> List<U> runSession(
            Config cfg,
            List<JsonNode> items,
            String fnName,
            Class<U> resultType,
            MapOptions opts) throws KopiException {

        // Resolve options from config defaults
        int workers = opts.getWorkers() > 0 ? opts.getWorkers() : cfg.getDefaultWorkers();
        int cpu = opts.getCpu() > 0 ? opts.getCpu() : cfg.getDefaultCpu();
        int memoryGb = opts.getMemoryGb() > 0 ? opts.getMemoryGb() : cfg.getDefaultMemoryGb();
        String backend = (opts.getBackend() != null && !opts.getBackend().isBlank())
                ? opts.getBackend() : cfg.getBackend();
        boolean spot = opts.isSpot() || cfg.isSpot();
        double maxCost = opts.getMaxCost() > 0.0 ? opts.getMaxCost() : cfg.getMaxCostPerJob();
        int timeoutSeconds = opts.getTimeoutSeconds();
        String region = (opts.getRegion() != null && !opts.getRegion().isBlank())
                ? opts.getRegion() : cfg.getRegion();
        String arch = (opts.getArch() != null && !opts.getArch().isBlank())
                ? opts.getArch() : "amd64";

        // Cost preflight
        double costPerHour = estimateCostPerHour(cpu, memoryGb, workers);
        if (maxCost > 0.0 && costPerHour > maxCost) {
            throw new KopiException(String.format(
                    "estimated cost %.4f $/hr exceeds limit %.4f $/hr",
                    costPerHour, maxCost));
        }

        String sessionId = SessionId.generateJava();
        LOG.info("kopi session starting: session={} fn={} items={}", sessionId, fnName, items.size());

        // Build AWS clients
        S3AsyncClient s3 = buildS3Client(region);
        EcsClient ecs = buildEcsClient(region);
        Ec2Client ec2 = buildEc2Client(region);

        List<List<JsonNode>> chunks = chunkItems(items, workers);
        int nChunks = chunks.size();
        LOG.info("kopi chunked into {} chunks (requested {} workers)", nChunks, workers);

        try {
            // Upload task files
            uploadTasks(s3, cfg.getS3Bucket(), sessionId, fnName, chunks);
            LOG.info("kopi task files uploaded");

            // Write manifest
            Manifest manifest = buildManifest(sessionId, nChunks, workers, cpu, memoryGb,
                    backend, spot, region, costPerHour);
            writeManifest(s3, cfg.getS3Bucket(), manifest);

            // Launch ECS workers
            launchWorkers(ecs, ec2, cfg, sessionId, fnName, nChunks,
                    workers, cpu, memoryGb, spot, region, arch);
            LOG.info("kopi workers launched");

            // Poll until done
            long deadlineMs = timeoutSeconds > 0
                    ? System.currentTimeMillis() + (long) timeoutSeconds * 1000L
                    : Long.MAX_VALUE;
            pollUntilDone(s3, cfg.getS3Bucket(), sessionId, nChunks, deadlineMs);
            LOG.info("kopi all tasks terminal");

            // Download results
            List<ResultPayload> payloads = downloadResults(s3, cfg.getS3Bucket(), sessionId, nChunks);

            // Fire-and-forget cleanup
            final S3AsyncClient s3Cleanup = s3;
            CompletableFuture.runAsync(() ->
                    cleanupTaskFiles(s3Cleanup, cfg.getS3Bucket(), sessionId, nChunks));

            // Flatten results
            return flattenResults(payloads, resultType);

        } catch (KopiException e) {
            throw e;
        } catch (Exception e) {
            throw new KopiException("session failed: " + e.getMessage(), e);
        }
    }

    // ---------- Chunking ----------

    static List<List<JsonNode>> chunkItems(List<JsonNode> items, int nChunks) {
        if (nChunks <= 0 || items.isEmpty()) return Collections.emptyList();
        int n = Math.min(nChunks, items.size());
        int size = items.size() / n;
        List<List<JsonNode>> chunks = new ArrayList<>(n);
        int offset = 0;
        for (int i = 0; i < n; i++) {
            int end = (i == n - 1) ? items.size() : offset + size;
            chunks.add(new ArrayList<>(items.subList(offset, end)));
            offset = end;
        }
        return chunks;
    }

    // ---------- S3 key helpers ----------

    public static String taskKey(String sessionId, String taskId) {
        return "sessions/" + sessionId + "/tasks/" + taskId + ".task";
    }

    public static String resultKey(String sessionId, String taskId) {
        return "sessions/" + sessionId + "/tasks/" + taskId + ".result";
    }

    public static String statusKey(String sessionId, String taskId) {
        return "sessions/" + sessionId + "/tasks/" + taskId + ".status";
    }

    public static String manifestKey(String sessionId) {
        return "sessions/" + sessionId + "/manifest.json";
    }

    public static String taskId(int index) {
        return String.format("task-%04d", index);
    }

    // ---------- S3 upload ----------

    private static void uploadTasks(
            S3AsyncClient s3,
            String bucket,
            String sessionId,
            String fnName,
            List<List<JsonNode>> chunks) throws KopiException {

        Semaphore sem = new Semaphore(S3_CONCURRENCY);
        List<CompletableFuture<Void>> futures = new ArrayList<>(chunks.size());

        for (int i = 0; i < chunks.size(); i++) {
            final int idx = i;
            final List<JsonNode> chunk = chunks.get(i);
            try {
                sem.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KopiException("interrupted while uploading tasks", e);
            }

            CompletableFuture<Void> f = CompletableFuture.supplyAsync(() -> {
                try {
                    TaskPayload payload = new TaskPayload(chunk, fnName, idx);
                    byte[] body = MAPPER.writeValueAsBytes(payload);
                    String key = taskKey(sessionId, taskId(idx));
                    s3.putObject(
                            PutObjectRequest.builder().bucket(bucket).key(key).build(),
                            AsyncRequestBody.fromBytes(body)
                    ).join();
                    return (Void) null;
                } catch (Exception e) {
                    throw new CompletionException(e);
                } finally {
                    sem.release();
                }
            });
            futures.add(f);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException e) {
            throw new KopiException("S3 upload failed: " + e.getCause().getMessage(), e.getCause());
        }
    }

    // ---------- Manifest ----------

    private static Manifest buildManifest(
            String sessionId, int nChunks, int workersRequested,
            int cpu, int memoryGb, String backend, boolean spot,
            String region, double costPerHour) {
        Manifest m = new Manifest();
        m.sessionId = sessionId;
        m.language = "java";
        m.status = "running";
        m.tasksTotal = nChunks;
        m.tasksComplete = 0;
        m.tasksFailed = 0;
        m.workersActive = 0;
        m.costActual = 0.0;
        m.costEstimatePerHour = costPerHour;
        m.createdAt = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        m.chunkCount = nChunks;
        m.taskCount = nChunks;
        m.workersRequested = workersRequested;
        m.workersActual = nChunks;
        m.cpu = cpu;
        m.memoryGb = memoryGb;
        m.backend = backend;
        m.spot = spot;
        m.region = region;
        m.envHash = "";
        m.libraryVersion = LIBRARY_VERSION;
        return m;
    }

    private static void writeManifest(S3AsyncClient s3, String bucket, Manifest manifest)
            throws KopiException {
        try {
            byte[] body = MAPPER.writeValueAsBytes(manifest);
            String key = manifestKey(manifest.sessionId);
            s3.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    AsyncRequestBody.fromBytes(body)
            ).join();
        } catch (Exception e) {
            throw new KopiException("failed to write manifest: " + e.getMessage(), e);
        }
    }

    // ---------- ECS launch ----------

    private static void launchWorkers(
            EcsClient ecs,
            Ec2Client ec2,
            Config cfg,
            String sessionId,
            String fnName,
            int taskCount,
            int workersRequested,
            int cpu,
            int memoryGb,
            boolean spot,
            String region,
            String arch) throws KopiException {

        String family = "burst-" + sessionId;
        String cpuStr = String.valueOf(cpu * 1024);
        String memStr = String.valueOf(memoryGb * 1024);

        // Static env baked into the task definition
        List<KeyValuePair> staticEnv = List.of(
                kv("BURST_SESSION_ID", sessionId),
                kv("BURST_S3_BUCKET", cfg.getS3Bucket()),
                kv("BURST_REGION", region),
                kv("BURST_WORKER", "1")
        );

        // Derive image URI: {ecr_base_uri}/kopi-workers:latest
        String imageUri = cfg.getEcrBaseUri() + "/kopi-workers:latest";

        ContainerDefinition containerDef = ContainerDefinition.builder()
                .name("worker")
                .image(imageUri)
                .essential(true)
                .environment(staticEnv)
                .logConfiguration(LogConfiguration.builder()
                        .logDriver(LogDriver.AWSLOGS)
                        .options(Map.of(
                                "awslogs-group", "/burst/workers",
                                "awslogs-region", region,
                                "awslogs-stream-prefix", "burst",
                                "awslogs-create-group", "true"
                        ))
                        .build())
                .build();

        RegisterTaskDefinitionRequest regReq = RegisterTaskDefinitionRequest.builder()
                .family(family)
                .networkMode(NetworkMode.AWSVPC)
                .requiresCompatibilities(Compatibility.FARGATE)
                .cpu(cpuStr)
                .memory(memStr)
                .executionRoleArn(cfg.getExecutionRoleArn())
                .taskRoleArn(cfg.getTaskRoleArn())
                .containerDefinitions(containerDef)
                .runtimePlatform(RuntimePlatform.builder()
                        .cpuArchitecture("arm64".equals(arch)
                                ? software.amazon.awssdk.services.ecs.model.CPUArchitecture.ARM64
                                : software.amazon.awssdk.services.ecs.model.CPUArchitecture.X86_64)
                        .operatingSystemFamily(software.amazon.awssdk.services.ecs.model.OSFamily.LINUX)
                        .build())
                .build();

        RegisterTaskDefinitionResponse regResp;
        try {
            regResp = ecs.registerTaskDefinition(regReq);
        } catch (Exception e) {
            throw new KopiException("failed to register ECS task definition: " + e.getMessage(), e);
        }

        String taskDefArn = regResp.taskDefinition().taskDefinitionArn();
        LOG.debug("registered ECS task definition: {}", taskDefArn);

        // Determine wave size from vCPU quota
        int waveSize = computeWaveSize(cfg.getFargateQuotaVcpu(), cpu, taskCount);

        // Discover default VPC networking
        List<String> subnets = getDefaultVpcSubnets(ec2);
        String sg = getDefaultSecurityGroup(ec2);

        int launched = 0;
        while (launched < taskCount) {
            int end = Math.min(launched + waveSize, taskCount);
            for (int i = launched; i < end; i++) {
                runOneTask(ecs, cfg, taskDefArn, fnName, i, subnets, sg, spot);
            }
            launched = end;
        }
    }

    private static int computeWaveSize(double quotaVcpu, int cpuPerTask, int taskCount) {
        if (quotaVcpu > 0.0 && cpuPerTask > 0) {
            int ws = (int) quotaVcpu / cpuPerTask;
            if (ws > 0 && ws < taskCount) return ws;
        }
        return taskCount;
    }

    private static void runOneTask(
            EcsClient ecs,
            Config cfg,
            String taskDefArn,
            String fnName,
            int index,
            List<String> subnets,
            String sg,
            boolean spot) throws KopiException {

        String tid = taskId(index);

        // Per-task env overrides
        List<KeyValuePair> perTaskEnv = List.of(
                kv("BURST_TASK_ID", tid),
                kv("BURST_FUNCTION_NAME", fnName)
        );

        ContainerOverride containerOverride = ContainerOverride.builder()
                .name("worker")
                .environment(perTaskEnv)
                .build();

        TaskOverride taskOverride = TaskOverride.builder()
                .containerOverrides(containerOverride)
                .build();

        AwsVpcConfiguration vpcCfg = AwsVpcConfiguration.builder()
                .subnets(subnets)
                .securityGroups(sg)
                .assignPublicIp(AssignPublicIp.ENABLED)
                .build();

        NetworkConfiguration netCfg = NetworkConfiguration.builder()
                .awsvpcConfiguration(vpcCfg)
                .build();

        RunTaskRequest.Builder reqBuilder = RunTaskRequest.builder()
                .cluster(cfg.getEcsCluster())
                .taskDefinition(taskDefArn)
                .networkConfiguration(netCfg)
                .overrides(taskOverride);

        if (spot) {
            reqBuilder.capacityProviderStrategy(
                    CapacityProviderStrategyItem.builder()
                            .capacityProvider("FARGATE_SPOT")
                            .weight(1)
                            .build());
        } else {
            reqBuilder.launchType(LaunchType.FARGATE);
        }

        RunTaskResponse resp;
        try {
            resp = ecs.runTask(reqBuilder.build());
        } catch (Exception e) {
            throw new KopiException("RunTask failed for " + tid + ": " + e.getMessage(), e);
        }

        if (!resp.failures().isEmpty()) {
            Failure f = resp.failures().get(0);
            throw new KopiException("ECS task failure for " + tid + ": " +
                    f.reason() + " — " + f.detail());
        }

        LOG.debug("launched ECS task: {}", tid);
    }

    // ---------- VPC helpers ----------

    private static List<String> getDefaultVpcSubnets(Ec2Client ec2) throws KopiException {
        var vpcsResp = ec2.describeVpcs(DescribeVpcsRequest.builder()
                .filters(Filter.builder().name("isDefault").values("true").build())
                .build());

        if (vpcsResp.vpcs().isEmpty()) {
            throw new KopiException(
                    "no default VPC found in region — run `aws ec2 create-default-vpc`");
        }
        String vpcId = vpcsResp.vpcs().get(0).vpcId();

        var subnetsResp = ec2.describeSubnets(DescribeSubnetsRequest.builder()
                .filters(Filter.builder().name("vpc-id").values(vpcId).build())
                .build());

        List<String> ids = subnetsResp.subnets().stream()
                .map(s -> s.subnetId())
                .collect(Collectors.toList());

        if (ids.isEmpty()) {
            throw new KopiException("no subnets found in default VPC");
        }
        return ids;
    }

    private static String getDefaultSecurityGroup(Ec2Client ec2) throws KopiException {
        var resp = ec2.describeSecurityGroups(DescribeSecurityGroupsRequest.builder()
                .filters(Filter.builder().name("group-name").values("default").build())
                .build());

        if (resp.securityGroups().isEmpty()) {
            throw new KopiException("no default security group found");
        }
        return resp.securityGroups().get(0).groupId();
    }

    // ---------- Polling ----------

    private static void pollUntilDone(
            S3AsyncClient s3,
            String bucket,
            String sessionId,
            int taskCount,
            long deadlineMs) throws KopiException {

        while (true) {
            if (System.currentTimeMillis() >= deadlineMs) {
                throw new KopiException("timeout waiting for session " + sessionId);
            }

            int done = countTerminalTasks(s3, bucket, sessionId, taskCount);
            LOG.debug("kopi polling: {}/{} tasks done", done, taskCount);

            if (done >= taskCount) return;

            try {
                Thread.sleep(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KopiException("interrupted while polling results", e);
            }
        }
    }

    private static int countTerminalTasks(
            S3AsyncClient s3, String bucket, String sessionId, int taskCount) {

        int done = 0;
        List<CompletableFuture<String>> futures = new ArrayList<>(taskCount);

        for (int i = 0; i < taskCount; i++) {
            String key = statusKey(sessionId, taskId(i));
            futures.add(s3.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build(),
                    AsyncResponseTransformer.toBytes()
            ).handle((resp, ex) -> {
                if (ex != null) return "";
                return resp.asUtf8String().trim();
            }));
        }

        for (CompletableFuture<String> f : futures) {
            String status = f.join();
            if ("done".equals(status) || "partial".equals(status) || "failed".equals(status)) {
                done++;
            }
        }
        return done;
    }

    // ---------- Result download ----------

    private static List<ResultPayload> downloadResults(
            S3AsyncClient s3, String bucket, String sessionId, int taskCount)
            throws KopiException {

        Semaphore sem = new Semaphore(S3_CONCURRENCY);
        List<CompletableFuture<ResultPayload>> futures = new ArrayList<>(taskCount);

        for (int i = 0; i < taskCount; i++) {
            final int idx = i;
            try {
                sem.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KopiException("interrupted while downloading results", e);
            }

            String key = resultKey(sessionId, taskId(idx));
            CompletableFuture<ResultPayload> f = s3.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build(),
                    AsyncResponseTransformer.toBytes()
            ).thenApply(resp -> {
                try {
                    return MAPPER.readValue(resp.asByteArray(), ResultPayload.class);
                } catch (Exception e) {
                    throw new CompletionException("parsing result " + key, e);
                }
            }).whenComplete((r, ex) -> sem.release());

            futures.add(f);
        }

        List<ResultPayload> payloads = new ArrayList<>(taskCount);
        try {
            for (CompletableFuture<ResultPayload> f : futures) {
                payloads.add(f.join());
            }
        } catch (CompletionException e) {
            throw new KopiException("result download failed: " + e.getCause().getMessage(), e.getCause());
        }
        return payloads;
    }

    // ---------- Cleanup ----------

    private static void cleanupTaskFiles(
            S3AsyncClient s3, String bucket, String sessionId, int taskCount) {
        for (int i = 0; i < taskCount; i++) {
            String tid = taskId(i);
            for (String key : new String[]{
                    taskKey(sessionId, tid),
                    resultKey(sessionId, tid),
                    statusKey(sessionId, tid)}) {
                try {
                    s3.deleteObject(
                            DeleteObjectRequest.builder().bucket(bucket).key(key).build()
                    ).join();
                } catch (Exception e) {
                    LOG.warn("cleanup: failed to delete {}: {}", key, e.getMessage());
                }
            }
        }
        LOG.debug("kopi cleanup complete for session {}", sessionId);
    }

    // ---------- Result flattening ----------

    @SuppressWarnings("unchecked")
    private static <U> List<U> flattenResults(
            List<ResultPayload> payloads, Class<U> resultType) throws KopiException {

        List<Object> allResults = new ArrayList<>();
        List<String> allErrors = new ArrayList<>();
        int failed = 0;
        int succeeded = 0;

        for (ResultPayload payload : payloads) {
            List<JsonNode> results = payload.getResults();
            List<String> errors = payload.getErrors();
            int n = results.size();
            for (int i = 0; i < n; i++) {
                String err = (errors != null && i < errors.size()) ? errors.get(i) : null;
                boolean hasErr = err != null && !err.isBlank();
                if (hasErr) {
                    failed++;
                    allResults.add(null);
                } else {
                    succeeded++;
                    JsonNode node = results.get(i);
                    try {
                        allResults.add(MAPPER.treeToValue(node, resultType));
                    } catch (Exception e) {
                        throw new KopiException("failed to deserialize result: " + e.getMessage(), e);
                    }
                }
                allErrors.add(hasErr ? err : null);
            }
        }

        if (failed > 0) {
            LOG.warn("kopi partial failure: {} succeeded, {} failed", succeeded, failed);
            throw new PartialException(allResults, allErrors, failed, succeeded);
        }

        return (List<U>) allResults;
    }

    // ---------- Cost estimation ----------

    /** Rough Fargate cost estimate (us-west-2 on-demand rates). */
    static double estimateCostPerHour(int cpu, int memoryGb, int workers) {
        // $0.04048/vCPU/hr + $0.004445/GB/hr (approximate)
        double perWorker = cpu * 0.04048 + memoryGb * 0.004445;
        return perWorker * workers;
    }

    // ---------- AWS client builders ----------

    private static S3AsyncClient buildS3Client(String region) {
        String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
        var builder = S3AsyncClient.builder()
                .region(Region.of(region))
                .forcePathStyle(true);  // avoid 301 redirects on regional buckets
        if (endpointUrl != null && !endpointUrl.isBlank()) {
            builder.endpointOverride(URI.create(endpointUrl));
        }
        return builder.build();
    }

    private static EcsClient buildEcsClient(String region) {
        String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
        var builder = EcsClient.builder()
                .region(Region.of(region));
        if (endpointUrl != null && !endpointUrl.isBlank()) {
            builder.endpointOverride(URI.create(endpointUrl));
        }
        return builder.build();
    }

    private static Ec2Client buildEc2Client(String region) {
        String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
        var builder = Ec2Client.builder()
                .region(Region.of(region));
        if (endpointUrl != null && !endpointUrl.isBlank()) {
            builder.endpointOverride(URI.create(endpointUrl));
        }
        return builder.build();
    }

    // ---------- Manifest POJO ----------

    /** Manifest written to {@code sessions/{id}/manifest.json}. */
    static final class Manifest {

        @JsonProperty("session_id")
        String sessionId;

        @JsonProperty("language")
        String language;

        @JsonProperty("status")
        String status;

        @JsonProperty("tasks_total")
        int tasksTotal;

        @JsonProperty("tasks_complete")
        int tasksComplete;

        @JsonProperty("tasks_failed")
        int tasksFailed;

        @JsonProperty("workers_active")
        int workersActive;

        @JsonProperty("cost_actual")
        double costActual;

        @JsonProperty("cost_estimate_per_hour")
        double costEstimatePerHour;

        @JsonProperty("created_at")
        String createdAt;

        @JsonProperty("chunk_count")
        int chunkCount;

        @JsonProperty("task_count")
        int taskCount;

        @JsonProperty("workers_requested")
        int workersRequested;

        @JsonProperty("workers_actual")
        int workersActual;

        @JsonProperty("cpu")
        int cpu;

        @JsonProperty("memory_gb")
        int memoryGb;

        @JsonProperty("backend")
        String backend;

        @JsonProperty("spot")
        boolean spot;

        @JsonProperty("region")
        String region;

        @JsonProperty("env_hash")
        String envHash;

        @JsonProperty("library_version")
        String libraryVersion;
    }

    // ---------- Helpers ----------

    private static KeyValuePair kv(String name, String value) {
        return KeyValuePair.builder().name(name).value(value).build();
    }
}
