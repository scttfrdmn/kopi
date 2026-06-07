package dev.burst.kopi.worker;

import dev.burst.kopi.registry.FunctionRegistry;
import dev.burst.kopi.serialize.ResultPayload;
import dev.burst.kopi.serialize.TaskPayload;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Worker-side implementation of the burst protocol.
 *
 * <p>When {@code BURST_WORKER=1} is set in the environment, the application
 * should call {@link #run()} and exit with its return code.
 */
public final class Worker {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Worker() {}

    /**
     * Returns {@code true} if this process is running in worker mode
     * ({@code BURST_WORKER=1} environment variable is set).
     */
    public static boolean isWorker() {
        return "1".equals(System.getenv("BURST_WORKER"));
    }

    /**
     * Runs the worker lifecycle: download task → dispatch function → upload result.
     *
     * @return 0 on success, 1 on error
     */
    public static int run() {
        try {
            workerMain();
            return 0;
        } catch (Exception e) {
            LOG.error("kopi worker error: {}", e.getMessage(), e);
            System.err.println("kopi worker error: " + e.getMessage());
            return 1;
        }
    }

    private static void workerMain() throws Exception {
        String sessionId = requireEnv("BURST_SESSION_ID");
        String taskId = requireEnv("BURST_TASK_ID");
        String bucket = requireEnv("BURST_S3_BUCKET");
        String region = requireEnv("BURST_REGION");
        String fnName = requireEnv("BURST_FUNCTION_NAME");

        LOG.info("kopi worker starting: session={} task={} fn={}", sessionId, taskId, fnName);

        S3AsyncClient s3 = buildS3Client(region);

        String statusKey = "sessions/" + sessionId + "/tasks/" + taskId + ".status";
        String taskKey = "sessions/" + sessionId + "/tasks/" + taskId + ".task";
        String resultKey = "sessions/" + sessionId + "/tasks/" + taskId + ".result";

        // Signal "running"
        putText(s3, bucket, statusKey, "running");

        // Download task payload
        byte[] taskBytes;
        try {
            taskBytes = s3.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(taskKey).build(),
                    AsyncResponseTransformer.toBytes()
            ).join().asByteArray();
        } catch (Exception e) {
            LOG.error("failed to download task {}: {}", taskKey, e.getMessage());
            putText(s3, bucket, statusKey, "failed");
            throw new Exception("downloading task " + taskKey + ": " + e.getMessage(), e);
        }

        TaskPayload task;
        try {
            task = MAPPER.readValue(taskBytes, TaskPayload.class);
        } catch (Exception e) {
            LOG.error("failed to deserialize task: {}", e.getMessage());
            putText(s3, bucket, statusKey, "failed");
            throw new Exception("deserializing task: " + e.getMessage(), e);
        }

        int n = task.getItems().size();
        List<JsonNode> results = new ArrayList<>(n);
        List<String> errors = new ArrayList<>(n);
        boolean anyFailed = false;

        for (int i = 0; i < n; i++) {
            JsonNode item = task.getItems().get(i);
            try {
                JsonNode result = FunctionRegistry.call(task.getFunction(), item, MAPPER);
                results.add(result);
                errors.add(null);
            } catch (Exception e) {
                LOG.warn("item {} failed: {}", i, e.getMessage());
                results.add(NullNode.getInstance());
                errors.add(e.getMessage());
                anyFailed = true;
            }
        }

        // Upload result
        ResultPayload resultPayload = new ResultPayload(results, errors);
        byte[] resultBytes;
        try {
            resultBytes = MAPPER.writeValueAsBytes(resultPayload);
        } catch (Exception e) {
            putText(s3, bucket, statusKey, "failed");
            throw new Exception("serializing result: " + e.getMessage(), e);
        }

        try {
            s3.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(resultKey).build(),
                    AsyncRequestBody.fromBytes(resultBytes)
            ).join();
        } catch (Exception e) {
            putText(s3, bucket, statusKey, "failed");
            throw new Exception("uploading result: " + e.getMessage(), e);
        }

        String finalStatus = anyFailed ? "partial" : "done";
        putText(s3, bucket, statusKey, finalStatus);
        LOG.info("kopi worker complete: status={}", finalStatus);
    }

    // ---------- Helpers ----------

    private static void putText(S3AsyncClient s3, String bucket, String key, String text) {
        try {
            s3.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    AsyncRequestBody.fromBytes(text.getBytes(StandardCharsets.UTF_8))
            ).join();
        } catch (Exception e) {
            LOG.warn("failed to write status {}: {}", key, e.getMessage());
        }
    }

    private static String requireEnv(String name) throws Exception {
        String val = System.getenv(name);
        if (val == null || val.isBlank()) {
            throw new Exception("missing environment variable: " + name);
        }
        return val;
    }

    private static S3AsyncClient buildS3Client(String region) {
        String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
        var builder = S3AsyncClient.builder()
                .region(Region.of(region));
        if (endpointUrl != null && !endpointUrl.isBlank()) {
            builder.endpointOverride(URI.create(endpointUrl))
                   .forcePathStyle(true);
        }
        return builder.build();
    }
}
