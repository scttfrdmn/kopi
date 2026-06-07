package dev.burst.kopi.integration;

import dev.burst.kopi.registry.FunctionRegistry;
import dev.burst.kopi.serialize.ResultPayload;
import dev.burst.kopi.serialize.TaskPayload;
import dev.burst.kopi.session.Session;
import dev.burst.kopi.session.SessionId;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that exercise the full S3 protocol against a real (or local)
 * AWS endpoint.
 *
 * <p>Enabled only when {@code -DBURST_INTEGRATION_TEST=1} is set. When
 * {@code AWS_ENDPOINT_URL} is set, a local substrate endpoint is used instead
 * of real AWS.
 */
@EnabledIfSystemProperty(named = "BURST_INTEGRATION_TEST", matches = "1")
class S3IntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BUCKET = "kopi-integration-test";
    private static final String REGION = "us-east-1";

    private static S3AsyncClient s3;

    @BeforeAll
    static void setUp() throws Exception {
        s3 = buildS3Client();

        // Create bucket (ignore if already exists)
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()).join();
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg != null &&
                    (msg.contains("BucketAlreadyOwnedByYou") || msg.contains("BucketAlreadyExists"))) {
                // OK — bucket already exists
            } else {
                throw e;
            }
        }
    }

    @Test
    void s3RoundtripSingleTask() throws Exception {
        String sessionId = SessionId.generateJava();
        String tid = "task-0000";

        // Write task file
        List<JsonNode> items = List.of(IntNode.valueOf(5), IntNode.valueOf(10));
        TaskPayload taskPayload = new TaskPayload(items, "double", 0);
        byte[] taskBytes = MAPPER.writeValueAsBytes(taskPayload);
        String taskKey = Session.taskKey(sessionId, tid);

        s3.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(taskKey).build(),
                AsyncRequestBody.fromBytes(taskBytes)
        ).join();

        // Simulate worker writing result + status
        List<JsonNode> results = List.of(IntNode.valueOf(10), IntNode.valueOf(20));
        List<String> errors = new ArrayList<>();
        errors.add(null);
        errors.add(null);
        ResultPayload resultPayload = new ResultPayload(results, errors);
        byte[] resultBytes = MAPPER.writeValueAsBytes(resultPayload);

        String resultKey = Session.resultKey(sessionId, tid);
        String statusKey = Session.statusKey(sessionId, tid);

        s3.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(resultKey).build(),
                AsyncRequestBody.fromBytes(resultBytes)
        ).join();
        s3.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(statusKey).build(),
                AsyncRequestBody.fromBytes("done".getBytes(StandardCharsets.UTF_8))
        ).join();

        // Poll status and verify
        String status = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(statusKey).build(),
                AsyncResponseTransformer.toBytes()
        ).join().asUtf8String().trim();
        assertEquals("done", status);

        // Download and verify result
        byte[] downloadedResult = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(resultKey).build(),
                AsyncResponseTransformer.toBytes()
        ).join().asByteArray();

        ResultPayload downloaded = MAPPER.readValue(downloadedResult, ResultPayload.class);
        assertEquals(2, downloaded.getResults().size());
        assertEquals(10, downloaded.getResults().get(0).asInt());
        assertEquals(20, downloaded.getResults().get(1).asInt());
    }

    @Test
    void multipleTasksAllComplete() throws Exception {
        int taskCount = 3;
        String sessionId = SessionId.generateJava();

        // Write tasks
        for (int i = 0; i < taskCount; i++) {
            String tid = Session.taskId(i);
            TaskPayload payload = new TaskPayload(
                    List.of(IntNode.valueOf(i)),
                    "identity",
                    i
            );
            byte[] body = MAPPER.writeValueAsBytes(payload);
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET).key(Session.taskKey(sessionId, tid)).build(),
                    AsyncRequestBody.fromBytes(body)
            ).join();
        }

        // Simulate workers completing
        for (int i = 0; i < taskCount; i++) {
            String tid = Session.taskId(i);
            ResultPayload rp = new ResultPayload(
                    List.of(IntNode.valueOf(i * 2)),
                    java.util.Arrays.asList((String) null)
            );
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET).key(Session.resultKey(sessionId, tid)).build(),
                    AsyncRequestBody.fromBytes(MAPPER.writeValueAsBytes(rp))
            ).join();
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET).key(Session.statusKey(sessionId, tid)).build(),
                    AsyncRequestBody.fromBytes("done".getBytes(StandardCharsets.UTF_8))
            ).join();
        }

        // Verify all statuses are terminal
        int done = 0;
        for (int i = 0; i < taskCount; i++) {
            String key = Session.statusKey(sessionId, Session.taskId(i));
            String status = s3.getObject(
                    GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                    AsyncResponseTransformer.toBytes()
            ).join().asUtf8String().trim();
            if ("done".equals(status)) done++;
        }
        assertEquals(taskCount, done);

        // Verify results
        for (int i = 0; i < taskCount; i++) {
            String key = Session.resultKey(sessionId, Session.taskId(i));
            byte[] bytes = s3.getObject(
                    GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                    AsyncResponseTransformer.toBytes()
            ).join().asByteArray();
            ResultPayload rp = MAPPER.readValue(bytes, ResultPayload.class);
            assertEquals(i * 2, rp.getResults().get(0).asInt());
        }
    }

    private static S3AsyncClient buildS3Client() {
        String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
        var builder = S3AsyncClient.builder().region(Region.of(REGION));
        if (endpointUrl != null && !endpointUrl.isBlank()) {
            builder.endpointOverride(URI.create(endpointUrl))
                   .forcePathStyle(true);
        }
        return builder.build();
    }
}
