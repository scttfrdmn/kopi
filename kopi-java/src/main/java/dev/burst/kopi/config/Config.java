package dev.burst.kopi.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Configuration loaded from {@code ~/.burst/config.json} or the path
 * specified by the {@code BURST_CONFIG_PATH} environment variable.
 *
 * <p>All fields match the shared burst family config schema (snake_case JSON).
 */
public class Config {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("region")
    private String region = "us-west-2";

    @JsonProperty("s3_bucket")
    private String s3Bucket = "";

    @JsonProperty("ecs_cluster")
    private String ecsCluster = "burst-cluster";

    @JsonProperty("ecr_base_uri")
    private String ecrBaseUri = "";

    @JsonProperty("execution_role_arn")
    private String executionRoleArn = "";

    @JsonProperty("task_role_arn")
    private String taskRoleArn = "";

    @JsonProperty("default_workers")
    private int defaultWorkers = 10;

    @JsonProperty("default_cpu")
    private int defaultCpu = 2;

    @JsonProperty("default_memory_gb")
    private int defaultMemoryGb = 4;

    @JsonProperty("backend")
    private String backend = "fargate";

    @JsonProperty("spot")
    private boolean spot = false;

    @JsonProperty("max_cost_per_job")
    private double maxCostPerJob = 0.0;

    @JsonProperty("cost_alert_threshold")
    private double costAlertThreshold = 0.0;

    @JsonProperty("fargate_quota_vcpu")
    private double fargateQuotaVcpu = 4000.0;

    // Required by Jackson
    public Config() {}

    /**
     * Loads the burst config from {@code BURST_CONFIG_PATH} or
     * {@code ~/.burst/config.json}.
     *
     * @throws IOException if the file cannot be read or parsed
     */
    public static Config load() throws IOException {
        String envPath = System.getenv("BURST_CONFIG_PATH");
        Path configPath;
        if (envPath != null && !envPath.isBlank()) {
            configPath = Paths.get(envPath);
        } else {
            String home = System.getProperty("user.home");
            configPath = Paths.get(home, ".burst", "config.json");
        }

        if (!Files.exists(configPath)) {
            throw new IOException(
                    "burst config not found at " + configPath +
                    " — run `burst-core setup` to create it");
        }

        byte[] bytes = Files.readAllBytes(configPath);
        return MAPPER.readValue(bytes, Config.class);
    }

    // --- Getters ---

    public String getRegion() { return region; }
    public String getS3Bucket() { return s3Bucket; }
    public String getEcsCluster() { return ecsCluster; }
    public String getEcrBaseUri() { return ecrBaseUri; }
    public String getExecutionRoleArn() { return executionRoleArn; }
    public String getTaskRoleArn() { return taskRoleArn; }
    public int getDefaultWorkers() { return defaultWorkers; }
    public int getDefaultCpu() { return defaultCpu; }
    public int getDefaultMemoryGb() { return defaultMemoryGb; }
    public String getBackend() { return backend; }
    public boolean isSpot() { return spot; }
    public double getMaxCostPerJob() { return maxCostPerJob; }
    public double getCostAlertThreshold() { return costAlertThreshold; }
    public double getFargateQuotaVcpu() { return fargateQuotaVcpu; }
}
