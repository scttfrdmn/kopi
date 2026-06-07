package dev.burst.kopi;

/**
 * Builder for configuring a burst map operation.
 *
 * All fields are optional — unset fields (0 / null / false) fall back to
 * the values in {@code ~/.burst/config.json}.
 *
 * <pre>{@code
 * MapOptions opts = MapOptions.defaults()
 *     .workers(20)
 *     .cpu(2)
 *     .memoryGb(4)
 *     .spot(true);
 * }</pre>
 */
public class MapOptions {

    private int workers;
    private int cpu;
    private int memoryGb;
    private String backend;
    private boolean spot;
    private double maxCost;
    private int timeoutSeconds;
    private String region;
    private String arch = "amd64";

    private MapOptions() {}

    /** Creates a new {@code MapOptions} with all fields unset (use config defaults). */
    public static MapOptions defaults() {
        return new MapOptions();
    }

    /** Number of ECS workers to launch. */
    public MapOptions workers(int n) {
        this.workers = n;
        return this;
    }

    /** vCPUs per worker (valid Fargate values: 0.25, 0.5, 1, 2, 4, 8, 16). */
    public MapOptions cpu(int cpu) {
        this.cpu = cpu;
        return this;
    }

    /** Memory in GB per worker. */
    public MapOptions memoryGb(int gb) {
        this.memoryGb = gb;
        return this;
    }

    /** Compute backend: {@code "fargate"} (default) or {@code "ec2"}. */
    public MapOptions backend(String backend) {
        this.backend = backend;
        return this;
    }

    /** Use Fargate Spot for lower cost (with possible interruption). */
    public MapOptions spot(boolean spot) {
        this.spot = spot;
        return this;
    }

    /** Maximum cost ceiling in USD/hr. Throws {@link KopiException} if exceeded. */
    public MapOptions maxCost(double usd) {
        this.maxCost = usd;
        return this;
    }

    /** Timeout in seconds for waiting on results. 0 means unlimited. */
    public MapOptions timeout(int seconds) {
        this.timeoutSeconds = seconds;
        return this;
    }

    /** Override the AWS region from config. */
    public MapOptions region(String region) {
        this.region = region;
        return this;
    }

    /** CPU architecture for Fargate workers: {@code "amd64"} (default) or {@code "arm64"} (Graviton). */
    public MapOptions arch(String arch) {
        this.arch = arch;
        return this;
    }

    // --- getters ---

    public int getWorkers() { return workers; }
    public int getCpu() { return cpu; }
    public int getMemoryGb() { return memoryGb; }
    public String getBackend() { return backend; }
    public boolean isSpot() { return spot; }
    public double getMaxCost() { return maxCost; }
    public int getTimeoutSeconds() { return timeoutSeconds; }
    public String getRegion() { return region; }
    public String getArch() { return arch; }
}
