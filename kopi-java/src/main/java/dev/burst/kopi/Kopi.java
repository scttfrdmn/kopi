package dev.burst.kopi;

import dev.burst.kopi.config.Config;
import dev.burst.kopi.registry.FunctionRegistry;
import dev.burst.kopi.session.Session;
import dev.burst.kopi.worker.Worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Main public API entry point for kopi — cloud bursting for Java.
 *
 * <p>The same JAR that runs your application IS the worker. Register functions
 * before the worker check, then call {@link #map} to distribute work across
 * AWS Fargate.
 *
 * <pre>{@code
 * public static void main(String[] args) throws Exception {
 *     // Register before the worker check
 *     Kopi.register("double", Integer.class, Integer.class, x -> x * 2);
 *
 *     if (Kopi.isWorker()) {
 *         System.exit(Kopi.runWorker());
 *     }
 *
 *     List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
 *     List<Integer> results = Kopi.map("double", items, Integer.class);
 *     System.out.println("got " + results.size() + " results");
 * }
 * }</pre>
 */
public final class Kopi {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Kopi() {}

    /**
     * Registers a function by name for worker dispatch.
     *
     * @param name       unique function name
     * @param inputType  class of the input type (for JSON deserialization)
     * @param outputType class of the output type (for JSON serialization)
     * @param fn         the function to register
     * @param <T>        input type
     * @param <U>        output type
     */
    public static <T, U> void register(
            String name,
            Class<T> inputType,
            Class<U> outputType,
            ThrowingFunction<T, U> fn) {
        FunctionRegistry.register(name, inputType, outputType, fn);
    }

    /**
     * Returns {@code true} if this process is running in worker mode.
     *
     * <p>Worker mode is active when {@code BURST_WORKER=1} is set in the environment.
     */
    public static boolean isWorker() {
        return Worker.isWorker();
    }

    /**
     * Runs the worker lifecycle: download task → dispatch function → upload result.
     *
     * <p>Call from {@code main()} when {@link #isWorker()} returns {@code true},
     * then pass the return value to {@code System.exit()}.
     *
     * @return 0 on success, 1 on error
     */
    public static int runWorker() throws Exception {
        return Worker.run();
    }

    /**
     * Distributes {@code items} across AWS Fargate workers, calling the function
     * registered under {@code fnName} on each item. Returns results in the same
     * order as the input.
     *
     * @param fnName     name of the registered function
     * @param items      list of items to process
     * @param resultType class of the result type
     * @param opts       options (workers, CPU, memory, etc.)
     * @param <T>        input item type
     * @param <U>        result type
     * @return ordered list of results
     * @throws KopiException on AWS errors, cost limit exceeded, or partial failure
     */
    public static <T, U> List<U> map(
            String fnName,
            List<T> items,
            Class<U> resultType,
            MapOptions opts) throws KopiException {
        Config cfg;
        try {
            cfg = Config.load();
        } catch (Exception e) {
            throw new KopiException("failed to load burst config: " + e.getMessage(), e);
        }

        // Serialize items to JsonNode
        List<JsonNode> itemNodes = new java.util.ArrayList<>();
        for (T item : items) {
            itemNodes.add(MAPPER.valueToTree(item));
        }

        return Session.runSession(cfg, itemNodes, fnName, resultType, opts);
    }

    /**
     * Distributes {@code items} across AWS Fargate workers using default options.
     *
     * @see #map(String, List, Class, MapOptions)
     */
    public static <T, U> List<U> map(
            String fnName,
            List<T> items,
            Class<U> resultType) throws KopiException {
        return map(fnName, items, resultType, MapOptions.defaults());
    }
}
