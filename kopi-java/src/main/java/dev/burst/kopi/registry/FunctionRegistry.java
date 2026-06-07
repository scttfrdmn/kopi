package dev.burst.kopi.registry;

import dev.burst.kopi.ThrowingFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Global registry mapping function names to type-erased implementations.
 *
 * <p>Functions are stored as {@link FunctionEntry} records that hold the
 * raw function, input type, and output type for JSON round-tripping.
 */
public final class FunctionRegistry {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, FunctionEntry> REGISTRY = new ConcurrentHashMap<>();

    private FunctionRegistry() {}

    /**
     * Registers a function under {@code name}.
     *
     * @param name       unique function name
     * @param inputType  class of the deserialization target
     * @param outputType class of the serialization source
     * @param fn         the function
     * @param <T>        input type
     * @param <U>        output type
     */
    public static <T, U> void register(
            String name,
            Class<T> inputType,
            Class<U> outputType,
            ThrowingFunction<T, U> fn) {
        REGISTRY.put(name, new FunctionEntry(
                (item, mapper) -> {
                    T input = mapper.treeToValue(item, inputType);
                    U output = fn.apply(input);
                    return mapper.valueToTree(output);
                },
                inputType,
                outputType
        ));
    }

    /**
     * Calls the function registered under {@code name} with the given JSON item.
     *
     * @param name   function name
     * @param item   JSON representation of the input item
     * @param mapper Jackson ObjectMapper to use for (de)serialization
     * @return JSON representation of the result
     * @throws Exception if the function is not registered or throws
     */
    public static JsonNode call(String name, JsonNode item, ObjectMapper mapper) throws Exception {
        FunctionEntry entry = REGISTRY.get(name);
        if (entry == null) {
            throw new IllegalArgumentException(
                    "function \"" + name + "\" not registered — call Kopi.register() before map()");
        }
        return entry.invoke(item, mapper);
    }

    /** Returns {@code true} if a function is registered under {@code name}. */
    public static boolean isRegistered(String name) {
        return REGISTRY.containsKey(name);
    }

    // --- FunctionEntry ---

    /**
     * Type-erased wrapper around a registered function.
     * The {@code TypedFn} functional interface does the actual JSON conversion.
     */
    private record FunctionEntry(
            TypedFn fn,
            Class<?> inputType,
            Class<?> outputType) {

        JsonNode invoke(JsonNode item, ObjectMapper mapper) throws Exception {
            return fn.apply(item, mapper);
        }
    }

    @FunctionalInterface
    private interface TypedFn {
        JsonNode apply(JsonNode item, ObjectMapper mapper) throws Exception;
    }
}
