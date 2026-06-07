package dev.burst.kopi;

import dev.burst.kopi.registry.FunctionRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;

import dev.burst.kopi.session.Session;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RegistryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void registerAndCallIntFunction() throws Exception {
        FunctionRegistry.register(
                "registry_test_double",
                Integer.class,
                Integer.class,
                x -> x * 2
        );

        JsonNode result = FunctionRegistry.call(
                "registry_test_double", IntNode.valueOf(21), MAPPER);

        assertEquals(42, result.asInt());
    }

    @Test
    void registerAndCallStringFunction() throws Exception {
        FunctionRegistry.register(
                "registry_test_upper",
                String.class,
                String.class,
                String::toUpperCase
        );

        JsonNode result = FunctionRegistry.call(
                "registry_test_upper", TextNode.valueOf("hello"), MAPPER);

        assertEquals("HELLO", result.asText());
    }

    @Test
    void callUnregisteredFunctionThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                FunctionRegistry.call("__not_registered__", IntNode.valueOf(1), MAPPER));
    }

    @Test
    void functionThatThrowsProducesException() {
        FunctionRegistry.register(
                "registry_test_throws",
                Integer.class,
                Integer.class,
                x -> { throw new RuntimeException("intentional failure"); }
        );

        assertThrows(Exception.class, () ->
                FunctionRegistry.call("registry_test_throws", IntNode.valueOf(1), MAPPER));
    }

    @Test
    void isRegisteredReturnsTrueAfterRegister() {
        FunctionRegistry.register(
                "registry_test_check",
                Integer.class,
                Integer.class,
                x -> x
        );
        assertTrue(FunctionRegistry.isRegistered("registry_test_check"));
    }

    @Test
    void isRegisteredReturnsFalseForUnknown() {
        assertFalse(FunctionRegistry.isRegistered("__no_such_fn__"));
    }

    // -------------------------------------------------------------------------
    // PartialResult type tests
    // -------------------------------------------------------------------------

    @Test
    void partialResult_success_isSuccess() {
        PartialResult<Integer> r = PartialResult.success(42);
        assertTrue(r.isSuccess());
        assertEquals(42, r.value());
        assertNull(r.error());
    }

    @Test
    void partialResult_failure_isNotSuccess() {
        PartialResult<Integer> r = PartialResult.failure("something went wrong");
        assertFalse(r.isSuccess());
        assertNull(r.value());
        assertEquals("something went wrong", r.error());
    }

    @Test
    void flattenResultsTolerant_returnsMixedResults() throws Exception {
        // Build two ResultPayloads: one success, one failure
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();

        // Simulate a successful result payload (item value=10, no error)
        com.fasterxml.jackson.databind.node.IntNode successNode =
                com.fasterxml.jackson.databind.node.IntNode.valueOf(10);
        dev.burst.kopi.serialize.ResultPayload successPayload =
                new dev.burst.kopi.serialize.ResultPayload(
                        List.of(successNode), Arrays.asList((String) null));  // null-tolerant list

        // Simulate a failed result payload (null result, error string)
        com.fasterxml.jackson.databind.node.NullNode failNode =
                com.fasterxml.jackson.databind.node.NullNode.getInstance();
        dev.burst.kopi.serialize.ResultPayload failPayload =
                new dev.burst.kopi.serialize.ResultPayload(
                        List.of(failNode), List.of("odd: 5"));

        List<dev.burst.kopi.serialize.ResultPayload> payloads =
                List.of(successPayload, failPayload);

        List<PartialResult<Integer>> results =
                Session.flattenResultsTolerant(payloads, Integer.class);

        assertEquals(2, results.size());
        assertTrue(results.get(0).isSuccess());
        assertEquals(10, results.get(0).value());
        assertFalse(results.get(1).isSuccess());
        assertEquals("odd: 5", results.get(1).error());
    }
}
