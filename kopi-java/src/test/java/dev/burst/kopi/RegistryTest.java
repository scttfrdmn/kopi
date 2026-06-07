package dev.burst.kopi;

import dev.burst.kopi.registry.FunctionRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.junit.jupiter.api.Test;

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
}
