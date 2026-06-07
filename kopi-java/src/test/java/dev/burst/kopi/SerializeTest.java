package dev.burst.kopi;

import dev.burst.kopi.serialize.ResultPayload;
import dev.burst.kopi.serialize.TaskPayload;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SerializeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void taskPayloadRoundtrip() throws Exception {
        List<JsonNode> items = Arrays.asList(
                IntNode.valueOf(1),
                IntNode.valueOf(2),
                TextNode.valueOf("hello")
        );
        TaskPayload original = new TaskPayload(items, "my_fn", 3);

        byte[] bytes = MAPPER.writeValueAsBytes(original);
        TaskPayload deserialized = MAPPER.readValue(bytes, TaskPayload.class);

        assertEquals("my_fn", deserialized.getFunction());
        assertEquals(3, deserialized.getChunkIndex());
        assertEquals(3, deserialized.getItems().size());
        assertEquals(1, deserialized.getItems().get(0).asInt());
        assertEquals(2, deserialized.getItems().get(1).asInt());
        assertEquals("hello", deserialized.getItems().get(2).asText());
    }

    @Test
    void taskPayloadJsonKeys() throws Exception {
        TaskPayload payload = new TaskPayload(List.of(IntNode.valueOf(42)), "fn", 0);
        String json = MAPPER.writeValueAsString(payload);

        assertTrue(json.contains("\"items\""), "should have 'items' key");
        assertTrue(json.contains("\"function\""), "should have 'function' key");
        assertTrue(json.contains("\"chunk_index\""), "should have 'chunk_index' key (snake_case)");
    }

    @Test
    void resultPayloadRoundtrip() throws Exception {
        List<JsonNode> results = Arrays.asList(
                IntNode.valueOf(100),
                null,
                TextNode.valueOf("ok")
        );
        List<String> errors = Arrays.asList(null, "something failed", null);

        ResultPayload original = new ResultPayload(results, errors);

        byte[] bytes = MAPPER.writeValueAsBytes(original);
        ResultPayload deserialized = MAPPER.readValue(bytes, ResultPayload.class);

        assertEquals(3, deserialized.getResults().size());
        assertEquals(100, deserialized.getResults().get(0).asInt());
        assertEquals(3, deserialized.getErrors().size());
        assertEquals("something failed", deserialized.getErrors().get(1));
        assertNull(deserialized.getErrors().get(0));
        assertNull(deserialized.getErrors().get(2));
    }

    @Test
    void resultPayloadJsonKeys() throws Exception {
        List<String> errors = new java.util.ArrayList<>();
        errors.add(null);
        ResultPayload payload = new ResultPayload(
                List.of(IntNode.valueOf(1)),
                errors
        );
        String json = MAPPER.writeValueAsString(payload);

        assertTrue(json.contains("\"results\""), "should have 'results' key");
        assertTrue(json.contains("\"errors\""), "should have 'errors' key");
    }
}
