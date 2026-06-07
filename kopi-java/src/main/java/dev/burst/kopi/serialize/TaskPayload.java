package dev.burst.kopi.serialize;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * JSON payload written to S3 as {@code sessions/{id}/tasks/task-NNNN.task}.
 *
 * <pre>{@code
 * {"items": [...], "function": "fn_name", "chunk_index": 0}
 * }</pre>
 */
public class TaskPayload {

    @JsonProperty("items")
    private List<JsonNode> items;

    @JsonProperty("function")
    private String function;

    @JsonProperty("chunk_index")
    private int chunkIndex;

    /** Required by Jackson. */
    public TaskPayload() {}

    public TaskPayload(List<JsonNode> items, String function, int chunkIndex) {
        this.items = items;
        this.function = function;
        this.chunkIndex = chunkIndex;
    }

    public List<JsonNode> getItems() { return items; }
    public void setItems(List<JsonNode> items) { this.items = items; }

    public String getFunction() { return function; }
    public void setFunction(String function) { this.function = function; }

    public int getChunkIndex() { return chunkIndex; }
    public void setChunkIndex(int chunkIndex) { this.chunkIndex = chunkIndex; }
}
