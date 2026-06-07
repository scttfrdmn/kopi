package dev.burst.kopi.serialize;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * JSON payload written to S3 as {@code sessions/{id}/tasks/task-NNNN.result}.
 *
 * <pre>{@code
 * {"results": [value_or_null, ...], "errors": [null_or_string, ...]}
 * }</pre>
 *
 * {@code null} entries in {@code results} correspond to failed items;
 * {@code null} entries in {@code errors} correspond to successful items.
 */
public class ResultPayload {

    @JsonProperty("results")
    private List<JsonNode> results;

    @JsonProperty("errors")
    private List<String> errors;

    /** Required by Jackson. */
    public ResultPayload() {}

    public ResultPayload(List<JsonNode> results, List<String> errors) {
        this.results = results;
        this.errors = errors;
    }

    public List<JsonNode> getResults() { return results; }
    public void setResults(List<JsonNode> results) { this.results = results; }

    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors; }
}
