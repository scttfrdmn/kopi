package dev.burst.kopi;

import java.util.List;

/**
 * Thrown when a burst map completes but some items produced errors.
 * Partial results are available via {@link #getResults()}.
 */
public class PartialException extends KopiException {

    private final List<Object> results;
    private final List<String> errors;
    private final int failed;
    private final int succeeded;

    public PartialException(
            List<Object> results,
            List<String> errors,
            int failed,
            int succeeded) {
        super(String.format(
                "burst map partially failed: %d succeeded, %d failed",
                succeeded, failed));
        this.results = List.copyOf(results);
        this.errors = List.copyOf(errors);
        this.failed = failed;
        this.succeeded = succeeded;
    }

    /** Partial results — null entries correspond to failed items. */
    public List<Object> getResults() {
        return results;
    }

    /** Error messages — null entries correspond to successful items. */
    public List<String> getErrors() {
        return errors;
    }

    public int getFailed() {
        return failed;
    }

    public int getSucceeded() {
        return succeeded;
    }
}
