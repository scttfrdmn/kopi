package dev.burst.kopi;

/**
 * Holds a single item's outcome from {@link Kopi#mapTolerant}.
 * On success, {@link #value()} is non-null and {@link #error()} is null.
 * On failure, {@link #value()} is null and {@link #error()} describes the failure.
 */
public record PartialResult<U>(U value, String error) {

    /** Returns true if this item succeeded. */
    public boolean isSuccess() {
        return error == null;
    }

    /** Factory for a successful result. */
    public static <U> PartialResult<U> success(U value) {
        return new PartialResult<>(value, null);
    }

    /** Factory for a failed result. */
    public static <U> PartialResult<U> failure(String error) {
        return new PartialResult<>(null, error);
    }
}
