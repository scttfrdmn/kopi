package dev.burst.kopi;

/**
 * A function that may throw a checked exception.
 *
 * @param <T> input type
 * @param <U> output type
 */
@FunctionalInterface
public interface ThrowingFunction<T, U> {
    U apply(T input) throws Exception;
}
