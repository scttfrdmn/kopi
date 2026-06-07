package dev.burst.kopi;

/**
 * Base exception for all kopi errors.
 */
public class KopiException extends Exception {

    public KopiException(String message) {
        super(message);
    }

    public KopiException(String message, Throwable cause) {
        super(message, cause);
    }
}
