package dev.burst.kopi.session;

import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Generates session IDs conforming to the burst family protocol.
 *
 * <ul>
 *   <li>Java:  {@code jv-{yyyymmdd}-{random8hex}}
 *   <li>Scala: {@code sc-{yyyymmdd}-{random8hex}}
 * </ul>
 */
public final class SessionId {

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
    private static final SecureRandom RNG = new SecureRandom();

    private SessionId() {}

    /** Generates a Java session ID: {@code jv-{yyyymmdd}-{random8hex}}. */
    public static String generateJava() {
        return generate("jv");
    }

    /** Generates a Scala session ID: {@code sc-{yyyymmdd}-{random8hex}}. */
    public static String generateScala() {
        return generate("sc");
    }

    private static String generate(String prefix) {
        String date = DATE_FMT.format(Instant.now());
        byte[] bytes = new byte[4];
        RNG.nextBytes(bytes);
        String hex = bytesToHex(bytes);
        return prefix + "-" + date + "-" + hex;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }
}
