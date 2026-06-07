package dev.burst.kopi;

import dev.burst.kopi.session.SessionId;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class SessionIdTest {

    private static final Pattern JAVA_PATTERN =
            Pattern.compile("^jv-\\d{8}-[0-9a-f]{8}$");
    private static final Pattern SCALA_PATTERN =
            Pattern.compile("^sc-\\d{8}-[0-9a-f]{8}$");

    @Test
    void javaSessionIdMatchesFormat() {
        String id = SessionId.generateJava();
        assertTrue(JAVA_PATTERN.matcher(id).matches(),
                "Expected format jv-{yyyymmdd}-{hex8}, got: " + id);
    }

    @Test
    void scalaSessionIdMatchesFormat() {
        String id = SessionId.generateScala();
        assertTrue(SCALA_PATTERN.matcher(id).matches(),
                "Expected format sc-{yyyymmdd}-{hex8}, got: " + id);
    }

    @Test
    void consecutiveIdsAreUnique() {
        String id1 = SessionId.generateJava();
        String id2 = SessionId.generateJava();
        assertNotEquals(id1, id2, "Two consecutive session IDs should differ (random component)");
    }

    @Test
    void javaPrefixIsJv() {
        String id = SessionId.generateJava();
        assertTrue(id.startsWith("jv-"), "Java session ID must start with jv-");
    }

    @Test
    void scalaPrefixIsSc() {
        String id = SessionId.generateScala();
        assertTrue(id.startsWith("sc-"), "Scala session ID must start with sc-");
    }
}
