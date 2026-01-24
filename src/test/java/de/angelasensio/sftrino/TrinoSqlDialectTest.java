package de.angelasensio.sftrino;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for TrinoSqlDialect helper methods.
 */
public class TrinoSqlDialectTest {

    private final TrinoSqlDialect dialect = new TrinoSqlDialect();

    @Test
    public void testQuoteIdentifierSimple() {
        assertEquals("\"foo\"", dialect.quoteIdentifier("foo"));
    }

    @Test
    public void testQuoteIdentifierEscapesDoubleQuotes() {
        // Double-quotes inside an identifier must be escaped as ""
        assertEquals("\"foo\"\"bar\"", dialect.quoteIdentifier("foo\"bar"));
    }

    @Test
    public void testSupportsCharSetReturnsFalse() {
        assertFalse(dialect.supportsCharSet());
    }

    @Test
    public void testSupportsAliasedValuesReturnsTrue() {
        assertTrue(dialect.supportsAliasedValues());
    }

    @Test
    public void testEmulateNullDirectionReturnsNull() {
        // Trino supports NULLS FIRST/LAST natively, so emulation returns null
        assertNull(dialect.emulateNullDirection(null, true, false));
        assertNull(dialect.emulateNullDirection(null, false, true));
    }
}
