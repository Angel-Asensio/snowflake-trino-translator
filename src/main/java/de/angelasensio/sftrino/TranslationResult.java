package de.angelasensio.sftrino;

import java.util.List;

/**
 * The result of a SQL translation, including the translated SQL and any warnings.
 *
 * <p>Obtain via {@link SnowflakeTrinoTranslator#translateWithDiagnostics(String)}.
 * For callers that only need the SQL string, {@link SnowflakeTrinoTranslator#translate(String)}
 * remains available as a convenience wrapper.
 *
 * @param sql      the translated Trino SQL string
 * @param warnings zero or more warnings about arguments dropped, approximations made, or
 *                 unsupported features encountered during translation
 */
public record TranslationResult(String sql, List<TranslationWarning> warnings) {

    /** Returns {@code true} if the translation produced at least one warning. */
    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }
}
