package de.angelasensio.sftrino;

/**
 * A single warning produced during SQL translation.
 *
 * @param functionName the Snowflake function that triggered the warning
 * @param type         the category of the warning
 * @param message      human-readable description of what was lost or approximated
 */
public record TranslationWarning(String functionName, WarningType type, String message) {}
