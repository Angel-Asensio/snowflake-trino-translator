package de.angelasensio.sftrino;

/** Classification for translation warnings returned in {@link TranslationResult}. */
public enum WarningType {
    /** One or more function arguments were silently dropped during translation. */
    ARGUMENT_DROPPED,
    /** The translation is semantically approximate rather than exact. */
    APPROXIMATE_TRANSLATION,
    /** The Snowflake feature has no direct Trino equivalent and was passed through or skipped. */
    UNSUPPORTED_FEATURE,
    /** One or more lateral column alias references were expanded inline for Trino compatibility. */
    LATERAL_COLUMN_ALIAS_REWRITE
}
