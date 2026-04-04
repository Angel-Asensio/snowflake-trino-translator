package de.angelasensio.sftrino;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Public API for translating Snowflake SQL to Trino SQL.
 *
 * <p>Uses Apache Calcite's Babel parser to build an AST, transforms it via
 * {@link SnowflakeToTrinoConverter}, then renders it with {@link TrinoSqlDialect}.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * SnowflakeTrinoTranslator translator = new SnowflakeTrinoTranslator();
 *
 * // Simple — just the SQL string:
 * String trinoSql = translator.translate(snowflakeSql);
 *
 * // With diagnostics — SQL + structured warnings:
 * TranslationResult result = translator.translateWithDiagnostics(snowflakeSql);
 * if (result.hasWarnings()) {
 *     result.warnings().forEach(w -> log.warn("{}: {}", w.functionName(), w.message()));
 * }
 * }</pre>
 */
public class SnowflakeTrinoTranslator {

    private static final Logger logger = LoggerFactory.getLogger(SnowflakeTrinoTranslator.class);

    private final SqlParser.Config parserConfig;
    private final TrinoSqlDialect trinoDialect;
    private final SnowflakeToTrinoConverter converter;

    public SnowflakeTrinoTranslator() {
        this.parserConfig = SqlParser.config()
                .withParserFactory(SqlBabelParserImpl.FACTORY)
                .withLex(Lex.MYSQL)
                .withCaseSensitive(false);
        this.trinoDialect = new TrinoSqlDialect();
        this.converter = new SnowflakeToTrinoConverter();
    }

    /**
     * Returns the underlying {@link SnowflakeToTrinoConverter}, allowing callers to register
     * custom function converters via {@link SnowflakeToTrinoConverter#register(String, FunctionConverter)}.
     */
    public SnowflakeToTrinoConverter getConverter() {
        return converter;
    }

    /**
     * Translates Snowflake SQL to Trino SQL, returning only the translated string.
     *
     * <p>For structured access to warnings about dropped arguments or unsupported features,
     * use {@link #translateWithDiagnostics(String)} instead.
     *
     * @param snowflakeSql the input Snowflake SQL query
     * @return the translated Trino SQL string
     * @throws SqlTranslationException if parsing or transformation fails
     */
    public String translate(String snowflakeSql) throws SqlTranslationException {
        return translateWithDiagnostics(snowflakeSql).sql();
    }

    /**
     * Translates Snowflake SQL to Trino SQL, returning both the translated string and
     * any warnings accumulated during translation (e.g. dropped arguments, approximate translations).
     *
     * @param snowflakeSql the input Snowflake SQL query
     * @return a {@link TranslationResult} containing the Trino SQL and any warnings
     * @throws SqlTranslationException if parsing or transformation fails
     */
    public TranslationResult translateWithDiagnostics(String snowflakeSql) throws SqlTranslationException {
        if (snowflakeSql == null || snowflakeSql.trim().isEmpty()) {
            throw new SqlTranslationException("Input SQL cannot be null or empty");
        }

        try {
            String preprocessed = preprocess(snowflakeSql);
            logger.debug("Parsing Snowflake SQL: {}", preprocessed);

            SqlParser parser = SqlParser.create(preprocessed, parserConfig);
            SqlNode sqlNode = parser.parseQuery();
            logger.debug("Parsed AST: {}", sqlNode);

            converter.clearWarnings();
            SqlNode transformedNode = converter.convert(sqlNode);
            logger.debug("Transformed AST: {}", transformedNode);

            String trinoSql = transformedNode.toSqlString(trinoDialect).getSql();
            logger.debug("Translated Trino SQL: {}", trinoSql);

            return new TranslationResult(trinoSql, converter.getWarnings());

        } catch (SqlParseException e) {
            logger.error("Failed to parse Snowflake SQL", e);
            throw new SqlTranslationException("Failed to parse Snowflake SQL: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Failed to translate SQL", e);
            throw new SqlTranslationException("Failed to translate SQL: " + e.getMessage(), e);
        }
    }

    /**
     * Applies text-level normalizations required before Calcite can parse the SQL.
     *
     * <p>These transformations work around three classes of Calcite parser limitations:
     *
     * <ol>
     *   <li><b>Quoted time-unit strings</b> — Snowflake accepts {@code DATEADD('day', ...)} and
     *       {@code DATEDIFF('day', ...)} with the unit as a quoted string literal. Calcite's Babel
     *       parser only accepts an unquoted identifier in that position, so the quotes are stripped.
     *
     *   <li><b>CURRENT_DATE() / CURRENT_TIMESTAMP() with empty parens</b> — Calcite treats these
     *       as SQL keywords and rejects an empty argument list. The trailing {@code ()} is removed
     *       before parsing.
     *
     *   <li><b>CEIL / FLOOR / DATE_FROM_PARTS as reserved grammar rules</b> — Calcite's parser
     *       routes {@code CEIL} and {@code FLOOR} through a special FloorCeilOptions grammar rule
     *       (which does not accept a second scale argument) and routes {@code DATE_FROM_PARTS}
     *       through DATE keyword handling. All three are renamed to {@code SF_} prefixed aliases
     *       so they are parsed as plain user-defined functions. The converter handles them under
     *       their aliased names and outputs the correct Trino function names.
     * </ol>
     */
    private String preprocess(String sql) {
        // (1) Strip quotes from time-unit args: DATEADD('day', → DATEADD(day,
        sql = sql.replaceAll(
                "(?i)(DATEADD|DATEDIFF)\\s*\\(\\s*'([A-Za-z_]+)'",
                "$1($2");

        // (2) Strip empty parens from SQL keyword functions
        sql = sql.replaceAll("(?i)\\bCURRENT_DATE\\s*\\(\\s*\\)", "CURRENT_DATE");
        sql = sql.replaceAll("(?i)\\bCURRENT_TIMESTAMP\\s*\\(\\s*\\)", "CURRENT_TIMESTAMP");

        // (3) Rename grammar-conflicting functions to plain UDF aliases
        sql = sql.replaceAll("(?i)\\bCEIL\\s*\\(", "SF_CEIL(");
        sql = sql.replaceAll("(?i)\\bFLOOR\\s*\\(", "SF_FLOOR(");
        sql = sql.replaceAll("(?i)\\bDATE_FROM_PARTS\\s*\\(", "SF_DATE_FROM_PARTS(");

        return sql;
    }
}
