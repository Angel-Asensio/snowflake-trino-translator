package de.angelasensio.sftrino;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main translator class for converting Snowflake SQL to Trino SQL.
 * Uses Apache Calcite for parsing and transformation.
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
     * Translates Snowflake SQL to Trino SQL.
     *
     * @param snowflakeSql The input Snowflake SQL query
     * @return The translated Trino SQL query
     * @throws SqlTranslationException if translation fails
     */
    public String translate(String snowflakeSql) throws SqlTranslationException {
        if (snowflakeSql == null || snowflakeSql.trim().isEmpty()) {
            throw new SqlTranslationException("Input SQL cannot be null or empty");
        }
        
        try {
            // Snowflake accepts quoted time unit strings: DATEADD('day', ...) and DATEDIFF('day', ...).
            // Calcite's Babel parser only accepts unquoted identifiers in that position.
            // Normalize before parsing: DATEADD('day', → DATEADD(day,
            snowflakeSql = snowflakeSql.replaceAll(
                    "(?i)(DATEADD|DATEDIFF)\\s*\\(\\s*'([A-Za-z_]+)'",
                    "$1($2");

            // CURRENT_DATE() / CURRENT_TIMESTAMP() with parens: Calcite treats these as SQL keywords
            // and rejects the empty argument list. Strip the parens before parsing.
            snowflakeSql = snowflakeSql.replaceAll("(?i)\\bCURRENT_DATE\\s*\\(\\s*\\)", "CURRENT_DATE");
            snowflakeSql = snowflakeSql.replaceAll("(?i)\\bCURRENT_TIMESTAMP\\s*\\(\\s*\\)", "CURRENT_TIMESTAMP");

            // CEIL/FLOOR with a scale argument and DATE_FROM_PARTS: Calcite routes these through
            // its special built-in grammar (FloorCeilOptions / DATE keyword) and fails at parse time.
            // Rename them to internal aliases that are treated as plain user-defined functions.
            snowflakeSql = snowflakeSql.replaceAll("(?i)\\bCEIL\\s*\\(", "SF_CEIL(");
            snowflakeSql = snowflakeSql.replaceAll("(?i)\\bFLOOR\\s*\\(", "SF_FLOOR(");
            snowflakeSql = snowflakeSql.replaceAll("(?i)\\bDATE_FROM_PARTS\\s*\\(", "SF_DATE_FROM_PARTS(");

            logger.debug("Parsing Snowflake SQL: {}", snowflakeSql);

            // Parse the Snowflake SQL into an AST
            SqlParser parser = SqlParser.create(snowflakeSql, parserConfig);
            SqlNode sqlNode = parser.parseQuery();
            
            logger.debug("Parsed AST: {}", sqlNode);
            
            // Transform Snowflake-specific constructs to Trino equivalents
            SqlNode transformedNode = converter.convert(sqlNode);
            
            logger.debug("Transformed AST: {}", transformedNode);
            
            // Generate Trino SQL from the transformed AST
            String trinoSql = transformedNode.toSqlString(trinoDialect).getSql();
            
            logger.debug("Successfully translated SQL");
            logger.debug("Trino SQL: {}", trinoSql);
            
            return trinoSql;
            
        } catch (SqlParseException e) {
            logger.error("Failed to parse Snowflake SQL", e);
            throw new SqlTranslationException("Failed to parse Snowflake SQL: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Failed to translate SQL", e);
            throw new SqlTranslationException("Failed to translate SQL: " + e.getMessage(), e);
        }
    }
    
    /**
     * Main method for command-line usage.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -jar snowflake-trino-translator.jar \"<SQL_QUERY>\"");
            System.exit(1);
        }
        
        String snowflakeSql = args[0];
        SnowflakeTrinoTranslator translator = new SnowflakeTrinoTranslator();
        
        try {
            String trinoSql = translator.translate(snowflakeSql);
            System.out.println("Original Snowflake SQL:");
            System.out.println(snowflakeSql);
            System.out.println("\nTranslated Trino SQL:");
            System.out.println(trinoSql);
        } catch (SqlTranslationException e) {
            System.err.println("Translation failed: " + e.getMessage());
            System.exit(1);
        }
    }
}