package de.angelasensio.sftrino;

/**
 * Command-line entry point for the Snowflake-to-Trino SQL translator.
 *
 * <p>Usage:
 * <pre>
 *   java -jar snowflake-trino-translator.jar "SELECT DATEADD(day, 7, created_at) FROM orders"
 * </pre>
 */
public class SnowflakeTrinoTranslatorCli {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -jar snowflake-trino-translator.jar \"<SQL_QUERY>\"");
            System.exit(1);
        }

        String snowflakeSql = args[0];
        SnowflakeTrinoTranslator translator = new SnowflakeTrinoTranslator();

        try {
            TranslationResult result = translator.translateWithDiagnostics(snowflakeSql);

            System.out.println("Original Snowflake SQL:");
            System.out.println(snowflakeSql);
            System.out.println("\nTranslated Trino SQL:");
            System.out.println(result.sql());

            if (result.hasWarnings()) {
                System.out.println("\nWarnings:");
                result.warnings().forEach(w ->
                        System.out.printf("  [%s] %s: %s%n", w.type(), w.functionName(), w.message()));
            }
        } catch (SqlTranslationException e) {
            System.err.println("Translation failed: " + e.getMessage());
            System.exit(1);
        }
    }
}
