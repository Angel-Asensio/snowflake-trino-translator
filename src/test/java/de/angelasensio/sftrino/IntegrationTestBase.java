package de.angelasensio.sftrino;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Base class for integration tests. Loads JDBC connections from
 * src/test/resources/integration-test.properties (gitignored).
 * If the file is absent, all tests are skipped gracefully.
 *
 * Run with: mvn verify -Pintegration-tests
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class IntegrationTestBase {

    private static final Logger log = LoggerFactory.getLogger(IntegrationTestBase.class);

    protected Connection snowflakeConn;
    protected Connection trinoConn;
    protected SnowflakeTrinoTranslator translator;
    private boolean available = false;

    @BeforeAll
    void setUpConnections() throws Exception {
        translator = new SnowflakeTrinoTranslator();

        Properties props = loadProperties();
        if (props == null) {
            return;
        }

        // Load and use the Snowflake driver directly to avoid classloader isolation issues
        // with Maven Failsafe. JDBC_QUERY_RESULT_FORMAT=JSON avoids Arrow classpath conflicts
        // caused by Calcite's shaded Arrow dependency.
        java.sql.Driver sfDriver = (java.sql.Driver) Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")
                .getDeclaredConstructor().newInstance();
        Properties sfProps = new Properties();
        sfProps.setProperty("JDBC_QUERY_RESULT_FORMAT", "JSON");
        snowflakeConn = sfDriver.connect(props.getProperty("snowflake.jdbc.url"), sfProps);

        Properties trinoProps = new Properties();
        trinoProps.setProperty("user", "admin");
        trinoConn = DriverManager.getConnection(props.getProperty("trino.jdbc.url"), trinoProps);

        available = true;
    }

    private Properties loadProperties() {
        try (InputStream is = getClass().getClassLoader()
                .getResourceAsStream("integration-test.properties")) {
            if (is == null) return null;
            Properties p = new Properties();
            p.load(is);
            return p;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load integration-test.properties", e);
        }
    }

    protected void assumeAvailable() {
        assumeTrue(available,
                "integration-test.properties not found — skipping. "
                + "Copy src/test/resources/integration-test.properties.template and fill in credentials.");
    }

    /**
     * Execute a query and return all rows as normalized lowercase strings.
     * NULL values are returned as the string "null".
     */
    protected List<List<String>> query(Connection conn, String sql) throws Exception {
        List<List<String>> rows = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) {
                    String val = rs.getString(i);
                    row.add(val == null ? "null" : val.toLowerCase());
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * Translates snowflakeSql via the translator, runs both on their respective
     * databases, and asserts that the result sets are equal.
     */
    protected void assertTranslatesAndMatches(String snowflakeSql) throws Exception {
        assumeAvailable();
        String trinoSql = translator.translate(snowflakeSql);
        log.info("Snowflake: {}", snowflakeSql);
        log.info("Trino    : {}", trinoSql);
        List<List<String>> sfResult = query(snowflakeConn, snowflakeSql);
        List<List<String>> trinoResult = query(trinoConn, trinoSql);
        assertEquals(sfResult, trinoResult,
                "Results differ.\n  Snowflake SQL : " + snowflakeSql
                + "\n  Trino SQL     : " + trinoSql);
    }

    /**
     * Runs snowflakeSql on Snowflake and trinoSql on Trino and asserts that
     * the result sets are equal. Use this when the input data requires
     * system-specific syntax (e.g. ARRAY_CONSTRUCT vs ARRAY[...]) that the
     * translator cannot handle.
     */
    protected void assertResultsMatch(String snowflakeSql, String trinoSql) throws Exception {
        assumeAvailable();
        log.info("Snowflake: {}", snowflakeSql);
        log.info("Trino    : {}", trinoSql);
        List<List<String>> sfResult = query(snowflakeConn, snowflakeSql);
        List<List<String>> trinoResult = query(trinoConn, trinoSql);
        assertEquals(sfResult, trinoResult,
                "Results differ.\n  Snowflake SQL : " + snowflakeSql
                + "\n  Trino SQL     : " + trinoSql);
    }

    /**
     * Translates snowflakeSql and verifies Trino can execute it.
     * Use when the Snowflake function has a different name (e.g. URL_ENCODE → url_encode)
     * and cannot be run directly against Snowflake.
     */
    protected void assertTranslatesAndRunsOnTrino(String snowflakeSql) throws Exception {
        assumeAvailable();
        String trinoSql = translator.translate(snowflakeSql);
        log.info("Snowflake: {}", snowflakeSql);
        log.info("Trino    : {}", trinoSql);
        assertDoesNotThrow(() -> query(trinoConn, trinoSql),
                "Trino execution failed: " + trinoSql);
    }

    /**
     * Translates snowflakeSql and verifies that both databases execute it
     * without error. Use for non-deterministic (SYSDATE) or approximate
     * (APPROX_COUNT_DISTINCT) functions where exact result comparison is
     * not meaningful.
     */
    protected void assertTranslatesAndExecutes(String snowflakeSql) throws Exception {
        assumeAvailable();
        String trinoSql = translator.translate(snowflakeSql);
        log.info("Snowflake: {}", snowflakeSql);
        log.info("Trino    : {}", trinoSql);
        assertDoesNotThrow(() -> query(snowflakeConn, snowflakeSql),
                "Snowflake execution failed: " + snowflakeSql);
        assertDoesNotThrow(() -> query(trinoConn, trinoSql),
                "Trino execution failed: " + trinoSql);
    }
}
