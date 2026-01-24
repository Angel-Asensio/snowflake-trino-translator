package de.angelasensio.sftrino;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the Snowflake to Trino SQL translator.
 * Each test documents the Snowflake input and the expected Trino output.
 */
public class SnowflakeTrinoTranslatorTest {

    private SnowflakeTrinoTranslator translator;

    @BeforeEach
    public void setUp() {
        translator = new SnowflakeTrinoTranslator();
    }

    // ── Standard SQL (pass-through) ───────────────────────────────────────

    @Test
    public void testSimpleSelect() throws SqlTranslationException {
        // Snowflake: SELECT id, name FROM users WHERE age > 18
        // Trino:     SELECT "id", "name" FROM "users" WHERE "age" > 18
        String snowflakeSql = "SELECT id, name FROM users WHERE age > 18";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("SELECT"));
        assertTrue(trinoSql.toUpperCase().contains("FROM"));
        assertTrue(trinoSql.toUpperCase().contains("WHERE"));
    }

    @Test
    public void testJoinQuery() throws SqlTranslationException {
        // Snowflake: SELECT u.id, u.name, o.order_date FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed'
        // Trino:     SELECT "u"."id", "u"."name", "o"."order_date" FROM "users" AS "u" INNER JOIN "orders" AS "o" ON "u"."id" = "o"."user_id" WHERE "o"."status" = 'completed'
        String snowflakeSql =
            "SELECT u.id, u.name, o.order_date " +
            "FROM users u " +
            "INNER JOIN orders o ON u.id = o.user_id " +
            "WHERE o.status = 'completed'";

        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("JOIN"));
    }

    @Test
    public void testAggregationQuery() throws SqlTranslationException {
        // Snowflake: SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary FROM employees GROUP BY department HAVING COUNT(*) > 5
        // Trino:     SELECT "department", COUNT(*) AS "emp_count", AVG("salary") AS "avg_salary" FROM "employees" GROUP BY "department" HAVING COUNT(*) > 5
        String snowflakeSql =
            "SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary " +
            "FROM employees " +
            "GROUP BY department " +
            "HAVING COUNT(*) > 5";

        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("GROUP BY"));
        assertTrue(trinoSql.toUpperCase().contains("HAVING"));
    }

    // ── Date / Time ───────────────────────────────────────────────────────

    @Test
    public void testDateAdd() throws SqlTranslationException {
        // Snowflake: SELECT DATEADD(day, 5, created_date) FROM orders
        // Trino:     SELECT DATE_ADD('day', 5, "created_date") FROM "orders"
        String snowflakeSql = "SELECT DATEADD(day, 5, created_date) FROM orders";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("date_add"));
    }

    @Test
    public void testDateDiff() throws SqlTranslationException {
        // Snowflake: SELECT DATEDIFF(day, start_date, end_date) FROM projects
        // Trino:     SELECT DATE_DIFF('day', "start_date", "end_date") FROM "projects"
        String snowflakeSql = "SELECT DATEDIFF(day, start_date, end_date) FROM projects";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("date_diff"));
    }

    @Test
    public void testToDate() throws SqlTranslationException {
        // Snowflake: SELECT TO_DATE(date_string) FROM events
        // Trino:     SELECT CAST("date_string" AS DATE) FROM "events"
        String snowflakeSql = "SELECT TO_DATE(date_string) FROM events";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("CAST") ||
                   trinoSql.toLowerCase().contains("date_parse"));
    }

    @Test
    public void testToDateWithFormat() throws SqlTranslationException {
        // Snowflake: SELECT TO_DATE(date_str, 'YYYY-MM-DD HH24:MI:SS') FROM events
        // Trino:     SELECT DATE_PARSE("date_str", '%Y-%m-%d %H:%i:%s') FROM "events"
        String sql = "SELECT TO_DATE(date_str, 'YYYY-MM-DD HH24:MI:SS') FROM events";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("date_parse"));
        assertTrue(result.contains("%Y") || result.contains("%m") || result.contains("%d"));
    }

    @Test
    public void testToDateFormatAllSpecifiers() throws SqlTranslationException {
        // Snowflake: SELECT TO_DATE(s, 'YYYY/YY/MM/DD/HH24/HH12/MI/SS') FROM t
        // Trino:     SELECT DATE_PARSE("s", '%Y/%y/%m/%d/%H/%I/%i/%s') FROM "t"
        String sql = "SELECT TO_DATE(s, 'YYYY/YY/MM/DD/HH24/HH12/MI/SS') FROM t";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("date_parse"));
    }

    @Test
    public void testToTimestamp() throws SqlTranslationException {
        // Snowflake: SELECT TO_TIMESTAMP(ts_str) FROM events
        // Trino:     SELECT CAST("ts_str" AS TIMESTAMP) FROM "events"
        String sql = "SELECT TO_TIMESTAMP(ts_str) FROM events";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("CAST"));
        assertTrue(result.toUpperCase().contains("TIMESTAMP"));
    }

    @Test
    public void testSysdate() throws SqlTranslationException {
        // Snowflake: SELECT SYSDATE() FROM dual
        // Trino:     SELECT NOW() FROM "dual"
        String sql = "SELECT SYSDATE() FROM dual";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("now"));
    }

    @Test
    public void testGetdate() throws SqlTranslationException {
        // Snowflake: SELECT GETDATE() FROM dual
        // Trino:     SELECT NOW() FROM "dual"
        String sql = "SELECT GETDATE() FROM dual";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("now"));
    }

    @Test
    public void testLastDay() throws SqlTranslationException {
        // Snowflake: SELECT LAST_DAY(created_at) FROM orders
        // Trino:     SELECT LAST_DAY_OF_MONTH("created_at") FROM "orders"
        String sql = "SELECT LAST_DAY(created_at) FROM orders";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("last_day_of_month"));
    }

    // ── Type conversion ───────────────────────────────────────────────────

    @Test
    public void testToTime() throws SqlTranslationException {
        // Snowflake: SELECT TO_TIME(time_str) FROM events
        // Trino:     SELECT CAST("time_str" AS TIME) FROM "events"
        String sql = "SELECT TO_TIME(time_str) FROM events";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("CAST"));
        assertTrue(result.toUpperCase().contains("TIME"));
    }

    @Test
    public void testToNumber() throws SqlTranslationException {
        // Snowflake: SELECT TO_NUMBER(amount_str) FROM payments
        // Trino:     SELECT CAST("amount_str" AS DECIMAL) FROM "payments"
        String sql = "SELECT TO_NUMBER(amount_str) FROM payments";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("CAST"));
        assertTrue(result.toUpperCase().contains("DECIMAL"));
    }

    @Test
    public void testToNumberWithPrecisionScale() throws SqlTranslationException {
        // Snowflake: SELECT TO_NUMBER(amount_str, 10, 2) FROM payments
        // Trino:     SELECT CAST("amount_str" AS DECIMAL) FROM "payments"
        //            (precision/scale args dropped with a warning)
        String sql = "SELECT TO_NUMBER(amount_str, 10, 2) FROM payments";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("CAST"));
        assertTrue(result.toUpperCase().contains("DECIMAL"));
    }

    @Test
    public void testToNumeric() throws SqlTranslationException {
        // Snowflake: SELECT TO_NUMERIC(score_str) FROM results
        // Trino:     SELECT CAST("score_str" AS DECIMAL) FROM "results"
        String sql = "SELECT TO_NUMERIC(score_str) FROM results";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("CAST"));
        assertTrue(result.toUpperCase().contains("DECIMAL"));
    }

    @Test
    public void testToDecimal() throws SqlTranslationException {
        // Snowflake: SELECT TO_DECIMAL(price_str) FROM catalog
        // Trino:     SELECT CAST("price_str" AS DECIMAL) FROM "catalog"
        String sql = "SELECT TO_DECIMAL(price_str) FROM catalog";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("CAST"));
        assertTrue(result.toUpperCase().contains("DECIMAL"));
    }

    @Test
    public void testToDouble() throws SqlTranslationException {
        // Snowflake: SELECT TO_DOUBLE(ratio_str) FROM metrics
        // Trino:     SELECT CAST("ratio_str" AS DOUBLE) FROM "metrics"
        String sql = "SELECT TO_DOUBLE(ratio_str) FROM metrics";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("CAST"));
        assertTrue(result.toUpperCase().contains("DOUBLE"));
    }

    @Test
    public void testToBoolean() throws SqlTranslationException {
        // Snowflake: SELECT TO_BOOLEAN(flag_str) FROM settings
        // Trino:     SELECT CAST("flag_str" AS BOOLEAN) FROM "settings"
        String sql = "SELECT TO_BOOLEAN(flag_str) FROM settings";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("CAST"));
        assertTrue(result.toUpperCase().contains("BOOLEAN"));
    }

    @Test
    public void testToVarchar() throws SqlTranslationException {
        // Snowflake: SELECT TO_VARCHAR(amount) FROM payments
        // Trino:     SELECT CAST("amount" AS VARCHAR) FROM "payments"
        String snowflakeSql = "SELECT TO_VARCHAR(amount) FROM payments";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("CAST"));
        assertTrue(trinoSql.toUpperCase().contains("VARCHAR"));
    }

    @Test
    public void testToChar() throws SqlTranslationException {
        // Snowflake: SELECT TO_CHAR(price) FROM products
        // Trino:     SELECT CAST("price" AS VARCHAR) FROM "products"
        String snowflakeSql = "SELECT TO_CHAR(price) FROM products";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("CAST"));
        assertTrue(trinoSql.toUpperCase().contains("VARCHAR"));
    }

    @Test
    public void testToCharWithFormat() throws SqlTranslationException {
        // Snowflake: SELECT TO_CHAR(price, '$999.00') FROM catalog
        // Trino:     SELECT CAST("price" AS VARCHAR) FROM "catalog"
        //            (format argument dropped with a warning)
        String sql = "SELECT TO_CHAR(price, '$999.00') FROM catalog";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("CAST"));
        assertTrue(result.toUpperCase().contains("VARCHAR"));
    }

    // ── Conditional / Null handling ───────────────────────────────────────

    @Test
    public void testIfNull() throws SqlTranslationException {
        // Snowflake: SELECT IFNULL(email, 'N/A') FROM users
        // Trino:     SELECT COALESCE("email", 'N/A') FROM "users"
        String snowflakeSql = "SELECT IFNULL(email, 'N/A') FROM users";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("COALESCE"));
    }

    @Test
    public void testNvl() throws SqlTranslationException {
        // Snowflake: SELECT NVL(phone, 'Unknown') FROM contacts
        // Trino:     SELECT COALESCE("phone", 'Unknown') FROM "contacts"
        String snowflakeSql = "SELECT NVL(phone, 'Unknown') FROM contacts";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("COALESCE"));
    }

    @Test
    public void testNvl2() throws SqlTranslationException {
        // Snowflake: SELECT NVL2(email, email, 'N/A') FROM users
        // Trino:     SELECT IF("email" IS NOT NULL, "email", 'N/A') FROM "users"
        String sql = "SELECT NVL2(email, email, 'N/A') FROM users";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("IF"));
        assertTrue(result.toUpperCase().contains("NOT NULL"));
    }

    @Test
    public void testIff() throws SqlTranslationException {
        // Snowflake: SELECT IFF(status = 'active', 1, 0) FROM accounts
        // Trino:     SELECT IF("status" = 'active', 1, 0) FROM "accounts"
        String snowflakeSql = "SELECT IFF(status = 'active', 1, 0) FROM accounts";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("IF"));
    }

    @Test
    public void testZeroIfNull() throws SqlTranslationException {
        // Snowflake: SELECT ZEROIFNULL(revenue) FROM sales
        // Trino:     SELECT COALESCE("revenue", 0) FROM "sales"
        String snowflakeSql = "SELECT ZEROIFNULL(revenue) FROM sales";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("COALESCE"));
    }

    @Test
    public void testNullIfZero() throws SqlTranslationException {
        // Snowflake: SELECT NULLIFZERO(quantity) FROM inventory
        // Trino:     SELECT NULLIF("quantity", 0) FROM "inventory"
        String snowflakeSql = "SELECT NULLIFZERO(quantity) FROM inventory";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("NULLIF"));
    }

    @Test
    public void testDiv0() throws SqlTranslationException {
        // Snowflake: SELECT DIV0(total, count) FROM stats
        // Trino:     SELECT IF("count" = 0, 0, "total" / "count") FROM "stats"
        String snowflakeSql = "SELECT DIV0(total, count) FROM stats";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("IF") || trinoSql.toUpperCase().contains("CASE"));
    }

    @Test
    public void testDecode() throws SqlTranslationException {
        // Snowflake: SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM accounts
        // Trino:     SELECT CASE WHEN "status" = 'A' THEN 'Active' WHEN "status" = 'I' THEN 'Inactive' ELSE 'Unknown' END FROM "accounts"
        String snowflakeSql = "SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM accounts";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("CASE"));
        assertTrue(trinoSql.toUpperCase().contains("WHEN"));
        assertTrue(trinoSql.toUpperCase().contains("THEN"));
    }

    // ── String functions ──────────────────────────────────────────────────

    @Test
    public void testLeft() throws SqlTranslationException {
        // Snowflake: SELECT LEFT(name, 3) FROM products
        // Trino:     SELECT SUBSTR("name", 1, 3) FROM "products"
        String sql = "SELECT LEFT(name, 3) FROM products";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("substr"));
    }

    @Test
    public void testRight() throws SqlTranslationException {
        // Snowflake: SELECT RIGHT(code, 4) FROM products
        // Trino:     SELECT SUBSTR("code", LENGTH("code") - 4 + 1, 4) FROM "products"
        String sql = "SELECT RIGHT(code, 4) FROM products";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("substr"));
        assertTrue(result.toLowerCase().contains("length"));
    }

    @Test
    public void testStartsWith() throws SqlTranslationException {
        // Snowflake: SELECT STARTSWITH(url, 'https') FROM pages
        // Trino:     SELECT STARTS_WITH("url", 'https') FROM "pages"
        String sql = "SELECT STARTSWITH(url, 'https') FROM pages";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("starts_with"));
    }

    @Test
    public void testEndsWith() throws SqlTranslationException {
        // Snowflake: SELECT ENDSWITH(filename, '.csv') FROM files
        // Trino:     SELECT SUBSTR("filename", LENGTH("filename") - LENGTH('.csv') + 1) = '.csv' FROM "files"
        String sql = "SELECT ENDSWITH(filename, '.csv') FROM files";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("substr"));
    }

    @Test
    public void testContains() throws SqlTranslationException {
        // Snowflake: SELECT CONTAINS(description, 'keyword') FROM articles
        // Trino:     SELECT STRPOS("description", 'keyword') > 0 FROM "articles"
        String sql = "SELECT CONTAINS(description, 'keyword') FROM articles";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("strpos"));
    }

    @Test
    public void testInstr() throws SqlTranslationException {
        // Snowflake: SELECT INSTR(path, '/') FROM files
        // Trino:     SELECT STRPOS("path", '/') FROM "files"
        String sql = "SELECT INSTR(path, '/') FROM files";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("strpos"));
    }

    @Test
    public void testCharIndex() throws SqlTranslationException {
        // Snowflake: SELECT CHARINDEX('@', email) FROM users
        // Trino:     SELECT STRPOS("email", '@') FROM "users"
        //            (argument order reversed: CHARINDEX(substr, str) → STRPOS(str, substr))
        String snowflakeSql = "SELECT CHARINDEX('@', email) FROM users";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("strpos"));
    }

    @Test
    public void testCharIndexWithThreeArgs() throws SqlTranslationException {
        // Snowflake: SELECT CHARINDEX('@', email, 5) FROM users
        // Trino:     SELECT STRPOS("email", '@') FROM "users"
        //            (start_pos argument dropped with a warning)
        String sql = "SELECT CHARINDEX('@', email, 5) FROM users";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("strpos"));
    }

    @Test
    public void testRegexpSubstr() throws SqlTranslationException {
        // Snowflake: SELECT REGEXP_SUBSTR(description, '[0-9]+') FROM products
        // Trino:     SELECT REGEXP_EXTRACT("description", '[0-9]+') FROM "products"
        String snowflakeSql = "SELECT REGEXP_SUBSTR(description, '[0-9]+') FROM products";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("regexp_extract"));
    }

    @Test
    public void testRegexpSubstrWithExtraArgs() throws SqlTranslationException {
        // Snowflake: SELECT REGEXP_SUBSTR(col, '[0-9]+', 1, 1) FROM t
        // Trino:     SELECT REGEXP_EXTRACT("col", '[0-9]+') FROM "t"
        //            (position/occurrence args dropped with a warning)
        String sql = "SELECT REGEXP_SUBSTR(col, '[0-9]+', 1, 1) FROM t";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("regexp_extract"));
    }

    @Test
    public void testStrtok() throws SqlTranslationException {
        // Snowflake: SELECT STRTOK(full_name, ' ', 1) FROM contacts
        // Trino:     SELECT SPLIT_PART("full_name", ' ', 1) FROM "contacts"
        String snowflakeSql = "SELECT STRTOK(full_name, ' ', 1) FROM contacts";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("split_part"));
    }

    @Test
    public void testSquare() throws SqlTranslationException {
        // Snowflake: SELECT SQUARE(side_length) FROM shapes
        // Trino:     SELECT "side_length" * "side_length" FROM "shapes"
        String snowflakeSql = "SELECT SQUARE(side_length) FROM shapes";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.contains("*"));
    }

    // ── Aggregation ───────────────────────────────────────────────────────

    @Test
    public void testListAgg() throws SqlTranslationException {
        // Snowflake: SELECT LISTAGG(name, ',') FROM teams GROUP BY department
        // Trino:     SELECT ARRAY_JOIN(ARRAY_AGG("name"), ',') FROM "teams" GROUP BY "department"
        String snowflakeSql = "SELECT LISTAGG(name, ',') FROM teams GROUP BY department";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("array_agg") ||
                   trinoSql.toLowerCase().contains("array_join"));
    }

    @Test
    public void testListAggWithoutDelimiter() throws SqlTranslationException {
        // Snowflake: SELECT LISTAGG(name) FROM teams GROUP BY dept
        // Trino:     SELECT ARRAY_JOIN(ARRAY_AGG("name"), ',') FROM "teams" GROUP BY "dept"
        //            (default delimiter ',' applied)
        String sql = "SELECT LISTAGG(name) FROM teams GROUP BY dept";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("array_agg") || result.toLowerCase().contains("array_join"));
    }

    @Test
    public void testBoolAndAgg() throws SqlTranslationException {
        // Snowflake: SELECT BOOLAND_AGG(is_active) FROM flags GROUP BY group_id
        // Trino:     SELECT BOOL_AND("is_active") FROM "flags" GROUP BY "group_id"
        String snowflakeSql = "SELECT BOOLAND_AGG(is_active) FROM flags GROUP BY group_id";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("bool_and"));
    }

    @Test
    public void testBoolOrAgg() throws SqlTranslationException {
        // Snowflake: SELECT BOOLOR_AGG(has_error) FROM flags GROUP BY group_id
        // Trino:     SELECT BOOL_OR("has_error") FROM "flags" GROUP BY "group_id"
        String snowflakeSql = "SELECT BOOLOR_AGG(has_error) FROM flags GROUP BY group_id";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("bool_or"));
    }

    @Test
    public void testApproxCountDistinct() throws SqlTranslationException {
        // Snowflake: SELECT APPROX_COUNT_DISTINCT(user_id) FROM events
        // Trino:     SELECT APPROX_DISTINCT("user_id") FROM "events"
        String sql = "SELECT APPROX_COUNT_DISTINCT(user_id) FROM events";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("approx_distinct"));
    }

    @Test
    public void testMedian() throws SqlTranslationException {
        // Snowflake: SELECT MEDIAN(response_time) FROM requests
        // Trino:     SELECT APPROX_PERCENTILE("response_time", 0.5) FROM "requests"
        String sql = "SELECT MEDIAN(response_time) FROM requests";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("approx_percentile"));
        assertTrue(result.contains("0.5"));
    }

    @Test
    public void testObjectAgg() throws SqlTranslationException {
        // Snowflake: SELECT OBJECT_AGG(key_col, val_col) FROM kv_table GROUP BY group_id
        // Trino:     SELECT MAP_AGG("key_col", "val_col") FROM "kv_table" GROUP BY "group_id"
        String sql = "SELECT OBJECT_AGG(key_col, val_col) FROM kv_table GROUP BY group_id";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("map_agg"));
    }

    // ── Array / JSON ──────────────────────────────────────────────────────

    @Test
    public void testArraySize() throws SqlTranslationException {
        // Snowflake: SELECT ARRAY_SIZE(tags) FROM articles
        // Trino:     SELECT CARDINALITY("tags") FROM "articles"
        String snowflakeSql = "SELECT ARRAY_SIZE(tags) FROM articles";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("cardinality"));
    }

    @Test
    public void testArrayContains() throws SqlTranslationException {
        // Snowflake: SELECT ARRAY_CONTAINS('admin', roles) FROM accounts
        // Trino:     SELECT CONTAINS("roles", 'admin') FROM "accounts"
        //            (argument order reversed: ARRAY_CONTAINS(value, arr) → CONTAINS(arr, value))
        String snowflakeSql = "SELECT ARRAY_CONTAINS('admin', roles) FROM accounts";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("contains"));
    }

    @Test
    public void testArrayConcat() throws SqlTranslationException {
        // Snowflake: SELECT ARRAY_CONCAT(tags, labels) FROM articles
        // Trino:     SELECT CONCAT("tags", "labels") FROM "articles"
        String sql = "SELECT ARRAY_CONCAT(tags, labels) FROM articles";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("concat"));
    }

    @Test
    public void testArrayCat() throws SqlTranslationException {
        // Snowflake: SELECT ARRAY_CAT(arr1, arr2) FROM data
        // Trino:     SELECT CONCAT("arr1", "arr2") FROM "data"
        String sql = "SELECT ARRAY_CAT(arr1, arr2) FROM data";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("concat"));
    }

    @Test
    public void testArrayToString() throws SqlTranslationException {
        // Snowflake: SELECT ARRAY_TO_STRING(tags, ',') FROM articles
        // Trino:     SELECT ARRAY_JOIN("tags", ',') FROM "articles"
        String sql = "SELECT ARRAY_TO_STRING(tags, ',') FROM articles";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("array_join"));
    }

    @Test
    public void testParseJson() throws SqlTranslationException {
        // Snowflake: SELECT PARSE_JSON(payload) FROM events
        // Trino:     SELECT JSON_PARSE("payload") FROM "events"
        String snowflakeSql = "SELECT PARSE_JSON(payload) FROM events";
        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("json_parse"));
    }

    @Test
    public void testFlatten() throws SqlTranslationException {
        // Snowflake: SELECT FLATTEN(arr_col) FROM t
        // Trino:     SELECT "FLATTEN"("arr_col") FROM "t"
        //            (passed through with a warning; manual rewrite to CROSS JOIN UNNEST required)
        String sql = "SELECT FLATTEN(arr_col) FROM t";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("FLATTEN"));
    }

    // ── Encoding ──────────────────────────────────────────────────────────

    @Test
    public void testBase64Encode() throws SqlTranslationException {
        // Snowflake: SELECT BASE64_ENCODE(payload) FROM messages
        // Trino:     SELECT TO_BASE64(TO_UTF8("payload")) FROM "messages"
        String sql = "SELECT BASE64_ENCODE(payload) FROM messages";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("to_base64"));
        assertTrue(result.toLowerCase().contains("to_utf8"));
    }

    @Test
    public void testBase64DecodeString() throws SqlTranslationException {
        // Snowflake: SELECT BASE64_DECODE_STRING(encoded) FROM messages
        // Trino:     SELECT FROM_UTF8(FROM_BASE64("encoded")) FROM "messages"
        String sql = "SELECT BASE64_DECODE_STRING(encoded) FROM messages";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("from_base64"));
        assertTrue(result.toLowerCase().contains("from_utf8"));
    }

    // ── Semi-structured (pass-through) ────────────────────────────────────

    @Test
    public void testTryCast() throws SqlTranslationException {
        // Snowflake: SELECT TRY_CAST(col AS DOUBLE) FROM t
        // Trino:     SELECT TRY_CAST("col" AS DOUBLE) FROM "t"
        //            (Trino supports TRY_CAST natively; passed through unchanged)
        String sql = "SELECT TRY_CAST(col AS DOUBLE) FROM t";
        String result = translator.translate(sql);

        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("TRY_CAST") || result.toUpperCase().contains("CAST"));
    }

    // ── Complex multi-function queries ────────────────────────────────────

    @Test
    public void testComplexQuery() throws SqlTranslationException {
        // Snowflake: SELECT customer_id, IFNULL(email, 'N/A') as email,
        //              DATEADD(month, 1, signup_date) as next_month,
        //              IFF(status = 'active', 1, 0) as is_active
        //            FROM customers
        //            WHERE DATEDIFF(day, signup_date, CURRENT_DATE) < 30
        // Trino:     SELECT "customer_id", COALESCE("email", 'N/A') AS "email",
        //              DATE_ADD('month', 1, "signup_date") AS "next_month",
        //              IF("status" = 'active', 1, 0) AS "is_active"
        //            FROM "customers"
        //            WHERE DATE_DIFF('day', "signup_date", CURRENT_DATE) < 30
        String snowflakeSql =
            "SELECT " +
            "  customer_id, " +
            "  IFNULL(email, 'N/A') as email, " +
            "  DATEADD(month, 1, signup_date) as next_month, " +
            "  IFF(status = 'active', 1, 0) as is_active " +
            "FROM customers " +
            "WHERE DATEDIFF(day, signup_date, CURRENT_DATE) < 30";

        String trinoSql = translator.translate(snowflakeSql);

        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("coalesce"));
        assertTrue(trinoSql.toLowerCase().contains("date_add"));
        assertTrue(trinoSql.toLowerCase().contains("date_diff"));
    }

    // ── Error handling ────────────────────────────────────────────────────

    @Test
    public void testNullInput() {
        // Snowflake: null
        // Trino:     SqlTranslationException thrown
        assertThrows(SqlTranslationException.class, () -> {
            translator.translate(null);
        });
    }

    @Test
    public void testEmptyInput() {
        // Snowflake: ""
        // Trino:     SqlTranslationException thrown
        assertThrows(SqlTranslationException.class, () -> {
            translator.translate("");
        });
    }

    @Test
    public void testInvalidSql() {
        // Snowflake: THIS IS NOT VALID SQL
        // Trino:     SqlTranslationException thrown
        assertThrows(SqlTranslationException.class, () -> {
            translator.translate("THIS IS NOT VALID SQL");
        });
    }

    @Test
    public void testSqlTranslationExceptionWithCauseOnly() {
        // SqlTranslationException(Throwable) constructor
        Throwable cause = new RuntimeException("root cause");
        SqlTranslationException ex = new SqlTranslationException(cause);
        assertTrue(ex.getCause() == cause);
    }

    // ── Date/Time (new) ───────────────────────────────────────────────────

    @Test
    public void testDayOfWeek() throws SqlTranslationException {
        // Snowflake: SELECT DAYOFWEEK(event_date) FROM events
        // Trino:     SELECT DAY_OF_WEEK("event_date") FROM "events"
        String result = translator.translate("SELECT DAYOFWEEK(event_date) FROM events");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("day_of_week"));
    }

    @Test
    public void testDayOfYear() throws SqlTranslationException {
        // Snowflake: SELECT DAYOFYEAR(event_date) FROM events
        // Trino:     SELECT DAY_OF_YEAR("event_date") FROM "events"
        String result = translator.translate("SELECT DAYOFYEAR(event_date) FROM events");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("day_of_year"));
    }

    @Test
    public void testWeekOfYear() throws SqlTranslationException {
        // Snowflake: SELECT WEEKOFYEAR(event_date) FROM events
        // Trino:     SELECT WEEK_OF_YEAR("event_date") FROM "events"
        String result = translator.translate("SELECT WEEKOFYEAR(event_date) FROM events");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("week_of_year"));
    }

    @Test
    public void testAddMonths() throws SqlTranslationException {
        // Snowflake: SELECT ADD_MONTHS(start_date, 3) FROM contracts
        // Trino:     SELECT DATE_ADD('month', 3, "start_date") FROM "contracts"
        String result = translator.translate("SELECT ADD_MONTHS(start_date, 3) FROM contracts");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("date_add"));
        assertTrue(result.toLowerCase().contains("month"));
    }

    @Test
    public void testMonthName() throws SqlTranslationException {
        // Snowflake: SELECT MONTHNAME(event_date) FROM events
        // Trino:     SELECT FORMAT_DATETIME("event_date", 'MMMM') FROM "events"
        String result = translator.translate("SELECT MONTHNAME(event_date) FROM events");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("format_datetime"));
        assertTrue(result.contains("MMMM"));
    }

    @Test
    public void testDayName() throws SqlTranslationException {
        // Snowflake: SELECT DAYNAME(event_date) FROM events
        // Trino:     SELECT FORMAT_DATETIME("event_date", 'EEEE') FROM "events"
        String result = translator.translate("SELECT DAYNAME(event_date) FROM events");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("format_datetime"));
        assertTrue(result.contains("EEEE"));
    }

    // ── Bitwise (new) ─────────────────────────────────────────────────────

    @Test
    public void testBitAnd() throws SqlTranslationException {
        // Snowflake: SELECT BITAND(flags, 7) FROM permissions
        // Trino:     SELECT BITWISE_AND("flags", 7) FROM "permissions"
        String result = translator.translate("SELECT BITAND(flags, 7) FROM permissions");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("bitwise_and"));
    }

    @Test
    public void testBitOr() throws SqlTranslationException {
        // Snowflake: SELECT BITOR(flags, 4) FROM permissions
        // Trino:     SELECT BITWISE_OR("flags", 4) FROM "permissions"
        String result = translator.translate("SELECT BITOR(flags, 4) FROM permissions");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("bitwise_or"));
    }

    @Test
    public void testBitXor() throws SqlTranslationException {
        // Snowflake: SELECT BITXOR(a, b) FROM data
        // Trino:     SELECT BITWISE_XOR("a", "b") FROM "data"
        String result = translator.translate("SELECT BITXOR(a, b) FROM data");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("bitwise_xor"));
    }

    @Test
    public void testBitNot() throws SqlTranslationException {
        // Snowflake: SELECT BITNOT(flags) FROM permissions
        // Trino:     SELECT BITWISE_NOT("flags") FROM "permissions"
        String result = translator.translate("SELECT BITNOT(flags) FROM permissions");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("bitwise_not"));
    }

    @Test
    public void testBitShiftLeft() throws SqlTranslationException {
        // Snowflake: SELECT BITSHIFTLEFT(val, 2) FROM data
        // Trino:     SELECT BITWISE_LEFT_SHIFT("val", 2) FROM "data"
        String result = translator.translate("SELECT BITSHIFTLEFT(val, 2) FROM data");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("bitwise_left_shift"));
    }

    @Test
    public void testBitShiftRight() throws SqlTranslationException {
        // Snowflake: SELECT BITSHIFTRIGHT(val, 2) FROM data
        // Trino:     SELECT BITWISE_RIGHT_SHIFT("val", 2) FROM "data"
        String result = translator.translate("SELECT BITSHIFTRIGHT(val, 2) FROM data");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("bitwise_right_shift"));
    }

    // ── String / Hash (new) ───────────────────────────────────────────────

    @Test
    public void testEditDistance() throws SqlTranslationException {
        // Snowflake: SELECT EDITDISTANCE(s1, s2) FROM strings
        // Trino:     SELECT LEVENSHTEIN_DISTANCE("s1", "s2") FROM "strings"
        String result = translator.translate("SELECT EDITDISTANCE(s1, s2) FROM strings");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("levenshtein_distance"));
    }

    @Test
    public void testSpace() throws SqlTranslationException {
        // Snowflake: SELECT SPACE(5) FROM dual
        // Trino:     SELECT RPAD('', 5, ' ') FROM "dual"
        String result = translator.translate("SELECT SPACE(5) FROM dual");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("rpad"));
    }

    @Test
    public void testMd5() throws SqlTranslationException {
        // Snowflake: SELECT MD5(payload) FROM messages
        // Trino:     SELECT LOWER(TO_HEX(MD5(TO_UTF8("payload")))) FROM "messages"
        String result = translator.translate("SELECT MD5(payload) FROM messages");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("md5"));
        assertTrue(result.toLowerCase().contains("to_hex"));
        assertTrue(result.toLowerCase().contains("to_utf8"));
        assertTrue(result.toLowerCase().contains("lower"));
    }

    @Test
    public void testSha1() throws SqlTranslationException {
        // Snowflake: SELECT SHA1(payload) FROM messages
        // Trino:     SELECT LOWER(TO_HEX(SHA1(TO_UTF8("payload")))) FROM "messages"
        String result = translator.translate("SELECT SHA1(payload) FROM messages");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("sha1"));
        assertTrue(result.toLowerCase().contains("to_hex"));
        assertTrue(result.toLowerCase().contains("to_utf8"));
    }

    @Test
    public void testSha2Default() throws SqlTranslationException {
        // Snowflake: SELECT SHA2(payload) FROM messages
        // Trino:     SELECT LOWER(TO_HEX(SHA256(TO_UTF8("payload")))) FROM "messages"
        String result = translator.translate("SELECT SHA2(payload) FROM messages");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("sha256"));
        assertTrue(result.toLowerCase().contains("to_hex"));
    }

    @Test
    public void testSha2With512() throws SqlTranslationException {
        // Snowflake: SELECT SHA2(payload, 512) FROM messages
        // Trino:     SELECT LOWER(TO_HEX(SHA512(TO_UTF8("payload")))) FROM "messages"
        String result = translator.translate("SELECT SHA2(payload, 512) FROM messages");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("sha512"));
        assertTrue(result.toLowerCase().contains("to_hex"));
    }

    @Test
    public void testHexEncode() throws SqlTranslationException {
        // Snowflake: SELECT HEX_ENCODE(data) FROM blobs
        // Trino:     SELECT LOWER(TO_HEX(TO_UTF8("data"))) FROM "blobs"
        String result = translator.translate("SELECT HEX_ENCODE(data) FROM blobs");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("to_hex"));
        assertTrue(result.toLowerCase().contains("to_utf8"));
        assertTrue(result.toLowerCase().contains("lower"));
    }

    @Test
    public void testHexDecodeString() throws SqlTranslationException {
        // Snowflake: SELECT HEX_DECODE_STRING(hex_col) FROM blobs
        // Trino:     SELECT FROM_UTF8(FROM_HEX("hex_col")) FROM "blobs"
        String result = translator.translate("SELECT HEX_DECODE_STRING(hex_col) FROM blobs");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("from_hex"));
        assertTrue(result.toLowerCase().contains("from_utf8"));
    }

    // ── URL (new) ─────────────────────────────────────────────────────────

    @Test
    public void testUrlEncode() throws SqlTranslationException {
        // Snowflake: SELECT URL_ENCODE(path) FROM pages
        // Trino:     SELECT URL_ENCODE("path") FROM "pages"
        String result = translator.translate("SELECT URL_ENCODE(path) FROM pages");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("url_encode"));
    }

    @Test
    public void testUrlDecode() throws SqlTranslationException {
        // Snowflake: SELECT URL_DECODE(encoded_path) FROM pages
        // Trino:     SELECT URL_DECODE("encoded_path") FROM "pages"
        String result = translator.translate("SELECT URL_DECODE(encoded_path) FROM pages");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("url_decode"));
    }

    // ── Window (new) ──────────────────────────────────────────────────────

    @Test
    public void testRatioToReport() throws SqlTranslationException {
        // Snowflake: SELECT RATIO_TO_REPORT(sales) OVER (PARTITION BY region) FROM orders
        // Trino:     SELECT "sales" / (SUM("sales") OVER (PARTITION BY "region")) FROM "orders"
        String result = translator.translate(
                "SELECT RATIO_TO_REPORT(sales) OVER (PARTITION BY region) FROM orders");
        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("SUM"));
        assertTrue(result.toUpperCase().contains("OVER"));
        assertTrue(result.contains("/"));
    }

    @Test
    public void testRatioToReportNoPartition() throws SqlTranslationException {
        // Snowflake: SELECT RATIO_TO_REPORT(amount) OVER () FROM payments
        // Trino:     SELECT "amount" / (SUM("amount") OVER ()) FROM "payments"
        String result = translator.translate(
                "SELECT RATIO_TO_REPORT(amount) OVER () FROM payments");
        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("SUM"));
        assertTrue(result.toUpperCase().contains("OVER"));
        assertTrue(result.contains("/"));
    }

    // ── Aggregate (new) ───────────────────────────────────────────────────

    @Test
    public void testAnyValue() throws SqlTranslationException {
        // Snowflake: SELECT ANY_VALUE(name) FROM users GROUP BY dept
        // Trino:     SELECT ARBITRARY("name") FROM "users" GROUP BY "dept"
        String result = translator.translate("SELECT ANY_VALUE(name) FROM users GROUP BY dept");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("arbitrary"));
    }

    @Test
    public void testBitAndAgg() throws SqlTranslationException {
        // Snowflake: SELECT BITAND_AGG(flags) FROM permissions GROUP BY role
        // Trino:     SELECT BITWISE_AND_AGG("flags") FROM "permissions" GROUP BY "role"
        String result = translator.translate("SELECT BITAND_AGG(flags) FROM permissions GROUP BY role");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("bitwise_and_agg"));
    }

    @Test
    public void testBitOrAgg() throws SqlTranslationException {
        // Snowflake: SELECT BITOR_AGG(flags) FROM permissions GROUP BY role
        // Trino:     SELECT BITWISE_OR_AGG("flags") FROM "permissions" GROUP BY "role"
        String result = translator.translate("SELECT BITOR_AGG(flags) FROM permissions GROUP BY role");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("bitwise_or_agg"));
    }

    @Test
    public void testBitXorAgg() throws SqlTranslationException {
        // Snowflake: SELECT BITXOR_AGG(flags) FROM permissions GROUP BY role
        // Trino:     SELECT BITWISE_XOR_AGG("flags") FROM "permissions" GROUP BY "role"
        String result = translator.translate("SELECT BITXOR_AGG(flags) FROM permissions GROUP BY role");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("bitwise_xor_agg"));
    }

    @Test
    public void testSkew() throws SqlTranslationException {
        // Snowflake: SELECT SKEW(value) FROM metrics
        // Trino:     CASE WHEN COUNT(value) <= 2 THEN NULL ELSE SKEWNESS(value) * SQRT(n*(n-1)) / (n-2) END
        String result = translator.translate("SELECT SKEW(value) FROM metrics");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("skewness"));
    }

    @Test
    public void testBoolXorAgg() throws SqlTranslationException {
        // Snowflake: SELECT BOOLXOR_AGG(is_active) FROM flags GROUP BY group_id
        // Trino:     SELECT MOD(COUNT_IF("is_active"), 2) = 1 FROM "flags" GROUP BY "group_id"
        //            (true if an odd number of inputs are true)
        String result = translator.translate("SELECT BOOLXOR_AGG(is_active) FROM flags GROUP BY group_id");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("count_if"));
    }

    // ── Array (new) ───────────────────────────────────────────────────────

    @Test
    public void testArraySlice() throws SqlTranslationException {
        // Snowflake: SELECT ARRAY_SLICE(tags, 0, 3) FROM articles
        // Trino:     SELECT SLICE("tags", 0 + 1, 3 - 0) FROM "articles"
        //            (0-indexed [from,to) → 1-indexed (start,length))
        String result = translator.translate("SELECT ARRAY_SLICE(tags, 0, 3) FROM articles");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("slice"));
    }

    @Test
    public void testArrayFlatten() throws SqlTranslationException {
        // Snowflake: SELECT ARRAY_FLATTEN(nested_arr) FROM data
        // Trino:     SELECT FLATTEN("nested_arr") FROM "data"
        String result = translator.translate("SELECT ARRAY_FLATTEN(nested_arr) FROM data");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("flatten"));
    }

    // ── Numeric / Date constructors (new) ─────────────────────────────────

    @Test
    public void testTrunc() throws SqlTranslationException {
        // Snowflake: SELECT TRUNC(price, 2) FROM orders
        // Trino:     SELECT TRUNCATE("price", 2) FROM "orders"
        String trinoSql = translator.translate("SELECT TRUNC(price, 2) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("truncate"));
    }

    @Test
    public void testTruncNoScale() throws SqlTranslationException {
        // Snowflake: SELECT TRUNC(price) FROM orders
        // Trino:     SELECT TRUNCATE("price") FROM "orders"
        String trinoSql = translator.translate("SELECT TRUNC(price) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("truncate"));
    }

    @Test
    public void testCeilWithScale() throws SqlTranslationException {
        // Snowflake: SELECT CEIL(price, 2) FROM orders  [scale arg dropped in Trino]
        // Trino:     SELECT CEIL("price") FROM "orders"
        String trinoSql = translator.translate("SELECT CEIL(price, 2) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("ceil"));
    }

    @Test
    public void testCeilNoScale() throws SqlTranslationException {
        // Snowflake: SELECT CEIL(price) FROM orders
        // Trino:     SELECT CEIL("price") FROM "orders"
        String trinoSql = translator.translate("SELECT CEIL(price) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("ceil"));
    }

    @Test
    public void testFloorWithScale() throws SqlTranslationException {
        // Snowflake: SELECT FLOOR(price, 2) FROM orders  [scale arg dropped in Trino]
        // Trino:     SELECT FLOOR("price") FROM "orders"
        String trinoSql = translator.translate("SELECT FLOOR(price, 2) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toLowerCase().contains("floor"));
    }

    @Test
    public void testCurrentDateWithParens() throws SqlTranslationException {
        // Snowflake: SELECT CURRENT_DATE() FROM orders
        // Trino:     SELECT CURRENT_DATE FROM "orders"
        String trinoSql = translator.translate("SELECT CURRENT_DATE() FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("CURRENT_DATE"));
    }

    @Test
    public void testCurrentTimestampWithParens() throws SqlTranslationException {
        // Snowflake: SELECT CURRENT_TIMESTAMP() FROM orders
        // Trino:     SELECT CURRENT_TIMESTAMP FROM "orders"
        String trinoSql = translator.translate("SELECT CURRENT_TIMESTAMP() FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("CURRENT_TIMESTAMP"));
    }

    @Test
    public void testDateFromParts() throws SqlTranslationException {
        // Snowflake: SELECT DATE_FROM_PARTS(2024, 1, 15) FROM orders
        // Trino:     SELECT DATE(FORMAT('%04d-%02d-%02d', 2024, 1, 15)) FROM "orders"
        String trinoSql = translator.translate("SELECT DATE_FROM_PARTS(2024, 1, 15) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("FORMAT"));
        assertTrue(trinoSql.toLowerCase().contains("date("));
    }

    // ── Pass-through functions ────────────────────────────────────────────

    @Test
    public void testDateTrunc() throws SqlTranslationException {
        // Snowflake: SELECT DATE_TRUNC('month', created_at) FROM orders
        // Trino:     SELECT DATE_TRUNC('month', "created_at") FROM "orders"
        String trinoSql = translator.translate("SELECT DATE_TRUNC('month', created_at) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("DATE_TRUNC"));
    }

    @Test
    public void testYear() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT YEAR(created_at) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("YEAR"));
    }

    @Test
    public void testMonth() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT MONTH(created_at) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("MONTH"));
    }

    @Test
    public void testDay() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT DAY(created_at) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("DAY"));
    }

    @Test
    public void testHour() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT HOUR(created_at) FROM events");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("HOUR"));
    }

    @Test
    public void testMinute() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT MINUTE(created_at) FROM events");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("MINUTE"));
    }

    @Test
    public void testSecond() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT SECOND(created_at) FROM events");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("SECOND"));
    }

    @Test
    public void testQuarter() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT QUARTER(created_at) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("QUARTER"));
    }

    @Test
    public void testSplit() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT SPLIT(email, '@') FROM users");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("SPLIT"));
    }

    @Test
    public void testSplitPart() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT SPLIT_PART(email, '@', 1) FROM users");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("SPLIT_PART"));
    }

    @Test
    public void testRegexpLike() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT REGEXP_LIKE(name, '^A.*') FROM users");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("REGEXP_LIKE"));
    }

    @Test
    public void testRegexpReplace() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT REGEXP_REPLACE(name, '[0-9]', '') FROM users");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("REGEXP_REPLACE"));
    }

    @Test
    public void testRegexpCount() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT REGEXP_COUNT(name, '[0-9]') FROM users");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("REGEXP_COUNT"));
    }

    @Test
    public void testRepeat() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT REPEAT(name, 3) FROM users");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("REPEAT"));
    }

    @Test
    public void testReverse() throws SqlTranslationException {
        String trinoSql = translator.translate("SELECT REVERSE(name) FROM users");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("REVERSE"));
    }

    @Test
    public void testTruncate() throws SqlTranslationException {
        // Snowflake: SELECT TRUNCATE(price, 2) FROM orders
        // Trino:     SELECT TRUNCATE("price", 2) FROM "orders"
        String trinoSql = translator.translate("SELECT TRUNCATE(price, 2) FROM orders");
        assertNotNull(trinoSql);
        assertTrue(trinoSql.toUpperCase().contains("TRUNCATE"));
    }

    // ── main() entry point ────────────────────────────────────────────────

    @Test
    public void testMainMethodHappyPath() {
        // Snowflake: SELECT id FROM users
        // Trino:     SELECT "id" FROM "users"
        //            (printed to stdout by main())
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured));
        try {
            SnowflakeTrinoTranslator.main(new String[]{"SELECT id FROM users"});
        } finally {
            System.setOut(original);
        }
        String output = captured.toString();
        assertTrue(output.toUpperCase().contains("SELECT"));
    }
}
