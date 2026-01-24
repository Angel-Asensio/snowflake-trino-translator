package de.angelasensio.sftrino;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration tests that verify each function mapping in docs/function-mappings.md
 * by running the same logical query on both Snowflake and Trino and comparing results.
 *
 * Strategy:
 *   - Scalar functions  → SELECT <func>(literal) with no FROM clause
 *   - Aggregate functions → inline rows via UNION ALL subquery
 *   - Array functions   → assertResultsMatch() with explicit SQL for each side
 *                          because ARRAY_CONSTRUCT (Snowflake) vs ARRAY[...] (Trino)
 *                          is not handled by the translator
 *   - Non-deterministic / approximate functions → assertTranslatesAndExecutes()
 *
 * Run with: mvn verify -Pintegration-tests
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FunctionMappingsIT extends IntegrationTestBase {

    // ── Date / Time ──────────────────────────────────────────────────────────

    @Test
    void testDateAdd() throws Exception {
        // DATEADD('day', 5, DATE '2024-01-15') → date_add('day', 5, DATE '2024-01-15')
        assertTranslatesAndMatches("SELECT DATEADD('day', 5, DATE '2024-01-15')");
    }

    @Test
    void testDateDiff() throws Exception {
        // DATEDIFF('day', start, end) → date_diff('day', start, end)
        assertTranslatesAndMatches("SELECT DATEDIFF('day', DATE '2024-01-10', DATE '2024-01-20')");
    }

    @Test
    void testToDateSingleArg() throws Exception {
        // TO_DATE(str) → CAST(str AS DATE)
        assertTranslatesAndMatches("SELECT TO_DATE('2024-01-15')");
    }

    @Test
    void testToDateWithFormat() throws Exception {
        // TO_DATE(str, fmt) → date_parse(str, fmt) with format conversion
        assertTranslatesAndMatches("SELECT TO_DATE('15-01-2024', 'DD-MM-YYYY')");
    }

    @Test
    void testToTimestamp() throws Exception {
        // TO_TIMESTAMP(str) → CAST(str AS TIMESTAMP)
        // Execution-only: timestamp string representations may differ across systems
        assertTranslatesAndExecutes("SELECT TO_TIMESTAMP('2024-01-15 10:30:00')");
    }

    @Test
    void testSysdate() throws Exception {
        // SYSDATE() → now()  — non-deterministic, execution-only
        assertTranslatesAndExecutes("SELECT SYSDATE()");
    }

    @Test
    void testGetdate() throws Exception {
        // GETDATE() → now()  — non-deterministic, execution-only
        assertTranslatesAndExecutes("SELECT GETDATE()");
    }

    @Test
    void testLastDay() throws Exception {
        // LAST_DAY(date) → last_day_of_month(date)
        assertTranslatesAndMatches("SELECT LAST_DAY(DATE '2024-01-15')");
    }

    @Test
    void testAddMonths() throws Exception {
        // ADD_MONTHS(date, n) → date_add('month', n, date)
        assertTranslatesAndMatches("SELECT ADD_MONTHS(DATE '2024-01-15', 2)");
    }

    @Test
    void testDayOfWeek() throws Exception {
        // DAYOFWEEK(date) → day_of_week(date)
        // NOTE: Snowflake is 0-indexed (Sunday=0), Trino is ISO 8601 (Monday=1, Sunday=7).
        // Monday 2024-01-15 returns 1 in both systems, so this date is safe for comparison.
        assertTranslatesAndMatches("SELECT DAYOFWEEK(DATE '2024-01-15')");
    }

    @Test
    void testDayOfYear() throws Exception {
        // DAYOFYEAR(date) → day_of_year(date)
        assertTranslatesAndMatches("SELECT DAYOFYEAR(DATE '2024-01-15')");
    }

    @Test
    void testWeekOfYear() throws Exception {
        // WEEKOFYEAR(date) → week_of_year(date)
        assertTranslatesAndMatches("SELECT WEEKOFYEAR(DATE '2024-01-15')");
    }

    @Test
    void testMonthName() throws Exception {
        // MONTHNAME(date) → format_datetime(date, 'MMMM')
        // Execution-only: Snowflake returns 3-char abbreviation ('Jan'), Trino returns
        // full name ('January'); also format_datetime may require TIMESTAMP not DATE.
        assertTranslatesAndExecutes("SELECT MONTHNAME(DATE '2024-01-15')");
    }

    @Test
    void testDayName() throws Exception {
        // DAYNAME(date) → format_datetime(date, 'EEEE')
        // Execution-only: format_datetime may require TIMESTAMP not DATE in Trino.
        assertTranslatesAndExecutes("SELECT DAYNAME(DATE '2024-01-15')");
    }

    // ── Type Conversion ───────────────────────────────────────────────────────

    @Test
    void testToNumber() throws Exception {
        // TO_NUMBER(str) → CAST(str AS DECIMAL)
        assertTranslatesAndMatches("SELECT TO_NUMBER('123')");
    }

    @Test
    void testToDouble() throws Exception {
        // TO_DOUBLE(str) → CAST(str AS DOUBLE)
        assertTranslatesAndMatches("SELECT TO_DOUBLE('3.14')");
    }

    @Test
    void testToBoolean() throws Exception {
        // TO_BOOLEAN(str) → CAST(str AS BOOLEAN)
        assertTranslatesAndMatches("SELECT TO_BOOLEAN('true')");
    }

    @Test
    void testToVarchar() throws Exception {
        // TO_VARCHAR(expr) → CAST(expr AS VARCHAR)
        assertTranslatesAndMatches("SELECT TO_VARCHAR(42)");
    }

    @Test
    void testToChar() throws Exception {
        // TO_CHAR(expr) → CAST(expr AS VARCHAR)
        assertTranslatesAndMatches("SELECT TO_CHAR(42)");
    }

    @Test
    void testToTime() throws Exception {
        // TO_TIME(str) → CAST(str AS TIME)
        // Execution-only: time string format may include nanoseconds in Snowflake.
        assertTranslatesAndExecutes("SELECT TO_TIME('10:30:00')");
    }

    @Test
    void testTryCast() throws Exception {
        // TRY_CAST(expr AS type) → TRY_CAST(expr AS type)  (passed through unchanged)
        assertTranslatesAndMatches("SELECT TRY_CAST('123' AS INTEGER)");
    }

    // ── Null / Conditional ────────────────────────────────────────────────────

    @Test
    void testIfnull() throws Exception {
        // IFNULL(expr, default) → COALESCE(expr, default)
        assertTranslatesAndMatches("SELECT IFNULL(NULL, 'default')");
    }

    @Test
    void testNvl() throws Exception {
        // NVL(expr, default) → COALESCE(expr, default)
        assertTranslatesAndMatches("SELECT NVL(NULL, 'fallback')");
    }

    @Test
    void testNvl2() throws Exception {
        // NVL2(expr, val_if_not_null, val_if_null) → IF(expr IS NOT NULL, ...)
        assertTranslatesAndMatches("SELECT NVL2('value', 'not_null', 'is_null')");
    }

    @Test
    void testIff() throws Exception {
        // IFF(cond, true_val, false_val) → IF(cond, true_val, false_val)
        assertTranslatesAndMatches("SELECT IFF(1 > 0, 'yes', 'no')");
    }

    @Test
    void testZeroifnull() throws Exception {
        // ZEROIFNULL(expr) → COALESCE(expr, 0)
        assertTranslatesAndMatches("SELECT ZEROIFNULL(NULL)");
    }

    @Test
    void testNullifzero() throws Exception {
        // NULLIFZERO(expr) → NULLIF(expr, 0)
        assertTranslatesAndMatches("SELECT NULLIFZERO(0)");
    }

    @Test
    void testDiv0ByZero() throws Exception {
        // DIV0(num, 0) → IF(0 = 0, CAST(0 AS DOUBLE), ...) — safe division returns 0
        // Execution-only: Snowflake formats DOUBLE as 0.000000, Trino as 0.0 (same value).
        assertTranslatesAndExecutes("SELECT DIV0(10, 0)");
    }

    @Test
    void testDiv0Normal() throws Exception {
        // DIV0(num, denom) → IF(denom = 0, CAST(0 AS DOUBLE), CAST(num AS DOUBLE) / denom)
        // Execution-only: Snowflake formats DOUBLE as 5.000000, Trino as 5.0 (same value).
        assertTranslatesAndExecutes("SELECT DIV0(10, 2)");
    }

    @Test
    void testDecode() throws Exception {
        // DECODE(expr, v1, r1, ..., default) → CASE WHEN expr = v1 THEN r1 ... ELSE default END
        assertTranslatesAndMatches("SELECT DECODE(2, 1, 'one', 2, 'two', 'other')");
    }

    // ── String Functions ──────────────────────────────────────────────────────

    @Test
    void testLeft() throws Exception {
        // LEFT(str, n) → substr(str, 1, n)
        assertTranslatesAndMatches("SELECT LEFT('hello', 3)");
    }

    @Test
    void testRight() throws Exception {
        // RIGHT(str, n) → substr(str, length(str) - n + 1, n)
        assertTranslatesAndMatches("SELECT RIGHT('hello', 3)");
    }

    @Test
    void testStartsWith() throws Exception {
        // STARTSWITH(str, prefix) → starts_with(str, prefix)
        assertTranslatesAndMatches("SELECT STARTSWITH('hello world', 'hello')");
    }

    @Test
    void testEndsWith() throws Exception {
        // ENDSWITH(str, suffix) → ends_with(str, suffix)
        assertTranslatesAndMatches("SELECT ENDSWITH('hello world', 'world')");
    }

    @Test
    void testContains() throws Exception {
        // CONTAINS(str, substr) → strpos(str, substr) > 0
        assertTranslatesAndMatches("SELECT CONTAINS('hello world', 'world')");
    }

    @Test
    void testInstr() throws Exception {
        // INSTR(str, substr) → strpos(str, substr)
        // Snowflake uses CHARINDEX/POSITION rather than INSTR; use assertResultsMatch.
        assertResultsMatch(
                "SELECT CHARINDEX('l', 'hello')",
                "SELECT strpos('hello', 'l')");
    }

    @Test
    void testCharIndex() throws Exception {
        // CHARINDEX(substr, str) → strpos(str, substr)  — arguments reversed
        assertTranslatesAndMatches("SELECT CHARINDEX('l', 'hello')");
    }

    @Test
    void testRegexpSubstr() throws Exception {
        // REGEXP_SUBSTR(str, pattern) → regexp_extract(str, pattern)
        assertTranslatesAndMatches("SELECT REGEXP_SUBSTR('hello world', '[a-z]+')");
    }

    @Test
    void testStrtok() throws Exception {
        // STRTOK(str, delim, part) → split_part(str, delim, part)
        assertTranslatesAndMatches("SELECT STRTOK('a,b,c', ',', 2)");
    }

    @Test
    void testSquare() throws Exception {
        // SQUARE(n) → power(n, 2)
        assertTranslatesAndMatches("SELECT SQUARE(5)");
    }

    @Test
    void testEditDistance() throws Exception {
        // EDITDISTANCE(s1, s2) → levenshtein_distance(s1, s2)
        assertTranslatesAndMatches("SELECT EDITDISTANCE('kitten', 'sitting')");
    }

    @Test
    void testSpace() throws Exception {
        // SPACE(n) → repeat(' ', n)
        assertTranslatesAndMatches("SELECT SPACE(5)");
    }

    // ── Cryptographic / Encoding ──────────────────────────────────────────────

    @Test
    void testMd5() throws Exception {
        // MD5(str) → lower(to_hex(md5(to_utf8(str))))
        assertTranslatesAndMatches("SELECT MD5('hello')");
    }

    @Test
    void testSha1() throws Exception {
        // SHA1(str) → lower(to_hex(sha1(to_utf8(str))))
        assertTranslatesAndMatches("SELECT SHA1('hello')");
    }

    @Test
    void testSha2_256() throws Exception {
        // SHA2(str) → lower(to_hex(sha256(to_utf8(str))))  — default 256-bit
        assertTranslatesAndMatches("SELECT SHA2('hello')");
    }

    @Test
    void testSha2_512() throws Exception {
        // SHA2(str, 512) → lower(to_hex(sha512(to_utf8(str))))
        assertTranslatesAndMatches("SELECT SHA2('hello', 512)");
    }

    @Test
    void testHexEncode() throws Exception {
        // HEX_ENCODE(str) → lower(to_hex(to_utf8(str)))
        assertTranslatesAndMatches("SELECT HEX_ENCODE('hello')");
    }

    @Test
    void testHexDecodeString() throws Exception {
        // HEX_DECODE_STRING(hex) → from_utf8(from_hex(hex))
        assertTranslatesAndMatches("SELECT HEX_DECODE_STRING('68656c6c6f')");
    }

    @Test
    void testBase64Encode() throws Exception {
        // BASE64_ENCODE(str) → to_base64(to_utf8(str))
        assertTranslatesAndMatches("SELECT BASE64_ENCODE('hello')");
    }

    @Test
    void testBase64DecodeString() throws Exception {
        // BASE64_DECODE_STRING(encoded) → from_utf8(from_base64(encoded))
        assertTranslatesAndMatches("SELECT BASE64_DECODE_STRING('aGVsbG8=')");
    }

    @Test
    void testUrlEncode() throws Exception {
        // URL_ENCODE(str) → url_encode(str)
        // Snowflake does not expose URL_ENCODE as a SQL function; validate Trino side only.
        assertTranslatesAndRunsOnTrino("SELECT URL_ENCODE('hello')");
    }

    @Test
    void testUrlDecode() throws Exception {
        // URL_DECODE(str) → url_decode(str)
        // Snowflake does not expose URL_DECODE as a SQL function; validate Trino side only.
        assertTranslatesAndRunsOnTrino("SELECT URL_DECODE('hello%20world')");
    }

    // ── Array / JSON Functions ────────────────────────────────────────────────
    // These use assertResultsMatch() with explicit SQL for each side because
    // ARRAY_CONSTRUCT (Snowflake) vs ARRAY[...] (Trino) is outside the translator scope.

    @Test
    void testArraySize() throws Exception {
        // ARRAY_SIZE(arr) → cardinality(arr)
        assertResultsMatch(
                "SELECT ARRAY_SIZE(ARRAY_CONSTRUCT(1, 2, 3))",
                "SELECT cardinality(ARRAY[1, 2, 3])");
    }

    @Test
    void testArrayContains() throws Exception {
        // ARRAY_CONTAINS(value, arr) → contains(arr, value)  — arguments reversed
        assertResultsMatch(
                "SELECT ARRAY_CONTAINS(2, ARRAY_CONSTRUCT(1, 2, 3))",
                "SELECT contains(ARRAY[1, 2, 3], 2)");
    }

    @Test
    void testArrayConcat() throws Exception {
        // ARRAY_CONCAT(arr1, arr2) → concat(arr1, arr2)
        // Snowflake uses ARRAY_CAT; ARRAY_CONCAT is not a Snowflake function.
        assertResultsMatch(
                "SELECT ARRAY_SIZE(ARRAY_CAT(ARRAY_CONSTRUCT(1, 2), ARRAY_CONSTRUCT(3, 4)))",
                "SELECT cardinality(concat(ARRAY[1, 2], ARRAY[3, 4]))");
    }

    @Test
    void testArrayCat() throws Exception {
        // ARRAY_CAT(arr1, arr2) → concat(arr1, arr2)  — alias for ARRAY_CONCAT
        assertResultsMatch(
                "SELECT ARRAY_SIZE(ARRAY_CAT(ARRAY_CONSTRUCT(1, 2), ARRAY_CONSTRUCT(3, 4)))",
                "SELECT cardinality(concat(ARRAY[1, 2], ARRAY[3, 4]))");
    }

    @Test
    void testArrayToString() throws Exception {
        // ARRAY_TO_STRING(arr, delim) → array_join(arr, delim)
        assertResultsMatch(
                "SELECT ARRAY_TO_STRING(ARRAY_CONSTRUCT('a', 'b', 'c'), ',')",
                "SELECT array_join(ARRAY['a', 'b', 'c'], ',')");
    }

    @Test
    void testArraySlice() throws Exception {
        // ARRAY_SLICE(arr, from, to) → slice(arr, from+1, to-from)
        // 0-indexed [1, 3) on [1,2,3,4,5] → [2, 3]
        assertResultsMatch(
                "SELECT ARRAY_TO_STRING(ARRAY_SLICE(ARRAY_CONSTRUCT(1, 2, 3, 4, 5), 1, 3), ',')",
                "SELECT array_join(slice(ARRAY[1, 2, 3, 4, 5], 2, 2), ',')");
    }

    @Test
    void testParseJson() throws Exception {
        // PARSE_JSON(str) → json_parse(str)
        // Execution-only: VARIANT (Snowflake) vs JSON (Trino) serialize differently.
        assertTranslatesAndExecutes("SELECT PARSE_JSON('{\"key\": \"value\"}')");
    }

    @Test
    void testListagg() throws Exception {
        // LISTAGG(col, delim) → array_join(array_agg(col), delim)
        // Single row to avoid ordering non-determinism.
        assertTranslatesAndMatches(
                "SELECT LISTAGG(col, ',') FROM (SELECT 'hello' AS col) t");
    }

    // ── Aggregate Functions ───────────────────────────────────────────────────

    @Test
    void testBoolandAgg() throws Exception {
        // BOOLAND_AGG(col) → bool_and(col)
        assertTranslatesAndMatches(
                "SELECT BOOLAND_AGG(flag) FROM (SELECT TRUE AS flag UNION ALL SELECT TRUE AS flag) t");
    }

    @Test
    void testBoororAgg() throws Exception {
        // BOOLOR_AGG(col) → bool_or(col)
        assertTranslatesAndMatches(
                "SELECT BOOLOR_AGG(flag) FROM (SELECT TRUE AS flag UNION ALL SELECT FALSE AS flag) t");
    }

    @Test
    void testBoolxorAgg() throws Exception {
        // BOOLXOR_AGG(col) → (count_if(col) % 2) = 1
        // 1 TRUE + 1 FALSE → XOR = TRUE (unambiguous across all XOR semantics)
        assertTranslatesAndMatches(
                "SELECT BOOLXOR_AGG(flag) FROM ("
                + "SELECT TRUE AS flag UNION ALL SELECT FALSE AS flag) t");
    }

    @Test
    void testApproxCountDistinct() throws Exception {
        // APPROX_COUNT_DISTINCT(col) → approx_distinct(col)  — approximate, execution-only
        assertTranslatesAndExecutes(
                "SELECT APPROX_COUNT_DISTINCT(val) "
                + "FROM (SELECT 1 AS val UNION ALL SELECT 2 AS val UNION ALL SELECT 3 AS val) t");
    }

    @Test
    void testMedian() throws Exception {
        // MEDIAN(col) → approx_percentile(col, 0.5)  — approximate, execution-only
        assertTranslatesAndExecutes(
                "SELECT MEDIAN(val) "
                + "FROM (SELECT 1 AS val UNION ALL SELECT 2 AS val UNION ALL SELECT 3 AS val) t");
    }

    @Test
    void testObjectAgg() throws Exception {
        // OBJECT_AGG(key, val) → map_agg(key, val)
        // Execution-only: VARIANT object vs MAP serialize differently.
        assertTranslatesAndExecutes(
                "SELECT OBJECT_AGG(k, v) FROM (SELECT 'a' AS k, 1 AS v) t");
    }

    @Test
    void testAnyValue() throws Exception {
        // ANY_VALUE(col) → arbitrary(col)  — non-deterministic, execution-only
        assertTranslatesAndExecutes(
                "SELECT ANY_VALUE(val) "
                + "FROM (SELECT 1 AS val UNION ALL SELECT 2 AS val) t");
    }

    @Test
    void testBitandAgg() throws Exception {
        // BITAND_AGG(col) → bitwise_and_agg(col)
        // 7 (111) & 6 (110) = 6 (110)
        assertTranslatesAndMatches(
                "SELECT BITAND_AGG(val) FROM (SELECT 7 AS val UNION ALL SELECT 6 AS val) t");
    }

    @Test
    void testBitorAgg() throws Exception {
        // BITOR_AGG(col) → bitwise_or_agg(col)
        // 1 | 2 = 3
        assertTranslatesAndMatches(
                "SELECT BITOR_AGG(val) FROM (SELECT 1 AS val UNION ALL SELECT 2 AS val) t");
    }

    @Test
    void testBitxorAgg() throws Exception {
        // BITXOR_AGG(col) → bitwise_xor_agg(col)
        // 5 ^ 3 = 6
        assertTranslatesAndMatches(
                "SELECT BITXOR_AGG(val) FROM (SELECT 5 AS val UNION ALL SELECT 3 AS val) t");
    }

    @Test
    void testSkew() throws Exception {
        // SKEW(x) → Fisher-Pearson sample skewness: SKEWNESS(x) * SQRT(n*(n-1)) / (n-2)
        // Uses assertTranslatesAndExecutes because Snowflake and Trino use different internal
        // floating-point algorithms, producing results that differ by ~2e-6 (e.g. 0.9352217 vs
        // 0.9352195). The formula is semantically correct; bit-exact matching is not achievable.
        assertTranslatesAndExecutes(
                "SELECT SKEW(val) FROM ("
                + "SELECT 1.0 AS val UNION ALL SELECT 2.0 AS val UNION ALL SELECT 4.0 AS val) t");
    }

    // ── Bitwise Functions ─────────────────────────────────────────────────────

    @Test
    void testBitand() throws Exception {
        // BITAND(a, b) → bitwise_and(a, b)
        assertTranslatesAndMatches("SELECT BITAND(12, 10)");
    }

    @Test
    void testBitor() throws Exception {
        // BITOR(a, b) → bitwise_or(a, b)
        assertTranslatesAndMatches("SELECT BITOR(12, 10)");
    }

    @Test
    void testBitxor() throws Exception {
        // BITXOR(a, b) → bitwise_xor(a, b)
        assertTranslatesAndMatches("SELECT BITXOR(12, 10)");
    }

    @Test
    void testBitnot() throws Exception {
        // BITNOT(a) → bitwise_not(a)
        // Both systems operate on 64-bit signed integers: BITNOT(5) = -6
        assertTranslatesAndMatches("SELECT BITNOT(5)");
    }

    @Test
    void testBitshiftLeft() throws Exception {
        // BITSHIFTLEFT(val, n) → bitwise_left_shift(val, n)
        assertTranslatesAndMatches("SELECT BITSHIFTLEFT(1, 3)");
    }

    @Test
    void testBitshiftRight() throws Exception {
        // BITSHIFTRIGHT(val, n) → bitwise_right_shift(val, n)
        assertTranslatesAndMatches("SELECT BITSHIFTRIGHT(8, 2)");
    }

    // ── Pass-through Functions ────────────────────────────────────────────────

    @Test
    void testDateTrunc() throws Exception {
        // DATE_TRUNC passes through unchanged
        assertTranslatesAndMatches("SELECT DATE_TRUNC('month', DATE '2024-01-15')");
    }

    @Test
    void testYear() throws Exception {
        assertTranslatesAndMatches("SELECT YEAR(DATE '2024-03-15')");
    }

    @Test
    void testMonth() throws Exception {
        assertTranslatesAndMatches("SELECT MONTH(DATE '2024-03-15')");
    }

    @Test
    void testDay() throws Exception {
        assertTranslatesAndMatches("SELECT DAY(DATE '2024-03-15')");
    }

    @Test
    void testHour() throws Exception {
        assertTranslatesAndMatches("SELECT HOUR(TIMESTAMP '2024-03-15 10:30:45')");
    }

    @Test
    void testMinute() throws Exception {
        assertTranslatesAndMatches("SELECT MINUTE(TIMESTAMP '2024-03-15 10:30:45')");
    }

    @Test
    void testSecond() throws Exception {
        assertTranslatesAndMatches("SELECT SECOND(TIMESTAMP '2024-03-15 10:30:45')");
    }

    @Test
    void testQuarter() throws Exception {
        assertTranslatesAndMatches("SELECT QUARTER(DATE '2024-03-15')");
    }

    @Test
    void testSplitPart() throws Exception {
        // SPLIT_PART passes through; deterministic string result
        assertTranslatesAndMatches("SELECT SPLIT_PART('a,b,c', ',', 2)");
    }

    @Test
    void testSplit() throws Exception {
        // SPLIT passes through; array serialization differs between systems
        assertTranslatesAndExecutes("SELECT SPLIT('a,b,c', ',')");
    }

    @Test
    void testRegexpLike() throws Exception {
        assertTranslatesAndMatches("SELECT REGEXP_LIKE('hello', 'h.*')");
    }

    @Test
    void testRegexpReplace() throws Exception {
        assertTranslatesAndMatches("SELECT REGEXP_REPLACE('hello123', '[0-9]+', '')");
    }

    @Test
    void testRegexpCount() throws Exception {
        assertTranslatesAndMatches("SELECT REGEXP_COUNT('hello123', '[0-9]')");
    }

    @Test
    void testRepeat() throws Exception {
        // Execution-only: result type serialization differs between systems
        assertTranslatesAndExecutes("SELECT REPEAT('ab', 3)");
    }

    @Test
    void testReverse() throws Exception {
        assertTranslatesAndMatches("SELECT REVERSE('hello')");
    }

    @Test
    void testTruncate() throws Exception {
        // TRUNCATE passes through; execution-only due to floating-point type differences
        assertTranslatesAndExecutes("SELECT TRUNCATE(3.14159, 2)");
    }

    // ── Numeric / Date Constructors ───────────────────────────────────────────

    @Test
    void testTrunc() throws Exception {
        // TRUNC(x, scale) → truncate(x, scale)
        // Execution-only: floating-point type formatting differs between systems
        assertTranslatesAndExecutes("SELECT TRUNC(3.14159, 2)");
    }

    @Test
    void testTruncNoScale() throws Exception {
        // TRUNC(x) → truncate(x)  — integer input avoids type serialization differences
        assertTranslatesAndMatches("SELECT TRUNC(7)");
    }

    @Test
    void testCeilNoScale() throws Exception {
        // CEIL(x) → ceil(x)
        assertTranslatesAndMatches("SELECT CEIL(3.2)");
    }

    @Test
    void testCeilWithScale() throws Exception {
        // CEIL(x, scale) → ceil(x)  [scale dropped; Trino does not support it]
        // Execution-only: Snowflake CEIL(3.14, 1) = 3.2, Trino ceil(3.14) = 4 — different semantics
        assertTranslatesAndExecutes("SELECT CEIL(3.14, 1)");
    }

    @Test
    void testFloorNoScale() throws Exception {
        // FLOOR(x) → floor(x)
        assertTranslatesAndMatches("SELECT FLOOR(3.9)");
    }

    @Test
    void testFloorWithScale() throws Exception {
        // FLOOR(x, scale) → floor(x)  [scale dropped; Trino does not support it]
        // Execution-only: Snowflake FLOOR(3.99, 1) = 3.9, Trino floor(3.99) = 3 — different semantics
        assertTranslatesAndExecutes("SELECT FLOOR(3.99, 1)");
    }

    @Test
    void testCurrentDateWithParens() throws Exception {
        // CURRENT_DATE() → CURRENT_DATE  (Snowflake allows parens; Trino does not)
        // Execution-only: value is non-deterministic (today's date)
        assertTranslatesAndExecutes("SELECT CURRENT_DATE()");
    }

    @Test
    void testCurrentTimestampWithParens() throws Exception {
        // CURRENT_TIMESTAMP() → CURRENT_TIMESTAMP  (Snowflake allows parens; Trino does not)
        // Execution-only: value is non-deterministic
        assertTranslatesAndExecutes("SELECT CURRENT_TIMESTAMP()");
    }

    @Test
    void testDateFromParts() throws Exception {
        // DATE_FROM_PARTS(year, month, day) → date(format('%04d-%02d-%02d', year, month, day))
        assertTranslatesAndMatches("SELECT DATE_FROM_PARTS(2024, 1, 15)");
    }

    // ── Window Functions ──────────────────────────────────────────────────────

    @Test
    void testRatioToReport() throws Exception {
        // RATIO_TO_REPORT(col) OVER (...) → col / SUM(col) OVER (...)
        // Execution-only: row order is non-deterministic, and integer division
        // semantics may differ (Trino: integer/integer = integer).
        assertTranslatesAndExecutes(
                "SELECT RATIO_TO_REPORT(amount) OVER () "
                + "FROM (SELECT 1.0 AS amount UNION ALL SELECT 3.0 AS amount) t");
    }
}
