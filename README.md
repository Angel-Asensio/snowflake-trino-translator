# Snowflake to Trino SQL Translator

Translates Snowflake SQL syntax to Trino-compatible SQL using Apache Calcite for AST-based parsing and transformation. Unlike regex-based approaches, this library parses the full SQL grammar into an abstract syntax tree, transforms Snowflake-specific constructs, and regenerates valid Trino SQL — preserving query structure and semantics.

## Requirements

- Java 23+
- Maven 3.8+

## Build

```bash
# Compile and run tests
mvn clean package

# Skip tests (faster)
mvn clean package -DskipTests
```

Produces a shaded JAR at `target/snowflake-trino-translator-1.0-SNAPSHOT.jar` with all dependencies bundled.

---

## Quick Start

### Command Line

```bash
java -jar target/snowflake-trino-translator-1.0-SNAPSHOT.jar \
  "SELECT DATEADD(day, 7, created_at) FROM orders"
```

Output:

```
Original Snowflake SQL:
SELECT DATEADD(day, 7, created_at) FROM orders

Translated Trino SQL:
SELECT "date_add"('day', 7, "created_at")
FROM "orders"
```

### Java API

```java
SnowflakeTrinoTranslator translator = new SnowflakeTrinoTranslator();

try {
    String trinoSql = translator.translate(snowflakeSql);
    System.out.println(trinoSql);
} catch (SqlTranslationException e) {
    System.err.println("Translation failed: " + e.getMessage());
}
```

`translate()` throws `SqlTranslationException` (checked) on parse or transformation errors.

---

## Architecture

```
Input Snowflake SQL
        │
        ▼
┌──────────────────-───────┐
│  SnowflakeTrinoTranslator│  ← Entry point / orchestrator
│  (Calcite Babel parser)  │    Parses SQL into a SqlNode AST
└──────────┬────────-──────┘
           │  SqlNode AST
           ▼
┌─────────────────────────-┐
│ SnowflakeToTrinoConverter│  ← Visitor (extends SqlShuttle)
│  switch on function name │    Transforms Snowflake-specific
│  per-function converters │    constructs node-by-node
└──────────┬──────────────-┘
           │  Transformed SqlNode AST
           ▼
┌────────────────────-─────--┐
│     TrinoSqlDialect        │  ← SQL generator
│  (extends PrestoSqlDialect)│ Controls identifier quoting,
│                            │   NULL collation, output format
└──────────┬──────────────---┘
           │
           ▼
    Trino SQL string
```

### Components

**`SnowflakeTrinoTranslator`** — The public API. Configures the Calcite Babel parser (MySQL lexer mode, case-insensitive), invokes `SnowflakeToTrinoConverter`, then calls `SqlNode.toSqlString(trinoDialect)` to produce the output.

**`SnowflakeToTrinoConverter`** — Extends `SqlShuttle` (Calcite's visitor pattern). Overrides `visit(SqlCall)` and dispatches on the uppercased function name. Each Snowflake function has a dedicated private converter method. Window expressions (`OVER`) are intercepted before the switch to inspect the inner aggregate. Unknown functions fall through to `super.visit()`, which recursively visits operands.

**`TrinoSqlDialect`** — Extends `PrestoSqlDialect` (Trino is a Presto fork). Configures double-quoted identifiers, native `NULLS FIRST/LAST` support, and suppresses charset declarations not understood by Trino.

**`SqlTranslationException`** — Checked exception wrapping `SqlParseException` and transformation errors.

---

## Supported Translations

### Date / Time

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `DATEADD(unit, n, date)` | `date_add('unit', n, date)` | Unit identifier → string literal |
| `DATEDIFF(unit, start, end)` | `date_diff('unit', start, end)` | Unit identifier → string literal |
| `TO_DATE(str)` | `CAST(str AS DATE)` | |
| `TO_DATE(str, format)` | `CAST(date_parse(str, format) AS DATE)` | Format string converted (see below); wrapped in CAST to return DATE not TIMESTAMP |
| `TO_TIMESTAMP(str)` | `CAST(str AS TIMESTAMP)` | |
| `SYSDATE()` / `GETDATE()` | `now()` | |
| `CURRENT_DATE()` | `CURRENT_DATE` | Trailing parens stripped before parsing |
| `CURRENT_TIMESTAMP()` | `CURRENT_TIMESTAMP` | Trailing parens stripped before parsing |
| `LAST_DAY(date)` | `last_day_of_month(date)` | |
| `DAYOFWEEK(date)` | `day_of_week(date)` | |
| `DAYOFYEAR(date)` | `day_of_year(date)` | |
| `WEEKOFYEAR(date)` | `week_of_year(date)` | |
| `ADD_MONTHS(date, n)` | `date_add('month', n, date)` | Argument order changes |
| `MONTHNAME(date)` | `format_datetime(date, 'MMMM')` | |
| `DAYNAME(date)` | `format_datetime(date, 'EEEE')` | |
| `DATE_FROM_PARTS(year, month, day)` | `date(format('%04d-%02d-%02d', year, month, day))` | |

**Date format specifiers** (`TO_DATE` with a format string):

| Snowflake token | Trino token |
|---|---|
| `YYYY` | `%Y` |
| `YY` | `%y` |
| `MM` | `%m` |
| `DD` | `%d` |
| `HH24` | `%H` |
| `HH12` | `%I` |
| `MI` | `%i` |
| `SS` | `%s` |

### Type Conversion

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `TO_NUMBER(x)` / `TO_NUMERIC(x)` / `TO_DECIMAL(x)` | `CAST(x AS DECIMAL)` | Precision/scale args dropped with warning |
| `TO_DOUBLE(x)` | `CAST(x AS DOUBLE)` | |
| `TO_BOOLEAN(x)` | `CAST(x AS BOOLEAN)` | |
| `TO_TIME(x)` | `CAST(x AS TIME)` | |
| `TO_VARCHAR(x)` / `TO_CHAR(x)` | `CAST(x AS VARCHAR)` | Format arg (2-arg form) dropped with warning |

### Conditional / Null Handling

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `IFF(cond, t, f)` | `IF(cond, t, f)` | |
| `IFNULL(x, default)` | `COALESCE(x, default)` | |
| `NVL(x, default)` | `COALESCE(x, default)` | |
| `NVL2(expr, not_null, null_val)` | `IF(expr IS NOT NULL, not_null, null_val)` | |
| `ZEROIFNULL(x)` | `COALESCE(x, 0)` | |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` | |
| `DIV0(x, y)` | `IF(y = 0, CAST(0 AS DOUBLE), CAST(x AS DOUBLE) / y)` | Returns 0.0 on divide-by-zero; both operands cast to DOUBLE to match Snowflake's floating-point return type |
| `DECODE(expr, s1,r1, s2,r2, …, default)` | `CASE WHEN expr=s1 THEN r1 … ELSE default END` | Arbitrary number of search/result pairs |

### String

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `LEFT(str, n)` | `substr(str, 1, n)` | |
| `RIGHT(str, n)` | `substr(str, length(str)-n+1, n)` | |
| `STARTSWITH(str, prefix)` | `starts_with(str, prefix)` | |
| `ENDSWITH(str, suffix)` | `substr(str, length(str)-length(suffix)+1) = suffix` | SQL-standard expression for compatibility with older Trino versions |
| `CONTAINS(str, sub)` | `strpos(str, sub) > 0` | String containment; distinct from `ARRAY_CONTAINS` |
| `INSTR(str, sub)` | `strpos(str, sub)` | |
| `CHARINDEX(sub, str)` | `strpos(str, sub)` | Argument order reversed |
| `REGEXP_SUBSTR(str, pat)` | `regexp_extract(str, pat)` | Extra args dropped with warning |
| `STRTOK(str, delim, n)` | `split_part(str, delim, n)` | |
| `EDITDISTANCE(s1, s2)` | `levenshtein_distance(s1, s2)` | |
| `SPACE(n)` | `rpad('', n, ' ')` | `repeat(' ', n)` resolves to the array overload in Trino for char literals |

### Hash / Encoding

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `MD5(str)` | `lower(to_hex(md5(to_utf8(str))))` | Trino `md5` takes `varbinary`; wrapping handles the type conversion |
| `SHA1(str)` | `lower(to_hex(sha1(to_utf8(str))))` | Same pattern |
| `SHA2(str)` | `lower(to_hex(sha256(to_utf8(str))))` | Defaults to 256-bit |
| `SHA2(str, 512)` | `lower(to_hex(sha512(to_utf8(str))))` | 512-bit variant |
| `HEX_ENCODE(str)` | `lower(to_hex(to_utf8(str)))` | |
| `HEX_DECODE_STRING(str)` | `from_utf8(from_hex(str))` | |
| `BASE64_ENCODE(str)` | `to_base64(to_utf8(str))` | |
| `BASE64_DECODE_STRING(str)` | `from_utf8(from_base64(str))` | |

### URL

| Snowflake | Trino |
|-----------|-------|
| `URL_ENCODE(str)` | `url_encode(str)` |
| `URL_DECODE(str)` | `url_decode(str)` |

### Math / Bitwise (Scalar)

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `SQUARE(x)` | `x * x` | Multiplication preserves integer type; `power(x, 2)` returns DOUBLE in Trino |
| `TRUNC(x)` | `truncate(x)` | |
| `TRUNC(x, scale)` | `truncate(x, scale)` | |
| `CEIL(x)` | `ceil(x)` | |
| `CEIL(x, scale)` | `ceil(x)` | Scale argument silently dropped |
| `FLOOR(x)` | `floor(x)` | |
| `FLOOR(x, scale)` | `floor(x)` | Scale argument silently dropped |
| `BITAND(x, y)` | `bitwise_and(x, y)` | |
| `BITOR(x, y)` | `bitwise_or(x, y)` | |
| `BITXOR(x, y)` | `bitwise_xor(x, y)` | |
| `BITNOT(x)` | `bitwise_not(x)` | |
| `BITSHIFTLEFT(x, n)` | `bitwise_left_shift(x, n)` | |
| `BITSHIFTRIGHT(x, n)` | `bitwise_right_shift(x, n)` | |

### Array / JSON

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `ARRAY_SIZE(arr)` | `cardinality(arr)` | |
| `ARRAY_CONTAINS(val, arr)` | `contains(arr, val)` | Argument order reversed |
| `ARRAY_CONCAT(a, b)` / `ARRAY_CAT(a, b)` | `concat(a, b)` | |
| `ARRAY_TO_STRING(arr, delim)` | `array_join(arr, delim)` | |
| `ARRAY_SLICE(arr, from, to)` | `slice(arr, from+1, to-from)` | 0-indexed `[from, to)` → 1-indexed `(start, length)` |
| `ARRAY_FLATTEN(arr)` | `flatten(arr)` | |
| `PARSE_JSON(str)` | `json_parse(str)` | |

### Aggregate

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `LISTAGG(col, delim)` | `array_join(array_agg(col), delim)` | |
| `BOOLAND_AGG(x)` | `bool_and(x)` | |
| `BOOLOR_AGG(x)` | `bool_or(x)` | |
| `BOOLXOR_AGG(x)` | `(count_if(x) % 2) = 1` | True if an odd number of inputs are true |
| `APPROX_COUNT_DISTINCT(x)` | `approx_distinct(x)` | |
| `MEDIAN(x)` | `approx_percentile(x, 0.5)` | Trino has no exact median; uses t-digest approximation |
| `OBJECT_AGG(key, val)` | `map_agg(key, val)` | |
| `ANY_VALUE(x)` | `arbitrary(x)` | |
| `BITAND_AGG(x)` | `bitwise_and_agg(x)` | |
| `BITOR_AGG(x)` | `bitwise_or_agg(x)` | |
| `BITXOR_AGG(x)` | `bitwise_xor_agg(x)` | |
| `SKEW(x)` | `CASE WHEN count(x) <= 2 THEN NULL ELSE skewness(x) * sqrt(count(x) * (count(x)-1)) / (count(x)-2) END` | Applies the adjusted Fisher-Pearson bias correction to match Snowflake's G1 skewness; returns NULL for n ≤ 2 |

### Window

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `RATIO_TO_REPORT(x) OVER (w)` | `x / SUM(x) OVER (w)` | The same window clause is reused for `SUM` |

Standard SQL window functions (`RANK`, `DENSE_RANK`, `ROW_NUMBER`, `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `CUME_DIST`, `PERCENT_RANK`) and aggregate window functions (`AVG`, `SUM`, `MIN`, `MAX`, `COUNT` with `OVER`) pass through unchanged.

### Pass-through (no translation needed)

These functions have identical syntax in both dialects and pass through unchanged:

`UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `LENGTH`, `CONCAT`, `REPLACE`, `LPAD`, `RPAD`, `SUBSTR` / `SUBSTRING`, `REGEXP_REPLACE`, `REGEXP_LIKE`, `SPLIT_PART`, `INITCAP`, `REPEAT`, `REVERSE`, `DATE_TRUNC`, `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `WEEK`, `QUARTER`, `ABS`, `ROUND`, `MOD`, `SIGN`, `LN`, `EXP`, `POWER`, `LOG`, `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`, `COUNT_IF`, `KURTOSIS`, `ARRAY_AGG`, `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTILE`, `CORR`, `COVAR_POP`, `COVAR_SAMP`, `STDDEV`, `STDDEV_SAMP`, `STDDEV_POP`, `VAR_POP`, `VAR_SAMP`, `VARIANCE`, `REGR_SLOPE`, `REGR_INTERCEPT`, `REGR_R2`, `PERCENTILE_CONT`, `PERCENTILE_DISC`, `TRY_CAST`.

---

## Examples

### Null handling and conditional logic

```sql
-- Snowflake
SELECT
  IFNULL(email, 'N/A')            AS email,
  NVL(phone, 'Unknown')           AS phone,
  NVL2(score, score * 1.1, 0)     AS adjusted_score,
  ZEROIFNULL(revenue)             AS revenue,
  IFF(status = 'active', 1, 0)   AS is_active,
  DECODE(tier, 'A', 'Gold', 'B', 'Silver', 'Bronze') AS tier_label
FROM customers;

-- Trino
SELECT
  COALESCE("email", 'N/A')                                        AS "email",
  COALESCE("phone", 'Unknown')                                    AS "phone",
  IF("score" IS NOT NULL, "score" * 1.1, 0)                      AS "adjusted_score",
  COALESCE("revenue", 0)                                          AS "revenue",
  IF("status" = 'active', 1, 0)                                   AS "is_active",
  CASE WHEN "tier" = 'A' THEN 'Gold'
       WHEN "tier" = 'B' THEN 'Silver'
       ELSE 'Bronze' END                                           AS "tier_label"
FROM "customers";
```

### Date arithmetic

```sql
-- Snowflake
SELECT
  DATEADD(month, 1, signup_date)              AS next_billing,
  DATEDIFF(day, signup_date, CURRENT_DATE)    AS days_since_signup,
  ADD_MONTHS(billing_date, 3)                 AS q_ahead,
  TO_DATE(date_str, 'YYYY/MM/DD')             AS parsed_date,
  LAST_DAY(billing_date)                      AS period_end,
  DAYNAME(event_date)                         AS day_label,
  MONTHNAME(event_date)                       AS month_label
FROM orders;

-- Trino
SELECT
  "date_add"('month', 1, "signup_date")                 AS "next_billing",
  "date_diff"('day', "signup_date", CURRENT_DATE)       AS "days_since_signup",
  "date_add"('month', 3, "billing_date")                AS "q_ahead",
  CAST("date_parse"("date_str", '%Y/%m/%d') AS DATE)     AS "parsed_date",
  "last_day_of_month"("billing_date")                   AS "period_end",
  "format_datetime"("event_date", 'EEEE')               AS "day_label",
  "format_datetime"("event_date", 'MMMM')               AS "month_label"
FROM "orders";
```

### Safe division and type casting

```sql
-- Snowflake
SELECT
  DIV0(total_sales, num_transactions) AS avg_sale,
  TO_NUMBER(revenue_str)              AS revenue,
  TO_DOUBLE(ratio_str)                AS ratio,
  TO_BOOLEAN(active_str)             AS is_active
FROM stats;

-- Trino
SELECT
  IF("num_transactions" = 0, CAST(0 AS DOUBLE), CAST("total_sales" AS DOUBLE) / "num_transactions") AS "avg_sale",
  CAST("revenue_str" AS DECIMAL)                                      AS "revenue",
  CAST("ratio_str" AS DOUBLE)                                         AS "ratio",
  CAST("active_str" AS BOOLEAN)                                       AS "is_active"
FROM "stats";
```

### String and hash functions

```sql
-- Snowflake
SELECT
  LEFT(full_name, 1)               AS initial,
  RIGHT(account_code, 4)           AS suffix,
  EDITDISTANCE(name_a, name_b)     AS edit_dist,
  SPACE(4)                         AS padding,
  MD5(password_col)                AS password_hash,
  SHA2(secret, 256)                AS secret_hash,
  HEX_ENCODE(raw_bytes)            AS hex_str,
  HEX_DECODE_STRING(hex_col)       AS decoded
FROM users;

-- Trino
SELECT
  "substr"("full_name", 1, 1)                                     AS "initial",
  "substr"("account_code", "length"("account_code") - 4 + 1, 4)  AS "suffix",
  "levenshtein_distance"("name_a", "name_b")                      AS "edit_dist",
  "rpad"('', 4, ' ')                                               AS "padding",
  "lower"("to_hex"("md5"("to_utf8"("password_col"))))             AS "password_hash",
  "lower"("to_hex"("sha256"("to_utf8"("secret"))))                AS "secret_hash",
  "lower"("to_hex"("to_utf8"("raw_bytes")))                       AS "hex_str",
  "from_utf8"("from_hex"("hex_col"))                              AS "decoded"
FROM "users";
```

### Bitwise operations

```sql
-- Snowflake
SELECT
  BITAND(flags, 7)          AS masked,
  BITOR(flags, 4)           AS with_bit,
  BITXOR(a, b)              AS xored,
  BITNOT(flags)             AS inverted,
  BITSHIFTLEFT(val, 2)      AS shifted_left,
  BITSHIFTRIGHT(val, 2)     AS shifted_right,
  BITAND_AGG(permissions)   AS combined_perms
FROM data
GROUP BY group_id;

-- Trino
SELECT
  "bitwise_and"("flags", 7)         AS "masked",
  "bitwise_or"("flags", 4)          AS "with_bit",
  "bitwise_xor"("a", "b")           AS "xored",
  "bitwise_not"("flags")            AS "inverted",
  "bitwise_left_shift"("val", 2)    AS "shifted_left",
  "bitwise_right_shift"("val", 2)   AS "shifted_right",
  "bitwise_and_agg"("permissions")  AS "combined_perms"
FROM "data"
GROUP BY "group_id";
```

### Array functions

```sql
-- Snowflake
SELECT
  ARRAY_SIZE(tags)                AS tag_count,
  ARRAY_CONTAINS('admin', roles)  AS is_admin,
  ARRAY_SLICE(items, 0, 3)        AS first_three,
  ARRAY_CONCAT(tags, labels)      AS all_tags,
  ARRAY_TO_STRING(tags, ', ')     AS tag_string,
  ARRAY_FLATTEN(nested)           AS flat
FROM articles;

-- Trino
SELECT
  "cardinality"("tags")                  AS "tag_count",
  "contains"("roles", 'admin')           AS "is_admin",
  "slice"("items", 0 + 1, 3 - 0)        AS "first_three",
  "concat"("tags", "labels")             AS "all_tags",
  "array_join"("tags", ', ')             AS "tag_string",
  "flatten"("nested")                    AS "flat"
FROM "articles";
```

### Aggregate functions

```sql
-- Snowflake
SELECT
  department,
  ANY_VALUE(manager_name)          AS a_manager,
  APPROX_COUNT_DISTINCT(user_id)   AS approx_users,
  MEDIAN(salary)                   AS p50_salary,
  KURTOSIS(response_ms)            AS latency_kurtosis,
  BOOLAND_AGG(is_verified)         AS all_verified,
  BOOLOR_AGG(has_error)            AS any_error,
  BOOLXOR_AGG(is_flagged)          AS odd_flags,
  OBJECT_AGG(metric_key, val)      AS metrics
FROM telemetry
GROUP BY department;

-- Trino
SELECT
  "department",
  "arbitrary"("manager_name")                   AS "a_manager",
  "approx_distinct"("user_id")                  AS "approx_users",
  "approx_percentile"("salary", 0.5)            AS "p50_salary",
  "kurtosis"("response_ms")                     AS "latency_kurtosis",
  "bool_and"("is_verified")                     AS "all_verified",
  "bool_or"("has_error")                        AS "any_error",
  ("count_if"("is_flagged") % 2) = 1            AS "odd_flags",
  "map_agg"("metric_key", "val")                AS "metrics"
FROM "telemetry"
GROUP BY "department";
```

### Window functions

```sql
-- Snowflake
SELECT
  region,
  sales,
  RATIO_TO_REPORT(sales) OVER (PARTITION BY region) AS pct_of_region,
  RATIO_TO_REPORT(sales) OVER ()                    AS pct_of_total
FROM sales_data;

-- Trino
SELECT
  "region",
  "sales",
  "sales" / SUM("sales") OVER (PARTITION BY "region") AS "pct_of_region",
  "sales" / SUM("sales") OVER ()                      AS "pct_of_total"
FROM "sales_data";
```

---

## Running Tests

```bash
# All tests
mvn test

# Single test class
mvn test -Dtest=SnowflakeTrinoTranslatorTest

# Single test method
mvn test -Dtest=SnowflakeTrinoTranslatorTest#testDateAdd
```

---

## Known Limitations

- **DML statements** — Only `SELECT` queries are supported. `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and DDL are not translated.
- **`FLATTEN`** — Snowflake's lateral flatten has no direct SQL-level equivalent in Trino. It requires query restructuring to `CROSS JOIN UNNEST(...)`. The translator passes it through unchanged and logs a warning.
- **`TO_DATE` / `TO_TIMESTAMP` with format strings** — Only the 8 format specifiers listed above are converted. Unknown tokens are left as-is.
- **`TO_VARCHAR` / `TO_CHAR` with format strings** — The 2-argument form (e.g., `TO_CHAR(amount, '$999.00')`) is not supported. The format argument is dropped with a warning.
- **`REGEXP_SUBSTR` with position/occurrence/parameters** — Only the first two arguments (`string`, `pattern`) are used. Extra arguments are dropped with a warning.
- **`CHARINDEX` with start position** — The optional third argument is not translated and is dropped with a warning.
- **`SHA2` with non-standard bit lengths** — Only 256 (default) and 512 are supported. Other values fall back to `sha256` with a warning.
- **`CEIL` / `FLOOR` with scale argument** — Snowflake's 2-argument form (e.g., `CEIL(x, 2)`) rounds to a given number of decimal places. Trino's `ceil`/`floor` do not accept a scale; the second argument is silently dropped.
- **`BOOLXOR_AGG`** — Translated as `(count_if(x) % 2) = 1`. This is semantically equivalent but produces a `boolean` expression, not a standalone aggregate call, which may affect how you reference it in outer queries.
- **Semi-structured / variant data** — Snowflake's colon-path syntax (`col:field`), `GET_PATH`, `OBJECT_CONSTRUCT`, and similar variant functions are not handled.
- **Identifier quoting** — All identifiers in the output are double-quoted. This is valid Trino syntax but may look verbose.

---

## Adding New Translations

1. Add a `case "FUNCTION_NAME":` branch in `SnowflakeToTrinoConverter.visit()`.
2. Implement a private `convertFunctionName(SqlCall call)` method.
3. Add a test in `SnowflakeTrinoTranslatorTest`.

### Simple rename (same argument order)

```java
case "MY_FUNCTION":
    return convertMyFunction(call);

// ...

private SqlNode convertMyFunction(SqlCall call) {
    if (call.operandCount() != 2) { return call; }
    return buildFunction("trino_function_name",
            call.operand(0).accept(this),
            call.operand(1).accept(this));
}
```

### Nested calls (e.g. wrapping with type conversion)

```java
private SqlNode convertMyHash(SqlCall call) {
    return buildFunction("lower",
            buildFunction("to_hex",
                    buildFunction("sha256",
                            buildFunction("to_utf8", call.operand(0).accept(this)))));
}
```

### Reordering arguments with arithmetic

```java
private SqlNode convertMySlice(SqlCall call) {
    SqlNode arr  = call.operand(0).accept(this);
    SqlNode from = call.operand(1).accept(this);
    SqlNode to   = call.operand(2).accept(this);
    SqlNode one  = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
    SqlNode start  = SqlStdOperatorTable.PLUS.createCall(SqlParserPos.ZERO, from, one);
    SqlNode length = SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, to, from);
    return buildFunction("slice", arr, start, length);
}
```

### Injecting a string literal

```java
SqlLiteral.createCharString("month", SqlParserPos.ZERO)
```

### Injecting a numeric literal

```java
SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO)
```

Always call `.accept(this)` on every operand you forward — this ensures nested Snowflake functions inside an operand are also translated.

---

## Contributing

Contributions are welcome — bug reports, fixes, and new function translations.

### Reporting a Bug

Open an issue and include:

- The **Snowflake SQL** that was passed as input
- The **output you received** (or the exception message)
- The **output you expected**

A minimal, self-contained example is ideal. If the query is large, try to trim it down to the smallest version that still reproduces the problem.

### Submitting a Fix or New Translation

1. **Fork** the repository and create a branch from `main`.
2. **Write a failing test first** — add a test case in `SnowflakeTrinoTranslatorTest` that captures the expected input/output.
3. **Implement the fix** following the patterns in [Adding New Translations](#adding-new-translations).
4. **Run the full test suite** and confirm all tests pass:
   ```bash
   mvn test
   ```
5. **Open a pull request** against `main`. Include a short description of what the change does and reference any related issue.

### Guidelines

- Keep each PR focused on a single function or bug — smaller PRs are easier to review.
- Always call `.accept(this)` on every operand you forward so nested Snowflake functions are also translated.
- Do not drop arguments silently without logging a warning (see existing converters for the pattern).
- If a translation is approximate or has known edge cases, document them in [Known Limitations](#known-limitations).

---

## License

Copyright 2026 Angel Asensio (angel.asensio@gmail.com)

This project is licensed under the [PolyForm Strict License 1.0.0](LICENSE).

Use for **commercial purposes** and **redistribution or modification** of the software are not permitted. Personal, educational, and noncommercial use is allowed.

This project depends on [Apache Calcite](https://calcite.apache.org/), which is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). All rights and permissions for Apache Calcite remain under its original license. The PolyForm Strict license applies only to the original code in this repository and does not override or restrict the Apache License 2.0 terms for Calcite or any other Apache-licensed dependency.

---

## Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| Apache Calcite Core | 1.41.0 | SQL parsing and AST |
| Apache Calcite Babel | 1.41.0 | Multi-dialect parser (MySQL lexer mode, case-insensitive) |
| SLF4J | 2.0.9 | Logging |
| JUnit Jupiter | 5.10.1 | Testing |
