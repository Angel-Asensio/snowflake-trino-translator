# Snowflake → Trino Function Mappings

## Date / Time Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `DATEADD(part, n, date)` | `date_add('part', n, date)` | Date part converted to string literal |
| `DATEDIFF(part, start, end)` | `date_diff('part', start, end)` | Date part converted to string literal |
| `TO_DATE(str)` | `CAST(str AS DATE)` | Single-arg form |
| `TO_DATE(str, fmt)` | `CAST(date_parse(str, fmt) AS DATE)` | Format string converted: `YYYY→%Y`, `YY→%y`, `MM→%m`, `DD→%d`, `HH24→%H`, `HH12→%I`, `MI→%i`, `SS→%s`; wrapped in CAST because `date_parse` returns TIMESTAMP |
| `TO_TIMESTAMP(str)` | `CAST(str AS TIMESTAMP)` | Single-arg only; extra args ignored |
| `SYSDATE()` | `now()` | |
| `GETDATE()` | `now()` | |
| `CURRENT_DATE()` | `CURRENT_DATE` | Trailing parens stripped before parsing |
| `CURRENT_TIMESTAMP()` | `CURRENT_TIMESTAMP` | Trailing parens stripped before parsing |
| `LAST_DAY(date)` | `last_day_of_month(date)` | |
| `ADD_MONTHS(date, n)` | `date_add('month', n, date)` | Arguments reordered |
| `DAYOFWEEK(date)` | `day_of_week(date)` | |
| `DAYOFYEAR(date)` | `day_of_year(date)` | |
| `WEEKOFYEAR(date)` | `week_of_year(date)` | |
| `MONTHNAME(date)` | `format_datetime(date, 'MMMM')` | |
| `DAYNAME(date)` | `format_datetime(date, 'EEEE')` | |
| `DATE_FROM_PARTS(year, month, day)` | `date(format('%04d-%02d-%02d', year, month, day))` | |

## Type Conversion Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `TO_TIME(str)` | `CAST(str AS TIME)` | |
| `TO_NUMBER(str)` | `CAST(str AS DECIMAL)` | |
| `TO_NUMBER(str, precision, scale)` | `CAST(str AS DECIMAL)` | Precision/scale dropped with warning |
| `TO_NUMERIC(str)` | `CAST(str AS DECIMAL)` | |
| `TO_DECIMAL(str)` | `CAST(str AS DECIMAL)` | |
| `TO_DOUBLE(str)` | `CAST(str AS DOUBLE)` | |
| `TO_BOOLEAN(str)` | `CAST(str AS BOOLEAN)` | |
| `TO_VARCHAR(expr)` | `CAST(expr AS VARCHAR)` | |
| `TO_VARCHAR(expr, fmt)` | `CAST(expr AS VARCHAR)` | Format argument dropped with warning |
| `TO_CHAR(expr)` | `CAST(expr AS VARCHAR)` | |
| `TO_CHAR(expr, fmt)` | `CAST(expr AS VARCHAR)` | Format argument dropped with warning |
| `TRY_CAST(expr AS type)` | `TRY_CAST(expr AS type)` | Passed through unchanged; Trino supports natively |

## Null / Conditional Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `IFNULL(expr, default)` | `COALESCE(expr, default)` | |
| `NVL(expr, default)` | `COALESCE(expr, default)` | |
| `NVL2(expr, val_if_not_null, val_if_null)` | `IF(expr IS NOT NULL, val_if_not_null, val_if_null)` | |
| `IFF(cond, true_val, false_val)` | `IF(cond, true_val, false_val)` | 3-operand only |
| `ZEROIFNULL(expr)` | `COALESCE(expr, 0)` | |
| `NULLIFZERO(expr)` | `NULLIF(expr, 0)` | |
| `DIV0(num, denom)` | `IF(denom = 0, CAST(0 AS DOUBLE), CAST(num AS DOUBLE) / denom)` | Safe division; returns 0.0 on divide-by-zero; operands cast to DOUBLE to match Snowflake's floating-point return type |
| `DECODE(expr, v1, r1, v2, r2, ..., default)` | `CASE WHEN expr = v1 THEN r1 WHEN expr = v2 THEN r2 ... ELSE default END` | Converts to searched CASE; optional default |

## String Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `LEFT(str, n)` | `substr(str, 1, n)` | |
| `RIGHT(str, n)` | `substr(str, length(str) - n + 1, n)` | Index calculation for rightmost chars |
| `STARTSWITH(str, prefix)` | `starts_with(str, prefix)` | |
| `ENDSWITH(str, suffix)` | `substr(str, length(str) - length(suffix) + 1) = suffix` | SQL-standard expression for compatibility with older Trino versions |
| `CONTAINS(str, substr)` | `strpos(str, substr) > 0` | String version; returns boolean |
| `INSTR(str, substr)` | `strpos(str, substr)` | Extra args beyond 2 ignored |
| `CHARINDEX(substr, str)` | `strpos(str, substr)` | **Arguments reversed**; start position arg dropped |
| `CHARINDEX(substr, str, pos)` | `strpos(str, substr)` | start_pos dropped with warning |
| `REGEXP_SUBSTR(str, pattern)` | `regexp_extract(str, pattern)` | |
| `REGEXP_SUBSTR(str, pattern, pos, occ, params)` | `regexp_extract(str, pattern)` | pos/occurrence/params dropped with warning |
| `STRTOK(str, delim, part)` | `split_part(str, delim, part)` | All 3 operands required |
| `EDITDISTANCE(s1, s2)` | `levenshtein_distance(s1, s2)` | |
| `SPACE(n)` | `rpad('', n, ' ')` | `repeat(' ', n)` resolves to the array overload in Trino for char literals |

## Cryptographic / Encoding Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `MD5(str)` | `lower(to_hex(md5(to_utf8(str))))` | Nested UTF-8 encoding + hex + lower |
| `SHA1(str)` | `lower(to_hex(sha1(to_utf8(str))))` | |
| `SHA2(str)` | `lower(to_hex(sha256(to_utf8(str))))` | Default is 256-bit |
| `SHA2(str, 512)` | `lower(to_hex(sha512(to_utf8(str))))` | Only 256 and 512 bits supported |
| `HEX_ENCODE(str)` | `lower(to_hex(to_utf8(str)))` | |
| `HEX_DECODE_STRING(hex)` | `from_utf8(from_hex(hex))` | |
| `BASE64_ENCODE(str)` | `to_base64(to_utf8(str))` | |
| `BASE64_DECODE_STRING(encoded)` | `from_utf8(from_base64(encoded))` | |
| `URL_ENCODE(str)` | `url_encode(str)` | Direct name mapping |
| `URL_DECODE(str)` | `url_decode(str)` | Direct name mapping |

## Array / JSON Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `ARRAY_SIZE(arr)` | `cardinality(arr)` | |
| `ARRAY_CONTAINS(value, arr)` | `contains(arr, value)` | **Arguments reversed** |
| `ARRAY_CONCAT(arr1, arr2)` | `concat(arr1, arr2)` | Variadic |
| `ARRAY_CAT(arr1, arr2)` | `concat(arr1, arr2)` | Alias for ARRAY_CONCAT |
| `ARRAY_TO_STRING(arr, delim)` | `array_join(arr, delim)` | |
| `ARRAY_SLICE(arr, from, to)` | `slice(arr, from + 1, to - from)` | 0-indexed `[from, to)` → 1-indexed `(start, length)` |
| `ARRAY_FLATTEN(arr)` | `flatten(arr)` | |
| `FLATTEN(arr)` | *(passed through)* | Trino FLATTEN has different semantics; manual rewrite to `CROSS JOIN UNNEST` recommended |
| `PARSE_JSON(str)` | `json_parse(str)` | |
| `LISTAGG(col, delim)` | `array_join(array_agg(col), delim)` | |
| `LISTAGG(col)` | `array_join(array_agg(col), ',')` | Default delimiter `,` |

## Aggregate Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `APPROX_COUNT_DISTINCT(col)` | `approx_distinct(col)` | |
| `MEDIAN(col)` | `approx_percentile(col, 0.5)` | Maps to 50th percentile |
| `BOOLAND_AGG(col)` | `bool_and(col)` | |
| `BOOLOR_AGG(col)` | `bool_or(col)` | |
| `BOOLXOR_AGG(col)` | `(count_if(col) % 2) = 1` | Returns true if odd number of true inputs |
| `OBJECT_AGG(key, val)` | `map_agg(key, val)` | |
| `ANY_VALUE(col)` | `arbitrary(col)` | |
| `BITAND_AGG(col)` | `bitwise_and_agg(col)` | |
| `BITOR_AGG(col)` | `bitwise_or_agg(col)` | |
| `BITXOR_AGG(col)` | `bitwise_xor_agg(col)` | |
| `SKEW(col)` | `CASE WHEN count(col) <= 2 THEN NULL ELSE skewness(col) * sqrt(count(col) * (count(col) - 1)) / (count(col) - 2) END` | Applies adjusted Fisher-Pearson bias correction to match Snowflake's G1 skewness; returns NULL for n ≤ 2 |

## Math Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `SQUARE(n)` | `n * n` | Multiplication preserves integer type; `power(n, 2)` returns DOUBLE in Trino |
| `TRUNC(x)` | `truncate(x)` | |
| `TRUNC(x, scale)` | `truncate(x, scale)` | |
| `CEIL(x)` | `ceil(x)` | |
| `CEIL(x, scale)` | `ceil(x)` | Scale argument silently dropped |
| `FLOOR(x)` | `floor(x)` | |
| `FLOOR(x, scale)` | `floor(x)` | Scale argument silently dropped |

## Bitwise Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `BITAND(a, b)` | `bitwise_and(a, b)` | |
| `BITOR(a, b)` | `bitwise_or(a, b)` | |
| `BITXOR(a, b)` | `bitwise_xor(a, b)` | |
| `BITNOT(a)` | `bitwise_not(a)` | |
| `BITSHIFTLEFT(val, n)` | `bitwise_left_shift(val, n)` | |
| `BITSHIFTRIGHT(val, n)` | `bitwise_right_shift(val, n)` | |

## Window Functions

| Snowflake | Trino | Notes |
|-----------|-------|-------|
| `RATIO_TO_REPORT(col) OVER (...)` | `col / SUM(col) OVER (...)` | Converts to division by window SUM; OVER clause preserved |
