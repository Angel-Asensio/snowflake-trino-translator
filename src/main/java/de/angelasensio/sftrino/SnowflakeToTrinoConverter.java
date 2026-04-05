package de.angelasensio.sftrino;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Snowflake-specific SQL constructs to Trino-compatible equivalents.
 *
 * <p>Uses the Visitor pattern via {@link SqlShuttle} to traverse and transform the AST.
 * Function converters are stored in a registry ({@link #converters}) keyed by the upper-cased
 * Snowflake function name, which makes it easy to add new translations or override existing ones
 * via {@link #register(String, FunctionConverter)}.
 *
 * <p>Warnings accumulated during a conversion are available via {@link #getWarnings()} after
 * calling {@link #convert(SqlNode)}. Call {@link #clearWarnings()} before each conversion
 * to start with a clean slate.
 *
 * <p><b>Thread safety:</b> this class is <em>not</em> thread-safe. It holds mutable warning
 * state that is reset and read back across the {@code clearWarnings → convert → getWarnings}
 * sequence. Do not share a single instance across threads. {@link SnowflakeTrinoTranslator}
 * enforces the same constraint.
 */
public class SnowflakeToTrinoConverter extends SqlShuttle {

    private static final Logger logger = LoggerFactory.getLogger(SnowflakeToTrinoConverter.class);

    private final Map<String, FunctionConverter> converters = new HashMap<>();
    private List<TranslationWarning> warnings = new ArrayList<>();

    public SnowflakeToTrinoConverter() {
        initConverters();
    }

    // ── Registry ────────────────────────────────────────────────────────────

    /**
     * Registers (or replaces) a converter for the given Snowflake function name.
     * The name is matched case-insensitively at runtime (stored upper-cased here).
     *
     * @throws NullPointerException if either argument is null
     */
    public void register(String snowflakeName, FunctionConverter converter) {
        Objects.requireNonNull(snowflakeName, "snowflakeName must not be null");
        Objects.requireNonNull(converter, "converter must not be null");
        converters.put(snowflakeName.toUpperCase(), converter);
    }

    private void initConverters() {
        // Date / Time
        converters.put("DATEADD",           (c, ctx) -> convertDateAdd(c));
        converters.put("DATEDIFF",          (c, ctx) -> convertDateDiff(c));
        converters.put("TO_DATE",           (c, ctx) -> convertToDate(c));
        converters.put("TO_TIMESTAMP",      (c, ctx) -> convertToTimestamp(c));
        converters.put("SYSDATE",           (c, ctx) -> convertSysdate(c));
        converters.put("GETDATE",           (c, ctx) -> convertSysdate(c));
        converters.put("LAST_DAY",          (c, ctx) -> convertLastDay(c));
        converters.put("DAYOFWEEK",         (c, ctx) -> convertDayOfWeek(c));
        converters.put("DAYOFYEAR",         (c, ctx) -> convertDayOfYear(c));
        converters.put("WEEKOFYEAR",        (c, ctx) -> convertWeekOfYear(c));
        converters.put("ADD_MONTHS",        (c, ctx) -> convertAddMonths(c));
        converters.put("MONTHNAME",         (c, ctx) -> convertMonthName(c));
        converters.put("DAYNAME",           (c, ctx) -> convertDayName(c));
        converters.put("SF_DATE_FROM_PARTS",(c, ctx) -> convertDateFromParts(c));

        // Type conversion
        converters.put("TO_TIME",           (c, ctx) -> convertToTime(c));
        converters.put("TO_NUMBER",         (c, ctx) -> convertToDecimal(c));
        converters.put("TO_NUMERIC",        (c, ctx) -> convertToDecimal(c));
        converters.put("TO_DECIMAL",        (c, ctx) -> convertToDecimal(c));
        converters.put("TO_DOUBLE",         (c, ctx) -> convertToDouble(c));
        converters.put("TO_BOOLEAN",        (c, ctx) -> convertToBoolean(c));
        converters.put("TO_VARCHAR",        (c, ctx) -> convertToVarchar(c));
        converters.put("TO_CHAR",           (c, ctx) -> convertToVarchar(c));
        converters.put("TRY_CAST",          (c, ctx) -> convertTryCast(c));

        // Conditional / null handling
        converters.put("IFF",               (c, ctx) -> convertIff(c));
        converters.put("IFNULL",            (c, ctx) -> convertIfNull(c));
        converters.put("NVL",               (c, ctx) -> convertNvl(c));
        converters.put("NVL2",              (c, ctx) -> convertNvl2(c));
        converters.put("ZEROIFNULL",        (c, ctx) -> convertZeroIfNull(c));
        converters.put("NULLIFZERO",        (c, ctx) -> convertNullIfZero(c));
        converters.put("DIV0",              (c, ctx) -> convertDiv0(c));
        converters.put("DECODE",            (c, ctx) -> convertDecode(c));

        // String
        converters.put("LEFT",              (c, ctx) -> convertLeft(c));
        converters.put("RIGHT",             (c, ctx) -> convertRight(c));
        converters.put("STARTSWITH",        (c, ctx) -> convertStartsWith(c));
        converters.put("ENDSWITH",          (c, ctx) -> convertEndsWith(c));
        converters.put("CONTAINS",          (c, ctx) -> convertContains(c));
        converters.put("INSTR",             (c, ctx) -> convertInstr(c));
        converters.put("REGEXP_SUBSTR",     (c, ctx) -> convertRegexpSubstr(c));
        converters.put("STRTOK",            (c, ctx) -> convertStrtok(c));
        converters.put("EDITDISTANCE",      (c, ctx) -> convertEditDistance(c));
        converters.put("SPACE",             (c, ctx) -> convertSpace(c));
        converters.put("CHARINDEX",         (c, ctx) -> convertCharIndex(c));

        // Hash / Encoding
        converters.put("MD5",               (c, ctx) -> convertMd5(c));
        converters.put("SHA1",              (c, ctx) -> convertSha1(c));
        converters.put("SHA2",              (c, ctx) -> convertSha2(c));
        converters.put("HEX_ENCODE",        (c, ctx) -> convertHexEncode(c));
        converters.put("HEX_DECODE_STRING", (c, ctx) -> convertHexDecodeString(c));
        converters.put("BASE64_ENCODE",     (c, ctx) -> convertBase64Encode(c));
        converters.put("BASE64_DECODE_STRING", (c, ctx) -> convertBase64DecodeString(c));

        // URL
        converters.put("URL_ENCODE",        (c, ctx) -> convertUrlEncode(c));
        converters.put("URL_DECODE",        (c, ctx) -> convertUrlDecode(c));

        // Math / Bitwise
        converters.put("SQUARE",            (c, ctx) -> convertSquare(c));
        converters.put("TRUNC",             (c, ctx) -> convertTrunc(c));
        converters.put("SF_CEIL",           (c, ctx) -> convertCeil(c));
        converters.put("SF_FLOOR",          (c, ctx) -> convertFloor(c));
        converters.put("BITAND",            (c, ctx) -> convertBitAnd(c));
        converters.put("BITOR",             (c, ctx) -> convertBitOr(c));
        converters.put("BITXOR",            (c, ctx) -> convertBitXor(c));
        converters.put("BITNOT",            (c, ctx) -> convertBitNot(c));
        converters.put("BITSHIFTLEFT",      (c, ctx) -> convertBitShiftLeft(c));
        converters.put("BITSHIFTRIGHT",     (c, ctx) -> convertBitShiftRight(c));

        // Array / JSON
        converters.put("ARRAY_SIZE",        (c, ctx) -> convertArraySize(c));
        converters.put("ARRAY_CONTAINS",    (c, ctx) -> convertArrayContains(c));
        converters.put("ARRAY_CONCAT",      (c, ctx) -> convertArrayConcat(c));
        converters.put("ARRAY_CAT",         (c, ctx) -> convertArrayConcat(c));
        converters.put("ARRAY_TO_STRING",   (c, ctx) -> convertArrayToString(c));
        converters.put("ARRAY_SLICE",       (c, ctx) -> convertArraySlice(c));
        converters.put("ARRAY_FLATTEN",     (c, ctx) -> convertArrayFlatten(c));
        converters.put("PARSE_JSON",        (c, ctx) -> convertParseJson(c));
        converters.put("FLATTEN",           (c, ctx) -> convertFlatten(c));

        // Aggregate
        converters.put("LISTAGG",           (c, ctx) -> convertListAgg(c));
        converters.put("BOOLAND_AGG",       (c, ctx) -> convertBoolAndAgg(c));
        converters.put("BOOLOR_AGG",        (c, ctx) -> convertBoolOrAgg(c));
        converters.put("BOOLXOR_AGG",       (c, ctx) -> convertBoolXorAgg(c));
        converters.put("APPROX_COUNT_DISTINCT", (c, ctx) -> convertApproxCountDistinct(c));
        converters.put("MEDIAN",            (c, ctx) -> convertMedian(c));
        converters.put("OBJECT_AGG",        (c, ctx) -> convertObjectAgg(c));
        converters.put("ANY_VALUE",         (c, ctx) -> convertAnyValue(c));
        converters.put("BITAND_AGG",        (c, ctx) -> convertBitAndAgg(c));
        converters.put("BITOR_AGG",         (c, ctx) -> convertBitOrAgg(c));
        converters.put("BITXOR_AGG",        (c, ctx) -> convertBitXorAgg(c));
        converters.put("SKEW",              (c, ctx) -> convertSkew(c));
    }

    // ── Warnings ─────────────────────────────────────────────────────────────

    /** Clears accumulated warnings. Call before each new conversion. */
    void clearWarnings() {
        warnings = new ArrayList<>();
    }

    /** Returns an unmodifiable view of warnings accumulated since the last {@link #clearWarnings()}. */
    List<TranslationWarning> getWarnings() {
        return Collections.unmodifiableList(warnings);
    }

    /** Logs a warning and records it for structured retrieval. */
    void warn(String functionName, WarningType type, String message) {
        logger.warn("[{}] {}", functionName, message);
        warnings.add(new TranslationWarning(functionName, type, message));
    }

    // ── Entry point ──────────────────────────────────────────────────────────

    /** Main entry point: applies this visitor to the given AST node. */
    public SqlNode convert(SqlNode node) {
        return node.accept(this);
    }

    @Override
    public SqlNode visit(SqlCall call) {
        SqlOperator operator = call.getOperator();
        String functionName = operator.getName().toUpperCase();

        logger.debug("Visiting function call: {}", functionName);

        // Window call: intercept before registry lookup so we can inspect the inner aggregate.
        if ("OVER".equals(functionName)) {
            SqlCall aggCall = (SqlCall) call.operand(0);
            String aggName = aggCall.getOperator().getName().toUpperCase();
            if ("RATIO_TO_REPORT".equals(aggName)) {
                return convertRatioToReport(aggCall, (SqlWindow) call.operand(1));
            }
            return super.visit(call);
        }

        FunctionConverter converter = converters.get(functionName);
        if (converter != null) {
            return converter.convert(call, this);
        }
        return super.visit(call);
    }

    // ── Helper factories (package-private so external FunctionConverter impls can use them) ──

    /**
     * Builds a Trino function call with the given name and operands.
     * The function is emitted as a plain identifier (unquoted keyword style).
     */
    SqlBasicCall buildFunction(String name, SqlNode... operands) {
        return new SqlBasicCall(
                new SqlFunction(name, SqlKind.OTHER_FUNCTION,
                        null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION),
                operands,
                SqlParserPos.ZERO
        );
    }

    /** Builds a {@code CAST(value AS typeName)} call. */
    SqlNode buildCast(SqlNode value, SqlTypeName typeName) {
        SqlDataTypeSpec type = new SqlDataTypeSpec(
                new SqlBasicTypeNameSpec(typeName, SqlParserPos.ZERO),
                SqlParserPos.ZERO
        );
        return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, value, type);
    }

    /**
     * Converts a date-part node (the unit argument of DATEADD / DATEDIFF) to a
     * single-quoted Trino string literal, e.g. {@code 'day'}, {@code 'month'}.
     *
     * <p>The Babel parser may represent the unit as a {@code SqlIdentifier}, {@code SqlLiteral},
     * {@code SqlIntervalQualifier}, or already-quoted {@code SqlCharStringLiteral}.
     */
    SqlNode toDatePartLiteral(SqlNode node) {
        if (node instanceof SqlCharStringLiteral) {
            return node;
        }
        if (node instanceof SqlIdentifier) {
            return SqlLiteral.createCharString(
                    ((SqlIdentifier) node).getSimple().toLowerCase(), SqlParserPos.ZERO);
        }
        if (node instanceof SqlIntervalQualifier) {
            String unit = ((SqlIntervalQualifier) node).timeUnitRange.startUnit.name().toLowerCase();
            return SqlLiteral.createCharString(unit, SqlParserPos.ZERO);
        }
        if (node instanceof SqlLiteral) {
            Object value = ((SqlLiteral) node).getValue();
            if (value != null) {
                return SqlLiteral.createCharString(value.toString().toLowerCase(), SqlParserPos.ZERO);
            }
        }
        return SqlLiteral.createCharString(node.toString().toLowerCase(), SqlParserPos.ZERO);
    }

    /**
     * Converts a Snowflake date format string to Trino {@code date_parse} format.
     * Tokens are matched longest-first to avoid partial substitution (e.g. YYYY before YY).
     */
    String convertDateFormat(String snowflakeFormat) {
        return snowflakeFormat
                .replace("YYYY", "%Y")
                .replace("YY", "%y")
                .replace("MM", "%m")
                .replace("DD", "%d")
                .replace("HH24", "%H")
                .replace("HH12", "%I")
                .replace("MI", "%i")
                .replace("SS", "%s");
    }

    // ── Date / Time converters ───────────────────────────────────────────────

    /**
     * DATEADD(unit, n, date) → date_add('unit', n, date)
     */
    private SqlNode convertDateAdd(SqlCall call) {
        if (call.operandCount() != 3) {
            warn("DATEADD", WarningType.ARGUMENT_DROPPED, "expects 3 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("date_add",
                toDatePartLiteral(call.operand(0)),
                call.operand(1).accept(this),
                call.operand(2).accept(this));
    }

    /**
     * DATEDIFF(unit, start, end) → date_diff('unit', start, end)
     */
    private SqlNode convertDateDiff(SqlCall call) {
        if (call.operandCount() != 3) {
            warn("DATEDIFF", WarningType.ARGUMENT_DROPPED, "expects 3 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("date_diff",
                toDatePartLiteral(call.operand(0)),
                call.operand(1).accept(this),
                call.operand(2).accept(this));
    }

    /**
     * TO_DATE(str) → CAST(str AS DATE)
     * TO_DATE(str, format) → CAST(date_parse(str, trinoFormat) AS DATE)
     */
    private SqlNode convertToDate(SqlCall call) {
        if (call.operandCount() == 1) {
            return buildCast(call.operand(0).accept(this), SqlTypeName.DATE);
        } else if (call.operandCount() == 2) {
            SqlNode valueNode = call.operand(0).accept(this);
            SqlNode formatNode = call.operand(1);
            if (formatNode instanceof SqlCharStringLiteral) {
                String snowflakeFormat = ((SqlCharStringLiteral) formatNode).getNlsString().getValue();
                formatNode = SqlLiteral.createCharString(convertDateFormat(snowflakeFormat), SqlParserPos.ZERO);
            }
            return buildCast(buildFunction("date_parse", valueNode, formatNode), SqlTypeName.DATE);
        }
        return call;
    }

    /**
     * TO_TIMESTAMP(str) → CAST(str AS TIMESTAMP)
     */
    private SqlNode convertToTimestamp(SqlCall call) {
        if (call.operandCount() == 1) {
            SqlDataTypeSpec timestampType = new SqlDataTypeSpec(
                    new SqlBasicTypeNameSpec(SqlTypeName.TIMESTAMP, SqlParserPos.ZERO),
                    SqlParserPos.ZERO
            );
            return SqlStdOperatorTable.CAST.createCall(
                    SqlParserPos.ZERO,
                    call.operand(0).accept(this),
                    timestampType
            );
        }
        return call;
    }

    /** SYSDATE() / GETDATE() → now() */
    private SqlNode convertSysdate(SqlCall call) {
        return buildFunction("now");
    }

    /** LAST_DAY(date) → last_day_of_month(date) */
    private SqlNode convertLastDay(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("LAST_DAY", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("last_day_of_month", call.operand(0).accept(this));
    }

    /** DAYOFWEEK(date) → day_of_week(date) */
    private SqlNode convertDayOfWeek(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("DAYOFWEEK", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("day_of_week", call.operand(0).accept(this));
    }

    /** DAYOFYEAR(date) → day_of_year(date) */
    private SqlNode convertDayOfYear(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("DAYOFYEAR", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("day_of_year", call.operand(0).accept(this));
    }

    /** WEEKOFYEAR(date) → week_of_year(date) */
    private SqlNode convertWeekOfYear(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("WEEKOFYEAR", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("week_of_year", call.operand(0).accept(this));
    }

    /** ADD_MONTHS(date, n) → date_add('month', n, date) */
    private SqlNode convertAddMonths(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("ADD_MONTHS", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("date_add",
                SqlLiteral.createCharString("month", SqlParserPos.ZERO),
                call.operand(1).accept(this),
                call.operand(0).accept(this));
    }

    /** MONTHNAME(date) → format_datetime(date, 'MMMM') */
    private SqlNode convertMonthName(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("MONTHNAME", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("format_datetime",
                call.operand(0).accept(this),
                SqlLiteral.createCharString("MMMM", SqlParserPos.ZERO));
    }

    /** DAYNAME(date) → format_datetime(date, 'EEEE') */
    private SqlNode convertDayName(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("DAYNAME", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("format_datetime",
                call.operand(0).accept(this),
                SqlLiteral.createCharString("EEEE", SqlParserPos.ZERO));
    }

    /**
     * DATE_FROM_PARTS(year, month, day) → date(format('%04d-%02d-%02d', year, month, day))
     * Note: called as SF_DATE_FROM_PARTS after pre-processing; see SnowflakeTrinoTranslator.preprocess().
     */
    private SqlNode convertDateFromParts(SqlCall call) {
        if (call.operandCount() != 3) {
            warn("DATE_FROM_PARTS", WarningType.ARGUMENT_DROPPED, "expects 3 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("date",
                buildFunction("format",
                        SqlLiteral.createCharString("%04d-%02d-%02d", SqlParserPos.ZERO),
                        call.operand(0).accept(this),
                        call.operand(1).accept(this),
                        call.operand(2).accept(this)));
    }

    // ── Type conversion converters ───────────────────────────────────────────

    /** TO_TIME(str) → CAST(str AS TIME) */
    private SqlNode convertToTime(SqlCall call) {
        if (call.operandCount() < 1) {
            warn("TO_TIME", WarningType.ARGUMENT_DROPPED, "expects at least 1 operand, got " + call.operandCount());
            return call;
        }
        return buildCast(call.operand(0).accept(this), SqlTypeName.TIME);
    }

    /** TO_NUMBER / TO_NUMERIC / TO_DECIMAL → CAST(x AS DECIMAL) */
    private SqlNode convertToDecimal(SqlCall call) {
        String name = call.getOperator().getName();
        if (call.operandCount() < 1) {
            warn(name, WarningType.ARGUMENT_DROPPED, "expects at least 1 operand, got " + call.operandCount());
            return call;
        }
        if (call.operandCount() > 1) {
            warn(name, WarningType.ARGUMENT_DROPPED, "precision/scale args not supported; using CAST(x AS DECIMAL)");
        }
        return buildCast(call.operand(0).accept(this), SqlTypeName.DECIMAL);
    }

    /** TO_DOUBLE(str) → CAST(str AS DOUBLE) */
    private SqlNode convertToDouble(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("TO_DOUBLE", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildCast(call.operand(0).accept(this), SqlTypeName.DOUBLE);
    }

    /** TO_BOOLEAN(str) → CAST(str AS BOOLEAN) */
    private SqlNode convertToBoolean(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("TO_BOOLEAN", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildCast(call.operand(0).accept(this), SqlTypeName.BOOLEAN);
    }

    /** TO_VARCHAR / TO_CHAR → CAST(x AS VARCHAR). Format arg dropped with warning. */
    private SqlNode convertToVarchar(SqlCall call) {
        String name = call.getOperator().getName();
        if (call.operandCount() < 1) {
            warn(name, WarningType.ARGUMENT_DROPPED, "expects at least 1 operand, got " + call.operandCount());
            return call;
        }
        if (call.operandCount() > 1) {
            warn(name, WarningType.ARGUMENT_DROPPED, "format argument is not supported; dropped");
        }
        SqlDataTypeSpec varcharType = new SqlDataTypeSpec(
                new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, SqlParserPos.ZERO),
                SqlParserPos.ZERO
        );
        return SqlStdOperatorTable.CAST.createCall(
                SqlParserPos.ZERO,
                call.operand(0).accept(this),
                varcharType
        );
    }

    /** TRY_CAST — Trino supports TRY_CAST natively; recurse into operands. */
    private SqlNode convertTryCast(SqlCall call) {
        return super.visit(call);
    }

    // ── Conditional / null handling converters ───────────────────────────────

    /** IFF(cond, t, f) → IF(cond, t, f) */
    private SqlNode convertIff(SqlCall call) {
        if (call.operandCount() != 3) {
            warn("IFF", WarningType.ARGUMENT_DROPPED, "expects 3 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("IF",
                call.operand(0).accept(this),
                call.operand(1).accept(this),
                call.operand(2).accept(this));
    }

    /** IFNULL(x, default) → COALESCE(x, default) */
    private SqlNode convertIfNull(SqlCall call) {
        return SqlStdOperatorTable.COALESCE.createCall(
                SqlParserPos.ZERO,
                call.operand(0).accept(this),
                call.operand(1).accept(this)
        );
    }

    /** NVL(x, default) → COALESCE(x, default) */
    private SqlNode convertNvl(SqlCall call) {
        return SqlStdOperatorTable.COALESCE.createCall(
                SqlParserPos.ZERO,
                call.operand(0).accept(this),
                call.operand(1).accept(this)
        );
    }

    /** NVL2(expr, not_null_val, null_val) → IF(expr IS NOT NULL, not_null_val, null_val) */
    private SqlNode convertNvl2(SqlCall call) {
        if (call.operandCount() != 3) {
            warn("NVL2", WarningType.ARGUMENT_DROPPED, "expects 3 operands, got " + call.operandCount());
            return call;
        }
        SqlNode expr = call.operand(0).accept(this);
        SqlNode isNotNull = SqlStdOperatorTable.IS_NOT_NULL.createCall(SqlParserPos.ZERO, expr);
        return buildFunction("IF", isNotNull, call.operand(1).accept(this), call.operand(2).accept(this));
    }

    /** ZEROIFNULL(x) → COALESCE(x, 0) */
    private SqlNode convertZeroIfNull(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("ZEROIFNULL", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return SqlStdOperatorTable.COALESCE.createCall(
                SqlParserPos.ZERO,
                call.operand(0).accept(this),
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO)
        );
    }

    /** NULLIFZERO(x) → NULLIF(x, 0) */
    private SqlNode convertNullIfZero(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("NULLIFZERO", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return SqlStdOperatorTable.NULLIF.createCall(
                SqlParserPos.ZERO,
                call.operand(0).accept(this),
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO)
        );
    }

    /**
     * DIV0(x, y) → IF(y = 0, CAST(0 AS DOUBLE), CAST(x AS DOUBLE) / y)
     * Uses DOUBLE to match Snowflake's floating-point return type.
     */
    private SqlNode convertDiv0(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("DIV0", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        SqlNode x = call.operand(0).accept(this);
        SqlNode y = call.operand(1).accept(this);
        SqlNode zero = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
        SqlNode condition = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, y, zero);
        SqlNode division = SqlStdOperatorTable.DIVIDE.createCall(SqlParserPos.ZERO, buildCast(x, SqlTypeName.DOUBLE), y);
        return buildFunction("IF", condition, buildCast(zero, SqlTypeName.DOUBLE), division);
    }

    /**
     * DECODE(expr, s1,r1, s2,r2, …, default) →
     * CASE WHEN expr=s1 THEN r1 … ELSE default END
     */
    private SqlNode convertDecode(SqlCall call) {
        int operandCount = call.operandCount();
        if (operandCount < 3) {
            warn("DECODE", WarningType.ARGUMENT_DROPPED, "expects at least 3 operands, got " + operandCount);
            return call;
        }
        SqlNode expr = call.operand(0).accept(this);
        int remaining = operandCount - 1;
        boolean hasElse = (remaining % 2 == 1);
        int pairCount = remaining / 2;

        List<SqlNode> whenNodes = new ArrayList<>();
        List<SqlNode> thenNodes = new ArrayList<>();
        for (int i = 0; i < pairCount; i++) {
            SqlNode searchVal = call.operand(1 + i * 2).accept(this);
            SqlNode resultVal = call.operand(2 + i * 2).accept(this);
            whenNodes.add(SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, expr, searchVal));
            thenNodes.add(resultVal);
        }
        SqlNode elseClause = hasElse
                ? call.operand(operandCount - 1).accept(this)
                : SqlLiteral.createNull(SqlParserPos.ZERO);

        return new SqlCase(
                SqlParserPos.ZERO,
                null,
                new SqlNodeList(whenNodes, SqlParserPos.ZERO),
                new SqlNodeList(thenNodes, SqlParserPos.ZERO),
                elseClause
        );
    }

    // ── String converters ────────────────────────────────────────────────────

    /** LEFT(str, n) → substr(str, 1, n) */
    private SqlNode convertLeft(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("LEFT", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("substr",
                call.operand(0).accept(this),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
                call.operand(1).accept(this));
    }

    /** RIGHT(str, n) → substr(str, length(str) - n + 1, n) */
    private SqlNode convertRight(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("RIGHT", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        SqlNode str = call.operand(0).accept(this);
        SqlNode n = call.operand(1).accept(this);
        SqlNode one = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlNode lengthCall = buildFunction("length", str);
        SqlNode startPos = SqlStdOperatorTable.PLUS.createCall(SqlParserPos.ZERO,
                SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, lengthCall, n), one);
        return buildFunction("substr", str, startPos, n);
    }

    /** STARTSWITH(str, prefix) → starts_with(str, prefix) */
    private SqlNode convertStartsWith(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("STARTSWITH", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("starts_with", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /**
     * ENDSWITH(str, suffix) → SUBSTR(str, LENGTH(str) - LENGTH(suffix) + 1) = suffix
     * Uses a SQL-standard expression for compatibility with older Trino versions.
     */
    private SqlNode convertEndsWith(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("ENDSWITH", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        SqlNode str = call.operand(0).accept(this);
        SqlNode suffix = call.operand(1).accept(this);
        SqlNode one = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlNode start = SqlStdOperatorTable.PLUS.createCall(SqlParserPos.ZERO,
                SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO,
                        buildFunction("length", str), buildFunction("length", suffix)),
                one);
        return SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, buildFunction("substr", str, start), suffix);
    }

    /** CONTAINS(str, sub) → strpos(str, sub) > 0 */
    private SqlNode convertContains(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("CONTAINS", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        SqlNode strpos = buildFunction("strpos", call.operand(0).accept(this), call.operand(1).accept(this));
        return SqlStdOperatorTable.GREATER_THAN.createCall(SqlParserPos.ZERO,
                strpos, SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO));
    }

    /** INSTR(str, sub) → strpos(str, sub) */
    private SqlNode convertInstr(SqlCall call) {
        if (call.operandCount() < 2) {
            warn("INSTR", WarningType.ARGUMENT_DROPPED, "expects at least 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("strpos", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /**
     * REGEXP_SUBSTR(str, pattern [, …]) → regexp_extract(str, pattern)
     * Extra arguments are dropped with a warning.
     */
    private SqlNode convertRegexpSubstr(SqlCall call) {
        if (call.operandCount() < 2) {
            warn("REGEXP_SUBSTR", WarningType.ARGUMENT_DROPPED, "expects at least 2 operands, got " + call.operandCount());
            return call;
        }
        if (call.operandCount() > 2) {
            warn("REGEXP_SUBSTR", WarningType.ARGUMENT_DROPPED,
                    "has " + call.operandCount() + " args; only the first 2 (str, pattern) are translated");
        }
        return buildFunction("regexp_extract",
                call.operand(0).accept(this),
                call.operand(1).accept(this));
    }

    /**
     * CHARINDEX(substr, str [, start_pos]) → strpos(str, substr)
     * Argument order is reversed; optional start_pos is dropped with a warning.
     */
    private SqlNode convertCharIndex(SqlCall call) {
        if (call.operandCount() < 2) {
            warn("CHARINDEX", WarningType.ARGUMENT_DROPPED, "expects at least 2 operands, got " + call.operandCount());
            return call;
        }
        if (call.operandCount() > 2) {
            warn("CHARINDEX", WarningType.ARGUMENT_DROPPED, "start_pos argument is not supported; dropped");
        }
        return buildFunction("strpos",
                call.operand(1).accept(this),
                call.operand(0).accept(this));
    }

    /** STRTOK(str, delim, n) → split_part(str, delim, n) */
    private SqlNode convertStrtok(SqlCall call) {
        if (call.operandCount() != 3) {
            warn("STRTOK", WarningType.ARGUMENT_DROPPED, "expects 3 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("split_part",
                call.operand(0).accept(this),
                call.operand(1).accept(this),
                call.operand(2).accept(this));
    }

    /** EDITDISTANCE(s1, s2) → levenshtein_distance(s1, s2) */
    private SqlNode convertEditDistance(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("EDITDISTANCE", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("levenshtein_distance", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /**
     * SPACE(n) → rpad('', n, ' ')
     * Note: repeat(' ', n) in Trino resolves to the array repeat variant for char literals.
     */
    private SqlNode convertSpace(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("SPACE", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("rpad",
                SqlLiteral.createCharString("", SqlParserPos.ZERO),
                call.operand(0).accept(this),
                SqlLiteral.createCharString(" ", SqlParserPos.ZERO));
    }

    // ── Hash / Encoding converters ───────────────────────────────────────────

    /** MD5(str) → lower(to_hex(md5(to_utf8(str)))) */
    private SqlNode convertMd5(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("MD5", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("lower",
                buildFunction("to_hex",
                        buildFunction("md5",
                                buildFunction("to_utf8", call.operand(0).accept(this)))));
    }

    /** SHA1(str) → lower(to_hex(sha1(to_utf8(str)))) */
    private SqlNode convertSha1(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("SHA1", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("lower",
                buildFunction("to_hex",
                        buildFunction("sha1",
                                buildFunction("to_utf8", call.operand(0).accept(this)))));
    }

    /**
     * SHA2(str[, bits]) → lower(to_hex(sha256|sha512(to_utf8(str))))
     * Only 256 (default) and 512 bits are supported.
     */
    private SqlNode convertSha2(SqlCall call) {
        if (call.operandCount() < 1 || call.operandCount() > 2) {
            warn("SHA2", WarningType.ARGUMENT_DROPPED, "expects 1 or 2 operands, got " + call.operandCount());
            return call;
        }
        String hashFn = "sha256";
        if (call.operandCount() == 2) {
            SqlNode bitsNode = call.operand(1);
            if (bitsNode instanceof SqlLiteral) {
                String bitsStr = ((SqlLiteral) bitsNode).toValue();
                if ("512".equals(bitsStr)) {
                    hashFn = "sha512";
                } else if (!"256".equals(bitsStr) && !"0".equals(bitsStr)) {
                    warn("SHA2", WarningType.UNSUPPORTED_FEATURE,
                            "bits=" + bitsStr + " not supported; only 256 and 512 are; defaulting to sha256");
                }
            }
        }
        return buildFunction("lower",
                buildFunction("to_hex",
                        buildFunction(hashFn,
                                buildFunction("to_utf8", call.operand(0).accept(this)))));
    }

    /** HEX_ENCODE(str) → lower(to_hex(to_utf8(str))) */
    private SqlNode convertHexEncode(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("HEX_ENCODE", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("lower",
                buildFunction("to_hex",
                        buildFunction("to_utf8", call.operand(0).accept(this))));
    }

    /** HEX_DECODE_STRING(str) → from_utf8(from_hex(str)) */
    private SqlNode convertHexDecodeString(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("HEX_DECODE_STRING", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("from_utf8",
                buildFunction("from_hex", call.operand(0).accept(this)));
    }

    /** BASE64_ENCODE(str) → to_base64(to_utf8(str)) */
    private SqlNode convertBase64Encode(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("BASE64_ENCODE", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("to_base64", buildFunction("to_utf8", call.operand(0).accept(this)));
    }

    /** BASE64_DECODE_STRING(str) → from_utf8(from_base64(str)) */
    private SqlNode convertBase64DecodeString(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("BASE64_DECODE_STRING", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("from_utf8", buildFunction("from_base64", call.operand(0).accept(this)));
    }

    // ── URL converters ───────────────────────────────────────────────────────

    /** URL_ENCODE(str) → url_encode(str) */
    private SqlNode convertUrlEncode(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("URL_ENCODE", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("url_encode", call.operand(0).accept(this));
    }

    /** URL_DECODE(str) → url_decode(str) */
    private SqlNode convertUrlDecode(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("URL_DECODE", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("url_decode", call.operand(0).accept(this));
    }

    // ── Math / Bitwise converters ────────────────────────────────────────────

    /**
     * SQUARE(x) → x * x
     * Uses multiplication to preserve integer type; power(x, 2) returns DOUBLE in Trino.
     * The operand is accepted twice to produce two independent (but structurally equal) AST
     * subtrees, avoiding shared-node aliasing in the output AST.
     * Note: if x is non-deterministic (e.g. random()), it will be evaluated twice at runtime.
     */
    private SqlNode convertSquare(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("SQUARE", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        SqlNode left  = call.operand(0).accept(this);
        SqlNode right = call.operand(0).accept(this);
        return SqlStdOperatorTable.MULTIPLY.createCall(SqlParserPos.ZERO, left, right);
    }

    /** TRUNC(x) / TRUNC(x, scale) → truncate(x) / truncate(x, scale) */
    private SqlNode convertTrunc(SqlCall call) {
        if (call.operandCount() == 1) {
            return buildFunction("truncate", call.operand(0).accept(this));
        }
        return buildFunction("truncate", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /**
     * CEIL(x) / CEIL(x, scale) → ceil(x)
     * Scale arg is dropped; Trino's ceil does not support a scale argument.
     * Note: called as SF_CEIL after pre-processing; see SnowflakeTrinoTranslator.preprocess().
     */
    private SqlNode convertCeil(SqlCall call) {
        if (call.operandCount() > 1) {
            warn("CEIL", WarningType.ARGUMENT_DROPPED, "scale argument is not supported in Trino; dropped");
        }
        return buildFunction("ceil", call.operand(0).accept(this));
    }

    /**
     * FLOOR(x) / FLOOR(x, scale) → floor(x)
     * Scale arg is dropped; Trino's floor does not support a scale argument.
     * Note: called as SF_FLOOR after pre-processing; see SnowflakeTrinoTranslator.preprocess().
     */
    private SqlNode convertFloor(SqlCall call) {
        if (call.operandCount() > 1) {
            warn("FLOOR", WarningType.ARGUMENT_DROPPED, "scale argument is not supported in Trino; dropped");
        }
        return buildFunction("floor", call.operand(0).accept(this));
    }

    /** BITAND(x, y) → bitwise_and(x, y) */
    private SqlNode convertBitAnd(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("BITAND", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("bitwise_and", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** BITOR(x, y) → bitwise_or(x, y) */
    private SqlNode convertBitOr(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("BITOR", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("bitwise_or", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** BITXOR(x, y) → bitwise_xor(x, y) */
    private SqlNode convertBitXor(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("BITXOR", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("bitwise_xor", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** BITNOT(x) → bitwise_not(x) */
    private SqlNode convertBitNot(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("BITNOT", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("bitwise_not", call.operand(0).accept(this));
    }

    /** BITSHIFTLEFT(x, n) → bitwise_left_shift(x, n) */
    private SqlNode convertBitShiftLeft(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("BITSHIFTLEFT", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("bitwise_left_shift", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** BITSHIFTRIGHT(x, n) → bitwise_right_shift(x, n) */
    private SqlNode convertBitShiftRight(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("BITSHIFTRIGHT", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("bitwise_right_shift", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    // ── Array / JSON converters ──────────────────────────────────────────────

    /** ARRAY_SIZE(arr) → cardinality(arr) */
    private SqlNode convertArraySize(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("ARRAY_SIZE", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("cardinality", call.operand(0).accept(this));
    }

    /** ARRAY_CONTAINS(value, arr) → contains(arr, value) — argument order reversed */
    private SqlNode convertArrayContains(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("ARRAY_CONTAINS", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("contains",
                call.operand(1).accept(this),
                call.operand(0).accept(this));
    }

    /** ARRAY_CONCAT / ARRAY_CAT → concat(…) */
    private SqlNode convertArrayConcat(SqlCall call) {
        if (call.operandCount() < 2) {
            warn("ARRAY_CONCAT", WarningType.ARGUMENT_DROPPED, "expects at least 2 operands, got " + call.operandCount());
            return call;
        }
        SqlNode[] operands = new SqlNode[call.operandCount()];
        for (int i = 0; i < call.operandCount(); i++) {
            operands[i] = call.operand(i).accept(this);
        }
        return buildFunction("concat", operands);
    }

    /** ARRAY_TO_STRING(arr, delim) → array_join(arr, delim) */
    private SqlNode convertArrayToString(SqlCall call) {
        if (call.operandCount() < 2) {
            warn("ARRAY_TO_STRING", WarningType.ARGUMENT_DROPPED, "expects at least 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("array_join", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /**
     * ARRAY_SLICE(arr, from, to) → slice(arr, from+1, to-from)
     * Snowflake is 0-indexed [from, to); Trino slice is 1-indexed (start, length).
     */
    private SqlNode convertArraySlice(SqlCall call) {
        if (call.operandCount() != 3) {
            warn("ARRAY_SLICE", WarningType.ARGUMENT_DROPPED, "expects 3 operands, got " + call.operandCount());
            return call;
        }
        SqlNode arr  = call.operand(0).accept(this);
        SqlNode from = call.operand(1).accept(this);
        SqlNode to   = call.operand(2).accept(this);
        SqlNode one  = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlNode start  = SqlStdOperatorTable.PLUS.createCall(SqlParserPos.ZERO, from, one);
        SqlNode length = SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, to, from);
        return buildFunction("slice", arr, start, length);
    }

    /** ARRAY_FLATTEN(arr) → flatten(arr) */
    private SqlNode convertArrayFlatten(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("ARRAY_FLATTEN", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("flatten", call.operand(0).accept(this));
    }

    /** PARSE_JSON(str) → json_parse(str) */
    private SqlNode convertParseJson(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("PARSE_JSON", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("json_parse", call.operand(0).accept(this));
    }

    /**
     * FLATTEN — Snowflake's lateral flatten has no direct SQL-level equivalent in Trino.
     * Requires query restructuring to CROSS JOIN UNNEST. Passed through unchanged.
     */
    private SqlNode convertFlatten(SqlCall call) {
        warn("FLATTEN", WarningType.UNSUPPORTED_FEATURE,
                "lateral flatten has no direct Trino equivalent; manual rewrite to CROSS JOIN UNNEST required");
        return call;
    }

    // ── Aggregate converters ─────────────────────────────────────────────────

    /** LISTAGG(col[, delim]) → array_join(array_agg(col), delim) */
    private SqlNode convertListAgg(SqlCall call) {
        SqlNode columnNode = call.operand(0).accept(this);
        SqlNode delimiterNode = call.operandCount() > 1
                ? call.operand(1).accept(this)
                : SqlLiteral.createCharString(",", SqlParserPos.ZERO);
        return buildFunction("array_join", buildFunction("array_agg", columnNode), delimiterNode);
    }

    /** BOOLAND_AGG(x) → bool_and(x) */
    private SqlNode convertBoolAndAgg(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("BOOLAND_AGG", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("bool_and", call.operand(0).accept(this));
    }

    /** BOOLOR_AGG(x) → bool_or(x) */
    private SqlNode convertBoolOrAgg(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("BOOLOR_AGG", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("bool_or", call.operand(0).accept(this));
    }

    /**
     * BOOLXOR_AGG(x) → (count_if(x) % 2) = 1
     * True if an odd number of inputs are true. Note: produces a boolean expression,
     * not a standalone aggregate call — may affect how it can be referenced in outer queries.
     */
    private SqlNode convertBoolXorAgg(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("BOOLXOR_AGG", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        SqlNode countIf = buildFunction("count_if", call.operand(0).accept(this));
        SqlNode two = SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO);
        SqlNode one = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlNode mod = SqlStdOperatorTable.MOD.createCall(SqlParserPos.ZERO, countIf, two);
        return SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, mod, one);
    }

    /** APPROX_COUNT_DISTINCT(x) → approx_distinct(x) */
    private SqlNode convertApproxCountDistinct(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("APPROX_COUNT_DISTINCT", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("approx_distinct", call.operand(0).accept(this));
    }

    /**
     * MEDIAN(x) → approx_percentile(x, 0.5)
     * Trino has no exact median; uses t-digest approximation.
     */
    private SqlNode convertMedian(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("MEDIAN", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        warn("MEDIAN", WarningType.APPROXIMATE_TRANSLATION,
                "translated to approx_percentile(x, 0.5); Trino has no exact median");
        return buildFunction("approx_percentile",
                call.operand(0).accept(this),
                SqlLiteral.createExactNumeric("0.5", SqlParserPos.ZERO));
    }

    /** OBJECT_AGG(key, value) → map_agg(key, value) */
    private SqlNode convertObjectAgg(SqlCall call) {
        if (call.operandCount() != 2) {
            warn("OBJECT_AGG", WarningType.ARGUMENT_DROPPED, "expects 2 operands, got " + call.operandCount());
            return call;
        }
        return buildFunction("map_agg", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** ANY_VALUE(x) → arbitrary(x) */
    private SqlNode convertAnyValue(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("ANY_VALUE", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("arbitrary", call.operand(0).accept(this));
    }

    /** BITAND_AGG(x) → bitwise_and_agg(x) */
    private SqlNode convertBitAndAgg(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("BITAND_AGG", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("bitwise_and_agg", call.operand(0).accept(this));
    }

    /** BITOR_AGG(x) → bitwise_or_agg(x) */
    private SqlNode convertBitOrAgg(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("BITOR_AGG", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("bitwise_or_agg", call.operand(0).accept(this));
    }

    /** BITXOR_AGG(x) → bitwise_xor_agg(x) */
    private SqlNode convertBitXorAgg(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("BITXOR_AGG", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        return buildFunction("bitwise_xor_agg", call.operand(0).accept(this));
    }

    /**
     * SKEW(x) → adjusted Fisher-Pearson sample skewness.
     * Trino's SKEWNESS computes biased population skewness; we apply correction factor
     * G1 = skewness * sqrt(n*(n-1)) / (n-2). Returns NULL for n ≤ 2.
     */
    private SqlNode convertSkew(SqlCall call) {
        if (call.operandCount() != 1) {
            warn("SKEW", WarningType.ARGUMENT_DROPPED, "expects 1 operand, got " + call.operandCount());
            return call;
        }
        SqlNode arg = call.operand(0).accept(this);
        SqlNode skewness = buildFunction("skewness", arg);
        SqlNode n1 = buildCast(buildFunction("count", arg), SqlTypeName.DOUBLE);
        SqlNode n2 = buildCast(buildFunction("count", arg), SqlTypeName.DOUBLE);
        SqlNode n3 = buildCast(buildFunction("count", arg), SqlTypeName.DOUBLE);
        SqlNode nMinus1 = SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, n1,
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
        SqlNode sqrt = buildFunction("sqrt",
                SqlStdOperatorTable.MULTIPLY.createCall(SqlParserPos.ZERO, n2, nMinus1));
        SqlNode numerator = SqlStdOperatorTable.MULTIPLY.createCall(SqlParserPos.ZERO, skewness, sqrt);
        SqlNode nMinus2 = SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, n3,
                SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO));
        SqlNode formula = SqlStdOperatorTable.DIVIDE.createCall(SqlParserPos.ZERO, numerator, nMinus2);
        SqlNode condition = SqlStdOperatorTable.LESS_THAN_OR_EQUAL.createCall(SqlParserPos.ZERO,
                buildFunction("count", arg),
                SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO));
        return new SqlCase(
                SqlParserPos.ZERO,
                null,
                new SqlNodeList(List.of(condition), SqlParserPos.ZERO),
                new SqlNodeList(List.of(SqlLiteral.createNull(SqlParserPos.ZERO)), SqlParserPos.ZERO),
                formula
        );
    }

    // ── Window converter ─────────────────────────────────────────────────────

    /**
     * RATIO_TO_REPORT(x) OVER (w) → x / SUM(x) OVER (w)
     * The same window clause is reused for the SUM.
     * The operand is accepted twice to produce two independent AST subtrees for the dividend
     * and the SUM argument, avoiding shared-node aliasing.
     */
    private SqlNode convertRatioToReport(SqlCall aggCall, SqlWindow window) {
        SqlNode dividend  = aggCall.operand(0).accept(this);
        SqlNode sumArg    = aggCall.operand(0).accept(this);
        SqlNode sumCall   = buildFunction("SUM", sumArg);
        SqlNode sumOver   = SqlStdOperatorTable.OVER.createCall(SqlParserPos.ZERO, sumCall, window);
        return SqlStdOperatorTable.DIVIDE.createCall(SqlParserPos.ZERO, dividend, sumOver);
    }
}
