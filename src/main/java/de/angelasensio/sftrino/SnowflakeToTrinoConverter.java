package de.angelasensio.sftrino;

import java.util.ArrayList;
import java.util.List;

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
 * Uses the Visitor pattern via SqlShuttle to traverse and transform the AST.
 */
public class SnowflakeToTrinoConverter extends SqlShuttle {

    private static final Logger logger = LoggerFactory.getLogger(SnowflakeToTrinoConverter.class);

    /**
     * Main entry point for conversion.
     */
    public SqlNode convert(SqlNode node) {
        return node.accept(this);
    }

    @Override
    public SqlNode visit(SqlCall call) {
        // Transform function calls
        SqlOperator operator = call.getOperator();
        String functionName = operator.getName().toUpperCase();

        logger.debug("Visiting function call: {}", functionName);

        // Handle Snowflake-specific functions
        switch (functionName) {
            case "OVER": {
                // Window call: operand(0)=agg function, operand(1)=window spec
                SqlCall aggCall = (SqlCall) call.operand(0);
                String aggName = aggCall.getOperator().getName().toUpperCase();
                if ("RATIO_TO_REPORT".equals(aggName)) {
                    return convertRatioToReport(aggCall, (SqlWindow) call.operand(1));
                }
                return super.visit(call);
            }
            case "DATEADD":
                return convertDateAdd(call);
            case "DATEDIFF":
                return convertDateDiff(call);
            case "TO_DATE":
                return convertToDate(call);
            case "TO_TIMESTAMP":
                return convertToTimestamp(call);
            case "IFNULL":
                return convertIfNull(call);
            case "NVL":
                return convertNvl(call);
            case "LISTAGG":
                return convertListAgg(call);
            case "FLATTEN":
                return convertFlatten(call);
            case "TRY_CAST":
                return convertTryCast(call);
            case "IFF":
                return convertIff(call);
            case "ZEROIFNULL":
                return convertZeroIfNull(call);
            case "NULLIFZERO":
                return convertNullIfZero(call);
            case "SQUARE":
                return convertSquare(call);
            case "REGEXP_SUBSTR":
                return convertRegexpSubstr(call);
            case "CHARINDEX":
                return convertCharIndex(call);
            case "ARRAY_SIZE":
                return convertArraySize(call);
            case "ARRAY_CONTAINS":
                return convertArrayContains(call);
            case "PARSE_JSON":
                return convertParseJson(call);
            case "BOOLAND_AGG":
                return convertBoolAndAgg(call);
            case "BOOLOR_AGG":
                return convertBoolOrAgg(call);
            case "DECODE":
                return convertDecode(call);
            case "TO_VARCHAR":
            case "TO_CHAR":
                return convertToVarchar(call);
            case "DIV0":
                return convertDiv0(call);
            case "STRTOK":
                return convertStrtok(call);
            case "NVL2":
                return convertNvl2(call);
            case "LEFT":
                return convertLeft(call);
            case "RIGHT":
                return convertRight(call);
            case "STARTSWITH":
                return convertStartsWith(call);
            case "ENDSWITH":
                return convertEndsWith(call);
            case "CONTAINS":
                return convertContains(call);
            case "INSTR":
                return convertInstr(call);
            case "SYSDATE":
            case "GETDATE":
                return convertSysdate(call);
            case "LAST_DAY":
                return convertLastDay(call);
            case "TO_TIME":
                return convertToTime(call);
            case "TO_NUMBER":
            case "TO_NUMERIC":
            case "TO_DECIMAL":
                return convertToDecimal(call);
            case "TO_DOUBLE":
                return convertToDouble(call);
            case "TO_BOOLEAN":
                return convertToBoolean(call);
            case "APPROX_COUNT_DISTINCT":
                return convertApproxCountDistinct(call);
            case "MEDIAN":
                return convertMedian(call);
            case "OBJECT_AGG":
                return convertObjectAgg(call);
            case "ARRAY_CONCAT":
            case "ARRAY_CAT":
                return convertArrayConcat(call);
            case "ARRAY_TO_STRING":
                return convertArrayToString(call);
            case "BASE64_ENCODE":
                return convertBase64Encode(call);
            case "BASE64_DECODE_STRING":
                return convertBase64DecodeString(call);
            case "DAYOFWEEK":
                return convertDayOfWeek(call);
            case "DAYOFYEAR":
                return convertDayOfYear(call);
            case "WEEKOFYEAR":
                return convertWeekOfYear(call);
            case "ADD_MONTHS":
                return convertAddMonths(call);
            case "MONTHNAME":
                return convertMonthName(call);
            case "DAYNAME":
                return convertDayName(call);
            case "BITAND":
                return convertBitAnd(call);
            case "BITOR":
                return convertBitOr(call);
            case "BITXOR":
                return convertBitXor(call);
            case "BITNOT":
                return convertBitNot(call);
            case "BITSHIFTLEFT":
                return convertBitShiftLeft(call);
            case "BITSHIFTRIGHT":
                return convertBitShiftRight(call);
            case "EDITDISTANCE":
                return convertEditDistance(call);
            case "SPACE":
                return convertSpace(call);
            case "MD5":
                return convertMd5(call);
            case "SHA1":
                return convertSha1(call);
            case "SHA2":
                return convertSha2(call);
            case "HEX_ENCODE":
                return convertHexEncode(call);
            case "HEX_DECODE_STRING":
                return convertHexDecodeString(call);
            case "URL_ENCODE":
                return convertUrlEncode(call);
            case "URL_DECODE":
                return convertUrlDecode(call);
            case "ARRAY_SLICE":
                return convertArraySlice(call);
            case "ARRAY_FLATTEN":
                return convertArrayFlatten(call);
            case "ANY_VALUE":
                return convertAnyValue(call);
            case "BITAND_AGG":
                return convertBitAndAgg(call);
            case "BITOR_AGG":
                return convertBitOrAgg(call);
            case "BITXOR_AGG":
                return convertBitXorAgg(call);
            case "SKEW":
                return convertSkew(call);
            case "BOOLXOR_AGG":
                return convertBoolXorAgg(call);
            case "TRUNC":
                return convertTrunc(call);
            case "SF_CEIL":
                return convertCeil(call);
            case "SF_FLOOR":
                return convertFloor(call);
            case "SF_DATE_FROM_PARTS":
                return convertDateFromParts(call);
            default:
                // Recursively process operands for other functions
                return super.visit(call);
        }
    }

    // ── Helper factories ────────────────────────────────────────────────────

    /**
     * Builds a Trino function call with the given name and operands.
     * Uses SqlFunction with a null sqlIdentifier so the name is output
     * as a keyword (unquoted) rather than as a quoted identifier.
     */
    private SqlBasicCall buildFunction(String name, SqlNode... operands) {
        return new SqlBasicCall(
                new SqlFunction(name, SqlKind.OTHER_FUNCTION,
                        null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION),
                operands,
                SqlParserPos.ZERO
        );
    }

    /** Builds a CAST(value AS typeName) call. */
    private SqlNode buildCast(SqlNode value, SqlTypeName typeName) {
        SqlDataTypeSpec type = new SqlDataTypeSpec(
                new SqlBasicTypeNameSpec(typeName, SqlParserPos.ZERO),
                SqlParserPos.ZERO
        );
        return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, value, type);
    }

    /**
     * Converts Snowflake DATEADD to Trino date_add.
     * Snowflake: DATEADD(day, 5, date_column)
     * Trino: date_add('day', 5, date_column)
     */
    private SqlNode convertDateAdd(SqlCall call) {
        if (call.operandCount() != 3) {
            logger.warn("DATEADD expects 3 operands, got {}", call.operandCount());
            return call;
        }

        SqlNode datePartNode = call.operand(0);
        SqlNode intervalNode = call.operand(1);
        SqlNode dateNode = call.operand(2);

        // Convert date part to string literal if it's an identifier
        return buildFunction("date_add",
                toDatePartLiteral(datePartNode),
                intervalNode.accept(this),
                dateNode.accept(this));
    }

    /**
     * Converts Snowflake DATEDIFF to Trino date_diff.
     * Snowflake: DATEDIFF(day, start_date, end_date)
     * Trino: date_diff('day', start_date, end_date)
     */
    private SqlNode convertDateDiff(SqlCall call) {
        if (call.operandCount() != 3) {
            logger.warn("DATEDIFF expects 3 operands, got {}", call.operandCount());
            return call;
        }

        SqlNode datePartNode = call.operand(0);
        SqlNode startDateNode = call.operand(1);
        SqlNode endDateNode = call.operand(2);

        return buildFunction("date_diff",
                toDatePartLiteral(datePartNode),
                startDateNode.accept(this),
                endDateNode.accept(this));
    }

    /**
     * Converts Snowflake TO_DATE to Trino date_parse or CAST.
     * Snowflake: TO_DATE(column, 'YYYY-MM-DD')
     * Trino: date_parse(column, '%Y-%m-%d') or CAST(column AS DATE)
     */
    private SqlNode convertToDate(SqlCall call) {
        if (call.operandCount() == 1) {
            // Simple case: TO_DATE(column) -> CAST(column AS DATE)
            return buildCast(call.operand(0).accept(this), SqlTypeName.DATE);
        } else if (call.operandCount() == 2) {
            // Format specified: use date_parse with converted format
            SqlNode valueNode = call.operand(0).accept(this);
            SqlNode formatNode = call.operand(1);

            // Convert Snowflake format to Trino format
            if (formatNode instanceof SqlCharStringLiteral) {
                String snowflakeFormat = ((SqlCharStringLiteral) formatNode).getNlsString().getValue();
                String trinoFormat = convertDateFormat(snowflakeFormat);
                formatNode = SqlLiteral.createCharString(trinoFormat, SqlParserPos.ZERO);
            }

            // date_parse returns TIMESTAMP; cast to DATE to match Snowflake's TO_DATE return type
            return buildCast(buildFunction("date_parse", valueNode, formatNode), SqlTypeName.DATE);
        }

        return call;
    }

    /**
     * Converts Snowflake TO_TIMESTAMP to Trino from_unixtime or CAST.
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

    /**
     * Converts IFNULL to COALESCE.
     * Snowflake: IFNULL(column, default)
     * Trino: COALESCE(column, default)
     */
    private SqlNode convertIfNull(SqlCall call) {
        return SqlStdOperatorTable.COALESCE.createCall(
                SqlParserPos.ZERO,
                call.operand(0).accept(this),
                call.operand(1).accept(this)
        );
    }

    /**
     * Converts NVL to COALESCE.
     */
    private SqlNode convertNvl(SqlCall call) {
        return SqlStdOperatorTable.COALESCE.createCall(
                SqlParserPos.ZERO,
                call.operand(0).accept(this),
                call.operand(1).accept(this)
        );
    }

    /**
     * Converts LISTAGG to Trino array_agg + array_join.
     * Snowflake: LISTAGG(column, ',')
     * Trino: array_join(array_agg(column), ',')
     */
    private SqlNode convertListAgg(SqlCall call) {
        SqlNode columnNode = call.operand(0).accept(this);
        SqlNode delimiterNode = call.operandCount() > 1 ?
                call.operand(1).accept(this) :
                SqlLiteral.createCharString(",", SqlParserPos.ZERO);

        return buildFunction("array_join", buildFunction("array_agg", columnNode), delimiterNode);
    }

    /**
     * Converts FLATTEN to CROSS JOIN UNNEST.
     * This is a simplified conversion - full FLATTEN has more complex semantics.
     */
    private SqlNode convertFlatten(SqlCall call) {
        logger.warn("FLATTEN conversion is simplified - manual review recommended");
        // Return as-is for now, as full conversion requires query rewriting
        return call;
    }

    /**
     * Converts TRY_CAST to Trino TRY_CAST (should work as-is).
     */
    private SqlNode convertTryCast(SqlCall call) {
        // Trino supports TRY_CAST natively
        return super.visit(call);
    }

    /**
     * Converts Snowflake IFF to Trino IF.
     * Snowflake: IFF(condition, true_value, false_value)
     * Trino: IF(condition, true_value, false_value)
     */
    private SqlNode convertIff(SqlCall call) {
        if (call.operandCount() != 3) {
            return call;
        }

        return buildFunction("IF",
                call.operand(0).accept(this),
                call.operand(1).accept(this),
                call.operand(2).accept(this));
    }

    /**
     * Converts ZEROIFNULL to COALESCE(x, 0).
     * Snowflake: ZEROIFNULL(column)
     * Trino: COALESCE(column, 0)
     */
    private SqlNode convertZeroIfNull(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("ZEROIFNULL expects 1 operand, got {}", call.operandCount());
            return call;
        }
        SqlNode zero = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
        return SqlStdOperatorTable.COALESCE.createCall(
                SqlParserPos.ZERO,
                call.operand(0).accept(this),
                zero
        );
    }

    /**
     * Converts NULLIFZERO to NULLIF(x, 0).
     * Snowflake: NULLIFZERO(column)
     * Trino: NULLIF(column, 0)
     */
    private SqlNode convertNullIfZero(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("NULLIFZERO expects 1 operand, got {}", call.operandCount());
            return call;
        }
        SqlNode zero = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
        return SqlStdOperatorTable.NULLIF.createCall(
                SqlParserPos.ZERO,
                call.operand(0).accept(this),
                zero
        );
    }

    /**
     * Converts SQUARE to x * x.
     * Using multiplication preserves integer type; power(x, 2) returns DOUBLE in Trino.
     */
    private SqlNode convertSquare(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("SQUARE expects 1 operand, got {}", call.operandCount());
            return call;
        }
        SqlNode operand = call.operand(0).accept(this);
        return SqlStdOperatorTable.MULTIPLY.createCall(SqlParserPos.ZERO, operand, operand);
    }

    /**
     * Converts REGEXP_SUBSTR to regexp_extract.
     * Snowflake: REGEXP_SUBSTR(str, pattern [, pos, occurrence, params, group])
     * Trino: regexp_extract(str, pattern)
     */
    private SqlNode convertRegexpSubstr(SqlCall call) {
        if (call.operandCount() < 2) {
            logger.warn("REGEXP_SUBSTR expects at least 2 operands, got {}", call.operandCount());
            return call;
        }
        if (call.operandCount() > 2) {
            logger.warn("REGEXP_SUBSTR with {} args: only first 2 args translated", call.operandCount());
        }
        return buildFunction("regexp_extract",
                call.operand(0).accept(this),
                call.operand(1).accept(this));
    }

    /**
     * Converts CHARINDEX to strpos with swapped argument order.
     * Snowflake: CHARINDEX(substr, str)
     * Trino: strpos(str, substr)
     */
    private SqlNode convertCharIndex(SqlCall call) {
        if (call.operandCount() < 2) {
            logger.warn("CHARINDEX expects at least 2 operands, got {}", call.operandCount());
            return call;
        }
        if (call.operandCount() > 2) {
            logger.warn("CHARINDEX with start_pos argument is not fully supported; start_pos ignored");
        }
        // Snowflake CHARINDEX(substr, str) → Trino strpos(str, substr): args are swapped
        return buildFunction("strpos",
                call.operand(1).accept(this),
                call.operand(0).accept(this));
    }

    /**
     * Converts ARRAY_SIZE to cardinality.
     * Snowflake: ARRAY_SIZE(arr)
     * Trino: cardinality(arr)
     */
    private SqlNode convertArraySize(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("ARRAY_SIZE expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildFunction("cardinality", call.operand(0).accept(this));
    }

    /**
     * Converts ARRAY_CONTAINS to contains with swapped argument order.
     * Snowflake: ARRAY_CONTAINS(value, arr)
     * Trino: contains(arr, value)
     */
    private SqlNode convertArrayContains(SqlCall call) {
        if (call.operandCount() != 2) {
            logger.warn("ARRAY_CONTAINS expects 2 operands, got {}", call.operandCount());
            return call;
        }
        // Snowflake ARRAY_CONTAINS(value, arr) → Trino contains(arr, value): args are swapped
        return buildFunction("contains",
                call.operand(1).accept(this),
                call.operand(0).accept(this));
    }

    /**
     * Converts PARSE_JSON to json_parse.
     * Snowflake: PARSE_JSON(str)
     * Trino: json_parse(str)
     */
    private SqlNode convertParseJson(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("PARSE_JSON expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildFunction("json_parse", call.operand(0).accept(this));
    }

    /**
     * Converts BOOLAND_AGG to bool_and.
     * Snowflake: BOOLAND_AGG(x)
     * Trino: bool_and(x)
     */
    private SqlNode convertBoolAndAgg(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("BOOLAND_AGG expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildFunction("bool_and", call.operand(0).accept(this));
    }

    /**
     * Converts BOOLOR_AGG to bool_or.
     * Snowflake: BOOLOR_AGG(x)
     * Trino: bool_or(x)
     */
    private SqlNode convertBoolOrAgg(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("BOOLOR_AGG expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildFunction("bool_or", call.operand(0).accept(this));
    }

    /**
     * Converts DECODE to a searched CASE expression.
     * Snowflake: DECODE(expr, s1, r1, s2, r2, ..., default)
     * Trino: CASE WHEN expr = s1 THEN r1 WHEN expr = s2 THEN r2 ... ELSE default END
     */
    private SqlNode convertDecode(SqlCall call) {
        int operandCount = call.operandCount();
        if (operandCount < 3) {
            logger.warn("DECODE expects at least 3 operands, got {}", operandCount);
            return call;
        }

        SqlNode expr = call.operand(0).accept(this);

        // Remaining operands after expr; if odd count, last one is the default/else
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

    /**
     * Converts TO_VARCHAR / TO_CHAR to CAST(x AS VARCHAR).
     * Snowflake: TO_VARCHAR(x) or TO_CHAR(x)
     * Trino: CAST(x AS VARCHAR)
     */
    private SqlNode convertToVarchar(SqlCall call) {
        if (call.operandCount() < 1) {
            logger.warn("TO_VARCHAR/TO_CHAR expects at least 1 operand, got {}", call.operandCount());
            return call;
        }
        if (call.operandCount() > 1) {
            logger.warn("TO_VARCHAR/TO_CHAR with format argument is not fully supported; format ignored");
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

    /**
     * Converts DIV0 to IF(y = 0, CAST(0 AS DOUBLE), CAST(x AS DOUBLE) / y).
     * Using DOUBLE to match Snowflake's floating-point return type for DIV0.
     */
    private SqlNode convertDiv0(SqlCall call) {
        if (call.operandCount() != 2) {
            logger.warn("DIV0 expects 2 operands, got {}", call.operandCount());
            return call;
        }
        SqlNode x = call.operand(0).accept(this);
        SqlNode y = call.operand(1).accept(this);
        SqlNode zero = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
        SqlNode zeroCast = buildCast(zero, SqlTypeName.DOUBLE);
        SqlNode xCast = buildCast(x, SqlTypeName.DOUBLE);

        SqlNode condition = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, y, zero);
        SqlNode division = SqlStdOperatorTable.DIVIDE.createCall(SqlParserPos.ZERO, xCast, y);

        return buildFunction("IF", condition, zeroCast, division);
    }

    /**
     * Converts STRTOK to split_part.
     * Snowflake: STRTOK(str, delimiters, part_number)
     * Trino: split_part(str, delimiter, part_number)
     */
    private SqlNode convertStrtok(SqlCall call) {
        if (call.operandCount() != 3) {
            logger.warn("STRTOK expects 3 operands, got {}", call.operandCount());
            return call;
        }
        return buildFunction("split_part",
                call.operand(0).accept(this),
                call.operand(1).accept(this),
                call.operand(2).accept(this));
    }

    // ── Tier-1 high-frequency converter methods ─────────────────────────────

    /**
     * NVL2(expr, not_null_val, null_val) → IF(expr IS NOT NULL, not_null_val, null_val)
     */
    private SqlNode convertNvl2(SqlCall call) {
        if (call.operandCount() != 3) {
            logger.warn("NVL2 expects 3 operands, got {}", call.operandCount());
            return call;
        }
        SqlNode expr = call.operand(0).accept(this);
        SqlNode notNullVal = call.operand(1).accept(this);
        SqlNode nullVal = call.operand(2).accept(this);
        SqlNode isNotNull = SqlStdOperatorTable.IS_NOT_NULL.createCall(SqlParserPos.ZERO, expr);
        return buildFunction("IF", isNotNull, notNullVal, nullVal);
    }

    /**
     * LEFT(str, n) → substr(str, 1, n)
     */
    private SqlNode convertLeft(SqlCall call) {
        if (call.operandCount() != 2) {
            logger.warn("LEFT expects 2 operands, got {}", call.operandCount());
            return call;
        }
        return buildFunction("substr",
                call.operand(0).accept(this),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
                call.operand(1).accept(this));
    }

    /**
     * RIGHT(str, n) → substr(str, length(str) - n + 1, n)
     */
    private SqlNode convertRight(SqlCall call) {
        if (call.operandCount() != 2) {
            logger.warn("RIGHT expects 2 operands, got {}", call.operandCount());
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

    /**
     * STARTSWITH(str, prefix) → starts_with(str, prefix)
     */
    private SqlNode convertStartsWith(SqlCall call) {
        if (call.operandCount() != 2) {
            logger.warn("STARTSWITH expects 2 operands, got {}", call.operandCount());
            return call;
        }
        return buildFunction("starts_with", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /**
     * ENDSWITH(str, suffix) → SUBSTR(str, LENGTH(str) - LENGTH(suffix) + 1) = suffix
     * Using a SQL-standard expression instead of ends_with() for compatibility with older Trino.
     */
    private SqlNode convertEndsWith(SqlCall call) {
        if (call.operandCount() != 2) {
            logger.warn("ENDSWITH expects 2 operands, got {}", call.operandCount());
            return call;
        }
        SqlNode str = call.operand(0).accept(this);
        SqlNode suffix = call.operand(1).accept(this);
        SqlNode one = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlNode lenStr = buildFunction("length", str);
        SqlNode lenSuffix = buildFunction("length", suffix);
        SqlNode start = SqlStdOperatorTable.PLUS.createCall(SqlParserPos.ZERO,
                SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, lenStr, lenSuffix),
                one);
        SqlNode sub = buildFunction("substr", str, start);
        return SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, sub, suffix);
    }

    /**
     * CONTAINS(str, sub) → strpos(str, sub) > 0
     * Note: this handles the Snowflake string CONTAINS function, distinct from ARRAY_CONTAINS.
     */
    private SqlNode convertContains(SqlCall call) {
        if (call.operandCount() != 2) {
            logger.warn("CONTAINS expects 2 operands, got {}", call.operandCount());
            return call;
        }
        SqlNode strpos = buildFunction("strpos", call.operand(0).accept(this), call.operand(1).accept(this));
        return SqlStdOperatorTable.GREATER_THAN.createCall(SqlParserPos.ZERO,
                strpos, SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO));
    }

    /**
     * INSTR(str, sub) → strpos(str, sub)
     */
    private SqlNode convertInstr(SqlCall call) {
        if (call.operandCount() < 2) {
            logger.warn("INSTR expects at least 2 operands, got {}", call.operandCount());
            return call;
        }
        return buildFunction("strpos", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /**
     * SYSDATE() / GETDATE() → now()
     */
    private SqlNode convertSysdate(SqlCall call) {
        return buildFunction("now");
    }

    /**
     * LAST_DAY(date) → last_day_of_month(date)
     */
    private SqlNode convertLastDay(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("LAST_DAY expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildFunction("last_day_of_month", call.operand(0).accept(this));
    }

    /**
     * TO_TIME(str) → CAST(str AS TIME)
     */
    private SqlNode convertToTime(SqlCall call) {
        if (call.operandCount() < 1) {
            logger.warn("TO_TIME expects at least 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildCast(call.operand(0).accept(this), SqlTypeName.TIME);
    }

    /**
     * TO_NUMBER(str) / TO_NUMERIC(str) / TO_DECIMAL(str) → CAST(str AS DECIMAL)
     */
    private SqlNode convertToDecimal(SqlCall call) {
        if (call.operandCount() < 1) {
            logger.warn("TO_NUMBER/TO_NUMERIC/TO_DECIMAL expects at least 1 operand, got {}", call.operandCount());
            return call;
        }
        if (call.operandCount() > 1) {
            logger.warn("TO_NUMBER/TO_NUMERIC/TO_DECIMAL with precision/scale args: using simple CAST(x AS DECIMAL)");
        }
        return buildCast(call.operand(0).accept(this), SqlTypeName.DECIMAL);
    }

    /**
     * TO_DOUBLE(str) → CAST(str AS DOUBLE)
     */
    private SqlNode convertToDouble(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("TO_DOUBLE expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildCast(call.operand(0).accept(this), SqlTypeName.DOUBLE);
    }

    /**
     * TO_BOOLEAN(str) → CAST(str AS BOOLEAN)
     */
    private SqlNode convertToBoolean(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("TO_BOOLEAN expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildCast(call.operand(0).accept(this), SqlTypeName.BOOLEAN);
    }

    /**
     * APPROX_COUNT_DISTINCT(x) → approx_distinct(x)
     */
    private SqlNode convertApproxCountDistinct(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("APPROX_COUNT_DISTINCT expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildFunction("approx_distinct", call.operand(0).accept(this));
    }

    /**
     * MEDIAN(x) → approx_percentile(x, 0.5)
     */
    private SqlNode convertMedian(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("MEDIAN expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildFunction("approx_percentile",
                call.operand(0).accept(this),
                SqlLiteral.createExactNumeric("0.5", SqlParserPos.ZERO));
    }

    /**
     * OBJECT_AGG(key, value) → map_agg(key, value)
     */
    private SqlNode convertObjectAgg(SqlCall call) {
        if (call.operandCount() != 2) {
            logger.warn("OBJECT_AGG expects 2 operands, got {}", call.operandCount());
            return call;
        }
        return buildFunction("map_agg", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /**
     * ARRAY_CONCAT(arr1, arr2, ...) / ARRAY_CAT(arr1, arr2) → concat(arr1, arr2, ...)
     */
    private SqlNode convertArrayConcat(SqlCall call) {
        if (call.operandCount() < 2) {
            logger.warn("ARRAY_CONCAT/ARRAY_CAT expects at least 2 operands, got {}", call.operandCount());
            return call;
        }
        SqlNode[] operands = new SqlNode[call.operandCount()];
        for (int i = 0; i < call.operandCount(); i++) {
            operands[i] = call.operand(i).accept(this);
        }
        return buildFunction("concat", operands);
    }

    /**
     * ARRAY_TO_STRING(arr, delim) → array_join(arr, delim)
     */
    private SqlNode convertArrayToString(SqlCall call) {
        if (call.operandCount() < 2) {
            logger.warn("ARRAY_TO_STRING expects at least 2 operands, got {}", call.operandCount());
            return call;
        }
        return buildFunction("array_join", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /**
     * BASE64_ENCODE(str) → to_base64(to_utf8(str))
     */
    private SqlNode convertBase64Encode(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("BASE64_ENCODE expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildFunction("to_base64", buildFunction("to_utf8", call.operand(0).accept(this)));
    }

    /**
     * BASE64_DECODE_STRING(str) → from_utf8(from_base64(str))
     */
    private SqlNode convertBase64DecodeString(SqlCall call) {
        if (call.operandCount() != 1) {
            logger.warn("BASE64_DECODE_STRING expects 1 operand, got {}", call.operandCount());
            return call;
        }
        return buildFunction("from_utf8", buildFunction("from_base64", call.operand(0).accept(this)));
    }

    // ── Window converter methods ─────────────────────────────────────────────

    /**
     * RATIO_TO_REPORT(x) OVER (w) → x / SUM(x) OVER (w)
     * Snowflake: RATIO_TO_REPORT(sales) OVER (PARTITION BY region)
     * Trino:     sales / SUM(sales) OVER (PARTITION BY region)
     *
     * Window calls in Calcite are SqlBasicCall(OVER, [aggCall, window]).
     */
    private SqlNode convertRatioToReport(SqlCall aggCall, SqlWindow window) {
        SqlNode x = aggCall.operand(0).accept(this);
        // Build SUM(x)
        SqlNode sumCall = buildFunction("SUM", x);
        // Build SUM(x) OVER (w)
        SqlNode sumOver = SqlStdOperatorTable.OVER.createCall(SqlParserPos.ZERO, sumCall, window);
        // Return x / SUM(x) OVER (w)
        return SqlStdOperatorTable.DIVIDE.createCall(SqlParserPos.ZERO, x, sumOver);
    }

    // ── Date/Time converter methods ──────────────────────────────────────────

    /** DAYOFWEEK(date) → day_of_week(date) */
    private SqlNode convertDayOfWeek(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("DAYOFWEEK expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("day_of_week", call.operand(0).accept(this));
    }

    /** DAYOFYEAR(date) → day_of_year(date) */
    private SqlNode convertDayOfYear(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("DAYOFYEAR expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("day_of_year", call.operand(0).accept(this));
    }

    /** WEEKOFYEAR(date) → week_of_year(date) */
    private SqlNode convertWeekOfYear(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("WEEKOFYEAR expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("week_of_year", call.operand(0).accept(this));
    }

    /**
     * ADD_MONTHS(date, n) → date_add('month', n, date)
     * Snowflake: ADD_MONTHS(date, n)
     * Trino: date_add('month', n, date)
     */
    private SqlNode convertAddMonths(SqlCall call) {
        if (call.operandCount() != 2) { logger.warn("ADD_MONTHS expects 2 operands, got {}", call.operandCount()); return call; }
        return buildFunction("date_add",
                SqlLiteral.createCharString("month", SqlParserPos.ZERO),
                call.operand(1).accept(this),
                call.operand(0).accept(this));
    }

    /** MONTHNAME(date) → format_datetime(date, 'MMMM') */
    private SqlNode convertMonthName(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("MONTHNAME expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("format_datetime",
                call.operand(0).accept(this),
                SqlLiteral.createCharString("MMMM", SqlParserPos.ZERO));
    }

    /** DAYNAME(date) → format_datetime(date, 'EEEE') */
    private SqlNode convertDayName(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("DAYNAME expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("format_datetime",
                call.operand(0).accept(this),
                SqlLiteral.createCharString("EEEE", SqlParserPos.ZERO));
    }

    // ── Bitwise converter methods ────────────────────────────────────────────

    /** BITAND(x, y) → bitwise_and(x, y) */
    private SqlNode convertBitAnd(SqlCall call) {
        if (call.operandCount() != 2) { logger.warn("BITAND expects 2 operands, got {}", call.operandCount()); return call; }
        return buildFunction("bitwise_and", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** BITOR(x, y) → bitwise_or(x, y) */
    private SqlNode convertBitOr(SqlCall call) {
        if (call.operandCount() != 2) { logger.warn("BITOR expects 2 operands, got {}", call.operandCount()); return call; }
        return buildFunction("bitwise_or", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** BITXOR(x, y) → bitwise_xor(x, y) */
    private SqlNode convertBitXor(SqlCall call) {
        if (call.operandCount() != 2) { logger.warn("BITXOR expects 2 operands, got {}", call.operandCount()); return call; }
        return buildFunction("bitwise_xor", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** BITNOT(x) → bitwise_not(x) */
    private SqlNode convertBitNot(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("BITNOT expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("bitwise_not", call.operand(0).accept(this));
    }

    /** BITSHIFTLEFT(x, n) → bitwise_left_shift(x, n) */
    private SqlNode convertBitShiftLeft(SqlCall call) {
        if (call.operandCount() != 2) { logger.warn("BITSHIFTLEFT expects 2 operands, got {}", call.operandCount()); return call; }
        return buildFunction("bitwise_left_shift", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** BITSHIFTRIGHT(x, n) → bitwise_right_shift(x, n) */
    private SqlNode convertBitShiftRight(SqlCall call) {
        if (call.operandCount() != 2) { logger.warn("BITSHIFTRIGHT expects 2 operands, got {}", call.operandCount()); return call; }
        return buildFunction("bitwise_right_shift", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    // ── String / Hash converter methods ─────────────────────────────────────

    /** EDITDISTANCE(s1, s2) → levenshtein_distance(s1, s2) */
    private SqlNode convertEditDistance(SqlCall call) {
        if (call.operandCount() != 2) { logger.warn("EDITDISTANCE expects 2 operands, got {}", call.operandCount()); return call; }
        return buildFunction("levenshtein_distance", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** SPACE(n) → rpad('', n, ' ') — repeat() in Trino resolves to the array version for char literals */
    private SqlNode convertSpace(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("SPACE expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("rpad",
                SqlLiteral.createCharString("", SqlParserPos.ZERO),
                call.operand(0).accept(this),
                SqlLiteral.createCharString(" ", SqlParserPos.ZERO));
    }

    /** TRUNC(x) / TRUNC(x, scale) → truncate(x) / truncate(x, scale) */
    private SqlNode convertTrunc(SqlCall call) {
        if (call.operandCount() == 1) {
            return buildFunction("truncate", call.operand(0).accept(this));
        }
        return buildFunction("truncate", call.operand(0).accept(this), call.operand(1).accept(this));
    }

    /** CEIL(x) / CEIL(x, scale) → ceil(x)  [scale dropped: Trino does not support it] */
    private SqlNode convertCeil(SqlCall call) {
        return buildFunction("ceil", call.operand(0).accept(this));
    }

    /** FLOOR(x) / FLOOR(x, scale) → floor(x)  [scale dropped: Trino does not support it] */
    private SqlNode convertFloor(SqlCall call) {
        return buildFunction("floor", call.operand(0).accept(this));
    }

    /**
     * DATE_FROM_PARTS(year, month, day) → date(format('%04d-%02d-%02d', year, month, day))
     */
    private SqlNode convertDateFromParts(SqlCall call) {
        if (call.operandCount() != 3) {
            logger.warn("DATE_FROM_PARTS expects 3 operands, got {}", call.operandCount());
            return call;
        }
        return buildFunction("date",
                buildFunction("format",
                        SqlLiteral.createCharString("%04d-%02d-%02d", SqlParserPos.ZERO),
                        call.operand(0).accept(this),
                        call.operand(1).accept(this),
                        call.operand(2).accept(this)));
    }

    /** MD5(str) → lower(to_hex(md5(to_utf8(str)))) */
    private SqlNode convertMd5(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("MD5 expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("lower",
                buildFunction("to_hex",
                        buildFunction("md5",
                                buildFunction("to_utf8", call.operand(0).accept(this)))));
    }

    /** SHA1(str) → lower(to_hex(sha1(to_utf8(str)))) */
    private SqlNode convertSha1(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("SHA1 expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("lower",
                buildFunction("to_hex",
                        buildFunction("sha1",
                                buildFunction("to_utf8", call.operand(0).accept(this)))));
    }

    /**
     * SHA2(str[, bits]) → lower(to_hex(sha256|sha512(to_utf8(str))))
     * Default is 256 bits; 512 also supported.
     */
    private SqlNode convertSha2(SqlCall call) {
        if (call.operandCount() < 1 || call.operandCount() > 2) {
            logger.warn("SHA2 expects 1 or 2 operands, got {}", call.operandCount());
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
                    logger.warn("SHA2 with bits={}: only 256 and 512 supported, defaulting to sha256", bitsStr);
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
        if (call.operandCount() != 1) { logger.warn("HEX_ENCODE expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("lower",
                buildFunction("to_hex",
                        buildFunction("to_utf8", call.operand(0).accept(this))));
    }

    /** HEX_DECODE_STRING(str) → from_utf8(from_hex(str)) */
    private SqlNode convertHexDecodeString(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("HEX_DECODE_STRING expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("from_utf8",
                buildFunction("from_hex", call.operand(0).accept(this)));
    }

    // ── URL converter methods ────────────────────────────────────────────────

    /** URL_ENCODE(str) → url_encode(str) */
    private SqlNode convertUrlEncode(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("URL_ENCODE expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("url_encode", call.operand(0).accept(this));
    }

    /** URL_DECODE(str) → url_decode(str) */
    private SqlNode convertUrlDecode(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("URL_DECODE expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("url_decode", call.operand(0).accept(this));
    }

    // ── Aggregate converter methods ──────────────────────────────────────────

    /** ANY_VALUE(x) → arbitrary(x) */
    private SqlNode convertAnyValue(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("ANY_VALUE expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("arbitrary", call.operand(0).accept(this));
    }

    /** BITAND_AGG(x) → bitwise_and_agg(x) */
    private SqlNode convertBitAndAgg(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("BITAND_AGG expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("bitwise_and_agg", call.operand(0).accept(this));
    }

    /** BITOR_AGG(x) → bitwise_or_agg(x) */
    private SqlNode convertBitOrAgg(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("BITOR_AGG expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("bitwise_or_agg", call.operand(0).accept(this));
    }

    /** BITXOR_AGG(x) → bitwise_xor_agg(x) */
    private SqlNode convertBitXorAgg(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("BITXOR_AGG expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("bitwise_xor_agg", call.operand(0).accept(this));
    }

    /**
     * SKEW(x) → adjusted Fisher-Pearson sample skewness.
     * Snowflake uses G1 = SKEWNESS * SQRT(n*(n-1)) / (n-2), where n = COUNT(x).
     * Trino's SKEWNESS computes the biased population skewness, so we apply the correction factor.
     * For n <= 2, returns NULL (matching Snowflake's behaviour).
     */
    private SqlNode convertSkew(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("SKEW expects 1 operand, got {}", call.operandCount()); return call; }
        SqlNode arg = call.operand(0).accept(this);

        // SKEWNESS(x)
        SqlNode skewness = buildFunction("skewness", arg);

        // Three separate CAST(COUNT(x) AS DOUBLE) nodes for n1, n2, n3
        SqlNode n1 = buildCast(buildFunction("count", arg), SqlTypeName.DOUBLE);
        SqlNode n2 = buildCast(buildFunction("count", arg), SqlTypeName.DOUBLE);
        SqlNode n3 = buildCast(buildFunction("count", arg), SqlTypeName.DOUBLE);

        // SQRT(n * (n - 1))
        SqlNode nMinus1 = SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, n1,
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
        SqlNode sqrt = buildFunction("sqrt",
                SqlStdOperatorTable.MULTIPLY.createCall(SqlParserPos.ZERO, n2, nMinus1));

        // SKEWNESS(x) * SQRT(n*(n-1)) / (n - 2)
        SqlNode numerator = SqlStdOperatorTable.MULTIPLY.createCall(SqlParserPos.ZERO, skewness, sqrt);
        SqlNode nMinus2 = SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, n3,
                SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO));
        SqlNode formula = SqlStdOperatorTable.DIVIDE.createCall(SqlParserPos.ZERO, numerator, nMinus2);

        // CASE WHEN COUNT(x) <= 2 THEN NULL ELSE formula END
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

    /**
     * BOOLXOR_AGG(x) → (count_if(x) % 2) = 1
     * Returns true if an odd number of input values are true.
     */
    private SqlNode convertBoolXorAgg(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("BOOLXOR_AGG expects 1 operand, got {}", call.operandCount()); return call; }
        SqlNode countIf = buildFunction("count_if", call.operand(0).accept(this));
        SqlNode two = SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO);
        SqlNode one = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlNode mod = SqlStdOperatorTable.MOD.createCall(SqlParserPos.ZERO, countIf, two);
        return SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, mod, one);
    }

    // ── Array converter methods ──────────────────────────────────────────────

    /**
     * ARRAY_SLICE(arr, from, to) → slice(arr, from+1, to-from)
     * Snowflake uses 0-indexed [from, to) semantics; Trino slice uses 1-indexed (start, length).
     */
    private SqlNode convertArraySlice(SqlCall call) {
        if (call.operandCount() != 3) { logger.warn("ARRAY_SLICE expects 3 operands, got {}", call.operandCount()); return call; }
        SqlNode arr  = call.operand(0).accept(this);
        SqlNode from = call.operand(1).accept(this);
        SqlNode to   = call.operand(2).accept(this);
        SqlNode one  = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        // start = from + 1
        SqlNode start = SqlStdOperatorTable.PLUS.createCall(SqlParserPos.ZERO, from, one);
        // length = to - from
        SqlNode length = SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, to, from);
        return buildFunction("slice", arr, start, length);
    }

    /** ARRAY_FLATTEN(arr) → flatten(arr) */
    private SqlNode convertArrayFlatten(SqlCall call) {
        if (call.operandCount() != 1) { logger.warn("ARRAY_FLATTEN expects 1 operand, got {}", call.operandCount()); return call; }
        return buildFunction("flatten", call.operand(0).accept(this));
    }

    /**
     * Converts a date-part node (the unit argument of DATEADD / DATEDIFF) into a
     * single-quoted Trino string literal, e.g. 'day', 'month', 'year'.
     *
     * The Babel parser may represent the unit as:
     *   - SqlIdentifier  (e.g. "day" parsed as a plain name)
     *   - SqlLiteral / SYMBOL  (e.g. DAY parsed as a time-unit keyword)
     *   - SqlIntervalQualifier  (e.g. as part of INTERVAL syntax)
     *   - SqlCharStringLiteral (already quoted – keep as-is)
     */
    private SqlNode toDatePartLiteral(SqlNode node) {
        if (node instanceof SqlCharStringLiteral) {
            return node; // already 'day' style
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
        // fallback: render to string and wrap in quotes
        return SqlLiteral.createCharString(node.toString().toLowerCase(), SqlParserPos.ZERO);
    }

    /**
     * Converts Snowflake date format strings to Trino format.
     */
    private String convertDateFormat(String snowflakeFormat) {
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
}