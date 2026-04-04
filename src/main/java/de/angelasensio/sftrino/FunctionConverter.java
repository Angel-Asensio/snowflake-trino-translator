package de.angelasensio.sftrino;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

/**
 * Strategy for converting a single Snowflake function call to a Trino-compatible SqlNode.
 *
 * <p>Register implementations via {@link SnowflakeToTrinoConverter#register(String, FunctionConverter)}.
 * The {@code ctx} parameter provides access to helper factories ({@link SnowflakeToTrinoConverter#buildFunction},
 * {@link SnowflakeToTrinoConverter#buildCast}, etc.) and the visitor itself so nested operands can be
 * recursively translated with {@code operand.accept(ctx)}.
 */
@FunctionalInterface
public interface FunctionConverter {
    SqlNode convert(SqlCall call, SnowflakeToTrinoConverter ctx);
}
