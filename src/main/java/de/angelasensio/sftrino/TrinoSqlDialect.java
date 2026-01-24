package de.angelasensio.sftrino;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PrestoSqlDialect;

/**
 * Custom SQL dialect for Trino.
 * Extends PrestoSqlDialect since Trino is a fork of Presto and shares most syntax.
 */
public class TrinoSqlDialect extends PrestoSqlDialect {
    
    public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(DatabaseProduct.UNKNOWN)
            .withIdentifierQuoteString("\"")
            .withNullCollation(NullCollation.LOW)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withQuotedCasing(Casing.UNCHANGED)
            .withCaseSensitive(false);
    
    public TrinoSqlDialect() {
        super(DEFAULT_CONTEXT);
    }

    @Override
    public boolean supportsCharSet() {
        return false;
    }
    
    @Override
    public boolean supportsAliasedValues() {
        return true;
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        // Custom unparsing logic for Trino-specific constructs can go here
        super.unparseCall(writer, call, leftPrec, rightPrec);
    }
    
    @Override
    public String quoteIdentifier(String val) {
        // Trino uses double quotes for identifiers
        return "\"" + val.replace("\"", "\"\"") + "\"";
    }
    
    @Override
    public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
        // Trino supports NULLS FIRST/LAST natively
        return null;
    }
}