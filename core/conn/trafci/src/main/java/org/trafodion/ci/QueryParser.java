package org.trafodion.ci;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import org.trafodion.ci.parser.*;

public class QueryParser extends TrafciBaseVisitor<Boolean> {

    public TrafciParser.ProgramContext parserQuery(String sql) throws IOException {
        InputStream input = new ByteArrayInputStream(sql.getBytes("UTF-8"));
        TrafciLexer lexer = new TrafciLexer(new ANTLRInputStream(input));
        lexer.removeErrorListeners();
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        TrafciParser parser = new TrafciParser(tokens);
        parser.removeErrorListeners();
        return parser.program();
    }

    public Boolean match(String sql) throws IOException {
        TrafciParser.ProgramContext ctx = parserQuery(sql);
        if (ctx.create_routine_stmt() != null || ctx.declare_stmt() != null || ctx.begin_stmt() != null)
            return true;
        else 
            return false;
    }

    public String modifySql(String sql) throws IOException {
        TrafciParser.Create_routine_stmtContext ctx = parserQuery(sql).create_routine_stmt();
        if (ctx != null && ctx.T_PACKAGE() != null) {
            int as_stopIndex = ctx.T_AS().getSymbol().getStopIndex();
            sql = sql.substring(0, as_stopIndex + 1) + "//" + sql.substring(as_stopIndex + 1) + "//";
        }
        return sql;
    }
}
