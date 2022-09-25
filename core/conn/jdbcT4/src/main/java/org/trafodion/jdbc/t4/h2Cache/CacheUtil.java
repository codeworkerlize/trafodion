package org.trafodion.jdbc.t4.h2Cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.LoggerFactory;

public class CacheUtil {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(CacheUtil.class);
    private static final String JDBC_URL = "jdbc:h2:mem:memdb%d;DB_CLOSE_DELAY=-1;MODE=Oracle";
    private static final String USER = "sa";
    private static final String PASSWORD = "";
    private static final String DRIVER_CLASS = "org.h2.Driver";
    private static AtomicInteger id = new AtomicInteger();

    public static String transDDL(String tableName, String ddl) {
        String ddlString = ddl.replaceAll("\\s+", " ");
        String finalSQL = ddlString.replaceAll("NO\\s+DEFAULT|NOT\\s+DROPPABLE|NOT\\s+SERIALIZED"
                        + "|\\s+DEFAULT\\s+NULL"
                        + "|\\s+CHARACTER\\s+SET\\s+ISO88591"
                        + "|\\s+CHARACTER\\s+SET\\s+UCS2"
                        + "|\\s+CHARACTER\\s+SET\\s+UTF8"
                        + "|\\s+COLLATE"
                        + "|\\s+BYTES"
                        + "|\\s+NULLABLE"
                , "")
                .replaceAll("\\s+LARGEINT", " BIGINT")
                .replaceAll("\\s+CHARS\\s*\\)", ")")
                .replaceAll("\\s+NOT NULL", "")
                .replaceAll("\\s+", " ");

        String[] ddlarry = finalSQL.split(" , ");
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < ddlarry.length; i++){
            if(ddlarry[i].contains("DEFAULT")){
                ddlarry[i] = ddlarry[i].replaceAll("\\s+DEFAULT+.*", "");
            }
            sb.append(ddlarry[i]);
            if(i != ddlarry.length - 1){
                sb.append(",");
            }
        }
        if(!sb.toString().contains("PRIMARY KEY")){
            sb.append(")");
        }
        return "create table IF NOT EXISTS " + tableName + "(" + sb.toString();
    }

    public static String genKey(String sql) {
        return sql.replaceAll("\\s+", "").toUpperCase();
    }

    public static Connection getH2Connection() {
        Connection connection;
        try {
            Class.forName(DRIVER_CLASS);
            connection = DriverManager.getConnection(String.format(JDBC_URL, id.incrementAndGet()));
        } catch (Exception e) {
            connection = null;
            LOG.warn("Failed to get H2 connection : {}", e.getMessage());
        }
        return connection;
    }
}
