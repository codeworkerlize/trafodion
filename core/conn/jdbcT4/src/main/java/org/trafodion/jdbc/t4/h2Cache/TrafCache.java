package org.trafodion.jdbc.t4.h2Cache;

import com.sun.rowset.CachedRowSetImpl;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.T4LoggingUtilities;
import org.trafodion.jdbc.t4.T4Properties;
import org.trafodion.jdbc.t4.TrafT4Connection;

public class TrafCache {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafCache.class);

    private Set<String> cachedTableNameSet = new HashSet<String>();
    private Set<String> configuredTableNameSet = new HashSet<String>();

    private Connection conn_ = null;
    private boolean forceDisable = false;

    private final Object locker = new Object();
    //SQL && prepare
    private ConcurrentHashMap<String, PreparedStatement> cachedPreparedStatement = new ConcurrentHashMap<String, PreparedStatement>();

    private final int MAX_CACHE_ROWS = 1000;

    public Vector<TrafT4Connection> getTrafT4ConnectionVector() {
        return trafT4ConnectionVector;
    }

    private Vector<TrafT4Connection> trafT4ConnectionVector;


    public TrafCache(T4Properties t4props) {
        if (t4props.isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(t4props, Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        Statement statement;
        try {
            conn_ = CacheUtil.getH2Connection();
            statement = conn_.createStatement();
            statement.execute(
                    "CREATE ALIAS to_number FOR \"org.trafodion.jdbc.t4.function.CacheFun.to_number\"");
            trafT4ConnectionVector = new Vector<TrafT4Connection>();
        } catch (Exception e) {
            if (t4props.isLogEnable(Level.WARNING)) {
                T4LoggingUtilities.log(t4props, Level.WARNING,
                        "ENTRY. Initialize H2 error:" + e.getMessage(), e);
            }
            if (LOG.isWarnEnabled()) {
                LOG.warn("ENTRY. Initialize H2 error: {}", e.getMessage(), e);
            }
            forceDisable = true;
        }
    }


    public void cacheTableName(T4Properties t4props) throws Exception {
        String cacheTable = t4props.getCacheTable();
        if (t4props.isLogEnable(Level.INFO)) {
            T4LoggingUtilities.log(t4props, Level.INFO,
                    "ENTRY. cacheTable is " + cacheTable + " and forceDisable is " + forceDisable);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("ENTRY. cacheTable is <{}> or forceDisable is <{}>", cacheTable,
                    forceDisable);
        }
        if (cacheTable == null || forceDisable) {
            return;
        }
        try {
            //add table name to HashSet
            if (cacheTable.contains(",")) {
                String[] names = cacheTable.split(",");
                for (String name : names) {
                    if (name.contains(".")) {
                        configuredTableNameSet
                                .add(name.substring(name.lastIndexOf(".") + 1).toUpperCase());
                    } else {
                        configuredTableNameSet.add(name.toUpperCase());
                    }
                }
            } else {
                if (!configuredTableNameSet.contains(cacheTable.toUpperCase())) {
                    configuredTableNameSet.add(cacheTable.toUpperCase());
                }
            }
            if (t4props.isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(t4props, Level.FINER,
                        "ENTRY. configuredTableNameSet : " + configuredTableNameSet);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("ENTRY. configuredTableNameSet : {}", configuredTableNameSet);
            }
        } catch (Exception e) {
            if (t4props.isLogEnable(Level.WARNING)) {
                T4LoggingUtilities.log(t4props, Level.WARNING,
                        "ENTRY. Cache Table Name error: " + e.getMessage()
                                + ",forceDisable will be true", e);
            }
            if (LOG.isWarnEnabled()) {
                LOG.warn("ENTRY. Cache Table Name error: {},forceDisable will be true",
                        e.getMessage(), e);
            }
            throw e;
        }
    }

    public void cacheTablesInit(Connection srcConn, T4Properties t4props) throws SQLException {
        Statement st = null;
        try {
            st = srcConn.createStatement();
            for (String tableName : configuredTableNameSet) {
                if (cachedTableNameSet.contains(tableName)) {
                    continue;
                }
                cacheSingleTable(tableName, st, srcConn, t4props);
            }
            if (t4props.isLogEnable(Level.INFO)) {
                T4LoggingUtilities.log(t4props, Level.INFO,
                        "ENTRY. cachedTableNameSet : " + cachedTableNameSet);
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("ENTRY. cachedTableNameSet : {}", cachedTableNameSet);
            }
        } catch (SQLException e) {
            if (t4props.isLogEnable(Level.WARNING)) {
                T4LoggingUtilities.log(t4props, Level.WARNING,
                        "ENTRY. Cache Tables Init error: " + e.getMessage()
                                + ",forceDisable will be true");
            }
            if (LOG.isWarnEnabled()) {
                LOG.warn("ENTRY. Cache Tables Init error: {},forceDisable will be true",
                        e.getMessage());
            }
            throw e;
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException e) {
                LOG.error("Close statement error: ", e);
                e.printStackTrace();
            }
        }
    }

    private void cacheSingleTable(String tableName, Statement srcStatement,
            Connection srcConn, T4Properties t4props) throws SQLException {
        ResultSet rs = null;
        try {
            // get ddl
            srcStatement.execute("SHOWDDL " + tableName);
            rs = srcStatement.getResultSet();
            String line = "";
            boolean needed = false;
            StringBuilder ddl = new StringBuilder();
            while (rs.next()) {
                line = rs.getString(1);
                if (needed) {
                    ddl.append(line);
                }
                if (line.matches("\\s*\\(\\s*")) {
                    needed = true;
                } else if (line.matches("\\s*\\)\\s*")) {
                    break;
                }
            }
            String ddlSql = CacheUtil.transDDL(tableName, ddl.toString());
            if (t4props.isLogEnable(Level.FINER)) {
                T4LoggingUtilities
                        .log(t4props, Level.FINER, "ENTRY. Cache Single Table DDL: " + ddlSql);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("ENTRY. Cache Single Table DDL:<{}>", ddlSql);
            }
            Statement statement = null;
            String dropDDL = "DROP TABLE IF EXISTS " + tableName;
            boolean isDropped = false;
            try {
                statement = conn_.createStatement();
                //do ddl
                statement.execute(ddlSql);
                boolean wasCached;
                synchronized (locker){
                    wasCached = loadData(tableName, srcConn, conn_, t4props);
                }
                if (wasCached) {
                    cachedTableNameSet.add(tableName);
                } else {
                    if (t4props.isLogEnable(Level.FINER)) {
                        T4LoggingUtilities.log(t4props, Level.FINER,
                                "ENTRY. Load Data Fail, and Drop table: " + tableName);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ENTRY. Load Data Fail, and Drop table: {}", tableName);
                    }
                    //Drop table
                    statement.execute(dropDDL);
                    isDropped = true;
                }
            } catch (SQLException se) {
                if (t4props.isLogEnable(Level.WARNING)) {
                    T4LoggingUtilities.log(t4props, Level.WARNING,
                            "ENTRY. Cache Single Table Error:" + se.getMessage());
                }
                if (LOG.isWarnEnabled()) {
                    LOG.warn("ENTRY. Cache Single Table Error:<{}>", se.getMessage());
                }
                if (!isDropped) {
                    statement.execute(dropDDL);
                }
                throw se;
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    if (t4props.isLogEnable(Level.WARNING)) {
                        T4LoggingUtilities.log(t4props, Level.WARNING,
                                "ENTRY. Close resultset for cache:" + e.getMessage());
                    }
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("ENTRY. Close resultset for cache:{}", e.getMessage());
                    }
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean loadData(String tableName, Connection t4Conn, Connection h2tConn,
            T4Properties t4props)
            throws SQLException {
        Statement srcStatement = null;
        ResultSet t4Rs = null;
        PreparedStatement h2pstmt = null;
        try {
            String select = "SELECT * FROM " + tableName;
            srcStatement = t4Conn.createStatement();
            t4Rs = srcStatement.executeQuery(select);
            StringBuilder insertSQL = new StringBuilder();
            insertSQL.append("insert into ");
            insertSQL.append(tableName);
            insertSQL.append(" values(");
            for (int i = 0; i < t4Rs.getMetaData().getColumnCount(); i++) {
                if (i == 0) {
                    insertSQL.append("?");
                } else {
                    insertSQL.append(",?");
                }
            }
            insertSQL.append(")");
            h2tConn.setAutoCommit(false);
            //delete h2
            Statement h2stmt = h2tConn.createStatement();
            h2stmt.execute("delete from " + tableName);
            // insert into data to H2
            h2pstmt = h2tConn.prepareStatement(insertSQL.toString());
            int n = 1;
            while (t4Rs.next()) {
                int maxcacherows = t4props.getMaxCachedRows() > 0 ? t4props.getMaxCachedRows()
                        : MAX_CACHE_ROWS;
                if (n++ > maxcacherows) {
                    if (t4props.isLogEnable(Level.WARNING)) {
                        T4LoggingUtilities.log(t4props, Level.WARNING,
                                "ENTRY.The number of cached data rows exceeds max cache rows,>>>>>MAX_CACHE_ROWS:"
                                        + maxcacherows + ",>>>>>ROWS:" + n);
                    }
                    if (LOG.isWarnEnabled()) {
                        LOG.warn(
                                "ENTRY.The number of cached data rows exceeds max cache rows,>>>>>MAX_CACHE_ROWS:<{}>,>>>>>ROWS:<{}>",
                                maxcacherows, n);
                    }
                    return false;
                }
                for (int i = 1; i <= t4Rs.getMetaData().getColumnCount(); i++) {
                    h2pstmt.setObject(i, t4Rs.getObject(i));
                }
                h2pstmt.addBatch();

            }
            h2pstmt.executeBatch();
            h2tConn.commit();
        } catch (SQLException e) {
            if (t4props.isLogEnable(Level.WARNING)) {
                T4LoggingUtilities.log(t4props, Level.WARNING,
                        "ENTRY.Load Data Error,tableName is + " + tableName + ",and Error:" + e
                                .getMessage());
            }
            if (LOG.isWarnEnabled()) {
                LOG.warn("ENTRY.Load Data Error,tableName is <{}>,and Error<{}>", tableName,
                        e.getMessage());
            }
            throw e;
        } finally {
            if (t4Rs != null) {
                try {
                    t4Rs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (srcStatement != null) {
                try {
                    srcStatement.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (h2pstmt != null) {
                try {
                    h2pstmt.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean containsPreparedStatement(String sql) {
        return cachedPreparedStatement.containsKey(CacheUtil.genKey(sql));
    }

    private PreparedStatement getPreparedStatement(String sql) {
        return this.cachedPreparedStatement.get(CacheUtil.genKey(sql));
    }

    private String configuredCheck(String sql, T4Properties t4props) {
        if (t4props.isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(t4props, Level.FINER,
                    "ENTRY. Cache Table Configured Check,forceDisable is: " + forceDisable
                            + ",configuredTableNameSet is: " + configuredTableNameSet + ",sql is: "
                            + sql);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "ENTRY. Cache Table Configured Check,forceDisable is: <{}>,configuredTableNameSet is: <{}>,sql is: <{}>",
                    forceDisable, configuredTableNameSet, sql);
        }
        if (forceDisable) {
            return null;
        }
        if (configuredTableNameSet.size() == 0) {
            return "";
        }
        for (String tableName : configuredTableNameSet) {
            boolean exists = sql.toUpperCase().contains(tableName);
            if (exists) {
                return tableName;
            }
        }
        return null;
    }

    private void cachePreparedStatement(String sql, PreparedStatement proxyPs, T4Properties t4props)
            throws SQLException {
        String key = CacheUtil.genKey(sql);
        if (this.cachedPreparedStatement.containsKey(key)) {
            try {
                this.cachedPreparedStatement.remove(key).close();
            } catch (SQLException e) {
                if (t4props.isLogEnable(Level.WARNING)) {
                    T4LoggingUtilities.log(t4props, Level.WARNING, "ENTRY.", e.getMessage(), e);
                }
                if (LOG.isWarnEnabled()) {
                    LOG.warn("ENTRY. Error Message is {}", e.getMessage(), e);
                }
            }
        }
        proxyPs.setFetchSize(100000);
        this.cachedPreparedStatement.put(key, proxyPs);
    }

    private static Vector<String> errorSqls = new Vector<String>();

    public boolean checkErrorSql(String sql) {
        return errorSqls.contains(sql.toUpperCase().replaceAll("\\s+", ""));
    }

    public void addErrorSql(String sql) {
        errorSqls.add(sql.toUpperCase().replaceAll("\\s+", ""));
    }

    public void prepareCache(TrafT4Connection srcConn, String sql, int resultSetType,
            int resultSetConcurrency, T4Properties t4props) {
        this.prepareCache(sql, 2, resultSetType, resultSetConcurrency, 0, t4props);
    }

    public void prepareCache(String sql, int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability, T4Properties t4props) {
        this.prepareCache(sql, 3, resultSetType, resultSetConcurrency,
                resultSetHoldability, t4props);
    }

    public PreparedStatement prepareCache(String sql, T4Properties t4props) {
        return this.prepareCache(sql, 0, 0, 0, 0, t4props);
    }

    public PreparedStatement prepareCache(String sql,
            int numOfParams,
            int resultSetType,
            int resultSetConcurrency, int resultSetHoldability, T4Properties t4props) {
        synchronized (locker) {
            if (t4props.isLogEnable(Level.FINER)) {
                T4LoggingUtilities
                        .log(t4props, Level.FINER, "ENTRY. Do Prepare Cache sql is: " + sql);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("ENTRY. Do Prepare Cache sql is <{}>", sql);
            }
            if (!sql.trim().startsWith("select") && !sql.trim().startsWith("SELECT")) {
                return null;
            }
            String tableName = configuredCheck(sql, t4props);
            if (tableName == null || "".equals(tableName)) {
                return null;
            }
            if (!cachedTableNameSet.contains(tableName)) {
                //null means rows in table is over maxCachedRows or Table cache exception
                return null;
            }
            if (containsPreparedStatement(sql)) {
                return getPreparedStatement(sql);
            }
            PreparedStatement proxyPs = null;
            try {
                if (conn_ != null && !checkErrorSql(sql)) {
                    switch (numOfParams) {
                        case 2:
                            proxyPs = conn_
                                    .prepareStatement(sql, resultSetType, resultSetConcurrency);
                            break;
                        case 3:
                            proxyPs = conn_
                                    .prepareStatement(sql, resultSetType, resultSetConcurrency,
                                            resultSetHoldability);
                            break;
                        default:
                            proxyPs = conn_.prepareStatement(sql);
                    }
                    if (t4props.isLogEnable(Level.INFO)) {
                        T4LoggingUtilities.log(t4props, Level.INFO,
                                "ENTRY.Using cache for prepare sql is: " + sql);
                    }
                    if (LOG.isInfoEnabled()) {
                        LOG.info("ENTRY.Using cache for prepare sql is:<{}>", sql);
                    }
                    cachePreparedStatement(sql, proxyPs, t4props);
                } else {
                    if (t4props.isLogEnable(Level.FINER)) {
                        T4LoggingUtilities.log(t4props, Level.FINER,
                                "ENTRY.H2Conn is null or Error Sql, errorSqls:" + errorSqls);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ENTRY.H2Conn is null or Error Sql, errorSqls:<{}>", errorSqls);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (proxyPs != null) {
                    return proxyPs;
                } else {
                    addErrorSql(sql);
                }
            }
            return null;
        }
    }

    public ResultSet executeQuery(String sql,
            Object[] paramsValue_, T4Properties t4props) {
        synchronized (locker) {
            try {
                if (t4props.isLogEnable(Level.FINER)) {
                    T4LoggingUtilities
                            .log(t4props, Level.FINER, "ENTRY. Do Execute Query sql is:" + sql);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ENTRY. Do Execute Query sql is:<{}>", sql);
                }
                String tableName = configuredCheck(sql, t4props);
                if (tableName == null || "".equals(tableName)) {
                    return null;
                }
                //TODO refresh
                if (!cachedTableNameSet.contains(tableName)) {
                    //null means rows in table is over maxCachedRows
                    return null;
                }
                if (!containsPreparedStatement(sql)) {
                    return null;
                }
                PreparedStatement preparedStatement = getPreparedStatement(sql);
                if (paramsValue_ != null) {
                    for (int i = 0; i < paramsValue_.length; i++) {
                        preparedStatement.setObject(i + 1, paramsValue_[i]);
                    }
                }
                ResultSet resultset = null;
                CachedRowSetImpl cachedResultSet = new CachedRowSetImpl();
                try {
                    resultset = preparedStatement.executeQuery();
                    cachedResultSet.populate(resultset);
                } finally {
                    if (resultset != null) {
                        resultset.close();
                    }
                }
                if (t4props.isLogEnable(Level.INFO)) {
                    T4LoggingUtilities
                            .log(t4props, Level.INFO, "ENTRY.Do H2 execute success, sql is:" + sql);
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("ENTRY.Do H2 execute success, sql is:<{}>", sql);
                }
                return cachedResultSet;
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

}
