// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.trafodion.jdbc.t4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.PooledConnection;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.h2Cache.TrafCache;
import org.trafodion.jdbc.t4.trace.TraceAverageUsedTime;
import org.trafodion.jdbc.t4.trace.TraceQueryUsedTime;
import org.trafodion.jdbc.t4.h2Cache.TrafCachePool;

/**
 * <p>
 * JDBC Type 4 TrafT4Connection class.
 * </p>
 * <p>
 * Description: The <code>TrafT4Connection</code> class is an implementation of
 * the <code>java.sql.Connection</code> interface.
 * </p>
 *
 */
public class TrafT4Connection extends PreparedStatementManager implements java.sql.Connection {

    private T4DatabaseMetaData metaData_;
    protected boolean isOverLoginTimeout = false;
    protected AtomicInteger pstmtNum = new AtomicInteger(0);
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4Connection.class);
    private int reconnNum = 1;
    private Timer timer = null;
    private short dcsVersion = 0;
    private String activeMasterIP = null;

    private TraceQueryUsedTime traceQueryUsedTime;
    private TraceAverageUsedTime traceAverageUsedTime;
    private boolean batchFlag = false;
    private TrafCache trafCache_;

	public void setTrafCache_(TrafCache trafCache_) {
		this.trafCache_ = trafCache_;
	}

	public TrafCache getTrafCache_() {
		return trafCache_;
	}

	/**
     * we expect the number of prepared statements which are not yet closed
     * should less than {@link T4Properties#MAX_PSTMT}
     * @throws SQLException
     */
    protected void checkPrepreadStmtNum()throws SQLException {
        synchronized (pstmtNum) {
            if (pstmtNum.incrementAndGet() > T4Properties.MAX_PSTMT) {
                releaseOutOfUsedPreparedStmt();
                if (pstmtNum.get() > T4Properties.MAX_PSTMT) {
					pstmtNum.decrementAndGet();
                    throw TrafT4Messages.createSQLException(this.getT4props(), "prepare_exceed_error",
                            T4Properties.MAX_PSTMT);
                }
            }
        }
    }

    protected void decrementPrepreadStmtNum() {
        pstmtNum.decrementAndGet();
    }

    protected int getPrepreadStmtNum() {
        return pstmtNum.get();
    }

	/**
	 * Validates the connection by clearing warnings and verifying that the
	 * Connection is still open.
	 *
	 * @throws SQLException
	 *             If the Connection is not valid
	 */
	private void validateConnection() throws SQLException {
		clearWarnings();

		if (this.ic_ == null || this.ic_.isClosed()) {
			throw TrafT4Messages.createSQLException(this.getT4props(), "invalid_connection");
		}
	}

	public String getRemoteProcess() throws SQLException {
		if(this.ic_ != null) {
			return this.ic_.getRemoteProcess();
		} else {
			return null;
		}
	}

	synchronized public void close() throws SQLException {
		if (getT4props().isLogEnable(Level.FINEST)) {
		    T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
		}
		if (LOG.isTraceEnabled()) {
		    LOG.trace("ENTRY");
		}
		if (this.isUseCacheTable() && getT4props().getCacheTable() != null) {
			TrafCachePool.getTrafCachePool().removeConnection(this);
		}
		if (this.ic_ == null || this.ic_.isClosed())
			return;

		// only hard-close if no pooled connection exists
        close((pc_ == null), true);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE");
        }
	}

	public void commit() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		validateConnection();

		try {
			ic_.commit();
		} catch (SQLException se) {
			performConnectionErrorChecks(se);
			throw se;
		}
        if (getT4props().isTraceTransTime() && !getAutoCommit()) {
            this.getTraceQueryUsedTime().setTransactionEndTime(System.currentTimeMillis());
            this.getTraceQueryUsedTime().print(getT4props(), this.getTraceAverageUsedTime());
        }
        ic_.reconnIfNeeded();
	}

	public void resetServerIdleTimer() throws SQLException {
		clearWarnings();
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}

		this.setConnectionAttr(InterfaceConnection.RESET_IDLE_TIMER, 0, "0");
	}

	public String getApplicationName() throws SQLException {
		validateConnection();

		return this.ic_.getApplicationName();
	}

	public String getServerDataSource() throws SQLException {
		validateConnection();

		return this.ic_.getServerDataSource();
	}

	public boolean getEnforceISO() throws SQLException {
		validateConnection();

		return this.ic_.getEnforceISO();
	}

	public int getISOMapping() throws SQLException {
		validateConnection();

		return this.ic_.getISOMapping();
	}

	public String getRoleName() throws SQLException {
		validateConnection();

		return this.ic_.getRoleName();
	}

	public int getTerminalCharset() throws SQLException {
		validateConnection();

		return this.ic_.getTerminalCharset();
	}

	public String getSessionName() throws SQLException {
		validateConnection();

		return this.ic_.getSessionName();
	}

	public Statement createStatement() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        validateConnection();

        if (ic_.getAutoCommit()) {
            ic_.reconnIfNeeded();
        }

		try {
            if (getT4props().isAllowMultiQueries()) {
                return new TrafT4MultiQueriesStatement(this);
            } else {
                return new TrafT4Statement(this);
            }
		} catch (SQLException se) {
			performConnectionErrorChecks(se);
			throw se;
		}
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", resultSetType, resultSetConcurrency);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", resultSetType, resultSetConcurrency);
        }
        validateConnection();

        if (ic_.getAutoCommit()) {
            ic_.reconnIfNeeded();
        }


		try {
			return new TrafT4Statement(this, resultSetType, resultSetConcurrency);
		} catch (SQLException se) {
			performConnectionErrorChecks(se);
			throw se;
		}
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
        validateConnection();

        if (ic_.getAutoCommit()) {
            ic_.reconnIfNeeded();
        }

		try {
			return new TrafT4Statement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
		} catch (SQLException se) {
			performConnectionErrorChecks(se);
			throw se;
		}
	}

	Locale getLocale() {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
	    }
	    if (LOG.isTraceEnabled()) {
	        LOG.trace("ENTRY");
	    }
		if (ic_ != null) {
			return ic_.getLocale();
		} else {
			return null;
		}
	}

	public boolean getAutoCommit() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
	    }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		validateConnection();

		return ic_.getAutoCommit();
	}

	public String getCatalog() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
	    }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		validateConnection();

        if (getT4props().isLogEnable(Level.FINER)) {
            String temp = "Leave with return = " + ic_.getCatalog();
            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Leave with return = {}", ic_.getCatalog());
        }
		return ic_.getCatalog();
	}

    public String getSchema() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

        validateConnection();

        if (getT4props().isLogEnable(Level.FINER)) {
            String temp = "Leave with return = " + ic_.getSchema();
            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Leave with return = {}", ic_.getSchema());
        }
        return ic_.getSchema();
    }

	public int getHoldability() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		validateConnection();

        if (getT4props().isLogEnable(Level.FINER)) {
            String temp = "Leave with return = " + holdability_;
            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Leave with return = {}", holdability_);
        }
		return holdability_;
	}

	public DatabaseMetaData getMetaData() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		validateConnection();

        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE");
        }
		return this.metaData_;
	}

	public int getTransactionIsolation() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		validateConnection();

        if (getT4props().isLogEnable(Level.FINER)) {
            String temp = "Leave with return = " + ic_.getTransactionIsolation();
            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Leave with return = {}", ic_.getTransactionIsolation());
        }
		return ic_.getTransactionIsolation();
	}

	public Map getTypeMap() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		validateConnection();

        if (getT4props().isLogEnable(Level.FINER)) {
            String temp = "Leave with return = " + userMap_;
            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Leave with return = {}", userMap_);
        }
		return userMap_;
	}

	void isConnectionOpen() throws SQLException {
		validateConnection();
	}

	public boolean isClosed() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
	    }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		boolean rv = true;

		if (ic_ == null) {
			rv = true;
			// return true;
		} else {
			clearWarnings();
			rv = ic_.getIsClosed();
		}
		if (getT4props().isLogEnable(Level.FINER)) {
		    String temp = "Leave with return = " + rv;
		    T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
		}
		if (LOG.isDebugEnabled()) {
            LOG.debug("Leave with return = {}", rv);
        }

		return rv;
		// return ic_.get_isClosed();
	}

	// New method that checks if the connection is closed
	// However, this is to be used only be internal classes
	// It does not clear any warnings associated with the current connection
	// Done for CASE 10_060123_4011 ; Swastik Bihani
	boolean _isClosed() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
	    }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		boolean rv = true;

		if (ic_ == null) {
			rv = true;
		} else {
			rv = ic_.getIsClosed();
		}
        if (getT4props().isLogEnable(Level.FINER)) {
            String temp = "Leave with return = " + rv;
            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Leave with return = {}", rv);
        }

		return rv;
	}

	/**
	 * @deprecated
	 */
	public String getServiceName() throws SQLException {
		return "";
	}

	/**
	 * @deprecated
	 */
	public void setServiceName(String serviceName) throws SQLException {

	}

	public boolean isReadOnly() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		validateConnection();

		return ic_.isReadOnly();
	}

	public String nativeSQL(String sql) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
	    }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }

		validateConnection();

		return sql;
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", sql);
        }

		TrafT4CallableStatement stmt;

		clearWarnings();
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}

		try {
//			if (isStatementCachingEnabled()) {
//				stmt = (TrafT4CallableStatement) getPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY,
//						ResultSet.CONCUR_READ_ONLY, holdability_);
//
//				if (stmt != null) {
//					return stmt;
//				}
//			}

			stmt = new TrafT4CallableStatement(this, sql);
			stmt.prepareCall(stmt.sql_, stmt.queryTimeout_, stmt.resultSetHoldability_);

//			if (isStatementCachingEnabled()) {
//				addPreparedStatement(this, sql, stmt, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
//						holdability_);
//			}
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
		return stmt;
	}

	public CallableStatement prepareCall(String sql, String stmtLabel) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, stmtLabel);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, stmtLabel);
        }

        final String QUOTE = "\"";

		if (stmtLabel == null || stmtLabel.length() == 0) {
			throw TrafT4Messages.createSQLException(getT4props(), "null_data");
		}

		if (stmtLabel.startsWith(QUOTE) && stmtLabel.endsWith(QUOTE)) {
			int len = stmtLabel.length();
			if (len == 2) {
				throw TrafT4Messages.createSQLException(getT4props(), "null_data");
			} else {
				stmtLabel = stmtLabel.substring(1, len - 1);
			}
		} else {
			stmtLabel = stmtLabel.toUpperCase();
		}

		TrafT4CallableStatement stmt = null;

		clearWarnings();
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}

		try {
//			if (isStatementCachingEnabled()) {
//				stmt = (TrafT4CallableStatement) getPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY,
//						ResultSet.CONCUR_READ_ONLY, holdability_);
//
//				if (stmt != null) {
//					return stmt;
//				}
//			}
            if (stmtLabel.equalsIgnoreCase("null")) {
                stmt = new TrafT4CallableStatement(this, sql);
            } else {
                stmt = new TrafT4CallableStatement(this, sql, stmtLabel);
            }
			stmt.prepareCall(stmt.sql_, stmt.queryTimeout_, stmt.resultSetHoldability_);

//			if (isStatementCachingEnabled()) {
//				addPreparedStatement(this, sql, stmt, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
//						holdability_);
//			}
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
		return stmt;
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, resultSetType, resultSetConcurrency);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", sql, resultSetType, resultSetConcurrency);
        }
		TrafT4CallableStatement stmt;

		clearWarnings();
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}

		try {
//			if (isStatementCachingEnabled()) {
//				stmt = (TrafT4CallableStatement) getPreparedStatement(this, sql, resultSetType, resultSetConcurrency,
//						holdability_);
//				if (stmt != null) {
//					return stmt;
//				}
//			}

			stmt = new TrafT4CallableStatement(this, sql, resultSetType, resultSetConcurrency);
			stmt.prepareCall(stmt.sql_, stmt.queryTimeout_, stmt.resultSetHoldability_);

//			if (isStatementCachingEnabled()) {
//				addPreparedStatement(this, sql, stmt, resultSetType, resultSetConcurrency, holdability_);
//			}
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
		return stmt;
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", sql, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
		TrafT4CallableStatement stmt;

		clearWarnings();
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}
		try {
//			if (isStatementCachingEnabled()) {
//				stmt = (TrafT4CallableStatement) getPreparedStatement(this, sql, resultSetType, resultSetConcurrency,
//						resultSetHoldability);
//				if (stmt != null) {
//					return stmt;
//				}
//			}

			stmt = new TrafT4CallableStatement(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);

			stmt.prepareCall(stmt.sql_, stmt.queryTimeout_, stmt.resultSetHoldability_);

//			if (isStatementCachingEnabled()) {
//				addPreparedStatement(this, sql, stmt, resultSetType, resultSetConcurrency, resultSetHoldability);
//			}
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
		return stmt;
	}

	/**
	 * Creates a <code>PreparedStatement</code> object for sending
	 * parameterized SQL statements to the database.
	 *
	 * @param sql
	 *            SQL statement that might contain one or more '?' IN parameter
	 *            placeholders
	 * @param stmtLabel
	 *            SQL statement label that can be passed to the method instead
	 *            of generated by the database system.
	 * @returns a new default PreparedStatement object containing the
	 *          pre-compiled SQL statement
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public PreparedStatement prepareStatement(String sql, String stmtLabel) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, stmtLabel);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, stmtLabel);
        }
        final String QUOTE = "\"";

        if (stmtLabel == null || stmtLabel.length() == 0) {
            throw TrafT4Messages.createSQLException(getT4props(), "null_data");
        }
        // TODO to clear same named pstmt in connection
        if (this.getPrepStmtsInCache() != null && this.getPrepStmtsInCache().size() > 0) {
            String lookupKey = createKey(this, sql, this.holdability_);
            this.getPrepStmtsInCache().remove(lookupKey);
        }

        if (stmtLabel.startsWith(QUOTE) && stmtLabel.endsWith(QUOTE)) {
            int len = stmtLabel.length();
            if (len == 2) {
                throw TrafT4Messages.createSQLException(getT4props(), "null_data");
            } else {
                stmtLabel = stmtLabel.substring(1, len - 1);
            }
        } else {
            stmtLabel = stmtLabel.toUpperCase();
        }
        TrafT4PreparedStatement stmt;
        clearWarnings();

        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }
        if(this.isUseCacheTable() && this.getT4props().getCacheTable() != null){
			this.trafCache_.prepareCache(sql,this.getT4props());
		}

        if (ic_.getAutoCommit()) {
            ic_.reconnIfNeeded();
        }
		try {
			if (isStatementCachingEnabled()) {
				stmt = (TrafT4PreparedStatement) getPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY, holdability_);
				if (stmt != null) {
					return stmt;
				}
			}

			stmt = new TrafT4PreparedStatement(this, sql, stmtLabel);

			stmt.prepare(stmt.sql_, stmt.queryTimeout_, stmt.resultSetHoldability_);

			if (isStatementCachingEnabled()) {
				addPreparedStatement(this, sql, stmt, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
						holdability_);
			}
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
		return stmt;
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", sql);
        }
        TrafT4PreparedStatement stmt = null;

        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }

		if(ic_.getAutoCommit()) {
            ic_.reconnIfNeeded();
        }

		if(this.isUseCacheTable() && this.getT4props().getCacheTable() != null){
			this.trafCache_.prepareCache(sql,this.getT4props());
		}

		try {
			if (isStatementCachingEnabled()) {
				stmt = (TrafT4PreparedStatement) getPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY, holdability_);
				if (stmt != null) {
					if (getT4props().isLogEnable(Level.FINER)) {
						T4LoggingUtilities.log(getT4props(), Level.FINER, "From Pool", sql, stmt.stmtLabel_);
					}
					if (LOG.isDebugEnabled()) {
					    LOG.debug("From Pool. {}, {}", sql, stmt.stmtLabel_);
					}
					return stmt;
				}
			}

            if (getT4props().isAllowMultiQueries()) {
                stmt = new TrafT4MultiQueriesPreparedStatement(this, sql);
            } else {
                stmt = new TrafT4PreparedStatement(this, sql);
                stmt.prepare(stmt.sql_, stmt.queryTimeout_, stmt.resultSetHoldability_);
            }

            if (isStatementCachingEnabled()) {
                addPreparedStatement(this, sql, stmt, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    holdability_);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Created a PreparedStatement and register in the pool. {}, {}", sql,
                        stmt.stmtLabel_);
                }
            }else {
                if (LOG.isDebugEnabled()) {
                    LOG.info("Created a PreparedStatement without pool. {}, {}, isPoolEnabled{}",
                        sql, stmt.stmtLabel_, false);
                }
            }
        } catch (TrafT4Exception se) {
            performConnectionErrorChecks(se);
            throw se;
        }finally {
            if (getT4props().isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(getT4props(), Level.FINER, "Finished for getting statement", sql, stmt.stmtLabel_);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finished for getting statement. {}, {}", sql, stmt.stmtLabel_);
            }
        }
        return stmt;
    }

	// SB 12/02/2004 - only for LOB statements - these will be not added to the
	// statement cache
	PreparedStatement prepareLobStatement(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", sql);
        }

		TrafT4PreparedStatement stmt;

		clearWarnings();
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}

		try {
			stmt = new TrafT4PreparedStatement(this, sql);
			stmt.prepare(stmt.sql_, stmt.queryTimeout_, stmt.resultSetHoldability_);
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
		return stmt;

	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, autoGeneratedKeys);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, autoGeneratedKeys);
        }

		if (autoGeneratedKeys == TrafT4Statement.NO_GENERATED_KEYS) {
			return prepareStatement(sql);
		} else {
			throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
		}
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, columnIndexes);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, columnIndexes);
        }

		if (columnIndexes != null && columnIndexes.length > 0) {
			throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
		} else {
			return prepareStatement(sql);
		}
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, resultSetType, resultSetConcurrency);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", sql, resultSetType, resultSetConcurrency);
        }
        TrafT4PreparedStatement stmt;

        clearWarnings();

        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }

		if (this.isUseCacheTable() && this.getT4props().getCacheTable() != null) {
			this.trafCache_.prepareCache(this, sql, resultSetType, resultSetConcurrency,this.getT4props());
		}

        if(ic_.getAutoCommit()) {
            ic_.reconnIfNeeded();
        }

		try {
			if (isStatementCachingEnabled()) {
				stmt = (TrafT4PreparedStatement) getPreparedStatement(this, sql, resultSetType, resultSetConcurrency,
						holdability_);
				if (stmt != null) {
					return stmt;
				}
			}

			stmt = new TrafT4PreparedStatement(this, sql, resultSetType, resultSetConcurrency);
			stmt.prepare(stmt.sql_, stmt.queryTimeout_, stmt.resultSetHoldability_);

			if (isStatementCachingEnabled()) {
				addPreparedStatement(this, sql, stmt, resultSetType, resultSetConcurrency, holdability_);
			}
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
		return stmt;
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", sql, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
        TrafT4PreparedStatement stmt;

        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }

		if (this.isUseCacheTable() && this.getT4props().getCacheTable() != null) {
			this.trafCache_.prepareCache(sql, resultSetType, resultSetConcurrency, resultSetHoldability, this.getT4props());
		}

        if(ic_.getAutoCommit()) {
            ic_.reconnIfNeeded();
        }

		try {
			if (isStatementCachingEnabled()) {
				stmt = (TrafT4PreparedStatement) getPreparedStatement(this, sql, resultSetType, resultSetConcurrency,
						resultSetHoldability);
				if (stmt != null) {
					return stmt;
				}
			}

			stmt = new TrafT4PreparedStatement(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
			stmt.prepare(stmt.sql_, stmt.queryTimeout_, stmt.resultSetHoldability_);

			if (isStatementCachingEnabled()) {
				addPreparedStatement(this, sql, stmt, resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
		return stmt;
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, columnNames);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, columnNames);
        }
		if (columnNames != null && columnNames.length > 0) {
			throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
		} else {
			return prepareStatement(sql);
		}
	}

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", savepoint);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", savepoint);
        }

        validateConnection();

        try {
            ic_.releaseSavepoint(savepoint);
        } catch (TrafT4Exception se) {
            performConnectionErrorChecks(se);
            throw se;
        }
    }

	public void rollback() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		clearWarnings();
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}

		// if (ic_.getTxid() == 0) - XA
		// return;

		// commit the Transaction
		try {
			ic_.rollback();
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
        if (getT4props().isTraceTransTime()  && !getAutoCommit()) {
            this.getTraceQueryUsedTime().setTransactionEndTime(System.currentTimeMillis());
            this.getTraceQueryUsedTime().print(getT4props(), this.getTraceAverageUsedTime());
        }
		// ic_.setTxid(0); - XA
        ic_.reconnIfNeeded();
	}

    public void rollback(Savepoint savepoint) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", savepoint);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", savepoint);
        }

        validateConnection();
        try {
            ic_.rollbackSavepoint(savepoint);
        } catch (TrafT4Exception se) {
            performConnectionErrorChecks(se);
            throw se;
        }
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", autoCommit);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", autoCommit);
        }
        if (autoCommit == false) {
            ic_.reconnIfNeeded();
        }

        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }
        try {
            ic_.setAutoCommit(this, autoCommit);
        } catch (TrafT4Exception se) {
            performConnectionErrorChecks(se);
            throw se;
        }
    }

    public void setCatalog(String catalog) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", catalog);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", catalog);
        }
        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }
        if (catalog != null) {
            try {
                ic_.setCatalog(this, catalog);
            } catch (TrafT4Exception se) {
                performConnectionErrorChecks(se);
                throw se;
            }
        }
    }
    public void setSessionDebug(boolean sessionDebugEnable) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sessionDebugEnable);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", sessionDebugEnable);
        }
        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }
		try {
			ic_.setSessionDebug(this, sessionDebugEnable);
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
    }

    public void setClipVarchar(int clipVarchar) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", clipVarchar);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", clipVarchar);
        }
        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }
		try {
			ic_.setClipVarchar(this, clipVarchar);
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
    }

    public void setHoldability(int holdability) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", holdability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", holdability);
        }
        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }

        if (holdability != TrafT4ResultSet.CLOSE_CURSORS_AT_COMMIT)

        {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_holdability");
        }
        holdability_ = holdability;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", readOnly);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", readOnly);
        }
        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }
        try {
            // ic_.setReadOnly(readOnly);
            ic_.setReadOnly(this, readOnly);
        } catch (TrafT4Exception se) {
            performConnectionErrorChecks(se);
            throw se;
        }
    }

    public void setConnectionAttr(short attr, int valueNum, String valueString) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", attr, valueNum, valueString);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", attr, valueNum, valueString);
        }
        ic_.setConnectionAttr(this, attr, valueNum, valueString);
    }

    //3196 - NDCS transaction for SPJ
    public void joinUDRTransaction(long transId) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", transId);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", transId);
        }
        String sTransid = String.valueOf(transId);
        ic_.setConnectionAttr(this,  InterfaceConnection.SQL_ATTR_JOIN_UDR_TRANSACTION, 0, sTransid);
    }

    //3196 - NDCS transaction for SPJ
    public void suspendUDRTransaction() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        String sTransid = String.valueOf(ic_.transId_);
        ic_.setConnectionAttr(this, InterfaceConnection.SQL_ATTR_SUSPEND_UDR_TRANSACTION, 0, sTransid);
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", name);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", name);
        }

        if (name == null || name.length() == 0)
        {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid savepoint name");
        }

        validateConnection();
        Savepoint sp = new TrafT4Savepoint(name);
        try {
            ic_.setSavepoint(sp);
        } catch (TrafT4Exception se) {
            performConnectionErrorChecks(se);
            throw se;
        }
        return sp;
    }

    public Savepoint setSavepoint() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        validateConnection();

        String name = "SVPT" + UUID.randomUUID().toString().replace("-", "").substring(28);
        Savepoint sp= new TrafT4Savepoint(name);
        try {
            ic_.setSavepoint(sp);
        } catch (TrafT4Exception se) {
            performConnectionErrorChecks(se);
            throw se;
        }
        return sp;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", level);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", level);
        }
        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }
        try {
            ic_.setTransactionIsolation(this, level);
        } catch (TrafT4Exception se) {
            performConnectionErrorChecks(se);
            throw se;
        }
    }

    // JDK 1.2
    public void setTypeMap(java.util.Map map) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", map);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", map);
        }
        clearWarnings();
        userMap_ = map;
    }

    public void begintransaction() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}
		try {
			ic_.beginTransaction();

			if (ic_.beginTransaction() == 0) {
				return;
			} else {
				setAutoCommit(false);
			}
		} catch (TrafT4Exception se) {
			performConnectionErrorChecks(se);
			throw se;
		}
	}

	public long getCurrentTransaction() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}
		return ic_.getTxid();
	}

	public void setTxid(long txid) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", txid);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", txid);
        }
		setTransactionToJoin(Bytes.createLongBytes(txid, this.ic_.getByteSwap()));
	}

	public void setTransactionToJoin(byte[] txid) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", txid);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", txid);
        }

		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}

		transactionToJoin = txid;
	}

	void gcStmts() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		Reference pRef;
		String stmtLabel;
		InterfaceStatement ist = null;
		while ((pRef = refStmtQ_.poll()) != null) {
			stmtLabel = (String) refToStmt_.get(pRef)[0];
			// All PreparedStatement objects are added to HashMap
			// Only Statement objects that produces ResultSet are added to
			// HashMap
			// Hence stmtLabel could be null
			if (stmtLabel != null) {
				try {
					if (ist == null) {
						ist = new InterfaceStatement(this, (Boolean) refToStmt_.get(pRef)[1]);
					}
					ist.setStmtLabel(stmtLabel);
					ist.close();

				} catch (SQLException e) {
					performConnectionErrorChecks(e);
				} finally {
					refToStmt_.remove(pRef);
				}
			}
		}
	}

	void removeElement(Reference pRef) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pRef);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", pRef);
        }

		refToStmt_.remove(pRef);
		pRef.clear();
	}

	void addElement(Reference pRef, Object[] trafT4Stmt) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pRef, trafT4Stmt[0],trafT4Stmt[1]);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", pRef, trafT4Stmt[0],trafT4Stmt[1]);
        }
		refToStmt_.put(pRef, trafT4Stmt);
	}

	private void physicalCloseStatements() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		// close all the statements
		ArrayList<Object[]> stmts = new ArrayList<Object[]>(refToStmt_.values());
		int size = stmts.size();
		for (int i = 0; i < size; i++) {
			try {
				String stmtLabel = (String) stmts.get(i)[0];
				TrafT4Statement stmt = new TrafT4Statement(this, stmtLabel);
				stmt.close();
				stmt = null;
			} catch (SQLException se) {
				// Ignore any exception and proceed to the next statement
			}
		}
		refToStmt_.clear();

        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE");
        }
	}

	private void rollbackAndIgnoreError() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		// Rollback the Transaction when autoCommit mode is OFF
		try {
			if (getAutoCommit() == false && !ic_.shouldReconn()) {
				rollback();
			}
		} catch (SQLException sqex) {
	        if (getT4props().isLogEnable(Level.FINER)) {
	            T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY", "warning: "+sqex.getMessage());
	        }
	        if (LOG.isDebugEnabled()) {
	            LOG.debug("ENTRY. warning: {}", sqex.getMessage());
	        }
		}
	}


    synchronized void close(boolean hardClose, boolean sendEvents) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", hardClose, sendEvents);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", hardClose, sendEvents);
        }
        clearWarnings();
        try {
            if (!hardClose) {
                if (this.ic_ != null && this.ic_.getIsClosed()) {
                    return;
                }
                if (isStatementCachingEnabled()) {
                    closePreparedStatementsAll();
                } else {
                    physicalCloseStatements();
                }
                rollbackAndIgnoreError();

                /*
                 * //inform the NCS server to disregard the T4 ConnectionTimeout value try{ if (ic_
                 * != null) { ic_.disregardT4ConnectionTimeout(this); } }catch(SQLException e){
                 * //ignore - new property from old MXCS ABD version (now known as NCS) //ignored
                 * for backward compatibility }
                 */

                // Need to logicallcally close the statement
                pc_.logicalClose(sendEvents);
                if (ic_ != null) {
                    ic_.setIsClosed(true);
                }
	            if (getT4props().isTraceTransTime()) {
		            this.getTraceAverageUsedTime().clear();
	            }
            } else {
                if (getServerHandle() == null) {
                    return;
                }

                // close all the statements
                physicalCloseStatements();

                // Need to logicallcally close the statement
                // Rollback the Transaction when autoCommit mode is OFF
                rollbackAndIgnoreError();

                if (isStatementCachingEnabled() && !ic_.shouldReconn()) {
                    clearPreparedStatementsAll();
                }

                // Close the connection
                try {
                    if (ic_ != null) {
                        ic_.close();
                    }
                } finally {
                    if (ic_ != null) {
                        ic_.removeElement(this);
                    }
                    ic_ = null;
                }
            }
        } catch (SQLException e) {
            performConnectionErrorChecks(e);
            throw e;
        } finally {
            // close the timer thread
            if (ic_ != null && ic_.getT4Connection() != null) {
                ic_.getT4Connection().closeTimers();
            }
        }
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE");
        }
	}

	protected void finalize() {
		if (ic_ != null && ic_.getT4Connection() != null) {
			ic_.getT4Connection().closeTimers();
		}
	}

	void reuse() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		ic_.reuse();
		/*
		 * try { ic_.enforceT4ConnectionTimeout(this); } catch (TrafT4Exception
		 * se) { //performConnectionErrorChecks(se); //throw se; //ignore - new
		 * property from old MXCS ABD version (now known as NCS) //ignored for
		 * backward compatibility }
		 */
	}


	// Extension method for WLS, this method gives the pooledConnection object
	// associated with the given connection object.
	public PooledConnection getPooledConnection() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (pc_ != null) {
			return pc_;
		} else {
			throw TrafT4Messages.createSQLException(getT4props(), "null_pooled_connection");
		}
	}

	TrafT4Connection(TrafT4DataSource ds, T4Properties t4props) throws SQLException {
		super(t4props);
		if (getT4props().isLogEnable(Level.FINEST)) {
		    T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", ds, t4props);
		}
		if (LOG.isTraceEnabled()) {
		    LOG.trace("ENTRY. {}, {}", ds, t4props);
		}

		t4props.setConnectionID(Integer.toString(this.hashCode()));
		setupLogging(t4props);

		ds_ = ds;


		makeConnection(t4props);
		holdability_ = TrafT4ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	TrafT4Connection(TrafT4PooledConnection poolConn, T4Properties t4props) throws SQLException {
		super(t4props);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", poolConn, t4props);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", poolConn, t4props);
        }
		t4props.setConnectionID(Integer.toString(this.hashCode()));
		setupLogging(t4props);

		pc_ = poolConn;

		makeConnection(t4props);
		holdability_ = TrafT4ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

    protected void checkLoginTimeout(final T4Properties t4props) throws TrafT4Exception {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", t4props);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", t4props);
        }

        if (this.isOverLoginTimeout) {
            if (this.ic_ != null) {
                this.ic_.setIsClosed(true);
            }
            throw TrafT4Messages.createSQLException(t4props, "ids_s1_t00", ic_.getTargetServer(),
                    ic_.getLoginTimeout());
        }
    }

    protected void makeConnection(final T4Properties t4props) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINER)) {
		    T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY");
	    }
	    if (LOG.isTraceEnabled()) {
		    LOG.trace("ENTRY");
	    }
	    Timer timer = null;
	    try {
		    timer = new Timer();
		    timer.schedule(new TimerTask() {
			    @Override
			    public void run() {
				    try {
					    TrafT4Connection.this.isOverLoginTimeout = true;
				    } catch (Exception e) {
					    e.printStackTrace();
				    }
			    }
		    }, t4props.getLoginTimeout() * 1000);

		    this.checkLoginTimeout(t4props);
		    ic_ = new InterfaceConnection(this, t4props);

		    try {
			    doConnect(t4props);
			    this._isUseCache = ic_.outContext.isUseCache();
		    } catch (SQLException se) {
			    this.checkLoginTimeout(t4props);
			    throw se;
		    }

		    if (ic_.sqlwarning_ != null) {
			    setSqlWarning(ic_.sqlwarning_);
		    }

		    refStmtQ_ = new ReferenceQueue();
		    refToStmt_ = new ConcurrentHashMap<Reference, Object[]>();
		    pRef_ = new WeakReference<TrafT4Connection>(this, ic_.refQ_);
		    ic_.refTosrvrCtxHandle_.put(pRef_, ic_);

		    ic_.enableNARSupport(this, getT4props().getBatchRecovery());

			if (getT4props().getSPJEnv()) {
				ic_.enableProxySyntax(this);
			}
			this.metaData_ = new T4DatabaseMetaData(this);
		} finally {
			if (timer != null) {
				timer.cancel();
			}
            TrafT4Connection.this.isOverLoginTimeout = false;

            if (getT4props().isLogEnable(Level.FINEST)) {
				T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
			}
			if (LOG.isTraceEnabled()) {
				LOG.trace("LEAVE");
			}
		}
	}

	protected ArrayList<String> createAddressList(T4Properties t4props) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		ArrayList<String> addresses = new ArrayList<String>(); //10 addresses by default
		if (t4props.getUrlList().size() == 0) {
			addresses.add(t4props.getUrl());
		} else {
			addresses.addAll(t4props.getUrlList());
		}
		String os = System.getProperty("os.name");
		String enable = System.getProperty("t4jdbc.redirectaddr");

		if(enable != null && enable.equals("true") && os != null && os.equals("NONSTOP_KERNEL")) { //  TODO get real name
			String providedUrl = t4props.getUrl();
			String providedHost = providedUrl.substring(16).toLowerCase();
			String hostPrefix = null;
			try {
				hostPrefix = java.net.InetAddress.getLocalHost().getHostName().substring(0, 5).toLowerCase();
			}catch(Exception e) {
			}

			if(hostPrefix != null && providedHost.startsWith(hostPrefix)) {
				File f = new File("/E/" + hostPrefix + "01/usr/t4jdbc/jdbcaddr.txt");
				if(f.exists()) {
					addresses.clear();

					String urlSuffix = providedUrl.substring(providedUrl.indexOf("/:"));

					try {
				        BufferedReader in = new BufferedReader(new FileReader(f));
				        String host;
				        while ((host = in.readLine()) != null) {
				            if(host.indexOf(':') == -1) {
				            	host += ":18650";
				            }
				            addresses.add(String.format("jdbc:t4jdbc://" + host + urlSuffix));
				        }
				        in.close();
				    } catch (IOException e) {
				    }
				}
			}
		}

		return addresses;
	}



	// --------------------------------------------------------
	private void setupLogging(T4Properties t4props) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		String ID = T4LoggingUtilities.getUniqueID();
		String name = T4LoggingUtilities.getUniqueLoggerName(ID);

		if (t4props.getT4LogLevel() == Level.OFF) {
			if (dummyLogger_ == null) {
				dummyLogger_ = Logger.getLogger(name);
			}
			t4props.t4Logger_ = dummyLogger_;
		} else {
			t4props.t4Logger_ = Logger.getLogger(name);
		}

		// t4props.t4Logger_ = Logger.getLogger(name);
		t4props.t4Logger_.setUseParentHandlers(false);
		t4props.t4Logger_.setLevel(t4props.getT4LogLevel());

		if (t4props.getT4LogLevel() != Level.OFF) {
			FileHandler fh1 = t4props.getT4LogFileHandler();
			t4props.t4Logger_.addHandler(fh1);
		}
	} // end setupLogging

	// --------------------------------------------------------

	// Interface Methods
	InterfaceConnection getServerHandle() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		return ic_;
	}

	// Interface Methods
	public int getDialogueId() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		return getServerHandle().getDialogueId();
	}

	/**
	 * Returns true if the data format needs to be converted. Used by the
	 * <CODE>TrafT4ResultSet</CODE> class.
	 *
	 * @return true if conversion is needed; otherwise, false.
	 */
	public boolean getDateConversion() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		validateConnection();

		return ic_.getDateConversion();
	}

	int getServerMajorVersion() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		validateConnection();

		return ic_.getServerMajorVersion();
	}

	int getServerMinorVersion() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		validateConnection();

		return ic_.getServerMinorVersion();
	}


	void closeErroredConnection(TrafT4Exception se) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		try {
			if (!erroredConnection) { // don't issue close repeatedly
				erroredConnection = true;
				if (pc_ != null) {
					pc_.sendConnectionErrorEvent(se);
				} else {
					// hardclose
					close(true, true);
				}
			}
		} catch (Exception e) {
			// ignore
		}
	}


	boolean erroredConnection = false;

	static Logger dummyLogger_ = null;

	// Fields
    InterfaceConnection ic_;

	// Connection
	Map userMap_;
	ReferenceQueue refStmtQ_;
	ConcurrentHashMap<Reference, Object[]> refToStmt_;
	int holdability_;
	TrafT4DataSource ds_;
	TrafT4PooledConnection pc_;
//	T4Driver driver_;
	WeakReference<TrafT4Connection> pRef_;
	PreparedStatement psForValid_ = null;
	byte[] transactionToJoin;


	public Object unwrap(Class iface) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", iface);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", iface);
        }
		try {
			return iface.cast(this);
		} catch (ClassCastException cce) {
			throw TrafT4Messages.createSQLException(getT4props(), "unable_unwrap", iface.toString());
		}
	}

	public boolean isWrapperFor(Class iface) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", iface);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", iface);
        }
		if (_isClosed() == true) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		} else {
			return iface.isInstance(this);
		}
	}

	public Clob createClob() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        validateConnection();
		return new TrafT4Clob(this);
	}

	public Blob createBlob() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        validateConnection();
		return new TrafT4Blob(this);
	}

	public NClob createNClob() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        validateConnection();
		// TODO Auto-generated method stub
		return null;
	}

	public SQLXML createSQLXML() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        validateConnection();
		// TODO Auto-generated method stub
		return null;
	}

    public boolean isValid(int timeout) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

        if (timeout < 0) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_parameter_value", timeout);
        }
        if (ic_ == null) {
            if (getT4props().isLogEnable(Level.FINE)) {
                T4LoggingUtilities.log(getT4props(), Level.FINE, "isValid is false becasue connection is null");
            }
            return false;
        }
        if (ic_.getIsClosed()) {
            if (getT4props().isLogEnable(Level.FINE)) {
                T4LoggingUtilities.log(getT4props(), Level.FINE, "isvalid is false because connection is closed");
            }
            return false;
        }

        if (ic_.isProcessing()) {
            return true;
        }

        ResultSet tmpRs = null;
        try {
            if (psForValid_ == null) {
                psForValid_ = prepareStatement("select 1 from dual");
            }
            if (timeout > 0) {
                psForValid_.setQueryTimeout(timeout);
            }
            // if the operation can get the correct result, we think the conn is valid
            // or it will run into catch and return false
            tmpRs = psForValid_.executeQuery();
            boolean valid = false;
            while (tmpRs.next()) {
                valid = tmpRs.getInt(1) == 1;
            }
            return valid;
        } catch (SQLException e) {
            return false;
        } finally {
            if(tmpRs != null) {
                tmpRs.close();
            }
        }
    }

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", name, value);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", name, value);
        }

        if (name.equalsIgnoreCase("TransactionType")) {
            try {
                this.setConnectionAttr(TRANSPORT.SQL_ATTR_TRANSACTION_TYPE, 0, value);
            } catch (SQLException ex) {
            }
        }
        ic_.setClientInfo(name, value);
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", properties);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", properties);
        }
		ic_.setClientInfoProperties( properties);
	}

	public String getClientInfo(String name) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", name);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", name);
        }
		validateConnection();
		return ic_.getClientInfoProperties().getProperty(name);
	}

	public Properties getClientInfo() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		validateConnection();
		return ic_.getClientInfoProperties();
	}

	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", typeName, elements);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", typeName, elements);
        }
		// TODO Auto-generated method stub
		return null;
	}

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", typeName, attributes);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", typeName, attributes);
        }
		// TODO Auto-generated method stub
		return null;
	}

    public void setSchema(String schema) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", schema);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", schema);
        }
        clearWarnings();
        if (_isClosed() == true) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
        }
        if (schema != null) {
            try {
            	if(!schema.startsWith("\"") && !schema.endsWith("\"")){
            		schema = schema.toUpperCase();
	            }
                ic_.setSchema(this, schema);
            } catch (TrafT4Exception se) {
                performConnectionErrorChecks(se);
                throw se;
            }
        }
    }

	public void abort(Executor executor) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", executor);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", executor);
        }
		if (ic_.getT4Connection().getInputOutput() != null) {
			ic_.getT4Connection().getInputOutput().closeIO();
		}
		ic_.setIsClosed(true);
	}

	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", executor, milliseconds);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", executor, milliseconds);
        }
        validateConnection();
        getT4props().setNetworkTimeoutInMillis(milliseconds);
	}


	public int getNetworkTimeout() throws SQLException {
		if (getT4props().isLogEnable(Level.FINEST)) {
			T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("ENTRY");
		}
		validateConnection();
		return getT4props().getNetworkTimeoutInMillis();
	}

	public void setLinkThreshold(long linkThreshold) {
		getT4props().setLinkThreshold(linkThreshold);
	}

    protected TraceQueryUsedTime getTraceQueryUsedTime() {
        return traceQueryUsedTime;
    }

    protected TraceAverageUsedTime getTraceAverageUsedTime() {
        return traceAverageUsedTime;
    }

    protected boolean isBatchFlag() {
        return batchFlag;
    }

    protected void setBatchFlag(boolean flag) {
        this.batchFlag = flag;
    }

    public int getRemotePid(){
		return ic_.outContext.processId;
    }

    protected void checkTransStartTime(long startTime) throws SQLException {
        if (getTraceQueryUsedTime().getTransactionStartTime() == 0) {
            getTraceQueryUsedTime().setTransactionStartTime(startTime);
        }
    }

    protected void addTransactionUsedTimeList(String label, String sql, long startTime,
        short queryType)
        throws SQLException {
        if (getT4props().isTraceTransTime() && !getAutoCommit()) {
            long end = System.currentTimeMillis();
            TraceQueryUsedTime tmptqt = new TraceQueryUsedTime(label, sql, startTime, end,
                queryType);
            getTraceQueryUsedTime().add(tmptqt);
        }
    }

    public KeepAliveCheckReply startKeepAlive(String dialogueIds, String ip) {
        KeepAliveCheckReply kcr = null;
        int retryCount = 1;
        boolean success = false;
        boolean isActiveMaster = false;
        String addr = null;
        String realAdress = null;
        if (ic_ != null && !ic_.isClosed()) {
            while (!success && retryCount <= 3) {
                try {
	                TrafCheckActiveMaster activeMaster = TrafCheckActiveMaster
			                .getActiveMasterCacheMap().get(getT4props().getT4Url());
	                addr = activeMaster.getDcsMaster(this, false);
                    if (addr.split(":")[0].equals(ip)) {
                        isActiveMaster = true;
                        success = true;
                        break;
                    }
                    success = true;
                } catch (SQLException e) {
                    if (getT4props().isLogEnable(Level.WARNING)) {
                        T4LoggingUtilities.log(getT4props(), Level.WARNING,
                            "erro while checkMasterIp, retryCount is : " + retryCount);
                    }
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("erro while checkMasterIp, retryCount is <{}>", retryCount);
                    }
                    //sometimes active master down, we need wait until next active master up
                    //sleep 1 minute, then retry
                    try {
                        Thread.sleep(60 * 1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }catch (Exception e2){
                    e2.printStackTrace();
                    return null;
                }
                retryCount++;
            }
            try {
                if (!isActiveMaster) {
                    // re new t4DcsConnection
                    realAdress = T4Address.extractUrlListFromUrl(getT4props().getT4Url(), addr);
                    ic_.t4DcsConnection = new T4DcsConnection(ic_, realAdress);
                    if (LOG.isInfoEnabled()) {
                        LOG.info("keepAlive doIO with new ip = <{}>, realAdress = <{}>", addr, realAdress);
                    }

                }
                kcr = ic_.t4DcsConnection
                        .keepAliveCheck(dialogueIds, getRemoteProcess());
            } catch (SQLException e) {
                if (getT4props().isLogEnable(Level.WARNING)) {
                    T4LoggingUtilities.log(getT4props(), Level.WARNING, "erro while startKeepAlive : " + e.getMessage());
                }
                if (LOG.isWarnEnabled()) {
                    LOG.warn("erro while startKeepAlive {}", e.getMessage());
                }
                kcr = null;
            }
        }
        return kcr;

    }

    protected void setDcsVersion(short dcsVersion){
        if(dcsVersion < 10){
	        this.dcsVersion = dcsVersion;
        }
    }

    public short getDcsVersion(){
	    return this.dcsVersion;
    }


    protected void setActiveMasterIP(String ip) {
        this.activeMasterIP = ip.split(":")[0];
    }

    public String getActiveMasterIP() {
        return this.activeMasterIP;
    }

    public void notifyIcClose(){
	    if(ic_ != null && !ic_.isClosed()){
	        ic_.setIsClosed(true);
        }
    }

    private void doConnect(final T4Properties t4props) throws SQLException {
	    long start = 0;
	    String t4Url = t4props.getT4Url();
	    TrafCheckActiveMaster activeMaster;
	    String masterAddr;
	    int masterIndex = 0;
	    StringBuilder logBuilder;

	    //do check Active Master
	    TrafCheckActiveMaster.checkTrafCheckActiveMaster(t4props);
	    activeMaster = TrafCheckActiveMaster.getActiveMasterCacheMap().get(t4Url);
	    masterAddr = activeMaster.getDcsMaster(this, true);
	    //do connect
	    try {
		    setActiveMasterIP(masterAddr);
		    if (t4props.isTraceTransTime()) {
			    start = System.currentTimeMillis();
		    }
		    if (TRANSPORT.ACTIVE_MASTER_REPLY_ERROR.equals(masterAddr) || masterAddr == null) {
			    //DCS does not contain Check master API
			    ic_.connect();
		    } else {
			    logBuilder = new StringBuilder("URL : <").append(t4Url).append(">, masterAddr : <")
					    .append(masterAddr).append(">");
			    if (getT4props().isLogEnable(Level.FINER)) {
				    T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY", logBuilder.toString());
			    }
			    if (LOG.isDebugEnabled()) {
				    LOG.debug("ENTRY. Do Connect, URL : <{}>， masterAddr : <{}>", t4Url,
						    masterAddr);
			    }
			    ic_.connect(T4Address.extractUrlListFromUrl(t4Url, masterAddr));
		    }
	    } catch (SQLException e) {
		    String errorMsg;
		    switch (e.getErrorCode()) {
			    case -29230:
				    errorMsg = "Distribution server error, and need to retry";
				    break;
			    case -29228:
				    errorMsg = "Connect to dcs timeout";
				    break;
			    case -29117:
				    errorMsg = "Error while closing session";
				    break;
			    case -29113:
				    errorMsg = "Error while opening socket";
				    break;
			    default:
			    	logBuilder = new StringBuilder("Do connect error, URL: <").append(t4Url)
						    .append(">, masterAddr: <").append(masterAddr).append(">, errorMess:<")
						    .append(e.getMessage()).append(">");
				    if (getT4props().isLogEnable(Level.WARNING)) {
					    T4LoggingUtilities.log(getT4props(), Level.WARNING, "ENTRY", logBuilder.toString());
				    }
				    if (LOG.isWarnEnabled()) {
					    LOG.warn(
							    "ENTRY. Do connect error, URL: <{}>, masterAddr: <{}>, errorMess: <{}>",
							    t4Url, masterAddr, e.getMessage(), e);
				    }
				    throw e;
		    }
		    logBuilder = new StringBuilder(errorMsg).append(", URL:<").append(t4Url)
				    .append(">, old masterAddr:<").append(masterAddr).append(">,  errorMess:<")
				    .append(e.getMessage()).append(">");
		    if (getT4props().isLogEnable(Level.WARNING)) {
			    T4LoggingUtilities
					    .log(getT4props(), Level.WARNING, "ENTRY", logBuilder.toString());
		    }
		    if (LOG.isWarnEnabled()) {
			    LOG.warn("ENTRY. {}, URL:<{}>,old masterAddr:<{}>, errorMess:<{}>", errorMsg, t4Url,
					    masterAddr, e.getMessage(),
					    e);
		    }
		    List<String> masterList = activeMaster.getMasterList();
		    for (int i = 0; i < masterList.size(); i++) {
			    if (masterAddr.equals(masterList.get(i))) {
				    masterIndex = i;
				    break;
			    }
		    }
		    logBuilder = new StringBuilder("masterList:<").append(masterList)
				    .append(">, masterIndex: <").append(masterIndex).append(">");
		    if (getT4props().isLogEnable(Level.FINER)) {
			    T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY", logBuilder.toString());
		    }
		    if (LOG.isDebugEnabled()) {
			    LOG.debug("ENTRY. masterList:<{}>, masterIndex:<{}>", masterList, masterIndex);
		    }
		    //do Retry connect
		    boolean connSuccess = false;
		    int outerRetry = 1;
		    while (!connSuccess && outerRetry <= 10) {
			    if ((masterIndex + 1) % masterList.size() == 0) {
				    masterIndex = 0;
			    } else {
				    masterIndex++;
			    }
			    masterAddr = masterList.get(masterIndex);
			    logBuilder = new StringBuilder("Re connect enter,masterAddr:<").append(masterAddr)
					    .append(">,retryCount:<").append(outerRetry).append(">,url:<").append(t4Url)
					    .append(">,").append("masterList:<").append(masterList).append(">");
			    if (getT4props().isLogEnable(Level.WARNING)) {
				    T4LoggingUtilities.log(getT4props(), Level.WARNING, "ENTRY", logBuilder.toString());
			    }
			    if (LOG.isWarnEnabled()) {
				    LOG.warn("ENTRY. Re connect enter,masterAddr:<{}>,retryCount:<{}>,url:<{}>,masterList:<{}>",
						    masterAddr, outerRetry, t4Url,masterList);
			    }
			    try {
				    checkLoginTimeout(getT4props());
				    ic_ = new InterfaceConnection(this, t4props);
				    if ((TRANSPORT.ACTIVE_MASTER_REPLY_ERROR.equals(masterAddr))) {
					    //DCS does not contain Check master API
					    ic_.connect();
					    connSuccess = true;
				    } else {
					    connSuccess = ic_.connect(
							    T4Address.extractUrlListFromUrl(t4Url, masterAddr));
				    }
			    } catch (SQLException e1) {
				    switch (e1.getErrorCode()) {
					    case -29230:
					    case -29228:
					    case -29117:
					    case -29113:
						    e = e1;
						    break;
					    default:
						    throw e1;
				    }
			    }
			    outerRetry++;
		    }
		    if (!connSuccess) {
			    if (e.getErrorCode() == -29228) {
				    throw TrafT4Messages
						    .createSQLException(getT4props(), "connect_dcs_failed",
								    outerRetry);
			    }
			    throw e;
		    }
	    }
	    if (t4props.isTraceTransTime()) {
		    long end = System.currentTimeMillis();
		    traceQueryUsedTime = new TraceQueryUsedTime("conn", "conn", start, end,
				    (short) 3);
		    traceAverageUsedTime = new TraceAverageUsedTime();
	    }
    }

    public boolean isUseCacheTable(){
		return this._isUseCache;
    }
	private boolean _isUseCache = false;
	/*
	 * JDK 1.6 functions public Clob createClob() throws SQLException { return
	 * null; }
	 *
	 *
	 * public Blob createBlob() throws SQLException { return null; }
	 *
	 *
	 * public NClob createNClob() throws SQLException { return null; }
	 *
	 *
	 * public SQLXML createSQLXML() throws SQLException { return null; }
	 *
	 *
	 * public boolean isValid(int _int) throws SQLException { return false; }
	 *
	 *
	 * public void setClientInfo(String string, String string1) throws
	 * SQLClientInfoException { }
	 *
	 *
	 * public void setClientInfo(Properties properties) throws
	 * SQLClientInfoException { }
	 *
	 *
	 * public String getClientInfo(String string) throws SQLException { return
	 * ""; }
	 *
	 *
	 * public Properties getClientInfo() throws SQLException { return null; }
	 *
	 *
	 * public Array createArrayOf(String string, Object[] objectArray) throws
	 * SQLException { return null; }
	 *
	 *
	 * public Struct createStruct(String string, Object[] objectArray) throws
	 * SQLException { return null; }
	 *
	 *
	 * public Object unwrap(Class _class) throws SQLException { return null; }
	 *
	 *
	 * public boolean isWrapperFor(Class _class) throws SQLException { return
	 * false; }
	 */
}
