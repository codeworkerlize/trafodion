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

import java.lang.ref.WeakReference;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.trace.TraceLinkThreshold;

/**
 * <p>
 * JDBC Type 4 TrafT4Statement class.
 * </p>
 * <p>
 * Description: The <code>TrafT4Statement</code> class is an implementation of
 * the <code>java.sql.Statement</code> interface.
 * </p>
 */
public class TrafT4Statement extends TrafT4Handle implements java.sql.Statement {
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4Statement.class);
    private TraceLinkThreshold traceLinkThreshold;
    private ArrayList<TraceLinkThreshold> traceLinkList;
	// java.sql.Statement interface Methods
    T4Properties  getT4props(){return connection_.getT4props();}
    private long heapSize;

    public void addBatch(String sql) throws SQLException{
	    if (getT4props().isLogEnable(Level.FINEST)) {
		    T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
	    }
	    if (LOG.isTraceEnabled()) {
		    LOG.trace("ENTRY. {}", sql);
	    }

	    if (batchCommands_ == null) {
		    batchCommands_ = new ArrayList<String>();
	    }


	    batchCommands_.add(sql);
    }

    public  void cancel() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
		    T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
	    }
	    if (LOG.isTraceEnabled()) {
		    LOG.trace("ENTRY");
	    }
	    // Donot clear warning, since the warning may pertain to
	    // previous opertation and it is not yet seen by the application
	    //

	    // if the statement is already closed
	    // No need to cancel the statement

	    if (isClosed_)
		    return;

	    ist_.cancel();
	    if (getT4props().isLogEnable(Level.FINEST)) {
		    T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
	    }
	    if (LOG.isTraceEnabled()) {
		    LOG.trace("LEAVE");
	    }
    }

	public void clearBatch() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
	if (batchCommands_ != null) {
	    batchCommands_.clear();
	}
	}

	synchronized public void close() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		if (isClosed_) {
			return;
		}

		try {
			if (connection_._isClosed() == false) {
				for (int i = 0; i < num_result_sets_; i++) {
					if (resultSet_[i] != null) {
						resultSet_[i].close(false);
					}
				}
				ist_.close();
			}
		} finally {
			isClosed_ = true;
			connection_.removeElement(pRef_);
			initResultSets();
		}
	}

	void initResultSets() {
		num_result_sets_ = 1;
		result_set_offset = 0;
		resultSet_[result_set_offset] = null;
	}

	// ------------------------------------------------------------------
	/**
	 * This method will execute an operation.
	 * 
	 * @return true
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */

    public boolean execute() throws SQLException {
        traceLinkList.clear();
        traceLinkThreshold.setEnterDriverTimestamp(System.currentTimeMillis());
        traceLinkThreshold.setOdbcAPI(this.getOperationID());
        try {
            ist_.executeDirect(queryTimeout_, this);
        } catch (SQLException se) {
            performConnectionErrorChecks(se);
            throw se;
        }
        traceLinkThreshold.setLeaveDriverTimestamp(System.currentTimeMillis());
        TraceLinkThreshold traceTmp = new TraceLinkThreshold();
        doCopyLinkThreshold(traceLinkThreshold, traceTmp);
        traceLinkList.add(traceTmp);
        traceLinkThreshold.checkLinkThreshold("TrafT4Statement execute", getT4props(), traceLinkList);
        traceLinkList.clear();
        return true;
    } // end execute

	// ------------------------------------------------------------------

    public boolean execute(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        traceLinkList.clear();
        traceLinkThreshold.setOdbcAPI(TRANSPORT.SRVR_API_SQLEXECDIRECT);
        traceLinkThreshold.setEnterDriverTimestamp(System.currentTimeMillis());
		if (validateExecDirectInvocation(sql)) {
			try {
				ist_.execute(TRANSPORT.SRVR_API_SQLEXECDIRECT, 0, 0, null, queryTimeout_, sql_,
						this);

				checkSQLWarningAndClose();
			} catch (SQLException se) {
				try {
					if (num_result_sets_ == 1 && resultSet_[result_set_offset] == null) {
						internalClose();
					}
				} catch (SQLException closeException) {
					se.setNextException(closeException);
				}
				performConnectionErrorChecks(se);
				throw se;
			}
		}
        traceLinkThreshold.setLeaveDriverTimestamp(System.currentTimeMillis());
		TraceLinkThreshold traceTmp = new TraceLinkThreshold();
		doCopyLinkThreshold(traceLinkThreshold, traceTmp);
		traceLinkList.add(traceTmp);
        traceLinkThreshold.checkLinkThreshold(sql_, getT4props(), traceLinkList);
        traceLinkList.clear();
        if (resultSet_[result_set_offset] != null) {
            return true;
        } else {
            return false;
        }
	}

	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, autoGeneratedKeys);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, autoGeneratedKeys);
        }
		boolean ret;

		if (autoGeneratedKeys == TrafT4Statement.NO_GENERATED_KEYS) {
			ret = execute(sql);
		} else {
			throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
		}
		return ret;
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, columnIndexes);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, columnIndexes);
        }
		boolean ret;

		if (columnIndexes == null) {
			ret = execute(sql);
		} else if (columnIndexes.length == 0) {
			ret = execute(sql);
		} else {
			throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
		}
		return ret;
	}

	public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, columnNames);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, columnNames);
        }
		boolean ret;

		if (columnNames == null) {
			ret = execute(sql);
		} else if (columnNames.length == 0) {
			ret = execute(sql);
		} else {
			throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
		}
		return ret;
	}

	public int[] executeBatch() throws SQLException, BatchUpdateException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        traceLinkList.clear();
        traceLinkThreshold.setBatchFlag(true);
        traceLinkThreshold.setOdbcAPI(TRANSPORT.SRVR_API_SQLEXECDIRECT);
        traceLinkThreshold.setEnterDriverTimestamp(System.currentTimeMillis());
		try {
			int i = 0;
			SQLException se;

			validateExecDirectInvocation();
			if ((batchCommands_ == null) || (batchCommands_.isEmpty())) {
				return new int[] {};
			}

			for (i = 0; i < batchCommands_.size(); i++) {
				String sql = (String) batchCommands_.get(i);

				if (sql == null) {
					se = TrafT4Messages.createSQLException(getT4props(),
							"batch_command_failed", "Invalid SQL String");
					throw new BatchUpdateException(se.getMessage(), se.getSQLState(), new int[0]);
				}

				sqlStmtType_ = ist_.getSqlStmtType(sql);
			}

			int[] batchRowCount = new int[batchCommands_.size()];
			String sql;
			int rowCount = 0;

			try {
				for (i = 0; i < batchCommands_.size(); i++) {
					sql = batchCommands_.get(i);

					if (validateExecDirectInvocation(sql)) {

						ist_.execute(TRANSPORT.SRVR_API_SQLEXECDIRECT, 0, 0, null, queryTimeout_, sql_, this);

						checkSQLWarningAndClose();

						batchRowCount[i] = (int) ist_.getRowCount(); // the member will
						// be set by
						// execute...keep
						// them in our local
						// array
						rowCount += ist_.getRowCount();
					}
				}
				// CTS requirement.
				if (batchCommands_.size() < 1) {
					batchRowCount = new int[] {};
				}
			} catch (SQLException e) {
				ist_.setRowCount(rowCount);
				batchRowCount_ = new int[i];
				System.arraycopy(batchRowCount, 0, batchRowCount_, 0, i);

				BatchUpdateException be;

				se = TrafT4Messages.createSQLException(getT4props(),
						"batch_command_failed", e.getMessage());
				be = new BatchUpdateException(se.getMessage(), se.getSQLState(), batchRowCount_);
				be.setNextException(e);

				try {
					if (resultSet_[result_set_offset] == null) {
						internalClose();
					}
				} catch (SQLException closeException) {
					be.setNextException(closeException);
				}
				performConnectionErrorChecks(e);

				throw be;
			}

			ist_.setRowCount(rowCount);
			batchRowCount_ = new int[i];
			System.arraycopy(batchRowCount, 0, batchRowCount_, 0, i);
			traceLinkThreshold.setLeaveDriverTimestamp(System.currentTimeMillis());
			TraceLinkThreshold traceTmp = new TraceLinkThreshold();
			doCopyLinkThreshold(traceLinkThreshold, traceTmp);
			traceLinkList.add(traceTmp);
			traceLinkThreshold.checkLinkThreshold("TrafT4Statement executeBatch ", getT4props(), traceLinkList);
			traceLinkList.clear();
			return batchRowCount_;
		} finally {
			clearBatch();
		}

	}

	public ResultSet executeQuery(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        traceLinkList.clear();
        traceLinkThreshold.setSelectFlag(true);
        traceLinkThreshold.setOdbcAPI(TRANSPORT.SRVR_API_SQLEXECDIRECT);
        traceLinkThreshold.setEnterDriverTimestamp(System.currentTimeMillis());
		if (validateExecDirectInvocation(sql)) {
			try {
				this.executeApiType = TRANSPORT.EXECUTEQUERY;
				ist_.execute(TRANSPORT.SRVR_API_SQLEXECDIRECT, 0, 0, null, queryTimeout_, sql_, this);
				checkSQLWarningAndClose();
			} catch (SQLException se) {
				try {
					if (resultSet_[result_set_offset] == null) {
						internalClose();
					}
				} catch (SQLException closeException) {
					se.setNextException(closeException);
				}
				performConnectionErrorChecks(se);
				throw se;
			}
			finally {
				this.executeApiType = TRANSPORT.NORMAL;
			}
		}
        if (traceLinkThreshold.isEndOfData()) {
            traceLinkThreshold.setLeaveDriverTimestamp(System.currentTimeMillis());
            TraceLinkThreshold traceTmp = new TraceLinkThreshold();
            doCopyLinkThreshold(traceLinkThreshold, traceTmp);
            traceLinkList.add(traceTmp);
            traceLinkThreshold.checkLinkThreshold(sql_, getT4props(), traceLinkList);
            traceLinkList.clear();
        } else {
            TraceLinkThreshold traceTmp = new TraceLinkThreshold();
            doCopyLinkThreshold(traceLinkThreshold, traceTmp);
            traceLinkList.add(traceTmp);
        }
		return resultSet_[result_set_offset];
	}

	public int executeUpdate(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
		long count = executeUpdate64(sql);

		if (count > Integer.MAX_VALUE)
			this.setSQLWarning(null, "numeric_out_of_range", null);

		return (int) count;
	}

	public long executeUpdate64(String sql) throws SQLException {

	    traceLinkList.clear();
	    traceLinkThreshold.setOdbcAPI(TRANSPORT.SRVR_API_SQLEXECDIRECT);
	    traceLinkThreshold.setEnterDriverTimestamp(System.currentTimeMillis());
		if (validateExecDirectInvocation(sql)) {
			try {
				ist_.execute(TRANSPORT.SRVR_API_SQLEXECDIRECT, 0, 0, null, queryTimeout_, sql_, this);

				checkSQLWarningAndClose();
			} catch (SQLException se) {
				try {
					if (resultSet_[result_set_offset] == null) {
						internalClose();
					}
				} catch (SQLException closeException) {
					se.setNextException(closeException);
				}
				performConnectionErrorChecks(se);
				throw se;
			}
		}
		traceLinkThreshold.setLeaveDriverTimestamp(System.currentTimeMillis());
		TraceLinkThreshold traceTmp = new TraceLinkThreshold();
		doCopyLinkThreshold(traceLinkThreshold, traceTmp);
		traceLinkList.add(traceTmp);
		traceLinkThreshold.checkLinkThreshold(sql_, getT4props(), traceLinkList);
		traceLinkList.clear();
		return ist_.getRowCount();
	}

	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, autoGeneratedKeys);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, autoGeneratedKeys);
        }
		int ret;

		if (autoGeneratedKeys == TrafT4Statement.NO_GENERATED_KEYS) {
			ret = executeUpdate(sql);
		} else {
			throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
		}
		return ret;
	}

	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, columnIndexes);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, columnIndexes);
        }
		int ret;

		if (columnIndexes == null) {
			ret = executeUpdate(sql);
		} else if (columnIndexes.length == 0) {
			ret = executeUpdate(sql);
		} else {
			throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
		}
		return ret;
	}

	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, columnNames);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", sql, columnNames);
        }
		int ret;

		if (columnNames == null) {
			ret = executeUpdate(sql);
		} else if (columnNames.length == 0) {
			ret = executeUpdate(sql);
		} else {
			throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
		}
		return ret;
	}

	public Connection getConnection() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		return connection_;
	}

	public int getFetchDirection() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		return fetchDirection_;
	}

	public int getFetchSize() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		return fetchSize_;
	}

	public ResultSet getGeneratedKeys() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throw TrafT4Messages.createSQLException(getT4props(), "auto_generated_keys_not_supported");
	}

	public int getMaxFieldSize() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		return maxFieldSize_;
	}

	public int getMaxRows() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		return maxRows_;
	}

	public boolean getMoreResults() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
	}

	public boolean getMoreResults(int current) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		switch (current) {
		case Statement.CLOSE_ALL_RESULTS:
			for (int i = 0; i <= result_set_offset; i++) {
				if (resultSet_[i] != null) {
					resultSet_[i].close();
				}
			}
			break;
		case Statement.KEEP_CURRENT_RESULT:
			break;
		case Statement.CLOSE_CURRENT_RESULT: // this is the default action
		default:
			if (resultSet_[result_set_offset] != null) {
				resultSet_[result_set_offset].close();
			}
			break;
		}
		ist_.setRowCount(-1);
		if (result_set_offset < num_result_sets_ - 1) {
			result_set_offset++;
			return true;
		}
		return false;
	}

	public int getQueryTimeout() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		return queryTimeout_;
	}

	public ResultSet getResultSet() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		return resultSet_[result_set_offset];
	}

	public int getResultSetConcurrency() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		return resultSetConcurrency_;
	}

	public int getResultSetHoldability() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		return resultSetHoldability_;
	}

	public int getResultSetType() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		return resultSetType_;
	}

	public int getUpdateCount() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		long count = getUpdateCount64();

		if (count > Integer.MAX_VALUE)
			this.setSQLWarning(null, "numeric_out_of_range", null);

		return (int) count;
	}

	public long getUpdateCount64() throws SQLException {
		if (ist_ == null) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_statement_handle");
		}

		// Spec wants -1 when the resultset is current and no more rows.
		long count = ist_.getRowCount();
		if (resultSet_ != null && resultSet_[result_set_offset] != null) {
			count = -1;
		}
		return count;
	}

	// ------------------------------------------------------------------
	/**
	 * This method will get the operation ID for this statement. -1 is returned
	 * if the operation ID has not been set.
	 * 
	 * @retrun The operation ID or -1 if the operation ID has not been set.
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	public short getOperationID() throws SQLException {
		return operationID_;
	} // end getOperationID

	// ------------------------------------------------------------------
	/**
	 * This method will get the operation buffer for this statement. Null is
	 * returned if the operation buffer has not been set.
	 * 
	 * @retrun The operation buffer or null if the operation ID has not been
	 *         set.
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	public byte[] getOperationBuffer() throws SQLException {
		// System.out.println("in getOperation");
		return operationBuffer_;
	}

	// ------------------------------------------------------------------
	/**
	 * This method will get the operation reply buffer for this statement. Null
	 * is returned if the operation reply buffer has not been set.
	 * 
	 * @retrun The operation reply buffer or null.
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	public byte[] getOperationReplyBuffer() throws SQLException {
		// System.out.println("in getOperationReplyBuffer");
		return operationReply_;
	}

	// ------------------------------------------------------------------

	public void setCursorName(String name) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", name);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", name);
        }
		// TODO: May need to check the Statement STATE
		cursorName_ = name;
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", enable);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", enable);
        }

		escapeProcess_ = enable;

	}

	public void setFetchDirection(int direction) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", direction);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", direction);
        }

		switch (direction) {
		case ResultSet.FETCH_FORWARD:
			fetchDirection_ = direction;
			break;
		case ResultSet.FETCH_REVERSE:
		case ResultSet.FETCH_UNKNOWN:
			fetchDirection_ = ResultSet.FETCH_FORWARD;
			break;
		default:
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_fetch_direction");
		}
	}

	public void setFetchSize(int rows) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", rows);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", rows);
        }

		if (rows < 0) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_fetch_size");
		} else if (rows == 0) {
			fetchSize_ = TrafT4ResultSet.DEFAULT_FETCH_SIZE;
		} else {
			fetchSize_ = rows;
		}
	}

	public void setMaxFieldSize(int max) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", max);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", max);
        }

		if (max < 0) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_maxFieldSize_value");
		}
		maxFieldSize_ = max;
	}

	public void setMaxRows(int max) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", max);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", max);
        }

		if (max < 0) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_maxRows_value");
		}
		maxRows_ = max;
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		if (getT4props().isLogEnable(Level.FINEST)) {
			T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", seconds,
					"ignoreCancel:" + getT4props().isIgnoreCancel());
		}
		if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", seconds);
        }
		if (getT4props().isIgnoreCancel()) {
			return;
		}
		//TrafT4Messages.throwUnsupportedFeatureException(connection_.props_, connection_.getLocale(), "setQueryTimeout()");
		
		if (seconds < 0) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_queryTimeout_value");
		}
		queryTimeout_ = seconds;
        ist_.setQueryTimeout(seconds);
	}

	// ------------------------------------------------------------------
	/**
	 * This method will set the operation ID for this statement.
	 * 
	 * @param opID
	 *            the operation ID to associate with this statement.
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	public void setOperationID(short opID) throws SQLException {
		operationID_ = opID;
	} // end setOperationID

	// ------------------------------------------------------------------
	/**
	 * This method will set the operation buffer for this statement.
	 * 
	 * @param The
	 *            operation buffer.
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	public void setOperationBuffer(byte[] opBuffer) throws SQLException {
		operationBuffer_ = opBuffer;
	}


	boolean validateExecDirectInvocation(String sql) throws SQLException {
		if (sql != null && sql.trim().length() != 0) {
			if (getT4props().isLogEnable(Level.FINEST)) {
				T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
			}
			if (LOG.isTraceEnabled()) {
				LOG.trace("ENTRY. {}", sql);
			}

			validateExecDirectInvocation();
			sqlStmtType_ = ist_.getSqlStmtType(sql);
			sql_ = sql;
			return true;
		} else {
			return false;
		}
	}

	void validateExecDirectInvocation() throws SQLException {
		if (getT4props().isLogEnable(Level.FINEST)) {
			T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("ENTRY");
		}

		ist_.setRowCount(-1);
		clearWarnings();
		if (isClosed_) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_statement");
		}
		try {
			// connection_.getServerHandle().isConnectionOpen();
			connection_.isConnectionOpen();

			// close the previous resultset, if any
			for (int i = 0; i < num_result_sets_; i++) {
				if (resultSet_[i] != null) {
					resultSet_[i].close();
				}
			}
		} catch (SQLException se) {
			performConnectionErrorChecks(se);
			throw se;
		}
	}
	
	// This functions ensure that Database Resources are cleaned up,
	// but leave the java Statement object
	// intact.
	void internalClose() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		if (connection_._isClosed() == false) {
			ist_.close();
		}
	}

	protected void setResultSet(TrafT4Desc[] outputDesc) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		initResultSets();
		if (outputDesc != null) {
			resultSet_[result_set_offset] = new TrafT4ResultSet(this, outputDesc);
		} else {
			resultSet_[result_set_offset] = null;
		}
	}

	protected void setTransactionToJoin(byte[] txid) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", new Object[] {txid});
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", new Object[] {txid});
        }

		this.transactionToJoin = txid;
	}

	void setMultipleResultSets(int num_result_sets, TrafT4Desc[][] output_descriptors, String[] stmt_labels,
			String[] proxySyntax) throws SQLException {
		if (num_result_sets < 1)
			return;

		resultSet_ = new TrafT4ResultSet[num_result_sets];
		num_result_sets_ = num_result_sets;
		for (int i = 0; i < num_result_sets; i++) {
			TrafT4Desc[] desc = output_descriptors[i];
			if (desc == null) {
				resultSet_[i] = null;
			} else {
				resultSet_[i] = new TrafT4ResultSet(this, desc, stmt_labels[i], 
                                        (ist_.getSqlQueryType() == TRANSPORT.SQL_CALL_WITH_RESULT_SETS ||
                                         ist_.getSqlQueryType() == TRANSPORT.SQL_CALL_NO_RESULT_SETS));
				resultSet_[i].proxySyntax_ = proxySyntax[i];
			}
		}
	}

	// ----------------------------------------------------------------------------------
	void setExecute2Outputs(byte[] values, short rowsAffected, boolean endOfData, String[] proxySyntax, TrafT4Desc[] desc)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", rowsAffected, endOfData,
                    proxySyntax, desc);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", rowsAffected, endOfData, proxySyntax, desc);
        }
        if (getT4props().isLogEnable(Level.FINEST) && values.length < 1024 * 1024) {
            // too large value will lead to OutOfMemoryError
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", values);
        }
        if (LOG.isTraceEnabled() && values.length < 1024 * 1024) {
            LOG.trace("ENTRY. {}", values);
        }
		num_result_sets_ = 1;
		result_set_offset = 0;

		// if NO DATA FOUND is returned from the server, desc = null but
		// we still want to save our descriptors from PREPARE
		if (desc != null)
			outputDesc_ = desc;

		resultSet_ = new TrafT4ResultSet[num_result_sets_];

		if (outputDesc_ != null) {
			resultSet_[result_set_offset] = new TrafT4ResultSet(this, outputDesc_);
			resultSet_[result_set_offset].proxySyntax_ = proxySyntax[result_set_offset];

			if (rowsAffected == 0) {
				if (endOfData == true) {
					resultSet_[result_set_offset].setFetchOutputs(new ObjectArray[0], 0, true);
				}
			} else {
				 if(resultSet_[result_set_offset].keepRawBuffer_ == true)
			          resultSet_[result_set_offset].rawBuffer_ = values;
				//if setExecute2FetchOutputs is not called by fetch set flag to false , data has not been clipped 
				resultSet_[result_set_offset].irs_.setExecute2FetchOutputs(resultSet_[result_set_offset], rowsAffected, endOfData,
						values);
			}
		} else {
			resultSet_[result_set_offset] = null;
		}
	}

	// Constructors with access specifier as "default"
	TrafT4Statement() {
		if (T4Properties.t4GlobalLogger.isLoggable(Level.FINEST) == true) {
			T4Properties.t4GlobalLogger.logp(Level.FINEST, "TrafT4Statement", "<init>", "");
		}
		if (LOG.isTraceEnabled()) {
		    LOG.trace("TrafT4Statement, <init>");
		}
		resultSet_ = new TrafT4ResultSet[1];
		initResultSets();
	}

	/*
	 * * For closing statements using label.
	 */
	TrafT4Statement(TrafT4Connection connection, String stmtLabel) throws SQLException {
	    this(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                TrafT4ResultSet.CLOSE_CURSORS_AT_COMMIT, stmtLabel);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
	}

	TrafT4Statement(TrafT4Connection connection) throws SQLException {
        this(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                TrafT4ResultSet.CLOSE_CURSORS_AT_COMMIT, null);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
	}

	TrafT4Statement(TrafT4Connection connection, int resultSetType, int resultSetConcurrency) throws SQLException {
		this(connection, resultSetType, resultSetConcurrency, TrafT4ResultSet.CLOSE_CURSORS_AT_COMMIT, null);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
	}
	TrafT4Statement(TrafT4Connection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability,
			String stmtLabel) throws SQLException {
        connection_ = connection;
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", connection, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY && resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE
		        && resultSetType != ResultSet.TYPE_SCROLL_SENSITIVE) {
		    throw TrafT4Messages.createSQLException(getT4props(), "invalid_resultset_type", resultSetType);
		}
		if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY && resultSetConcurrency != ResultSet.CONCUR_UPDATABLE) {
		    throw TrafT4Messages.createSQLException(getT4props(), "invalid_resultset_concurrency", resultSetConcurrency);
		}
		if ((resultSetHoldability != 0) && (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
		        && (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
		    throw TrafT4Messages.createSQLException(getT4props(), "invalid_holdability", resultSetHoldability);
		}

		if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
		    resultSetType_ = ResultSet.TYPE_SCROLL_INSENSITIVE;
		    connection.setSQLWarning(null, "scrollResultSetChanged", null);
		    //setSQLWarning(null, "scrollResultSetChanged", null);
		} else {
		    resultSetType_ = resultSetType;
		}
		operationID_ = -1;

		resultSetConcurrency_ = resultSetConcurrency;
		resultSetHoldability_ = resultSetHoldability;
		queryTimeout_ = connection_.getServerHandle().getQueryTimeout();
		ddlTimeout_ = connection_.getServerHandle().getDDLTimeout();

		if (stmtLabel == null || stmtLabel.length() == 0) {
            stmtLabel_ = generateStmtLabel();
        } else {
            stmtLabel_ = stmtLabel;
        }
		fetchSize_ = TrafT4ResultSet.DEFAULT_FETCH_SIZE;
		maxRows_ = 0;
		fetchDirection_ = ResultSet.FETCH_FORWARD;

		connection_.gcStmts();
		pRef_ = new WeakReference(this, connection_.refStmtQ_);
		ist_ = new InterfaceStatement(this);
		connection_.addElement(pRef_, new Object[]{stmtLabel_,(this instanceof TrafT4PreparedStatement)});
		roundingMode_ = getT4props().getRoundingMode();

        resultSet_ = new TrafT4ResultSet[1];
        if (stmtLabel == null || stmtLabel.length() == 0) {
            initResultSets();
        }
        traceLinkThreshold = new TraceLinkThreshold();
        traceLinkList = new ArrayList<TraceLinkThreshold>();
	}

	TrafT4Statement(TrafT4Connection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
	    this(connection, resultSetType, resultSetConcurrency, resultSetHoldability, null);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
	}

	//max length for a label is 32 characters.
    String generateStmtLabel() {
        String id = String.valueOf(this.connection_.ic_.getSequenceNumber());
        if (id.length() > 24) {
            id = id.substring(id.length() - 24);
        }
        String label = "SQL_CUR_" + id;
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "Generated stmt label is " + label);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Generated stmt label is {}", label);
        }
        return label;
    }
	
	// Database statement are not deallocated when there is a
	// SQLWarning or SQLException or when a resultSet is produced
	void checkSQLWarningAndClose() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (sqlWarning_ != null) {
			if (resultSet_[result_set_offset] == null) {
				try {
					internalClose();
				} catch (SQLException closeException1) {
				}
			}
		}
	}

	public void setRoundingMode(int roundingMode) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", roundingMode);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", roundingMode);
        }
		roundingMode_ = Utility.getRoundingMode(roundingMode);
	}

	public void setRoundingMode(String roundingMode) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", roundingMode);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", roundingMode);
        }
		roundingMode_ = Utility.getRoundingMode(roundingMode);
	}

	void closeErroredConnection(TrafT4Exception sme) {
		connection_.closeErroredConnection(sme);
	}

	/**
	 * Use this method to retrieve the statement-label name that was used when
	 * creating the statement through the Trafodion connectivity service. You can
	 * subsequently use the name retrieved as the cursor name when invoking
	 * INFOSTATS to gather resource statistics through either the
	 * <code>executeQuery(String sql)</code> or
	 * <code>execute(String sql)</code> methods.
	 */
	public String getStatementLabel() {
		return new String(stmtLabel_);
	}

	/**
	 * Returns the raw SQL associated with the statement
	 * 
	 * @return the SQL text
	 */
	public String getSQL() {
		return this.sql_;
	}

	/**
	 * Returns the MXCS statement handle
	 * 
	 * @return the MXCS statement handle
	 */
	public int getStmtHandle() {
		return this.ist_.getStmtHandle();
	}

	// static fields
	public static final int NO_GENERATED_KEYS = 2;
	// Fields

	TrafT4Connection connection_;
	int resultSetType_;
	int resultSetConcurrency_;
	String sql_;
	int queryTimeout_;
	int ddlTimeout_;
	int maxRows_;
	int maxFieldSize_;
	int fetchSize_;
	int fetchDirection_;
	boolean escapeProcess_;
	String cursorName_ = "";
	TrafT4ResultSet[] resultSet_; // Added for SPJ RS - SB 11/21/2005
	int num_result_sets_; // Added for SPJ RS - SB 11/21/2005
	int result_set_offset; // Added for SPJ RS - SB 11/21/2005
	String stmtLabel_;
	short sqlStmtType_;
	boolean isClosed_;
	ArrayList<String> batchCommands_;
	int[] batchRowCount_;
	WeakReference pRef_;
	int resultSetHoldability_;
	InterfaceStatement ist_;

	int inputParamsLength_;
	int outputParamsLength_;
	int inputDescLength_;
	int outputDescLength_;

	int inputParamCount_;
	int outputParamCount_;

	int roundingMode_ = BigDecimal.ROUND_HALF_EVEN;

	TrafT4Desc[] inputDesc_, outputDesc_;

	short operationID_;
	byte[] operationBuffer_;
	byte[] operationReply_;

	boolean usingRawRowset_;
	ByteBuffer rowwiseRowsetBuffer_;

	byte[] transactionToJoin;
	
	int _lastCount = -1;
	int executeApiType = TRANSPORT.NORMAL;

	/**
	 * @return the inputParamsLength_
	 */
	public int getInputParamsLength_() {
		return inputParamsLength_;
	}

	/**
	 * @return the outputParamsLength_
	 */
	public int getOutputParamsLength_() {
		return outputParamsLength_;
	}

	public Object unwrap(Class iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isWrapperFor(Class iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
                return isClosed_;
	}

	public void setPoolable(boolean poolable) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public boolean isPoolable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	protected void updateStmtIc(InterfaceConnection ic) {
        this.ist_.getT4statement().updateConnContext(ic);
    }
    
	/**
	 * datatype defined by {@link java.sql.Types}
	 * @param index
	 * @return
	 */
    protected int getInputDataType(int index) {
        return inputDesc_[index].dataType_;
    }
	protected int getSqlPrecision(int index) {
		return inputDesc_[index].sqlPrecision_;
	}

    protected int getInputPrecision(int index) {
        return inputDesc_[index].precision_;
    }

    protected int getInputScale(int index) {
        return inputDesc_[index].scale_;
    }

    protected int getInputSqlDatetimeCode(int index) {
        return inputDesc_[index].sqlDatetimeCode_;
    }

    protected int getInputFsDataType(int index) {
        return inputDesc_[index].fsDataType_;
    }

    protected boolean getInputSigned(int index) {
        return inputDesc_[index].isSigned_;
    }

    /**
     * maxLength
     * @param index
     * @return
     */
    protected int getInputSqlOctetLength(int index) {
        return inputDesc_[index].sqlOctetLength_;
    }

    /**
     * datatype defined by our db
     * @param index
     * @return
     */
    protected int getInputSqlDataType(int index) {
        return inputDesc_[index].sqlDataType_;
    }

    protected int getInputSqlCharset(int index) {
        return inputDesc_[index].sqlCharset_;
    }

    protected int getInputNoNullValue(int index) {
        return inputDesc_[index].noNullValue_;
    }

    protected int getInputNullValue(int index) {
        return inputDesc_[index].nullValue_;
    }

    protected int getInputMaxLen(int index) {
        return inputDesc_[index].maxLen_;
    }

    protected boolean isInputSigned(int index) {
        return inputDesc_[index].isSigned_;
    }

    protected String getInputName(int index) {
        return inputDesc_[index].name_;
    }

    protected int getInputLobInlineMaxLen(int index) {
        return inputDesc_[index].lobInlineMaxLen_;
    }

    protected int getInputLobChunkMaxLen(int index) {
        return inputDesc_[index].lobChunkMaxLen_;
    }

    protected int getInputLobVersion(int index) {
        return inputDesc_[index].lobVersion_;
    }

    protected String getInputHeadingName(int index) {
        return inputDesc_[index].headingName_;
    }

    protected int getOutputLobInlineMaxLen(int index) {
        return outputDesc_[index].lobInlineMaxLen_;
    }

    protected int getOutputLobChunkMaxLen(int index) {
        return outputDesc_[index].lobChunkMaxLen_;
    }

    protected int getOutputLobVersion(int index) {
        return outputDesc_[index].lobVersion_;
    }

    protected String getOutputColumnLabel(int index) {
        return outputDesc_[index].columnLabel_;
    }

    protected TraceLinkThreshold getTraceLinkThreshold() {
        return traceLinkThreshold;
    }

    protected ArrayList<TraceLinkThreshold> getTraceLinkList() {
        return traceLinkList;
    }

    protected void doCopyLinkThreshold (TraceLinkThreshold src, TraceLinkThreshold target) {
        target.setEnterDriverTimestamp(src.getEnterDriverTimestamp());
        target.setSendToMxoTimestamp(src.getSendToMxoTimestamp());
        target.setEnterMxoTimestamp(src.getEnterMxoTimestamp());
        target.setEnterEngineTimestamp(src.getEnterEngineTimestamp());
        target.setLeaveEngineTimestamp(src.getLeaveEngineTimestamp());
        target.setLeaveMxoTimestamp(src.getLeaveMxoTimestamp());
        target.setRecvFromMxoTimestamp(src.getRecvFromMxoTimestamp());
        target.setLeaveDriverTimestamp(src.getLeaveDriverTimestamp());
        target.setOdbcAPI(src.getOdbcAPI());
        target.setSelectFlag(src.isSelectFlag());
        target.setBatchFlag(src.isBatchFlag());
    }

    protected void setHeapSize(long usedSize){
        this.heapSize = usedSize;
    }

    protected long getHeapSize(){
        return this.heapSize;
    }
}
