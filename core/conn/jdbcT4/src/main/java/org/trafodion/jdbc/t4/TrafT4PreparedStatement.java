// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional tracermation
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import java.util.regex.Pattern;
import org.trafodion.jdbc.t4.tmpl.*;
import org.trafodion.jdbc.t4.trace.TraceLinkThreshold;
import org.trafodion.jdbc.t4.trace.TraceQueryUsedTime;
import org.trafodion.jdbc.t4.utils.JDBCType;
import org.trafodion.jdbc.t4.h2Cache.TrafCachePool;
// java.sql.PreparedStatement interface methods
public class TrafT4PreparedStatement extends TrafT4Statement implements java.sql.PreparedStatement {
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4PreparedStatement.class);
    private TraceLinkThreshold traceLinkThreshold;
    private ArrayList<TraceLinkThreshold> traceLinkList;
    private boolean batchFlag = false;
    private int startRowNum = 0;
    private int[] realBatchResults;

	public void addBatch() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		if (inputDesc_ == null) {
			return;
		}

		// Check if all parameters are set for current set
		checkIfAllParamsSet();
		// Add to the number of Rows Count
		if (rowsValue_ == null) {
			rowsValue_ = new ArrayList<Object[]>();
		}
		rowsValue_.add(paramsValue_);
		paramRowCount_++;
		paramsValue_ = new Object[inputDesc_.length];
        // Clear the isValueSet_ and paramValue flag in inputDesc_
        for (int i = 0; i < inputDesc_.length; i++) {
            inputDesc_[i].paramValue_ = null;
            inputDesc_[i].isValueSet_ = false;
        }
        if (isAnyLob) {
            addLobColNames  = true;
        }

	}

    public void clearBatch() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

        if (inputDesc_ == null) {
            return;
        }
        if (rowsValue_ != null) {
            rowsValue_.clear();
        }
        clearLobObjects();
        paramRowCount_ = 0;
        startRowNum = 0;
        // Clear the isValueSet_ flag in inputDesc_
        for (int i = 0; i < inputDesc_.length; i++) {
            inputDesc_[i].isValueSet_ = false;
            paramsValue_[i] = null;
            inputDesc_[i].paramValue_ = null;
        }
        batchRowCount_ = new int[] {};
    }

	public void clearParameters() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (isClosed_) {
            throw TrafT4Messages.createSQLException(getT4props(), "stmt_closed");
        }
		// Clear the isValueSet_ flag in inputDesc_
		if (inputDesc_ == null) {
			return;
		}

		for (int i = 0; i < inputDesc_.length; i++) {
			inputDesc_[i].isValueSet_ = false;
			paramsValue_[i] = null;
			inputDesc_[i].paramValue_ = null;
		} 
        if (lobObjects != null)
            lobObjects.clear();
        isAnyLob = false;
	}

	public void close() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "Close PreparedStatement", this.stmtLabel_);
        }
	    if (LOG.isDebugEnabled()) {
            LOG.debug("Close PreparedStatement {}", this.stmtLabel_);
        }

		try {
		    if (isClosed_) {
		        return;
		    }
			if (connection_._isClosed() == false) {
                if (getT4props().isLogEnable(Level.FINER)) {
                    T4LoggingUtilities.log(getT4props(), Level.FINER, "pstmt used heap size = "+ getHeapSize()+", sql = " + sql_);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("pstmt used heap size = <{}>, sql = <{}>", getHeapSize(), sql_);
                }
				if (!connection_.isStatementCachingEnabled() || (getHeapSize() > getT4props().getStmtMaxUsedSize() && getT4props().getStmtMaxUsedSize() != 0)) {
                    if (getT4props().isLogEnable(Level.FINER)) {
                        String tmp = "close preparedStetement: " + this.stmtLabel_;
                        T4LoggingUtilities.log(getT4props(), Level.FINER, tmp);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("close preparedStetement: {}", this.stmtLabel_);
                    }
					super.close();
                    connection_.removePreparedStatement(connection_,this, resultSetHoldability_);
				} else {
					logicalClose();
				}
			}
		} catch (SQLException e) {
			performConnectionErrorChecks(e);
			throw e;
		} finally {
			isClosed_ = true;
			if (!connection_.isStatementCachingEnabled()) {
				connection_.removeElement(pRef_);
			}
            if (getT4props().isLogEnable(Level.FINEST)) {
                T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("LEAVE");
            }
		}

	}

    public boolean execute() throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("ENTRY. {}", stmtLabel_);
        }
		Object[] valueArray = null;
		int inDescLength = 0;

        validateExecuteInvocation();

        // *******************************************************************
        // * If the setAutoCommit is set to false by the application, then there is no issue. 
        // * If the setAutoCommit to true,  either JDBC driver or MXOSRVR needs to modify the behavior as if it is auto-commit off, 
        // * do the needed work (insert into base table and the lob table) and then commit or rollback work.
        // *******************************************************************
        if (isAnyLob ) {
            if (outputDesc_ != null) {
                throw TrafT4Messages.createSQLException(getT4props(), "lob_as_param_not_support");
            }
            boolean shouldChangeAutoCommit = connection_.getAutoCommit();
            if(shouldChangeAutoCommit) {
                connection_.setAutoCommit(false);
            }

            try {
                lobLocators = getLobHandles();
                setLobHandles();
                populateLobObjects();
                connection_.commit();
            } catch (SQLException e) {
                connection_.rollback();
                throw e;
            } finally {
                if(shouldChangeAutoCommit) {
                    connection_.setAutoCommit(true);
                }
            }
            return false;
        } else {
            if (inputDesc_ != null) {
                if (!usingRawRowset_)
                    valueArray = getValueArray();
                inDescLength = inputDesc_.length;
            }
            execute(paramRowCount_ + 1, inDescLength, valueArray, queryTimeout_);
            if (resultSet_ != null && resultSet_[result_set_offset] != null) {
                return true;
            } else {
                return false;
            }
        }

    }

    public int[] executeBatch() throws SQLException, BatchUpdateException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        realBatchResults = new int[paramRowCount_];
        try {
            clearWarnings();
            TrafT4Exception se;
            Object[] valueArray = null;
            if (isClosed_) {
                throw TrafT4Messages.createSQLException(getT4props(), "stmt_closed");
            }
            if (inputDesc_ == null) {
                this.execute();
                int count;
                long  rowCount= ist_.getRowCount();
                if (rowCount > (long) Integer.MIN_VALUE - 1 && rowCount < (long) Integer.MAX_VALUE + 1) {
                    count = (int) rowCount;
                } else {
                    throw TrafT4Messages.createSQLException(getT4props(), "numeric_out_of_range",
                            rowCount);
                }
                return (new int[] {count});
            }
            if (connection_._isClosed()) {
                se = TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
                connection_.closeErroredConnection(se);
                throw new BatchUpdateException(se.getMessage(), se.getSQLState(), new int[0]);
            }
            if (isAnyLob) {
                if (outputDesc_ != null) {
                    throw TrafT4Messages.createSQLException(getT4props(), "lob_as_param_not_support");
                }
                boolean shouldChangeAutoCommit = connection_.getAutoCommit();
                if (shouldChangeAutoCommit) {
                    connection_.setAutoCommit(false);
                }

                try {
                    lobLocators = getLobHandles();
                    setLobHandles();
                    populateLobObjects();
                    connection_.commit();
                } catch (SQLException e) {
                    connection_.rollback();
                    throw e;
                } finally {
                    if(shouldChangeAutoCommit) {
                        connection_.setAutoCommit(true);
                    }
                }
                return LobBatch;
            } else {
                int prc = usingRawRowset_ ? (paramRowCount_ + 1) : paramRowCount_;

                if (paramRowCount_ < 1) {
                    if (!getT4props().getDelayedErrorMode()) {
                        return (new int[] {});
                    }
                }

                try {
                    if (!usingRawRowset_) {
                        valueArray = getValueArray();
                    }
                    setBatchFlag(true);
                    long start = 0;
                    if (connection_.getT4props().isTraceTransTime() && !connection_.getAutoCommit()) {
                        start = System.currentTimeMillis();
                        connection_.checkTransStartTime(start);
                    }
                    execute(prc, inputDesc_.length, valueArray, queryTimeout_);
                    connection_.addTransactionUsedTimeList(stmtLabel_, sql_, start, TRANSPORT.TRACE_EXECUTEBATCH_TIME);
                    setBatchFlag(false);

                } catch (SQLException e) {
                    BatchUpdateException be;
                    se = TrafT4Messages.createSQLException(getT4props(), "batch_command_failed", e.getMessage());
                    if (realBatchResults == null) // we failed before execute
                    {
                        realBatchResults = new int[paramRowCount_];
                        Arrays.fill(realBatchResults, -3);
                    }
                    be = new BatchUpdateException(se.getMessage(), se.getSQLState(), realBatchResults);
                    be.setNextException(e);

                    throw be;
                }

                if (getT4props().getDelayedErrorMode()) {
                    _lastCount = paramRowCount_;
                }

                return realBatchResults;
            }
        } finally {
            clearBatch();
        }
    }

	public ResultSet executeQuery() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", stmtLabel_);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            if (this.connection_.isUseCacheTable() && getT4props().getCacheTable() != null) {
                ResultSet cachedResultSet = this.connection_.getTrafCache_()
                        .executeQuery(this.sql_, this.paramsValue_, getT4props());
                if (cachedResultSet != null) {
                    return cachedResultSet;
                } else {
                    if (getT4props().isLogEnable(Level.FINER)) {
                        T4LoggingUtilities.log(getT4props(), Level.FINER,
                                "ENTRY. cached ResultSet is null,ResultSet:", cachedResultSet);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ENTRY. cached ResultSet is null,ResultSet:<{}>",
                                cachedResultSet);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
		Object[] valueArray = null;
		int inDescLength = 0;

		validateExecuteInvocation();
        if (inputDesc_ != null) {
			if (!usingRawRowset_)
				valueArray = getValueArray();
			inDescLength = inputDesc_.length;
		}
		traceLinkThreshold.setSelectFlag(true);
		execute(paramRowCount_ + 1, inDescLength, valueArray, queryTimeout_);
		return resultSet_[result_set_offset];
	}

	public int executeUpdate() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", stmtLabel_);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		long count = executeUpdate64();

		if (count > Integer.MAX_VALUE)
			this.setSQLWarning(null, "numeric_out_of_range", null);

		return (int) count;
	}

    public long executeUpdate64() throws SQLException {
		Object[] valueArray = null;
		int inDescLength = 0;

		validateExecuteInvocation();
		if (usingRawRowset_ == false) {
			if (inputDesc_ != null) {
			    valueArray = getValueArray();
				inDescLength = inputDesc_.length;
			}
		} else {
			valueArray = this.paramsValue_; // send it along raw in case we need
			// it
			paramRowCount_ -= 1; // we need to make sure that paramRowCount
			// stays exactly what we set it to since we
			// add one during execute
		}

		// *******************************************************************
        // * If the setAutoCommit is set to false by the application, then there is no issue. 
        // * If the setAutoCommit to true,  either JDBC driver or MXOSRVR needs to modify the behavior as if it is auto-commit off, 
        // * do the needed work (insert into base table and the lob table) and then commit or rollback work.
        // *******************************************************************
        if (isAnyLob) {
            if (outputDesc_ != null) {
                throw TrafT4Messages.createSQLException(getT4props(), "lob_as_param_not_support");
            }
            boolean shouldChangeAutoCommit = connection_.getAutoCommit();
            if(shouldChangeAutoCommit) {
                connection_.setAutoCommit(false);
            }
            try {
                lobLocators = getLobHandles();
                setLobHandles();
                populateLobObjects();
                connection_.commit();
            } catch (SQLException e) {
                connection_.rollback();
                throw e;
            } finally {
                if(shouldChangeAutoCommit) {
                    connection_.setAutoCommit(true);
                }
            }
            return lobRowCount;
        } else {
            execute(paramRowCount_ + 1, inDescLength, valueArray, queryTimeout_);
            return ist_.getRowCount();
        }
	}

	public ResultSetMetaData getMetaData() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		if (outputDesc_ != null) {
			return new TrafT4ResultSetMetaData(this, outputDesc_);
		} else {
			return null;
		}
	}

    public List<String> getDescribeMessages(int messagesType) {
	    //messagesType input 1  out 2
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        List<String> resultList = new ArrayList<String>();
        if(messagesType == 1 && inputDesc_ != null){
            for (int i = 0; i < inputDesc_.length; i++) {
                StringBuilder sb = new StringBuilder("[");
                sb.append(i + 1).append("]");
                resultList.add(inputDesc_[i].getDescribeMessage(sb, i + 1));
            }
            return resultList;
        }
        if (messagesType == 2 && outputDesc_ != null) {
            for (int i = 0; i < outputDesc_.length; i++) {
                StringBuilder sb = new StringBuilder("[");
                sb.append(i + 1).append("]");
                resultList.add(outputDesc_[i].getDescribeMessage(sb, i + 1));
            }
            return resultList;
        }
        return null;
    }

	public ParameterMetaData getParameterMetaData() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (inputDesc_ != null) {
			return new TrafT4ParameterMetaData(this, inputDesc_);
		} else {
			return null;
		}
	}

	// JDK 1.2
	public void setArray(int parameterIndex, Array x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		//validateSetInvocation(parameterIndex);
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "setArray()");
	}

	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, length);
        }

		validateSetInvocation(parameterIndex);
		if (x == null) {
		    addParamValue(parameterIndex, null);
		    return;
		}

		int dataType = getInputDataType(parameterIndex - 1);

		switch (dataType) {

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.BINARY: 
		case Types.VARBINARY: 
		case Types.LONGVARBINARY:  // At this time Database does not
			// have this column data type
			byte[] buffer = new byte[length];
			try {
				x.read(buffer);
			} catch (IOException e) {
				throw TrafT4Messages.createSQLException(getT4props(), "io_exception",
						e.getMessage());
			}

			try {
				addParamValue(parameterIndex, new String(buffer, "ASCII"));
			} catch (java.io.UnsupportedEncodingException e) {
				throw TrafT4Messages.createSQLException(getT4props(),
						"unsupported_encoding", e.getMessage());
			}
			break;
		case Types.CLOB:
            int lobInlineMaxLen = getInputLobInlineMaxLen(parameterIndex - 1);
            if (lobInlineMaxLen > 0 && length <= lobInlineMaxLen) {
                try {
                    byte[] buf = new byte[length];
                    int bufLength = x.read(buf);
                    String inStr;
                    if (bufLength == length)
                        inStr = new String(buf);
                    else
                        inStr = new String(buf, 0, bufLength);
                    addParamValue(parameterIndex, inStr);
                    addLobObjects(parameterIndex, null);
                } catch (IOException e) {
                    throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
                }
            } else {
                isAnyLob = true;
                TrafT4Clob clob = new TrafT4Clob(this, inputDesc_[parameterIndex - 1], x, length);
                inputDesc_[parameterIndex - 1].paramValue_ = "";
                addParamValue(parameterIndex, new byte[0]);
                addLobObjects(parameterIndex, clob);
            }
            break;
		default:
			throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                    JDBCType.valueOf(dataType));
		}
	}

	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }

		validateSetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		int sqltype = inputDesc_[parameterIndex - 1].sqlDataType_;
		int dataType = inputDesc_[parameterIndex - 1].dataType_;

		if (x != null) { 
            int sqlQueryType = this.ist_.getSqlQueryType();
		    if (dataType == Types.NUMERIC && sqlQueryType == 3){
                int sqlPrecision_ = inputDesc_[parameterIndex - 1].sqlPrecision_ - 1;
                int scale_ = inputDesc_[parameterIndex - 1].scale_ - 1;
                Utility.checkDecimalTruncation( x, sqlPrecision_, scale_);
            } else {
                if (sqltype == InterfaceResultSet.SQLTYPECODE_LARGEINT){
                    Utility.checkDecimalTruncation(parameterIndex, getT4props(), x,
                            inputDesc_[parameterIndex - 1].precision_, inputDesc_[parameterIndex - 1].scale_);
                }
            }
			addParamValue(parameterIndex, x.toString());
		} else {
			addParamValue(parameterIndex, null);
		}
	}

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        if (x == null) {
            addParamValue(parameterIndex, null);
            return;
        }
        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);

        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.TIMESTAMP:
            case Types.TIME:
                // At this time Database does not have this column data type
                byte[] buffer2 = new byte[length];

                try {
                    x.read(buffer2);
                } catch (IOException e) {
                    throw TrafT4Messages.createSQLException(getT4props(), "io_exception",
                            e.getMessage());
                }
                addParamValue(parameterIndex, buffer2);
                break;
            case Types.BLOB:
                try {
                    int lobInlineMaxLen = getInputLobInlineMaxLen(parameterIndex - 1);
                    if (lobInlineMaxLen > 0 && length <= lobInlineMaxLen) {
                        byte[] buf = new byte[length];
                        int readLength = x.read(buf);
                        byte[] readBuf = buf;
                        if (readLength != length)
                            readBuf = Arrays.copyOf(buf, readLength);
                        addParamValue(parameterIndex, readBuf);
                        addLobObjects(parameterIndex, null);
                    } else {
                        isAnyLob = true;
                        TrafT4Blob lob = new TrafT4Blob(this, inputDesc_[parameterIndex - 1], x, length);
                        addParamValue(parameterIndex, new byte[0]);
                        addLobObjects(parameterIndex, lob);
                    }
                } catch (IOException e) {
                    throw TrafT4Messages.createSQLException(getT4props(), "io_exception",
                            e.getMessage());
                }
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

	/*
	 * Sets the designated parameter to the given <tt>Blob</tt> object. The
	 * driver converts this to an SQL <tt>BLOB</tt> value when it sends it to
	 * the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ... @param x a <tt>Blob</tt>
	 * object that maps an SQL <tt>BLOB</tt> value
	 *
	 * @throws SQLException invalid data type for column
	 */
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        if (x == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }
        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.BLOB:
                isAnyLob = true;
                TrafT4Blob blob = new TrafT4Blob(this, inputDesc_[parameterIndex - 1], x);
                addParamValue(parameterIndex, new byte[0]);
                addLobObjects(parameterIndex, blob);
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		validateSetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		int sqltype = inputDesc_[parameterIndex - 1].sqlDataType_;
		Object valueObj = null;
		if (sqltype == InterfaceResultSet.SQLTYPECODE_BOOLEAN) {
			if (x) {
				valueObj = 1;
			} else {
				valueObj = 0;
			}
		} else {
			if (x) {
				valueObj = "1";
			} else {
				valueObj = "0";
			}
		}
		addParamValue(parameterIndex, valueObj);
	}

	public void setByte(int parameterIndex, byte x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		validateSetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		addParamValue(parameterIndex, Byte.toString(x));
	}

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        if(x == null) {
            addParamValue(parameterIndex, null);
            return;
        }
        byte[] tmpArray = new byte[x.length];
        System.arraycopy(x, 0, tmpArray, 0, x.length);
        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.BLOB:
                int lobInlineMaxLen = getInputLobInlineMaxLen(parameterIndex - 1);
                if (lobInlineMaxLen > 0 && x.length <= lobInlineMaxLen) {
                    addParamValue(parameterIndex, tmpArray);
                    addLobObjects(parameterIndex, null);
                } else {
                    isAnyLob = true;
                    TrafT4Blob blob = new TrafT4Blob(this, inputDesc_[parameterIndex - 1], x, x.length);
                    addParamValue(parameterIndex, new byte[0]);
                    addLobObjects(parameterIndex, blob);
                }
                break;
            case Types.CLOB:
                lobInlineMaxLen = getInputLobInlineMaxLen(parameterIndex - 1);
                if (lobInlineMaxLen > 0 && x.length <= lobInlineMaxLen) {
                    addParamValue(parameterIndex, tmpArray);
                    addLobObjects(parameterIndex, null);
                } else {
                        isAnyLob  = true;
                        TrafT4Clob clob = new TrafT4Clob(this, inputDesc_[parameterIndex - 1], x, x.length);
                        inputDesc_[parameterIndex - 1].paramValue_ = "";
                        addParamValue(parameterIndex, new byte[0]);
                        addLobObjects(parameterIndex, clob);
                    }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.OTHER:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                addParamValue(parameterIndex, tmpArray);
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

    public void setCharacterStream(int parameterIndex, Reader x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, length);
        }
        if (x == null) {
            addParamValue(parameterIndex, null);
            return;
        }
        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CLOB:
                int lobInlineMaxLen = getInputLobInlineMaxLen(parameterIndex - 1);
                if (lobInlineMaxLen > 0 && length <= lobInlineMaxLen && length > 0) {
                    try {
                        char[] buf = new char[length];
                        int bufLength = x.read(buf);
                        String inStr;
                        if (bufLength == length)
                            inStr = new String(buf);
                        else
                            inStr = new String(buf, 0, bufLength);
                        addParamValue(parameterIndex, inStr);
                        addLobObjects(parameterIndex, null);
                    } catch (IOException e) {
                        throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
                    }
                } else {
                    isAnyLob = true;
                    TrafT4Clob clob = new TrafT4Clob(this, inputDesc_[parameterIndex - 1], x, length);
                    addParamValue(parameterIndex, new byte[0]);
                    addLobObjects(parameterIndex, clob);
                }
                break;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));

            default:
                char[] value = new char[length];
                try {
                    int valuePos = x.read(value);
                    if (valuePos < 1) {
                        Object[] messageArguments = new Object[1];
                        messageArguments[0] = "No data to read from the Reader";
                        throw TrafT4Messages.createSQLException(getT4props(), "io_exception", messageArguments);
                    }

                    while (valuePos < length) {
                        char temp[] = new char[length - valuePos];
                        int tempReadLen = x.read(temp, 0, length - valuePos);
                        System.arraycopy(temp, 0, value, valuePos, tempReadLen);
                        valuePos += tempReadLen;
                    }
                } catch (java.io.IOException e) {
                    Object[] messageArguments = new Object[1];
                    messageArguments[0] = e.getMessage();
                    throw TrafT4Messages.createSQLException(getT4props(), "io_exception", messageArguments);
                }
                addParamValue(parameterIndex, new String(value));
                break;
        }
    }

	/**
	 * Sets the designated parameter to the given <tt>Clob</tt> object. The
	 * driver converts this to an SQL <tt>CLOB</tt> value when it sends it to
	 * the database.
	 *
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            a <tt>Clob</tt> object that maps an SQL <tt>CLOB</tt>
	 *
	 * @throws SQLException
	 *             invalid data type for column, or restricted data type.
	 */
	public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        if (x == null) {
            addParamValue(parameterIndex, null);
        }
		validateSetInvocation(parameterIndex);
		int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CLOB:
                isAnyLob = true;
                TrafT4Clob clob = new TrafT4Clob(this, inputDesc_[parameterIndex - 1], x);
                addParamValue(parameterIndex, new byte[0]);
                addLobObjects(parameterIndex, clob);
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
	}

    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }

        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);
        int dtCode = getInputSqlDatetimeCode(parameterIndex - 1);

        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.DATE:
            case Types.TIMESTAMP:
                if(dtCode == InterfaceResultSet.SQLDTCODE_TIME){
                    throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                            JDBCType.valueOf(Types.TIME));
                }
                if (x != null) {
                    if (dataType == Types.TIMESTAMP) {
                        Timestamp t1 = new Timestamp(x.getTime());
                        addParamValue(parameterIndex, t1.toString());
                    } else {
                        addParamValue(parameterIndex, x.toString());
                    }
                } else {
                    addParamValue(parameterIndex, null);
                }
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }

    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, cal);
        }

        validateSetInvocation(parameterIndex);

        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                // Ignore the cal, since SQL would expect it to store it in the local
                // time zone
                if (x != null) {
                    if (dataType == Types.TIMESTAMP) {
                        Timestamp t1 = new Timestamp(x.getTime());
                        addParamValue(parameterIndex, t1.toString());
                    } else {
                        addParamValue(parameterIndex, x.toString());
                    }
                } else {
                    addParamValue(parameterIndex, null);

                }
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

	public void setDouble(int parameterIndex, double x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }

		validateSetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		addParamValue(parameterIndex, Double.toString(x));
		inputDesc_[parameterIndex - 1].isValueSet_ = true;
	}

	public void setFloat(int parameterIndex, float x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		validateSetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		addParamValue(parameterIndex, Float.toString(x));
	}

	public void setInt(int parameterIndex, int x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		validateSetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		addParamValue(parameterIndex, Integer.toString(x));
	}

	public void setLong(int parameterIndex, long x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		validateSetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		addParamValue(parameterIndex, Long.toString(x));
	}

	private void setLong(int parameterIndex, BigDecimal x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		validateSetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		addParamValue(parameterIndex, x);
	}

	public void setNull(int parameterIndex, int sqlType) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, sqlType);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, sqlType);
        }
      validateSetInvocation(parameterIndex);
      int type = getInputDataType(parameterIndex - 1);
      switch (type) {
          case Types.BLOB:
          case Types.CLOB:
              addLobObjects(parameterIndex, null);
          default:
              addParamValue(parameterIndex, null);
              break;
      }
  }


	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, sqlType, typeName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, sqlType, typeName);
        }
		setNull(parameterIndex, sqlType);
	}

	public void setObject(int parameterIndex, Object x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        boolean checkFlag = false;
        TrafCheckActiveMaster activeMaster = TrafCheckActiveMaster.getActiveMasterCacheMap().get(getT4props().getT4Url());
        if (activeMaster.isEmptyEqualsNull()) {
            checkFlag = (x == null || x.toString().length() == 0);
        }else {
            checkFlag = x == null;
        }
		if (checkFlag) {
			setNull(parameterIndex, Types.NULL);
		} else if (x instanceof BigDecimal) {
			setBigDecimal(parameterIndex, (BigDecimal) x);
		} else if (x instanceof java.sql.Date) {
			setDate(parameterIndex, (Date) x);
		} else if (x instanceof java.sql.Time) {
			setTime(parameterIndex, (Time) x);
		} else if (x instanceof java.sql.Timestamp) {
			setTimestamp(parameterIndex, (Timestamp) x);
		} else if (x instanceof Double) {
			setDouble(parameterIndex, ((Double) x).doubleValue());
		} else if (x instanceof Float) {
			setFloat(parameterIndex, ((Float) x).floatValue());
		} else if (x instanceof Long) {
			setLong(parameterIndex, ((Long) x).longValue());
		} else if (x instanceof Integer) {
			setInt(parameterIndex, ((Integer) x).intValue());
		} else if (x instanceof Short) {
			setShort(parameterIndex, ((Short) x).shortValue());
		} else if (x instanceof Byte) {
			setByte(parameterIndex, ((Byte) x).byteValue());
		} else if (x instanceof Boolean) {
			setBoolean(parameterIndex, ((Boolean) x).booleanValue());
		} else if (x instanceof String) {
			setString(parameterIndex, x.toString());
		} else if (x instanceof byte[]) {
			setBytes(parameterIndex, (byte[]) x);
		} else if (x instanceof Clob) {
			setClob(parameterIndex, (Clob) x);
		} else if (x instanceof Blob) {
			setBlob(parameterIndex, (Blob) x);
			/*
			 * else if (x instanceof DataWrapper) {
			 * validateSetInvocation(parameterIndex); setObject(parameterIndex,
			 * x, inputDesc_[parameterIndex - 1].dataType_); }
			 */
		} else if (x instanceof BigInteger) {
			setBigDecimal(parameterIndex, new BigDecimal((BigInteger) x));
		} else {
	        int dataType = getInputDataType(parameterIndex - 1);
			throw TrafT4Messages.createSQLException(getT4props(),
					"object_type_not_supported", JDBCType.valueOf(dataType));
		}
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		setObject(parameterIndex, x, targetSqlType, -1);
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		BigDecimal tmpbd;
		int precision;
        boolean checkFlag = false;
        TrafCheckActiveMaster activeMaster = TrafCheckActiveMaster.getActiveMasterCacheMap().get(getT4props().getT4Url());
        if (activeMaster.isEmptyEqualsNull()) {
            checkFlag = (x == null || x.toString().length() == 0);
        }else {
            checkFlag = x == null;
        }

		if (checkFlag) {
			setNull(parameterIndex, Types.NULL);
		} else {
            int type = getInputSqlDataType(parameterIndex - 1);
			switch (targetSqlType) {
                case Types.CLOB:
                    if (x instanceof Clob) {
                        setClob(parameterIndex, (Clob) x);
                    } else if (x instanceof String) {
                        setString(parameterIndex, (String) x);
                    } else if (x instanceof Reader) {
                        setCharacterStream(parameterIndex, (Reader) x, scale);
                    } else if (x instanceof InputStream) {
                        setAsciiStream(parameterIndex, (InputStream) x, scale);
                    } else {
                        throw TrafT4Messages.createSQLException(getT4props(), "object_type_not_supported",
                                JDBCType.valueOf(targetSqlType));
                    }
                    break;
                case Types.BLOB:
                    if (x instanceof Blob) {
                        setBlob(parameterIndex, (Blob) x);
                    } else if (x instanceof String) {
                        setString(parameterIndex, (String) x);
                    } else if (x instanceof byte[]) {
                        setBytes(parameterIndex, (byte[]) x);
                    } else if (x instanceof InputStream) {
                        setBinaryStream(parameterIndex, (InputStream) x, scale);
                    } else {
                        throw TrafT4Messages.createSQLException(getT4props(), "object_type_not_supported",
                                JDBCType.valueOf(targetSqlType));
                    }
                    break;
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
				setString(parameterIndex, x.toString());
				break;
			case Types.NCHAR:
			case Types.NVARCHAR:
			    setNString(parameterIndex, x.toString());
			    break;
			case Types.VARBINARY:
			case Types.BINARY:
			case Types.LONGVARBINARY:
				setBytes(parameterIndex, (byte[]) x);
				break;
			case Types.TIMESTAMP:
				if (x instanceof Timestamp) {
					setTimestamp(parameterIndex, (Timestamp) x);
				} else if (x instanceof Date) {
					setTimestamp(parameterIndex, Timestamp.valueOf(x.toString() + " 00:00:00.0"));
				} else {
					setString(parameterIndex, x.toString());
				}
				break;
                case Types.TIME:
                    if (x instanceof Time) {
                        setTime(parameterIndex, (Time) x);
                    } else if (x instanceof Date) {
                        setTime(parameterIndex, new Time(((Date) x).getTime()));
                    } else if (x instanceof Timestamp) {
                        setTime(parameterIndex, new Time(((Timestamp) x).getTime()));
                    } else {
                        String timeData = x.toString();
                        try {
                            timeData = Time.valueOf(timeData).toString();
                        } catch (Exception e) {
                            throw TrafT4Messages.createSQLException(getT4props(),
                                    "invalid_parameter_value",
                                    "[" + timeData
                                            + "] TIME format is incorrect or date value is invalide. "
                                            + "Supported format: hh:mm:ss");
                        }
                        setString(parameterIndex, timeData);
                    }
                    break;
			case Types.DATE:
				try {
					if (x instanceof Date) {
						setDate(parameterIndex, (Date) x);
					} else if (x instanceof Time) {
						setDate(parameterIndex, new Date(((Time) x).getTime()));
					} else if (x instanceof Timestamp) {
						setDate(parameterIndex, new Date(((Timestamp) x).getTime()));
					} else {
						setDate(parameterIndex, Date.valueOf(x.toString()));
					}
				} catch (IllegalArgumentException iex) {
					throw TrafT4Messages.createSQLException(getT4props(),
							"invalid_parameter_value", x.toString());
				}
				break;
			case Types.BOOLEAN:
				setBoolean(parameterIndex, (Boolean.valueOf(x.toString())).booleanValue());
				break;
			case Types.SMALLINT:
				tmpbd = Utility.getBigDecimalValue(getT4props(), x);
				Utility.checkShortBoundary(getT4props(), tmpbd);
				//Utility.checkLongTruncation(parameterIndex, tmpbd);
				setShort(parameterIndex, tmpbd.shortValue());
				break;
			case Types.INTEGER:
				tmpbd = Utility.getBigDecimalValue(getT4props(), x);
				//Utility.checkLongTruncation(parameterIndex, tmpbd);
				Utility.checkIntegerBoundary(getT4props(), tmpbd);
				setInt(parameterIndex, tmpbd.intValue());
				break;
			case Types.BIGINT:
				tmpbd = Utility.getBigDecimalValue(getT4props(), x);
				//Utility.checkLongTruncation(parameterIndex, tmpbd);
				if (type == InterfaceResultSet.SQLTYPECODE_LARGEINT_UNSIGNED){
                	Utility.checkUnsignedLongBoundary(getT4props(), tmpbd);
					setLong(parameterIndex, tmpbd);
				} else if (type == InterfaceResultSet.SQLTYPECODE_INTEGER_UNSIGNED) {
				    // if data is unsigned int ,the java.sql.type is -5 (bigint), our sql type is -401
				    Utility.checkUnsignedIntegerBoundary(getT4props(), tmpbd);
				    setLong(parameterIndex, tmpbd);
				} else{
					Utility.checkLongBoundary(getT4props(), tmpbd);
					setLong(parameterIndex, tmpbd.longValue());
				}
				break;
			case Types.DECIMAL:
				// precision = getPrecision(parameterIndex - 1);
				tmpbd = Utility.getBigDecimalValue(getT4props(), x);
				tmpbd = Utility.setScale(tmpbd, scale, BigDecimal.ROUND_HALF_EVEN);
				// Utility.checkDecimalBoundary(getT4props(), tmpbd, precision);
				setBigDecimal(parameterIndex, tmpbd);
				break;
			case Types.NUMERIC:
				// precision = getPrecision(parameterIndex - 1);
				tmpbd = Utility.getBigDecimalValue(getT4props(), x);
				tmpbd = Utility.setScale(tmpbd, scale, BigDecimal.ROUND_HALF_EVEN);
				// Utility.checkDecimalBoundary(getT4props(), tmpbd, precision);
				setBigDecimal(parameterIndex, tmpbd);
				break;
			case Types.TINYINT:
				tmpbd = Utility.getBigDecimalValue(getT4props(), x);
				tmpbd = Utility.setScale(tmpbd, scale, roundingMode_);
				if (type == InterfaceResultSet.SQLTYPECODE_TINYINT_UNSIGNED) {
					Utility.checkUnsignedTinyintBoundary(getT4props(), tmpbd);
				} else {
					Utility.checkSignedTinyintBoundary(getT4props(), tmpbd);
				}
				setShort(parameterIndex, tmpbd.shortValue());
				break;
			case Types.FLOAT:
				tmpbd = Utility.getBigDecimalValue(getT4props(), x);
				Utility.checkFloatBoundary(getT4props(), tmpbd);
				setDouble(parameterIndex, tmpbd.doubleValue());
				break;
			case Types.DOUBLE:
				tmpbd = Utility.getBigDecimalValue(getT4props(), x);
				Utility.checkDoubleBoundary(getT4props(), tmpbd);
				setDouble(parameterIndex, tmpbd.doubleValue());
				break;
			case Types.REAL:
				tmpbd = Utility.getBigDecimalValue(getT4props(), x);
				setFloat(parameterIndex, tmpbd.floatValue());
				break;
			case Types.OTHER:
				if (inputDesc_[parameterIndex].fsDataType_ == InterfaceResultSet.SQLTYPECODE_INTERVAL) {
					if (x instanceof byte[]) {
						addParamValue(parameterIndex, x);
					} else if (x instanceof String) {
						addParamValue(parameterIndex, x);
					} else {
						throw TrafT4Messages.createSQLException(getT4props(), "conversion_not_allowed");
					}
					break;
				}
			case Types.ARRAY:
			case Types.BIT:
			case Types.DATALINK:
			case Types.DISTINCT:
			case Types.JAVA_OBJECT:
			case Types.STRUCT:
			default:
				throw TrafT4Messages.createSQLException(getT4props(),
						"object_type_not_supported", JDBCType.valueOf(targetSqlType));
			}
		}
	}

	// JDK 1.2
	public void setRef(int parameterIndex, Ref x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		//validateSetInvocation(parameterIndex);
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "setRef()");
	}

	public void setShort(int parameterIndex, short x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		validateSetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		addParamValue(parameterIndex, Short.toString(x));
	}

    public void setString(int parameterIndex, String x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        if (x == null) {
            addParamValue(parameterIndex, null);
            return;
        }
        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);

        switch (dataType) {
            case Types.CLOB:
                int lobInlineMaxLen = getInputLobInlineMaxLen(parameterIndex - 1);
                if (lobInlineMaxLen > 0 && x.length() <= lobInlineMaxLen) {
                    addParamValue(parameterIndex, x);
                    addLobObjects(parameterIndex, null);
                } else {
                    isAnyLob = true;
                    TrafT4Clob clob = new TrafT4Clob(this, inputDesc_[parameterIndex - 1], x, x.length());
                    addParamValue(parameterIndex, new byte[0]);
                    addLobObjects(parameterIndex, clob);
                }
                break;
            case Types.BLOB:
                byte[] b = null;
                try {
                    b = x.getBytes(InterfaceUtilities.getCharsetName(InterfaceUtilities.SQLCHARSETCODE_UTF8));
                } catch (UnsupportedEncodingException e) {
                    throw TrafT4Messages.createSQLException(getT4props(), e.getMessage());
                }
                lobInlineMaxLen = getInputLobInlineMaxLen(parameterIndex - 1);
                if (lobInlineMaxLen > 0 && x.length() <= lobInlineMaxLen) {
                    addParamValue(parameterIndex, b);
                    addLobObjects(parameterIndex, null);
                } else {
                    isAnyLob = true;
                    byte[] datas = x.getBytes();
                    TrafT4Blob blob = new TrafT4Blob(this, inputDesc_[parameterIndex - 1], datas, datas.length);
                    addParamValue(parameterIndex, new byte[0]);
                    addLobObjects(parameterIndex, blob);
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                String targetCharset = InterfaceUtilities.getCharsetName(this.inputDesc_[parameterIndex - 1].sqlCharset_);
                if (targetCharset.equals("ISO-8859-1")) {
                    String targetStr = null;
                    try {
                        targetStr = new String(x.getBytes(targetCharset), targetCharset);
                    } catch (Exception ex) {
                        throw new SQLException("ERROR: Validate the charset failed");
                    }
                    if (!x.equals(targetStr)) {
                        String msg = "ERROR[8690] An invalid character value encountered in function. Target charset:" + targetCharset;
                        throw new SQLException(msg);
                    }
                }
                addParamValue(parameterIndex, x);
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                addParamValue(parameterIndex,x);
                break;
            case Types.OTHER: // This type maps to the Database
                // INTERVAL
                addParamValue(parameterIndex, x);
                break;
            case Types.ARRAY:
            case Types.BIT:
            case Types.DATALINK:
            case Types.JAVA_OBJECT:
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (x != null) {
                    x = x.trim(); // SQLJ is using numeric string with
                    // leading/trailing whitespace
                }
            case Types.BOOLEAN:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.LONGVARBINARY:
            case Types.NULL:
            case Types.REAL:
            case Types.BINARY:
            case Types.VARBINARY:
                setObject(parameterIndex, x, dataType);
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "object_type_not_supported",
                        JDBCType.valueOf(dataType));
        }
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }

        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.TIME:
            case Types.TIMESTAMP:
                if (x != null) {
                    if (dataType == Types.TIMESTAMP) {
                        Timestamp t1 = new Timestamp(x.getTime());
                        addParamValue(parameterIndex, t1.toString());
                    } else {
                        addParamValue(parameterIndex, x.toString());
                    }
                } else {
                    addParamValue(parameterIndex, null);
                }
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, cal);
        }

        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.TIME:
            case Types.TIMESTAMP:
                // Ignore the cal, since SQL would expect it to store it in the local
                // time zone
                if (x != null) {
                    if (dataType == Types.TIMESTAMP) {
                        Timestamp t1 = new Timestamp(x.getTime());
                        addParamValue(parameterIndex, t1.toString());
                    } else {
                        addParamValue(parameterIndex, x.toString());
                    }
                } else {
                    addParamValue(parameterIndex, null);
                }
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }

        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                if (x != null) {
                    switch (dataType) {
                        case Types.DATE:
                            Date d1 = new Date(x.getTime());
                            addParamValue(parameterIndex, d1.toString());
                            break;
                        case Types.TIME:
                            Time t1 = new Time(x.getTime());
                            addParamValue(parameterIndex, t1.toString());
                            break;
                        default:
                            addParamValue(parameterIndex, x.toString());
                            break;
                    }
                } else {
                    addParamValue(parameterIndex, null);
                }
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, cal);
        }

        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                // Ignore the cal, since SQL would expect it to store it in the local
                // time zone
                if (x != null) {
                    switch (dataType) {
                        case Types.DATE:
                            Date d1 = new Date(x.getTime());
                            addParamValue(parameterIndex, d1.toString());
                            break;
                        case Types.TIME:
                            Time t1 = new Time(x.getTime());
                            addParamValue(parameterIndex, t1.toString());
                            break;
                        default:
                            addParamValue(parameterIndex, x.toString());
                            break;
                    }
                } else {
                    addParamValue(parameterIndex, null);
                }
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
		byte[] buffer = new byte[length]; // length = number of bytes in
		// stream
		validateSetInvocation(parameterIndex);
		String s;

		if (x == null) {
			addParamValue(parameterIndex, null);
		} else {
			int dataType = getInputDataType(parameterIndex - 1);
			switch (dataType) {
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.TINYINT:
				throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
			default:
				try {
					x.read(buffer, 0, length);
				} catch (java.io.IOException e) {
					Object[] messageArguments = new Object[1];
					messageArguments[0] = e.getMessage();
					throw TrafT4Messages.createSQLException(getT4props(), "io_exception",
							messageArguments);
				}
				try {
					s = new String(buffer, "UnicodeBig");
					addParamValue(parameterIndex, s);
				} catch (java.io.UnsupportedEncodingException e) {
					Object[] messageArguments = new Object[1];
					messageArguments[0] = e.getMessage();
					throw TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding", messageArguments);
				}
				break;
			}
		}
	}

	public void setURL(int parameterIndex, URL x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
		//validateSetInvocation(parameterIndex);
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "setURL()");
	} // end setURL

	// -------------------------------------------------------------------------------------------
	/**
	 * This method will associate user defined data with the prepared statement.
	 * The user defined data must be in SQL/MX rowwise rowset format.
	 *
	 * @param numRows
	 *            the number of rows contained in buffer
	 * @param buffer
	 *            a buffer containing the rows
	 *
	 * @exception SQLException
	 *                SQLException is thrown
	 */
	public void setDataBuffer(int numRows, ByteBuffer buffer) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", numRows, buffer);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", numRows, buffer);
        }
		usingRawRowset_ = true;
		paramRowCount_ = numRows;
		rowwiseRowsetBuffer_ = buffer;
	} // end setDataBufferBuffer

	// -------------------------------------------------------------------------------------------

	// Other methods
	protected void updatePstmtIc(InterfaceConnection ic){
	    this.ist_.getT4statement().updateConnContext(ic);
	}
	protected void validateExecuteInvocation() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		clearWarnings();
		if (isClosed_) {
			throw TrafT4Messages.createSQLException(getT4props(), "stmt_closed");
		}
		// connection_.getServerHandle().isConnectionOpen();
		connection_.isConnectionOpen();
		// close the previous resultset, if any
		for (int i = 0; i < num_result_sets_; i++) {
			if (resultSet_[i] != null) {
				resultSet_[i].close();
			}
		}
		if (paramRowCount_ > 0 && usingRawRowset_ == false) {
			throw TrafT4Messages.createSQLException(getT4props(), "function_sequence_error");
		}

		if (usingRawRowset_ == false)
			checkIfAllParamsSet();

	}

	private void checkIfAllParamsSet() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		int paramNumber;

		if (inputDesc_ == null) {
			return;
		}
		for (paramNumber = 0; paramNumber < inputDesc_.length; paramNumber++) {
			if (!inputDesc_[paramNumber].isValueSet_) {
				Object[] messageArguments = new Object[2];
				messageArguments[0] = new Integer(paramNumber + 1);
				messageArguments[1] = new Integer(paramRowCount_ + 1);
				throw TrafT4Messages.createSQLException(getT4props(), "parameter_not_set",
						messageArguments);
			}
		}
	}

	private void validateSetInvocation(int parameterIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (isClosed_) {
			throw TrafT4Messages.createSQLException(getT4props(), "stmt_closed");
		}
		// connection_.getServerHandle().isConnectionOpen();
		connection_.isConnectionOpen();
		if (inputDesc_ == null) {
			throw TrafT4Messages.createSQLException(getT4props(),
					"invalid_parameter_index", parameterIndex);
		}
		if (parameterIndex < 1 || parameterIndex > inputDesc_.length) {
			throw TrafT4Messages.createSQLException(getT4props(),
					"invalid_parameter_index", parameterIndex);
		}
		if (inputDesc_[parameterIndex - 1].paramMode_ == DatabaseMetaData.procedureColumnOut) {
			throw TrafT4Messages.createSQLException(getT4props(), "is_a_output_parameter");
		}
	}

	void addParamValue(int parameterIndex, Object x) {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		paramsValue_[parameterIndex - 1] = x;
		inputDesc_[parameterIndex - 1].isValueSet_ = true;
	}

	Object[] getValueArray() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		Object[] valueArray;
		int length;
		int i;
		int j;
		int index;
		Object[] rows;

		if (paramRowCount_ > 0) {
			valueArray = new Object[(paramRowCount_ + 1) * inputDesc_.length];
			length = rowsValue_.size();
			for (i = 0, index = 0; i < length; i++) {
				rows = (Object[]) rowsValue_.get(i);
				for (j = 0; j < rows.length; j++, index++) {
					valueArray[index] = rows[j];
				}
			}
		} else {
			valueArray = paramsValue_;
		}
		return valueArray;
	}

	void logicalClose() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		isClosed_ = true;
		if (rowsValue_ != null) {
			rowsValue_.clear();

		}
        if (lobObjects != null) {
            lobObjects.clear();
            lobColNames.clear();;
            lobColIds.clear();;
        }
		paramRowCount_ = 0;
		for (int i = 0; i < num_result_sets_; i++) {
			if (resultSet_[i] != null) {
				resultSet_[i].close();
				// Clear the isValueSet_ flag in inputDesc_
			}
		}
		result_set_offset = 0;
		resultSet_[result_set_offset] = null;
		if (inputDesc_ != null) {
			for (int i = 0; i < inputDesc_.length; i++) {
				inputDesc_[i].isValueSet_ = false;
				paramsValue_[i] = null;
			}
		}
		isAnyLob = false;
		if (!connection_.closePreparedStatement(connection_, sql_, resultSetType_, resultSetConcurrency_,
				resultSetHoldability_)) {
			this.close(true); // if the statement is not in the cache
			// hardclose it afterall
		}

	}

	// ----------------------------------------------------------------------------------
	// Method used by JNI Layer to update the results of Prepare
	void setPrepareOutputs(TrafT4Desc[] inputDesc, TrafT4Desc[] outputDesc, int inputParamCount, int outputParamCount)
			throws SQLException {
		inputDesc_ = inputDesc;
		outputDesc_ = outputDesc;
		paramRowCount_ = 0;

		// Prepare updares inputDesc_ and outputDesc_
		if (inputDesc_ != null) {
			paramsValue_ = new Object[inputDesc_.length];
		} else {
			paramsValue_ = null;
		}
	} // end setPrepareOutputs

	// ----------------------------------------------------------------------------------
	void setPrepareOutputs2(TrafT4Desc[] inputDesc, TrafT4Desc[] outputDesc, int inputParamCount, int outputParamCount,
			int inputParamsLength, int outputParamsLength, int inputDescLength, int outputDescLength)
			throws SQLException {
		if (getT4props().isLogEnable(Level.FINEST)) {
		    T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", inputDesc, outputDesc, inputParamCount,
                    outputParamCount, inputParamsLength, outputParamsLength, inputDescLength, outputDescLength);
        }
		if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}, {}, {}, {}", inputDesc, outputDesc,
                    inputParamCount, outputParamCount, inputParamsLength, outputParamsLength,
                    inputDescLength, outputDescLength);
        }
		inputParamCount_ = inputParamCount;
		outputParamCount_ = outputParamCount;
		inputParamsLength_ = inputParamsLength;
		outputParamsLength_ = outputParamsLength;
		inputDescLength_ = inputDescLength;
		outputDescLength_ = outputDescLength;
		setPrepareOutputs(inputDesc, outputDesc, inputParamCount, outputParamCount);
	} // end setPrepareOutputs2

	// ----------------------------------------------------------------------------------
	// Method used by JNI layer to update the results of Execute
	void setExecuteOutputs(int rowCount) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", rowCount);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", rowCount);
        }
		batchRowCount_ = new int[1];
		batchRowCount_[0] = rowCount;
		num_result_sets_ = 1;
		result_set_offset = 0;
		if (outputDesc_ != null) {
			resultSet_[result_set_offset] = new TrafT4ResultSet(this, outputDesc_);
		} else {
			resultSet_[result_set_offset] = null;
		}
	}

	/*
	 * //----------------------------------------------------------------------------------
	 * void setExecuteSingletonOutputs(SQLValue_def[] sqlValue_def_array, short
	 * rowsAffected) throws SQLException { batchRowCount_ = new int[1];
	 * batchRowCount_[0] = rowsAffected; if (outputDesc_ != null) { resultSet_ =
	 * new TrafT4ResultSet(this, outputDesc_); } else { resultSet_ = null; } if
	 * (rowsAffected == 0) { resultSet_.setFetchOutputs(new ObjectRow[0], 0, true, 0); }
	 * else { resultSet_.irs_.setSingletonFetchOutputs(resultSet_, rowsAffected,
	 * true, 0, sqlValue_def_array); } }
	 */

	// ----------------------------------------------------------------------------------
	// Method used by JNI layer to update the results of Execute
	void setExecuteBatchOutputs(int[] rowCount) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", rowCount);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", rowCount);
        }
		num_result_sets_ = 1;
		result_set_offset = 0;
		if (outputDesc_ != null) {
			resultSet_[result_set_offset] = new TrafT4ResultSet(this, outputDesc_);
		} else {
			resultSet_[result_set_offset] = null;
		}
		batchRowCount_ = rowCount;
	}

	void reuse(TrafT4Connection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection,
                    resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", connection, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY && resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE
				&& resultSetType != ResultSet.TYPE_SCROLL_SENSITIVE) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_resultset_type", resultSetType);
		}
		if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
			resultSetType_ = ResultSet.TYPE_SCROLL_INSENSITIVE;
			setSQLWarning(null, "scrollResultSetChanged", null);
		} else {
			resultSetType_ = resultSetType;
		}
		if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY && resultSetConcurrency != ResultSet.CONCUR_UPDATABLE) {
			throw TrafT4Messages.createSQLException(getT4props(),
					"invalid_resultset_concurrency", resultSetConcurrency);
		}
		resultSetConcurrency_ = resultSetConcurrency;
		resultSetHoldability_ = resultSetHoldability;
		queryTimeout_ = connection_.getServerHandle().getQueryTimeout();
		fetchSize_ = TrafT4ResultSet.DEFAULT_FETCH_SIZE;
		maxRows_ = 0;
		fetchDirection_ = ResultSet.FETCH_FORWARD;
		isClosed_ = false;
	}

	public void close(boolean hardClose) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", hardClose);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY", hardClose);
        }

		if (connection_._isClosed()) {
			return;
		}
		try {
			if (hardClose) {
				ist_.close();
			} else {
				logicalClose();
			}
		} catch (SQLException e) {
			performConnectionErrorChecks(e);
			throw e;
		} finally {
			isClosed_ = true;
			if (hardClose) {
				connection_.removeElement(pRef_);
			}
		}

	}

    public TrafT4PreparedStatement() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
    }

    TrafT4PreparedStatement(TrafT4Connection connection, String sql, String stmtLabel) throws SQLException {
        this(connection, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.holdability_,
                stmtLabel);
        connection.ic_.getT4props().setUseArrayBinding(false);
        connection.ic_.getT4props().setBatchRecovery(false);
    }

    // Constructors with access specifier as "default"
    TrafT4PreparedStatement(TrafT4Connection connection, String sql) throws SQLException {
        this(connection, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.holdability_, null);
    }

    TrafT4PreparedStatement(TrafT4Connection connection, String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        this(connection, sql, resultSetType, resultSetConcurrency, connection.holdability_, null);
    }

    TrafT4PreparedStatement(TrafT4Connection connection, String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        this(connection, sql, resultSetType, resultSetConcurrency, resultSetHoldability, null);
    }

        TrafT4PreparedStatement(TrafT4Connection connection, String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability, String stmtLabel) throws SQLException {
        super(connection, resultSetType, resultSetConcurrency, resultSetHoldability, stmtLabel);
        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "New PreparedStatement", connection, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability, stmtLabel);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("New PreparedStatement. {}, {}, {}, {}, {}, {}", connection, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability, stmtLabel);
        }
        // connection_.getServerHandle().isConnectionOpen();
        connection_.isConnectionOpen();
        sqlStmtType_ = TRANSPORT.TYPE_UNKNOWN;
        sql_ = sql;

        usingRawRowset_ = false;
        traceLinkThreshold = new TraceLinkThreshold();
        traceLinkList = new ArrayList<TraceLinkThreshold>();
    }

	// Interface methods
    public void prepare(String sql, int queryTimeout, int holdability) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, queryTimeout, holdability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", sql, queryTimeout, holdability);
        }
        try {
            if (getT4props().isLogEnable(Level.FINEST)) {
                String tmp = "prepare sql : " + sql + ", stmtLabel : " + stmtLabel_;
                T4LoggingUtilities.log(getT4props(), Level.FINEST, tmp);
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("prepare sql : {}, stmtLabel : {}", sql, stmtLabel_);
            }
            super.ist_.prepare(sql, queryTimeout, this);
            sqlStmtType_ = ist_.getSqlStmtType(sql);
        } catch (SQLException e) {
            /*
            if (rePrepare(e, sql, queryTimeout, this)) {
                return;
            } else {
                performPrepareErrorChecks();
                performConnectionErrorChecks(e);
                throw e;
            }
            */
		performPrepareErrorChecks();
		performConnectionErrorChecks(e);
		throw e;
        }
    }

        private boolean rePrepare(SQLException e, String sql, int queryTimeout, TrafT4Statement pstmt) throws SQLException {
            int errorCode = e.getErrorCode();
            if(connection_.isStatementCachingEnabled()){
                connection_.removePreparedStatement(connection_, this, resultSetHoldability_);
            }
            long startTime = System.currentTimeMillis();
            if (errorCode == -8738 || errorCode == -8582 || errorCode == -4401 || errorCode == -8750 || errorCode == -8751) {
                rePrepareExecuteTrace("ENTRY re-prepare:" + errorCode);
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                    try {
                        super.ist_.prepare(sql_, queryTimeout, this);
                        sqlStmtType_ = ist_.getSqlStmtType(sql_);
                        rePrepareExecuteTrace("re-prepare success");
                        return true;
                    } catch (SQLException ex) {
                        if (connection_.isStatementCachingEnabled()) {
                            connection_.removePreparedStatement(connection_, this, resultSetHoldability_);
                        }
                        long endTime = System.currentTimeMillis();
                        int time = (int) (System.currentTimeMillis() - startTime) / 1000;
                        if (time >= ddlTimeout_) {
                            String msg = "re-prepare timeout:" + ex.getErrorCode();
                            rePrepareExecuteTrace(msg);
                            SQLException se = TrafT4Messages.createSQLException(getT4props(), "reprepare_reexecute_error", msg);
                            throw se;
                        }
                    }
                }
            } else {
                return false;
            }
        }

    protected void performPrepareErrorChecks() {
        // if prepare error the prepareReply should be null
        if (ist_.isPstmsAdd()) {
            connection_.decrementPrepreadStmtNum();
            ist_.setPstmsAdd(false);
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
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_fetchSize_value", rows);
        }
        if (rows > 0) {
            fetchSize_ = rows;
        }
        // If the value specified is zero, then the hint is ignored.
    }

    private void execute(int paramRowCount, int paramCount, Object[] paramValues, int queryTimeout) throws SQLException {
        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY", paramRowCount, paramCount,
                paramValues, queryTimeout);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}", paramRowCount, paramCount, paramValues,
                queryTimeout, isAnyLob);
        }
        traceLinkList.clear();
        traceLinkThreshold.setOdbcAPI(TRANSPORT.SRVR_API_SQLEXECUTE2);
        traceLinkThreshold.setEnterDriverTimestamp(System.currentTimeMillis());
        int oneBatchMaxRows = 0;
        int tmpRows = 0;
        int realBatchCount = 0;
        boolean tmpFlag = true;
        int maxBatchSize = 512 * 1024 * 1024; //512M
        if (isBatchFlag()) {
            if (this.inputParamsLength_ == 0) {
                oneBatchMaxRows = Integer.MAX_VALUE;
            }else if (this.inputParamsLength_ >= maxBatchSize){
                maxBatchSize = maxBatchSize * 2; //1G
                if (this.inputParamsLength_ >= maxBatchSize){
                    throw TrafT4Messages.createSQLException(getT4props(), "row_width_too_large", this.inputParamsLength_);
                }
                oneBatchMaxRows = (int) (maxBatchSize / this.inputParamsLength_);
            }else {
                oneBatchMaxRows = (int) (maxBatchSize / this.inputParamsLength_);
            }
        }
        try {
            if (isBatchFlag()){
                do {
                    if (paramRowCount > oneBatchMaxRows
                        && paramRowCount > tmpRows + oneBatchMaxRows) {
                        setStartRowNum(tmpRows);
                        tmpRows += oneBatchMaxRows;
                        realBatchCount = oneBatchMaxRows;
                    } else if (paramRowCount > oneBatchMaxRows) {
                        setStartRowNum(tmpRows);
                        realBatchCount = paramRowCount - tmpRows;
                        tmpFlag = false;
                    } else {
                        realBatchCount = paramRowCount;
                        tmpFlag = false;
                    }
                    ist_.execute(TRANSPORT.SRVR_API_SQLEXECUTE2, realBatchCount, paramCount,
                        paramValues, queryTimeout, null,
                        this);
                    System.arraycopy(batchRowCount_,0, realBatchResults, getStartRowNum(), batchRowCount_.length);
                } while (tmpFlag);
            } else {
                ist_.execute(TRANSPORT.SRVR_API_SQLEXECUTE2, paramRowCount, paramCount, paramValues,
                    queryTimeout, null,
                    this);
            }
            if (getT4props().isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(getT4props(), Level.FINER, "LEAVE", paramRowCount, paramCount,
                    paramValues, queryTimeout);
            }
        } catch (SQLException e) {
            int times = 0;
            if (reExecuteAndPrepare(e, TRANSPORT.SRVR_API_SQLEXECUTE2, paramRowCount, paramCount,
                    paramValues, queryTimeout, this, times, e.getErrorCode())) {
                return;
            } else {
                performConnectionErrorChecks(e);
                throw e;
            }
        }
        if (!traceLinkThreshold.isSelectFlag() || traceLinkThreshold.isEndOfData()) {
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
    }

	    private void rePrepareExecuteTrace(String msg){
            if (getT4props().isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(getT4props(), Level.FINER, msg);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(msg);
            }
        }

        private boolean reExecuteAndPrepare(SQLException e, short srvrApiSqlexecute2,
                int paramRowCount, int paramCount, Object[] paramValues, int queryTimeout,
                TrafT4PreparedStatement trafT4PreparedStatement, int times, int firstErrorCode)
                throws SQLException {
            int errorCode = e.getErrorCode();
            SQLException error8839 = e;
            if (times == 3) {
                if (connection_.isStatementCachingEnabled()) {
                    connection_.removePreparedStatement(connection_, this, resultSetHoldability_);
                }
                if (LOG.isErrorEnabled()) {
                    LOG.error(
                            "Error Code:{} and First Error code:{}; re-Prepare and Execute failed and The number of retries has exceeded three",
                            errorCode, firstErrorCode);
                }
                performConnectionErrorChecks(e);
                throw e;
            }
            while (error8839 != null) {
                if (error8839.getErrorCode() == -8839) {
                    try {
                        super.ist_.prepare(sql_, queryTimeout, this);
                        sqlStmtType_ = ist_.getSqlStmtType(sql_);
                        if (LOG.isWarnEnabled()) {
                            LOG.warn(
                                    "Error Code:8839 and First Error code:{};re-prepare(no re-execute) success",
                                    firstErrorCode);
                        }
                        return false;
                    } catch (SQLException ex) {
                        if (connection_.isStatementCachingEnabled()) {
                            connection_
                                    .removePreparedStatement(connection_, this,
                                            resultSetHoldability_);
                        }
                        String message = e.getMessage() + ",\nre-prepared failed with : " + ex.getMessage();
                        if (LOG.isErrorEnabled()) {
                            LOG.error(
                                    "Re-prepare failure caused by error code 8839, error message is :\n{}", message);
                        }
                        e.setNextException(ex);
                        performConnectionErrorChecks(e);
                        throw e;
                    }
                } else {
                    error8839 = error8839.getNextException();
                }
            }
            switch (errorCode) {
                case -29183:
                    ist_.setRetryExecute(true);
                case -8751:
                case -8750:
                case -4401:
                case -8738:
                case -8582:
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        if(LOG.isTraceEnabled()){
                            LOG.trace("error while sleep : {}", ex.getMessage());
                        }
                        ex.printStackTrace();
                    }
                    try {
                        times++;
                        if (LOG.isWarnEnabled()) {
                            LOG.warn(
                                    "re-prepare And execute until success or times > 3. now is:{}; First Error code:{}; ErrorCode:{};",
                                    times, firstErrorCode, errorCode);
                        }
                        super.ist_.prepare(sql_, queryTimeout, this);
                        sqlStmtType_ = ist_.getSqlStmtType(sql_);
                        ist_.execute(srvrApiSqlexecute2, paramRowCount, paramCount, paramValues,
                                queryTimeout, null, trafT4PreparedStatement);
                        if (LOG.isWarnEnabled()) {
                            LOG.warn("re-prepare And execute success");
                        }
                        return true;
                    } catch (SQLException epx) {
                        if(errorCode == -29183){
                            throw epx;
                        }
                        return reExecuteAndPrepare(epx, srvrApiSqlexecute2, paramRowCount,
                                paramCount,
                                paramValues, queryTimeout, trafT4PreparedStatement, times,
                                firstErrorCode);
                    }
                default:
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Error Code:{}; and First Error code:{};", errorCode,
                                firstErrorCode);
                    }
                    performConnectionErrorChecks(e);
                    throw e;
            }
        }

    /*
	 * protected void setSingleton(boolean value) { singleton_ = value; }
	 * protected boolean getSingleton() { return singleton_; }
	 */

	/**
	 * Use this method to retrieve the statement type that was used when
	 * creating the statement through the connectivity service. ie. SELECT,
	 * UPDATE, DELETE, INSERT.
	 */
	public String getStatementType() {
		String stmtType = "";
		switch (sqlStmtType_) {
		case TRANSPORT.TYPE_SELECT:
			stmtType = "SELECT";
			break;
		case TRANSPORT.TYPE_UPDATE:
			stmtType = "UPDATE";
			break;
		case TRANSPORT.TYPE_DELETE:
			stmtType = "DELETE";
			break;
		case TRANSPORT.TYPE_INSERT:
		case TRANSPORT.TYPE_INSERT_PARAM:
			stmtType = "INSERT";
			break;
		case TRANSPORT.TYPE_CREATE:
			stmtType = "CREATE";
			break;
		case TRANSPORT.TYPE_GRANT:
			stmtType = "GRANT";
			break;
		case TRANSPORT.TYPE_DROP:
			stmtType = "DROP";
			break;
		case TRANSPORT.TYPE_CALL:
			stmtType = "CALL";
			break;
		case TRANSPORT.TYPE_EXPLAIN:
			stmtType = "EXPLAIN";
			break;
		case TRANSPORT.TYPE_STATS:
			stmtType = "INFOSTATS";
			break;
		case TRANSPORT.TYPE_CONFIG:
			stmtType = "CONFIG";
			break;
		default:
			stmtType = "";
			break;
		}

		return stmtType;
	}

	/**
	 * Use this method to retrieve the statement type that was used when
	 * creating the statement through the connectivity service. ie. SELECT,
	 * UPDATE, DELETE, INSERT.
	 */
	public short getStatementTypeShort() {
		return sqlStmtType_;
	}

	/**
	 * Use this method to retrieve the statement type that was used when
	 * creating the statement through the connectivity service. ie. SELECT,
	 * UPDATE, DELETE, INSERT.
	 */
	public int getStatementTypeInt() {
		return ist_.getSqlQueryType();
	}

	ArrayList<String> getKeyColumns() {
		return keyColumns;
	}

	void setKeyColumns(ArrayList<String> keyColumns) {
		this.keyColumns = keyColumns;
	}

	ArrayList<String> keyColumns = null;

	int paramRowCount_;
	String moduleName_;
	int moduleVersion_;
	long moduleTimestamp_;

	private boolean isAnyLob = false;
    private long lobRowCount;
    private int[] LobBatch;

    private ArrayList<TrafT4Lob> lobObjects;
    private ArrayList<String> lobColNames;
    private ArrayList<Integer> lobColIds;
    private String[] lobLocators;
    private boolean addLobColNames = false; // lobColNames are added for one row
    private boolean resetLobObjects = false;

	ArrayList<Object[]> rowsValue_;
	Object[] paramsValue_;



	// boolean singleton_ = false;

	// ================ SQL Statement type ====================
	public static final short TYPE_UNKNOWN = 0;
	public static final short TYPE_SELECT = 0x0001;
	public static final short TYPE_UPDATE = 0x0002;
	public static final short TYPE_DELETE = 0x0004;
	public static final short TYPE_INSERT = 0x0008;
	public static final short TYPE_EXPLAIN = 0x0010;
	public static final short TYPE_CREATE = 0x0020;
	public static final short TYPE_GRANT = 0x0040;
	public static final short TYPE_DROP = 0x0080;
	public static final short TYPE_INSERT_PARAM = 0x0100;
	public static final short TYPE_SELECT_CATALOG = 0x0200;
	public static final short TYPE_SMD = 0x0400;
	public static final short TYPE_CALL = 0x0800;
	public static final short TYPE_STATS = 0x1000;
	public static final short TYPE_CONFIG = 0x2000;

	// =================== SQL Query ===================
	public static final int SQL_OTHER = -1;
	public static final int SQL_UNKNOWN = 0;
	public static final int SQL_SELECT_UNIQUE = 1;
	public static final int SQL_SELECT_NON_UNIQUE = 2;
	public static final int SQL_INSERT_UNIQUE = 3;
	public static final int SQL_INSERT_NON_UNIQUE = 4;
	public static final int SQL_UPDATE_UNIQUE = 5;
	public static final int SQL_UPDATE_NON_UNIQUE = 6;
	public static final int SQL_DELETE_UNIQUE = 7;
	public static final int SQL_DELETE_NON_UNIQUE = 8;
	public static final int SQL_CONTROL = 9;
	public static final int SQL_SET_TRANSACTION = 10;
	public static final int SQL_SET_CATALOG = 11;
	public static final int SQL_SET_SCHEMA = 12;

	// =================== new identifiers ===================
	public static final int SQL_CREATE_TABLE = SQL_SET_SCHEMA + 1;
	public static final int SQL_CREATE_VIEW = SQL_CREATE_TABLE + 1;
	public static final int SQL_CREATE_INDEX = SQL_CREATE_VIEW + 1;
	public static final int SQL_CREATE_UNIQUE_INDEX = SQL_CREATE_INDEX + 1;
	public static final int SQL_CREATE_SYNONYM = SQL_CREATE_UNIQUE_INDEX + 1;
	public static final int SQL_CREATE_VOLATILE_TABLE = SQL_CREATE_SYNONYM + 1;;
	public static final int SQL_CREATE_MV = SQL_CREATE_VOLATILE_TABLE + 1;
	public static final int SQL_CREATE_MVG = SQL_CREATE_MV + 1;
	public static final int SQL_CREATE_MP_ALIAS = SQL_CREATE_MVG + 1;
	public static final int SQL_CREATE_PROCEDURE = SQL_CREATE_MP_ALIAS + 1;
	public static final int SQL_CREATE_TRIGGER = SQL_CREATE_PROCEDURE + 1;
	public static final int SQL_CREATE_SET_TABLE = SQL_CREATE_TRIGGER + 1;
	public static final int SQL_CREATE_MULTISET_TABLE = SQL_CREATE_SET_TABLE + 1;

	public static final int SQL_DROP_TABLE = SQL_CREATE_MULTISET_TABLE + 1;
	public static final int SQL_DROP_VIEW = SQL_DROP_TABLE + 1;
	public static final int SQL_DROP_INDEX = SQL_DROP_VIEW + 1;
	public static final int SQL_DROP_SYNONYM = SQL_DROP_INDEX + 1;
	public static final int SQL_DROP_VOLATILE_TABLE = SQL_DROP_SYNONYM + 1;;
	public static final int SQL_DROP_MV = SQL_DROP_VOLATILE_TABLE + 1;
	public static final int SQL_DROP_MVG = SQL_DROP_MV + 1;
	public static final int SQL_DROP_MP_ALIAS = SQL_DROP_MVG + 1;
	public static final int SQL_DROP_PROCEDURE = SQL_DROP_MP_ALIAS + 1;
	public static final int SQL_DROP_TRIGGER = SQL_DROP_PROCEDURE + 1;
	public static final int SQL_DROP_SET_TABLE = SQL_DROP_TRIGGER + 1;
	public static final int SQL_DROP_MULTISET_TABLE = SQL_DROP_SET_TABLE + 1;

	public static final int SQL_ALTER_TABLE = SQL_DROP_MULTISET_TABLE + 1;
	public static final int SQL_ALTER_INDEX = SQL_ALTER_TABLE + 1;
	public static final int SQL_ALTER_TRIGGER = SQL_ALTER_INDEX + 1;
	public static final int SQL_ALTER_MP_ALIAS = SQL_ALTER_TRIGGER + 1;
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

	public Object unwrap(Class iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isWrapperFor(Class iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNString(int parameterIndex, String x)
			throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }

        validateSetInvocation(parameterIndex);
        int dataType = getInputDataType(parameterIndex - 1);

        switch (dataType) {
        case Types.CHAR:
        case Types.VARCHAR:
            addParamValue(parameterIndex, x);
            break;
        default:
            throw TrafT4Messages.createSQLException(getT4props(),
                    "object_type_not_supported", JDBCType.valueOf(dataType));
        }
	}

	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
	    setCharacterStream(parameterIndex, reader, length);
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		setBinaryStream(parameterIndex, inputStream, (int) length);
	}

	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex, length);
        }
	    if(length > Integer.MAX_VALUE) {
	        TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "setAsciiStream() unsupport length larger than maximun of int");
	    }
        setAsciiStream(parameterIndex, x, (int)length);
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex, length);
        }
        if(length > Integer.MAX_VALUE) {
            TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "setBinaryStream() unsupport length larger than maximun of int");
        }
		setBinaryStream(parameterIndex, x, (int)length);
	}

	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex, length);
        }
        if(length > Integer.MAX_VALUE) {
            TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "setCharacterStream() unsupport length larger than maximun of int");
        }
		setCharacterStream(parameterIndex, reader, (int)length);
	}

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex);
        }
        validateSetInvocation(parameterIndex);

        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex);
        }
        if (x == null) {
            addParamValue(parameterIndex, null);
            return;
        }
        validateSetInvocation(parameterIndex);
        int len = 4 * 1024;
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                byte[] readBytes = new byte[len];
                byte[] buffer = new byte[len];
                byte[] tmpBytes = buffer;
                int readLen = 0;
                int totalReadLen = 0;
                int multi = 1;
                try {
                    while ((readLen = x.read(readBytes)) != -1) {
                        totalReadLen += readLen;
                        if (totalReadLen > len) {
                            len *= ++multi;
                            buffer = new byte[len];
                            System.arraycopy(tmpBytes, 0, buffer, 0, tmpBytes.length);
                            tmpBytes = buffer;
                        }
                        System.arraycopy(readBytes, 0, buffer, totalReadLen - readLen, readLen);
                    }
                    if (totalReadLen < len) {
                        buffer = new byte[totalReadLen];
                        System.arraycopy(tmpBytes, 0, buffer, 0, totalReadLen);
                    }
                } catch (IOException e) {
                    throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
                }

                addParamValue(parameterIndex, buffer);
                break;
            case Types.CLOB:
                isAnyLob = true;
                TrafT4Clob lob = new TrafT4Clob(this, inputDesc_[parameterIndex - 1], x, Integer.MAX_VALUE);
                addParamValue(parameterIndex, new byte[0]);
                addLobObjects(parameterIndex, lob);
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex);
        }
        if (x == null) {
            addParamValue(parameterIndex, null);
            return;
        }
        validateSetInvocation(parameterIndex);
        int len = 4 * 1024;
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                byte[] readBytes = new byte[len];
                byte[] buffer = new byte[len];
                byte[] tmpBytes = buffer;
                int readLen = 0;
                int totalReadLen = 0;
                int multi = 1;
                try {
                    while ((readLen = x.read(readBytes)) != -1) {
                        totalReadLen += readLen;
                        if (totalReadLen > len) {
                            len *= ++multi;
                            buffer = new byte[len];
                            System.arraycopy(tmpBytes, 0, buffer, 0, tmpBytes.length);
                            tmpBytes = buffer;
                        }
                        System.arraycopy(readBytes, 0, buffer, totalReadLen - readLen, readLen);
                    }
                    if (totalReadLen < len) {
                        buffer = new byte[totalReadLen];
                        System.arraycopy(tmpBytes, 0, buffer, 0, totalReadLen);
                    }
                } catch (IOException e) {
                    throw TrafT4Messages
                            .createSQLException(getT4props(), "io_exception", e.getMessage());
                }
                addParamValue(parameterIndex, buffer);
                break;
            case Types.BLOB:
                isAnyLob = true;
                TrafT4Lob lob = new TrafT4Blob(this, inputDesc_[parameterIndex - 1], x,
                        Integer.MAX_VALUE);
                addParamValue(parameterIndex, new byte[0]);
                addLobObjects(parameterIndex, lob);
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", parameterIndex);
        }
        if (x == null) {
            addParamValue(parameterIndex, null);
            return;
        }
        validateSetInvocation(parameterIndex);
        int len = 4 * 1024;
        int dataType = getInputDataType(parameterIndex - 1);
        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                char[] readChars = new char[len];
                char[] buffer = new char[len];
                char[] tmpChars = buffer;
                int readLen = 0;
                int totalReadLen = 0;
                int multi = 1;
                try {
                    while ((readLen = x.read(readChars)) != -1) {
                        totalReadLen += readLen;
                        if (totalReadLen > len) {
                            len *= ++multi;
                            buffer = new char[len];
                            System.arraycopy(tmpChars, 0, buffer, 0, tmpChars.length);
                            tmpChars = buffer;
                        }
                        System.arraycopy(readChars, 0, buffer, totalReadLen - readLen, readLen);
                    }
                    if (totalReadLen < len) {
                        buffer = new char[totalReadLen];
                        System.arraycopy(tmpChars, 0, buffer, 0, totalReadLen);
                    }
                } catch (IOException e) {
                    throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
                }

                addParamValue(parameterIndex, Bytes.getBytes(buffer));
                break;
            case Types.CLOB:
                isAnyLob = true;
                TrafT4Clob lob = new TrafT4Clob(this, inputDesc_[parameterIndex - 1], x, Integer.MAX_VALUE);
                addParamValue(parameterIndex, new byte[0]);
                addLobObjects(parameterIndex, lob);
                break;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
	    if (reader instanceof StringReader) {
            try {
                long length = ((StringReader) reader).skip(Long.MAX_VALUE);
                ((StringReader) reader).reset();
                setCharacterStream(parameterIndex, reader, length);
            } catch (IOException e) {
                throw TrafT4Messages.createSQLException(getT4props(), "io_exception",
                        e.getMessage());
            }
        } else if (reader instanceof TrafT4Reader) {
            long length = ((TrafT4Reader) reader).getLength();
            setCharacterStream(parameterIndex, reader, length);
        } else {
            setCharacterStream(parameterIndex, reader);
        }
	}

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        if (inputStream instanceof ByteArrayInputStream) {
            int length = ((ByteArrayInputStream) inputStream).available();
            setBinaryStream(parameterIndex, inputStream, length);
        } else if (inputStream instanceof TrafT4InputStream) {
            long length = ((TrafT4InputStream) inputStream).getLength();
            setBinaryStream(parameterIndex, inputStream, length);
        } else {
            setBinaryStream(parameterIndex, inputStream);
        }
    }

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

    protected long getPrepareTimeMills() {
        return ist_.getPrepareTimeMills();
    }

    public boolean isCachedPrepareStmtFull() {
        return ist_.isCachedPreparedStmtFull();
    }

    protected void setCachedPreparedStmtFull(boolean cachedPreparedStmtFull) {
        ist_.setCachedPreparedStmtFull(cachedPreparedStmtFull);
    }

    public boolean isPreparedStmtUseESP() {
        return ist_.isPreparedStmtUseESP();
    }

    protected void setPreparedStmtUseESP(boolean cachedPreparedStmtFull) {
        ist_.setPreparedStmtUseESP(cachedPreparedStmtFull);
    }

    private void addLobObjects(int parameterIndex, TrafT4Lob x) {
        if (lobObjects == null) {
            lobObjects = new ArrayList<TrafT4Lob>();
        }
        if (lobColNames == null) {
            lobColNames = new ArrayList<String>();
            lobColIds = new ArrayList<Integer>();
        }
        clearLobObjects();
        if (!addLobColNames && !lobColIds.contains(parameterIndex)) {
            lobColNames.add(getInputHeadingName(parameterIndex -1));
            lobColIds.add(parameterIndex);
        }
        lobObjects.add(x);
    }

    private void clearLobObjects() {
        if (resetLobObjects) {
            if (lobObjects != null) {
                lobObjects.clear();
                lobLocators = null;
            }
            resetLobObjects = false;
        }
        if (addLobColNames) {
            if (lobColNames != null) {
                lobColNames.clear();
                lobColIds.clear();
            }
            addLobColNames = false;
        }
    }

    private int getNumLobColumns() {
        if (lobColNames == null)
            return 0;
        else
            return lobColNames.size();
    }

    private String getLobColumns() {
        if (lobColNames == null)
            return "";
        StringBuilder colNames = new StringBuilder();
        colNames.append(lobColNames.get(0));
        for (int i = 1; i < lobColNames.size(); i++)
            colNames.append(", ").append(lobColNames.get(i));
        return colNames.toString();
    }

    private String[] getLobHandles() throws SQLException {
        TrafT4PreparedStatement lobLocatorStmt = null;
        TrafT4ResultSet lobLocatorRS = null;
        //int lBatchBindingSize = getT4props().getbatchBindingSize_;
        try {
            String selectForLobHandle = "select " + getLobColumns() + " from ( " + this.sql_ + " ) as x";
//            connection_.batchBindingSize_ = paramRowCount_;
            lobLocatorStmt = (TrafT4PreparedStatement) connection_.prepareStatement(selectForLobHandle);
            copyParameters(lobLocatorStmt);
            // when there finish executeBatch, the finally block will do clearBatch,
            // this will set paramRowCount_ as 0, so a tmp variable here to hold the paramRowCount_
            int tmpParamRowCount = lobLocatorStmt.paramRowCount_;
            if (tmpParamRowCount == 0) {
                lobLocatorRS = (TrafT4ResultSet) lobLocatorStmt.executeQuery();
                lobRowCount = lobLocatorStmt.ist_.getRowCount();
            } else {
                LobBatch = lobLocatorStmt.executeBatch();
                lobLocatorRS = (TrafT4ResultSet) lobLocatorStmt.getResultSet();
            }
            int numLocators = ((tmpParamRowCount == 0 ? lobLocatorStmt.paramRowCount_= 1 : tmpParamRowCount)
                    * getNumLobColumns());

            String lobLocators[] = new String[numLocators];
            int locatorsIdx = 0;
            while (lobLocatorRS.next()) {
                for (int i = 0; i < getNumLobColumns(); i++) {
                    if (locatorsIdx < lobLocators.length)
                        lobLocators[locatorsIdx++] = lobLocatorRS.getLobHandle(i + 1);
                    else
                        throw TrafT4Messages.createSQLException(getT4props(),
                                "locators out of space");
                }
            }
            return lobLocators;
        } finally {
            if (lobLocatorRS != null)
                lobLocatorRS.close();
            if (lobLocatorStmt != null)
                lobLocatorStmt.close();
//            connection_.batchBindingSize_ = lBatchBindingSize;
        }
    }

    private void setLobHandles() throws SQLException {
        if (lobLocators.length != lobObjects.size())
            throw TrafT4Messages.createSQLException(getT4props(),
                    "lob_objects_and_locators_dont_match");
        int lobLocatorIdx = 0;
        for (TrafT4Lob lobObject : lobObjects) {
            if (lobObject != null)
                lobObject.setLobHandleByInlineData(lobLocators[lobLocatorIdx]);
            lobLocatorIdx++;
        }
    }

    // deep copy of parameters
    void copyParameters(TrafT4PreparedStatement other) {
        other.paramRowCount_ = paramRowCount_;
        other.paramsValue_ = new Object[other.inputDesc_.length];
        if (rowsValue_ != null) {
            if (other.rowsValue_ == null) {
                other.rowsValue_ = new ArrayList<Object[]>(rowsValue_.size());
            }
            other.rowsValue_.addAll(rowsValue_);
        } else {
            for (int paramNumber = 0; paramNumber < other.inputDesc_.length; paramNumber++) {
                int type = other.getInputDataType(paramNumber);
                switch (type) {
                    case Types.BLOB:
                    case Types.CLOB:
                        other.paramsValue_[paramNumber] = new byte[0];// an empty byte will be a lob handle
                        break;
                    default:
                        other.paramsValue_[paramNumber] = paramsValue_[paramNumber];
                        break;
                }
                other.inputDesc_[paramNumber].isValueSet_ = true;
            }
        }
    }

    protected void populateLobObjects() throws SQLException {
        try {
            TrafT4Lob lob;
            if (lobObjects != null) {
                for (int i = 0; i < lobObjects.size(); i++) {
                    lob = lobObjects.get(i);
                    if (lob == null) {
                        continue;
                    }
                    lob.populate();
                }
            }
        } finally {

        }
        resetLobObjects = true;
        addLobColNames = true;
    }

    protected TraceLinkThreshold getTraceLinkThreshold() {
        return traceLinkThreshold;
    }

    protected ArrayList<TraceLinkThreshold> getTraceLinkList() {
        return traceLinkList;
    }

    protected boolean isBatchFlag() {
        return batchFlag;
    }

    protected void setBatchFlag(boolean flag) {
        this.batchFlag = flag;
    }

    protected int getStartRowNum() {
        return startRowNum;
    }

    protected void setStartRowNum(int count) {
        this.startRowNum = count;
    }
}
