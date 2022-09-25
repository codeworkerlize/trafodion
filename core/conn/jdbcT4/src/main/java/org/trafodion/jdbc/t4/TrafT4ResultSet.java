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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DataTruncation;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import org.trafodion.jdbc.t4.utils.JDBCType;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

// ----------------------------------------------------------------------------
// This class partially implements the result set class as defined in 
// java.sql.ResultSet.  
// ----------------------------------------------------------------------------
public class TrafT4ResultSet extends TrafT4Handle implements java.sql.ResultSet {
    private static final long FETCH_BYTES_LIMIT = 1024 * 1024 * 1024;
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4ResultSet.class);

    private void throwIfClose() throws TrafT4Exception {
        if (isClosed_) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_cursor_state");
        }
    }

    private void throwIfForwardOnly() throws SQLException {
        if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
            throw TrafT4Messages.createSQLException(getT4props(), "forward_only_cursor");
        }
    }
    private void throwIfConcurReadOnly() throws SQLException {
        if (getConcurrency() == ResultSet.CONCUR_READ_ONLY) {
            throw TrafT4Messages.createSQLException(getT4props(), "read_only_concur");
        }
    }

    private void throwIfOnInsertRow() throws TrafT4Exception {
        if (onInsertRow_) {
            String method = Thread.currentThread().getStackTrace()[2].getMethodName() + "()";
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_cursor_position", method);
        }
    }
    private void throwIfNotOnInsertRow() throws TrafT4Exception {
        if (!onInsertRow_) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_cursor_position_2");
        }
    }

    private int getISOMapping() {
        return this.irs_.ic_.getISOMapping();
    }

    private boolean getEnforceISO() {
        return this.irs_.ic_.getEnforceISO();
    }

    private String getISO88591() {
        return irs_.ic_.getT4props().getISO88591();
    }

	T4Properties getT4props() {
		if (this.connection_ != null )
			return this.connection_.getT4props();
		return new T4Properties();
	}

	// java.sql.ResultSet interface methods
	public boolean absolute(int row) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", row);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", row);
        }
        boolean flag = false;
		int absRow;

		clearWarnings();
		throwIfClose();
		throwIfForwardOnly();
		throwIfOnInsertRow();

        if (row == 0) {
            beforeFirst();
            return flag;
        }


		if (row > 0) {
			if (row <= numRows_) {
				currentRow_ = row;
				isBeforeFirst_ = false;
				isAfterLast_ = false;
				onInsertRow_ = false;
				flag = true;
			} else {
				do {
					flag = next();
					if (!flag) {
						break;
					}
				} while (currentRow_ < row);
			}
		} else {
			absRow = -row;
			afterLast();
			if (absRow <= numRows_) {
				currentRow_ = numRows_ - absRow + 1;
				isAfterLast_ = false;
				isBeforeFirst_ = false;
				onInsertRow_ = false;
				flag = true;
			} else {
				beforeFirst();
			}
		}
		return flag;
	}

	public void afterLast() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		clearWarnings();
		throwIfClose();
		throwIfForwardOnly();
		throwIfOnInsertRow();

		last();
		// currentRow_++;
		isAfterLast_ = true;
		isBeforeFirst_ = false;
	}

	public void beforeFirst() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		clearWarnings();
		throwIfClose();
		throwIfForwardOnly();
		throwIfOnInsertRow();

		currentRow_ = 0;
		isBeforeFirst_ = true;
		isAfterLast_ = false;
		onInsertRow_ = false;
	}

    public void cancelRowUpdates() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();
        throwIfConcurReadOnly();
        clearWarnings();
        throwIfOnInsertRow();

        ObjectArray currentRow = getCurrentRow();
        // If no updates have been made or updateRow has already been called, this method has noeffect.
        if (!currentRow.isUpdated()) {
            currentRow.backArrayElement();
        }
    }

	/**
	 * Close the resultSet. This method is synchronized to prevent many threads
	 * talking to the same server after close().
	 * 
	 * @throws SQLException
	 */
	synchronized public void close() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		clearWarnings();
		if (isClosed_) {
			return;
		}
		if (connection_._isClosed()) {
			connection_.closeErroredConnection(null);
			return;
		}


		if (stmt_ instanceof TrafT4PreparedStatement) {
			close(false);
		} else {
			close(true);
		}
	}

    public void deleteRow() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();
        throwIfConcurReadOnly();
        throwIfOnInsertRow();

        prepareDeleteStmt();
        ObjectArray currentRow = getCurrentRow();
        // currentRow
        try {
            int paramIndex = 1;
            for (int i = 1; i <= getNumOfColumns(); i++) {
                if (paramCols_.get(i - 1)) {
                    Object object = currentRow.getUpdatedArrayElement(i);
                    boolean nchar = InterfaceResultSet.SQLTYPECODE_CHAR == getSQLDataType(i)
                            && InterfaceUtilities.SQLCHARSETCODE_UNICODE == getSqlCharset(i);
                    // for the situation, the column is nchar data type, when do update/insert/delete
                    // there should change the raw byte array from server side to string,
                    // in order to avoid data len problem
                    if (object instanceof byte[] && nchar && !currentRow.isElementUpdated(i)) {
                        deleteStmt_.setObject(paramIndex++, getLocalString(i, false));
                    } else {
                        deleteStmt_.setObject(paramIndex++, object);
                    }
                }
            }
            int count = deleteStmt_.executeUpdate();
            if (count == 0) {
                throw TrafT4Messages.createSQLException(getT4props(), "row_modified");
            }
            // Remove the row from the resultSet
            cachedRows_.remove(--currentRow_);
            --numRows_;
            if ((getType() == ResultSet.TYPE_FORWARD_ONLY)
                    && (getConcurrency() == ResultSet.CONCUR_UPDATABLE)) {
                int temp = currentRowCount_;
                if (!next()) {
                    if (temp == 1) {
                        isBeforeFirst_ = true;
                    }
                    currentRowCount_ = 0;
                } else {
                    --currentRowCount_;
                }
            } else {
                if (currentRow_ == 0) {
                    isBeforeFirst_ = true;
                }
            }
            rowDeleted = true;
        } catch (SQLException e) {
            performConnectionErrorChecks(e);
            throw e;
        }
    }


    public int findColumn(String columnName) throws SQLException {
        // Need to check if uppercase is ok and column name for expression
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();
        if (colMap_ == null) {
            colMap_ = new HashMap<String, Integer>();
            for (int columnIndex = 1; columnIndex <= outputDesc_.length; columnIndex++) {
                if(!colMap_.containsKey(getName(columnIndex))){
                    colMap_.put(getName(columnIndex).toUpperCase(), columnIndex);
                }
            }
        }
        if (columnName == null) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_column_name");
        }
        Integer colIdx = colMap_.get(columnName.toUpperCase());
        if (colIdx == null) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_column_name");
        }
        return colIdx;
    }

	public boolean first() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		boolean flag = true;

		clearWarnings();
		throwIfClose();
		throwIfForwardOnly();
		throwIfOnInsertRow();

		if (isBeforeFirst_) {
			flag = next();
		}
		if (numRows_ > 0) {
			currentRow_ = 1;
			isAfterLast_ = false;
			isBeforeFirst_ = false;
			onInsertRow_ = false;
		}
		return flag;
	}

	// JDK 1.2
	public Array getArray(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "getArray()");
		return null;
	}

	// JDK 1.2
	public Array getArray(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getArray(columnIndex);
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
        validateGetInvocation(columnIndex);
        // For LOB Support - SB 10/8/2004
        int dataType = getDataType(columnIndex);
        switch (dataType) {

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                String data = getString(columnIndex);
                if (data != null) {
                    return new ByteArrayInputStream(data.getBytes());
                } else {
                    return null;
                }
            case Types.BLOB:
            case Types.CLOB:
                Clob clob = getClob(columnIndex);
                return clob.getAsciiStream();
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }

    }

	public InputStream getAsciiStream(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getAsciiStream(columnIndex);
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);
		outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// String returned may not be numeric in case of SQL_CHAR, SQL_VARCHAR
		// and SQL_LONGVARCHAR
		// fields. Hoping that java might throw invalid value exception
		String data = getLocalString(columnIndex);
		if (data != null && data.trim().length() != 0) {
			data = data.trim();
            BigDecimal retValue;
            Double d;
			try {
				String strData = new BigDecimal(data).toPlainString();
				retValue = new BigDecimal(strData);
			} catch (NumberFormatException e) {
				try {
					d = new Double(data);
				} catch (NumberFormatException e1) {
					throw TrafT4Messages.createSQLException(getT4props(), "invalid_cast_specification");
				}
				retValue = new BigDecimal(d.doubleValue());
			}
			return retValue;
		} else {
			return null;
		}
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, scale);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, scale);
        }
		BigDecimal retValue = getBigDecimal(columnIndex);
		if (retValue != null) {
			return retValue.setScale(scale);
		} else {
			return null;
		}
	}

	public BigDecimal getBigDecimal(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getBigDecimal(columnIndex);
	}

	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, scale);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, scale);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getBigDecimal(columnIndex, scale);
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);

		// For LOB Support - SB 10/8/2004
		int dataType = getDataType(columnIndex);
		switch (dataType) {
		

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		    byte[] data = getBytes(columnIndex);
			if (data != null) {
				return new ByteArrayInputStream(data);
			} else {
				return null;
			}
		case Types.BLOB:
            Blob blob = getBlob(columnIndex);
            if (blob != null) {
                return blob.getBinaryStream();
            } else {
                return null;
            }
		default:
            throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type", JDBCType.valueOf(dataType));
		}
	}

	public InputStream getBinaryStream(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getBinaryStream(columnIndex);
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);
		outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());
		String data = getLocalString(columnIndex);
		if (data != null) {
			data = data.trim();
			if ((data.equalsIgnoreCase("true")) || (data.equalsIgnoreCase("1"))) {
				return true;
			} else if ((data.equalsIgnoreCase("false")) || (data.equalsIgnoreCase("0"))) {
				return false;
			} else {
			    short shortValue;
				try {
					shortValue = getShort(columnIndex);
				} catch (NumberFormatException e) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "invalid_cast_specification");
				}
				switch (shortValue) {
				case 0:
					return false;
				case 1:
					return true;
				default:
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "numeric_out_of_range", shortValue);
				}
			}
		} else {
			return false;
		}
	}

	public boolean getBoolean(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getBoolean(columnIndex);
	}

	public byte getByte(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);
		outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());
		String data = getLocalString(columnIndex);
		if (data != null) {
		    byte retValue;
		    Double d;
		    double d1;
			try {
				retValue = Byte.parseByte(data);
			} catch (NumberFormatException e) {
				try {
					d = new Double(data);
				} catch (NumberFormatException e1) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "invalid_cast_specification");
				}

				d1 = d.doubleValue();
				// To allow -128.999.. and 127.999...
				if (d1 > (double) Byte.MIN_VALUE - 1 && d1 < (double) Byte.MAX_VALUE + 1) {
					retValue = d.byteValue();
					if ((double) retValue != d1) {
						setSQLWarning(null, "data_truncation", null);
					}
				} else {
                    throw TrafT4Messages.createSQLException(getT4props(), "numeric_out_of_range",
                            d1);
				}
			}
			return retValue;
		} else {
			return 0;
		}
	}

	public byte getByte(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getByte(columnIndex);
	}

    public byte[] getBytes(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
        validateGetInvocation(columnIndex);
        int dataType = getDataType(columnIndex);
        switch (dataType) {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.CHAR:
            case Types.VARCHAR: // Extension allows varchar and
            case Types.LONGVARCHAR: // longvarchar data types
            case Types.DATE:
            case Types.TIME:
                Object x = getCurrentRow().getUpdatedArrayElement(columnIndex);
                if (x == null) {
                    wasNull_ = true;
                    return null;
                }
                wasNull_ = false;
                if (x instanceof byte[]) {
                    return (byte[]) x;
                } else {
                    return x.toString().getBytes();
                }
            case Types.BLOB:
                x = getLobHandle(columnIndex);
                TrafT4Blob blob = new TrafT4Blob(stmt_, (String)x, outputDesc_[columnIndex - 1]);
                return blob.getBytes(1, (int) blob.length());
            case Types.CLOB:
                x = getLobHandle(columnIndex);
                TrafT4Clob clob = new TrafT4Clob(stmt_, (String)x, outputDesc_[columnIndex - 1]);
                return clob.getSubString(1, (int) clob.length()).getBytes();
            case Types.TIMESTAMP://Because of precision problems, TimeStamp needs to use String method
                String stringData = getString(columnIndex);
                if (stringData == null) {
                    return null;
                } else {
                    return stringData.getBytes();
                }
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type", JDBCType.valueOf(dataType));
        }
    }

	public byte[] getBytes(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getBytes(columnIndex);
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
        String data;

        validateGetInvocation(columnIndex);
        int dataType = getDataType(columnIndex);
        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                data = getString(columnIndex);
                if (data != null) {
                    return new StringReader(data);
                } else {
                    return null;
                }
            case Types.BLOB:
            case Types.CLOB:
                Clob clob = getClob(columnIndex);
                return clob.getCharacterStream();
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }

    }

	public Reader getCharacterStream(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getCharacterStream(columnIndex);
	}

	public int getConcurrency() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
		if (stmt_ != null) {
			return stmt_.resultSetConcurrency_;
		} else {
			return ResultSet.CONCUR_READ_ONLY;
		}
	}

	public String getCursorName() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
		if (spj_rs_ && stmtLabel_ != null) {
			return stmtLabel_;
		} else if (stmt_ != null) {
			String cursorName = stmt_.cursorName_;
			if (cursorName == null || cursorName.trim().equals("")) {
				cursorName = stmt_.stmtLabel_;
			}
			return cursorName;
		} else {
			return null;
		}
	}

	public Date getDate(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);
		int dataType = getDataType(columnIndex);

        switch (dataType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.DATE:
            case Types.TIMESTAMP:
                String data = getLocalString(columnIndex);
                if (data != null && data.trim().length() != 0) {
                    Date retValue;
                    try {
                        boolean convertDate = connection_.getDateConversion();
                        if (getT4props().isLogEnable(Level.FINER)) {
                            String temp = "Convert Date=" + convertDate;
                            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Convert Date= {}", convertDate);
                        }
                        if (convertDate) {
                            String dt = Utility.convertDateFormat(data);
                            retValue = Utility.valueOf(dt);
                        } else {
                            retValue = Date.valueOf(data);
                        }
                    } catch (IllegalArgumentException e) {
                        data = data.trim();
                        int endIndex;
                        if ((endIndex = data.indexOf(' ')) != -1) {
                            data = data.substring(0, endIndex);
                        }
                        try {
                            retValue = Date.valueOf(data);
                            setSQLWarning(null, "data_truncation", null);

                        } catch (IllegalArgumentException ex) {
                            throw TrafT4Messages.createSQLException(getT4props(),
                                    "invalid_cast_specification");
                        }
                    }
                    return retValue;
                } else {
                    return null;
                }
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type", JDBCType.valueOf(dataType));
        }
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, cal);
        }
		Date sqlDate = getDate(columnIndex);
		if (sqlDate != null) {
			if (cal != null) {
				cal.setTime(sqlDate);
				sqlDate = new Date(cal.getTimeInMillis());
			}
			return sqlDate;
		} else {
			return null;
		}
	}

	public Date getDate(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getDate(columnIndex);
	}

	public Date getDate(String columnName, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, cal);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getDate(columnIndex, cal);
	}

	public double getDouble(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);
		outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());
		String data = getLocalString(columnIndex);
		if (data != null && data.trim().length() != 0) {
			try {
				return Double.parseDouble(data);
			} catch (NumberFormatException e1) {
                throw TrafT4Messages.createSQLException(getT4props(), "invalid_cast_specification");
			}
		} else {
			return 0;
		}
	}

	public double getDouble(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getDouble(columnIndex);
	}

	public int getFetchDirection() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
		return fetchDirection_;
	}

	public int getFetchSize() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
		return fetchSize_;
	}

	public float getFloat(int columnIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);

		outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// parseFloat doesn't return error when
		// the value exceds the float max
		double data = getDouble(columnIndex);
		if (data >= Float.NEGATIVE_INFINITY && data <= Float.POSITIVE_INFINITY) {
			return (float) data;
		} else {
            throw TrafT4Messages.createSQLException(getT4props(), "numeric_out_of_range", data);
		}
	}

	public float getFloat(String columnName) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getFloat(columnIndex);
	}

	public int getInt(int columnIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		int retValue;

		validateGetInvocation(columnIndex);
		outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());
		String data = getLocalString(columnIndex);
		if (data != null && data.trim().length() != 0) {
			try {
				retValue = Integer.parseInt(data);
			} catch (NumberFormatException e) {
			    double d;
				try {
					d = new Double(data).doubleValue();
				} catch (NumberFormatException e1) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "invalid_cast_specification");
				}

				if (d > (double) Integer.MIN_VALUE - 1 && d < (double) Integer.MAX_VALUE + 1) {
					retValue = (int) d;
					if ((double) retValue != d) {
						setSQLWarning(null, "data_truncation", null);
					}
				} else {
                    throw TrafT4Messages.createSQLException(getT4props(), "numeric_out_of_range",
                            d);
				}
			}
		} else {
			retValue = 0;
		}

		return retValue;
	}

	public int getInt(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getInt(columnIndex);
	}

	public long getLong(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
        long retValue;

		validateGetInvocation(columnIndex);
		outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());
		String data = getLocalString(columnIndex);

		if (data != null && data.trim().length() != 0) {
			try {
				retValue = Long.parseLong(data);
			} catch (NumberFormatException e) {
			    BigDecimal bd;
				try {
					bd = new BigDecimal(data);
					retValue = bd.longValue();
					if (bd.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0
							&& bd.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0) {
						
						if (bd.compareTo(BigDecimal.valueOf(retValue)) != 0) {
							setSQLWarning(null, "data_truncation", null);
						}
					} else {
                        throw TrafT4Messages.createSQLException(getT4props(),
                                "numeric_out_of_range", bd);
					}
				} catch (NumberFormatException e2) {
				    double d;
					try {
						d = new Double(data).doubleValue();
					} catch (NumberFormatException e1) {
                        throw TrafT4Messages.createSQLException(getT4props(),
                                "invalid_cast_specification");
					}

					if (d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
						retValue = (long) d;
						
						if ((double) retValue != d) {
							setSQLWarning(null, "data_truncation", null);
						}
					} else {
                        throw TrafT4Messages.createSQLException(getT4props(),
                                "numeric_out_of_range", d);
					}
				}
			}
		} else {
			retValue = 0;
		}

		return retValue;
	}

	public long getLong(String columnName) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getLong(columnIndex);
	}

	public ResultSetMetaData getMetaData() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
		return new TrafT4ResultSetMetaData(this, outputDesc_);
	}

	public Object getObject(int columnIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		// byte byteValue;
		short shortValue;
		int intValue;
		// long longValue;
		float floatValue;
		double doubleValue;
		boolean booleanValue;

		validateGetInvocation(columnIndex);
		int dataType = getDataType(columnIndex);
		int precision = getSqlPrecision(columnIndex);
		int sqltype = getSQLDataType(columnIndex);
		switch (dataType) {
		case Types.TINYINT:
			if (sqltype == InterfaceResultSet.SQLTYPECODE_TINYINT_UNSIGNED) {
				short s = getShort(columnIndex);
				if (wasNull_) {
					return null;
				}
				return s;
			} else {
				byte b = getByte(columnIndex);
				if (wasNull_) {
					return null;
				}
				return b;
			}
		case Types.SMALLINT:
			shortValue = getShort(columnIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Short(shortValue);
			}
		case Types.INTEGER:
			intValue = getInt(columnIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Integer(intValue);
			}
		case Types.BIGINT:
			if (sqltype == InterfaceResultSet.SQLTYPECODE_LARGEINT_UNSIGNED) {
				BigDecimal bd = getBigDecimal(columnIndex);
				if (wasNull_) {
					return null;
				}
				return bd;
			} else {
				long l = getLong(columnIndex);
                if (wasNull_) {
                    return null;
                }
				return l;
			}
		case Types.REAL:
			floatValue = getFloat(columnIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Float(floatValue);
			}
		case Types.FLOAT:
		case Types.DOUBLE:
			doubleValue = getDouble(columnIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Double(doubleValue);
			}
		case Types.DECIMAL:
		case Types.NUMERIC:
			return getBigDecimal(columnIndex);
		case Types.BIT:
			booleanValue = getBoolean(columnIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Boolean(booleanValue);
			}
		case Types.CHAR:
			if (sqltype == InterfaceResultSet.SQLTYPECODE_BOOLEAN) {
				boolean b = getBoolean(columnIndex);
				if (wasNull_) {
					return null;
				}
				return b;
			}
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.BLOB:
		case Types.CLOB:
			return getString(columnIndex);
		case Types.BINARY:
		case Types.VARBINARY:
			return getBytes(columnIndex);
		case Types.LONGVARBINARY:
			return getBinaryStream(columnIndex);
		case Types.DATE:
			return getDate(columnIndex);
		case Types.TIME:
			if (precision > 0)
				return getString(columnIndex);

			return getTime(columnIndex);
		case Types.TIMESTAMP:
			return getTimestamp(columnIndex);
			// For LOB Support - SB 10/8/2004


		case Types.OTHER:
			return getString(columnIndex);
		default:
            throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type", JDBCType.valueOf(dataType));
		}
	}

	// JDK 1.2
	public Object getObject(int columnIndex, Map map) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, map);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, map);
        }
		validateGetInvocation(columnIndex);
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "getObject()");
		return null;
	}

	public Object getObject(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getObject(columnIndex);
	}

	// JDK 1.2
	public Object getObject(String columnName, Map map) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, map);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, map);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getObject(columnIndex, map);
	}

	// JDK 1.2
	public Ref getRef(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "getRef()");
		return null;
	}

	// JDK 1.2
	public Ref getRef(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getRef(columnIndex);
	}

	public int getRow() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		clearWarnings();
		throwIfClose();
		if (isBeforeFirst_ || isAfterLast_ || onInsertRow_) {
			return 0;
		}

		if ((getType() == ResultSet.TYPE_FORWARD_ONLY) && (getConcurrency() == ResultSet.CONCUR_UPDATABLE)) {
			return currentRowCount_;
		}
		return currentRow_;
	}

	public short getShort(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		short retValue;

		validateGetInvocation(columnIndex);
		outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());

		String data = getLocalString(columnIndex);
		if (data != null && data.trim().length() != 0) {
			try {
				retValue = Short.parseShort(data);
			} catch (NumberFormatException e) {
			    double d;
				try {
					d = new Double(data).doubleValue();
				} catch (NumberFormatException e1) {
					throw TrafT4Messages.createSQLException(getT4props(),
							"invalid_cast_specification");
				}

				if (d > (double) Short.MIN_VALUE - 1 && d < (double) Short.MAX_VALUE + 1) {
					retValue = (short) d;
					if ((double) retValue != d)
					{
						setSQLWarning(null, "data_truncation", null);
					}
				} else {
                    throw TrafT4Messages.createSQLException(getT4props(), "numeric_out_of_range",
                            d);
				}
			}

		} else {
			retValue = 0;
		}

		return retValue;
	}

	public short getShort(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getShort(columnIndex);
	}

	public Statement getStatement() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
        if (getT4props().isAllowMultiQueries()) {
            return stmt_.multiQueriesStmt;
        } else {
            return stmt_;
        }
	}

	public String getString(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		String data;

		validateGetInvocation(columnIndex);
		ObjectArray currentRow = getCurrentRow();
		Object x = currentRow.getUpdatedArrayElement(columnIndex);

		if (x == null) {
			wasNull_ = true;
			return null;
		}

		wasNull_ = false;
		int targetSqlType = getDataType(columnIndex);
		int precision = getSqlPrecision(columnIndex);
		int sqltype = getSQLDataType(columnIndex);
		switch (targetSqlType) {

		case Types.CHAR:
			if (sqltype == InterfaceResultSet.SQLTYPECODE_BOOLEAN) {
				return String.valueOf(getBoolean(columnIndex));
			}
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			data = getLocalString(columnIndex);
			if (stmt_ != null && stmt_.maxFieldSize_ != 0) {
				if (data.length() > stmt_.maxFieldSize_) {
					data = data.substring(0, stmt_.maxFieldSize_);
				}
			}
			break;
        case Types.BLOB:
            data = getLobHandle(columnIndex);
            if ( !getT4props().getUseLobHandle() && data != null) {
                Blob blob = new TrafT4Blob(stmt_, data, outputDesc_[columnIndex - 1]);
                data = new String((blob.getBytes(1, (int) blob.length())));
            }
            break;
        case Types.CLOB:
            data = getLobHandle(columnIndex);
            if ( !getT4props().getUseLobHandle() && data != null) {
                Clob clob = new TrafT4Clob(stmt_, data, outputDesc_[columnIndex - 1]);
                data = clob.getSubString(1, (int)clob.length());
            }
            break;
		case Types.VARBINARY:
		case Types.BINARY:
		case Types.LONGVARBINARY:
			data = String.valueOf(getBytes(columnIndex));
			break;
		case Types.TIMESTAMP:
			Timestamp t = getTimestamp(columnIndex);
			data = "" + t.getNanos();
			int l = data.length();
			data = t.toString();
			
			if(precision > 0) {
				for(int i=0;i<precision-l;i++)
					data += '0';
			} else {
				data = data.substring(0,data.lastIndexOf('.'));
			}

			break;
		case Types.TIME:
			if (precision > 0)
				data = x.toString();
			else
				data = String.valueOf(getTime(columnIndex));
			break;
		case Types.DATE:
			data = String.valueOf(getDate(columnIndex));
			break;
		case Types.BOOLEAN:
			data = String.valueOf(getBoolean(columnIndex));
			break;
		case Types.SMALLINT:
			data = String.valueOf(getShort(columnIndex));
			break;
		case Types.TINYINT:
			if (sqltype == InterfaceResultSet.SQLTYPECODE_TINYINT_UNSIGNED) {
				data = String.valueOf(getShort(columnIndex));
			} else {
				data = String.valueOf(getByte(columnIndex));
			}
			break;
		case Types.REAL:
			data = String.valueOf(getFloat(columnIndex));
			break;
		case Types.DOUBLE:
		case Types.FLOAT:
			data = String.valueOf(getDouble(columnIndex));
			break;
		case Types.DECIMAL:
		case Types.NUMERIC:
	        BigDecimal bd = getBigDecimal(columnIndex);
	        if (_javaVersion >= 1.5) {
	            // as of Java 5.0 and above, BigDecimal.toPlainString() should be used.
	            data = bd.toPlainString();
	        } else {
	        	data = bd.toString();
	        }			
			break;
		case Types.BIGINT:
			if (sqltype == InterfaceResultSet.SQLTYPECODE_LARGEINT_UNSIGNED) {
                if (_javaVersion >= 1.5) {
                    // as of Java 5.0 and above, BigDecimal.toPlainString() should be used.
                    data = getBigDecimal(columnIndex).toPlainString();
                } else {
                    data = String.valueOf(getBigDecimal(columnIndex));
                }
            } else {
				data = String.valueOf(getLong(columnIndex));
			}
			break;
		case Types.INTEGER:
			data = String.valueOf(getInt(columnIndex));
			break;
		case Types.OTHER: {
			if (x instanceof byte[]) {
				try {
					data = new String((byte[]) x, "ASCII");
				} catch (Exception e) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "unsupported_encoding", "ASCII");
				}
			} else {
				data = x.toString();
			}
			// only 2 supported today
			// 1. SQLTYPECODE_INTERVAL
			// 2. SQLTYPECODE_DATETIME
			// Within DATETIME we check for only the SQL/MP specific data types
			// in another switch-case statement
			switch (getFSDataType(columnIndex)) {
			case InterfaceResultSet.SQLTYPECODE_INTERVAL: {
				// if data does no start with a hyphen (representing a negative
				// sign)
				// then send back data without the byte that holds the hyphen
				// Reason: for Interval data types first byte is holding either
				// the
				// a negative sign or if number is positive, it is just an extra
				// space
				data = Utility.trimRightZeros(data);
				if (!data.startsWith(hyphen_string)) {
					data = data.substring(1);
				}
			}
				break;
			case InterfaceResultSet.SQLTYPECODE_DATETIME: {
				switch (getSqlDatetimeCode(columnIndex)) {
				case TrafT4Desc.SQLDTCODE_YEAR:
				case TrafT4Desc.SQLDTCODE_YEAR_TO_MONTH:
				case TrafT4Desc.SQLDTCODE_MONTH:
				case TrafT4Desc.SQLDTCODE_MONTH_TO_DAY:
				case TrafT4Desc.SQLDTCODE_DAY:
				case TrafT4Desc.SQLDTCODE_HOUR:
				case TrafT4Desc.SQLDTCODE_HOUR_TO_MINUTE:
				case TrafT4Desc.SQLDTCODE_MINUTE:
				case TrafT4Desc.SQLDTCODE_MINUTE_TO_SECOND:
					// case TrafT4Desc.SQLDTCODE_MINUTE_TO_FRACTION:
				case TrafT4Desc.SQLDTCODE_SECOND:
					// case TrafT4Desc.SQLDTCODE_SECOND_TO_FRACTION:
				case TrafT4Desc.SQLDTCODE_YEAR_TO_HOUR:
				case TrafT4Desc.SQLDTCODE_YEAR_TO_MINUTE:
				case TrafT4Desc.SQLDTCODE_MONTH_TO_HOUR:
				case TrafT4Desc.SQLDTCODE_MONTH_TO_MINUTE:
				case TrafT4Desc.SQLDTCODE_MONTH_TO_SECOND:
					// case TrafT4Desc.SQLDTCODE_MONTH_TO_FRACTION:
				case TrafT4Desc.SQLDTCODE_DAY_TO_HOUR:
				case TrafT4Desc.SQLDTCODE_DAY_TO_MINUTE:
				case TrafT4Desc.SQLDTCODE_DAY_TO_SECOND:
					// case TrafT4Desc.SQLDTCODE_DAY_TO_FRACTION:
				case TrafT4Desc.SQLDTCODE_HOUR_TO_FRACTION:
					break;
				default:
                    throw TrafT4Messages.createSQLException(getT4props(), "object_type_not_supported");
				}
			}
				break;
			default:
                    throw TrafT4Messages.createSQLException(getT4props(), "object_type_not_supported");
			}
		}
			break;
		case Types.ARRAY:
		case Types.BIT:
		case Types.REF:
		case Types.DATALINK:
		case Types.DISTINCT:
		case Types.JAVA_OBJECT:
		case Types.STRUCT:
		default:
			throw TrafT4Messages.createSQLException(getT4props(), "object_type_not_supported");
		}
		return data;
	}

	public String getString(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getString(columnIndex);
	}

    public Time getTime(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
        validateGetInvocation(columnIndex);
        int dataType = getDataType(columnIndex);
        if (dataType != Types.CHAR && dataType != Types.VARCHAR && dataType != Types.LONGVARCHAR
                && dataType != Types.TIME && dataType != Types.TIMESTAMP) {
            throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                    JDBCType.valueOf(dataType));
        }
        Time retValue = null;
        Timestamp timestamp;
        String data = getLocalString(columnIndex);
        if (data != null && data.trim().length() != 0) {
            switch (dataType) {
                case Types.TIMESTAMP:
                    try {
                        timestamp = Timestamp.valueOf(data);
                        retValue = new Time(timestamp.getTime());
                    } catch (IllegalArgumentException e) {
                        throw TrafT4Messages
                                .createSQLException(getT4props(), "invalid_cast_specification");
                    }
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.BLOB:
                case Types.CLOB:
                    data = data.trim(); // Fall Thru
                case Types.TIME:
                    try {
                        if (getSqlPrecision(columnIndex) > 0) {
                            retValue = Time.valueOf(data.substring(0, data.indexOf(".")));
                        } else {
                            retValue = Time.valueOf(data);
                        }
                    } catch (IllegalArgumentException e) {
                        throw TrafT4Messages
                                .createSQLException(getT4props(), "invalid_cast_specification");
                    }
                    break;
                default:
                    throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                            JDBCType.valueOf(dataType));
            }
            return retValue;
        } else {
            return null;
        }
    }

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, cal);
        }
		Time sqlTime = getTime(columnIndex);
		if (sqlTime != null) {
			if (cal != null) {
				cal.setTime(sqlTime);
				sqlTime = new Time(cal.getTimeInMillis());
			}
			return sqlTime;
		} else {
			return (sqlTime);
		}
	}

	public Time getTime(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getTime(columnIndex);
	}

	public Time getTime(String columnName, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, cal);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getTime(columnIndex, cal);
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		int dataType;
		String data;
		Timestamp retValue;
		Date dateValue;
		Time timeValue;

		validateGetInvocation(columnIndex);
		dataType = getDataType(columnIndex);
		if (dataType != Types.CHAR && dataType != Types.VARCHAR && dataType != Types.LONGVARCHAR
				&& dataType != Types.DATE && dataType != Types.TIME && dataType != Types.TIMESTAMP) {
			throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type", JDBCType.valueOf(dataType));
		}
		data = getLocalString(columnIndex);
		if (data != null) {
			switch (dataType) {
			case Types.DATE:
				try {
					dateValue = Date.valueOf(data);
					retValue = new Timestamp(dateValue.getTime());
				} catch (IllegalArgumentException e) {
					throw TrafT4Messages.createSQLException(getT4props(),
							"invalid_cast_specification");
				}
				break;
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
		        case Types.BLOB:
		        case Types.CLOB:
				data = data.trim();
			case Types.TIMESTAMP:
			case Types.TIME:
				try {
					retValue = Timestamp.valueOf(data);
				} catch (IllegalArgumentException e) {
					try {
						dateValue = Date.valueOf(data);
						retValue = new Timestamp(dateValue.getTime());
					} catch (IllegalArgumentException e1) {
						try {
							int nano = 0;
							int sqlPrecision = getSqlPrecision(columnIndex);
							if (sqlPrecision > 0) {
								nano = Integer.parseInt(data.substring(data.indexOf(".") + 1));
								nano *= Math.pow(10, 9 - sqlPrecision);
								data = data.substring(0, data.indexOf("."));
							}

							timeValue = Time.valueOf(data);
							retValue = new Timestamp(timeValue.getTime());
							retValue.setNanos(nano);
						} catch (IllegalArgumentException e2) {
							throw TrafT4Messages.createSQLException(getT4props(), "invalid_cast_specification");
						}

					}
				}
				break;
			default:
				throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type", JDBCType.valueOf(dataType));
			}
			return retValue;
		} else {
			return null;
		}
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, cal);
        }
		Timestamp sqlTimestamp = getTimestamp(columnIndex);
		if (sqlTimestamp != null) {
			if (cal != null) {
			    int nanos = sqlTimestamp.getNanos();
				cal.setTime(sqlTimestamp);
				sqlTimestamp = new Timestamp(cal.getTimeInMillis());
				sqlTimestamp.setNanos(nanos);
			}
			return sqlTimestamp;
		} else {
			return (sqlTimestamp);
		}
	}

	public Timestamp getTimestamp(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getTimestamp(columnIndex);
	}

	public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, cal);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getTimestamp(columnIndex, cal);
	}

	public int getType() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
		if (stmt_ != null) {
			return stmt_.resultSetType_;
		} else {
			return ResultSet.TYPE_FORWARD_ONLY;
		}

	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);
		String data = getLocalString(columnIndex);
		if (data != null) {
			try {
                return new java.io.ByteArrayInputStream(data.getBytes(InterfaceUtilities
                        .getCharsetName(InterfaceUtilities.SQLCHARSETCODE_UNICODE)));
			} catch (java.io.UnsupportedEncodingException e) {
				Object[] messageArguments = new Object[1];
				messageArguments[0] = e.getMessage();
				throw TrafT4Messages.createSQLException(getT4props(),
						"unsupported_encoding", messageArguments);
			}
		} else {
			return null;
		}

	}

	public InputStream getUnicodeStream(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getUnicodeStream(columnIndex);
	}

	public URL getURL(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		validateGetInvocation(columnIndex);
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "getURL()");
		return null;
	}

	public URL getURL(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getURL(columnIndex);
	}

	private byte[] getRawBytes(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
		TrafT4Desc desc;
		byte[] ret;

		if (!keepRawBuffer_) // if you dont set the property, we will not
			// support the call
			TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "getRawBytes()");

		validateGetInvocation(columnIndex); // verify columnIndex and that we
		// are not closed

		desc = outputDesc_[columnIndex - 1];
		int rowOffset = (currentRow_ - 1) * desc.rowLength_;

        if (desc.nullValue_ != -1 && Bytes.extractShort(rawBuffer_, desc.nullValue_ + rowOffset,
                this.stmt_.connection_.ic_.getByteSwap()) == -1) {
            ret = null;
        } else {
            boolean shortLength = desc.maxLen_ <= Short.MAX_VALUE;
            int dataOffset = ((shortLength) ? InterfaceUtilities.SHORT_BYTE_LEN : InterfaceUtilities.INT_BYTE_LEN);
            int maxLen = (desc.sqlDataType_ != InterfaceResultSet.SQLTYPECODE_VARCHAR_WITH_LENGTH
                    && desc.sqlDataType_ != InterfaceResultSet.SQLTYPECODE_BLOB
                    && desc.sqlDataType_ != InterfaceResultSet.SQLTYPECODE_CLOB) ? desc.maxLen_
                            : desc.maxLen_ + dataOffset;
            ret = new byte[maxLen];
            System.arraycopy(rawBuffer_, desc.noNullValue_ + rowOffset, ret, 0, maxLen);
        }
		return ret;
	}

	// ------------------------------------------------------------------
	/**
	 * This method will get the next available set of rows in rowwise rowset
	 * format.
	 * 
	 * @exception A
	 *                SQLException is thrown
	 * 
	 * @return A byte buffer containing the rows. Null is returned there are no
	 *         more rows.
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	private byte[] getNextFetchBuffer() throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		boolean done = false;
		byte[] retValue = null;

		keepRawBuffer_ = true;
		while (fetchComplete_ == false && done == false)
			done = next();
		fetchComplete_ = false;

		if (done == false)
			retValue = null;
		else
			retValue = rawBuffer_;

		return retValue;
	}

	// ------------------------------------------------------------------
    public void insertRow() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();
        throwIfConcurReadOnly();
        throwIfNotOnInsertRow();

        prepareInsertStmt();
        ObjectArray currentRow = getCurrentRow();
        int paramIndex = 1;
        for (int i = 1; i <= getNumOfColumns(); i++) {
            if (paramCols_.get(i - 1)) {
                Object object = currentRow.getUpdatedArrayElement(i);
                if (object instanceof byte[]) {
                    insertStmt_.setObject(paramIndex++, object);
                } else {
                    insertStmt_.setObject(paramIndex++, getLocalString(i, false));
                }
            }
        }

        try{
            insertStmt_.execute();
            currentRow.setUpdated();
        }catch (SQLException e){
            throw e;
        }
        // oracle does not insert the row to the resultset
        // set as updated for method rowUpdated() and update cache
        // currentRow.setUpdated();
        // int pos;
        // if (isBeforeFirst_ || isAfterLast_) {
        // pos = currentRow_;
        // } else {
        // pos = currentRow_ - 1;
        // }
        // cachedRows_.add(pos, currentRow);
        // numRows_++;
    }

	public boolean isAfterLast() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
		return isAfterLast_;
	}

    public boolean isBeforeFirst() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();
        if (numRows_ > 0 && currentRow_ == 0) {
            isBeforeFirst_ = true;
        }else {
            isBeforeFirst_ = false;
        }
        return isBeforeFirst_;
    }

    public boolean isFirst() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
		if ((getType() == ResultSet.TYPE_FORWARD_ONLY) && (getConcurrency() == ResultSet.CONCUR_UPDATABLE)) {
			if (!onInsertRow_ && currentRowCount_ == 1) {
				return true;
			} else {
				return false;
			}
		} else {
			if (!onInsertRow_ && currentRow_ == 1) {
				return true;
			} else {
				return false;
			}
		}
	}

	public boolean isLast() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		throwIfClose();
		/*
		 */
        boolean isLast = false;
        if (onInsertRow_) {
            return isLast;
        }
        if (endOfData_) {
            if (currentRow_ == numRows_) {
                // return true;
                isLast = (isAfterLast_ ? false : true);
                return isLast;
            }
            return isLast;
        }

        boolean found = next();
        if (found) {
            // previous();
            --currentRow_;
        } else {
            isAfterLast_ = false;
        }

        if (numRows_ == 0) {
            isLast = false;
        } else {
            isLast = !found;
        }

        return (isLast);

    }

	public boolean last() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		clearWarnings();
		throwIfClose();
		throwIfForwardOnly();
		throwIfOnInsertRow();

		onInsertRow_ = false;
		if (endOfData_) {
			currentRow_ = numRows_;
			isBeforeFirst_ = false;
			isAfterLast_ = false;
		} else {
			while (next()) {
				;
			}
		}
		if (currentRow_ != 0) {
			isAfterLast_ = false;
			return true;
		} else {
			return false;
		}
	}

	public void moveToCurrentRow() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		clearWarnings();
		throwIfClose();
		throwIfConcurReadOnly();

		if (!onInsertRow_) {
			return;
		} else {
			currentRow_ = savedCurrentRow_;
			onInsertRow_ = false;
			return;
		}
	}

    public void moveToInsertRow() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();
        throwIfConcurReadOnly();

        if (insertRow_ == null) {
            if (outputDesc_.length > 0) {
                insertRow_ = new ObjectArray(outputDesc_.length);
            }
        }
        if (insertRow_ != null) {
            onInsertRow_ = true;
            savedCurrentRow_ = currentRow_;
            insertRow_.resetElementUpdated();
        }
    }

	public boolean next() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		boolean validRow = false;

		int maxRowCnt;
		int maxRows;
		int queryTimeout;

		clearWarnings();
		throwIfClose();
		throwIfOnInsertRow();

		onInsertRow_ = false;
		if (currentRow_ < numRows_) {
			validRow = true;
			currentRow_++;
			isBeforeFirst_ = false;
		} else {
			if (endOfData_) {
				isAfterLast_ = true;
				isBeforeFirst_ = false;
			} else {
				if (stmt_ != null) {
					maxRows = stmt_.maxRows_;
					queryTimeout = stmt_.queryTimeout_;
				} else {
					maxRows = 0;
					queryTimeout = 0;
				}
				setFetchSizeIfExceedLimit();
				if (maxRows == 0 || maxRows > totalRowsFetched_ + fetchSize_) {
					maxRowCnt = fetchSize_;
				} else {
					maxRowCnt = maxRows - totalRowsFetched_;
				}

				if (maxRowCnt <= 0) {
					validRow = false;
				} else {
					try {
						validRow = irs_.fetch(stmtLabel_, maxRowCnt, queryTimeout, holdability_, this);
						fetchComplete_ = true;
					} catch (SQLException e) {
						performConnectionErrorChecks(e);
						throw e;
					}
				}
				if (validRow) {
					currentRow_++;
					isAfterLast_ = false;
					isBeforeFirst_ = false;
				} else {
					// In some cases endOfData_ is reached when fetch returns
					// false;
					endOfData_ = true;
					isAfterLast_ = true;
					isBeforeFirst_ = false;
				}
			}
		}
		if (validRow) {
			currentRowCount_++;
		} else {
			currentRowCount_ = 0;
		}

		return validRow;
	}

	public boolean previous() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		boolean validRow = false;
		clearWarnings();
		throwIfClose();
		throwIfForwardOnly();
		throwIfOnInsertRow();

		onInsertRow_ = false;
		if (currentRow_ > 1) {
			// --currentRow_;
			currentRow_ = isAfterLast_ ? currentRow_ : --currentRow_;
			validRow = true;
			isBeforeFirst_ = false;
			isAfterLast_ = false;
		} else {
			currentRow_ = 0;
			isBeforeFirst_ = true;
			isAfterLast_ = false;
		}
		return validRow;
        }

    public void refreshRow() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();
        throwIfOnInsertRow();
        throwIfForwardOnly();

        prepareSelectStmt();
        ObjectArray currentRow = getCurrentRow();
        currentRow.resetElementUpdated();
        int paramIndex = 1;

        for (int i = 1; i <= getNumOfColumns(); i++) {
            if (keyCols_.get(i - 1)) {
                selectStmt_.setObject(paramIndex++, currentRow.getUpdatedArrayElement(i));
            }
        }
        ResultSet rs = selectStmt_.executeQuery();
        if (rs == null) {
            return;
        }
        try {
            rs.next();
            paramIndex = 1;
            for (int i = 1; i <= getNumOfColumns(); i++) {
                if (paramCols_.get(i - 1)) {
                    currentRow.refreshArrayElement(i, rs.getObject(paramIndex++));
                }
            }
        } catch (SQLException e) {
            performConnectionErrorChecks(e);
            throw e;
        } finally {
            rs.close();
        }
    }

	public boolean relative(int row) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", row);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", row);
        }
		int absRow;
		int rowInc;
		boolean flag = false;

		clearWarnings();
		throwIfClose();
		throwIfForwardOnly();
		throwIfOnInsertRow();


		onInsertRow_ = false;
		if (row > 0) {
			rowInc = 0;
			do {
				flag = next();
				if (!flag) {
					break;
				}
			} while (++rowInc < row);
		} else {
			absRow = -row;
			if (absRow < currentRow_) {
				currentRow_ -= absRow;
				isAfterLast_ = false;
				isBeforeFirst_ = false;
				flag = true;
			} else {
				beforeFirst();
			}
		}
		return flag;
	}

    public boolean rowDeleted() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();

        return rowDeleted;
    }

    public boolean rowInserted() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();

        return getCurrentRow().isUpdated();
    }

    public boolean rowUpdated() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        throwIfClose();
        return getCurrentRow().isUpdated();
    }

    public void setFetchDirection(int direction) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", direction);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", direction);
        }
        throwIfClose();
        throwIfOnInsertRow();
        switch (direction) {
            case ResultSet.FETCH_UNKNOWN:
            case ResultSet.FETCH_REVERSE:
                if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
                    throw TrafT4Messages
                            .createSQLException(getT4props(), "invalid_fetch_direction");
                }
                fetchDirection_ = direction;
                break;
            case ResultSet.FETCH_FORWARD:
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
        throwIfClose();
        throwIfOnInsertRow();
        if (rows < 0) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_fetch_size");
        } else if (rows == 0) {
            fetchSize_ = DEFAULT_FETCH_SIZE;
        } else {
            fetchSize_ = rows;
        }

        setFetchSizeIfExceedLimit();
    }

    /**
     * if (row width) * (fetch rows) too large, there will have core in server side. once fetch
     * bytes bigger than 1GB, divide it into several times to fetch, each time fetch bytes less than
     * 1GB.
     */
    private void setFetchSizeIfExceedLimit() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (outputDesc_ != null && outputDesc_[0] != null) {
            long rowLength = outputDesc_[0].rowLength_;
            long fetchBytes = rowLength * fetchSize_;
            if (fetchBytes >= FETCH_BYTES_LIMIT) {
                fetchSize_ = (int) Math.ceil(FETCH_BYTES_LIMIT / (double) rowLength);
                if (getT4props().isLogEnable(Level.FINEST)) {
                    T4LoggingUtilities.log(getT4props(), Level.FINEST, "fetch size exceed limit, change it to " + fetchSize_);
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("fetch size exceed limit, change it to {}", fetchSize_);
                }
            }
        }
    }

	public void updateArray(int columnIndex, Array x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "updateArray()");
	}

	public void updateArray(String columnName, Array x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateArray(columnIndex, x);
	}

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

        validateUpdInvocation(columnIndex);

        if (x == null) {
            updateNull(columnIndex);
            return;
        }

        byte[] value = new byte[length];
        try {
            x.read(value, 0, length);
        } catch (IOException e) {
            throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
        }
        try {
            getCurrentRow().updateArrayElement(columnIndex, new String(value, "ASCII"));
        } catch (UnsupportedEncodingException e) {
            throw TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding",
                    e.getMessage());
        }
    }

    private void fillInBlank(int blank, byte[] value) {
        if (blank < 0) {
            return;
        }
        for (int i = blank; i < value.length; i++) {
            value[i] = 32;
        }
    }

	public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", columnName, x, length);
        }
		int columnIndex = validateUpdInvocation(columnName);
		updateAsciiStream(columnIndex, x, length);
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, x);
	}

	public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateBigDecimal(columnIndex, x);
	}

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", columnIndex, x, length);
        }

        validateUpdInvocation(columnIndex);
        if (x == null) {
            updateNull(columnIndex);
            return;
        }
        byte[] value = new byte[length];
        try {
            x.read(value, 0, length);
        } catch (IOException e) {
            throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
        }
        ObjectArray objectArray = getCurrentRow();
        objectArray.updateArrayElement(columnIndex, value);
    }

	public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", columnName, x, length);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateBinaryStream(columnIndex, x, length);
	}


	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, new Boolean(x));
	}

	public void updateBoolean(String columnName, boolean x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateBoolean(columnIndex, x);
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, new Byte(x));
	}

	public void updateByte(String columnName, byte x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateByte(columnIndex, x);
	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, x);
	}

	public void updateBytes(String columnName, byte[] x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateBytes(columnIndex, x);
	}

    public void updateCharacterStream(int columnIndex, Reader reader, int length)
            throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, reader, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", columnIndex, reader, length);
        }
        if (reader == null) {
            updateNull(columnIndex);
            return;
        }
        validateUpdInvocation(columnIndex);
        char value[] = new char[length];
        try {
            reader.read(value, 0, length);
        } catch (IOException e) {
            throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
        }
        getCurrentRow().updateArrayElement(columnIndex, new String(value));
    }

	public void updateCharacterStream(String columnName, Reader x, int length) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateCharacterStream(columnIndex, x, length);
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, x);
	}

	public void updateDate(String columnName, Date x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateDate(columnIndex, x);
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, new Double(x));
	}

	public void updateDouble(String columnName, double x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateDouble(columnIndex, x);
	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, new Float(x));
	}

	public void updateFloat(String columnName, float x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateFloat(columnIndex, x);
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, new Integer(x));
	}

	public void updateInt(String columnName, int x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateInt(columnIndex, x);
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, new Long(x));
	}

	public void updateLong(String columnName, long x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateLong(columnIndex, x);
	}

	public void updateNull(int columnIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, null);
	}

	public void updateNull(String columnName) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateNull(columnIndex);
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		updateObject(columnIndex, x, 0);
	}

	public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x, scale);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", columnIndex, x, scale);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, x);
	}

	public void updateObject(String columnName, Object x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateObject(columnIndex, x, 0);
	}

	public void updateObject(String columnName, Object x, int scale) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x, scale);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateObject(columnIndex, x, scale);
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "updateRef()");
	}

	public void updateRef(String columnName, Ref x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateRef(columnIndex, x);
	}

    public void updateRow() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

        throwIfClose();
        throwIfConcurReadOnly();

        clearWarnings();
        throwIfOnInsertRow();


        if (connection_.getAutoCommit()) {
            // should set a warning for the user
            setSQLWarning(getT4props(), "resultSet_updateRow_with_autocommit", null);
        }
        prepareUpdateStmt();
        ObjectArray currentRow = getCurrentRow();

        int paramIndex = 1;

        // update tbl set col=? where keyCol=?;
        // set col fisrt
        for (int i = 1; i <= getNumOfColumns(); i++) {
            Object object = currentRow.getUpdatedArrayElement(i);
            boolean nchar = InterfaceResultSet.SQLTYPECODE_CHAR == getSQLDataType(i)
                    && InterfaceUtilities.SQLCHARSETCODE_UNICODE == getSqlCharset(i);
            // for the situation, the column is nchar data type, when do update/insert/delete
            // there should change the raw byte array from server side to string,
            // in order to avoid data len problem
            if (object instanceof byte[] && nchar && !currentRow.isElementUpdated(i)) {
                updateStmt_.setObject(paramIndex++, getLocalString(i, false));
            } else {
                updateStmt_.setObject(paramIndex++, object);
            }
        }
        // set keyCol then
        for (int i = 1; i <= getNumOfColumns(); i++) {
            // if (paramCols.get(i))
            if (keyCols_.get(i - 1)) {
                Object object = currentRow.getUpdatedArrayElement(i);
                boolean nchar = InterfaceResultSet.SQLTYPECODE_CHAR == getSQLDataType(i)
                        && InterfaceUtilities.SQLCHARSETCODE_UNICODE == getSqlCharset(i);
                if (object instanceof byte[] && nchar) {
                    updateStmt_.setObject(paramIndex++, getLocalString(i, true));
                } else {
                    updateStmt_.setObject(paramIndex++, object);
                }
            }
        }
        int count = updateStmt_.executeUpdate();
        if (count == 0) {
            throw TrafT4Messages.createSQLException(getT4props(), "row_modified");
        }
        // set as updated for method rowUpdated() and update cache
        currentRow.setUpdated();
        cachedRows_.set(currentRow_ - 1, currentRow);
    }

	public void updateShort(int columnIndex, short x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, new Short(x));
	}

	public void updateShort(String columnName, short x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateShort(columnIndex, x);
	}

	public void updateString(int columnIndex, String x) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, x);
	}

	public void updateString(String columnName, String x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateString(columnIndex, x);
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, x);
	}

	public void updateTime(String columnName, Time x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateTime(columnIndex, x);
	}

	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

		validateUpdInvocation(columnIndex);
		getCurrentRow().updateArrayElement(columnIndex, x);
	}

	public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnName, x);
        }

		int columnIndex = validateUpdInvocation(columnName);
		updateTimestamp(columnIndex, x);
	}

	public boolean wasNull() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("");
        }

		throwIfClose();
		return wasNull_;
	}

	void setColumnName(int columnIndex, String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, columnName);
        }

		if (columnIndex < 1 || columnIndex > getNumOfColumns()) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_column_index");
		}
		outputDesc_[columnIndex - 1].name_ = columnName;
	}

	// Other methods
	void close(boolean dropStmt) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", dropStmt);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", dropStmt);
        }

		if (!isClosed_) {
			clearWarnings();
			try {
				irs_.close();
			} finally {
				isClosed_ = true;
				irs_ = null;

				if (stmt_ != null) {
					if (dropStmt) {
						for (int i = 0; i < stmt_.num_result_sets_; i++)
							stmt_.resultSet_[i] = null;
						stmt_.result_set_offset = 0;
					}
				}
			}
		}
	}

	private int validateGetInvocation(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }

		return findColumn(columnName);
	}

	private void validateGetInvocation(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }

		throwIfClose();
		if (columnIndex < 1 || columnIndex > getNumOfColumns()) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_column_index");
		}
	}

	public Connection getConnection() {
		return this.connection_;
	}

	private int validateUpdInvocation(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		if (getConcurrency() == ResultSet.CONCUR_READ_ONLY) {
            throw TrafT4Messages.createSQLException(getT4props(), "read_only_concur");
		}
		return findColumn(columnName);
	}

	private void validateUpdInvocation(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }

		if (getConcurrency() == ResultSet.CONCUR_READ_ONLY) {
            throw TrafT4Messages.createSQLException(getT4props(), "read_only_concur");
		}
		if (columnIndex < 1 || columnIndex > getNumOfColumns()) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_column_index");
		}
	}

	private ObjectArray getCurrentRow() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

		if (onInsertRow_) {
		    if (insertRow_ == null) {
		        moveToInsertRow();
            }
			return insertRow_;
		} else {
			if (isBeforeFirst_) {
				throw TrafT4Messages.createSQLException(getT4props(), "cursor_is_before_first_row");
			}
			if (isAfterLast_) {
				throw TrafT4Messages.createSQLException(getT4props(), "cursor_after_last_row");
			}
			return cachedRows_.get(currentRow_ - 1);
		}
	}

	private int getRowCount() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		return numRows_;
	}

	private void getKeyColumns() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		String colName;
		int index = 0;
		int rowCount = 0;
		int colNo;
		int columnCount;
		StringBuffer insertColsStr;
		StringBuffer insertValueStr;
		StringBuffer updateStr;
		StringBuffer whereClause;
		StringBuffer deleteWhereClause;
		StringBuffer selectWhereClause;
		StringBuffer selectClause;
		String keyCatalogNm;
		String keySchemaNm;
		String keyTableNm;
		ArrayList<String> keyColumns = null;
		String columnName;

		if (noKeyFound_) {
			throw TrafT4Messages.createSQLException(getT4props(), "no_primary_key");
		}

		try {
			DatabaseMetaData dbmd = connection_.getMetaData();
			ResultSetMetaData rsmd = getMetaData();

			// The table of the first column in the select list is being treated
			// as the table to be updated. Columns from the other tables are
			// ignored
			columnCount = rsmd.getColumnCount();
			keyCatalogNm = rsmd.getCatalogName(1);
			keySchemaNm = rsmd.getSchemaName(1);
			keyTableNm = rsmd.getTableName(1);

			if (this.stmt_ instanceof TrafT4PreparedStatement) {
				keyColumns = ((TrafT4PreparedStatement) this.stmt_).getKeyColumns();
			}

			if (keyColumns == null) {
				ResultSet rs = dbmd.getPrimaryKeys(keyCatalogNm, keySchemaNm, keyTableNm);

				keyColumns = new ArrayList<String>();
				while (rs.next()) {
					rowCount++;
					colName = rs.getString(4);
					keyColumns.add(index++, colName);
				}

				rowCount = ((TrafT4ResultSet) rs).getRowCount();
				if (rowCount == 0) {
					noKeyFound_ = true;
					stmt_.resultSetConcurrency_ = ResultSet.CONCUR_READ_ONLY;
                    throw TrafT4Messages.createSQLException(getT4props(), "no_primary_key");
				}

				if (this.stmt_ instanceof TrafT4PreparedStatement) {
					((TrafT4PreparedStatement) this.stmt_).setKeyColumns(keyColumns);
				}
			}

			// Commented out since rs.close() will end the transaction and hence
			// the application
			// may not be able to fetch its next row. It is ok not to close,
			// since it is an internal
			// stmt
			// rs.close();
			// Check if Key columns are there in the result Set
			keyCols_ = new BitSet(columnCount);
			for (index = 0; index < keyColumns.size(); index++) {
				for (colNo = 1; colNo <= columnCount; colNo++) {
					if (rsmd.getColumnName(colNo).equals((String) keyColumns.get(index))
							&& rsmd.getTableName(colNo).equals(keyTableNm)
							&& rsmd.getSchemaName(colNo).equals(keySchemaNm)
							&& rsmd.getCatalogName(colNo).equals(keyCatalogNm)) {
						keyCols_.set(colNo - 1);
						break;
					}
				}
				if (colNo > columnCount) {
					noKeyFound_ = true;
					stmt_.resultSetConcurrency_ = ResultSet.CONCUR_READ_ONLY;
                    throw TrafT4Messages.createSQLException(getT4props(), "no_primary_key");
				}
			}
			// Create a Update, Insert, Delete and Select statements
			// Select statement where clause has only primary keys
			// Update and Delete statments where clause has all columns of the
			// table being modified
			// Update statement set clause doesn't contain the primary keys,
			// since it can't be updated
			// Insert statement has all the columns of the table in the value
			// list
			paramCols_ = new BitSet(columnCount);
			whereClause = new StringBuffer(2048).append(" where ");
			deleteWhereClause = new StringBuffer(2048).append(" where ");
			insertColsStr = new StringBuffer(2048).append("(");
			insertValueStr = new StringBuffer(2048).append(" values (");
			updateStr = new StringBuffer(2048).append(" set ");
			selectWhereClause = new StringBuffer(2048).append(" where ");
			selectClause = new StringBuffer(2048).append("select ");
			for (colNo = 1; colNo < columnCount; colNo++) {
				if (rsmd.getTableName(colNo).equals(keyTableNm) && rsmd.getSchemaName(colNo).equals(keySchemaNm)
						&& rsmd.getCatalogName(colNo).equals(keyCatalogNm)) {
					paramCols_.set(colNo - 1);
					columnName = rsmd.getColumnName(colNo);
					insertColsStr = insertColsStr.append(columnName).append(", ");
					insertValueStr = insertValueStr.append("?, ");
                    if (keyCols_.get(colNo - 1)) {
                        selectWhereClause = selectWhereClause.append(columnName).append(" = ? and ");
                        whereClause = whereClause.append(columnName).append(" = ? and ");
                    }
					updateStr = updateStr.append(columnName).append(" = ?, ");
					deleteWhereClause = deleteWhereClause.append(columnName).append(" = ? and ");
					selectClause = selectClause.append(columnName).append(", ");
				}
			}
			paramCols_.set(colNo - 1);
			columnName = rsmd.getColumnName(colNo);
			insertColsStr = insertColsStr.append(columnName).append(")");
			insertValueStr = insertValueStr.append("?)");
			if (!keyCols_.get(colNo - 1)) { // do not include key columns

				// We do not want non-key columns in the where clause, but we
				// added an extra
				// " and " above
				int selectWhereClause_len = selectWhereClause.length();
				selectWhereClause.delete(selectWhereClause_len - 5, selectWhereClause_len);

				int whereClause_len = whereClause.length();
				whereClause.delete(whereClause_len - 5, whereClause_len);
			} else {
				selectWhereClause = selectWhereClause.append(columnName).append(" = ? ");
				whereClause = whereClause.append(columnName).append(" = ? ");
			}
			updateStr = updateStr.append(columnName).append(" = ? ");
			deleteWhereClause = deleteWhereClause.append(columnName).append(" = ? ");
			selectClause = selectClause.append(columnName).append(" from ");
			selectCmd_ = new StringBuffer(2048).append(selectClause).append(keyCatalogNm).append(".").append(
					keySchemaNm).append(".").append(keyTableNm).append(selectWhereClause);
			deleteCmd_ = new StringBuffer(2048).append("delete from ").append(keyCatalogNm).append(".").append(
					keySchemaNm).append(".").append(keyTableNm).append(deleteWhereClause);
			insertCmd_ = new StringBuffer(2048).append("insert into ").append(keyCatalogNm).append(".").append(
					keySchemaNm).append(".").append(keyTableNm).append(insertColsStr).append(insertValueStr);
			updateCmd_ = new StringBuffer(2048).append("update ").append(keyCatalogNm).append(".").append(keySchemaNm)
					.append(".").append(keyTableNm).append(updateStr).append(whereClause);
		} catch (SQLException e) {
			performConnectionErrorChecks(e);
			throw e;
		}
	}

	void prepareDeleteStmt() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (deleteStmt_ == null) {
			if (deleteCmd_ == null) {
				getKeyColumns();
			}
			deleteStmt_ = connection_.prepareStatement(deleteCmd_.toString());
		}
	}

	void prepareInsertStmt() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (insertStmt_ == null) {
			if (insertCmd_ == null) {
				getKeyColumns();
			}
			insertStmt_ = connection_.prepareStatement(insertCmd_.toString());
		}
	}

	void prepareUpdateStmt() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (updateStmt_ == null) {
			if (updateCmd_ == null) {
				getKeyColumns();
			}
			updateStmt_ = connection_.prepareStatement(updateCmd_.toString());
		}
	}

	void prepareSelectStmt() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (selectStmt_ == null) {
			if (selectCmd_ == null) {
				getKeyColumns();
			}
			selectStmt_ = connection_.prepareStatement(selectCmd_.toString());
		}
	}


	// This method is called from the JNI method Fetch for user given SQL
	// statements. For
	// DatabaseMetaData catalog APIs also call this method to update the rows
	// fetched
	//
	void setFetchOutputs(ObjectArray[] row, int rowsFetched, boolean endOfData) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", row, rowsFetched, endOfData);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", row, rowsFetched, endOfData);
        }

		// numRows_ contain the number of rows in the ArrayList. In case of
		// TYPE_FORWARD_ONLY,
		// the numRows_ is reset to 0 whenever this method is called.
		// totalRowsFetched_ contain the total number of rows fetched

		if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
			cachedRows_.clear();
			numRows_ = 0;
			currentRow_ = 0;
		}
		for (int i = 0; i < row.length; i++) {
			cachedRows_.add(row[i]);
			// add to the totalRowsFetched
		}
		totalRowsFetched_ += rowsFetched;
		numRows_ += rowsFetched;
		endOfData_ = endOfData;

        // isBeforeFirst_ = false;
	}

	// Method used by JNI layer to set the Data Truncation warning
	void setDataTruncation(int index, boolean parameter, boolean read, int dataSize, int transferSize) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", index, parameter, read, dataSize, transferSize);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}", index, parameter, read, dataSize, transferSize);
        }

		DataTruncation dtLeaf = new DataTruncation(index, parameter, read, dataSize, transferSize);
		if (sqlWarning_ == null) {
			sqlWarning_ = dtLeaf;
		} else {
			sqlWarning_.setNextWarning(dtLeaf);
		}
	}

    int getFSDataType(int columnIndex) {
        return outputDesc_[columnIndex - 1].fsDataType_;
    }

    int getSQLDataType(int columnIndex) { // 0 -based
        return outputDesc_[columnIndex - 1].sqlDataType_;
    }

    int getPrecision(int columnIndex) {
        return outputDesc_[columnIndex - 1].precision_;
    }

    int getSqlPrecision(int columnIndex) {
        return outputDesc_[columnIndex - 1].sqlPrecision_;
    }

    int getDataType(int columnIndex) {
        return outputDesc_[columnIndex - 1].dataType_;
    }

    int getScale(int columnIndex) {
        return outputDesc_[columnIndex - 1].scale_;
    }

    int getSqlDatetimeCode(int columnIndex) {
        return outputDesc_[columnIndex - 1].sqlDatetimeCode_;
    }

    int getSqlCharset(int columnIndex) {
        return outputDesc_[columnIndex - 1].sqlCharset_;
    }

    int getNumOfColumns() {
        return outputDesc_.length;
    }

    int getSqlOctetLength(int columnIndex) {
        return outputDesc_[columnIndex - 1].sqlOctetLength_;
    }

    boolean isSigned(int columnIndex) {
        return outputDesc_[columnIndex - 1].isSigned_;
    }

    String getName(int columnIndex) {
        return outputDesc_[columnIndex - 1].name_;
    }

    // true for getRawArrayElement
    // false for getUpdatedArrayElement
    private String getLocalString(int columnIndex, boolean rawOrUpdate) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, rawOrUpdate);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, rawOrUpdate);
        }
        Object obj = null;
        String data = null;
        int clientCharset = this.irs_.ic_.getTerminalCharset();
        int sqlCharset = getSqlCharset(columnIndex) == 0 ? InterfaceUtilities.SQLCHARSETCODE_UTF8
                : getSqlCharset(columnIndex);

        sqlCharset = clientCharset == 0 ? sqlCharset : clientCharset;
        
        wasNull_ = true;
        ObjectArray row = getCurrentRow();
        if (row != null) {
            if (rawOrUpdate) {
                obj = row.getRawArrayElement(columnIndex);
            } else {
                obj = row.getUpdatedArrayElement(columnIndex);
            }
            if (obj != null) {
                if (obj instanceof byte[]) {
                    try {
                        if (getISOMapping() == InterfaceUtilities.SQLCHARSETCODE_ISO88591
                                && !getEnforceISO()
                                && sqlCharset == InterfaceUtilities.SQLCHARSETCODE_ISO88591) {
                            data = new String((byte[]) obj, getISO88591());
                        } else {
                            if (getSQLDataType(columnIndex) == InterfaceResultSet.SQLTYPECODE_INTERVAL ||
                                getSQLDataType(columnIndex) == InterfaceResultSet.SQLTYPECODE_CLOB) {
                                // here is for resultset update for INTERVAL column
                                data = new String((byte[]) obj, "ASCII");
                            } else {
                                data = this.irs_.ic_.decodeBytes((byte[]) obj, sqlCharset);
                            }
                        }
                        // M-9409 if charset is utf8, it will occupy 4 bytes for one char,
                        // use sqlprecision to clip the length
                        int targetSqlType = getDataType(columnIndex);
                        if (sqlCharset == InterfaceUtilities.SQLCHARSETCODE_UTF8
                                && targetSqlType == Types.CHAR) {
                            int sqlPrecision = getSqlPrecision(columnIndex);
                            int precision = getPrecision(columnIndex);
                            // utf8 with char
                            if (sqlPrecision > 0) {
                                data = data.substring(0, sqlPrecision);
                            } else {
                                // utf8 with char(n byte)
                                int shortOne = Math.min(precision, data.length());
                                data = data.substring(0, shortOne);
                            }
                        }
                        wasNull_ = false;
                    } catch (CharacterCodingException e) {
                        try{
                            data = new String((byte[]) obj);
                        } catch (UnsupportedCharsetException ue) {
                            SQLException se = TrafT4Messages.createSQLException(getT4props(),
                                    "unsupported_encoding", ue.getCharsetName());
                            se.initCause(ue);
                            throw se;
                        }
                        Object[] messageArguments = new Object[2];
                        messageArguments[0] = new String("getLocalString");
                        messageArguments[1] = new String("the charset of data does not match the charset of the column");
                        setSQLWarning(null, "translation_of_parameter_failed", messageArguments);
                    } catch (UnsupportedCharsetException e) {
                        SQLException se = TrafT4Messages.createSQLException(getT4props(),
                                "unsupported_encoding", e.getCharsetName());
                        se.initCause(e);
                        throw se;
                    } catch (UnsupportedEncodingException e) {
                        SQLException se = TrafT4Messages.createSQLException(getT4props(),
                                "unsupported_encoding", e.getMessage());
                        se.initCause(e);
                        throw se;
                    }
                } else {
                    data = obj.toString();
                    wasNull_ = false;
                }
            }
        }
        return data;
    }

    private String getLocalString(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
        return getLocalString(columnIndex, false);
    }

	public String getProxySyntax() {
		return proxySyntax_;
	}

	public boolean isClosed() throws SQLException {
		return (isClosed_ || connection_.isClosed());
	}

	public boolean hasLOBColumns() {
		for (int i = 1; i <= getNumOfColumns(); i++) {
		    int dataType = getDataType(i);
			if (dataType == Types.CLOB || dataType == Types.BLOB) {
				return true;
			}
		}
		return false;
	}

	public long getSequenceNumber() {
		return seqNum_;
	}

	public boolean useOldDateFormat() {
		return useOldDateFormat_;
	}

	void closeErroredConnection(TrafT4Exception sme) {
		connection_.closeErroredConnection(sme);
	}

	// Constructors - used in TrafT4Statement
	TrafT4ResultSet(TrafT4Statement stmt, TrafT4Desc[] outputDesc) throws SQLException {
		this(stmt, outputDesc, stmt.stmtLabel_, false);
        if (stmt.getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(stmt.getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
	}

	// Constructors - used for SPJ ResultSets
	TrafT4ResultSet(TrafT4Statement stmt, TrafT4Desc[] outputDesc, String stmt_label, boolean spj_result_set)
			throws SQLException {
        if (stmt.getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(stmt.getT4props(), Level.FINEST, "ENTRY", stmt, stmt_label, spj_result_set);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", stmt, stmt_label, spj_result_set);
        }
		stmt_ = stmt;
		outputDesc_ = outputDesc;
		// update all the resultSet fields from stmt
		connection_ = stmt.connection_;
		keepRawBuffer_ = getT4props().getKeepRawFetchBuffer();
		fetchSize_ = stmt.fetchSize_;
		fetchDirection_ = stmt.fetchDirection_;
		// Don't use the statement label from the statement object
		// Instead, use it from the one provided (added for SPJ RS support) - SB
		// 11/21/2005
		stmtLabel_ = stmt_label;
		cachedRows_ = new ArrayList<ObjectArray>();
		isBeforeFirst_ = true;
		holdability_ = stmt.resultSetHoldability_;
		spj_rs_ = spj_result_set;
		fetchComplete_ = false;
		
		seqNum_ = seqCount_++;
		try {
			irs_ = new InterfaceResultSet(this);
		} catch (SQLException e) {
			performConnectionErrorChecks(e);
			throw e;
		}
	}

	// Constructor - used in T4DatabaseMetaData
	// ResultSet is not created on Java side in
	// T4DatanaseMetaData due to threading issues
	TrafT4ResultSet(T4DatabaseMetaData dbMetaData, TrafT4Desc[] outputDesc, String stmtLabel, boolean oldDateFormat)
			throws SQLException {
        if (dbMetaData.getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(dbMetaData.getT4props(), Level.FINEST, "ENTRY", dbMetaData, outputDesc, stmtLabel, oldDateFormat);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", dbMetaData, outputDesc, stmtLabel, oldDateFormat);
        }
        connection_ = dbMetaData.connection_;
		outputDesc_ = outputDesc;
		keepRawBuffer_ = getT4props().getKeepRawFetchBuffer();
		stmtLabel_ = stmtLabel;
		fetchSize_ = DEFAULT_FETCH_SIZE;
		fetchDirection_ = ResultSet.FETCH_FORWARD;
		cachedRows_ = new ArrayList<ObjectArray>();
		isBeforeFirst_ = true;
		holdability_ = CLOSE_CURSORS_AT_COMMIT;
		spj_rs_ = false;
		useOldDateFormat_ = oldDateFormat;
		fetchComplete_ = false;
		
		seqNum_ = seqCount_++;
		try {
			irs_ = new InterfaceResultSet(this);
		} catch (SQLException e) {
			performConnectionErrorChecks(e);
			throw e;
		}
	}
	
	// Fields
	InterfaceResultSet irs_;
	TrafT4Desc[] outputDesc_;
	TrafT4Statement stmt_;
	TrafT4Connection connection_;
	boolean isClosed_;
	int currentRow_;
	boolean endOfData_;
	// Fetch outputs updated by JNI Layer
	boolean wasNull_;
	int totalRowsFetched_;
	int fetchSize_;
	int fetchDirection_;
	String stmtLabel_;

	ArrayList<ObjectArray> cachedRows_;
	boolean onInsertRow_;
	ObjectArray insertRow_;
	int savedCurrentRow_;
	boolean showInserted_;
	int numRows_;
	boolean isAfterLast_;
	boolean isBeforeFirst_ = true;
	boolean rowDeleted = false;
	private static float _javaVersion;
	private static Method _toPlainString;

	boolean noKeyFound_;
	StringBuffer deleteCmd_;
	StringBuffer insertCmd_;
	StringBuffer updateCmd_;
	StringBuffer selectCmd_;
	PreparedStatement deleteStmt_;
	PreparedStatement insertStmt_;
	PreparedStatement updateStmt_;
	PreparedStatement selectStmt_;
	BitSet paramCols_;
	BitSet keyCols_;
	int holdability_;

	int currentRowCount_ = 0;
	static final int DEFAULT_FETCH_SIZE = 100;
	static final String hyphen_string = new String("-");

	static long seqCount_ = 0;
	long seqNum_ = 0;
	String proxySyntax_ = "";
	boolean useOldDateFormat_ = false;

	// For LOB Support - SB 10/8/2004
	boolean isAnyLob_;

	// For SPJ RS support - SB 11/21/2005
	boolean spj_rs_;

	boolean keepRawBuffer_;
	byte[] rawBuffer_;
	boolean fetchComplete_;
	HashMap<String, Integer> colMap_;

	static {
		_javaVersion = Float.parseFloat(System.getProperty("java.specification.version"));
		if (_javaVersion >= 1.5) {
			try {
				_toPlainString = java.math.BigDecimal.class.getMethod("toPlainString", (Class[]) null);
			} catch(Exception e) {}
		}
	}

	public Object unwrap(Class iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isWrapperFor(Class iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public RowId getRowId(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public RowId getRowId(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}
	/**/
    public Clob getClob(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }
        validateGetInvocation(columnIndex);
        outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());

        int dataType = getDataType(columnIndex);
        switch (dataType) {
            case Types.CLOB:
                String lobHandle = getLobHandle(columnIndex);
                if (lobHandle != null) {
                    return new TrafT4Clob(stmt_, lobHandle, outputDesc_[columnIndex - 1]);
                }
                return null;

            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }
	
	public Clob getClob(String columnName)  throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }
		int columnIndex = validateGetInvocation(columnName);
		return getClob(columnIndex);
	}
	
    public Blob getBlob(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINE)) {
            T4LoggingUtilities.log(getT4props(), Level.FINE, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }

        validateGetInvocation(columnIndex);
        outputDesc_[columnIndex - 1].checkValidNumericConversion(connection_.getLocale());

        int dataType = getDataType(columnIndex);
        switch (dataType) {
            case Types.BLOB:
                String lobHandle = getLobHandle(columnIndex);
                if (lobHandle != null) {
                    return new TrafT4Blob(stmt_, lobHandle, outputDesc_[columnIndex - 1]);
                }
                return null;
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type",
                        JDBCType.valueOf(dataType));
        }
    }

	public Blob getBlob(String columnName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnName);
        }

		int columnIndex = validateGetInvocation(columnName);

		return getBlob(columnIndex);
	}
    /**/
	public NClob getNClob(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

    public String getNString(int columnIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnIndex);
        }

        validateGetInvocation(columnIndex);
        ObjectArray currentRow = getCurrentRow();
        Object x = currentRow.getUpdatedArrayElement(columnIndex);

        if (x == null) {
            wasNull_ = true;
            return null;
        }

        wasNull_ = false;
        int dataType = getDataType(columnIndex);
        int charset = getSqlCharset(columnIndex);
        switch (dataType) {
            case Types.CHAR:
                if (charset == InterfaceUtilities.SQLCHARSETCODE_UNICODE) {
                    return getLocalString(columnIndex);
                }
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (charset == InterfaceUtilities.SQLCHARSETCODE_UNICODE) {
                    String data = getLocalString(columnIndex);
                    if (stmt_ != null && stmt_.maxFieldSize_ != 0) {
                        if (data.length() > stmt_.maxFieldSize_) {
                            data = data.substring(0, stmt_.maxFieldSize_);
                        }
                    }
                    return data;
                }
            default:
                throw TrafT4Messages.createSQLException(getT4props(), "object_type_not_supported",
                        JDBCType.valueOf(dataType));
        }
    }

    public String getNString(String columnLabel) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnLabel);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", columnLabel);
        }

        int columnIndex = validateGetInvocation(columnLabel);
        return getNString(columnIndex);
    }

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	/**/
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		// TODO Auto-generated method stub
	}
	
	public void updateBlob(String columnName, Blob x) throws SQLException {
		// TODO Auto-generated method stub
	}
	
	public void updateClob(String columnName, Clob x) throws SQLException {
		// TODO Auto-generated method stub
	}
	
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		// TODO Auto-generated method stub
	}
    /**/
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

        if (x == null) {
            updateNull(columnIndex);
            return;
        }
        int available = 0;
        try {
            available = x.available();
        } catch (IOException e) {
            throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
        }
        updateAsciiStream(columnIndex, x, available);
    }

	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnIndex, x);
        }

        if (x == null) {
            updateNull(columnIndex);
            return;
        }
        int available = 0;
        try {
            available = x.available();
        } catch (IOException e) {
            throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
        }
        updateBinaryStream(columnIndex, x, available);
	}

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnLabel, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnLabel, x);
        }

        if (x == null) {
            updateNull(columnLabel);
            return;
        }
        int available = 0;
        try {
            available = x.available();
        } catch (IOException e) {
            throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
        }
        updateAsciiStream(columnLabel, x, available);
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", columnLabel, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", columnLabel, x);
        }

        if (x == null) {
            updateNull(columnLabel);
            return;
        }
        int available = 0;
        try {
            available = x.available();
        } catch (IOException e) {
            throw TrafT4Messages.createSQLException(getT4props(), "io_exception", e.getMessage());
        }
        updateBinaryStream(columnLabel, x, available);
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        // TODO Auto-generated method stub

    }

    public Object getObject(int columnIndex, Class type) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public Object getObject(String columnLabel, Class type) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    
    protected void updateResultSetIc(InterfaceConnection ic) {
        this.irs_.t4resultSet_.updateConnContext(ic);
    }

    protected String getLobHandle(int columnIndex) throws SQLException {
        validateGetInvocation(columnIndex);
        int dataType = outputDesc_[columnIndex - 1].dataType_;
        if (dataType != Types.CLOB && dataType != Types.BLOB)
            throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type", JDBCType.valueOf(dataType));
        int charset = InterfaceUtilities.SQLCHARSETCODE_UTF8;

        byte[] obj = (byte[]) getCurrentRow().getUpdatedArrayElement(columnIndex);
        if(obj == null) {
            return null;
        }
        String data = null;
        try {
            switch (dataType) {
                case Types.BLOB:
                    data = new String(obj, InterfaceUtilities.getCharsetName(InterfaceUtilities.SQLCHARSETCODE_ISO88591));
                    break;
                case Types.CLOB:
                    data = new String(obj, InterfaceUtilities.getCharsetName(InterfaceUtilities.SQLCHARSETCODE_UTF8));
                    break;
                default:
                    break;
            }
        } catch (UnsupportedEncodingException e) {
            SQLException se = TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding", e.getMessage());
            se.initCause(e);
            throw se;
        }
        return data.trim();
    }
}
