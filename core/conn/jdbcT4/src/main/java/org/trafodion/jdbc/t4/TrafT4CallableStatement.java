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

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;

public class TrafT4CallableStatement extends TrafT4PreparedStatement implements java.sql.CallableStatement {
    private final static org.slf4j.Logger LOG =
            LoggerFactory.getLogger(TrafT4CallableStatement.class);
    
	public Array getArray(int parameterIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
	    }
	    if (LOG.isTraceEnabled()) {
	        LOG.trace("ENTRY. {}", parameterIndex);
	    }
		clearWarnings();
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "getArray()");
		return null;
	}

	public Array getArray(String parameterName) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
	    }
	    if (LOG.isTraceEnabled()) {
	        LOG.trace("ENTRY. {}", parameterName);
	    }
		int parameterIndex = validateGetInvocation(parameterName);
		return getArray(parameterIndex);
	}

	public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
	    }
	    if (LOG.isTraceEnabled()) {
	        LOG.trace("ENTRY. {}", parameterIndex);
	    }
		BigDecimal retValue;
		String data;

		validateGetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// String returned may not be numeric in case of SQL_CHAR, SQL_VARCHAR
		// and SQL_LONGVARCHAR
		// fields. Hoping that java might throw invalid value exception
		// For LOB Support - SB
		// data = inputDesc_[parameterIndex-1].paramValue_;
		data = getString(parameterIndex);
		if (data == null) {
			wasNull_ = true;
			return null;
		} else {
			wasNull_ = false;
			try {
				retValue = new BigDecimal(data);
			} catch (NumberFormatException e) {
				throw TrafT4Messages.createSQLException(getT4props(),
						"invalid_cast_specification");
			}
			return retValue;
		}
	}

	public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, scale);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, scale);
        }
		BigDecimal retValue;

		retValue = getBigDecimal(parameterIndex);
		if (retValue != null) {
			return retValue.setScale(scale);
		} else {
			return null;
		}
	}

	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getBigDecimal(parameterIndex);
	}

	public BigDecimal getBigDecimal(String parameterName, int scale) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, scale);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, scale);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getBigDecimal(parameterIndex, scale);
	}


	public boolean getBoolean(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		String data;

		validateGetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// For LOB Support - SB
		// data = inputDesc_[parameterIndex-1].paramValue_;
		data = getString(parameterIndex);

		if (data != null) {
			wasNull_ = false;
			return (!data.equals("0"));
		} else {
			wasNull_ = true;
			return false;
		}
	}

	public boolean getBoolean(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getBoolean(parameterIndex);
	}

	public byte getByte(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		String data;

		validateGetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// For LOB Support - SB
		// data = inputDesc_[parameterIndex-1].paramValue_;
		data = getString(parameterIndex);

		if (data != null) {
			wasNull_ = false;
			return Byte.parseByte(data);
		} else {
			wasNull_ = true;
			return 0;
		}
	}

	public byte getByte(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getByte(parameterIndex);
	}

	public byte[] getBytes(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		int dataType;

		validateGetInvocation(parameterIndex);
		dataType = inputDesc_[parameterIndex - 1].dataType_;
		if (dataType != Types.BINARY && dataType != Types.VARBINARY && dataType != Types.LONGVARBINARY) {
			throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type");
		}
		return getBytes(parameterIndex);
	}

	public byte[] getBytes(String parameterName) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
	    }
	    if (LOG.isTraceEnabled()) {
	        LOG.trace("ENTRY. {}", parameterName);
	    }
		int parameterIndex = validateGetInvocation(parameterName);
		return getBytes(parameterIndex);
	}

	public Date getDate(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		int dataType;
		String dateStr;
		Date retValue;

		validateGetInvocation(parameterIndex);
		dataType = inputDesc_[parameterIndex - 1].dataType_;
		if (dataType != Types.CHAR && dataType != Types.VARCHAR && dataType != Types.LONGVARCHAR
				&& dataType != Types.DATE && dataType != Types.TIMESTAMP) {
			throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type");
		}
		// For LOB Support - SB
		// dateStr = inputDesc_[parameterIndex-1].paramValue_;
		dateStr = getString(parameterIndex);

		if (dateStr != null) {
			wasNull_ = false;
			try {
				boolean convertDate = connection_.getDateConversion();

				if (convertDate) {
					String dt = Utility.convertDateFormat(dateStr);
					retValue = Utility.valueOf(dt);
				} else {
					retValue = Date.valueOf(dateStr);
				}
			} catch (IllegalArgumentException e) {
				throw TrafT4Messages.createSQLException(getT4props(), 
						"invalid_cast_specification");
			}
			return retValue;
		} else {
			wasNull_ = true;
			return null;
		}
	}

	public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, cal);
        }
		Date sqlDate;
		java.util.Date d;

		sqlDate = getDate(parameterIndex);
		if (sqlDate != null) {
			if (cal != null) {
				cal.setTime(sqlDate);
				d = cal.getTime();
				sqlDate = new Date(d.getTime());
			}
			return sqlDate;
		} else {
			return (sqlDate);
		}
	}

	public Date getDate(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getDate(parameterIndex);
	}

	public Date getDate(String parameterName, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, cal);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getDate(parameterIndex, cal);
	}

	public double getDouble(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		String data;

		validateGetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// For LOB Support - SB
		// data = inputDesc_[parameterIndex-1].paramValue_;
		data = getString(parameterIndex);
		if (data != null) {
			wasNull_ = false;
			return Double.parseDouble(data);
		} else {
			wasNull_ = true;
			return 0;
		}
	}

	public double getDouble(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getDouble(parameterIndex);
	}

	public float getFloat(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		String data;

		validateGetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// For LOB Support - SB
		// data = inputDesc_[parameterIndex-1].paramValue_;
		data = getString(parameterIndex);

		if (data != null) {
			wasNull_ = false;
			return Float.parseFloat(data);
		} else {
			wasNull_ = true;
			return 0;
		}
	}

	public float getFloat(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getFloat(parameterIndex);
	}

	public int getInt(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		String data;

		validateGetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// For LOB Support - SB
		// data = inputDesc_[parameterIndex-1].paramValue_;
		data = getString(parameterIndex);
		if (data != null) {
			wasNull_ = false;
			return Integer.parseInt(data);
		} else {
			wasNull_ = true;
			return 0;
		}
	}

	public int getInt(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getInt(parameterIndex);
	}

	public long getLong(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		String data;

		validateGetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// For LOB Support - SB
		// data = inputDesc_[parameterIndex-1].paramValue_;
		data = getString(parameterIndex);
		if (data != null) {
			wasNull_ = false;
			return Long.parseLong(data);
		} else {
			wasNull_ = true;
			return 0;
		}
	}

	public long getLong(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getLong(parameterIndex);
	}

	public Object getObject(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		int dataType;
		byte byteValue;
		short shortValue;
		int intValue;
		long longValue;
		float floatValue;
		double doubleValue;
		boolean booleanValue;

		validateGetInvocation(parameterIndex);
		dataType = inputDesc_[parameterIndex - 1].dataType_;
		switch (dataType) {
		case Types.TINYINT:
			byteValue = getByte(parameterIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Byte(byteValue);
			}
		case Types.SMALLINT:
			intValue = getShort(parameterIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Integer(intValue);
			}
		case Types.INTEGER:
			intValue = getInt(parameterIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Integer(intValue);
			}
		case Types.BIGINT:
			longValue = getLong(parameterIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Long(longValue);
			}
		case Types.REAL:
			floatValue = getFloat(parameterIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Float(floatValue);
			}
		case Types.FLOAT:
		case Types.DOUBLE:
			doubleValue = getDouble(parameterIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Double(doubleValue);
			}
		case Types.DECIMAL:
		case Types.NUMERIC:
			return getBigDecimal(parameterIndex);
		case Types.BIT:
			booleanValue = getBoolean(parameterIndex);
			if (wasNull_) {
				return null;
			} else {
				return new Boolean(booleanValue);
			}
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			return getString(parameterIndex);
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return getBytes(parameterIndex);
		case Types.DATE:
			return getDate(parameterIndex);
		case Types.TIME:
			return getTime(parameterIndex);
		case Types.TIMESTAMP:
			return getTimestamp(parameterIndex);
		default:
			throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type");
		}
	}

	public Object getObject(int parameterIndex, Map map) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, map);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, map);
        }
		return getObject(parameterIndex, map);
	}

	public Object getObject(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getObject(parameterIndex);
	}

	public Object getObject(String parameterName, Map map) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, map);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, map);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getObject(parameterIndex, map);
	}

	public Ref getRef(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "getRef()");
		return null;
	}

	public Ref getRef(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getRef(parameterIndex);
	}

	public short getShort(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		String data;

		validateGetInvocation(parameterIndex);
		inputDesc_[parameterIndex - 1].checkValidNumericConversion(connection_.getLocale());
		// For LOB Support - SB
		// data = inputDesc_[parameterIndex-1].paramValue_;
		data = getString(parameterIndex);
		if (data != null) {
			wasNull_ = false;
			return Short.parseShort(data);
		} else {
			wasNull_ = true;
			return 0;
		}
	}

	public short getShort(String parameterName) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getShort(parameterIndex);
	}

	public String getString(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		String data;
		// For LOB Support - SB 10/8/2004
		Object x;
		int targetSqlType;
		int sqlCharset;

		validateGetInvocation(parameterIndex);

		targetSqlType = inputDesc_[parameterIndex - 1].dataType_;
		sqlCharset = inputDesc_[parameterIndex - 1].sqlCharset_;
		x = inputDesc_[parameterIndex - 1].paramValue_;

		if (x == null) {
			wasNull_ = true;
			data = null;
		} else {
			if (x instanceof byte[]) {
				try {
					if (this.ist_.getInterfaceConnection().getISOMapping() == InterfaceUtilities.SQLCHARSETCODE_ISO88591
							&& !this.ist_.getInterfaceConnection().getEnforceISO()
							&& sqlCharset == InterfaceUtilities.SQLCHARSETCODE_ISO88591)
						data = new String((byte[]) x, ist_.getInterfaceConnection().getT4props().getISO88591());
					else
						data = this.ist_.getInterfaceConnection().decodeBytes((byte[]) x, sqlCharset);

					wasNull_ = false;
				} catch (CharacterCodingException e) {
					SQLException se = TrafT4Messages.createSQLException(this.connection_.ic_.getT4props(), "translation_of_parameter_failed", "getLocalString", e.getMessage());
					se.initCause(e);
					throw se;
				} catch (UnsupportedCharsetException e) {
					SQLException se = TrafT4Messages.createSQLException(this.connection_.ic_.getT4props(),  "unsupported_encoding", e.getCharsetName());
					se.initCause(e);
					throw se;
				} catch (UnsupportedEncodingException e) {
					SQLException se = TrafT4Messages.createSQLException(this.connection_.ic_.getT4props(),  "unsupported_encoding", e.getMessage());
					se.initCause(e);
					throw se;
				}
			} else {
				data = x.toString();
				wasNull_ = false;
			}
		}
		return data;
	}

	public String getString(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getString(parameterIndex);
	}

	public Time getTime(int parameterIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		int dataType;
		String timeStr;
		Time retValue;

		validateGetInvocation(parameterIndex);
		dataType = inputDesc_[parameterIndex - 1].dataType_;
		if (dataType != Types.CHAR && dataType != Types.VARCHAR && dataType != Types.LONGVARCHAR
				&& dataType != Types.TIME && dataType != Types.TIMESTAMP) {
			throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type");
		}

		// For LOB Support - SB 10/8/2004
		// timeStr = inputDesc_[parameterIndex-1].paramValue_;
		timeStr = getString(parameterIndex);
		if (timeStr != null) {
			try {
				wasNull_ = false;
				retValue = Time.valueOf(timeStr);
			} catch (IllegalArgumentException e) {
				throw TrafT4Messages.createSQLException(getT4props(), "invalid_cast_specification");
			}
			return retValue;
		} else {
			wasNull_ = true;
			return null;
		}
	}

	public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
	      if (getT4props().isLogEnable(Level.FINEST)) {
	            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, cal);
	        }
	      if (LOG.isTraceEnabled()) {
	            LOG.trace("ENTRY. {}, {}", parameterIndex, cal);
	        }
		Time sqlTime;
		java.util.Date d;

		sqlTime = getTime(parameterIndex);
		if (sqlTime != null) {
			if (cal != null) {
				cal.setTime(sqlTime);
				d = cal.getTime();
				sqlTime = new Time(d.getTime());
			}
			return sqlTime;
		} else {
			return (sqlTime);
		}
	}

	public Time getTime(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getTime(parameterIndex);
	}

	public Time getTime(String parameterName, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, cal);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getTime(parameterIndex, cal);
	}

	public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		int dataType;
		String timestampStr;
		Timestamp retValue;

		validateGetInvocation(parameterIndex);
		dataType = inputDesc_[parameterIndex - 1].dataType_;
		if (dataType != Types.CHAR && dataType != Types.VARCHAR && dataType != Types.LONGVARCHAR
				&& dataType != Types.DATE && dataType != Types.TIMESTAMP) {
			throw TrafT4Messages.createSQLException(getT4props(), "restricted_data_type");
		}

		// For LOB Support - SB 10/8/2004
		// timestampStr = inputDesc_[parameterIndex - 1].paramValue_;
		timestampStr = getString(parameterIndex);
		if (timestampStr != null) {
			try {
				wasNull_ = false;
				retValue = Timestamp.valueOf(timestampStr);
			} catch (IllegalArgumentException e) {
				throw TrafT4Messages.createSQLException(getT4props(), "invalid_cast_specification");
			}
			return retValue;
		} else {
			wasNull_ = true;
			return null;
		}
	}

	public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, cal);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, cal);
        }
		Timestamp sqlTimestamp;
		java.util.Date d;
		int nanos;

		sqlTimestamp = getTimestamp(parameterIndex);
		if (sqlTimestamp != null) {
			if (cal != null) {
				nanos = sqlTimestamp.getNanos();
				cal.setTime(sqlTimestamp);
				d = cal.getTime();
				sqlTimestamp = new Timestamp(d.getTime());
				sqlTimestamp.setNanos(nanos);
			}
			return sqlTimestamp;
		} else {
			return (sqlTimestamp);
		}
	}

	public Timestamp getTimestamp(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getTimestamp(parameterIndex);
	}

	public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, cal);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getTimestamp(parameterIndex, cal);
	}

	public URL getURL(int parameterIndex) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		clearWarnings();
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "getURL()");
		return null;
	}

	public URL getURL(String parameterName) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		return getURL(parameterIndex);
	}

	public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, sqlType);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, sqlType);
        }
		// Ignoring sqlType and scale
		validateGetInvocation(parameterIndex);
		if (inputDesc_[parameterIndex - 1].paramMode_ == DatabaseMetaData.procedureColumnOut) {
			inputDesc_[parameterIndex - 1].isValueSet_ = true;
		}
	}

	public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, sqlType, scale);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, sqlType, scale);
        }
		// Ignoring sqlType and scale
		validateGetInvocation(parameterIndex);
		if (inputDesc_[parameterIndex - 1].paramMode_ == DatabaseMetaData.procedureColumnOut) {
			inputDesc_[parameterIndex - 1].isValueSet_ = true;
		}
	}

	public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, sqlType, typeName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, sqlType, typeName);
        }
		// Ignoring sqlType and typeName
		validateGetInvocation(parameterIndex);
		if (inputDesc_[parameterIndex - 1].paramMode_ == DatabaseMetaData.procedureColumnOut) {
			inputDesc_[parameterIndex - 1].isValueSet_ = true;
		}
	}

	public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, sqlType);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, sqlType);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		registerOutParameter(parameterIndex, sqlType);
	}

	public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, sqlType, scale);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, sqlType, scale);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		registerOutParameter(parameterIndex, sqlType, scale);
	}

	public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, sqlType, typeName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, sqlType, typeName);
        }
		int parameterIndex = validateGetInvocation(parameterName);
		registerOutParameter(parameterIndex, sqlType, typeName);
	}

	public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, length);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, x, length);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setAsciiStream(parameterIndex, x, length);
	}

	public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setBigDecimal(parameterIndex, x);
	}

	public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, x, length);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setBinaryStream(parameterIndex, x, length);
	}

	public void setBoolean(String parameterName, boolean x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setBoolean(parameterIndex, x);
	}

	public void setByte(String parameterName, byte x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setByte(parameterIndex, x);
	}

	public void setBytes(String parameterName, byte[] x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setBytes(parameterIndex, x);
	}

	public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, reader, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, reader, length);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setCharacterStream(parameterIndex, reader, length);
	}

	public void setDate(String parameterName, Date x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setDate(parameterIndex, x);
	}

	public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, x, cal);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setDate(parameterIndex, x, cal);
	}

	public void setDouble(String parameterName, double x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setDouble(parameterIndex, x);
	}

	public void setFloat(String parameterName, float x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setFloat(parameterIndex, x);
	}

	public void setInt(String parameterName, int x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setInt(parameterIndex, x);
	}

	public void setLong(String parameterName, long x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setLong(parameterIndex, x);
	}

	public void setNull(String parameterName, int sqlType) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, sqlType);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, sqlType);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setNull(parameterIndex, sqlType);
	}

	public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, sqlType, typeName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, sqlType, typeName);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setNull(parameterIndex, sqlType, typeName);
	}

	public void setObject(String parameterName, Object x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setObject(parameterIndex, x);
	}

	public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, targetSqlType);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, x, targetSqlType);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setObject(parameterIndex, x, targetSqlType);
	}

	public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, targetSqlType, scale);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", parameterName, x, targetSqlType, scale);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setObject(parameterIndex, x, targetSqlType, scale);
	}

	public void setShort(String parameterName, short x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setShort(parameterIndex, x);
	}

	public void setString(String parameterName, String x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setString(parameterIndex, x);
	}

	public void setTime(String parameterName, Time x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setTime(parameterIndex, x);
	}

	public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, x, cal);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setTime(parameterIndex, x, cal);
	}

	public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setTimestamp(parameterIndex, x);
	}

	public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, x, cal);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setTimestamp(parameterIndex, x, cal);
	}

	public void setUnicodeStream(String parameterName, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, x, length);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setUnicodeStream(parameterIndex, x, length);
	}

	public void setURL(String parameterName, URL x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
		int parameterIndex = validateSetInvocation(parameterName);
		setURL(parameterIndex, x);
	}

	public boolean wasNull() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		return wasNull_;
	}

	public boolean execute() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		Object[] valueArray = null;
		int inDescLength = 0;
		if (inputDesc_ != null) {
			valueArray = getValueArray();
			inDescLength = inputDesc_.length;
		}

		validateExecuteInvocation();

		valueArray = getValueArray();

        execute(paramRowCount_, inDescLength, valueArray, queryTimeout_);

		// SPJ: 5-18-2007
		// if (resultSet_[result_set_offset] != null)
		if (resultSet_[result_set_offset] != null && resultSet_[result_set_offset].spj_rs_) {
			return true;
		} else {
			return false;
		}
	}

    private void execute(int paramRowCount, int paramCount, Object[] paramValues, int queryTimeout) throws SQLException {
        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY", paramRowCount, paramCount, paramValues, queryTimeout);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", paramRowCount, paramCount, paramValues, queryTimeout);
        }
        try {
            ist_.execute(TRANSPORT.SRVR_API_SQLEXECUTE2, paramRowCount, paramCount, paramValues, queryTimeout, null, this);
            if (getT4props().isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(getT4props(), Level.FINER, "LEAVE", paramRowCount, paramCount, paramValues, queryTimeout);
            }
        } catch (SQLException e) {
            reExecute(e, TRANSPORT.SRVR_API_SQLEXECUTE2, paramRowCount, paramCount, paramValues, queryTimeout);
        }
    }

    private void reExecute(SQLException e, short srvrApiSqlexecute2, int paramRowCount,
                           int paramCount, Object[] paramValues, int queryTimeout) throws SQLException {
        int time = 0;
        while (time < 3 && checkErroCode(e)) {
            if (LOG.isTraceEnabled()) {
                LOG.debug("ENTRY. {}","Start to rePrepareCall and execute " + (time + 1) + " times");
            }
            try{
                this.prepareCall(sql_, queryTimeout_, 0 /* Not used */);
                ist_.execute(srvrApiSqlexecute2, paramRowCount, paramCount, paramValues, queryTimeout, null, this);
            }catch (SQLException sqle){
                if (sqle instanceof TrafT4Exception) {
                    TrafT4Exception trafT4Exception = (TrafT4Exception) sqle;
                    TrafT4Exception newTrafT4Exception = TrafT4Exception.getNewTrafT4Exception(trafT4Exception," reExecute failed for the " + (time + 1) + " time!");
                    if (LOG.isTraceEnabled()) {
                        LOG.error(newTrafT4Exception + " reExecute failed for the " + (time + 1) + " time!");
                    }
                    e = newTrafT4Exception;
                }
                else {
                    e = sqle;
                }
                time++;
                continue;
            }
            return;
        }
        performConnectionErrorChecks(e);
        throw e;
    }

    /**
     * @Description:  This method is used to check if the error code exists in the error code Set
     * @Param:  SQLException e
     * @return:  boolean
     */
    private boolean checkErroCode(SQLException e){
        boolean results = true;
        switch (e.getErrorCode()){
            case -8738: break;
            default:results = false;break;
        }
        return results;
    }

	public int[] executeBatch() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if ((batchCommands_ == null) || (paramRowCount_ < 1)) {
			return new int[] {};
		}

		if (batchCommands_.isEmpty()) {
			return new int[] {};
		}

		clearWarnings();
		TrafT4Messages.throwUnsupportedFeatureException(getT4props(), connection_.getLocale(), "executeBatch()");
		return null;
	}

	public ResultSet executeQuery() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		Object[] valueArray = null;
		int inDescLength = 0;
		if (inputDesc_ != null) {
			valueArray = getValueArray();
			inDescLength = inputDesc_.length;
		}

		validateExecuteInvocation();

        execute(paramRowCount_, inDescLength, valueArray, queryTimeout_);

		return resultSet_[result_set_offset];
	}

	public int executeUpdate() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		Object[] valueArray = null;
		int inDescLength = 0;
		if (inputDesc_ != null) {
			valueArray = getValueArray();
			inDescLength = inputDesc_.length;
		}

		validateExecuteInvocation();
		valueArray = getValueArray();

        execute(paramRowCount_, inDescLength, valueArray, queryTimeout_);

		return (1);
	}

	// Other methods
	protected void validateGetInvocation(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
		clearWarnings();
		// connection_.getServerHandle().isConnectionOpen();
		connection_.isConnectionOpen();
		if (isClosed_) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_statement");
		}
		if (inputDesc_ == null) {
			throw TrafT4Messages.createSQLException(getT4props(),
					"not_a_output_parameter");
		}
		if (parameterIndex < 1 || parameterIndex > inputDesc_.length) {
			throw TrafT4Messages.createSQLException(getT4props(),
					"invalid_parameter_index",parameterIndex);
		}
		if (inputDesc_[parameterIndex - 1].paramMode_ != DatabaseMetaData.procedureColumnInOut
				&& inputDesc_[parameterIndex - 1].paramMode_ != DatabaseMetaData.procedureColumnOut) {
			throw TrafT4Messages.createSQLException(getT4props(),
					"not_a_output_parameter");
		}
	}

	protected int validateGetInvocation(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int i;

		if (isClosed_) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_statement");
		}
		if (inputDesc_ == null) {
			throw TrafT4Messages.createSQLException(getT4props(),
					"not_a_output_parameter");
		}
		for (i = 0; i < inputDesc_.length; i++) {
			if (parameterName.equalsIgnoreCase(inputDesc_[i].name_)) {
				return i + 1;
			}
		}
		throw TrafT4Messages.createSQLException(getT4props(), "invalid_parameter_name");
	}

	private int validateSetInvocation(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
		int i;

		if (isClosed_) {
			throw TrafT4Messages.createSQLException(getT4props(), "stmt_closed");
		}
		if (inputDesc_ == null) {
			throw TrafT4Messages.createSQLException(getT4props(),
					"invalid_parameter_index");
		}
		for (i = 0; i < inputDesc_.length; i++) {
			if (parameterName.equalsIgnoreCase(inputDesc_[i].name_)) {
				return i + 1;
			}
		}
		throw TrafT4Messages.createSQLException(getT4props(), "invalid_parameter_name");
	}

	void setExecuteCallOutputs(Object[] outputValues, short rowsAffected) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", outputValues, rowsAffected);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", outputValues, rowsAffected);
        }
		if (outputValues != null) {
			for (int i = 0; i < outputValues.length; i++) {
				if (outputValues[i] == null) {
					inputDesc_[i].paramValue_ = null;
				} else if (outputValues[i] instanceof byte[]) {
					inputDesc_[i].paramValue_ = outputValues[i];
				} else {
					inputDesc_[i].paramValue_ = outputValues[i].toString();
				}
			}
		}
		returnResultSet_ = rowsAffected;
	}

	// Constructors with access specifier as "default"
	TrafT4CallableStatement(TrafT4Connection connection, String sql) throws SQLException {
		super(connection, sql);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", connection, sql);
        }
	}

	TrafT4CallableStatement(TrafT4Connection connection, String sql, String stmtLabel) throws SQLException {
		super(connection, sql, stmtLabel);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, sql, stmtLabel);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", connection, sql, stmtLabel);
        }
	}

	TrafT4CallableStatement(TrafT4Connection connection, String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		super(connection, sql, resultSetType, resultSetConcurrency);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, sql, resultSetType, resultSetConcurrency);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", connection, sql, resultSetType,
                    resultSetConcurrency);
        }
	}

	TrafT4CallableStatement(TrafT4Connection connection, String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		super(connection, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}", connection, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        }
	}
	
	// Interface methods
	void prepareCall(String sql, int queryTimeout, int holdability) throws SQLException {
		super.ist_.prepare(sql, queryTimeout, this);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql, queryTimeout, holdability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", sql, queryTimeout, holdability);
        }
	};

	void executeCall(int inputParamCount, Object[] inputParamValues, int queryTimeout) throws SQLException {
		/*
		 * super.ist_.execute( inputParamCount, inputParamValues, queryTimeout,
		 * this);
		 */
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", inputParamCount, inputParamValues, queryTimeout);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", inputParamCount, inputParamValues, queryTimeout);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "executeCall");
	};

	void cpqPrepareCall(String moduleName, int moduleVersion, long moduleTimestamp, String stmtName, int queryTimeout,
			int holdability) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", moduleName, moduleVersion,
                    moduleTimestamp, stmtName, queryTimeout, holdability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}, {}", moduleName, moduleVersion, moduleTimestamp,
                    stmtName, queryTimeout, holdability);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "cpqPrepareCall");
	};

	// fields
	boolean wasNull_;
	short returnResultSet_;

    public RowId getRowId(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getRowId");
    }

    public RowId getRowId(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getRowId");
    }

    public void setRowId(String parameterName, RowId x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName ,x);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setRowId");
    }

    public void setNString(String parameterName, String value) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, value);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, value);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setNString");
    }

    public void setNCharacterStream(String parameterName, Reader value, long length)
            throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, value, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, value, length);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "setNCharacterStream");

    }

    public void setNClob(String parameterName, NClob value) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, value);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, value);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setNClob");

    }

    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, reader,
                    length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, reader, length);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setClob");
    }

    public void setBlob(String parameterName, InputStream inputStream, long length)
            throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, inputStream,
                    length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, inputStream, length);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setBlob");
    }

    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, reader,
                    length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, reader, length);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setNClob");
    }

    /**/
    public Blob getBlob(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getBlob");
    }

    public Blob getBlob(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getBlob");
    }

    public Clob getClob(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getClob");
    }

    public Clob getClob(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getClob");
    }

    public NClob getNClob(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getNClob");
    }

    public NClob getNClob(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getNClob");
    }

    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, xmlObject);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, xmlObject);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setSQLXML");
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getSQLXML");
    }

    public SQLXML getSQLXML(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getSQLXML");
    }

    public String getNString(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getNString");
    }

    public String getNString(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getNString");
    }

    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "getNCharacterStream");
    }

    public Reader getNCharacterStream(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "getNCharacterStream");
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterIndex);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "getCharacterStream");
    }

    public Reader getCharacterStream(String parameterName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", parameterName);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "getCharacterStream");
    }

    public void setBlob(String parameterName, Blob x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setBlob");
    }

    public void setClob(String parameterName, Clob x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setClob");
    }

    public void setAsciiStream(String parameterName, InputStream x, long length)
            throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, x, length);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "setAsciiStream");
    }

    public void setBinaryStream(String parameterName, InputStream x, long length)
            throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, x, length);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "setBinaryStream");
    }

    public void setCharacterStream(String parameterName, Reader reader, long length)
            throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, reader,
                    length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterName, reader, length);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "setCharacterStream");
    }

    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "setAsciiStream");
    }

    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, x);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "setBinaryStream");
    }

    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, reader);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, reader);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "setCharacterStream");
    }

    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, value);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, value);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature",
                "setNCharacterStream");
    }

    public void setClob(String parameterName, Reader reader) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, reader);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, reader);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setClob");
    }

    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, inputStream);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, inputStream);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setBlob");
    }

    public void setNClob(String parameterName, Reader reader) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, reader);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, reader);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "setNClob");
    }

    public Object getObject(int parameterIndex, Class type) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, type);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, type);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getObject");
    }

    public Object getObject(String parameterName, Class type) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterName, type);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterName, type);
        }
        throw TrafT4Messages.createSQLException(getT4props(), "unsupported_feature", "getObject");
    }
}
