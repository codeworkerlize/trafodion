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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Date;
import org.trafodion.jdbc.t4.utils.JDBCType;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.tmpl.*;
import org.trafodion.jdbc.t4.trace.TraceLinkThreshold;

class InterfaceStatement {
    static final short SQL_DROP = 1;
    static final short EXTERNAL_STMT = 0;

    InterfaceConnection ic_;
    private T4Statement t4statement_;
    private TrafT4Statement stmt_;
    private int sqlStmtType_ = TRANSPORT.TYPE_UNKNOWN;
    private int stmtType_ = 0;
    private String stmtLabel_;
    private String cursorName_;
    private int sqlQueryType_;
    private int stmtHandle_ = -1;
    private long rowCount_;
    // private int estimatedCost_;
    // private boolean prepare2 = false;
    private long prepareTimeMills = 0L;
    private boolean cachedPreparedStmtFull;
    private boolean preparedStmtUseESP;
    private boolean isprepareStmt;
    private boolean isPstmsAdd = false;
    private boolean isRetryExecute = false;
    // used for SPJ transaction
	static Class LmUtility_class_ = null;
	static java.lang.reflect.Method LmUtility_getTransactionId_ = null;

	private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(InterfaceStatement.class);
	PrepareReply pr_;

	// ----------------------------------------------------------------------
	InterfaceStatement(TrafT4Statement stmt) throws SQLException {
		this.ic_ = ((TrafT4Connection) stmt.getConnection()).getServerHandle();
		stmtLabel_ = stmt.stmtLabel_;
		cursorName_ = stmt.cursorName_;
		t4statement_ = new T4Statement(this);
		stmt_ = stmt;
        this.isprepareStmt = false;
		sqlQueryType_ = TRANSPORT.SQL_QUERY_TYPE_NOT_SET;
	};

    // ----------------------------------------------------------------------
    InterfaceStatement(TrafT4Connection trafT4Connection, boolean isprepareStmt) throws SQLException {
        this.ic_ = trafT4Connection.getServerHandle();
        t4statement_ = new T4Statement(this);
        this.isprepareStmt = isprepareStmt;
        sqlQueryType_ = TRANSPORT.SQL_QUERY_TYPE_NOT_SET;
    };

    protected void setStmtLabel(String stmtLabel) {
        this.stmtLabel_ = stmtLabel;
    }

    /**
     * This method will take an object and convert it to the approperite format
     * for sending to TrafT4.
     * @param pstmt
     * @param paramValue
     * @param paramRowCount
     * @param paramNumber
     * @param values
     * @param nullValueBuf
     * @param valueCurOffset
     * @param rowNumber
     * @param row
     * @param clipVarchar
     * @return
     * @throws SQLException
     */
    int convertObjectToSQL2(TrafT4Statement pstmt, Object paramValue, int paramRowCount, int paramNumber,
            byte[] values, byte[] nullValueBuf, int valueCurOffset, int rowNumber, int row, int clipVarchar)
            throws SQLException {

        int sqlDataType = pstmt.getInputSqlDataType(paramNumber);

        // setup the offsets
        int noNullValue = pstmt.getInputNoNullValue(paramNumber);
        int nullValue = pstmt.getInputNullValue(paramNumber);
        int maxLen = pstmt.getInputMaxLen(paramNumber);

        if (clipVarchar >= pstmt.getInputPrecision(paramNumber)  || clipVarchar == 0) { // if clipVarchar isnot needed
            switch (sqlDataType) {
                case InterfaceResultSet.SQLTYPECODE_VARCHAR_WITH_LENGTH:
                case InterfaceResultSet.SQLTYPECODE_VARCHAR_LONG:
                    maxLen += isShortLen(pstmt.getInputPrecision(paramNumber)) ? InterfaceUtilities.SHORT_BYTE_LEN : InterfaceUtilities.INT_BYTE_LEN;
                    maxLen += maxLen & 1; // even number alignment
                    break;
                case InterfaceResultSet.SQLTYPECODE_CLOB:
                case InterfaceResultSet.SQLTYPECODE_BLOB:
                    maxLen += InterfaceUtilities.INT_BYTE_LEN;
                    maxLen += maxLen & 1; // even number alignment
                    break;
                default:
                    break;
            }
        } else {
            switch (sqlDataType) {
                case InterfaceResultSet.SQLTYPECODE_CLOB:
                case InterfaceResultSet.SQLTYPECODE_BLOB:
                    maxLen += InterfaceUtilities.INT_BYTE_LEN;
                    break;
                default:
                    break;
            }
        }

        int retcode = maxLen;

        if (nullValue != -1)
            nullValue = (nullValue * paramRowCount) + (rowNumber * 2);

        noNullValue = (noNullValue * paramRowCount) + (rowNumber * maxLen);

        if(clipVarchar > 0) {
            noNullValue = valueCurOffset;
        }

        if (getT4props().isLogEnable(Level.FINER)) {
            String temp = "Processing row count = " + paramRowCount + ", row = " + rowNumber
                    + ", col = " + paramNumber + ", nullValue = " + nullValue + ", noNullValue = "
                    + noNullValue + ",  sqlDataType = " + sqlDataType + ", paramValue = "
                    + paramValue;
            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing row count = {}, row = {}, col = {}, nullValue = {}, noNullValue = {},  sqlDataType = {}, paramValue = {}",
                    paramRowCount, rowNumber, paramNumber, nullValue, noNullValue, sqlDataType,
                    paramValue);
        }
        boolean checkFlag = false;
        TrafCheckActiveMaster activeMaster = TrafCheckActiveMaster.getActiveMasterCacheMap().get(getT4props().getT4Url());
        if (activeMaster.isEmptyEqualsNull()) {
            checkFlag = (paramValue == null || paramValue.toString().length() == 0);
        }else {
            checkFlag = paramValue == null;
        }
        if (checkFlag) {
            if (nullValue != -1) {
                // values[nullValue] = -1;
                Bytes.insertShort(values, nullValue, (short) -1, this.ic_.getByteSwap());
                if (clipVarchar > 0) {
                    // rwo * 2 is nullvalue offset
                    Bytes.insertShort(nullValueBuf, (row) * 2, (short) -1, this.ic_.getByteSwap());
                    if ((sqlDataType == InterfaceResultSet.SQLTYPECODE_VARCHAR_WITH_LENGTH || sqlDataType == InterfaceResultSet.SQLTYPECODE_VARCHAR_LONG)
                            && pstmt.getInputMaxLen(paramNumber) >= clipVarchar) {
                        return 0;
                    } else {
                        return retcode;
                    }
                } else {
                    // nullValue is offset
                    Bytes.insertShort(values, nullValue, (short) -1, this.ic_.getByteSwap());
                    return 0;
                }// null return 0
            } else {
                throw TrafT4Messages.createSQLException(getT4props(),
                    "null_parameter_for_not_null_column", paramNumber);
            }
        }

        switch (sqlDataType) {
            case InterfaceResultSet.SQLTYPECODE_CHAR:
                handleChar(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_VARCHAR:
                handleVarchar(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_DATETIME:
                handleDataTime(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_INTERVAL:
                handleInterval(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_VARCHAR_WITH_LENGTH:
            case InterfaceResultSet.SQLTYPECODE_VARCHAR_LONG:
                int realLen = handleVarcharWithLen(pstmt, paramValue, paramNumber, values, noNullValue);
                if (clipVarchar > 0 && clipVarchar <= pstmt.getInputMaxLen(paramNumber))
                    retcode = realLen;
                else
                    retcode = maxLen;
				
				T4LoggingUtilities.log(getT4props(), Level.FINE,"retcode = "+retcode);
                break;
            case InterfaceResultSet.SQLTYPECODE_BLOB:
                handleBlob(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_BINARY:
                handleBinary(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_VARBINARY:
                handleVarBinary(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_CLOB:
                handleClob(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_INTEGER:
            case InterfaceResultSet.SQLTYPECODE_INTEGER_UNSIGNED:
                handleInteger(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_TINYINT:
            case InterfaceResultSet.SQLTYPECODE_TINYINT_UNSIGNED:
                handleTinyInt(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_SMALLINT:
            case InterfaceResultSet.SQLTYPECODE_SMALLINT_UNSIGNED:
                handleSmallInt(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_LARGEINT:
            case InterfaceResultSet.SQLTYPECODE_LARGEINT_UNSIGNED:
                handleLargeInt(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_DECIMAL:
            case InterfaceResultSet.SQLTYPECODE_DECIMAL_UNSIGNED:
                handleDecimal(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_REAL:
                handleReal(pstmt, paramValue, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_FLOAT:
                handleFloat(pstmt, paramValue, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_DOUBLE:
                handleDouble(pstmt, paramValue, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_NUMERIC:
            case InterfaceResultSet.SQLTYPECODE_NUMERIC_UNSIGNED:
                handleNumeric(pstmt, paramValue, paramNumber, values, noNullValue);
                break;
            case InterfaceResultSet.SQLTYPECODE_BOOLEAN:
                handleBoolean(pstmt, paramValue, values, noNullValue);
                break;
            // You will not get this type, since server internally converts it
            // SMALLINT, INTERGER or LARGEINT
            case InterfaceResultSet.SQLTYPECODE_DECIMAL_LARGE:
            case InterfaceResultSet.SQLTYPECODE_DECIMAL_LARGE_UNSIGNED:
            case InterfaceResultSet.SQLTYPECODE_BIT:
            case InterfaceResultSet.SQLTYPECODE_BITVAR:
            case InterfaceResultSet.SQLTYPECODE_BPINT_UNSIGNED:
            default:
                int dataType = pstmt.getInputSqlDataType(paramNumber);
                throw TrafT4Messages.createSQLException(getT4props(),
                        "restricted_data_type", JDBCType.valueOf(dataType));
        }
        return retcode;
    } // end convertObjectToSQL2

    private T4Properties getT4props() {
        return ic_.t4props;
    }

    private void handleBoolean(TrafT4Statement pstmt, Object paramValue, byte[] values,
            int noNullValue) throws SQLException {
        BigDecimal tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);

        Bytes.insertByte(values, noNullValue, tmpbd.byteValue());
    }

    private void handleNumeric(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws SQLException {
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        int precision = pstmt.getInputPrecision(paramNumber);
        int scale = pstmt.getInputScale(paramNumber);

        BigDecimal tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
        // round off by the scale.
        tmpbd = tmpbd.setScale(scale, BigDecimal.ROUND_DOWN);

        Utility.checkPrecisionAndScale(pstmt.getT4props(), tmpbd, precision, scale);
        byte[] b = InterfaceUtilities.convertBigDecimalToSQLBigNum(tmpbd, sqlOctetLength, scale);
        System.arraycopy(b, 0, values, noNullValue, sqlOctetLength);
    }

    private void handleDouble(TrafT4Statement pstmt, Object paramValue, byte[] values,
            int noNullValue) throws SQLException {
        BigDecimal tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
        Utility.checkDoubleBoundary(pstmt.getT4props(), tmpbd);
        Bytes.insertLong(values, noNullValue, Double.doubleToLongBits(tmpbd.doubleValue()),
                this.ic_.getByteSwap());
    }

    private void handleFloat(TrafT4Statement pstmt, Object paramValue, byte[] values,
            int noNullValue) throws SQLException {
        BigDecimal tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
        Utility.checkFloatBoundary(pstmt.getT4props(), tmpbd);
        Bytes.insertLong(values, noNullValue, Double.doubleToLongBits(tmpbd.doubleValue()),
                this.ic_.getByteSwap());
    }

    private void handleBinary(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws TrafT4Exception {
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        int dataLen;
        if (paramValue instanceof InputStream) {
            InputStream is = (InputStream) paramValue;
            try {
                is.read(values, noNullValue, sqlOctetLength);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            byte[] tmpBarray = (byte[]) paramValue;
            dataLen = tmpBarray.length;
            if (sqlOctetLength > dataLen) {
                System.arraycopy(tmpBarray, 0, values, noNullValue, dataLen);
            } else {
                throw TrafT4Messages.createSQLException(getT4props(),
                        "BINARY input data is longer than the length for column: " + paramNumber);
            }
        }
    }

    private void handleVarBinary(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws TrafT4Exception {
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        int dataOffset =
                isShortLen(pstmt.getInputPrecision(paramNumber)) ? InterfaceUtilities.SHORT_BYTE_LEN
                        : InterfaceUtilities.INT_BYTE_LEN;
        int dataLen;
        if (paramValue instanceof InputStream) {
            InputStream is = (InputStream) paramValue;
            dataLen = 0;
            try {
                int bytesRead =
                        is.read(values, noNullValue + dataOffset, sqlOctetLength - dataOffset);
                dataLen = bytesRead;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.arraycopy(Bytes.createIntBytes(dataLen, this.ic_.getByteSwap()), 0, values,
                    noNullValue, dataOffset);
        } else {
            byte[] tmpBarray = (byte[]) paramValue;
            dataLen = tmpBarray.length;
            if (sqlOctetLength > dataLen) {
                System.arraycopy(Bytes.createIntBytes(dataLen, this.ic_.getByteSwap()), 0, values,
                        noNullValue, dataOffset);
                System.arraycopy(tmpBarray, 0, values, (noNullValue + dataOffset), dataLen);
            } else {
                throw TrafT4Messages.createSQLException(getT4props(),
                        "VARBINARY input data is longer than the length for column: " + paramNumber);
            }
        }
    }

    private void handleReal(TrafT4Statement pstmt, Object paramValue, byte[] values,
            int noNullValue) throws SQLException {
        BigDecimal tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
        Utility.checkFloatBoundary(pstmt.getT4props(), tmpbd);
        float fvalue = tmpbd.floatValue();
        int bits = Float.floatToIntBits(fvalue);

        Bytes.insertInt(values, noNullValue, bits, this.ic_.getByteSwap());
    }

    private void handleDecimal(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws SQLException, TrafT4Exception {
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);

        byte[] tmpBarray;
        BigDecimal tmpbd;
        // create an parameter with out "."
        try {
            tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
            int scale = pstmt.getInputScale(paramNumber);
            if (scale > 0) {
                tmpbd = tmpbd.movePointRight(scale);
            }
            tmpbd = Utility.setScale(tmpbd, 0, BigDecimal.ROUND_HALF_UP);

            // data truncation check.
            if (pstmt.roundingMode_ == BigDecimal.ROUND_UNNECESSARY) {
                Utility.checkLongTruncation(paramNumber, tmpbd);

                // get only the mantissa part
            }
            try {
                tmpBarray = String.valueOf(tmpbd.longValue()).getBytes("ASCII");
            } catch (UnsupportedEncodingException e) {
                throw TrafT4Messages.createSQLException(getT4props(),
                        "unsupported_encoding", e.getMessage());
            }
        } catch (NumberFormatException nex) {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_parameter_value", "DECIMAL data format incorrect for column: "
                            + paramNumber + ". Error is: " + nex.getMessage());
        }

        int dataLen = tmpBarray.length;

        // pad leading zero's if datalen < maxLength
        int desPos = 0;
        int srcPos = 0;
        boolean minus = false;

        // check if data is negative.
        if (tmpbd.signum() == -1) {
            minus = true;
            srcPos++;
            dataLen--;
        }

        // pad beginning 0 for empty space.
        int numOfZeros = sqlOctetLength - dataLen;

        // DataTruncation is happening.
        if (numOfZeros < 0) {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "data_truncation_exceed", dataLen, sqlOctetLength);
        }

        for (int i = 0; i < numOfZeros; i++) {
            values[noNullValue + desPos] = (byte) '0';
            desPos = desPos + 1;
        }
        System.arraycopy(tmpBarray, srcPos, values, noNullValue + desPos, dataLen);

        // handling minus sign in decimal. OR -80 with the first byte for
        // minus
        if (minus) {
            values[noNullValue] = (byte) ((byte) (-80) | values[noNullValue]);
        }
    }

    private void handleLargeInt(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws SQLException {
        int precision = pstmt.getInputPrecision(paramNumber);
        int scale = pstmt.getInputScale(paramNumber);
        boolean sign = pstmt.isInputSigned(paramNumber);
        BigDecimal tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);

        if (scale > 0) {
            tmpbd = tmpbd.movePointRight(scale);
            // check boundary condition for Numeric.
        }
        if (sign) {
            Utility.checkLongBoundary(pstmt.getT4props(), tmpbd);
        } else {
            Utility.checkUnsignedLongBoundary(pstmt.getT4props(), tmpbd);
        }
        Bytes.insertLong(values, noNullValue, tmpbd.longValue(), this.ic_.getByteSwap());
    }

    private void handleSmallInt(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws SQLException {
        int precision = pstmt.getInputPrecision(paramNumber);
        int scale = pstmt.getInputScale(paramNumber);
        boolean sign = pstmt.isInputSigned(paramNumber);

        BigDecimal tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
        BigDecimal oldtmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
        if (scale > 0) {
            tmpbd = tmpbd.movePointRight(scale);
        }

        // data truncation check
        if (pstmt.roundingMode_ == BigDecimal.ROUND_UNNECESSARY) {
            Utility.checkLongTruncation(paramNumber, tmpbd);

            // range checking
        }
        if (sign) {
            Utility.checkShortBoundary(pstmt.getT4props(), tmpbd);
        } else {
            Utility.checkUnsignedShortBoundary(pstmt.getT4props(), tmpbd);
        }

        // check boundary condition for Numeric.
        Utility.checkDecimalBoundary(pstmt.getT4props(), tmpbd, precision, oldtmpbd);

        Bytes.insertShort(values, noNullValue, tmpbd.shortValue(), this.ic_.getByteSwap());
    }

    private void handleTinyInt(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws SQLException, TrafT4Exception {
        int scale = pstmt.getInputScale(paramNumber);
        boolean sign = pstmt.isInputSigned(paramNumber);

        BigDecimal tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
        if (scale > 0) {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_parameter_value", "Cannot have scale for param: " + paramNumber);
        }
        if (sign) {
            Utility.checkSignedTinyintBoundary(pstmt.getT4props(), tmpbd);
        } else {
            Utility.checkUnsignedTinyintBoundary(pstmt.getT4props(), tmpbd);
        }

        Bytes.insertShort(values, noNullValue, tmpbd.byteValueExact(), this.ic_.getByteSwap());
    }

    private void handleInteger(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws SQLException {
        int precision = pstmt.getInputPrecision(paramNumber);
        int scale = pstmt.getInputScale(paramNumber);
        boolean sign = pstmt.isInputSigned(paramNumber);
        BigDecimal tmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
        BigDecimal oldtmpbd = Utility.getBigDecimalValue(pstmt.getT4props(), paramValue);
        if (sign) {
            Utility.checkIntegerBoundary(pstmt.getT4props(), tmpbd);
        } else {
            Utility.checkUnsignedIntegerBoundary(pstmt.getT4props(), tmpbd);
        }
        if (scale > 0) {
            tmpbd = tmpbd.movePointRight(scale);
        }

        // data truncation check
        if (pstmt.roundingMode_ == BigDecimal.ROUND_UNNECESSARY) {
            Utility.checkLongTruncation(paramNumber, tmpbd);
            // range checking
        }

        // check boundary condition for Numeric.
        Utility.checkDecimalBoundary(pstmt.getT4props(), tmpbd, precision, oldtmpbd);

        Bytes.insertInt(values, noNullValue, tmpbd.intValue(), this.ic_.getByteSwap());
    }

    private void handleClob(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws TrafT4Exception {
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        // int sqlCharset = pstmt.getInputSqlCharset(paramNumber);
        int maxLen = pstmt.getInputMaxLen(paramNumber);

        byte[] tmpBarray;
        // String charSet = getTargetCharset(sqlCharset);;
        if (paramValue instanceof byte[]) {
            tmpBarray = (byte[]) paramValue;
        } else {
            try {
                tmpBarray = ((String) paramValue).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw TrafT4Messages.createSQLException(getT4props(),
                        "unsupported_encoding", "UTF-8");
            }
        }
        int dataLen = tmpBarray.length;
        int dataOffset = InterfaceUtilities.INT_BYTE_LEN;
        if (sqlOctetLength > dataLen) {
            System.arraycopy(Bytes.createIntBytes(dataLen, this.ic_.getByteSwap()), 0, values,
                    noNullValue, dataOffset);
            System.arraycopy(tmpBarray, 0, values, (noNullValue + dataOffset), dataLen);
        } else {
            int colIndex = paramNumber+1;
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_string_parameter",
                    "CLOB input data is longer than the length for column: colIdex is : " + (colIndex+1), dataLen, maxLen);
        }
    }

    private void handleBlob(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws TrafT4Exception {
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        int maxLen = pstmt.getInputMaxLen(paramNumber);
        int dataOffset = InterfaceUtilities.INT_BYTE_LEN;
        byte[] tmpBarray = (byte[]) paramValue;
        int dataLen = tmpBarray.length;
        if (sqlOctetLength > dataLen) {
            System.arraycopy(Bytes.createIntBytes(dataLen, this.ic_.getByteSwap()), 0, values,
                    noNullValue, dataOffset);
            System.arraycopy(tmpBarray, 0, values, (noNullValue + dataOffset), dataLen);
        } else {
            int colIndex = paramNumber+1;
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_string_parameter",
                    "BLOB input data is longer than the length for column: colIdex is : " + (colIndex+1), dataLen, maxLen);
        }
    }

    private int handleVarcharWithLen(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws TrafT4Exception {
        byte[] tmpBarray = null;
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        int sqlCharset = pstmt.getInputSqlCharset(paramNumber);
        int maxLen = pstmt.getInputMaxLen(paramNumber);
        boolean shortLen = isShortLen(pstmt.getInputPrecision(paramNumber));
        int dataOffset =
                shortLen ? InterfaceUtilities.SHORT_BYTE_LEN : InterfaceUtilities.INT_BYTE_LEN;
        String charSet = getTargetCharset(sqlCharset);

        if (paramValue instanceof byte[]) {
            try {
                tmpBarray = new String((byte[]) paramValue).getBytes(charSet);
            } catch (Exception e) {
                throw TrafT4Messages.createSQLException(getT4props(),
                        "unsupported_encoding", charSet);
            }
        } else if (paramValue instanceof String) {

            try {
                tmpBarray = ((String) paramValue).getBytes(charSet);
            } catch (Exception e) {
                throw TrafT4Messages.createSQLException(getT4props(),
                        "unsupported_encoding", charSet);
            }
        } // end if (paramValue instanceof String)
        else {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_cast_specification",
                    "VARCHAR data should be either bytes or String for column: " + paramNumber);
        }

        int dataLen = tmpBarray.length;
        if (sqlOctetLength > (dataLen + dataOffset)) {
            if (shortLen) {
                System.arraycopy(Bytes.createShortBytes((short) dataLen, this.ic_.getByteSwap()), 0,
                        values, noNullValue, dataOffset);
            } else {
                System.arraycopy(Bytes.createIntBytes((int) dataLen, this.ic_.getByteSwap()), 0,
                        values, noNullValue, dataOffset);
            }
            System.arraycopy(tmpBarray, 0, values, (noNullValue + dataOffset), dataLen);
        } else {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_string_parameter",
                    "VARCHAR data longer than column length: colIdex is : " + (paramNumber + 1),
                    dataLen, maxLen, pstmt.sql_);
        }

        return dataLen + dataOffset;
    }

    private void handleInterval(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws TrafT4Exception {
        byte[] tmpBarray;
        if (paramValue instanceof byte[]) {
            tmpBarray = (byte[]) paramValue;
        } else if (paramValue instanceof String) {
            try {
                tmpBarray = ((String) paramValue).getBytes("ASCII");
            } catch (Exception e) {
                throw TrafT4Messages.createSQLException(getT4props(),
                        "unsupported_encoding", "ASCII");
            }
        } else {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_cast_specification",
                    "INTERVAL data should be either bytes or String for column: " + paramNumber);
        }

        int dataLen = tmpBarray.length;
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        int maxLen = pstmt.getInputMaxLen(paramNumber);
        if (sqlOctetLength >= dataLen) {
            dataLen = tmpBarray.length;
            if (sqlOctetLength == dataLen) {
                System.arraycopy(tmpBarray, 0, values, noNullValue, sqlOctetLength);
            } else if (sqlOctetLength > dataLen) {
                System.arraycopy(tmpBarray, 0, values, noNullValue, dataLen);

                // Don't know when we need this. padding blanks. Legacy??
                Arrays.fill(values, (noNullValue + dataLen), (noNullValue + sqlOctetLength),
                        (byte) ' ');
            }
        } else {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_string_parameter",
                    "INTERVAL data longer than column length: colIndex is : " + (paramNumber+1), dataLen, maxLen, pstmt.sql_);
        }
    }

    private void handleDataTime(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws TrafT4Exception {
        int sqlDatetimeCode = pstmt.getInputSqlDatetimeCode(paramNumber);
        int dataType = pstmt.getInputDataType(paramNumber);
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        int maxLen = pstmt.getInputMaxLen(paramNumber);
        byte[] tmpBarray;
        int dataLen;
        Date tmpdate = null;
        switch (sqlDatetimeCode) {
            case InterfaceResultSet.SQLDTCODE_DATE:
                try {
                    String tmpDate = null;
                    if (paramValue instanceof byte[]) {
                        tmpDate = new String((byte[]) paramValue, "ASCII");
                    } else {
                        tmpDate = (String) paramValue;
                    }
                    if (tmpDate.matches(
                            "(\\d{4}-\\d{1,2}-\\d{1,2})|(\\d{1,2}\\.\\d{1,2}\\.\\d{4})|(\\d{1,2}/\\d{1,2}/\\d{4})")) {
                        tmpdate = Date.valueOf(tmpDate
                                .replaceFirst("(\\d{1,2})\\.(\\d{1,2})\\.(\\d{4})", "$3-$2-$1")
                                .replaceFirst("(\\d{1,2})/(\\d{1,2})/(\\d{4})", "$3-$1-$2"));
                    } else {
                        throw new IllegalArgumentException();
                    }
                } catch (IllegalArgumentException e) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "invalid_parameter_value",
                            "[" + paramValue
                                    + "] Date format is incorrect or date value is invalide. "
                                    + "  Supported format: YYYY-MM-DD, MM/DD/YYYY, DD.MM.YYYY");
                } catch (UnsupportedEncodingException e) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "unsupported_encoding", e.getMessage());
                }
                try {
                    byte[] temp1 = tmpdate.toString().getBytes("ASCII");
                    System.arraycopy(temp1, 0, values, noNullValue, temp1.length);
                } catch (UnsupportedEncodingException e) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "unsupported_encoding", e.getMessage());
                }
                break;
            case InterfaceResultSet.SQLDTCODE_TIMESTAMP:
                Timestamp tmpts = null;
                String tmpStr = null;
                try {
                    if (paramValue instanceof byte[]) {
                        tmpStr = new String((byte[]) paramValue, "ASCII");
                    } else {
                        tmpStr = (String) paramValue;
                    }
                    String pattern = "(\\d{4}-\\d{1,2}-\\d{1,2}):(.*)";
                    if (tmpStr != null && tmpStr.matches(pattern)) {
                        tmpStr = tmpStr.replaceFirst(pattern, "$1 $2");
                    }
                    if (!checkDataTimeFormat(tmpStr,true)) {
                        throw new IllegalArgumentException();
                    }
                    tmpts = Timestamp.valueOf(tmpStr);
                } catch (IllegalArgumentException iex) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "invalid_parameter_value",
                            "Timestamp data format is incorrect for column: " + paramNumber + " = "
                                    + tmpStr);
                } catch (UnsupportedEncodingException e) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "unsupported_encoding", e.getMessage());
                }

                // ODBC precision is nano secs. JDBC precision is micro secs
                // so substract 3 from ODBC precision.
                sqlOctetLength = sqlOctetLength - 3;
                try {
                    tmpBarray = tmpts.toString().getBytes("ASCII");
                } catch (UnsupportedEncodingException e) {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "unsupported_encoding", e.getMessage());
                }
                dataLen = tmpBarray.length;

                if (sqlOctetLength > dataLen) {
                    System.arraycopy(tmpBarray, 0, values, noNullValue, dataLen);

                    // Don't know when we need this. padding blanks. Legacy??
                    Arrays.fill(values, (noNullValue + dataLen), (noNullValue + sqlOctetLength),
                            (byte) ' ');
                } else {
                    System.arraycopy(tmpBarray, 0, values, noNullValue, sqlOctetLength);
                }
                //TODO there may be sqlOctetLength < dataLen
                break;
            case InterfaceResultSet.SQLDTCODE_TIME:
                // If the OdbcDataType is equal to Types.Other, that means
                // that this is HOUR_TO_FRACTION and should be treated
                // as a Type.Other --> see in SQLDesc.java
                if (dataType != java.sql.Types.OTHER) {
                    // do the processing for TIME

                    //dataType is Time
                    if (dataType == Types.TIME) {
                        Time time;
                        String dataTime = null;
                        try {
                            if (paramValue instanceof byte[]) {
                                dataTime = new String((byte[]) paramValue, "ASCII");
                            } else {
                                dataTime = paramValue.toString();
                            }
                            if (!checkDataTimeFormat(dataTime,false)) {
                                throw new IllegalArgumentException();
                            }
                            time = Time.valueOf(dataTime);
                            tmpBarray = time.toString().getBytes("ASCII");
                        } catch (IllegalArgumentException iex) {
                            throw TrafT4Messages.createSQLException(getT4props(),
                                    "invalid_parameter_value",
                                    "Time data format is incorrect for column: " + paramNumber
                                            + " = "
                                            + dataTime);
                        } catch (UnsupportedEncodingException e) {
                            throw TrafT4Messages.createSQLException(getT4props(),
                                    "unsupported_encoding", e.getMessage());
                        }
                        //dataType is Time
                    } else {
                        Timestamp timestamp;
                        String timestampStr;
                        String dataTmp = null;
                        try {
                            if (paramValue instanceof byte[]) {
                                dataTmp = new String((byte[]) paramValue, "ASCII");
                            } else {
                                dataTmp = paramValue.toString();
                            }
                            if (dataTmp.length() >= 19) {
                                if (!checkDataTimeFormat(dataTmp,true)) {
                                    throw new IllegalArgumentException();
                                }
                                timestamp = Timestamp.valueOf(dataTmp);
                                int index = timestamp.toString().indexOf(" ");
                                timestampStr = timestamp.toString().substring(index + 1);

                            } else {
                                if (!checkDataTimeFormat(dataTmp,false)) {
                                    throw new IllegalArgumentException();
                                }
                                timestamp = Timestamp.valueOf("1970-01-01 " + dataTmp);
                                int index = timestamp.toString().indexOf(" ");
                                timestampStr = timestamp.toString().substring(index + 1);
                                if (dataTmp.length() < timestampStr.length()) {
                                    timestampStr = timestampStr.substring(0, dataTmp.length());
                                }
                            }
                            tmpBarray = timestampStr.getBytes("ASCII");
                        } catch (IllegalArgumentException iex) {
                            throw TrafT4Messages.createSQLException(getT4props(),
                                    "invalid_parameter_value",
                                    "Time data format is incorrect for column: " + paramNumber
                                            + " = "
                                            + dataTmp);
                        } catch (UnsupportedEncodingException e) {
                            throw TrafT4Messages.createSQLException(getT4props(),
                                    "unsupported_encoding", e.getMessage());
                        }
                    }
                    // ODBC precision is nano secs. JDBC precision is micro secs
                    // so substract 3 from ODBC precision.
                    sqlOctetLength = sqlOctetLength - 3;
                    if (sqlOctetLength > tmpBarray.length) {
                        System.arraycopy(tmpBarray, 0, values, noNullValue, tmpBarray.length);

                        // Don't know when we need this. padding blanks. Legacy??
                        Arrays.fill(values, (noNullValue + tmpBarray.length),
                                (noNullValue + sqlOctetLength),
                                (byte) ' ');
                    } else {
                        System.arraycopy(tmpBarray, 0, values, noNullValue, sqlOctetLength);
                    }
                    break;
                } else {
                    // TrafT4Desc.SQLDTCODE_HOUR_TO_FRACTION data type!!!
                    // let the next case structure handle it
                }
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
            default:
                if (paramValue instanceof String) {
                    try {
                        tmpBarray = ((String) paramValue).getBytes("ASCII");
                    } catch (Exception e) {
                        throw TrafT4Messages.createSQLException(getT4props(),
                                "unsupported_encoding", "ASCII");
                    }
                } else if (paramValue instanceof byte[]) {
                    tmpBarray = (byte[]) paramValue;
                } else {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "invalid_cast_specification",
                            "DATETIME data should be either bytes or String for column: "
                                    + paramNumber);
                }
                dataLen = tmpBarray.length;
                if (sqlOctetLength == dataLen) {
                    System.arraycopy(tmpBarray, 0, values, noNullValue, sqlOctetLength);
                } else if (sqlOctetLength > dataLen) {
                    System.arraycopy(tmpBarray, 0, values, noNullValue, dataLen);

                    // Don't know when we need this. padding blanks. Legacy??
                    Arrays.fill(values, (noNullValue + dataLen), (noNullValue + sqlOctetLength),
                            (byte) ' ');
                } else {
                    throw TrafT4Messages.createSQLException(getT4props(),
                            "invalid_string_parameter",
                            "DATETIME data longer than column length: colIndex is : " + (paramNumber
                                    + 1), dataLen, maxLen, pstmt.sql_);
                }
                break;
        }
    }

    private boolean checkDataTimeFormat(String time, boolean isStamp) {
        if (time == null){
            return false;
        }
        int index = time.indexOf(".");
        if (index != -1) {
            time = time.substring(0, index);
        }
        String smh;
        if (isStamp) {
            //YYYY-MM-DD HH:mm:ss
            smh = "^((([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29))\\s+([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$";
        } else {
            //HH-mm-ss
            smh = "([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$";
        }
        Pattern patRule = Pattern.compile(smh);
        return patRule.matcher(time).matches();
    }

    private void handleVarchar(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws TrafT4Exception {
        int sqlCharset = pstmt.getInputSqlCharset(paramNumber);
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        int maxLen = pstmt.getInputMaxLen(paramNumber);
        int dataLen;
        byte[] tmpBarray;
        if (paramValue instanceof byte[]) {
            tmpBarray = (byte[]) paramValue;
        } else if (paramValue instanceof String) {
            String charSet = "";

            try {
                charSet = getTargetCharset(sqlCharset);
                tmpBarray = ((String) paramValue).getBytes(charSet);
            } catch (Exception e) {
                throw TrafT4Messages.createSQLException(getT4props(),
                        "unsupported_encoding", charSet);
            }

        } else {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_parameter_value",
                    "VARCHAR data should be either bytes or String for column: " + paramNumber);
        }

        dataLen = tmpBarray.length;
        if (sqlOctetLength > (dataLen + InterfaceUtilities.SHORT_BYTE_LEN)) {
            Bytes.insertShort(values, noNullValue, (short) dataLen, this.ic_.getByteSwap());
            System.arraycopy(tmpBarray, 0, values, noNullValue + InterfaceUtilities.SHORT_BYTE_LEN, dataLen);
        } else {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_string_parameter",
                    "VARCHAR input data is longer than the length for column: colIndex is : " + (paramNumber+1), dataLen, maxLen, pstmt.sql_);
        }
    }

    private void handleChar(TrafT4Statement pstmt, Object paramValue, int paramNumber,
            byte[] values, int noNullValue) throws TrafT4Exception {
        int sqlCharSet = pstmt.getInputSqlCharset(paramNumber);
        int sqlOctetLength = pstmt.getInputSqlOctetLength(paramNumber);
        int maxLen = pstmt.getInputMaxLen(paramNumber);
        int dataLen;
        byte[] tmpBarray;

        if (!(paramValue instanceof byte[] || paramValue instanceof String)) {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_parameter_value",
                    "CHAR data should be either bytes or String for column: " + paramNumber);
        }
        String charSet = "";

        try {
            charSet = getTargetCharset(sqlCharSet);
            if (paramValue instanceof byte[]) {
                tmpBarray = (new String((byte[]) paramValue)).getBytes(charSet);
            } else {
                tmpBarray = ((String) paramValue).getBytes(charSet);
            }
        } catch (Exception e) {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "unsupported_encoding", charSet);
        }

        //
        // We now have a byte array containing the parameter
        //
        dataLen = tmpBarray.length;
        if (sqlOctetLength >= dataLen) {
            System.arraycopy(tmpBarray, 0, values, noNullValue, dataLen);
            // Blank pad for rest of the buffer
            if (sqlOctetLength > dataLen) {
                if (sqlCharSet == InterfaceUtilities.SQLCHARSETCODE_UNICODE) {
                    // pad with Unicode spaces (0x00 0x32)
                    int i2 = dataLen;
                    while (i2 < sqlOctetLength) {
                        values[noNullValue + i2] = (byte) ' ';
                        values[noNullValue + (i2 + 1)] = (byte) 0;
                        i2 += 2;
                    }
                } else {
                    Arrays.fill(values, (noNullValue + dataLen), (noNullValue + sqlOctetLength),
                            (byte) ' ');
                }
            }
        } else {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_string_parameter",
                    "CHAR input data is longer than the length for column: colIndex is : " + (paramNumber+1), dataLen, maxLen, pstmt.sql_);
        }
    }

    private String getTargetCharset(int sqlCharset) {
        String charSet;
        if (this.ic_.getISOMapping() == InterfaceUtilities.SQLCHARSETCODE_ISO88591
                && !this.ic_.getEnforceISO()
                && sqlCharset == InterfaceUtilities.SQLCHARSETCODE_ISO88591)
            // TODO we should really figure out WHY this could happen
            charSet = getT4props().getISO88591();
        else {
            if (sqlCharset == InterfaceUtilities.SQLCHARSETCODE_UNICODE
                    && this.ic_.getByteSwap())
                charSet = "UTF-16LE";
            else
                charSet = InterfaceUtilities.getCharsetName(sqlCharset);
        }
        return charSet;
    }

    private boolean isShortLen(int precision) {
        return precision <= Short.MAX_VALUE;
    }

	private SQLWarningOrError[] mergeErrors(SQLWarningOrError[] client, SQLWarningOrError[] server) {
		SQLWarningOrError[] target = new SQLWarningOrError[client.length + server.length];

		int si = 0; // server index
		int ci = 0; // client index
		int ti = 0; // target index

		int sr; // server rowId
		int cr; // client rowId

		int so = 0; // server offset

		while (ci < client.length && si < server.length) {
			cr = client[ci].rowId;
			sr = server[si].rowId + so;

			if (cr <= sr || server[si].rowId == 0) {
				so++;
				target[ti++] = client[ci++];
			} else {
				server[si].rowId += so;
				target[ti++] = server[si++];
			}
		}

		// we only have one array left
		while (ci < client.length) {
			target[ti++] = client[ci++];
		}

		while (si < server.length) {
			if (server[si].rowId != 0)
				server[si].rowId += so;
			target[ti++] = server[si++];
		}

		return target;
	}

    SQL_DataValue_def fillInSQLValues2(TrafT4Statement stmt, int paramRowCount, int paramCount, Object[] paramValues,
            ArrayList<SQLWarningOrError> clientErrors) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        SQL_DataValue_def dataValue = new SQL_DataValue_def();


        if (paramRowCount == 0 && paramValues != null && paramValues.length > 0)
            paramRowCount = 1; // fake a single row if we are doing inputParams for an SPJ

        // TODO: we should really figure out WHY this could happen
        if (stmt.inputParamsLength_ < 0) {
            dataValue.buffer = new byte[0];
            dataValue.length = 0;
        } else {
            int varchar_count = 0;
            int bufLen = stmt.inputParamsLength_ * paramRowCount;
            dataValue.buffer = new byte[bufLen];
            int clipVarchar = getT4props().getClipVarchar();
            int doClipVarchar = 0;
            for (int col = 0; col < paramCount; col++) {
                int dataType = stmt.getInputSqlDataType(col);;
                int precision = stmt.getInputPrecision(col);

                if ((dataType == InterfaceResultSet.SQLTYPECODE_VARCHAR_WITH_LENGTH
                        || dataType == InterfaceResultSet.SQLTYPECODE_VARCHAR_LONG) && precision >= clipVarchar) {
                    varchar_count ++;
                }
            }

            if(varchar_count > 0 )
                doClipVarchar = clipVarchar;

            int startBatchNum = 0;
            if (stmt instanceof TrafT4PreparedStatement){
                TrafT4PreparedStatement pst = (TrafT4PreparedStatement) stmt;
                if (pst.isBatchFlag()) {
                    startBatchNum = pst.getStartRowNum();
                }
            }
            if (doClipVarchar > 0) { // clipVarchar for column wise
                int curValOffset = 0;
                for (int col = 0; col < paramCount; col++) {
                    int nullValue = stmt.getInputNullValue(col);
                    byte[] nullValueBuf;
                    int nullValueLen = 0;
                    if (nullValue != -1) { // col nullable
                        nullValueLen = 2 * paramRowCount;
                        nullValueBuf = new byte[nullValueLen];
                    } else { // col not null
                        nullValueLen = 0;
                        nullValueBuf = new byte[0];
                    }
                    int colStartOffset = curValOffset;
                    curValOffset += nullValueLen;
                    for (int row = 0; row < paramRowCount; row++) {
                        try {
                            // copy val buff first ,Reserve ind buf storage space in front  

                            int retLen = convertObjectToSQL2(stmt, paramValues[(row+startBatchNum) * paramCount + col], paramRowCount,
                                    col, dataValue.buffer, nullValueBuf, curValOffset, row - clientErrors.size(), row,
                                    doClipVarchar);

                            curValOffset += retLen;
                            if (row == paramRowCount - 1 && nullValueLen != 0) {
                                System.arraycopy(nullValueBuf, 0, dataValue.buffer, colStartOffset, nullValueLen); // If it is the last row of a column, then copy indebuf to the top of the column
                            }
                        } catch (TrafT4Exception e) {
                            // if (paramRowCount == 1) // for single rows we need to
                            // // throw immediately
                            throw e; // clipVarchar throw immediately
                            //
                            // clientErrors.add(new SQLWarningOrError(row + 1, e.getErrorCode(), e.getMessage(), e
                            // .getSQLState()));
                            // break; // skip the rest of the row
                        }
                    }
                }
               // fix the column offsets if we had errors
                dataValue.length = curValOffset;
            } else {

                int skipRow = 0;
                for (int row = 0; row < paramRowCount; row++) {
                    for (int col = 0; col < paramCount; col++) {
                        try {
                            convertObjectToSQL2(stmt, paramValues[(row+startBatchNum) * paramCount + col], paramRowCount, col,
                                    dataValue.buffer, null, 0, row - clientErrors.size(), row, doClipVarchar);
                        } catch (TrafT4Exception e) {
                            if (paramRowCount == 1) {
                                // for single rows we need to throw immediately
                                throw e;
                            }
                            if(e.getErrorCode() == -29183 && !isRetryExecute){
                                // for Error Code 29183 we need to throw immediately
                                throw e;
                            }
                            clientErrors.add(
                                    new SQLWarningOrError(row + 1, e.getErrorCode(), e.getMessage(), e.getSQLState()));
                            break; // skip the rest of the row
                        }
                    }
                    if (clientErrors.size() > 0 && !stmt.getT4props().isBatchSkipProblemRow()) {
                        skipRow = paramRowCount - (row + 1);
                        break;
                    }
                }

                skipRow += clientErrors.size();
                // fix the column offsets if we had errors
                if (skipRow > 0) {
                    if (getT4props().isLogEnable(Level.FINER)) {
                        String temp = "Processing skipRow = " + skipRow;
                        T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Processing skipRow = {}", skipRow);
                    }
                    int oldOffset;
                    int newOffset;
                    int noNullValue;
                    int nullValue;
                    int colLength;
                    int dataType;

                    for (int i = 0; i < paramCount; i++) {
                        noNullValue = stmt.getInputNoNullValue(i);
                        nullValue = stmt.getInputNullValue(i);
                        colLength = stmt.getInputMaxLen(i);
                        dataType = stmt.getInputSqlDataType(i);
                        switch (dataType) {
                            case InterfaceResultSet.SQLTYPECODE_VARCHAR_WITH_LENGTH:
                            case InterfaceResultSet.SQLTYPECODE_BLOB:
                            case InterfaceResultSet.SQLTYPECODE_CLOB:
                            case InterfaceResultSet.SQLTYPECODE_VARCHAR_LONG:
                            case InterfaceResultSet.SQLTYPECODE_VARCHAR:
                                colLength += isShortLen(stmt.getInputPrecision(i)) ? InterfaceUtilities.SHORT_BYTE_LEN : InterfaceUtilities.INT_BYTE_LEN;
                                colLength += colLength & 1;
                                break;
                            default:
                                break;
                        }

                        if (i > 0) {// skip the first col
                            oldOffset = noNullValue * paramRowCount;
                            newOffset = oldOffset - (noNullValue * skipRow);
                            System.arraycopy(dataValue.buffer, oldOffset, dataValue.buffer, newOffset,
                                    colLength * (paramRowCount - skipRow));
                        }
                        if (nullValue != -1) {
                            oldOffset = nullValue * paramRowCount;
                            newOffset = oldOffset - (nullValue * skipRow);
                            System.arraycopy(dataValue.buffer, oldOffset, dataValue.buffer, newOffset,
                                    2 * (paramRowCount - skipRow));
                        }
                    }
                }

                dataValue.length = stmt.inputParamsLength_ * (paramRowCount - skipRow);
            }
        }
        if (getT4props().isLogEnable(Level.FINEST) && dataValue.length < 1024 * 1024) {
            String temp =
                    "Final buffer length = " + dataValue.length + ", buffer = " + Arrays.toString(dataValue.buffer);
            T4LoggingUtilities.log(getT4props(), Level.FINEST, temp);
        }
        if (LOG.isDebugEnabled() && dataValue.length < 1024 * 1024) {
            LOG.debug("Final buffer length = {}, buffer = {}", dataValue.length, Arrays.toString(dataValue.buffer));
        }
        return dataValue;
    }

	// -------------------------------------------------------------
	short getSqlStmtType(String str) {
		short rt1 = TRANSPORT.TYPE_UNKNOWN;
		switch (sqlQueryType_) {
			case TRANSPORT.SQL_SELECT_UNIQUE:
			case TRANSPORT.SQL_SELECT_NON_UNIQUE:
				rt1 = TRANSPORT.TYPE_SELECT;
				break;
			case TRANSPORT.SQL_INSERT_UNIQUE:
			case TRANSPORT.SQL_INSERT_NON_UNIQUE:
			case TRANSPORT.SQL_INSERT_RWRS:
				if (stmt_ .inputDesc_ != null && stmt_.inputDesc_.length > 0)
					rt1 = TRANSPORT.TYPE_INSERT_PARAM;
				else
					rt1 = TRANSPORT.TYPE_INSERT;
				break;
			case TRANSPORT.SQL_UPDATE_UNIQUE:
			case TRANSPORT.SQL_UPDATE_NON_UNIQUE:
				rt1 = TRANSPORT.TYPE_UPDATE;
				break;
			case TRANSPORT.SQL_DELETE_UNIQUE:
			case TRANSPORT.SQL_DELETE_NON_UNIQUE:
				rt1 = TRANSPORT.TYPE_DELETE;
				break;
			case TRANSPORT.SQL_CALL_NO_RESULT_SETS:
			case TRANSPORT.SQL_CALL_WITH_RESULT_SETS:
				rt1 = TRANSPORT.TYPE_CALL;
				break;
			default:
				break;
		}
		return rt1;
	} // end getSqlStmtType

	// -------------------------------------------------------------
	long getRowCount() {
		return rowCount_;
	}

	// -------------------------------------------------------------
	void setRowCount(long rowCount) {
		if (rowCount < 0) {
			rowCount_ = -1;
		} else {
			rowCount_ = rowCount;
		}
	}

	// -------------------------------------------------------------
	static TrafT4Desc[] NewDescArray(SQLItemDescList_def desc) {
		int index;
		TrafT4Desc[] trafT4DescArray;
		SQLItemDesc_def SQLDesc;

		if (desc.list == null || desc.list.length == 0) {
			return null;
		}

		trafT4DescArray = new TrafT4Desc[desc.list.length];

		for (index = 0; index < desc.list.length; index++) {
			SQLDesc = desc.list[index];
			boolean nullInfo = (((new Byte(SQLDesc.nullInfo)).shortValue()) == 1) ? true : false;
			boolean signType = (((new Byte(SQLDesc.signType)).shortValue()) == 1) ? true : false;
			trafT4DescArray[index] = new TrafT4Desc(SQLDesc.dataType, (short) SQLDesc.datetimeCode, SQLDesc.maxLen,
					SQLDesc.precision, SQLDesc.scale, nullInfo, SQLDesc.colHeadingNm, signType, SQLDesc.ODBCDataType,
					SQLDesc.ODBCPrecision, SQLDesc.SQLCharset, SQLDesc.ODBCCharset, SQLDesc.CatalogName,
					SQLDesc.SchemaName, SQLDesc.TableName, SQLDesc.dataType, SQLDesc.intLeadPrec, SQLDesc.paramMode);
		}
		return trafT4DescArray;
	}

	// -------------------------------------------------------------
	static TrafT4Desc[] NewDescArray(Descriptor2[] descArray) {
		int index;
		TrafT4Desc[] trafT4DescArray;
		Descriptor2 desc;

		if (descArray == null || descArray.length == 0) {
			return null;
		}

		trafT4DescArray = new TrafT4Desc[descArray.length];

		for (index = 0; index < descArray.length; index++) {
			desc = descArray[index];
			boolean nullInfo = false;
			boolean signType = false;

			if (desc.nullInfo_ != 0) {
				nullInfo = true;
			}
			if (desc.signed_ != 0) {
				signType = true;

			}
			trafT4DescArray[index] = new TrafT4Desc(desc.noNullValue_, desc.nullValue_, desc.version_, desc.dataType_,
					(short) desc.datetimeCode_, desc.maxLen_, (short) desc.precision_, (short) desc.scale_, nullInfo,
					signType, desc.odbcDataType_, desc.odbcPrecision_, desc.sqlCharset_, desc.odbcCharset_,
					desc.colHeadingNm_, desc.tableName_, desc.catalogName_, desc.schemaName_, desc.headingName_,
					desc.intLeadPrec_, desc.paramMode_, desc.dataType_, desc.getRowLength(), desc.lobInlineMaxLen_, desc.lobChunkMaxLen_);
		}
		return trafT4DescArray;
	}

	// -------------------------------------------------------------
	// Interface methods
	void executeDirect(int queryTimeout, TrafT4Statement stmt) throws SQLException {
		short executeAPI = stmt.getOperationID();
		byte[] messageBuffer = stmt.getOperationBuffer();
		GenericReply gr = null;

		stmt.getTraceLinkThreshold().setSendToMxoTimestamp(System.currentTimeMillis());
		long start = 0;
		if (stmt.connection_.getT4props().isTraceTransTime() && !stmt.connection_.getAutoCommit()) {
		    start = System.currentTimeMillis();
		    stmt.connection_.checkTransStartTime(start);
		}
		gr = t4statement_.ExecuteGeneric(executeAPI, messageBuffer);
		stmt.connection_.addTransactionUsedTimeList(stmt.stmtLabel_, stmt.sql_, start, TRANSPORT.TRACE_EXECUTEDIRECT_TIME);
		stmt.getTraceLinkThreshold().setRecvFromMxoTimestamp(System.currentTimeMillis());
		stmt.operationReply_ = gr.replyBuffer;

        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", queryTimeout, stmt);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}, {}", queryTimeout, stmt);
        }
	} // end executeDirect

	// --------------------------------------------------------------------------
	int close() throws SQLException {
        CloseReply cry_ = null;
        try {
            ic_.isConnectionOpen();
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "Closing = " + stmtLabel_;
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Closing = {}", stmtLabel_);
            }
            cry_ = t4statement_.Close();
        } finally {
            if ((isprepareStmt || (stmt_ instanceof TrafT4PreparedStatement)) && this
                    .isPstmsAdd()) {
                ic_.getTrafT4Conn().decrementPrepreadStmtNum();
                this.setPstmsAdd(false);
            }
        }
		switch (cry_.m_p1.exception_nr) {
		case TRANSPORT.CEE_SUCCESS:
			// ignore the SQLWarning for the static close
			break;
		case odbc_SQLSvc_Close_exc_.odbc_SQLSvc_Close_SQLError_exn_:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "Close error. odbc_SQLSvc_Close_exc_.odbc_SQLSvc_Close_SQLError_exn_";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Close error. odbc_SQLSvc_Close_exc_.odbc_SQLSvc_Close_SQLError_exn_");
            }
			TrafT4Messages.throwSQLException(getT4props(), cry_.m_p1.SQLError);
		default:
			throw TrafT4Messages.createSQLException(getT4props(), "ids_unknown_reply_error");
		} // end switch

        if (getT4props().isLogEnable(Level.FINER)) {
            String temp = "Leave closing = " + stmtLabel_;
            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Leave closing = {}", stmtLabel_);
        }
		return cry_.m_p2; // rowsAffected
	} // end close

	// --------------------------------------------------------------------------
	void cancel(long startTime) throws SQLException {
		// currently there are no callers to this statement
		// It is important that callers specify the startTime correctly for cancel work as expected.
		ic_.cancel(startTime);
	}

    void cancel() throws SQLException {
        ic_.cancel(pr_ == null? null:pr_.getQid());
    }

    // --------------------------------------------------------------------------
	// Interface methods for prepared statement
	void prepare(String sql, int queryTimeout, TrafT4PreparedStatement pstmt) throws SQLException {
		int sqlAsyncEnable = 0;
		this.stmtType_ = EXTERNAL_STMT;
		int stmtLabelCharset = 1;
		String cursorName = pstmt.cursorName_;
		int cursorNameCharset = 1;
		String moduleName = pstmt.moduleName_;
		int moduleNameCharset = 1;
		long moduleTimestamp = pstmt.moduleTimestamp_;
		String sqlString = sql;
		int sqlStringCharset = 1;
		String stmtOptions = "";
		int maxRowsetSize = pstmt.getMaxRows();

		byte[] txId;

//3196 - NDCS transaction for SPJ	
//		if (ic_.t4props_.getSPJEnv())
//			txId = getUDRTransaction(this.ic_.getByteSwap());
//		else
//			txId = Bytes.createIntBytes(0, false);
		txId = Bytes.createIntBytes(0, false);
		PrepareReply pr = t4statement_.Prepare(sqlAsyncEnable, (short) this.stmtType_, this.sqlStmtType_,
				pstmt.stmtLabel_, stmtLabelCharset, cursorName, cursorNameCharset, moduleName, moduleNameCharset,
				moduleTimestamp, sqlString, sqlStringCharset, stmtOptions, maxRowsetSize, txId);
        prepareTimeMills = t4statement_.getPrepareTimeMills();
		pr_ = pr;
		this.sqlQueryType_ = pr.sqlQueryType;
		this.sqlStmtType_ = getSqlStmtType(sqlString);
		this.cachedPreparedStmtFull = pr.isCachedPrepareStmtFull();
		this.preparedStmtUseESP = pr.useESP();

        if (pr.shouldReconn()) {
            ic_.setReconn(true);
        }
		switch (pr.returnCode) {
		case TRANSPORT.SQL_SUCCESS:
		case TRANSPORT.SQL_SUCCESS_WITH_INFO:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "Prepare success within " + prepareTimeMills + " ms. Sql query type = "
                        + pr.sqlQueryType + ", RMS memory reaches limit = "
                        + cachedPreparedStmtFull + ", stmtHandle = " + pr.stmtHandle;
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Prepare success within {} ms. Sql query type = {}, RMS memory reaches limit = {}, stmtHandle = {}",
                        prepareTimeMills, pr.sqlQueryType, cachedPreparedStmtFull,
                        pr.stmtHandle);
            }
			TrafT4Desc[] OutputDesc = InterfaceStatement.NewDescArray(pr.outputDesc);
			TrafT4Desc[] InputDesc = InterfaceStatement.NewDescArray(pr.inputDesc);
			pstmt.setPrepareOutputs2(InputDesc, OutputDesc, pr.inputNumberParams, pr.outputNumberParams,
					pr.inputParamLength, pr.outputParamLength, pr.inputDescLength, pr.outputDescLength);

			if (pr.errorList != null && pr.errorList.length > 0) {
				TrafT4Messages.setSQLWarning(getT4props(), pstmt, pr.errorList);
			}
            if (stmtHandle_ == -1 && !isPstmsAdd) {
                // if the preparedStmt has prepared, then there is not need to check limit
                // see TrafT4PreparedStatement.doRePrepareCheck()
                pstmt.connection_.checkPrepreadStmtNum();
                setPstmsAdd(true);
            }
			this.stmtHandle_ = pr.stmtHandle;
			break;

		case odbc_SQLSvc_Prepare_exc_.odbc_SQLSvc_Prepare_SQLError_exn_:

		default:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "Prepare error...current prepared stmt num ["
                        + pstmt.connection_.getPrepreadStmtNum() + "]...cached prepared stmt ["
                        + pstmt.connection_.getCachedState() + "].";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Prepare error...current prepared stmt num [{}]...cached prepared stmt [{}].",
                        pstmt.connection_.getPrepreadStmtNum(),
                        pstmt.connection_.getCachedState());
            }
            try {
                this.close();
            } catch (SQLException e) {
                if(getT4props().isLogEnable(Level.WARNING)){
                    String temp = "An exception occurred when the InterfaceStatement was closed,Error:" + e.getMessage();
                    T4LoggingUtilities.log(getT4props(), Level.WARNING, temp);
                }
                if (LOG.isWarnEnabled()) {
                    LOG.warn("An exception occurred when the InterfaceStatement was closed,Error:<{}>",e.getMessage());
                }
            }
			TrafT4Messages.throwSQLException(getT4props(), pstmt, pr.errorList);
		}

        if (ic_.getAutoCommit() && ic_.shouldReconn()) {
            ic_.reconnIfNeeded();
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "reprepare when do scrollrestart operation";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("reprepare when do scrollrestart operation");
            }
            this.prepare(sqlString, queryTimeout, pstmt);
        }

        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE");
        }
	}

	// used to keep the same transaction inside an SPJ. we call out to the UDR
	// server and use their transaction for all executes.
	byte[] getUDRTransaction(boolean swapBytes) throws SQLException {
		byte[] ret = null;

		try {
			// To get references to method
			InterfaceStatement.LmUtility_class_ = Class.forName("com.tandem.sqlmx.LmUtility");
			InterfaceStatement.LmUtility_getTransactionId_ = InterfaceStatement.LmUtility_class_.getMethod(
					"getTransactionId", new Class[] {});

			// To invoke the method
			short[] tId = (short[]) InterfaceStatement.LmUtility_getTransactionId_.invoke(null, new Object[] {});

			ret = new byte[tId.length * 2];

			for (int i = 0; i < tId.length; i++) {
				Bytes.insertShort(ret, i * 2, tId[i], swapBytes);
			}
		} catch (Exception e) {
	        if (getT4props().isLogEnable(Level.FINER)) {
	            String temp = "Error calling UDR for transaction id";
	            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
	        }
	        if (LOG.isDebugEnabled()) {
	            LOG.debug("Error calling UDR for transaction id");
	        }
			String s = e.toString() + "\r\n";
			StackTraceElement[] st = e.getStackTrace();

			for (int i = 0; i < st.length; i++) {
				s += st[i].toString() + "\r\n";
			}

			throw new SQLException(s);
		}

		return ret;
	}

	// -------------------------------------------------------------------
    void execute(short executeAPI, int paramRowCount, int paramCount, Object[] paramValues,
            int queryTimeout, String sql, TrafT4Statement stmt) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", executeAPI, paramRowCount,
                    paramCount, paramValues, queryTimeout, (sql == null) ? stmt.getSQL() : sql, stmt);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}, {}, {}", executeAPI, paramRowCount, paramCount,
                    paramValues, queryTimeout, sql, stmt);
        }
		cursorName_ = stmt.cursorName_;
		rowCount_ = 0;

		int sqlAsyncEnable = (stmt.getResultSetHoldability() == TrafT4ResultSet.HOLD_CURSORS_OVER_COMMIT) ? 1 : 0;
		int inputRowCnt = paramRowCount;
		int maxRowsetSize = stmt.getMaxRows();
		String sqlString = (sql == null) ? stmt.getSQL() : sql;
		int sqlStringCharset = 1;
		int cursorNameCharset = 1;
		int stmtLabelCharset = 1;
		byte[] txId;
		ArrayList clientErrors = new ArrayList();

//3196 - NDCS transaction for SPJ	
//		if (ic_.t4props_.getSPJEnv())
//			txId = getUDRTransaction(this.ic_.getByteSwap());
//		else if (stmt.transactionToJoin != null)
//			txId = stmt.transactionToJoin;
//		else if (stmt.connection_.transactionToJoin != null)
//			txId = stmt.connection_.transactionToJoin;
//		else
//			txId = Bytes.createIntBytes(0, false); // 0 length, no data
		if (stmt.transactionToJoin != null)
			txId = stmt.transactionToJoin;
		else if (stmt.connection_.transactionToJoin != null)
			txId = stmt.connection_.transactionToJoin;
		else
			txId = Bytes.createIntBytes(0, false); // 0 length, no data

		SQL_DataValue_def inputDataValue;
		SQLValueList_def inputValueList = new SQLValueList_def();
		byte[] inputParams = null;

		if (executeAPI == TRANSPORT.SRVR_API_SQLEXECDIRECT) {
			sqlStmtType_ = getSqlStmtType(sql);
			stmt.outputDesc_ = null; // clear the output descriptors
		}

		if (stmt.usingRawRowset_ == true) {
			executeAPI = TRANSPORT.SRVR_API_SQLEXECUTE2;
			inputDataValue = new SQL_DataValue_def();
			inputDataValue.userBuffer = stmt.rowwiseRowsetBuffer_;
			inputDataValue.length = stmt.rowwiseRowsetBuffer_.limit() - 4;

			if (this.sqlQueryType_ == TRANSPORT.SQL_INSERT_RWRS) // use the param values
			{
				try {
					inputRowCnt = Integer.parseInt(paramValues[0].toString());
					maxRowsetSize = Integer.parseInt(paramValues[1].toString());
				} catch (Exception e) {
					throw new SQLException(
							"Error setting inputRowCnt and maxRowsetSize.  Parameters not set or invalid.");
				}
			} else {
				inputRowCnt = paramRowCount - 1;
			}

		} else {
			inputDataValue = fillInSQLValues2(stmt, inputRowCnt, paramCount, paramValues, clientErrors);
		}

        int affectRow;
        if (stmt.inputParamsLength_ != 0) {
            affectRow = inputDataValue.length / stmt.inputParamsLength_;
        } else {
            // when do select, stmt.inputParamsLength_ is zero
            affectRow = inputRowCnt;
        }
        if(this.pr_ != null) {
            ic_.setQid(this.pr_.getQid());
        }else{
            ic_.setQid("null");
        }
        ic_.setSqlString(sqlString);

        if (getT4props().getClipVarchar() > 0) {
            affectRow = inputRowCnt - clientErrors.size();
        }
        stmt_.getTraceLinkThreshold().setSendToMxoTimestamp(System.currentTimeMillis());
        ExecuteReply er = t4statement_.Execute(executeAPI, sqlAsyncEnable, affectRow,
            maxRowsetSize, this.sqlStmtType_, this.stmtHandle_, sqlString, sqlStringCharset,
            this.cursorName_,cursorNameCharset, stmt.stmtLabel_, stmtLabelCharset, inputDataValue,
            inputValueList, txId, stmt.usingRawRowset_, stmt.executeApiType);
        stmt_.getTraceLinkThreshold().setEnterMxoTimestamp(er.enterMxoTimestamp);
        stmt_.getTraceLinkThreshold().setEnterEngineTimestamp(er.enterEngineTimestamp);
        stmt_.getTraceLinkThreshold().setLeaveEngineTimestamp(er.leaveEngineTimestamp);
        stmt_.getTraceLinkThreshold().setLeaveMxoTimestamp(er.leaveMxoTimestamp);
        stmt_.getTraceLinkThreshold().setRecvFromMxoTimestamp(System.currentTimeMillis());
        setRetryExecute(false);
        if (stmt_.getTraceLinkThreshold().isBatchFlag()) {
            TraceLinkThreshold traceTmp = new TraceLinkThreshold();
            stmt_.doCopyLinkThreshold(stmt_.getTraceLinkThreshold(), traceTmp);
            stmt_.getTraceLinkList().add(traceTmp);
        }
        if(ic_.getDcsVersion() > 5){
            stmt_.setHeapSize(er.heapSize);
        }
        if (ic_.getAutoCommit()) {
            ic_.storeCQDSql(executeAPI, paramRowCount, paramCount, paramValues, queryTimeout, sqlString, stmt);
        }

        if (er.shouldReconn()) {
            ic_.setReconn(true);
        }
		if (executeAPI == TRANSPORT.SRVR_API_SQLEXECDIRECT) {
			this.sqlQueryType_ = er.queryType;
		}

		if (clientErrors.size() > 0) {
			if (er.errorList == null)
				er.errorList = (SQLWarningOrError[]) clientErrors.toArray(new SQLWarningOrError[clientErrors.size()]);
			else
				er.errorList = mergeErrors((SQLWarningOrError[]) clientErrors
						.toArray(new SQLWarningOrError[clientErrors.size()]), er.errorList);
		}

		stmt_.result_set_offset = 0;
		rowCount_ = er.rowsAffected;
        if (er.numResultSets == 0) {
            stmtHandle_ = er.stmtHandle;
        }
		
		int numStatus;
		
		if (getT4props().getDelayedErrorMode())
		{
			if (stmt_._lastCount > 0) {
				numStatus = stmt_._lastCount;
			}
			else {
				numStatus = inputRowCnt;
			}
		}
		else
		{
			numStatus = inputRowCnt;
		}

	    if (numStatus < 1)
	    {
	    	numStatus = 1;
	    }

	    stmt.batchRowCount_ = new int[numStatus];
	    boolean batchException = false;	//3164

	    if (getT4props().getDelayedErrorMode() && stmt_._lastCount < 1) {
			Arrays.fill(stmt.batchRowCount_, -2); // fill with success
	    } 
	    else if (er.returnCode == TRANSPORT.SQL_SUCCESS || er.returnCode == TRANSPORT.SQL_SUCCESS_WITH_INFO
				|| er.returnCode == TRANSPORT.NO_DATA_FOUND) {
			Arrays.fill(stmt.batchRowCount_, -2); // fill with success

			// if we had errors with valid rowIds, update the array
			if (er.errorList != null)
			{
				for (int i = 0; i < er.errorList.length; i++) {
					int row = er.errorList[i].rowId - 1;
					if (row >= 0 && row < stmt.batchRowCount_.length) {
						stmt.batchRowCount_[row] = -3;
						batchException = true;	//3164
					}
				}
			}

            // Sometimes users may set schema through stmt.exec("set schema xx") instead of
            // conn.setSchema("xx"), so there need to update local schema when it success
            if (this.sqlQueryType_ == TRANSPORT.SQL_SET_SCHEMA) {
                String schema = extractSchema(sqlString);
                ic_.setSchemaDirect(schema);
            }

			// set the statement label if we didnt get one back.
			if (er.stmtLabels == null || er.stmtLabels.length == 0) {
				er.stmtLabels = new String[1];
				er.stmtLabels[0] = stmt.stmtLabel_;
			}

			// get the descriptors from the proper location
			TrafT4Desc[][] desc = null;

			// try from execute data first
			if (er.outputDesc != null && er.outputDesc.length > 0) {
				desc = new TrafT4Desc[er.outputDesc.length][];

                for (int i = 0; i < er.outputDesc.length; i++) {
					desc[i] = InterfaceStatement.NewDescArray(er.outputDesc[i]);
				}
			}
			// try from the prepare data
			else if (stmt.outputDesc_ != null && stmt.outputDesc_.length > 0) {
				desc = new TrafT4Desc[1][];
				desc[0] = stmt.outputDesc_;
			}

			if (sqlQueryType_ == TRANSPORT.SQL_CALL_NO_RESULT_SETS ||
					sqlQueryType_ == TRANSPORT.SQL_CALL_WITH_RESULT_SETS) {
				TrafT4CallableStatement cstmt = (TrafT4CallableStatement) stmt;
				Object[] outputValueArray;
				if(er.returnCode == TRANSPORT.NO_DATA_FOUND) { //this should really only happen with LAST0 specified
					if (null != cstmt.outputDesc_) {
					    outputValueArray = new Object[cstmt.outputDesc_.length];
					} else {
					    outputValueArray = null;
					}
				}
				else {
					outputValueArray = InterfaceResultSet.getExecute2Outputs(cstmt.connection_, cstmt.outputDesc_, 
						er.outValues, this.ic_.getByteSwap());
				}
				
				cstmt.setExecuteCallOutputs(outputValueArray, (short) er.rowsAffected);
				stmt.setMultipleResultSets(er.numResultSets, desc, er.stmtLabels, er.proxySyntax);
			} else {
				// fix until we start returning numResultsets for more than just
				// SPJs
				if (desc != null && desc.length > 0 && er.numResultSets == 0) {
					er.numResultSets = 1;
				}

                if (er.outValues != null && er.outValues.length > 0) {
                    stmt.setExecute2Outputs(er.outValues, (short) er.rowsAffected, er.rowsAffected < TrafT4ResultSet.DEFAULT_FETCH_SIZE ? true : false, er.proxySyntax, desc[0]);
                } else {
                    if (ic_.getMajorVersion() >= 13 && er.endOfData && desc != null) {
                        stmt.setExecute2Outputs(er.outValues, (short) er.rowsAffected, true, er.proxySyntax, desc[0]);
                    } else {
                        stmt.setMultipleResultSets(er.numResultSets, desc, er.stmtLabels, er.proxySyntax);
                    }
                }

			}
			if (er.errorList != null) {
				TrafT4Messages.setSQLWarning(getT4props(), stmt, er.errorList);
			}
		} else {
			Arrays.fill(stmt.batchRowCount_, -3); // fill with failed
			TrafT4Messages.throwSQLException(getT4props(), stmt, er.errorList);
		}
	    //3164
	    if (batchException) {
            TrafT4Messages.throwSQLException(getT4props(), stmt, er.errorList);
	    }
        if (executeAPI == TRANSPORT.SRVR_API_SQLEXECUTE2 && er.rowsAffected > 0 && er.rowsAffected < TrafT4ResultSet.DEFAULT_FETCH_SIZE) {
            stmt.getTraceLinkThreshold().setEndOfData(true);
        }
	    //if not select, reconn
        if (ic_.getAutoCommit()) {
            if (this.sqlQueryType_ == TRANSPORT.SQL_SELECT_UNIQUE || this.sqlQueryType_ == TRANSPORT.SQL_SELECT_NON_UNIQUE) {
                if (er.rowsAffected < TrafT4ResultSet.DEFAULT_FETCH_SIZE && er.outValues != null && er.outValues.length > 0) {
                    ic_.reconnIfNeeded();
                }
            }else {
                ic_.reconnIfNeeded();
            }
        }
	    
    }

	private String extractSchema(String sqlString) {
		String schemaRegex = "(SET)\\s+(SCHEMA)\\s+([a-zA-Z0-9]+\\s*\\.)\\s*([a-zA-Z0-9]+)\\s*";
		Pattern pattern = Pattern.compile(schemaRegex);
		Matcher m = pattern.matcher(sqlString.toUpperCase());
		while (m.find()) {
			return m.group(m.groupCount());
		}
		return "";
    	}

    protected void setQueryTimeout(int queryTimeout) {
        this.t4statement_.setQueryTimeout(queryTimeout);
    }

    protected T4Statement getT4statement() {
        return t4statement_;
    }

    protected InterfaceConnection getInterfaceConnection() {
        return ic_;
    }

    protected long getPrepareTimeMills() {
        return prepareTimeMills;
    }

    protected int getSqlStmtType() {
        return sqlStmtType_;
    }

    protected int getStmtHandle() {
        return stmtHandle_;
    }

    protected String getStmtLabel() {
        return stmtLabel_;
    }

    protected int getSqlQueryType() {
        return sqlQueryType_;
    }

    protected boolean isCachedPreparedStmtFull() {
        return cachedPreparedStmtFull;
    }

    protected void setCachedPreparedStmtFull(boolean cachedPreparedStmtFull) {
        this.cachedPreparedStmtFull = cachedPreparedStmtFull;
    }

    protected boolean isPreparedStmtUseESP() {
        return preparedStmtUseESP;
    }

    protected void setPreparedStmtUseESP(boolean use) {
        this.preparedStmtUseESP = use;
    }

    protected boolean isPstmsAdd() {
        return isPstmsAdd;
    }

    protected void setPstmsAdd(boolean pstmsAdd) {
        isPstmsAdd = pstmsAdd;
    }

    public void setRetryExecute(boolean retryExecute) {
        this.isRetryExecute = retryExecute;
    }
} // end class InterfaceStatement
