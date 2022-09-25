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
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
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
import java.util.Calendar;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;

public class TrafT4MultiQueriesPreparedStatement extends TrafT4PreparedStatement {

    private String[] sqlArr = null;
    private TrafT4PreparedStatement[] pstmtArr = null;
    private int[][] paramDescs = null;

    private int currentSqlIndex;
    private final static org.slf4j.Logger LOG =
            LoggerFactory.getLogger(TrafT4MultiQueriesPreparedStatement.class);

    TrafT4MultiQueriesPreparedStatement(TrafT4Connection connection, String sql) throws SQLException {
        this(connection, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                TrafT4ResultSet.CLOSE_CURSORS_AT_COMMIT, null);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", connection, sql);
        }
    }

    TrafT4MultiQueriesPreparedStatement(TrafT4Connection connection, String sql, String stmtLabel) throws SQLException {
        this(connection, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                TrafT4ResultSet.CLOSE_CURSORS_AT_COMMIT, stmtLabel);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, sql, stmtLabel);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", connection, sql, stmtLabel);
        }
    }

    TrafT4MultiQueriesPreparedStatement(TrafT4Connection connection, String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        this(connection, sql, resultSetType, resultSetConcurrency, connection.holdability_, null);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, sql, resultSetType,
                    resultSetConcurrency);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", connection, sql, resultSetType,
                    resultSetConcurrency);
        }
    }

    TrafT4MultiQueriesPreparedStatement(TrafT4Connection connection, String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        this(connection, sql, resultSetType, resultSetConcurrency, resultSetHoldability, null);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}", connection, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        }
    }

    TrafT4MultiQueriesPreparedStatement(TrafT4Connection connection, String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability, String stmtLabel) throws SQLException {
        super(connection, sql, resultSetType, resultSetConcurrency, resultSetHoldability, stmtLabel);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connection, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability, stmtLabel);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}, {}", connection, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability, stmtLabel);
        }
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY && resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE
                && resultSetType != ResultSet.TYPE_SCROLL_SENSITIVE) {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_resultset_type");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY && resultSetConcurrency != ResultSet.CONCUR_UPDATABLE) {
            throw TrafT4Messages.createSQLException(getT4props(),
                    "invalid_resultset_concurrency");
        }
        if ((resultSetHoldability != 0) && (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
                && (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_holdability");
        }

        sqlArr = sql.split(";");
        pstmtArr = new TrafT4PreparedStatement[sqlArr.length];
        paramDescs = new int[sqlArr.length][3];
        int total = 0;
        int currentParamsLen = 0;
        for (int i = 0; i < sqlArr.length; i++) {
            pstmtArr[i] = new TrafT4PreparedStatement(connection, sqlArr[i], resultSetType, resultSetConcurrency,
                    resultSetHoldability, stmtLabel);
            pstmtArr[i].prepare(sqlArr[i], queryTimeout_, resultSetHoldability_);

            pstmtArr[i].multiQueriesStmt = this;
            // this is to setup the paramDescs, each paramDesc has 3 column,
            // 1: index of preparedStatement
            // 2: num of params for current pstmt
            // 3: accumulated params for current index
            // this varible is used when there invoking setXXX api
            if (pstmtArr[i].inputDesc_ != null) {
                currentParamsLen = pstmtArr[i].inputDesc_.length;
                total += currentParamsLen;
            }
            int[] paramDesc = { i, currentParamsLen, total };
            paramDescs[i] = paramDesc;

        }
    }

    // this method will return a 2 column array.
    // first column means the index of preparedStatement
    // second column means the real parameter index of the column one preparedStatement
    // eg:
    // insert into tbl values (?,?);insert into tbl values (?,?,?);insert into tbl values (?,?)
    // the paramDescs is {0,2,2},{1,3,5},{2,2,7} , when use setXXX(6, x);
    // there will return {2,1} , means set param for the 1st param of the 3rd sql.
    private int[] getCurrentParamIndex(int paramIndex) {
        if (paramIndex < 1 || paramIndex > paramDescs[paramDescs.length - 1][2]) {
            return new int[] { 0, paramIndex };
        }
        for (int i = 0; i < paramDescs.length; i++) {
            if (paramIndex > paramDescs[i][2]) {
                continue;
            } else {
                int index = paramIndex - (paramDescs[i][2] - paramDescs[i][1]);
                return new int[] { i, index };
            }
        }

        return new int[] { 0, paramIndex };

    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setArray(paramIndex[1], x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setAsciiStream(paramIndex[1], x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setAsciiStream(paramIndex[1], x, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setAsciiStream(paramIndex[1], x, length);
    };

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setBigDecimal(paramIndex[1], x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setBinaryStream(paramIndex[1], x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setBinaryStream(paramIndex[1], x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setBinaryStream(paramIndex[1], x, length);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setBlob(paramIndex[1], x);
    }

    public void setBlob(int parameterIndex, InputStream x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setBlob(paramIndex[1], x);
    }

    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setBlob(paramIndex[1], x, length);
    };

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setBoolean(paramIndex[1], x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setByte(paramIndex[1], x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setBytes(paramIndex[1], x);
    }

    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setCharacterStream(paramIndex[1], x);
    }

    public void setCharacterStream(int parameterIndex, Reader x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setCharacterStream(paramIndex[1], x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setCharacterStream(paramIndex[1], x, length);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setClob(paramIndex[1], x);
    }

    public void setClob(int parameterIndex, Reader x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setClob(paramIndex[1], x);
    }

    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setClob(paramIndex[1], x, length);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setDate(paramIndex[1], x);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, cal);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setDate(paramIndex[1], x, cal);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setDouble(paramIndex[1], x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setFloat(paramIndex[1], x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setInt(paramIndex[1], x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setLong(paramIndex[1], x);
    }

    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setNCharacterStream(paramIndex[1], x);
    }

    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setNCharacterStream(paramIndex[1], x, length);
    }

    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setNClob(paramIndex[1], x);
    }

    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setNClob(paramIndex[1], x);
    }

    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setNClob(paramIndex[1], x, length);
    }

    public void setNString(int parameterIndex, String x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setNString(paramIndex[1], x);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, sqlType);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, sqlType);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setNull(paramIndex[1], sqlType);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, sqlType, typeName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, sqlType, typeName);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setNull(paramIndex[1], sqlType, typeName);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setObject(paramIndex[1], x);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, targetSqlType);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, targetSqlType);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setObject(paramIndex[1], x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, targetSqlType, scale);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}", parameterIndex, x, targetSqlType, scale);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setObject(paramIndex[1], x, targetSqlType, scale);
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setRef(paramIndex[1], x);
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setRowId(paramIndex[1], x);
    }

    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setSQLXML(paramIndex[1], x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setShort(paramIndex[1], x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setString(paramIndex[1], x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setTime(paramIndex[1], x);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, cal);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setTime(paramIndex[1], x, cal);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setTimestamp(paramIndex[1], x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, cal);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, cal);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setTimestamp(paramIndex[1], x, cal);
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}", parameterIndex, x, length);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setUnicodeStream(paramIndex[1], x, length);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", parameterIndex, x);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", parameterIndex, x);
        }
        int[] paramIndex = getCurrentParamIndex(parameterIndex);
        pstmtArr[paramIndex[0]].setURL(paramIndex[1], x);
    }

    public void addBatch() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        for (int i = 0; i < pstmtArr.length; i++) {
            pstmtArr[i].addBatch();
        }
    }

    public int[] executeBatch() throws SQLException, BatchUpdateException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        currentSqlIndex = 0;

        int[] results = null;
        for (int i = 0; i < pstmtArr.length; i++) {
            int[] result = pstmtArr[i].executeBatch();
            if (results == null) {
                results = result;
            } else {
                int[] tmp = new int[results.length + result.length];
                System.arraycopy(results, 0, tmp, 0, results.length);
                System.arraycopy(result, 0, tmp, results.length, result.length);
                results = tmp;
            }
        }

        return results;
    }

    public int getUpdateCount() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        long result = 0;
        currentSqlIndex = 0;

        for (int i = 0; i < pstmtArr.length; i++) {
            result += pstmtArr[i].getUpdateCount();
        }
        return (int) result;
    }

    public ResultSet executeQuery() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        currentSqlIndex = 0;

        for (int i = 0; i < pstmtArr.length; i++) {
            pstmtArr[i].executeQuery();
        }
        return getResultSet();
    }

    public boolean execute() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        boolean result = false;
        currentSqlIndex = 0;

        for (int i = 0; i < pstmtArr.length; i++) {
            result |= pstmtArr[i].execute();
        }
        return result;
    }

    public int executeUpdate() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        long result = 0;
        currentSqlIndex = 0;

        for (int i = 0; i < pstmtArr.length; i++) {
            result += pstmtArr[i].executeUpdate();
        }
        return (int) result;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        return pstmtArr[0].getMetaData();
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        return pstmtArr[0].getParameterMetaData();
    }

    public ResultSet getResultSet() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        return pstmtArr[currentSqlIndex].getResultSet();
    }

    public boolean getMoreResults() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (++currentSqlIndex >= sqlArr.length) {
            return false;
        }
        return true;
    }

    public void close() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (pstmtArr == null) {
            return;
        }
        SQLException[] eArr = new SQLException[pstmtArr.length];
        boolean hasException = false;
        int exceptionNum = 0;
        for (int i = 0; i < pstmtArr.length; i++) {
            try {
                pstmtArr[i].close();
            } catch (SQLException e) {
                hasException = true;
                exceptionNum++;
                eArr[i] = e;
            }
        }
        if (hasException) {
            if (exceptionNum > 1) {
                TrafT4Exception t4e = TrafT4Messages.createSQLException(getT4props(),
                        "prepared_statement_close_failed");
                SQLException sqlE = t4e;
                SQLException tmpE = null;
                for (int i = 0; i < eArr.length; i++) {
                    if (eArr[i] != null) {
                        if (tmpE == null) {
                            sqlE.setNextException(eArr[i]);
                            tmpE = eArr[i];
                        } else {
                            tmpE.setNextException(eArr[i]);
                            tmpE = eArr[i];
                        }
                    }
                }
                throw sqlE;
            } else {
                for (int i = 0; i < eArr.length; i++) {
                    if (eArr[i] != null) {
                        throw eArr[i];
                    }
                }
            }
        }
    }

    public void cancel() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (pstmtArr == null) {
            return;
        }
        SQLException[] eArr = new SQLException[pstmtArr.length];
        boolean hasException = false;
        int exceptionNum = 0;
        for (int i = 0; i < pstmtArr.length; i++) {
            try {
                pstmtArr[i].cancel();
            } catch (SQLException e) {
                hasException = true;
                exceptionNum++;
                eArr[i] = e;
            }
        }

        if (hasException) {
            if (exceptionNum > 1) {
                TrafT4Exception t4e = TrafT4Messages.createSQLException(getT4props(),
                        "prepared_statement_cancel_failed");
                SQLException sqlE = t4e;
                SQLException tmpE = null;
                for (int i = 0; i < eArr.length; i++) {
                    if (eArr[i] != null) {
                        if (tmpE == null) {
                            sqlE.setNextException(eArr[i]);
                            tmpE = eArr[i];
                        } else {
                            tmpE.setNextException(eArr[i]);
                            tmpE = eArr[i];
                        }
                    }
                }
                throw sqlE;
            } else {
                for (int i = 0; i < eArr.length; i++) {
                    if (eArr[i] != null) {
                        throw eArr[i];
                    }
                }
            }
        }
    }

    public void setMaxRows(int max) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", max);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", max);
        }
        if (pstmtArr == null) {
            super.setMaxRows(max);
            return;
        }
        for (int i = 0; i < this.sqlArr.length; i++) {
            this.pstmtArr[i].setMaxRows(max);
        }
    }

    public int getMaxRows() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (pstmtArr == null) {
            return super.getMaxRows();
        }
        return this.pstmtArr[currentSqlIndex].getMaxRows();
    }

    public void setCursorName(String name) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", name);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", name);
        }
        if (pstmtArr == null) {
            super.setCursorName(name);
            return;
        }
        for (int i = 0; i < this.sqlArr.length; i++) {
            this.pstmtArr[i].setCursorName(name);
        }
    }

    public void setFetchDirection(int direction) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", direction);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", direction);
        }
        if (pstmtArr == null) {
            super.setFetchDirection(direction);
            return;
        }
        for (int i = 0; i < this.sqlArr.length; i++) {
            this.pstmtArr[i].setFetchDirection(direction);
        }
    }

    public int getFetchDirection() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (pstmtArr == null) {
            return super.getFetchDirection();
        }
        return this.pstmtArr[currentSqlIndex].getFetchDirection();
    }

    public int getFetchSize() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (pstmtArr == null) {
            return super.getFetchSize();
        }
        return this.pstmtArr[currentSqlIndex].getFetchSize();
    }

    public void setFetchSize(int rows) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", rows);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", rows);
        }
        if (pstmtArr == null) {
            super.setFetchSize(rows);
            return;
        }
        for (int i = 0; i < this.sqlArr.length; i++) {
            this.pstmtArr[i].setFetchSize(rows);
        }
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", enable);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", enable);
        }
        if (pstmtArr == null) {
            super.setEscapeProcessing(enable);
            return;
        }
        for (int i = 0; i < this.sqlArr.length; i++) {
            this.pstmtArr[i].setEscapeProcessing(enable);
        }
    }

    public int getQueryTimeout() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (pstmtArr == null) {
            return super.getQueryTimeout();
        }
        return this.pstmtArr[currentSqlIndex].getQueryTimeout();
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", seconds);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", seconds);
        }
        if (pstmtArr == null) {
            super.setQueryTimeout(seconds);
            return;
        }
        for (int i = 0; i < this.sqlArr.length; i++) {
            this.pstmtArr[i].setQueryTimeout(seconds);
        }
    }

    public int getMaxFieldSize() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (pstmtArr == null) {
            return super.getMaxFieldSize();
        }
        return this.pstmtArr[currentSqlIndex].getMaxFieldSize();
    }

    public void setMaxFieldSize(int max) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", max);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", max);
        }
        if (pstmtArr == null) {
            super.setMaxFieldSize(max);
            return;
        }
        for (int i = 0; i < this.sqlArr.length; i++) {
            this.pstmtArr[i].setMaxFieldSize(max);
        }
    }

    public void addBatch(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "addBatch_not_supported");
    }

    public boolean execute(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "execute_not_supported");
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "execute_2_not_supported");
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "execute_3_not_supported");
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "execute_4_not_supported");
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "executeQuery_not_supported");
    }

    public int executeUpdate(String sql) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "executeUpdate_not_supported");
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "executeUpdate_2_not_supported");
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "executeUpdate_3_not_supported");
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sql);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", sql);
        }
        throw TrafT4Messages.createSQLException(getT4props(),
                "executeUpdate_4_not_supported");
    }

}
