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
import java.io.OutputStream;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;

public abstract class TrafT4Lob {

    protected enum SetDataType {
        empty, stringOrBytes, stream, write, lob;
    }

    static final int LOB_V1_EXT_HANDLE_LEN = 1024;
    static final int LOB_V2_EXT_HANDLE_LEN = 96;

    protected TrafT4Connection conn;
    protected TrafT4Statement stmt;
    protected TrafT4InputStream inputStream;// rs.get || clob
    protected TrafT4OutputStream outputStream;// exec
    protected SetDataType setDataType;
    protected long lobLen = -1;

    private String lobHandle;
    private boolean freed = false;
    private int lobType;
    private int lobChunkMaxLen;
    private int lobInlineMaxLen;
    private long lobTableUid = -1;
    private int lobVersion;
    private int stmtHandle;// optimze for one prepare+ multi exec
    private String inlineDataString;
    private byte[] inlineDataBytes;
    private boolean lobWithExternalData;

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4Lob.class);
    /**
     * for create lob when rs.getXXX
     * @throws SQLException
     */
    public TrafT4Lob(Statement statement, String lobHandle, int type, TrafT4Desc colDesc)
            throws SQLException {
        this.conn = (TrafT4Connection) statement.getConnection();
        this.stmt = (TrafT4Statement) statement;
        this.lobType = type;
        this.lobChunkMaxLen = colDesc.lobChunkMaxLen_;
        this.lobInlineMaxLen = colDesc.lobInlineMaxLen_;
        this.lobVersion = colDesc.lobVersion_;
        this.stmtHandle = stmt.ist_.getStmtHandle();
        this.lobWithExternalData = false;
        setLobHandleByInlineData(lobHandle);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", lobHandle, colDesc, stmtHandle);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}.", lobHandle, colDesc, stmtHandle);
        }
    }

    /**
     * for create lob when pstmt.setXXX
     * @throws SQLException
     */
    public TrafT4Lob(Statement statement, int type, TrafT4Desc colDesc) throws SQLException {
        this.conn = (TrafT4Connection) statement.getConnection();
        this.stmt = (TrafT4Statement) statement;
        this.lobType = type;
        this.lobChunkMaxLen = colDesc.lobChunkMaxLen_;
        this.lobInlineMaxLen = colDesc.lobInlineMaxLen_;
        this.lobVersion = colDesc.lobVersion_;
        this.stmtHandle = stmt.ist_.getStmtHandle();
        this.lobWithExternalData = true;
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", colDesc, stmtHandle);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}.", colDesc, stmtHandle);
        }
    }

    /**
     * for create lob by connection
     * @throws SQLException
     */
    public TrafT4Lob(TrafT4Connection connection, int type) throws SQLException {
        this.conn = connection;
        this.lobType = type;
        this.lobWithExternalData = true;
        // for conn.createClob(), need to add correlation when pstmt.setClob
        this.setDataType = SetDataType.empty;
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
    }

    protected long getLobTableUid() {
        if (lobTableUid == -1) {
            String tableUidStr = lobHandle.substring(16, 36);
            try {
                lobTableUid = Long.parseLong(tableUidStr);
            } catch (NumberFormatException ne) {
            }
        }
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE, lobTableUid="+lobTableUid);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. lobTableUid={} ", lobTableUid);
        }
        return lobTableUid;
    }

    protected long getLobLen() throws TrafT4Exception {
        if (lobWithExternalData) {
            return -1; // a lob create at ps.setXXX does not have a lob len, it only has value len
        }
        if (lobLen == -1 && lobHandle != null) {
            try {
                long extractLen = -1; // -1 means get lob length instead of get lob datas
                T4Connection t4connection = conn.getServerHandle().getT4Connection();
                LogicalByteArray wbuffer = ExtractLobMessage.marshal(stmtHandle, (short) 0, lobHandle, 1,
                        extractLen, conn.getServerHandle());

                LogicalByteArray rbuffer = t4connection.getReadBuffer(TRANSPORT.SRVR_API_EXTRACTLOB, wbuffer);
                ExtractLobReply reply = new ExtractLobReply(rbuffer, conn.getServerHandle());
                lobLen = reply.lobLength;
            } catch (SQLException e) {
                throw TrafT4Messages.createSQLException(getT4props(), "lob_io_error", e.getMessage());
            }
        }
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE, lobLen="+lobLen);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. lobLen={} ", lobLen);
        }
        return lobLen;
    }

    protected T4Properties getT4props() {
        return conn.getT4props();
    }

    protected void throwIfPosOutOfBound(long pos) throws TrafT4Exception {
        long len = 0L;
        try {
            len = length();
        } catch (SQLException e) {
        }
        if (pos < 1 || pos > len + 1)
            throw TrafT4Messages.createSQLException(conn.getT4props(), "invalid_position_value");
    }
    ExtractLobReply readChunk(int extractMode, long readLen) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", readLen);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. readLen={} ", readLen);
        }
        try {
            //long extractLen = getLobChunkMaxLen();
            T4Connection t4connection = conn.getServerHandle().getT4Connection();
            LogicalByteArray wbuffer = ExtractLobMessage.marshal(stmtHandle, (short) extractMode, lobHandle, 1,
                    readLen, conn.getServerHandle());

            LogicalByteArray rbuffer = t4connection.getReadBuffer(TRANSPORT.SRVR_API_EXTRACTLOB, wbuffer);
            ExtractLobReply reply = new ExtractLobReply(rbuffer, conn.getServerHandle());
            return reply;
        } finally {
        }
    }

    void writeChunk(byte[] chunk, int off, int writeLength, long pos) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", off, writeLength, pos);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}.", off, writeLength, pos);
        }
        T4Connection t4connection = conn.getServerHandle().getT4Connection();

        LogicalByteArray wbuffer = UpdateLobMessage.marshal((short) 1, lobHandle, writeLength, off, chunk, pos,
                writeLength, stmtHandle, conn.getServerHandle());

        LogicalByteArray rbuffer = t4connection.getReadBuffer(TRANSPORT.SRVR_API_UPDATELOB, wbuffer);
        UpdateLobReply ur = new UpdateLobReply(rbuffer, conn.getServerHandle());
        switch (ur.m_p1.exception_nr) {
            case TRANSPORT.CEE_SUCCESS:
                if (getT4props().isLogEnable(Level.FINER)) {
                    T4LoggingUtilities.log(getT4props(), Level.FINER, "SUCCESS");
                }
                break;
            default:
                if (getT4props().isLogEnable(Level.FINER)) {
                    T4LoggingUtilities.log(getT4props(), Level.FINER, "case ur.m_p1.exception_nr deafult");
                }
                throw TrafT4Messages.createSQLException(getT4props(), "lob_io_error");
        }
    }

    protected InputStream getInputStream(long pos, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pos, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}.", pos, length);
        }
        try {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                } finally {
                    inputStream = null;
                }
            }
            inputStream = new TrafT4InputStream(this, pos, length);
            return inputStream;

        } finally {
        }
    }

    protected String getLobHandle() {
        return lobHandle;
    }

    protected int getLobChunkMaxLen() {
        return lobChunkMaxLen;
    }

    protected int getLobInlineMaxLen() {
        return lobInlineMaxLen;
    }

    protected int getLobVersion() {
        return lobVersion;
    }

    protected int getLobType() {
        return lobType;
    }

    protected String getInlineDataString() {
        return inlineDataString;
    }

    protected byte[] getInlineDataBytes() {
        return inlineDataBytes;
    }

    public boolean isLobWithExternalData() {
        return lobWithExternalData;
    }

    protected void setLobHandle(String lobHandle) {
        this.lobHandle = lobHandle;
    }

    protected void setLobHandleByInlineData(String inlineData) {
        if (inlineData == null)
            return;
        if (lobVersion == 1)
            lobHandle = inlineData;
        else if (inlineData.length() <= LOB_V2_EXT_HANDLE_LEN)
            lobHandle = inlineData;
        else {
            lobHandle = inlineData.substring(0, LOB_V2_EXT_HANDLE_LEN);
            inlineDataString = inlineData.substring(LOB_V2_EXT_HANDLE_LEN);
            lobLen = Long.parseLong(inlineData.substring(36, 46));
            // adjust lobLen when it is clob
            if (this instanceof TrafT4Clob) {
                lobLen = inlineDataString.length();
            }
        }
    }

    protected void setLobHandleByInlineData(byte[] inlineData) {
        if (inlineData == null)
            return;
        if (lobVersion == 1)
            lobHandle = new String(inlineData);
        else if (inlineData.length <= LOB_V2_EXT_HANDLE_LEN)
            lobHandle = new String(inlineData);
        else {
            lobHandle = new String(inlineData, 0, LOB_V2_EXT_HANDLE_LEN);
            inlineDataBytes =
                    Arrays.copyOfRange(inlineData, LOB_V2_EXT_HANDLE_LEN, inlineData.length);
            lobLen = Long.parseLong(lobHandle.substring(36, 46));
        }
    }

    protected abstract void populate() throws SQLException;


    static String convSQLExceptionToIO(SQLException e) {
        try {
            SQLException e1;
            e1 = e;
            StringBuffer s = new StringBuffer(1000);
            do {
                s.append("SQLState :");
                s.append(e1.getSQLState());
                s.append(" ErrorCode :");
                s.append(e1.getErrorCode());
                s.append(" Message:");
                s.append(e1.getMessage());
            } while ((e1 = e1.getNextException()) != null);
            return s.toString();
        } finally {
        }
    }

    protected boolean isFreed() {
        return freed;
    }

    /*
     * After free has been called, any attempt to invoke a method other than free will result in a
     * SQLException being thrown.
     */
    protected void testAvailability() throws SQLException {
        if (freed)
            throw TrafT4Messages.createSQLException(getT4props(), "lob_has_been_freed");
    }

    public abstract long length() throws SQLException;

    public void free() throws SQLException {
        if (isFreed()) {
            return;
        }
        this.freed = true;
        this.setDataType = SetDataType.empty;
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
            } finally {
                inputStream = null;
            }
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
            } finally {
                inputStream = null;
            }
        }
    }

    protected OutputStream setOutputStream(long pos) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pos);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", pos);
        }
        throwIfPosOutOfBound(pos);

        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        outputStream = new TrafT4OutputStream(pos, this);
        return outputStream;
    }

    protected void close() throws IOException, SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (inputStream != null) {
            inputStream.close();
        }
        if (outputStream != null) {
            outputStream.close();
        }

        try {
            //long extractLen = getLobChunkMaxLen();
            T4Connection t4connection = conn.getServerHandle().getT4Connection();
            LogicalByteArray wbuffer = ExtractLobMessage.marshal(stmtHandle, (short) 2, lobHandle, 1, 0, conn.getServerHandle());

            LogicalByteArray rbuffer = t4connection.getReadBuffer(TRANSPORT.SRVR_API_EXTRACTLOB, wbuffer);
            ExtractLobReply reply = new ExtractLobReply(rbuffer, conn.getServerHandle());
        } finally {
        }
    }

    protected int getStmtHandle() {
        return stmtHandle;
    }

    protected void setStmtHandle(int stmtHandle) {
        this.stmtHandle = stmtHandle;
    }

    protected void copyByLob(TrafT4Lob another) {
        this.lobHandle = another.getLobHandle();
        this.stmtHandle = another.getStmtHandle();
        this.lobChunkMaxLen = another.getLobChunkMaxLen();
        this.lobInlineMaxLen = another.getLobChunkMaxLen();
        this.lobVersion = another.getLobVersion();
        this.lobTableUid = another.getLobTableUid();
    }
}
