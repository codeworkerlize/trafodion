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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;


public class TrafT4Blob extends TrafT4Lob implements Blob {
    private InputStream inputDatas;// pstmt.setXXXStream()
    private byte[] datas;// pstmt.setBytes
    private Blob inBlob;
    private long length;
    private int offset;
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4Blob.class);

    /**
     * conn.createBlob
     * @throws SQLException
     */
    public TrafT4Blob(TrafT4Connection connection) throws SQLException {
        super(connection, Types.BLOB);
    }

    /**
     * for rs.getXXX
     * @throws SQLException
     */
    public TrafT4Blob(TrafT4Statement statement, String lobHandle, TrafT4Desc colDesc) throws SQLException {
        super(statement, lobHandle, Types.BLOB, colDesc);
    }

    /**
     * for ps.setXXX
     * @throws SQLException
     */
    public TrafT4Blob(TrafT4Statement statement, TrafT4Desc colDesc, InputStream inputStream, long length)
            throws SQLException {
        super(statement, Types.BLOB, colDesc);
        this.inputDatas = inputStream;
        this.length = length;
        super.setDataType = SetDataType.stream;
    }

    /**
     * for ps.setXXX
     * @throws SQLException
     */
    public TrafT4Blob(TrafT4Statement statement, TrafT4Desc colDesc, byte[] datas, long length) throws SQLException {
        super(statement, Types.BLOB, colDesc);
        this.datas = datas;
        this.length = length;
        super.setDataType = SetDataType.stringOrBytes;
    }

    /**
     * for ps.setXXX
     * @throws SQLException
     */
    public TrafT4Blob(TrafT4Statement statement, TrafT4Desc colDesc, Blob inBlob) throws SQLException {
        super(statement, Types.BLOB, colDesc);
        this.inBlob = inBlob;
        super.setDataType = SetDataType.lob;
    }

    public InputStream getBinaryStream() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        return getBinaryStream(1, length());
    }


    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY, setDataType=" + setDataType, pos, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}.",setDataType, pos, length);
        }
        testAvailability();

        throwIfPosOutOfBound(pos);
        if (isLobWithExternalData()) {
            switch (setDataType) {
                case stream:
                    if (inputDatas != null) {
                        pos--;
                        if (pos > 0) {
                            try {
                                inputDatas.skip(pos);
                            } catch (IOException e) {
                            }
                        }
                        return inputDatas;
                    }
                    break;
                case stringOrBytes:
                    if (datas != null) {
                        int tmpLen = (int) (length > this.length ? this.length : length);
                        ByteArrayInputStream in = new ByteArrayInputStream(datas, 0, tmpLen);
                        pos--;
                        if (pos > 0) {
                            in.skip(pos);
                        }
                        return in;
                    }
                    break;
                case lob:
                    if (inBlob != null) {
                        return inBlob.getBinaryStream(pos, length);
                    }
                    break;
                default:
                    break;
            }
            return null;
        }
        return getInputStream(pos, length);
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pos, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}.", pos, length);
        }
        testAvailability();

        if (getLobHandle() == null) {
            return null;
        }

        throwIfPosOutOfBound(pos);
        InputStream is = getInputStream(pos, length);
        try {
            byte[] buf = new byte[length];
            int retLen = is.read(buf, 0, length);
            if (retLen < length && retLen > 0)
                buf = Arrays.copyOf(buf, retLen);
            return buf;
        } catch (IOException ioe) {
            throw new SQLException(ioe);
        }
    }

    public OutputStream setBinaryStream(long pos) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pos);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}.", pos);
        }
        testAvailability();
        // Close the writer and OutputStream hander over earlier
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
            } finally {
                outputStream = null;
            }
        }
        return setOutputStream(pos);
    }

    public int setBytes(long pos, byte[] bytes) throws SQLException {

        return setBytes(pos, bytes, 0, bytes.length);
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pos, offset, len);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}.", pos, offset, len);
        }
        testAvailability();
        try {
            if (pos < 0 || len < 0 || offset < 0 || bytes == null) {
                throw TrafT4Messages.createSQLException(getT4props(), "invalid_input_value",
                        "Blob.setBytes(long, byte[], int, int)");
            }
            if (pos > 1)
                TrafT4Messages.throwUnsupportedFeatureException(getT4props(),
                        "Blob.setBytes with position > 1 is not supported");
            this.setDataType = SetDataType.stringOrBytes;
            if(getT4props().isLobCopySrc()) {
                // deep copy the input bytes, to make sure the inner bytes and the input bytes
                // will have no effect for each other.
                this.datas = new byte[bytes.length];
                System.arraycopy(bytes, 0, datas, 0, bytes.length);
            } else {
                this.datas = bytes;
            }
            this.length = len;
            this.offset = offset;
            return len;
        } finally {
        }
    }

    public long position(Blob pattern, long start) throws SQLException {
        TrafT4Messages.throwUnsupportedMethodException(getT4props(), "TrafT4Blob", "position(byte[], long)");
        testAvailability();
        return position(pattern.getBytes(0, (int) pattern.length()), start);
    }

    public long position(byte[] pattern, long start) throws SQLException {
        TrafT4Messages.throwUnsupportedMethodException(getT4props(), "TrafT4Blob", "position(byte[], long)");
        testAvailability();
        return 0L;
    }

    public void truncate(long len) throws SQLException {
        TrafT4Messages.throwUnsupportedMethodException(getT4props(), "TrafT4Blob", "truncate(long)");
        testAvailability();
    }

    public long length() throws SQLException {
        testAvailability();
        return getLobLen() == -1 ? length : getLobLen();
    }

    // This function populates the Blob data from one of the following:
    // 1. InputStream set in PreparedStatement.setBinaryStream
    // 2. From another blob set in PreparedStatement.setBlob or ResultSet.updateBlob
    // This function is called at the time of PreparedStatement.executeUpdate, execute and 
    // executeBatch
    protected void populate() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY. setDataType=" + setDataType);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", setDataType);
        }
        try {
            switch (setDataType) {
                case stream:
                    if (inputDatas != null) {
                        // set by pstmt.setXXXStream
                        setOutputStream(1);
                        outputStream.populate(inputDatas, length);
                    }
                    break;
                case stringOrBytes:
                    if (datas != null) {
                        // set by pstmt.setBytes || lob.setBytes
                        InputStream in = new ByteArrayInputStream(datas, offset, datas.length);
                        setOutputStream(1);
                        outputStream.populate(in, length);
                    }
                    break;
                case lob:
                    if (inBlob != null) {
                        // conn.createBlob
                        // blob.setOutputStream
                        // psmt.setBlob
                        try {
                            TrafT4Blob blob = (TrafT4Blob) inBlob;
                            blob.copyByLob(this);
                            blob.populate();
                        } catch (SQLException e) {
                            throw TrafT4Messages.createSQLException(getT4props(), e.getMessage());
                        }
                    }
                    break;
                default:
                    break;
            }
        } finally {
            free();
        }
    }

    public void free() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        super.free();
        if(inputDatas != null) {
            try {
                inputDatas.close();
            } catch (IOException e) {
            } finally {
                inputDatas = null;
            }
        }
        datas = null;
        if (inBlob != null) {
            inBlob.free();
        }
    }
    protected void setDatas(byte[] bytes) {
        this.length = bytes.length;
        this.datas = bytes;
        this.setDataType = SetDataType.stringOrBytes;
    }
}
