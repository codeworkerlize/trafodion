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
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;

public class TrafT4Clob extends TrafT4Lob implements Clob {
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4Clob.class);
    private TrafT4Reader reader;
    private TrafT4Writer writer;
    private String datas;
    private byte[] byteDatas;
    private Reader inputReader;
    private InputStream inputDatas;
    private Clob inClob;

    private long startingPos;
    private int length;
    private int offset;
    private int sqlCharset;

    public TrafT4Clob(TrafT4Connection connection) throws SQLException {
        super(connection, Types.CLOB);
        this.sqlCharset = InterfaceUtilities.SQLCHARSETCODE_ISO88591;
    }

    public TrafT4Clob(TrafT4Statement statement, String lobHandle, TrafT4Desc colDesc) throws SQLException {
        super(statement, lobHandle, Types.CLOB, colDesc);
        this.sqlCharset = colDesc.getCharacterSet();
    }


    public TrafT4Clob(TrafT4Statement statement, TrafT4Desc colDesc, InputStream inputStream, int length)
            throws SQLException {
        super(statement, Types.CLOB, colDesc);
        this.sqlCharset = colDesc.getCharacterSet();
        this.inputDatas = inputStream;
        this.length = length;
        super.setDataType = SetDataType.stream;
    }

    public TrafT4Clob(TrafT4Statement statement, TrafT4Desc colDesc, Reader reader, int length) throws SQLException {
        super(statement, Types.CLOB, colDesc);
        this.sqlCharset = colDesc.getCharacterSet();
        this.inputReader = reader;
        this.length = length;
        super.setDataType = SetDataType.stream;
    }

    public TrafT4Clob(TrafT4Statement statement, TrafT4Desc colDesc, String datas, int length) throws SQLException {
        super(statement, Types.CLOB, colDesc);
        this.sqlCharset = colDesc.getCharacterSet();
        this.datas = datas;
        this.length = length;
        super.setDataType = SetDataType.stringOrBytes;
    }

    public TrafT4Clob(TrafT4Statement statement, TrafT4Desc colDesc, byte[] datas, int length) throws SQLException {
        super(statement, Types.CLOB, colDesc);
        this.sqlCharset = colDesc.getCharacterSet();
        this.byteDatas = datas;
        this.length = length;
        super.setDataType = SetDataType.stringOrBytes;
    }

    public TrafT4Clob(TrafT4Statement statement, TrafT4Desc colDesc, Clob inClob) throws SQLException {
        super(statement, Types.CLOB, colDesc);
        this.sqlCharset = colDesc.getCharacterSet();
        this.inClob = inClob;
        super.setDataType = SetDataType.lob;
    }

    public InputStream getAsciiStream() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        testAvailability();
        try {
            // Close the reader and inputStream hander over earlier
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                } finally {
                    reader = null;
                }
            }
            return getInputStream(1, length());
        } finally {
        }
    }

    public Reader getCharacterStream() throws SQLException {
        return getCharacterStream(1, length());
    }

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pos, length);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}.", pos, length);
        }
        testAvailability();

        throwIfPosOutOfBound(pos);
        if (isLobWithExternalData()) {
            switch (setDataType) {
                case stream:
                    if (inputReader != null) {
                        pos--;
                        if (pos > 0) {
                            try {
                                inputReader.skip(pos);
                            } catch (IOException e) {
                            }
                        }
                        return inputReader;
                    }
                    if (inputDatas != null) {
                        InputStreamReader tmpInput = new InputStreamReader(inputDatas);
                        pos--;
                        if (pos > 0) {
                            try {
                                tmpInput.skip(pos);
                            } catch (IOException e) {
                            }
                        }
                        return tmpInput;
                    }
                    break;
                case stringOrBytes:
                    if (datas != null) {
                        int tmpLen = (int) (length > this.length ? this.length : length);
                        StringReader tmpReader = null;
                        if (tmpLen > 0) {
                            tmpReader = new StringReader(datas.substring(0, tmpLen));
                        } else {
                            tmpReader = new StringReader(datas);
                        }
                        pos--;
                        if (pos > 0) {
                            try {
                                tmpReader.skip(pos);
                            } catch (IOException e) {
                            }
                        }
                        return tmpReader;
                    }
                    if (byteDatas != null) {
                        InputStreamReader tmpInput = new InputStreamReader(new ByteArrayInputStream(byteDatas));
                        pos--;
                        if (pos > 0) {
                            try {
                                tmpInput.skip(pos);
                            } catch (IOException e) {
                            }
                        }
                        return tmpInput;
                    }
                    break;
                case lob:
                    if (inClob != null) {
                        return inClob.getCharacterStream();
                    }
                    break;
                default:
                    break;
            }
            return null;
        }
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
            } finally {
                reader = null;
            }
        }
        reader = new TrafT4Reader(this, pos, length);
        return reader;
    }

    public String getSubString(long pos, int length) throws SQLException {
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

        Reader cr = getCharacterStream(pos, length);
        try {
            char[] buf = new char[length];
            int retLen = cr.read(buf, 0, length);
            if (retLen < length && retLen > 0)
                buf = Arrays.copyOf(buf, retLen);
            return new String(buf);
        } catch (IOException ioe) {
            throw new SQLException(ioe);
        }
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pos);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", pos);
        }
        testAvailability();
        if (pos != 1) {
            TrafT4Messages.throwUnsupportedFeatureException(getT4props(),
                    "Clob.setAsciiStream with position != 1 is not supported");
        }
        // Close the writer and OutputStream hander over earlier
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
            } finally {
                writer = null;
            }
        }
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

    public Writer setCharacterStream(long pos) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pos);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", pos);
        }
        testAvailability();

        if (pos != 1) {
            throw new SQLFeatureNotSupportedException("Clob.setCharacterStream with position != 1 is not supported");
        }
        // Close the writer and OutputStream hander over earlier
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
            } finally {
                writer = null;
            }
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
            } finally {
                outputStream = null;
            }
        }
        writer = new TrafT4Writer(this, pos);
        return writer;
    }

    public int setString(long pos, String str) throws SQLException {
        testAvailability();
        try {
            return setString(pos, str, 0, str.length());
        } finally {
        }
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", pos, offset, len);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}.", pos, offset, len);
        }
        testAvailability();
        try {
            if (str == null || pos < 0 || len < 0 || offset < 0) {
                throw TrafT4Messages.createSQLException(getT4props(), "invalid_input_value",
                        "Clob.setString(long, String, int, int)");
            }
            if (pos > 1)
                TrafT4Messages.throwUnsupportedFeatureException(getT4props(),
                        "Clob.setString with position > 1 is not supported");
            this.setDataType = SetDataType.stringOrBytes;
            this.datas = str;
            this.startingPos = pos;
            this.length = len;
            this.offset = offset;
            return len;
        } finally {
        }
    }

    public long length() throws SQLException {
        testAvailability();

        return getLobLen() == -1 ? length : getLobLen();
    }

    public long position(String searchstr, long start) throws SQLException {
        TrafT4Messages.throwUnsupportedMethodException(getT4props(), "TrafT4Clob", "position(String, long)");
        testAvailability();

        return 0L;
    }

    public long position(Clob searchstr, long start) throws SQLException {
        TrafT4Messages.throwUnsupportedMethodException(getT4props(), "TrafT4Clob", "position(Clob, long)");
        return position(searchstr.getSubString(0, (int) searchstr.length()), start);
    }


    @Override
    public void truncate(long len) throws SQLException {
        TrafT4Messages.throwUnsupportedMethodException(getT4props(), "TrafT4Clob", "truncate(long)");
        testAvailability();

    }

    //    protected void close() throws IOException {
    //        try {
    //            try {
    //                if (reader != null)
    //                    reader.close();
    //                if (writer != null)
    //                    writer.close();
    //                super.close();
    //            } catch (IOException e) {
    //                throw e;
    //            } finally {
    //                reader = null;
    //                writer = null;
    //            }
    //        } finally {
    //        }
    //    }
    //
    //    protected String getString() throws SQLException {
    //        if (inLength() == 0 || inLength() > getT4props().getInlineLobChunkSize()) {
    //            return null;
    //        }
    //        if (datas != null) {
    //            if (length == datas.length()) {
    //                return datas;
    //            } else {
    //                return datas.substring(offset, offset + length);
    //            }
    //        }
    //        if (inputDatas != null) {
    //            try {
    //                byte[] buf = new byte[length];
    //                int retLen = inputDatas.read(buf);
    //                if (retLen != length)
    //                    return new String(buf, 0, retLen);
    //                else
    //                    return new String(buf);
    //            } catch (IOException ioe) {
    //                throw new SQLException(ioe);
    //            }
    //        }
    //        if (inputReader != null) {
    //            try {
    //                char[] buf = new char[length];
    //                int retLen = inputReader.read(buf);
    //                if (retLen != length)
    //                    return new String(buf, 0, retLen);
    //                else
    //                    return new String(buf);
    //            } catch (IOException ioe) {
    //                throw new SQLException(ioe);
    //            }
    //        }
    //        return null;
    //    }

    // This function populates the Clob data from one of the following:
    // 1. InputStream set in PreparedStatement.setAsciiStream 
    // 2. Reader set in PreparedStatement.setCharacterStream
    // 3. From another clob set in PreparedStatement.setClob or ResultSet.updateClob
    // This function is called at the time of PreparedStatement.executeUpdate, execute and 
    // executeBatch
    @Override
    protected void populate() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY, setDataType=" + setDataType);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", setDataType);
        }
        try {
            switch (setDataType) {
                case stream:
                    if (inputReader != null) {
                        // pstmt.setXXXReader
                        setCharacterStream(1);
                        writer.populate(inputReader, length);
                    }
                    if (inputDatas != null) {
                        // pstmt.setXXXStream
                        setAsciiStream(1);
                        outputStream.populate(inputDatas, length);
                    }
                    break;
                case stringOrBytes:
                    if (datas != null) {
                        // pstmt.setString || lob.setBytes
                        StringReader reader = new StringReader(datas);
                        setCharacterStream(1);
                        writer.populate(reader, length);
                    }
                    if (byteDatas != null) {
                        // pstmt.setString || lob.setBytes
                        ByteArrayInputStream in = new ByteArrayInputStream(byteDatas, offset, byteDatas.length);
                        setOutputStream(1);
                        outputStream.populate(in, length);
                        try {
                            in.close();
                        } catch (IOException e) {
                        }
                    }
                    break;
                case lob:
                    if (inClob != null) {
                        // conn.createClob
                        // clob.setOutputStream
                        // psmt.setClob
                        try {
                            TrafT4Clob clob = (TrafT4Clob) inClob;
                            clob.copyByLob(this);
                            clob.populate();
                        } catch (SQLException e) {
                            throw TrafT4Messages.createSQLException(getT4props(), e.getMessage());
                        } finally {
                            inClob.free();
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
        if (isFreed()) {
            return;
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
        if(inputReader != null) {
            try {
                // if close inner, the outer reader will meet java.io.IOException: Stream closed
                // when user close the stream.

                //inputReader.close();
            } finally {
                inputReader = null;
            }
        }
        datas = null;
        byteDatas = null;
        if (inClob != null) {
            inClob.free();
        }
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
            }
        }
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
            }
        }
    }

    protected int getSqlCharset() {
        return sqlCharset;
    }

    @Override
    protected void copyByLob(TrafT4Lob another) {
        super.copyByLob(another);
        this.sqlCharset = ((TrafT4Clob)another).getSqlCharset();
    }

    protected void setDatas(char[] chars) {
        this.length = chars.length;
        this.datas = new String(chars);
        this.setDataType = SetDataType.stringOrBytes;
    }
    protected void setByteDatas(byte[] bytes) {
        this.length = bytes.length;
        this.byteDatas = bytes;
        this.setDataType = SetDataType.stringOrBytes;
    }
}
