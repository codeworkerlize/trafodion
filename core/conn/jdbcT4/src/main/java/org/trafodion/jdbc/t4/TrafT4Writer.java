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
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.TrafT4Lob.SetDataType;

public class TrafT4Writer extends Writer {
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4Writer.class);
    public void close() throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            if (!closed) {
                flush();
                closed = true;
            }
        } finally {
            chunk = null;
        }
    }

    private T4Properties getT4props() {
        return clob.getT4props();
    }

    public void flush() throws IOException {
        try {
            if (closed)
                return;
            if(emptyClob) {
                clob.setDatas(tmpChunk);
                return;
            }
            if (!flushed && currentChar > 0) {
                writeChunkThrowIO(chunk, 0, currentChar);
                currentChar = 0;
            }
        } finally {
        }
    }

    public void write(char[] cbuf) throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            if (cbuf == null)
                throw new IOException("Invalid input value");

            write(cbuf, 0, cbuf.length);
        } finally {
        }
    }

    public void write(char[] cbuf, int off, int len) throws IOException {
        try {
            int copyLen;
            int srcOffset;
            int tempLen;

            if (closed)
                throw new IOException("Writer is in closed state");
            if (cbuf == null)
                throw new IOException("Invalid input value");
            if (off < 0 || len < 0 || off > cbuf.length)
                throw new IndexOutOfBoundsException(
                        "length or offset is less than 0 or offset is greater than the length of array");
            if (emptyClob) {
                if(tmpChunk == null){
                    //first time write
                    tmpChunk = new char[len];
                }else{
                    // new and array copy
                    char[] tmpRef = tmpChunk;
                    if((long)len + tmpRef.length> Integer.MAX_VALUE){
                        throw new IOException(
                            "Total write length can not greater than INT_MAX for createBlob model");
                    }
                    tmpChunk = new char[len + tmpRef.length];
                    System.arraycopy(tmpRef, 0, tmpChunk, 0, tmpRef.length);

                }
                System.arraycopy(cbuf, off, tmpChunk, tmpChunk.length - len, len);
                return;

            }
            srcOffset = off;
            copyLen = len;
            while (true) {
                if ((copyLen + currentChar) < (clob.getLobChunkMaxLen())) {
                    System.arraycopy(cbuf, srcOffset, chunk, currentChar, copyLen);
                    currentChar += copyLen;
                    flushed = false;
                    break;
                } else {
                    if (currentChar != 0) {
                        tempLen = clob.getLobChunkMaxLen() - currentChar;
                        System.arraycopy(cbuf, srcOffset, chunk, currentChar, tempLen);
                        currentChar += tempLen;
                        writeChunkThrowIO(chunk, 0, currentChar);
                        currentChar = 0;
                    } else {
                        tempLen = clob.getLobChunkMaxLen();
                        writeChunkThrowIO(cbuf, srcOffset, tempLen);
                    }
                    copyLen -= tempLen;
                    srcOffset += tempLen;
                }
            }
        } finally {
        }
    }

    public void write(int c) throws IOException {
        throw new IOException("unsupported method write(int)");
    }

    public void write(String str) throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            if (str == null)
                throw new IOException("Invalid input value");
            write(str, 0, str.length());
        } finally {
        }
    }

    public void write(String str, int off, int len) throws IOException {
        try {
            int copyLen;
            int srcOffset;
            int tempLen;

            if (closed)
                throw new IOException("Output stream is in closed state");
            if (str == null)
                throw new IOException("Invalid input value");
            if (off < 0 || len < 0 || off > str.length())
                throw new IndexOutOfBoundsException(
                        "length or offset is less than 0 or offset is greater than the length of array");
            if (emptyClob) {
                tmpChunk = new char[len];
                System.arraycopy(str.toCharArray(), off, tmpChunk, 0, len);
                return;
            }
            srcOffset = off;
            copyLen = len;
            while (true) {
                if ((copyLen + currentChar) < clob.getLobChunkMaxLen()) {
                    System.arraycopy(str.toCharArray(), srcOffset, chunk, currentChar, copyLen);
                    currentChar += copyLen;
                    flushed = false;
                    break;
                } else {
                    if (currentChar != 0) {
                        tempLen = clob.getLobChunkMaxLen() - currentChar;
                        System.arraycopy(str.toCharArray(), srcOffset, chunk, currentChar, tempLen);
                        currentChar += tempLen;
                        writeChunkThrowIO(chunk, 0, currentChar);
                        currentChar = 0;
                    } else {
                        tempLen = clob.getLobChunkMaxLen();
                        writeChunkThrowIO(str.toCharArray(), 0, tempLen);
                    }
                    copyLen -= tempLen;
                    srcOffset += tempLen;
                }
            }
        } finally {
        }
    }

    /**
     * @param chunk Write from 0 to <b>len</b>.
     * @param offset Used for server side
     * @param len
     * @return the new offset
     * @throws IOException
     */
    int writeChunkThrowIO(char[] chunk, int offset, int len) throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (len == 0) {
            flushed = true;
            return offset;
        }
        try {
            CharBuffer cb = CharBuffer.wrap(chunk, 0, len);
            ByteBuffer bb = cs.encode(cb);
            int totalLen = bb.limit();
            while (totalLen > 0) {
                int writeLen = Math.min(clob.getLobChunkMaxLen(), totalLen);
                byte[] write = new byte[writeLen];
                bb.get(write);
                clob.writeChunk(write, offset, writeLen, 0);
                offset += writeLen;
                totalLen -= writeLen;
            }
        } catch (SQLException e) {
            throw new IOException(TrafT4Lob.convSQLExceptionToIO(e));
        } finally {
        }
        return offset;
    }


    void populate(Reader ir, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            int retLen;
            int offset = 0;

            try {
                while (length > 0) {
                    retLen = ir.read(chunk, 0, (int)Math.min(length, clob.getLobChunkMaxLen()));
                    if (retLen == -1)
                        break;
                    offset = writeChunkThrowIO(chunk, offset, retLen);
                    length -= retLen;
                }
            } catch (IOException e) {
                throw TrafT4Messages.createSQLException(conn.getT4props(), e.getMessage());
            }
        } finally {
            currentChar = 0;
        }
    }

    // constructors
    TrafT4Writer(TrafT4Clob clob, long pos) throws SQLException {
        try {
//            long length = clob.length();
//            if (pos < 1 || pos > length + 1)
//                throw TrafT4Messages.createSQLException(clob.getT4props(),
//                        "invalid_position_value");
            this.clob = clob;
            startingPos = pos;
            flushed = false;
            currentChar = 0;
            if(clob.setDataType == SetDataType.empty) {
                emptyClob = true;
            } else {
                chunk = new char[clob.getLobChunkMaxLen()];
            }
            String charsetName = InterfaceUtilities.getCharsetName(clob.getSqlCharset());
            cs = Charset.forName(charsetName);
        } finally {
        }
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
    }


    // Fields
    private TrafT4Clob clob;
    private long startingPos;
    private TrafT4Connection conn;
    private boolean closed;
    private char[] chunk;
    private int currentChar;
    private boolean flushed;
    private boolean emptyClob; // for conn.createClob
    private char[] tmpChunk; // for conn.createClob
    private Charset cs;
}
