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
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.TrafT4Lob.SetDataType;

public class TrafT4OutputStream extends OutputStream {
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4OutputStream.class);
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

    public void flush() throws IOException {
        try {
            if (closed)
                return;
            if(emptyLob) {
                if(lob instanceof TrafT4Blob) {
                    ((TrafT4Blob)lob).setDatas(tmpChunk);
                }else if(lob instanceof TrafT4Clob){
                    ((TrafT4Clob)lob).setByteDatas(tmpChunk);
                }
                return;
            }
            if (!flushed && currentPos > 0) {
                writeChunkThrowIO(chunk, 0, currentPos);
                currentPos = 0;
            }
        } finally {
        }
    }

    public void write(int b) throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        lob.setDataType = SetDataType.write;
        try {
            if (closed)
                throw new IOException("Output stream is in closed state");
            chunk[currentPos] = (byte) b;
            flushed = false;
            currentPos++;
            if (currentPos == lob.getLobChunkMaxLen()) {
                writeChunkThrowIO(chunk, 0, currentPos);
                currentPos = 0;
            }
        } finally {
        }
    }


    public void write(byte[] b) throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            if (b == null)
                throw new IOException("Invalid input value");
            write(b, 0, b.length);
        } finally {
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {
        try {
            int copyLen;
            int srcOffset;
            int tempLen;

            if (closed)
                throw new IOException("Output stream is in closed state");
            if (b == null)
                throw new IOException("Invalid input value");
            if (off < 0 || len < 0 || off > b.length)
                throw new IndexOutOfBoundsException(
                        "length or offset is less than 0 or offset is greater than the length of array");
            if (emptyLob) {
                if(tmpChunk == null){
                    //first time write
                    tmpChunk = new byte[len];
                }else{
                    // new and array copy
                    byte[] tmpRef = tmpChunk;
                    if((long)len + tmpRef.length> Integer.MAX_VALUE){
                        throw new IOException(
                            "Total write length can not greater than INT_MAX for createBlob model");
                    }
                    tmpChunk = new byte[len + tmpRef.length];
                    System.arraycopy(tmpRef, 0, tmpChunk, 0, tmpRef.length);

                }
                System.arraycopy(b, off, tmpChunk, tmpChunk.length - len, len);
                return;
            }
            srcOffset = off;
            copyLen = len;
            while (true) {
                if ((copyLen + currentPos) < lob.getLobChunkMaxLen()) {
                    System.arraycopy(b, srcOffset, chunk, currentPos, copyLen);
                    currentPos += copyLen;
                    flushed = false;
                    break;
                } else {
                    if (currentPos != 0) {
                        tempLen = lob.getLobChunkMaxLen() - currentPos;
                        System.arraycopy(b, srcOffset, chunk, currentPos, tempLen);
                        currentPos += tempLen;
                        writeChunkThrowIO(chunk, 0, currentPos);
                        currentPos = 0;
                    } else {
                        tempLen = lob.getLobChunkMaxLen();
                        writeChunkThrowIO(b, 0, tempLen);
                    }
                    copyLen -= tempLen;
                    srcOffset += tempLen;
                }
            }
        } finally {
        }
    }

    void writeChunkThrowIO(byte[] chunk, int off, int len) throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            try {
                writeChunk(chunk, off, len);
            } catch (SQLException e) {
                throw new IOException(TrafT4Lob.convSQLExceptionToIO(e));
            }
        } finally {
        }
    }

    void writeChunk(byte[] chunk, int off, int len) throws SQLException {

        long lobWritePos = 0;
//        long lobWritePos = (currentChunkNo * lob.getLobChunkMaxLen()) + startingPos + off -1;

        lob.writeChunk(chunk, off, len, lobWritePos);
        currentChunkNo++;
        currentPos = 0;
        flushed = true;

    }

    void populate(InputStream is, long length) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            int tempLen;
            long readLen;
            int retLen = 0;
            int offset = 0;
            readLen = length;
            try {
                while (readLen > 0) {
                    if (readLen <= lob.getLobChunkMaxLen())
                        tempLen = (int) readLen;
                    else
                        tempLen = lob.getLobChunkMaxLen();
                    retLen = is.read(chunk, 0, tempLen);
                    if (retLen == -1)
                        break;
                    writeChunk(chunk, offset, retLen);
                    offset+=retLen;
                    readLen -= retLen;
                }
            } catch (IOException e) {
                throw TrafT4Messages.createSQLException(lob.getT4props(), "io_exception", e.getMessage());
            }
        } finally {
            currentPos = 0;
        }
    }

    // constructors
    TrafT4OutputStream(long startingPos, TrafT4Lob lob) throws SQLException {
        try {
            this.lob = lob;
            this.startingPos = startingPos;
            flushed = false;
            currentPos = 0;
            currentChunkNo = 0;
            if(lob.setDataType == SetDataType.empty) {
                emptyLob = true;
            } else {
                chunk = new byte[lob.getLobChunkMaxLen()];
            }
        } finally {
        }
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
    }

    private T4Properties getT4props() {
        return lob.getT4props();
    }

    protected int length() {
        return currentPos;
    }

    // Fields
    private TrafT4Lob lob;
    private boolean closed;
    private boolean flushed;
    private byte[] chunk;
    private long startingPos;
    private int currentPos;
    private int currentChunkNo;
    private boolean emptyLob; // for conn.createClob
    private byte[] tmpChunk; // for conn.createClob
}
