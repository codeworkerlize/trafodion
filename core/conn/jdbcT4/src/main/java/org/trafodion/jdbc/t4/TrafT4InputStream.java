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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;

public class TrafT4InputStream extends InputStream {
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafT4InputStream.class);

    public int available() throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        int remainLen = bytesRead - currentPos;
        if (remainLen == 0) // 0 would mean all the bytes are read from chunk_
            remainLen = lob.getLobChunkMaxLen();
        return remainLen;
    }

    private T4Properties getT4props() {
        return lob.getT4props();
    }

    public void close() throws IOException {
        closed = true;
        chunk = null;
    }

    public void mark(int readlimit) {}

    public boolean markSupported() {
        return false;
    }

    public int read() throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            int retValue;
            if (currentPos == bytesRead) {
                retValue = readChunk();
                if (retValue == -1)
                    return retValue;
            }
            retValue = chunk.get();
            currentPos++;
            return retValue;
        } finally {
        }
    }

    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return read(b, off, len, false);
    }

    public int read(byte[] b, int off, int len, boolean skip) throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {

            if (b == null && !skip)
                throw new IOException("Invalid input value");
            int remainLen = len;
            int copyOffset = off;

            int availableLen;
            int copyLen;
            int copiedLen = 0;
            while (remainLen > 0) {
                availableLen = bytesRead - currentPos;
                if (availableLen > remainLen)
                    copyLen = remainLen;
                else
                    copyLen = availableLen;
                if (copyLen > 0) {
                    if (!skip) {
                        chunk.get(b, copyOffset, copyLen);
                    }
                    else {
                        chunk.position(currentPos + copyLen);
                    }
                    currentPos += copyLen;
                    copyOffset += copyLen;
                    copiedLen += copyLen;
                    remainLen -= copyLen;

                }
                if (remainLen > 0) {
                    int retLen = readChunk();
                    if (retLen == -1) {
                        if (copiedLen == 0) {
                            copiedLen = -1;
                        }
                        break;
                    }

                }
            }
            if (copiedLen == length - startLobPos) {
                try {
                    lob.close();
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
            return copiedLen;
        } finally {
        }
    }

    public void reset() throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        try {
            currentPos = 0;
            bytesRead = 0;
            currentLobPos = 0;
            return;
        } finally {
        }
    }

    public long skip(long n) throws IOException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        long totalSkippedLen = 0;
        long skipRemain = n;
        int skipLen;
        int skippedLen;
        while (skipRemain > 0) {
            if (skipRemain <= Integer.MAX_VALUE)
                skipLen = (int) skipRemain;
            else
                skipLen = Integer.MAX_VALUE;
            skippedLen = read(null, 0, skipLen, true);
            if (skippedLen == -1)
                break;
            skipRemain -= skippedLen;
            totalSkippedLen += skippedLen;
        }
        return totalSkippedLen;
    }


    int readChunk() throws IOException {
        try {
            // eos can be set to true before all the bytes are read from chunk
            // So, don't use eos in any other function
            int extractMode = 1; // get the lob data
            if (eos)
                return -1;
            int readLen;
            // limit the read by the required length
            if (currentLobPos >= startLobPos) {
                if (length > (currentLobPos + lob.getLobChunkMaxLen()))
                    readLen = lob.getLobChunkMaxLen();
                else
                    readLen = (int) (length - currentLobPos);
            } else
                readLen = lob.getLobChunkMaxLen();

            // when using enable inline feature, invoke rs.getStream or rs.getReader 
            // it will have an extra I/O to make sure read to the EOF of the lob
            // To reduce this extra I/O add one check for lobLen and currentLobPos
            if (readLen <= 0 || (lob.getLobInlineMaxLen() > 0 && lob.getLobLen() <= currentLobPos)) {
                eos = true;
                return -1;
            }
            do {
                long tmpCurrentLobPos = currentLobPos;
                ExtractLobReply er = lob.readChunk(extractMode, readLen);
                currentChunkNo++;
                if(er.extractData != null) {
                    chunk = ByteBuffer.wrap(er.extractData);
                }
                if (chunk != null && (bytesRead = chunk.capacity()) > 0) {
                    readLen = (int) er.extractLen;
                    if (tmpCurrentLobPos > startLobPos)
                        currentPos = 0;
                    else if ((tmpCurrentLobPos + bytesRead) > startLobPos) {
                        currentPos = (int) (startLobPos - tmpCurrentLobPos);
                    } else
                        currentPos = bytesRead;
                    chunk.position(currentPos);
                    currentLobPos += bytesRead;
                    if (bytesRead < readLen)
                        eos = true;
                } else {
                    bytesRead = -1;
                    eos = true;
                    break;
                }
                if(er.isEndOfData()) {
                    eos = true;
                    break;
                }

            } while (currentLobPos < startLobPos && !eos);
            return bytesRead;
        } catch (SQLException e) {
            throw new IOException(TrafT4Lob.convSQLExceptionToIO(e), e);
        }
    }

    // Constructor
    public TrafT4InputStream(TrafT4Lob lob, long startPos, long length) throws SQLException {
            this.lob = lob;
            this.chunk = null;
            this.bytesRead = 0;
            this.currentPos = 0;
            this.startLobPos = startPos;
            this.currentLobPos = 1;
            this.currentChunkNo = 0;
            if (length < (Long.MAX_VALUE - startLobPos))
                this.length = length + startLobPos;
            else
                this.length = length;
            this.eos = false;
            this.closed = false;
            if (lob.getInlineDataString() != null) {
                int inlineLen = 0;
                if(lob instanceof TrafT4Clob) {
                    int charset = ((TrafT4Clob)lob).getSqlCharset();
                    inlineLen = lob.getInlineDataString()
                            .getBytes(Charset.forName(InterfaceUtilities.getCharsetName(charset))).length;
                } else {
                    inlineLen = lob.getInlineDataString().length();
                }
                currentLobPos = inlineLen;
                if (inlineLen >= startLobPos) {
                    if(lob instanceof TrafT4Blob) {
                        int charset = InterfaceUtilities.SQLCHARSETCODE_ISO88591;
                        try {
                            chunk = ByteBuffer.wrap(lob.getInlineDataString().getBytes(InterfaceUtilities.getCharsetName(charset)), (int) startLobPos - 1,
                                inlineLen - (int) startLobPos + 1);
                        } catch (UnsupportedEncodingException e) {
                            SQLException se = TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding", e.getMessage());
                            se.initCause(e);
                            throw se;
                        }
                    } else {
                        chunk = ByteBuffer.wrap(lob.getInlineDataString().getBytes(), (int) startLobPos - 1,
                                inlineLen - (int) startLobPos + 1);
                    }

                    bytesRead = inlineLen;
                    currentPos = (int) startLobPos - 1;
                }
            }

        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
    }

    protected long getLength() {
        return length;
    }

    // Fields
    private TrafT4Lob lob;
    private ByteBuffer chunk;
    private int currentPos;
    private int bytesRead;
    private long startLobPos;
    private long currentLobPos;
    private long length;
    private boolean closed;
    private boolean eos;
    private int currentChunkNo;
}
