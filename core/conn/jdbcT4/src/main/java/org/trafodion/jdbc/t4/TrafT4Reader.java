// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.trafodion.jdbc.t4;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;

public class TrafT4Reader extends Reader {
    public void close() throws IOException {
        try {
            closed = true;
        } finally {
        }
    }

    public void mark(int readAheadLimit) throws IOException {}

    public boolean markSupported() {
        try {
            return false;
        } finally {
        }
    }

    public int read() throws IOException {
        try {
            int retValue = 0;

            if (currentPos == charsRead)
                retValue = readChunk();
            if (retValue != -1) {
                retValue = chunk.charAt(currentPos);
                currentPos++;
            }
            return retValue;
        } finally {
        }
    }

    public int read(char[] cbuf) throws IOException {
        try {
            if (cbuf == null)
                throw new IOException("Invalid input value");
            return read(cbuf, 0, cbuf.length);
        } finally {
        }
    }

    public int read(char[] cbuf, int off, int len) throws IOException {
        return read(cbuf, off, len, false);
    }

    public int read(char[] cbuf, int off, int len, boolean skip) throws IOException {
        try {
            if (cbuf == null && !skip)
                throw new IOException("Invalid input value");
            int remainLen = len;
            int copyOffset = off;
            int copyLen;
            int availableLen;
            int copiedLen = 0;

            while (remainLen > 0) {
                availableLen = charsRead - currentPos;
                if (availableLen > remainLen)
                    copyLen = remainLen;
                else
                    copyLen = availableLen;
                if (copyLen > 0) {
                    if (!skip)
                        chunk.getChars(currentPos, copyLen+currentPos, cbuf, copyOffset);
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
                    clob.close();
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
            return copiedLen;
        } finally {
        }
    }

    public long skip(long n) throws IOException {
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

    
    byte[] preHalf = new byte[10];
    int preHalfLen = 0;
    byte[] currHalf = new byte[10];
    int currHalfLen = 0;

    int readChunk() throws IOException {
        try {
            // eor can be set to true before all the bytes are read from chunk
            // So, don't use eor in any other function
            int extractMode = 1; // get the lob data
            if (eor && currHalfLen==0)
                return -1;
            int readLen;
            // limit the read by the required length_
            if (currentLobPos >= startLobPos) {
                if (length > (currentLobPos + clob.getLobChunkMaxLen()))
                    readLen = clob.getLobChunkMaxLen();
                else
                    readLen = (int) (length - currentLobPos);
            } else
                readLen = clob.getLobChunkMaxLen();

            // when using enable inline feature, invoke rs.getStream or rs.getReader 
            // it will have an extra I/O to make sure read to the EOF of the lob
            // To reduce this extra I/O add one check for lobLen and currentLobPos
            long lobLength = clob.getLobLen();
            if (readLen == 0
                || (clob.getLobInlineMaxLen() > 0 && lobLength == currentLobPos)
                || currentLobPos > lobLength) {
                eor = true;
                return -1;
            }

            int charset = clob.getSqlCharset();
            byte[] extract = new byte[0];
            if (!eor) {
                ExtractLobReply er = clob.readChunk(extractMode, readLen);
                extract = er.extractData;
                bytesRead += er.extractLen;
            }

            if (currHalfLen > 0) {
                System.arraycopy(currHalf, 0, preHalf, 0, currHalfLen);
            }
            preHalfLen = currHalfLen;
            currHalfLen = 0;

            int index;
            boolean hasUTF8 = false;
            if (bytesRead == length -1) { // the last fetch
                index = extract.length;
                eor = true;
            } else {
                for (index = extract.length - 1; index > extract.length - 1 - 10 && index>=0; index--) {
                    currHalf[currHalfLen++] = extract[index];
                    if (((extract[index] >> 6) & 3) == 3) {
                        hasUTF8 = true;
                        break;
                    }
                }
            }
            if(!hasUTF8) {
                index = extract.length;
                currHalfLen = 0;
            }
            byte[] clip = null;
            //clip the last half char
            if (preHalfLen > 0) {
                clip = new byte[preHalfLen + index];
                int tmpIndex = 0;
                for (int i = preHalfLen - 1; i >= 0; i--) {
                    clip[tmpIndex++] = preHalf[i];
                }
                System.arraycopy(extract, 0, clip, preHalfLen, index);
            } else {
                clip = new byte[index];
                System.arraycopy(extract, 0, clip, 0, index);
            }
            
            if (extract.length > 0) {
                String charsetName = InterfaceUtilities.getCharsetName(charset);
                if(charset == InterfaceUtilities.SQLCHARSETCODE_UNICODE) {
                    charsetName = "UTF-16LE";
                }
                chunk = new String(clip, Charset.forName(charsetName));
                charsRead = chunk.length();
                if (currentLobPos > startLobPos)
                    currentPos = 0;
                else if ((currentLobPos + charsRead) > startLobPos) {
                    currentPos = (int) (startLobPos - currentLobPos);
                } else
                    currentPos = charsRead;
                currentLobPos += charsRead;
            } else {
                chunk = null;
                charsRead = -1;
                eor = true;
            }
            return charsRead;
        } catch (SQLException e) {
            throw new IOException(TrafT4Lob.convSQLExceptionToIO(e));
        }
    }

    public TrafT4Reader(TrafT4Clob clob, long startPos, long length) {
        this.clob = clob;
        this.chunk = null;
        this.currentPos = 0;
        this.charsRead = 0;
        this.startLobPos = startPos;
        this.currentLobPos = 1;
        if (length < (Long.MAX_VALUE - startLobPos))
            this.length = length + startLobPos;
        else
            this.length = length;
        this.eor = false;
        this.closed = false;
        if (clob.getInlineDataString() != null) {
            int inlineLen = clob.getInlineDataString().length();
            currentLobPos = inlineLen;
            if (inlineLen >= startLobPos) {
                chunk = clob.getInlineDataString().substring((int) startLobPos - 1, inlineLen);
                charsRead = inlineLen;
                currentPos = (int) startLobPos - 1;
            }
        }
    }

    protected long getLength() {
        return length;
    }

    private TrafT4Clob clob;
    private boolean closed;
    private long bytesRead;
    private String chunk;
    private int currentPos;
    private int charsRead;
    private boolean eor;
    private long startLobPos;
    private long currentLobPos;
    private long length;
}
