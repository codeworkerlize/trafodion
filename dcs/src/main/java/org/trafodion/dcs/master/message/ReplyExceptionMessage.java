/**
 * @@@ START COPYRIGHT @@@
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @@@ END COPYRIGHT @@@
 */
package org.trafodion.dcs.master.message;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.master.listener.Util;
import io.netty.buffer.ByteBuf;

public class ReplyExceptionMessage {
    private static final Logger LOG = LoggerFactory.getLogger(ReplyExceptionMessage.class);

    private final int exception_nr;
    private final int exception_detail;
    private final String errorText;

    public ReplyExceptionMessage() {
        exception_nr = 0;
        exception_detail = 0;
        errorText = null;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("exception_nr : ").append(exception_nr).append(", exception_detail : ")
                .append(exception_detail).append(", errorText : ").append(errorText);
        return sb.toString();
    }

    public ReplyExceptionMessage(int exception_nr, int exception_detail, String errorText) {
        this.exception_nr = exception_nr;
        this.exception_detail = exception_detail;
        this.errorText = errorText;
    }

    /**
     * insert into {@link java.nio.ByteBuffer} for NIO
     * 
     * @param buf
     */
    void insertIntoByteBuffer(ByteBuffer buf) throws UnsupportedEncodingException {
        buf.putInt(exception_nr);
        buf.putInt(exception_detail);

        if(errorText != null){
            String error = errorText;
            if (errorText.length() > (buf.remaining() - 4)) {
                error = errorText.substring(0, (buf.remaining() - 4));
            }
            Util.insertString(error, buf);
        } else {
            Util.insertString(errorText, buf);
        }
    }

    /**
     * insert into {@link io.netty.buffer.ByteBuf} for Netty, BigEndian.
     * 
     * @param buf
     */
    void insertIntoByteBuf(ByteBuf buf) throws UnsupportedEncodingException {
        buf.writeInt(exception_nr);
        buf.writeInt(exception_detail);
        Util.insertString(errorText, buf);
    }

    /**
     * insert into {@link io.netty.buffer.ByteBuf} for Netty, Little Endian.
     * 
     * @param buf
     */
    public void insertIntoByteBufLE(ByteBuf buf) throws java.io.UnsupportedEncodingException {
        buf.writeIntLE(exception_nr);
        buf.writeIntLE(exception_detail);
        Util.insertStringLE(errorText, buf);
    }
}
