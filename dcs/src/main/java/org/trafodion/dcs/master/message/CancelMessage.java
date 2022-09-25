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

package org.trafodion.dcs.master.message;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.trafodion.dcs.master.listener.Util;
import io.netty.buffer.ByteBuf;

public final class CancelMessage {
    private final int dialogueId;
    private final int srvrType;
    private final String srvrObjRef;
    private final int stopType;

    private CancelMessage(int dialogueId, int srvrType, String srvrObjRef, int stopType) {
        super();
        this.dialogueId = dialogueId;
        this.srvrType = srvrType;
        this.srvrObjRef = srvrObjRef;
        this.stopType = stopType;
    }

    public int getDialogueId() {
        return dialogueId;
    }

    public int getSrvrType() {
        return srvrType;
    }

    public String getSrvrObjRef() {
        return srvrObjRef;
    }

    public int getStopType() {
        return stopType;
    }

    /**
     * extract From {@link java.nio.ByteBuffer} for NIO.
     * 
     * @param buf
     */
    public static CancelMessage extractCancelMessageFromByteBuffer(ByteBuffer buf)
            throws IOException {
        int dialogueId = buf.getInt();
        int srvrType = buf.getInt();
        String srvrObjRef = Util.extractString(buf);
        int stopType = buf.getInt();

        return new CancelMessage(dialogueId, srvrType, srvrObjRef, stopType);
    }

    /**
     * extract From {@link io.netty.buffer.ByteBuf} for Netty, Big Endian.
     * 
     * @param buf
     */
    public static CancelMessage extractCancelMessageFromByteBuf(ByteBuf buf)
            throws UnsupportedEncodingException {
        int dialogueId = buf.readInt();
        int srvrType = buf.readInt();
        String srvrObjRef = Util.extractString(buf);
        int stopType = buf.readInt();

        return new CancelMessage(dialogueId, srvrType, srvrObjRef, stopType);
    }


}
