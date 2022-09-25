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

import java.nio.ByteBuffer;
import io.netty.buffer.ByteBuf;

public class Version {

    private final short componentId;
    private final short majorVersion;
    private final short minorVersion;
    private int buildId;

    public Version(short componentId, short majorVersion, short minorVersion, int buildId) {
        super();
        this.componentId = componentId;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.buildId = buildId;
    }

    public short getComponentId() {
        return componentId;
    }

    public void modifyVersionBuildId(int value) {
        this.buildId |= value;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("componentId : ").append(componentId).append(", majorVersion : ")
                .append(majorVersion).append(", minorVersion : ").append(minorVersion)
                .append(", buildId :").append(buildId);

        return sb.toString();
    }

    /**
     * extract From {@link io.netty.buffer.ByteBuf} for Netty, Big Endian.
     * 
     * @param buf
     */
    static Version extractFromByteBuf(ByteBuf buf) {
        short componentId = buf.readShort();
        short majorVersion = buf.readShort();
        short minorVersion = buf.readShort();
        int buildId = buf.readInt();
        return new Version(componentId, majorVersion, minorVersion, buildId);
    }

    /**
     * insert into {@link io.netty.buffer.ByteBuf} for Netty, BigEndian.
     * 
     * @param buf
     */
    void insertIntoByteBuf(ByteBuf buf) {
        buf.writeShort(componentId);
        buf.writeShort(majorVersion);
        buf.writeShort(minorVersion);
        buf.writeInt(buildId);
    }

    /**
     * insert into {@link io.netty.buffer.ByteBuf} for Netty, Little Endian.
     * 
     * @param buf
     */
    void insertIntoByteBufLE(ByteBuf buf) {
        buf.writeShortLE(componentId);
        buf.writeShortLE(majorVersion);
        buf.writeShortLE(minorVersion);
        buf.writeIntLE(buildId);
    }

    /**
     * extract From {@link java.nio.ByteBuffer} for NIO.
     * 
     * @param buf
     */
    static Version extractFromByteBuffer(ByteBuffer buf) {
        short componentId = buf.getShort();
        short majorVersion = buf.getShort();
        short minorVersion = buf.getShort();
        int buildId = buf.getInt();
        return new Version(componentId, majorVersion, minorVersion, buildId);
    }

    /**
     * insert into {@link java.nio.ByteBuffer} for NIO
     * 
     * @param buf
     */
    void insertIntoByteBuffer(ByteBuffer buf) {
        buf.putShort(componentId);
        buf.putShort(majorVersion);
        buf.putShort(minorVersion);
        buf.putInt(buildId);
    }
}
