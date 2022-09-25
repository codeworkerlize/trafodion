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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.master.listener.Util;
import io.netty.buffer.ByteBuf;

public class UserDesc {
    private static final Logger LOG = LoggerFactory.getLogger(UserDesc.class);

    private final int userDescType;
    private final byte[] userSid;
    private final String domainName;
    private final String userName;
    private final String password;

    public int getUserDescType() {
        return userDescType;
    }

    public byte[] getUserSid() {
        return userSid;
    }

    public String getDomainName() {
        return domainName;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public UserDesc(int userDescType, byte[] userSid, String domainName, String userName,
            String password) {
        super();
        this.userDescType = userDescType;
        this.userSid = userSid;
        this.domainName = domainName;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("userDescType : ").append(userDescType).append(", userSid : ")
                .append(new String(userSid)).append(", domainName : ").append(domainName)
                .append(", userName : ").append(userName);

        return sb.toString();
    }

    /**
     * extract From {@link java.nio.ByteBuffer} for NIO.
     * 
     * @param buf
     */
    public static UserDesc extractFromByteBuffer(ByteBuffer buf) throws IOException {
        int userDescType = buf.getInt();
        byte[] userSid = Util.extractByteArray(buf);
        String domainName = Util.extractString(buf);
        String userName = Util.extractString(buf);
        String password = Util.extractString(buf);
        return new UserDesc(userDescType, userSid, domainName, userName, password);
    }

    /**
     * extract From {@link io.netty.buffer.ByteBuf} for Netty, Big Endian.
     * 
     * @param buf
     */
    public static UserDesc extractFromByteBuf(ByteBuf buf) throws IOException {
        int userDescType = buf.readInt();
        byte[] userSid = Util.extractByteArray(buf);
        String domainName = Util.extractString(buf);
        String userName = Util.extractString(buf);
        String password = Util.extractString(buf);
        return new UserDesc(userDescType, userSid, domainName, userName, password);
    }

}
