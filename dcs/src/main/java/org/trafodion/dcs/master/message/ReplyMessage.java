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
import java.util.List;
import java.util.Map;
import org.python.antlr.ast.Str;
import org.trafodion.dcs.master.listener.ListenerConstants;
import org.trafodion.dcs.master.listener.Util;
import io.netty.buffer.ByteBuf;

public final class ReplyMessage {
    private final ReplyExceptionMessage exception;
    private final int dialogueId;
    private final String dataSource;
    private final byte[] userSid;
    private final Version[] versions;
    private final int isoMapping;
    private final String serverHostName;
    private final int serverNodeId;
    private final int serverProcessId;
    private final String serverProcessName;
    private final String serverIpAddress;
    private final int serverPort;
    private final long timestamp;
    private final String clusterName;
    private final String enableSSL;
    private boolean replyException;
    private List<Integer> dialogueIdList;
    private String masterIps;
    private String isEmptyEqualsNull;

    public ReplyExceptionMessage getException() {
        return exception;
    }

    public int getDialogueId() {
        return dialogueId;
    }

    public String getDataSource() {
        return dataSource;
    }

    public byte[] getUserSid() {
        return userSid;
    }

    public Version[] getVersions() {
        return versions;
    }

    public int getIsoMapping() {
        return isoMapping;
    }

    public String getServerHostName() {
        return serverHostName;
    }

    public int getServerNodeId() {
        return serverNodeId;
    }

    public int getServerProcessId() {
        return serverProcessId;
    }

    public String getServerProcessName() {
        return serverProcessName;
    }

    public String getServerIpAddress() {
        return serverIpAddress;
    }

    public int getServerPort() {
        return serverPort;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getEnableSSL() {
        return enableSSL;
    }

    public boolean isReplyException() {
        return replyException;
    }

    public void enableBase64() {
        versions[0].modifyVersionBuildId(ListenerConstants.PASSWORD_SECURITY_BASE64);
    }

    /**
     * Instance for reply message which has exception
     *
     */
    public ReplyMessage(ReplyExceptionMessage exception) {
        super();
        this.exception = exception;
        this.dialogueId = 0;
        this.dataSource = null;
        this.userSid = null;
        this.versions = new Version[2];;
        this.isoMapping = 0;
        this.serverHostName = null;
        this.serverNodeId = 0;
        this.serverProcessId = 0;
        this.serverProcessName = null;
        this.serverIpAddress = null;
        this.serverPort = 0;
        this.timestamp = 0;
        this.clusterName = null;
        this.enableSSL = null;
        this.replyException = true;
    }

    public ReplyMessage(List<Integer> list) {
        super();
        this.exception = null;
        this.dialogueId = 0;
        this.dataSource = null;
        this.userSid = null;
        this.versions = new Version[2];;
        this.isoMapping = 0;
        this.serverHostName = null;
        this.serverNodeId = 0;
        this.serverProcessId = 0;
        this.serverProcessName = null;
        this.serverIpAddress = null;
        this.serverPort = 0;
        this.timestamp = 0;
        this.clusterName = null;
        this.enableSSL = null;
        this.replyException = true;
        this.dialogueIdList = list;
    }

    /**
     * Instance for reply message which dispatch mxosrvr successfully
     * 
     */
    public ReplyMessage(int dialogueId, String dataSource, byte[] userSid, Version[] versions,
            int isoMapping, String serverHostName, int serverNodeId, int serverProcessId,
            String serverProcessName, String serverIpAddress, int serverPort, long timestamp,
            String clusterName, String enableSSL) {
        super();
        this.exception = new ReplyExceptionMessage();
        this.replyException = false;
        this.dialogueId = dialogueId;
        this.dataSource = dataSource;
        this.userSid = userSid;
        this.versions = versions;
        this.isoMapping = isoMapping;
        this.serverHostName = serverHostName;
        this.serverNodeId = serverNodeId;
        this.serverProcessId = serverProcessId;
        this.serverProcessName = serverProcessName;
        this.serverIpAddress = serverIpAddress;
        this.serverPort = serverPort;
        this.timestamp = timestamp;
        this.clusterName = clusterName;
        this.enableSSL = enableSSL;
    }

    public ReplyMessage(String masterIps, String isEmptyEqualsNull) {
        super();
        this.exception = null;
        this.dialogueId = 0;
        this.dataSource = null;
        this.userSid = null;
        this.versions = new Version[2];;
        this.isoMapping = 0;
        this.serverHostName = null;
        this.serverNodeId = 0;
        this.serverProcessId = 0;
        this.serverProcessName = null;
        this.serverIpAddress = null;
        this.serverPort = 0;
        this.timestamp = 0;
        this.clusterName = null;
        this.enableSSL = null;
        this.replyException = true;
        this.masterIps = masterIps;
        this.isEmptyEqualsNull = isEmptyEqualsNull;
    }

    /**
     * insert into {@link java.nio.ByteBuffer} for NIO
     * 
     * @param buf
     */
    public void insertIntoByteBuffer(ByteBuffer buf) throws IOException {
        exception.insertIntoByteBuffer(buf);
        if (!isReplyException()) {
            buf.putInt(dialogueId);
            Util.insertString(dataSource, buf);
            Util.insertByteString(userSid, buf);
            buf.putInt(versions.length);
            for (int i = 0; i < versions.length; i++) {
                versions[i].insertIntoByteBuffer(buf);
            }
            buf.putInt(isoMapping);
            Util.insertString(serverHostName, buf);
            buf.putInt(serverNodeId);
            buf.putInt(serverProcessId);
            Util.insertString(serverProcessName, buf);
            Util.insertString(serverIpAddress, buf);
            buf.putInt(serverPort);
            buf.putLong(timestamp);
            Util.insertString(clusterName, buf);
            Util.insertString(enableSSL, buf);
        }
    }

    /**
     * insert into {@link java.nio.ByteBuffer} for NIO
     * @param buf
     * @param mode To compatible with ODBC. In ODBC, even there meets exception it will decode the buffer.
     * @throws IOException
     */
    public void insertIntoByteBufferCompatible(ByteBuffer buf, boolean mode) throws IOException {
        if (mode) {
            replyException = false;//set false to add info to buffer which ODBC needed.
            
            //set default versions, same as ConnectReply.java
            versions[0] = new Version((short) ListenerConstants.DCS_MASTER_COMPONENT,
                    (short) ListenerConstants.DCS_MASTER_VERSION_MAJOR_1,
                    (short) ListenerConstants.DCS_MASTER_VERSION_MINOR_0,
                    ListenerConstants.DCS_MASTER_BUILD_1 | ListenerConstants.CHARSET
                            | ListenerConstants.PASSWORD_SECURITY);
            versions[1] = new Version(
                    (short) (ListenerConstants.MXOSRVR_ENDIAN + ListenerConstants.ODBC_SRVR_COMPONENT),
                    (short) ListenerConstants.MXOSRVR_VERSION_MAJOR,
                    (short) ListenerConstants.MXOSRVR_VERSION_MINOR,
                    ListenerConstants.MXOSRVR_VERSION_BUILD);
            
        }
        insertIntoByteBuffer(buf);
    }

    /**
     * insert into {@link io.netty.buffer.ByteBuf} for Netty, Little Endian.
     * 
     * @param buf
     */
    public void insertIntoByteBufLE(ByteBuf buf) throws UnsupportedEncodingException {
        exception.insertIntoByteBufLE(buf);

        if (!isReplyException()) {
            buf.writeIntLE(dialogueId);
            Util.insertStringLE(dataSource, buf);
            Util.insertByteStringLE(userSid, buf);
            buf.writeIntLE(versions.length);
            for (int i = 0; i < versions.length; i++) {
                versions[i].insertIntoByteBufLE(buf);
            }
            buf.writeIntLE(isoMapping);
            Util.insertStringLE(serverHostName, buf);
            buf.writeIntLE(serverNodeId);
            buf.writeIntLE(serverProcessId);
            Util.insertStringLE(serverProcessName, buf);
            Util.insertStringLE(serverIpAddress, buf);
            buf.writeIntLE(serverPort);
            buf.writeLongLE(timestamp);
            Util.insertStringLE(clusterName, buf);
            Util.insertStringLE(enableSSL, buf);
        }
    }

    /**
     * insert into {@link io.netty.buffer.ByteBuf} for Netty, Little Endian.
     * @param buf
     * @param mode To compatible with ODBC. In ODBC, even there meets exception it will decode the buffer.
     * @throws IOException
     */
    public void insertIntoByteBufLECompatible(ByteBuf buf, boolean mode) throws IOException {
        if (mode) {
            replyException = false;//set false to add info to buffer which ODBC needed.
        }
        insertIntoByteBufLE(buf);
    }

    public void insertIntoByteBuffer(ByteBuffer buf, int keepAlive) throws IOException {
        buf.putInt(keepAlive);
        buf.putInt(dialogueIdList.size());
        if(dialogueIdList.size() > 0){
            for(int id : dialogueIdList){
                buf.putInt(id);
            }
        }
    }

    public void insertIntoByteBuffer(ByteBuffer buf, int api,int checkmaster) throws IOException {
        buf.putInt(api);
        if(isEmptyEqualsNull != null && isEmptyEqualsNull.equalsIgnoreCase("ENABLE")){
            //set 0 while isEmptyEqualsNull ENABLE
            buf.putInt(0);
        }else{
            buf.putInt(1);
        }
        Util.insertString(masterIps, buf);
    }

}
