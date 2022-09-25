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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.ListenerConstants;
import org.trafodion.dcs.master.listener.Util;
import io.netty.buffer.ByteBuf;

public final class ConnectMessage {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectMessage.class);

    private final String datasource;
    private final String catalog;
    private final String schema;
    private final String location;
    private final String userRole;

    private final short accessMode;
    private final short autoCommit;
    private final int queryTimeoutSec;
    private final int idleTimeoutSec;
    private final int loginTimeoutSec;
    private final short txnIsolationLevel;
    private final short rowSetSize;

    private final int diagnosticFlag;
    private final int processId;

    private final String computerName;
    private final String windowText;

    private final int ctxACP;
    private final int ctxDataLang;
    private final int ctxErrorLang;
    private final short ctxCtrlInferNXHAR;

    private final short cpuToUse;
    private final short cpuToUseEnd;
    private final String connectOptions;

    private final Version[] clientVersions;
    private final UserDesc user;

    private final int srvrType;
    private final short retryCount;
    private final int optionFlags1;
    private final int optionFlags2;

    private final String vproc;
    private final String client;

    private final HashMap<String, String> attributes;
    private final String ccExtention;

    private ConnectMessage(String datasource, String catalog, String schema, String location,
            String userRole, short accessMode, short autoCommit, int queryTimeoutSec,
            int idleTimeoutSec, int loginTimeoutSec, short txnIsolationLevel, short rowSetSize,
            int diagnosticFlag, int processId, String computerName, String windowText, int ctxACP,
            int ctxDataLang, int ctxErrorLang, short ctxCtrlInferNXHAR, short cpuToUse,
            short cpuToUseEnd, String connectOptions, Version[] clientVersions, UserDesc user,
            int srvrType, short retryCount, int optionFlags1, int optionFlags2, String vproc,
            String client, HashMap<String, String> attributes, String ccExtention) {
        super();
        this.datasource = datasource;
        this.catalog = catalog;
        this.schema = schema;
        this.location = location;
        this.userRole = userRole;
        this.accessMode = accessMode;
        this.autoCommit = autoCommit;
        this.queryTimeoutSec = queryTimeoutSec;
        this.idleTimeoutSec = idleTimeoutSec;
        this.loginTimeoutSec = loginTimeoutSec;
        this.txnIsolationLevel = txnIsolationLevel;
        this.rowSetSize = rowSetSize;
        this.diagnosticFlag = diagnosticFlag;
        this.processId = processId;
        this.computerName = computerName;
        this.windowText = windowText;
        this.ctxACP = ctxACP;
        this.ctxDataLang = ctxDataLang;
        this.ctxErrorLang = ctxErrorLang;
        this.ctxCtrlInferNXHAR = ctxCtrlInferNXHAR;
        this.cpuToUse = cpuToUse;
        this.cpuToUseEnd = cpuToUseEnd;
        this.connectOptions = connectOptions;
        this.clientVersions = clientVersions;
        this.user = user;
        this.srvrType = srvrType;
        this.retryCount = retryCount;
        this.optionFlags1 = optionFlags1;
        this.optionFlags2 = optionFlags2;
        this.vproc = vproc;
        this.client = client;
        this.attributes = attributes;
        this.ccExtention = ccExtention;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("datasource : ").append(datasource).append(", ");
        sb.append("catalog : ").append(catalog).append(", ");
        sb.append("schema : ").append(schema).append(", ");
        sb.append("location : ").append(location).append(", ");
        sb.append("userRole : ").append(userRole).append(", ");
        sb.append("accessMode : ").append(accessMode).append(", ");
        sb.append("autoCommit : ").append(autoCommit).append(", ");
        sb.append("queryTimeoutSec : ").append(queryTimeoutSec).append(", ");
        sb.append("idleTimeoutSec : ").append(idleTimeoutSec).append(", ");
        sb.append("loginTimeoutSec : ").append(loginTimeoutSec).append(", ");
        sb.append("txnIsolationLevel : ").append(txnIsolationLevel).append(", ");
        sb.append("rowSetSize : ").append(rowSetSize).append(", ");
        sb.append("diagnosticFlag : ").append(diagnosticFlag).append(", ");
        sb.append("processId : ").append(processId).append(", ");
        sb.append("computerName : ").append(computerName).append(", ");
        sb.append("windowText : ").append(windowText).append(", ");
        sb.append("ctxACP : ").append(ctxACP).append(", ");
        sb.append("ctxDataLang : ").append(ctxDataLang).append(", ");
        sb.append("ctxErrorLang : ").append(ctxErrorLang).append(", ");
        sb.append("ctxCtrlInferNXHAR : ").append(ctxCtrlInferNXHAR).append(", ");
        sb.append("cpuToUse : ").append(cpuToUse).append(", ");
        sb.append("cpuToUseEnd : ").append(cpuToUseEnd).append(", ");
        sb.append("connectOptions : ").append(connectOptions).append(", ");
        if (clientVersions.length > 0) {
            sb.append("clientVersionList : [");
            for (Version version : clientVersions) {
                sb.append("version : ").append(version).append(".  ");
            }
            sb.append("], ");
        }
        sb.append("user : ").append(user).append(", ");
        sb.append("srvrType : ").append(srvrType).append(", ");
        sb.append("retryCount : ").append(retryCount).append(", ");
        sb.append("optionFlags1 : ").append(optionFlags1).append(", ");
        sb.append("optionFlags2 : ").append(optionFlags2).append(", ");
        sb.append("vproc : ").append(vproc).append(", ");
        sb.append("client : ").append(client).append(", ");
        sb.append("ccExtention : ").append(ccExtention).append(".");

        return sb.toString();
    }

    public String getDatasource() {
        return datasource;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getLocation() {
        return location;
    }

    public String getUserRole() {
        return userRole;
    }

    public short getAccessMode() {
        return accessMode;
    }

    public short getAutoCommit() {
        return autoCommit;
    }

    public int getQueryTimeoutSec() {
        return queryTimeoutSec;
    }

    public int getIdleTimeoutSec() {
        return idleTimeoutSec;
    }

    public int getLoginTimeoutSec() {
        return loginTimeoutSec;
    }

    public short getTxnIsolationLevel() {
        return txnIsolationLevel;
    }

    public short getRowSetSize() {
        return rowSetSize;
    }

    public int getDiagnosticFlag() {
        return diagnosticFlag;
    }

    public int getProcessId() {
        return processId;
    }

    public String getComputerName() {
        return computerName;
    }

    public String getWindowText() {
        return windowText;
    }

    public int getCtxACP() {
        return ctxACP;
    }

    public int getCtxDataLang() {
        return ctxDataLang;
    }

    public int getCtxErrorLang() {
        return ctxErrorLang;
    }

    public short getCtxCtrlInferNXHAR() {
        return ctxCtrlInferNXHAR;
    }

    public short getCpuToUse() {
        return cpuToUse;
    }

    public short getCpuToUseEnd() {
        return cpuToUseEnd;
    }

    public String getConnectOptions() {
        return connectOptions;
    }

    public Version[] getClientVersions() {
        return clientVersions;
    }

    public int getClientComponentId() {
        // see ListenerConstants.JDBC_DRVR_COMPONENT
        return clientVersions[0].getComponentId();
    }

    public UserDesc getUser() {
        return user;
    }

    public int getSrvrType() {
        return srvrType;
    }

    public short getRetryCount() {
        return retryCount;
    }

    public int getOptionFlags1() {
        return optionFlags1;
    }

    public int getOptionFlags2() {
        return optionFlags2;
    }

    public String getVproc() {
        return vproc;
    }

    public String getClient() {
        return client;
    }

    public HashMap<String, String> getAttributes() {
        return attributes;
    }

    public String getCcExtention() {
        return ccExtention;
    }

    /**
     * extract From {@link java.nio.ByteBuffer} for NIO.
     * 
     * @param buf
     */
    public static ConnectMessage extractFromByteBuffer(ByteBuffer buf) throws IOException {
        String datasource = Util.extractString(buf);
        String catalog = Util.extractString(buf);
        String schema = Util.extractString(buf);
        String location = Util.extractString(buf);
        String userRole = Util.extractString(buf);

        short accessMode = buf.getShort();
        short autoCommit = buf.getShort();
        int queryTimeoutSec = buf.getInt();
        int idleTimeoutSec = buf.getInt();
        int loginTimeoutSec = buf.getInt();
        short txnIsolationLevel = buf.getShort();
        short rowSetSize = buf.getShort();

        int diagnosticFlag = buf.getInt();
        int processId = buf.getInt();

        String computerName = Util.extractString(buf);
        String windowText = Util.extractString(buf);

        int ctxACP = buf.getInt();
        int ctxDataLang = buf.getInt();
        int ctxErrorLang = buf.getInt();
        short ctxCtrlInferNXHAR = buf.getShort();

        short cpuToUse = buf.getShort();
        short cpuToUseEnd = buf.getShort();
        String connectOptions = Util.extractString(buf);

        // VersionList
        int len = buf.getInt();
        Version[] versions = new Version[len];
        for (int i = 0; i < len; i++) {
            versions[i] = Version.extractFromByteBuffer(buf);
        }

        // UserDesc
        UserDesc user = UserDesc.extractFromByteBuffer(buf);


        int srvrType = buf.getInt();
        short retryCount = buf.getShort();
        int optionFlags1 = buf.getInt();
        int optionFlags2 = buf.getInt();
        String vproc = Util.extractString(buf);
        String client = Util.extractString(buf);
        /*
         * ccExtention = String sessionName String clientIpAddress String clientHostName String
         * userName String roleName String applicationName
         */
        String ccExtention = null;
        boolean bExtention = false;
        if (buf.limit() > buf.position()) {
            ccExtention = Util.extractString(buf);
            bExtention = true;
        } else {
            ccExtention = "{}";
            bExtention = false;
        }
        HashMap<String, String> attributes = new HashMap<>();

        if (bExtention) {
            try {
                JSONObject jsonObj = new JSONObject(ccExtention);
                Iterator<?> it = jsonObj.keys();

                while (it.hasNext()) {
                    String key = it.next().toString().trim();
                    String value = jsonObj.get(key).toString().trim();
                    if (Constants.TENANT_NAME.equals(key) && value.length() > 0) {
                        value = value.toUpperCase();
                    }
                    attributes.put(key, value);
                }
            } catch (JSONException e) {
                LOG.error("JSONException : <{}>", e.getMessage(), e);
            }
            LOG.debug("ConnectMessage attributes {}.", attributes);
        }
        return new ConnectMessage(datasource, catalog, schema, location, userRole, accessMode,
                autoCommit, queryTimeoutSec, idleTimeoutSec, loginTimeoutSec, txnIsolationLevel,
                rowSetSize, diagnosticFlag, processId, computerName, windowText, ctxACP,
                ctxDataLang, ctxErrorLang, ctxCtrlInferNXHAR, cpuToUse, cpuToUseEnd, connectOptions,
                versions, user, srvrType, retryCount, optionFlags1, optionFlags2, vproc, client,
                attributes, ccExtention);

    }

    /**
     * extract From {@link io.netty.buffer.ByteBuf} for Netty, Big Endian.
     * 
     * @param buf
     */
    public static ConnectMessage extractFromByteBuf(ByteBuf buf) throws IOException {
        String datasource = Util.extractString(buf);
        String catalog = Util.extractString(buf);
        String schema = Util.extractString(buf);
        String location = Util.extractString(buf);
        String userRole = Util.extractString(buf);

        short accessMode = buf.readShort();
        short autoCommit = buf.readShort();
        int queryTimeoutSec = buf.readInt();
        int idleTimeoutSec = buf.readInt();
        int loginTimeoutSec = buf.readInt();
        short txnIsolationLevel = buf.readShort();
        short rowSetSize = buf.readShort();

        int diagnosticFlag = buf.readInt();
        int processId = buf.readInt();

        String computerName = Util.extractString(buf);
        String windowText = Util.extractString(buf);

        int ctxACP = buf.readInt();
        int ctxDataLang = buf.readInt();
        int ctxErrorLang = buf.readInt();
        short ctxCtrlInferNXHAR = buf.readShort();

        short cpuToUse = buf.readShort();
        short cpuToUseEnd = buf.readShort();
        String connectOptions = Util.extractString(buf);

        // VersionList
        int len = buf.readInt();
        Version[] versions = new Version[len];
        for (int i = 0; i < len; i++) {
            versions[i] = Version.extractFromByteBuf(buf);
        }

        // UserDesc
        UserDesc user = UserDesc.extractFromByteBuf(buf);

        int srvrType = buf.readInt();
        short retryCount = buf.readShort();
        int optionFlags1 = buf.readInt();
        int optionFlags2 = buf.readInt();
        String vproc = Util.extractString(buf);
        String client = Util.extractString(buf);
        /*
         * ccExtention = String sessionName String clientIpAddress String clientHostName String
         * userName String roleName String applicationName
         */
        String ccExtention = null;
        boolean bExtention = false;
        if (buf.readableBytes() > 0) {
            ccExtention = Util.extractString(buf);
            bExtention = true;
        } else {
            ccExtention = "{}";
            bExtention = false;
        }
        HashMap<String, String> attributes = new HashMap<>();

        if (bExtention) {
            try {
                JSONObject jsonObj = new JSONObject(ccExtention);
                Iterator<?> it = jsonObj.keys();

                while (it.hasNext()) {
                    String key = it.next().toString().trim();
                    String value = jsonObj.get(key).toString().trim();
                    if (Constants.TENANT_NAME.equals(key) && value.length() > 0) {
                        value = value.toUpperCase();
                    }
                    attributes.put(key, value);
                }
            } catch (JSONException e) {
                LOG.error("JSONException : <{}>", e.getMessage(), e);
            }
            LOG.debug("ConnectMessage attributes {}.", attributes);
        }

        return new ConnectMessage(datasource, catalog, schema, location, userRole, accessMode,
                autoCommit, queryTimeoutSec, idleTimeoutSec, loginTimeoutSec, txnIsolationLevel,
                rowSetSize, diagnosticFlag, processId, computerName, windowText, ctxACP,
                ctxDataLang, ctxErrorLang, ctxCtrlInferNXHAR, cpuToUse, cpuToUseEnd, connectOptions,
                versions, user, srvrType, retryCount, optionFlags1, optionFlags2, vproc, client,
                attributes, ccExtention);
    }
}
