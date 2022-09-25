/**
* @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

* @@@ END COPYRIGHT @@@
 */
package org.trafodion.dcs.master.listener;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.nio.BufferUnderflowException;
import java.sql.SQLException;
import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.message.CancelMessage;
import org.trafodion.dcs.master.message.ReplyExceptionMessage;
import org.trafodion.dcs.master.message.ReplyMessage;
import org.trafodion.dcs.script.ScriptContext;
import org.trafodion.dcs.script.ScriptManager;
import org.trafodion.dcs.util.GetJavaProperty;
import org.trafodion.dcs.zookeeper.ZkClient;


public class RequestCancelQuery {
    private static final Logger LOG = LoggerFactory.getLogger(RequestCancelQuery.class);

    private ZkClient zkc = null;
    private String parentDcsZnode = "";
    private ScriptContext scriptContext = new ScriptContext();

    public RequestCancelQuery(ConfigReader configReader){
        this.zkc = configReader.getZkc();
        this.parentDcsZnode = configReader.getParentDcsZnode();
    }


    public void processRequest(Data data) {
        if (LOG.isInfoEnabled()) {
            LOG.info("ENTRY. Cancel Queryk...");
        }
        boolean cancelConnection = false;
        SocketAddress s = data.getClientSocketAddress();
        CancelMessage cancelMessage = data.getCancelMessage();

        try {
            // get input
            int dialogueId = cancelMessage.getDialogueId();
            int srvrType = cancelMessage.getSrvrType();
            String srvrObjRef = cancelMessage.getSrvrObjRef();
            int stopType = cancelMessage.getStopType();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "<{}>. dialogueId : <{}>, srvrType : <{}>, srvrObjRef : <{}>, stopType : <{}>",
                        s, dialogueId, srvrType, srvrObjRef, stopType);
            }
            String sPort;
            String qid = null;
            String sqlString = null;
            if (srvrObjRef.startsWith("TCP:")){ //ODBC --- TCP:<IpAddress>/<portNumber>:ODBC
                String[] st = srvrObjRef.split(":");
                String ip[] = st[1].split("/");
                sPort = ip[1];
            } else {
                String[] st = srvrObjRef.split(":");          //JDBC --- port #
                sPort = st[0];
                if (st.length > 1) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("srvrObjRef : <{}>", srvrObjRef);
                    }
                    qid = st[1];
                }
                if (st.length > 2) {
                    sqlString = st[2];
                    if(sqlString.trim().length() > 256) sqlString.trim().substring(0, 255);
                }
            }
            int port = Integer.parseInt(sPort);
            // process request
            String registeredPath = parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED;
            String nodeRegisteredPath = "";
            List<String> servers = null;
            Stat stat = null;
            String nodeData = "";
            boolean found = false;
            String errorText = "";
            int nodeId = 0;
            int processId = 0;
            
            if (!registeredPath.startsWith("/"))
                registeredPath = "/" + registeredPath;
    
            zkc.sync(registeredPath,null,null);
            servers =  zkc.getChildren(registeredPath, null);
            if (servers.isEmpty()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Servers is Empty.");
                }
            } else {
                for(String server : servers) {
                    nodeRegisteredPath = registeredPath + "/" + server;
                    stat = zkc.exists(nodeRegisteredPath,false);
                    if(stat != null){
                        nodeData = new String(zkc.getData(nodeRegisteredPath, false, stat));
                        if (!nodeData.startsWith("CONNECTED:") && !nodeData.startsWith("CONNECTING:")) {
                            //do nothing
                        } else {
                            String[] stData = nodeData.split(":");
                            if (dialogueId == Long.parseLong(stData[2]) && port == Integer.parseInt(stData[7])){
                                nodeId=Integer.parseInt(stData[3]);
                                processId=Integer.parseInt(stData[4]);
                                found = true;
                                break;
                            }
                        }
                    }
                }
                if (found){
                    if (LOG.isInfoEnabled()) {
                        LOG.info(
                                "<{}>. Server found - dialogueId : <{}>, port : <{}>, nodeId : <{}>, processId : <{}>, begin to cancel query",
                                s, dialogueId, port, nodeId, processId);
                    }
                    if (!nodeData.startsWith("CONNECTING:")) {
                        //errorText = cancelQuery(nodeId, processId);
                        long  startTime = System.currentTimeMillis();
                        if (qid == null || "null".equals(qid.trim().toLowerCase())) {
                            String queryIdAndSqlString = getQueryIdAndSqlString(nodeId, processId);
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Get queryId and sqlString <{}> from sqlci.",
                                        queryIdAndSqlString);
                            }
                            int length = queryIdAndSqlString.split(":").length;
                            if (length > 1) {
                                String queryId = queryIdAndSqlString.split(":")[0];
                                qid = queryId.substring(0, (queryId.length() - 6)).trim();
                                String curSqlString = queryIdAndSqlString.split(":")[1].trim();
                                if(sqlString != null && !curSqlString
                                        .equalsIgnoreCase(sqlString.trim())) {
                                    throw new SQLException("SQL String does not match. Skip cancel query.");
                                }
                            } else {
                                qid = queryIdAndSqlString;
                            }
                        }
                        nodeData = nodeData.replace("CONNECTED", "CANCEL,"+qid);
                        zkc.setData(nodeRegisteredPath, nodeData.getBytes(), zkc.exists(nodeRegisteredPath, true).getVersion());
                        errorText = cancelQuery(qid);
                        if (System.currentTimeMillis() - startTime > 4000) {
                            throw new SQLException("Cancel time out...");
                        }
                    }
                } else {
                    errorText = "Server not found. Skip cancel query.";
                    if (LOG.isInfoEnabled()) {
                        LOG.info("<{}>. Server not found - dialogueId : <{}>, port : <{}>.",
                                s, dialogueId, port);
                    }
                }
            }
            // build output
            ReplyMessage reply = null;
            if (!errorText.equals("")) {
                ReplyExceptionMessage exception = new ReplyExceptionMessage(
                        ListenerConstants.DcsMasterParamError_exn, 0, errorText);
                reply = new ReplyMessage(exception);
            } else {
                reply = new ReplyMessage(new ReplyExceptionMessage());
            }
            data.setReplyMessage(reply);
        } catch (UnsupportedEncodingException ue){
            LOG.error("RequestCancelQuery.UnsupportedEncodingException: <{}> : <{}>.", s, ue.getMessage(), ue);
            cancelConnection = true;
        } catch (KeeperException ke){
            LOG.error("RequestCancelQuery.KeeperException: <{}> : <{}>.", s, ke.getMessage(), ke);
            cancelConnection = true;
        } catch (InterruptedException ie){
            LOG.error("RequestCancelQuery.InterruptedException: <{}> : <{}>.", s, ie.getMessage(), ie);
            cancelConnection = true;
        } catch (BufferUnderflowException e){
            LOG.error("RequestCancelQuery.BufferUnderflowException: <{}> : <{}>.", s, e.getMessage(), e);
            cancelConnection = true;
        } catch (IOException e) {
            LOG.error("RequestCancelQuery.IOException: <{}> : <{}>.", s, e.getMessage(), e);
            cancelConnection = true;
        } catch (SQLException e) {
            LOG.error("RequestCancelQuery.SQLException: <{}> : <{}>.", s, e.getMessage(), e);
            cancelConnection = true;
        }
        if (cancelConnection) {
            data.setRequestReply(ListenerConstants.REQUST_CLOSE);
        } else {
            data.setRequestReply(ListenerConstants.REQUST_WRITE_EXCEPTION);
        }
    }

    String getQueryIdAndSqlString(int nodeId, int processId) throws SQLException {
        String errorText = null;
        String queryIdAndSqlString = "";

        scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
        scriptContext.setStripStdOut(true);
        scriptContext.setStripStdErr(true);
        scriptContext.cleanStdDatas();

        String dcsHome = GetJavaProperty.getDcsHome();
        scriptContext.setCommand("cd "+ dcsHome
                + " ;bin/scripts/getqid.sh "
                + processId + " " + nodeId);

        try {
            ScriptManager.getInstance().runScript(scriptContext);// Blocking call

            String stdOut = scriptContext.getStdOut().toString();
            if (scriptContext.getExitCode() != 0) {
                int start = stdOut.indexOf("ERROR");
                int end = stdOut.indexOf("---");

                errorText = stdOut.substring(start, end)
                        + "; exitcode:"
                        + scriptContext.getExitCode();
            } else {
                int start = stdOut.indexOf("QUERY_ID") + 8;
                int end = stdOut.lastIndexOf("---");
                queryIdAndSqlString = stdOut.substring(start, end);
                queryIdAndSqlString = queryIdAndSqlString.replaceAll("-", "").trim();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            errorText = "Query cancel get qid failed. " + e.getMessage();
        }

        if(queryIdAndSqlString.endsWith("_PUBLICATION")) {
            errorText = "cancelQuery: Publication Query - Cancel Query Request is ignored.";
            if (LOG.isInfoEnabled()) {
                LOG.info(errorText);
            }
        }

        if (errorText != null) {
            throw new SQLException(errorText);
        }

        return queryIdAndSqlString;

    }

    String cancelQuery(String qid){
        if (LOG.isInfoEnabled()) {
            LOG.info("Start to cancel query : <{}>", qid);
        }
        String errorText = "";
        scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
        scriptContext.setStripStdOut(true);
        scriptContext.setStripStdErr(true);
        scriptContext.cleanStdDatas();

        String dcsHome = GetJavaProperty.getDcsHome();
        scriptContext.setCommand("cd "+ dcsHome
                + " ;bin/scripts/query_cancel.sh "
                + qid);
        try {
            ScriptManager.getInstance().runScript(scriptContext);// Blocking call

            if(scriptContext.getExitCode() != 0) {
                if(scriptContext.getExitCode() == 1) {
                    String stdOut = scriptContext.getStdOut().toString();
                    int start = stdOut.indexOf("ERROR");
                    int end = stdOut.indexOf("---");
                    errorText = stdOut.substring(start, end)
                            + "; exitcode:"
                            + scriptContext.getExitCode();
                } else {
                    errorText = scriptContext.getStdErr().append("; exitcode:")
                            .append(scriptContext.getExitCode())
                            .toString();
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            errorText = "Query cancel failed. " + e.getMessage();
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("cancelQuery errorText : <{}>", errorText);
        }
        return errorText;
    }
    
}
