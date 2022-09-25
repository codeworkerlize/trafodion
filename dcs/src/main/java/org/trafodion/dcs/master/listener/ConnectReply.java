//
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

// @@@ END COPYRIGHT @@@
//
package org.trafodion.dcs.master.listener;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.nio.NoAvailServerException;
import org.trafodion.dcs.master.message.Header;
import org.trafodion.dcs.master.message.ReplyExceptionMessage;
import org.trafodion.dcs.master.message.ReplyMessage;
import org.trafodion.dcs.master.message.Version;
import org.trafodion.dcs.util.Bytes;
import org.trafodion.dcs.zookeeper.ZkClient;
import com.esgyn.common.ASNodes;
import com.esgyn.common.NodeUnitCounters;
import com.esgyn.common.TenantNodes;

public class ConnectReply {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectReply.class);
    private static final TenantNodes allNodes = new ASNodes();

    private ZkClient zkc = null;
    private String parentDcsZnode = "";
    private Version[] versions = null;
    private Random random = null;

    private ConfigReader configReader;
    private ReplyMessage replyMessage;

    ConnectReply(ConfigReader configReader){
        this.configReader = configReader;
        zkc = configReader.getZkc();
        parentDcsZnode = configReader.getParentDcsZnode();
        random = new Random();
        versions = new Version[2];
        if(configReader.getEncryptBase64Enable() > 0) {
            versions[0] = new Version((short) ListenerConstants.DCS_MASTER_COMPONENT,
                    (short) ListenerConstants.DCS_MASTER_VERSION_MAJOR_1,
                    (short) ListenerConstants.DCS_MASTER_VERSION_MINOR_0,
                    ListenerConstants.DCS_MASTER_BUILD_1 | ListenerConstants.CHARSET
                    | ListenerConstants.PASSWORD_SECURITY | ListenerConstants.PASSWORD_SECURITY_BASE64);
        } else {
            versions[0] = new Version((short) ListenerConstants.DCS_MASTER_COMPONENT,
                    (short) ListenerConstants.DCS_MASTER_VERSION_MAJOR_1,
                    (short) ListenerConstants.DCS_MASTER_VERSION_MINOR_0,
                    ListenerConstants.DCS_MASTER_BUILD_1 | ListenerConstants.CHARSET
                    | ListenerConstants.PASSWORD_SECURITY);
        }
        versions[1] = new Version(
                (short) (ListenerConstants.MXOSRVR_ENDIAN + ListenerConstants.ODBC_SRVR_COMPONENT),
                (short) ListenerConstants.MXOSRVR_VERSION_MAJOR,
                (short) ListenerConstants.MXOSRVR_VERSION_MINOR,
                ListenerConstants.MXOSRVR_VERSION_BUILD);
    }

    public boolean checkWhitelist(ConnectionContext cc) {

        // 0 --- do not Forced close || 1 --- Forced close Whitelist
        if (configReader.isForcedCloseWhitelist() == 0) {
            try {
                //ip whitelist check
                configReader.getMapping().checkWhitelist(cc);
            } catch (Exception e) {
                cc.setErrText(e.getMessage());
                ReplyExceptionMessage exceptionMessage = null;
                exceptionMessage = new ReplyExceptionMessage(
                        ListenerConstants.DcsMasterIpwhitelist_exn, 0, cc.getErrText());
                replyMessage = new ReplyMessage(exceptionMessage);
                return true;
            }
            return false;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Whitelist is not enabled......");
        }
        return false;
    }

    int buildConnectReply(ConnectionContext cc, SocketAddress clientSocketAddress ) {
        if (LOG.isInfoEnabled()) {
            LOG.info("BuildConnectReply entry. clientSocketAddress: <{}>", clientSocketAddress);
        }
        cc.setErrText(null);
        cc.setException_nr(0);
        int replyExceptionNr = 0;
        String errMsg = null;
        byte[] data = null;
        String server = "";
        HashMap<String,Object> attributes = null;
        String serverHostName = "";
        Long timestamp = 0L;
        Integer serverNodeId = 0;
        Integer serverProcessId = 0;
        String serverProcessName = "";
        String serverIpAddress = "";
        Integer serverPort = 0;
        String clusterName = "";
        int dialogueId = 0;
        String dataSource = "";
        byte[] userSid;
        int isoMapping;
        String enableSSL = configReader.getEnableSSL();
        boolean ignoreLogErr = false;

        try {
            configReader.getMapping().findProfile(cc);
            configReader.getRegisteredServers().getServers(cc);

            String hostConfSelectionMode = configReader.getConfHostSelectionMode();
            String hostProfSelectionMode = cc.getProfHostSelectionMode();
            String hostSelectionMode = Constants.PREFERRED;
            //TODO not sure why this logic here
            if (Constants.PREFERRED.equals(hostProfSelectionMode)) {
                hostSelectionMode = hostConfSelectionMode;
            } else {
                hostSelectionMode = hostProfSelectionMode;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("hostSelectionMode : <{}>", hostSelectionMode);
            }

            String nodeRegisteredPath = "";

            HashSet<String> allServers = cc.getAllServers();
            HashMap<String, HashMap<String,Object>> reusedUserServers = cc.getReusedUserServers();
            HashMap<String, HashMap<String,Object>> reusedOtherServers = cc.getReusedOtherServers();
            HashMap<String, HashMap<String,Object>> idleServers = cc.getIdleServers();
            HashMap<String, HashMap<String,Object>> allAvailableServers = cc.getAllAvailableServers();
            HashMap<String, HashMap<String,Object>> applicatedServers = cc.getApplicatedServers();

            // we support hot load dcs-site.xml, so do get each time.
            boolean userAffinity = configReader.getUserAffinity();

            if (!userAffinity){
                if(idleServers.size() > 0){
                    reusedUserServers.putAll(idleServers);
                }
                if(reusedOtherServers.size() > 0){
                    reusedUserServers.putAll(reusedOtherServers);
                }
                idleServers.clear();
                reusedOtherServers.clear();
            }
            if(LOG.isDebugEnabled()){
                LOG.debug("ConnectedReply userAffinity : <{}>", userAffinity );
                LOG.debug("ConnectedReply reusedSlaServers size : <{}>", reusedUserServers.size() );
                LOG.debug("ConnectedReply idleServers size : <{}>", idleServers.size() );
                LOG.debug("ConnectedReply reusedOtherServer size : <{}>", reusedOtherServers.size() );
                LOG.debug("ConnectedReply allAvailableServers size : <{}>", allAvailableServers.size() );
                LOG.debug("ConnectedReply applicatedServers size : <{}>", applicatedServers.size() );
             }

            while (true) {
                if (cc.isMultiTenancy()){
                    boolean reuse = reusedUserServers.size() > 0;
                    boolean available = allAvailableServers.size() > 0;
                    if(reuse){
                      server = getServer(reusedUserServers, cc);
                      if (!"".equals(server)) {
                          attributes = reusedUserServers.get(server);
                          reusedUserServers.remove(server);
                          if (LOG.isDebugEnabled()) {
                              LOG.debug("reusedSlaServers server: <{}>.", server);
                          }
                      } else {
                          if (LOG.isDebugEnabled()) {
                              LOG.debug("reusedSlaServers is empty use other servers.");
                          }
                      }
                    }

                    if (available && (!reuse || "".equals(server))) {
                      server = getServer(allAvailableServers,cc);
                      attributes = allAvailableServers.get(server);
                      allAvailableServers.remove(server);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("allAvailableServers server: <{}>.", server);
                        }
                    }

                    if (!available && (!reuse || "".equals(server))) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("reusedOtherServers is empty. No Available Servers.");
                        }
                    }
                    if (LOG.isInfoEnabled()) {
                        LOG.info("is Multi Tenancy, server: <{}>", server);
                    }
                } else {
                    if (cc.isSpecifiedServer()) {
                        String hostname = cc.getSpecifiedServer().split(":")[0];
                        if(!allServers.contains(hostname)) {
                            throw new IOException("Specified server : <"+ cc.getSpecifiedServer() +"> is not exists. Please verify.");
                        } else {
                            server = getSpecifiedServer(allAvailableServers, cc);
                            if(server == null) {
                                throw new IOException("Specified server : <"+ cc.getSpecifiedServer() +"> is not available. Please specify other server.");
                            } else {
                                attributes = allAvailableServers.remove(server);
                                reusedUserServers.remove(server);
                                idleServers.remove(server);
                                reusedOtherServers.remove(server);
                                applicatedServers.remove(server);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Specified server : <{}>.", server);
                                }
                            }
                        }
                    }else if (applicatedServers.size() > 0) {
                        server = getServer(applicatedServers.keySet());
                        attributes =  applicatedServers.remove(server);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("applicated server: <{}>.", server);
                        }
                    } else if(reusedUserServers.size() > 0){
                        server = getServer(reusedUserServers.keySet());
                        attributes = reusedUserServers.remove(server);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("reusedSlaServers server: <{}>.", server);
                        }
                    } else if(idleServers.size() > 0){
                        server = getServer(idleServers.keySet());
                        attributes = idleServers.remove(server);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("idleServers server: <{}>.", server);
                        }
                    } else if(reusedOtherServers.size() > 0){
                        server = getServer(reusedOtherServers.keySet());
                        attributes = reusedOtherServers.remove(server);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("reusedOtherServers server: <{}>.", server);
                        }
                    } else if(allAvailableServers.size() > 0 && hostSelectionMode.equals(Constants.PREFERRED)){
                        server = getServer(allAvailableServers.keySet());
                        attributes = allAvailableServers.remove(server);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("allAvailableServers server: <{}>.", server);
                        }
                    }
                    if (LOG.isInfoEnabled()) {
                        LOG.info("server: <{}>", server);
                    }
                }

                if (server == null || server.length() == 0) {
                    StringBuffer sb = new StringBuffer();
                    sb.append("ConnectedReply userAffinity : <").append(userAffinity).append(">. ");
                    if (userAffinity) {
                        sb.append("reusedSlaServers size : <").append(reusedUserServers.size()).append(">. ");
                    } else {
                        sb.append("idleServers size : <").append(idleServers.size()).append(">. ");
                        sb.append("reusedOtherServers size : <").append(reusedOtherServers.size()).append(">. ");
                    }
                    sb.append("allAvailableServers size : <").append(allAvailableServers.size()).append(">. ");
                    cc.setException_nr(ListenerConstants.DcsMasterNoSrvrHdl_exn);
                    throw new NoAvailServerException("No Available Servers. " + sb.toString());
                } else {
                    String znode = parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED + "/" + server;
                    zkc.sync(znode,null,null);
                    Stat stat = zkc.exists(znode, false);
                    if(stat != null) {
                        zkc.sync(znode,null,null);
                        data = zkc.getData(znode, null, stat);
                        String sdata = new String(data);
                    if(sdata.startsWith(Constants.AVAILABLE)){
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Exit while loop with zk server <{}>.", server);
                        }
                        break;
                    } else {
                        server = "";
                    }
                  }
                }
            }//while (true)
            serverHostName=(String)attributes.get(Constants.HOST_NAME);
            Integer serverInstance=(Integer)attributes.get(Constants.INSTANCE);

            timestamp=(Long)attributes.get(Constants.TIMESTAMP);
            serverNodeId=(Integer)attributes.get(Constants.NODE_ID);
            serverProcessId=(Integer)attributes.get(Constants.PROCESS_ID);
            serverProcessName=(String)attributes.get(Constants.PROCESS_NAME);
            serverIpAddress=(String)attributes.get(Constants.IP_ADDRESS);
            serverPort=(Integer)attributes.get(Constants.PORT);

            // do ip mapping
            cc.setIpMapping(configReader.getDefaultIpMapping());
            if (!"".equals(cc.getIpMapping())
                    && configReader.loadIpMappingConfig().containsKey(cc.getIpMapping())
                    && configReader.loadIpMappingConfig().get(cc.getIpMapping())
                    .containsKey(serverIpAddress)) {
                String tmpServerIpAddress = configReader.loadIpMappingConfig().get(cc.getIpMapping()).get(serverIpAddress);
                if (tmpServerIpAddress.length() > 0) {
                    serverIpAddress = tmpServerIpAddress;
                }
            }
            dialogueId = random.nextInt();
            dialogueId = (dialogueId < 0 )? -dialogueId : dialogueId;
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "<{}> : enableSSL <{}>, serverHostName <{}>, serverInstance <{}>, serverNodeId <{}>, serverProcessId <{}>, serverProcessName <{}>, serverIpAddress <{}>, serverPort <{}>, timestamp <{}>, dialogueId<{}>",
                        clientSocketAddress, enableSSL, serverHostName, serverInstance,
                        serverNodeId, serverProcessId, serverProcessName, serverIpAddress,
                        serverPort, timestamp, dialogueId);
            }

            String formatData = String.format("CONNECTING:%d:%d:%d:%d:%s:%s:%d:%s:%s:%s:%s:%s:%s:%d:%s:%s:%d:",
                    timestamp,              //1
                    dialogueId,             //2
                    serverNodeId,           //3
                    serverProcessId,        //4
                    serverProcessName,      //5
                    serverIpAddress,        //6
                    serverPort,             //7
                    cc.computerName,        //8
                    clientSocketAddress,    //9,10
                    cc.windowText,          //11
                    cc.getSla(),            //12
                    cc.getConnectProfile(), //13
                    cc.getDisconnectProfile(), //14
                    cc.getLastUpdate(),     //15
                    cc.user.getUserName(),       //16
                    cc.getTenantName(),     //17
                    cc.getNodeNum(),        //18
                    enableSSL //19
            );
            data = Bytes.toBytes(formatData);
            nodeRegisteredPath = parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED + "/" + server;
            if (LOG.isInfoEnabled()) {
                LOG.info("Node Registered Path : <{}>", nodeRegisteredPath);
            }
            Stat stat = null;
            try {
                stat = zkc.exists(nodeRegisteredPath, false);
            } catch (KeeperException | InterruptedException e) {
                errMsg = String.format("clientSocketAddress: %s, %s: %s",
                        clientSocketAddress,
                        e.getClass().getName(),
                        e.getMessage());
                replyExceptionNr = -1;
                ignoreLogErr = true;
                LOG.error(errMsg, e);
            }

            if (stat != null) {
                String oldData;
                try {

                    oldData = Bytes.toString(zkc.getData(nodeRegisteredPath, false, stat));

                    if (oldData.startsWith(Constants.AVAILABLE)) {
                        zkc.setData(nodeRegisteredPath, data, stat.getVersion());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Node registered path : <{}>, data : <{}>.",
                                nodeRegisteredPath, formatData);
                        }
                    } else {
                        ignoreLogErr = true;
                        if (allAvailableServers.size() > 0) {
                            errMsg = "Node registered path is not AVAILABLE, changed by another dcsmaster.";
                            replyExceptionNr = ListenerConstants.DcsMasterDistributionSevr_exn;
                        } else {
                            errMsg = "Node registered path is not AVAILABLE";
                            replyExceptionNr = -1;
                        }
                        cc.setException_nr(replyExceptionNr);
                    }
                }catch ( KeeperException | InterruptedException e) {
                    if (allAvailableServers.size() > 0) {
                        replyExceptionNr = ListenerConstants.DcsMasterNoSrvrHdl_exn;
                    }
                    if (e instanceof BadVersionException) {
                        if (LOG.isInfoEnabled()) {
                            LOG.info("[Tips]Znode version conflict, it will try again: {}",
                                e.getMessage());
                        }
                        if (allAvailableServers.size() > 0) {
                            replyExceptionNr = ListenerConstants.DcsMasterDistributionSevr_exn;
                            cc.setException_nr(replyExceptionNr);
                        }
                    } else {
                        errMsg = String.format("clientSocketAddress: %s, %s: %s",
                            clientSocketAddress,
                            e.getClass().getName(),
                            e.getMessage());
                        LOG.error(errMsg, e);
                    }
                    ignoreLogErr = true;
                }
            } else {
                LOG.warn("stat is null for Node registered path: <{}>", nodeRegisteredPath);
                replyExceptionNr = -1;
            }
        } catch (KeeperException | InterruptedException | IOException e) {
            errMsg = String.format("clientSocketAddress: %s, %s: %s",
                clientSocketAddress,
                e.getClass().getName(),
                e.getMessage());
            replyExceptionNr = -1;
            if (e instanceof NoAvailServerException) {
                ignoreLogErr = true;
                LOG.warn("[Tips]" + errMsg);
            } else {
                LOG.error(errMsg, e);
            }
        }
        if (replyExceptionNr != 0) {
            if (cc.getException_nr() == 0)
                cc.setException_nr(replyExceptionNr);
            cc.setErrText(errMsg);
            ReplyExceptionMessage exception = new ReplyExceptionMessage(cc.getException_nr(), 0, cc.getErrText());
            if(!ignoreLogErr) {
                LOG.error(
                    "Exception[{}]:{}, clientSocketAddress:{}, isAvailable:{}, isLimit:{}, isThroughput:{}, isSessionLimit:{}",
                    cc.getException_nr(), cc.getErrText(),
                    clientSocketAddress, cc.isAvailable(), cc.isLimit(), cc.isThroughput(),
                    cc.isSessionLimit());
            }
            replyMessage = new ReplyMessage(exception);
        } else {
            if (cc.datasource.length() == 0)
                dataSource = "TDM_Default_DataSource";
            else
                dataSource = cc.datasource;

            if(LOG.isDebugEnabled()){
                LOG.debug(clientSocketAddress + ": " + "userName: " + cc.user.getUserName());
                LOG.debug(clientSocketAddress + ": " + "password: XXXXXX");
                LOG.debug(clientSocketAddress + ": " + "client: " + cc.client);
                LOG.debug(clientSocketAddress + ": " + "location: " + cc.location);
                LOG.debug(clientSocketAddress + ": " + "windowText: " + cc.windowText);
                LOG.debug(clientSocketAddress + ": " + "dataSource: " + dataSource);
                LOG.debug(clientSocketAddress + ": " + "client computer name:ipaddress:port " + cc.computerName+ ":" + clientSocketAddress);
                LOG.debug(clientSocketAddress + ": " + "sla :" + cc.getSla());
                LOG.debug(clientSocketAddress + ": " + "connectProfile :" + cc.getConnectProfile());
                LOG.debug(clientSocketAddress + ": " + "disconnectProfile :" + cc.getDisconnectProfile());
                LOG.debug(clientSocketAddress + ": " + "profile Last Update :" + cc.getLastUpdate());
            }
            userSid = cc.user.getUserName().getBytes(StandardCharsets.UTF_8);
            isoMapping = 0;
            clusterName = serverHostName + "-" + cc.client;
            if (LOG.isInfoEnabled()) {
                LOG.info("<{}>: clusterName <{}>.", clientSocketAddress, clusterName);
            }
            replyMessage = new ReplyMessage(dialogueId, dataSource, userSid, versions,
                    isoMapping, serverHostName, serverNodeId, serverProcessId, serverProcessName,
                    serverIpAddress, serverPort, timestamp, clusterName, enableSSL);
        }
        return replyExceptionNr;
    }

    public ReplyMessage getReplyMessage() {
        return replyMessage;
    }

    private <K, V> String getServer(Map<K, V> servers, ConnectionContext cc) {
        TenantNodes tenantNodes = cc.getTenantNodes();
        if (tenantNodes == null) {
            tenantNodes = allNodes;
        }
        if (LOG.isDebugEnabled()) {
            NodeUnitCounters existingConnections = cc.getExistingConnections();
            StringBuffer sb = new StringBuffer();
            sb.append("nodeIds : ");
            for (int i = 0; i < existingConnections.length(); i++) {
                sb.append(existingConnections.id(i)).append(" ");
            }
            sb.append("existingConnections : ");
            for (int i = 0; i < existingConnections.length(); i++) {
                sb.append(existingConnections.getDenseCounter(i)).append(" ");
            }
            sb.append(", limits : ");
            for (int i = 0; i < existingConnections.length(); i++) {
                sb.append(existingConnections.getDenseLimit(i)).append(" ");
            }
            if (cc.getTenantNodes() == null)
                sb.append(", useSystemTenant : true");
            sb.append(", tenant : ").append(tenantNodes.toSerializedForm());
            sb.append(", tenantSize : ").append(tenantNodes.getSize());
            LOG.debug(sb.toString());
        }
        int nodeNum = tenantNodes.placeConnection(cc.getExistingConnections(), true);
        if (LOG.isDebugEnabled()) {
            LOG.debug("nodeNum <{}>.", nodeNum);
        }
        if (nodeNum == -1) {
            return "";
        }
        List<String> clusterNodeList = cc.getClusterNodeList();
        if (LOG.isDebugEnabled()) {
            LOG.debug("clusterNodeList <{}>.", clusterNodeList);
        }
        String server = clusterNodeList.get(nodeNum);
        boolean found = false;
        for (Entry<K, V> entry : servers.entrySet()) {
            String serverName = entry.getKey().toString();
            if (serverName.startsWith(server)) {
                server = serverName;
                found = true;
                break;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("server <{}>.", server);
        }
        if (found) {
            cc.setNodeNum(nodeNum + "");
            return server;
        } else {
            return "";
        }
    }
    private Random r = new Random();

    private String getServer(Set<String> mxos) {
        if (mxos.size() == 0) {
            return "";
        }
        if (mxos.size() < 2) {
            return new ArrayList<String>(mxos).get(Math.abs(r.nextInt(mxos.size())));
        }
        Map<String, List<String>> mxoMap = new HashMap<>();
        String key;
        for (String mxo : mxos) {
            key = mxo.substring(0, mxo.lastIndexOf(":"));
            if (mxoMap.containsKey(key)) {
                mxoMap.get(key).add(mxo);
            } else {
                List<String> list = new ArrayList<>();
                list.add(mxo);
                mxoMap.put(key, list);
            }
        }
        List<String> maxMxo = null;
        int max = 0, size;
        for (String mxo : mxoMap.keySet()) {
            size = mxoMap.get(mxo).size();
            if (size > max) {
                max = size;
                maxMxo = mxoMap.get(mxo);
            }
        }
        return maxMxo.get(Math.abs(r.nextInt(maxMxo.size())));
    }

    // the given specified server format is "serverName:mxosrvrPort"
    private String getSpecifiedServer(Map<String, HashMap<String, Object>> map,
            ConnectionContext cc) {
        String[] specifiedServer = cc.getSpecifiedServer().split(":");

        String hostName = specifiedServer[0];
        int port = Integer.parseInt(specifiedServer[1]);
        for (String server : map.keySet()) {
            String[] pair = server.toString().split(":", 2);

            if (hostName.equals(pair[0])) {
                Map<String, Object> attributes = map.get(server);
                if ((int) attributes.get(Constants.PORT) == port) {
                    return server;
                }
            }
        }
        return null;
    }

    static <K, V> Entry<K, V> randEntry(Iterator<Entry<K, V>> it, int count) {
        int index = (int) (Math.random() * count);

        while (index > 0 && it.hasNext()) {
            it.next();
            index--;
        }

        return it.next();
    }

    static <K, V> Entry<K, V> randEntry(Set<Entry<K, V>> entries) {
        return randEntry(entries.iterator(), entries.size());
    }

    static <K, V> Entry<K, V> randEntry(Map<K, V> map) {
        return randEntry(map.entrySet());
    }

    static <K, V> K randEntryKey(Map<K, V> map) {
        return randEntry(map).getKey();
    }

    static <K, V> V randEntryValue(Map<K, V> map) {
        return randEntry(map).getValue();
    }

}
