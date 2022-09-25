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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.ServerManager;
import org.trafodion.dcs.master.message.ConnectMessage;
import org.trafodion.dcs.master.message.UserDesc;
import org.trafodion.dcs.master.message.Version;
import com.esgyn.common.CGroupHelper;
import com.esgyn.common.NodeUnitCounters;
import com.esgyn.common.TenantNodes;

public class ConnectionContext {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionContext.class);

    String datasource = "";
    String catalog = "";
    String schema = "";
    String location = "";
    String userRole = "";
    String connectOptions = "";

    short accessMode;
    short autoCommit;
    int queryTimeoutSec;
    int idleTimeoutSec;
    int loginTimeoutSec;
    short txnIsolationLevel;
    short rowSetSize;

    int diagnosticFlag;
    int processId;

    String computerName = "";
    String windowText = "";

    Version[] clientVersions = null;
    UserDesc user = null;

    int ctxACP;
    int ctxDataLang;
    int ctxErrorLang;
    short ctxCtrlInferNXHAR;

    short cpuToUse;
    short cpuToUseEnd;

    int srvrType;
    short retryCount;
    int optionFlags1;
    int optionFlags2;
    String vproc;
    String client;
    String ccExtention;
    private final HashSet<String> allServers = new HashSet<String>();
    private final HashMap<String, HashMap<String,Object>> allAvailableServers = new HashMap<String, HashMap<String,Object>>();
    private final HashMap<String, HashMap<String,Object>> idleServers = new HashMap<String, HashMap<String,Object>>();
    private final HashMap<String, HashMap<String,Object>> reusedUserServers = new HashMap<String, HashMap<String,Object>>();
    private final HashMap<String, HashMap<String,Object>> reusedOtherServers = new HashMap<String, HashMap<String,Object>>();
    private final HashMap<String, HashMap<String,Object>> applicatedServers = new HashMap<String, HashMap<String,Object>>();
	
    HashMap<String, String> attributes;
    String sla;
    String priority;
    int limit;
    int sessionLimit;
    int throughput;
    int curLimit;
    int curSessionLimit;
    int curThroughput;
/*
*/
    String connectProfile;
    String disconnectProfile;
    long lastUpdate;
    Set<String> hostList = new HashSet<String>();
    String hostSelectionMode = Constants.PREFERRED;

    String tenantName;
    TenantNodes tenantNodes = null;
    int nodeNum = 0;
    int tenantSessionLimit;
    int curTenantSessionLimit;

    NodeUnitCounters existingConnections = null;
    boolean multiTenancy = false;

    int exception_nr = 0;
    String errText = null;

    private String specifiedServer;
    private String ipMapping;
/*
*/

  ConnectionContext(ConnectMessage connectMessage){
      datasource = connectMessage.getDatasource();
      catalog = connectMessage.getCatalog();
      schema = connectMessage.getSchema();
      location = connectMessage.getLocation();
      userRole = connectMessage.getUserRole();

      accessMode = connectMessage.getAccessMode();
      autoCommit = connectMessage.getAutoCommit();
      queryTimeoutSec = connectMessage.getQueryTimeoutSec();
      idleTimeoutSec = connectMessage.getIdleTimeoutSec();
      loginTimeoutSec = connectMessage.getLoginTimeoutSec();
      txnIsolationLevel = connectMessage.getTxnIsolationLevel();
      rowSetSize = connectMessage.getRowSetSize();

      diagnosticFlag = connectMessage.getDiagnosticFlag();
      processId = connectMessage.getProcessId();

      computerName = connectMessage.getComputerName();
      windowText = connectMessage.getWindowText();

      ctxACP = connectMessage.getCtxACP();
      ctxDataLang = connectMessage.getCtxDataLang();
      ctxErrorLang = connectMessage.getCtxErrorLang();
      ctxCtrlInferNXHAR = connectMessage.getCtxCtrlInferNXHAR();

      cpuToUse = connectMessage.getCpuToUse();
      cpuToUseEnd = connectMessage.getCpuToUseEnd();
      connectOptions = connectMessage.getConnectOptions();

      clientVersions = connectMessage.getClientVersions();

      user = connectMessage.getUser();
      srvrType = connectMessage.getSrvrType();
      retryCount = connectMessage.getRetryCount();
      optionFlags1 = connectMessage.getOptionFlags1();
      optionFlags2 = connectMessage.getOptionFlags2();
      vproc = connectMessage.getVproc();
      client = connectMessage.getClient();
      ccExtention = connectMessage.getCcExtention();
      attributes = connectMessage.getAttributes();

      setSpecifiedServer(attributes.get(Constants.SPECIFIED_SERVER));
  }

  /*
  * value =  AVAILABLE:      0
  *          timestamp       1
  *          dialofueId      2       empty place
  *          nodeId          3
  *          processId       4
  *          processName     5
  *          ipAddress       6
  *          port            7
  *          ===================================
  *          computerName    8
  *          clientSocket    9
  *          clientPort      10
  *          windowText      11
  *          ====================================
  *          sla             12
  *          profile         13
  *          disProfile      14
  *          profileTimestamp 15
  *          userName        16
  *          tenantName      17
  *          affinityNum     18
  *          
  *          
  */
    public void setAllServers (HashMap<String, Integer> totalServers) {
        allServers.clear();
        Set<String> servers = totalServers.keySet();
        for (String server : servers) {
            allServers.add(server);
        }
    }

    public  void setAvailableServers(HashMap<String, String> availableServers){
        allAvailableServers.clear();
        reusedUserServers.clear();
        reusedOtherServers.clear();
        idleServers.clear();
        applicatedServers.clear();

        Set<String> servers = availableServers.keySet();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Available Servers : {}.", servers);
        }

        for( String server : servers){
            HashMap<String,Object> attr = buildAttributeHashMap(server, availableServers.get(server));
            allAvailableServers.put(server, attr);

            //add situation multi tenancy is enable
            if (multiTenancy || (hostList.isEmpty() || hostList.contains(attr.get(Constants.HOST_NAME)))){
                if(attr.size() == 8)
                  idleServers.put(server, attr);
                else if(attr.size() >= 17) {
                  if(attr.get(Constants.MAPPED_SLA).equals(sla) && attr.get(Constants.USER_NAME).equals(user.getUserName()))
                      if (multiTenancy && tenantName.equals(attr.get(Constants.TENANT_NAME))) {
                          reusedUserServers.put(server, attr);
                      }
                      if (!multiTenancy) {
                          reusedUserServers.put(server, attr);
                          if (attr.get(Constants.WINDOW_TEXT).equals(windowText)
                              && attr.get(Constants.USER_NAME).equals(user.getUserName())) {
                              applicatedServers.put(server, attr);
                          }
                      }
                  else
                      reusedOtherServers.put(server, attr);
                }
            }
        }
    }

    public void setUnAvailableServers(HashMap<String, String> unAvailableServers) throws IOException {
        Set<String> servers = unAvailableServers.keySet();
        if (LOG.isDebugEnabled()) {
            LOG.debug("unAvailable Servers : {}.", servers);
        }

        curSessionLimit = 0;
        curTenantSessionLimit = 0;
        for (String server : servers) {
            HashMap<String, Object> attr = buildAttributeHashMap(server, unAvailableServers.get(server));

            if (sla.equals(attr.get(Constants.SLA))) {
                curSessionLimit++;
            }

            if (isMultiTenancy() && tenantName.equals(attr.get(Constants.TENANT_NAME))) {
                curTenantSessionLimit++;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Current session limit after set unavailable servers <{}>. Current tenant session limit after set unavailable servers <{}>.",
                    curSessionLimit, curTenantSessionLimit);
        }

        if (isMultiTenancy() && isTenantSessionLimit()) {
            LOG.error("Session limit for TENANT <{}>, max session is <{}>, current is <{}>.",
                    getTenantName(), getTenantSessionLimit(), getCurTenantSessionLimit());
            throwException(ListenerConstants.DcsMasterTenantSessionLimit_exn, "Session limit for tenant : " + this.getTenantName());
        }

        if (isSessionLimit()) {
            LOG.error("Session limit for SLA <{}>, max session is <{}>, current is <{}>.", 
                    getSla(), getSessionLimit(), getCurSessionLimit());
            throwException(ListenerConstants.DcsMasterSessionLimit_exn, "Session limit for SLA : " + this.getSla());
        }
    }

    private static void sortByTimestamp(Map<String, LinkedHashMap<String,Object>> map) { 
        List<Set<Map.Entry<String,Object>>> list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator<Object>() {
            public int compare(Object o1, Object o2) {
                 Map<String,Object> m1 = ((LinkedHashMap<String,Object>)((Map.Entry)(o1)).getValue());
                 long orderNumber1 = (long)m1.get(Constants.TIMESTAMP);
                 Map<String,Object> m2 = ((LinkedHashMap<String,Object>)((Map.Entry)(o2)).getValue());
                 long orderNumber2 = (long)m2.get(Constants.TIMESTAMP);
                 return orderNumber1 > orderNumber2 ? 1 : (orderNumber1 < orderNumber2 ? -1 : 0);
             }
        });
        map.clear();
        for (Iterator<?> it = list.iterator(); it.hasNext();) {
               Map.Entry<String, LinkedHashMap<String,Object>> entry = (Map.Entry)it.next();
               map.put(entry.getKey(), entry.getValue());
        } 
    }
    public void setConnectedServers(HashMap<String, String> connectedServers){
        curLimit = connectedServers.size();
        curThroughput = 0;
        Set<String> keys = connectedServers.keySet();
        for (String key : keys) {
            String value = connectedServers.get(key);
            if (value.contains(":" + sla + ":")) {
                curThroughput++;
            }
        }
    }
    public void setHostList(String s){
        hostList.clear();
        String delims = "[,]";
        String[] tokens = s.split(delims);
        String tkn = "";
        for (int i = 0; i < tokens.length; i++){
            tkn = tokens[i].trim();
            if (LOG.isDebugEnabled()) {
                LOG.debug("setHostList : <{}>", tkn);
            }
            if(tkn.length() > 0)
                hostList.add(tkn);
        }
    }
    public void setProfHostSelectionMode(String hostSelectionMode){
        this.hostSelectionMode = hostSelectionMode;
    }
    public void setLastUpdate(String s){
        if (s == null || s.length()== 0) s = "0";
        lastUpdate = new Long(s);
    }
    public void setSla(String sla){
        this.sla = sla;
    }
    public void setPriority(String s){
        if (s == null || s.length()== 0) s = Constants.PRTY_LOW;
        priority = s;
    }
    public void setLimit(String s) {
        if (s == null || s.length() == 0)
            s = "0";
        limit = Integer.parseInt(s);
    }
    public void setSessionLimit(String s) {
        //if get blank, trans to -1, means no limit
        //if get 0 , means no one can connect
        if (s == null || s.length() == 0)
            s = "-1";
        sessionLimit = Integer.parseInt(s);
    }
    public void setThroughput(String s){
        if (s == null || s.length()== 0) s = "0";
        throughput = new Integer(s);;
    }
    public String getConnectProfile(){
        return connectProfile;
    }
    public String getDisconnectProfile(){
        return disconnectProfile;
    }
    public long getLastUpdate(){
       return lastUpdate;
    }
    public Set<String> getHostList(){
       return hostList;
    }
    public String getProfHostSelectionMode(){
        return hostSelectionMode;
    }
    public String getSla(){
        return sla;
    }
    public String getPriority(){
        return priority;
    }
    public int getLimit(){
        return limit;
    }
    public int getThroughput(){
        return throughput;
    }
    public int getCurrentLimit(){
        return curLimit;
    }
    public int getCurrentThroughput(){
        return curThroughput;
    }
    public int getSessionLimit() {
        return sessionLimit;
    }
    public int getCurSessionLimit() {
        return curSessionLimit;
    }
    public boolean isLimit(){
        if(limit == 0)return false;
        return curLimit > limit;
    }
    public boolean isSessionLimit(){
        if(sessionLimit == -1)return false;
        return curSessionLimit >= sessionLimit;
    }
    public HashMap<String, HashMap<String,Object>> getReusedUserServers(){
        return reusedUserServers;
    }
    public HashMap<String, HashMap<String, Object>> getApplicatedServers(){
        return applicatedServers;
    }
    public HashMap<String, HashMap<String,Object>> getReusedOtherServers(){
        return reusedOtherServers;
    }
    public HashMap<String, HashMap<String,Object>> getIdleServers(){
        return idleServers;
    }
    public HashMap<String, HashMap<String,Object>> getAllAvailableServers(){
        return allAvailableServers;
    }
    public HashSet<String> getAllServers() {
        return allServers;
    }
    public HashMap<String, String> getAttributes(){
        return attributes;
    }
    public void setConnectProfile(String v){
        connectProfile = v;
    }
    public void setDisconnectProfile(String v){
        disconnectProfile = v;
    }
    public boolean isThroughput(){
        if(throughput == 0)return false; 
        return curThroughput > throughput;
    }
    public boolean isAvailable(){
        if(idleServers.size() == 0 && reusedUserServers.size() == 0 && reusedOtherServers.size() == 0)return false; 
        return true;
    }

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public TenantNodes getTenantNodes() {
        return tenantNodes;
    }

    public void setTenantNodes(TenantNodes tenantNodes) {
        this.tenantNodes = tenantNodes;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    public void setNodeNum(String s) {
        if (s == null || s.length() == 0)
            s = "0";
        this.nodeNum = Integer.parseInt(s);
    }

    public int getCurTenantSessionLimit() {
        return curTenantSessionLimit;
    }

    public int getTenantSessionLimit() {
        return tenantSessionLimit;
    }
    public void setTenantSessionLimit(String s) {
        //if get blank, trans to -1, means no limit
        //if get 0 , means no one can connect
        if (s == null || s.length() == 0)
            s = "-1";
        this.tenantSessionLimit = Integer.parseInt(s);
    }

    public boolean isTenantSessionLimit() {
        if (tenantSessionLimit == -1)
            return false;
        return curTenantSessionLimit >= tenantSessionLimit;
    }

    public NodeUnitCounters getExistingConnections() {
        return existingConnections;
    }

    public List<String> getClusterNodeList() {
        return ServerManager.clusterServers;
    }

    public boolean isMultiTenancy() {
        return multiTenancy;
    }

    public void setMultiTenancy(boolean multiTenancy) {
        this.multiTenancy = multiTenancy;
    }

    public int getException_nr() {
        return exception_nr;
    }

    public void setException_nr(int exception_nr) {
        this.exception_nr = exception_nr;
    }

    public String getErrText() {
        return errText;
    }

    public void setErrText(String errText) {
        this.errText = errText;
    }

    public void setIpMapping(String defaultIpMapping) {
        if (attributes.get(Constants.IP_MAPPING) == null || attributes.get(Constants.IP_MAPPING).length() == 0) {
            ipMapping = defaultIpMapping;
            if (ipMapping == null || ipMapping.length() == 0) {
                ipMapping = "";
            }
        } else {
            ipMapping = attributes.get(Constants.IP_MAPPING);
        }
    }

    public String getIpMapping() {
        return ipMapping;
    }

    public String getSpecifiedServer() {
        return specifiedServer;
    }

    public void setSpecifiedServer(String specifiedServer) {
        this.specifiedServer = specifiedServer;
    }

    public boolean isSpecifiedServer() {
        if (specifiedServer != null && specifiedServer.length() > 0 && specifiedServer.contains(":")) {
            return true;
        }
        return false;
    }

    public HashMap<String,Object> buildAttributeHashMap(String server, String value){
        int firstColon = server.indexOf(":");
        int secColon = server.indexOf(":", firstColon + 1);

        String hostName = server.substring(0, firstColon);
        int instance = Integer.parseInt(server.substring(firstColon + 1, secColon));

        String[] sValue = value.split(":");
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Build attribute hashMap for server : <{}>, value : <{}>, value length : <{}>.",
                    server, value, sValue.length);
        }
        HashMap<String,Object> attr = new HashMap<String,Object>();
        attr.put(Constants.HOST_NAME, hostName);
        attr.put(Constants.INSTANCE, instance);
        attr.put(Constants.TIMESTAMP, Long.parseLong(sValue[1]));
        attr.put(Constants.NODE_ID, Integer.parseInt(sValue[3]));
        attr.put(Constants.PROCESS_ID, Integer.parseInt(sValue[4]));
        attr.put(Constants.PROCESS_NAME, sValue[5]);
        attr.put(Constants.IP_ADDRESS, sValue[6]);
        attr.put(Constants.PORT, (sValue[7].equals("")) ? Integer.parseInt("0") : Integer.parseInt(sValue[7]));
        if(sValue.length >= 17) {
            attr.put(Constants.COMPUTER_NAME, sValue[8]);
            attr.put(Constants.CLIENT_SOCKET, sValue[9]);
            attr.put(Constants.CLIENT_PORT, sValue[10]);
            attr.put(Constants.WINDOW_TEXT, sValue[11]);
            attr.put(Constants.MAPPED_SLA, sValue[12]);
            attr.put(Constants.MAPPED_CONNECT_PROFILE, sValue[13]);
            attr.put(Constants.MAPPED_DISCONNECT_PROFILE, sValue[14]);
            attr.put(Constants.MAPPED_PROFILE_TIMESTAMP, (sValue[15].equals(""))? Long.parseLong("0") : Long.parseLong(sValue[15]));
            attr.put (Constants.USER_NAME, sValue[16]);
            attr.put(Constants.TENANT_NAME, sValue[17]);
            attr.put(Constants.AFFINITY, sValue[18]);
        }
        return attr;
    }

    // input servers have struct of <instance, hostname>
    // sort the hosts by instance
    private List<String> sortServer(HashMap<Integer, String> servers) {
        List<Integer> instanceLists = new ArrayList<Integer>(servers.keySet());
        Collections.sort(instanceLists);

        List<String> serverLists = new ArrayList<String>(instanceLists.size());
        for (int i = 0; i < instanceLists.size(); i++) {
            serverLists.add(servers.get(instanceLists.get(i)));
        }
        return serverLists;
    }

    public void setExistingConnectionsAndLimits(HashMap<String, Integer> serverUsage,
                                                HashMap<String, Integer> allServers) throws IOException {
        List<String> serverLists = getClusterNodeList();
        int clusterSize = CGroupHelper.getClusterSize();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cluster node list {}.", serverLists);
        }
        int[] ids = new int[clusterSize];
        int[] counters = new int[clusterSize];
        int[] limits = new int[clusterSize];
        boolean hasUsage = false;

        for (int i = 0; i < clusterSize; i++) {
            Integer cnt = 0;
            Integer lim = 0;

            // TODO: instead of just using the index in
            //       serverLists, get the actual node number
            //       for this server
            if (serverLists.size() > i) {
                String nodeName = serverLists.get(i);

                cnt = serverUsage.get(nodeName);
                lim = allServers.get(nodeName);
              if (!allServers.containsKey(nodeName)) {
                LOG.error(String
                    .format("NodeName(%s) is not in cache %s. NodeName in servers file: %s",
                        nodeName, allServers, serverLists));
              }
            }
            ids[i] = CGroupHelper.getNodeId(i);
            counters[i] = (cnt == null ? 0 : cnt);
            limits[i] = (lim == null ? 0 : lim);
            if(limits[i] > 0) {
                hasUsage = true;
            }
        }
        existingConnections = new NodeUnitCounters(ids, counters, limits);
        if (!hasUsage && allServers.size() > 0) {
            LOG.error("DcsMaster unable to assign server process to service the client request, check configure file 'servers'.");
            throwException(ListenerConstants.DcsMasterNoSrvrHdl_exn, "DcsMaster unable to assign server process to service the client request");
        }
    }

    private void throwException(int expNum, String expMsg) throws IOException {
        this.setException_nr(expNum);
        this.setErrText(expMsg);
        throw new IOException(expMsg);
    }

}
