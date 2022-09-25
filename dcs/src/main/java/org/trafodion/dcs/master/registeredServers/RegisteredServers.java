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
package org.trafodion.dcs.master.registeredServers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.ConfigReader;
import org.trafodion.dcs.master.listener.ConnectionContext;
import org.trafodion.dcs.master.ServerManager;
import org.trafodion.dcs.util.LicenseHolder;
import org.trafodion.dcs.zookeeper.ZkClient;
import com.esgyn.common.LicenseHelper;

public class RegisteredServers  {
    private static  final Logger LOG = LoggerFactory.getLogger(RegisteredServers.class);
    private final Map<String, String> servers = Collections.synchronizedMap(new LinkedHashMap<String, String>());
    private final Map<String, String> removedServers = Collections.synchronizedMap(new LinkedHashMap<String, String>());
    private final Object lock = new Object();

    private ZkClient zkc = null;
    private String parentDcsZnode = "";
    private ConfigReader configReader = null;

    public RegisteredServers(ConfigReader configReader) throws Exception{
        this.configReader = configReader;
        zkc = configReader.getZkc();
        parentDcsZnode = configReader.getParentDcsZnode();
        initZkServers();
    }

    class ChildrenWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();

                    List<String> children = zkc.getChildren(znode,new ChildrenWatcher());
                    if( ! children.isEmpty()){
                       synchronized (lock){
                          Set<String> keyset = new HashSet<>(servers.keySet());
                          for(String child : children) {
                             stat = zkc.exists(znode + "/" + child,false);
                             if(stat != null) {
                                if(keyset.contains(child)){
                                   keyset.remove(child);
                                   continue;
                                }
                                //add new record
                                data = zkc.getData(znode + "/" + child, new DataWatcher(), stat);
                                 if (LOG.isDebugEnabled()) {
                                     LOG.debug(
                                             "ChildrenWatcher NodeChildrenChanged Add child <{}> data <{}>.",
                                             child, new String(data));
                                 }
                                updateWhenSrvrUp(child, new String(data));
                             }//end if znode exists
                          }//end for all children

                          // Keyset now contains deleted znodes - remove records
                           if (LOG.isDebugEnabled()) {
                               LOG.debug("ChildrenWatcher NodeChildrenChanged Remove children {}",
                                       keyset);
                           }
                          for(String child : keyset){
                             updateWhenSrvrDown(child);
                          }//end for keyset
                       }//end synchronized servers
                    }// if children
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }//end  EventType NodeChildrenChanged
            else {
                LOG.warn("ChildrenWatcher unknown type <{}> path <{}>.", event.getType(), event.getPath());
            }
        }//end process
    }//end ChildrenWatcher

    class DataWatcher implements Watcher {

        public void process(WatchedEvent event) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("Data Watcher <{}>.", event.getPath());
            }

            if(event.getType() == Event.EventType.NodeDataChanged){

                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    data = zkc.getData(znode, new DataWatcher(), stat);
                    synchronized(lock){
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Data Watcher NodeDataChanged child <{}> data <{}>.", child, new String(data));
                        }
                        updateWhenSrvrUp(child, new String(data));
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
           }
           else if(event.getType() == Event.EventType.NodeDeleted){
                try {
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    synchronized(lock){
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Data Watcher NodeDeleted <{}>.", child);
                        }
                        updateWhenSrvrDown(child);
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
           }
           else {
                LOG.warn("Data Watcher unknowntype <{}> path <{}>.", event.getType(), event.getPath());
            }
        }//end process
    }//end DataWatcher

    private void initZkServers() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("initZkServers <{}>.",
                    parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED);
        }
        Stat stat = null;
        byte[] data = null;

        String znode = parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED;
        List<String> children = null;
        synchronized (lock) {
            servers.clear();
            removedServers.clear();
            children = zkc.getChildren(znode, new ChildrenWatcher());
            if (!children.isEmpty()) {
                for (String child : children) {
                    stat = zkc.exists(znode + "/" + child, false);
                    if (stat != null) {
                        data = zkc.getData(znode + "/" + child, new DataWatcher(), stat);
                        servers.put(child, new String(data));
                    }
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Init servers {}.", servers);
        }
    }// end initZkServers

    public int getServerSize() {
        Set<String> serverSet = new HashSet<String>();
        for (String key : servers.keySet()) {
            Scanner scn = new Scanner(key);
            scn.useDelimiter(":");
            String hostName = scn.next();
            scn.close();
            serverSet.add(hostName);
        }
        return serverSet.size();
    }

    public void getServers(ConnectionContext cc) throws IOException{
        if (LOG.isDebugEnabled()) {
            LOG.debug("getServers entry.");
        }
        HashMap<String, String> availableServers = new HashMap<String, String>();
        HashMap<String, String> unAvailableServers = new HashMap<String, String>();
        HashMap<String, String> connectedServers = new HashMap<String, String>();
        HashMap<String, Integer> serverUsage = new HashMap<String, Integer>();
        HashMap<String, Integer> allServers = new HashMap<String, Integer>();

        synchronized(lock){
            Set<String> keys = new HashSet<>(servers.keySet());
            for(String key : keys) {
                String value = servers.get(key);

                Scanner scn = new Scanner(key);
                scn.useDelimiter(":");
                String hostName = scn.next();
                String hostId = scn.next();
                scn.close();

                if (!ServerManager.nodeEnabled(hostName + ":" + hostId))
                {
                    unAvailableServers.put(key, value);
                    continue;
                }

                if (allServers.containsKey(hostName)){
                    allServers.put(hostName, allServers.get(hostName)+1);
                }else{
                    allServers.put(hostName, 1);
                }

                if(value.startsWith(Constants.AVAILABLE)){
                    availableServers.put(key, value);
                }
                if (value.startsWith(Constants.CONNECTED) || value.startsWith(Constants.REBALANCE) || value.startsWith(Constants.RESTART)) {
                    connectedServers.put(key, value);
                    unAvailableServers.put(key, value);
                    if (serverUsage.containsKey(hostName)){
                        serverUsage.put(hostName, serverUsage.get(hostName)+1);
                    }else{
                        serverUsage.put(hostName, 1);
                    }
                }
                if(value.startsWith(Constants.CONNECTING)){
                    unAvailableServers.put(key, value);
                    if (serverUsage.containsKey(hostName)){
                        serverUsage.put(hostName, serverUsage.get(hostName)+1);
                    }else{
                        serverUsage.put(hostName, 1);
                    }
                }
            }
            if(LOG.isDebugEnabled()) {
                LOG.debug("allServers : {}.", allServers);
                LOG.debug("unAvailableServers : {}.", unAvailableServers);
                LOG.debug("availableServers : {}.", availableServers);
                LOG.debug("connectedServers : {}.", connectedServers);
            }
        }
        cc.setAllServers(allServers);
        cc.setUnAvailableServers(unAvailableServers);
        cc.setAvailableServers(availableServers);
        cc.setConnectedServers(connectedServers);
        if (LicenseHolder.isMultiTenancyEnabled()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("serverUsage : {}.", serverUsage);
            }
            cc.setExistingConnectionsAndLimits(serverUsage, allServers);
        }
    }//end getServers

    /**
     * zk watcher may lose when there are large num of node change , and zk is eventual consistency.
     * when there happen huge concurrence of client conns and queries,
     * the cache "servers" in this class may not be notify, means watcher not trigger.
     * Then will may happen conn lose(the size of "servers" less than configured size of mxosrvr).
     * So there is necessary to sync the content of "servers" the struct of removedServers is :
     * key: nodeName:nodeNum:mxoNum
     *      eg: esgvm.novalocal:1:1
     * value: nodeDownTime:checkedTimes
     *        nodeDownTime: recode node down time, if nodeDownTime up to certain time, do sync
     *        checkedTimes: number of times which triggers syncServersCache.
     *        eg: 1523525330058:0
     */
    public void syncServersCache() {
        if (removedServers.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No removed server and no need to do sync to zk.");
            }
            return;
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("There has removed server, do sync to zk.");
        }
        String znode = parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED;

        try {
            synchronized (lock) {
                Set<String> rmServers = new HashSet<String>(removedServers.keySet());
                for (String server : rmServers) {
                    if (!shouldSync(server)) {
                        continue;
                    }

                    String mxoPath = znode + "/" + server;
                    Stat s = zkc.exists(mxoPath, false);
                    if (s == null) {
                        resetRemovedServers(server);
                        continue;
                    } else {
                        byte[] data = zkc.getData(mxoPath, new DataWatcher(), s);
                        if (LOG.isInfoEnabled()) {
                            LOG.info("update for srvr <{}>, value is <{}>.", server,
                                    new String(data));
                        }
                        updateWhenSrvrUp(server, new String(data));
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Finish sync to zk.");
        }
    }

    /**
     * reset the value of certain server to init state for removedServers
     * @param server
     */
    private void resetRemovedServers(String server) {
        removedServers.put(server, System.currentTimeMillis() + ":" + 0);
    }

    /**
     * when mxosrvr start, the servers cache should add the znode&nodeVal, the removedServers cache should remove the znode
     * @param server
     *   the znode name
     * @param data
     *   the znode value
     */
    private void updateWhenSrvrUp(String server, String data) {
        servers.put(server, data);
        removedServers.remove(server);
    }

    /**
     * when mxosrvr down, the removedServers cache should add the znode&downTime&checkTimes, the servers cache should remove the znode
     * @param server
     *   the znode name
     */
    private void updateWhenSrvrDown(String server) {
        servers.remove(server);
        removedServers.put(server, System.currentTimeMillis() + ":0");
    }

    /**
     * update removedServer value by +1 from check times when there is no
     * @param server
     *   the znode name
     * @throws InterruptedException
     * @throws KeeperException
     */
    private boolean shouldSync(String server) throws KeeperException, InterruptedException {
        if (LOG.isInfoEnabled()) {
            LOG.info("ENTRY.");
        }
        String[] serverValue = removedServers.get(server).split(":");

        long current = System.currentTimeMillis();
        long nodeDownTime = 0;
        try {
            nodeDownTime = Long.parseLong(serverValue[0]);
        } catch (NumberFormatException e) {
            LOG.warn(e.getMessage(), e);
        }
        int checkedTimes = 0;
        try {
            checkedTimes = Integer.parseInt(serverValue[1]);
        } catch (NumberFormatException e) {
            LOG.warn(e.getMessage(), e);
        }

        int confCheckTimes = this.configReader.getMasterSyncCacheCheckTimes();
        if ((current - nodeDownTime) / 1000 < (Math.pow(2, checkedTimes) + 5) * 60 && checkedTimes <= confCheckTimes
                && (current - nodeDownTime) / 1000 < 12 * 60 * 60) {
            // update checked times
            removedServers.put(server, nodeDownTime + ":" + (++checkedTimes));
            return false;
        }else{
            return true;
        }
    }
}//end RegisteredServers
