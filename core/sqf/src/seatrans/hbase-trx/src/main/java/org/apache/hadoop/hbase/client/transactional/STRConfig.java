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
//
// @@@ END COPYRIGHT @@@

package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.pit.HBaseBinlog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

/**
 * STR Config.
 */
public class STRConfig {

    static final Log LOG = LogFactory.getLog(STRConfig.class);

    static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    static final String ZK_PORT   = "hbase.zookeeper.property.clientPort";

    public static final int PEER_CONNECTION_DOWN = 0; // temporary workaround, only applied to single peer
    public static final int PEER_CONNECTION_UP = 1;

    private static boolean                     sb_replicate = false;
    private static Map<Integer, Configuration> peer_configs;
    private static Map<Integer, Connection>    peer_connections;
    private static Map<Integer, PeerInfo>      peer_info_list;
    private static HBaseDCZK                   sv_dc_zk;
    private static String                      sv_traf_cluster_id;
    private static int                         sv_traf_cluster_id_int;
    private static String                      sv_traf_instance_id;
    private static int                         sv_traf_instance_id_int;
    private static int                         sv_configured_peer_count = 0;
    private static int                         sv_sup_peer_count = 0;
    private static int                         sv_first_remote_peer_id = -1;

    private static int                         sv_trafodion_node_count = -1;
    private static int                         sv_local_safemode = 0;
    private static int                         sv_peer_connection_state = PEER_CONNECTION_DOWN;

    private static STRConfig s_STRConfig = null; 

    private static void add_peer(Configuration pv_config,
				 int           pv_peer_num, boolean sup)
	throws IOException 
    {
	if (LOG.isTraceEnabled()) LOG.trace("Adding config info in the map for cluster id: " + pv_peer_num
					    + " peer config: " + pv_config.get(ZK_QUORUM));
	peer_configs.put(pv_peer_num, pv_config);
	if (LOG.isTraceEnabled()) LOG.trace("Added config info in the peer_configs map for cluster id: " + pv_peer_num);

        if (sup) { // if peer is SUP, then set up conection through peer ZK
	   try {
	       Connection lv_connection = ConnectionFactory.createConnection(pv_config);
	       if (LOG.isTraceEnabled()) LOG.trace("Created connection for peer: " + pv_peer_num
						+ " connection: " + lv_connection);
	       peer_connections.put(pv_peer_num, lv_connection);
               sv_peer_connection_state = PEER_CONNECTION_UP; // 0 is not set, 1 is set, 2 is to set if not yet 
	       if (LOG.isTraceEnabled()) LOG.trace("Added connection in the peer_connections map for cluster id: " + pv_peer_num);
	   }
	   catch (Exception e) {
               sv_peer_connection_state = PEER_CONNECTION_DOWN; // 0 is not set, 1 is set, 2 is to set if not yet         
	       LOG.error("Exception while creating the connection: disable peer connection " + sv_peer_connection_state +
                                              " exception " + e);
	       e.printStackTrace();
	       LOG.error("cause: " + e.getCause());
	   }
        } // sup
        else {
                sv_peer_connection_state = PEER_CONNECTION_DOWN; // 0 is not set, 1 is set, 2 is to set if not yet
                if (LOG.isWarnEnabled()) LOG.warn("Skip adding peer connection for cluster id: " + pv_peer_num
                                            + " peer config: " + pv_config.get(ZK_QUORUM) + " due to SUP " + sup
                                            + " peer connection state " + sv_peer_connection_state );
        }

       if (LOG.isDebugEnabled()) LOG.debug("peer#"
					  + pv_peer_num 
					  + ":zk quorum: " + (peer_configs.get(pv_peer_num)).get(ZK_QUORUM)
					  + ":zk clientPort: " + (peer_configs.get(pv_peer_num)).get(ZK_PORT)
                                          + " SUP " + sup + " peer connection state " + sv_peer_connection_state
					  );
    }

    private static void add_peer(Configuration pv_config,
				 String pv_peer_num_string,
				 String pv_quorum,
				 String pv_port, 
                                 boolean sup)
	throws IOException 
    {
	Configuration lv_config = HBaseConfiguration.create(pv_config);

	lv_config.set(ZK_QUORUM, pv_quorum);
	lv_config.set(ZK_PORT, pv_port);

	int lv_peer_num = Integer.parseInt(pv_peer_num_string);
	lv_config.setInt("esgyn.cluster.id", lv_peer_num);

        int zkTimeoutInt = 15000;
        String zkTimeout = System.getenv("TM_STRCONFIG_PEER_ZK_TIMEOUT");
        if (zkTimeout != null){
           zkTimeoutInt = Integer.parseInt(zkTimeout.trim());
           if (LOG.isDebugEnabled()) LOG.debug("xDC: TM_STRCONFIG_PEER_ZK_TIMEOUT: " + zkTimeoutInt);
        }

        int zkRetryInt = 1;
        String zkRetry = System.getenv("TM_STRCONFIG_PEER_ZK_RETRY");
        if (zkRetry != null){
           zkRetryInt = Integer.parseInt(zkRetry.trim());
           if (LOG.isDebugEnabled()) LOG.debug("xDC: TM_STRCONFIG_PEER_ZK_RETRY: " + zkRetryInt);
        }

        String value = lv_config.getTrimmed("zookeeper.session.timeout");
        if (LOG.isDebugEnabled()) LOG.debug("xDC: STRConfig Peer " + lv_peer_num + " ,zookeeper.session.timeout " + value);
        if (  (zkTimeout != null) && ((value == null) || (Integer.parseInt(value) > zkTimeoutInt))  ) {
             lv_config.set("zookeeper.session.timeout", Integer.toString(zkTimeoutInt));
             value = lv_config.getTrimmed("zookeeper.session.timeout");
             if (LOG.isDebugEnabled()) LOG.debug("xDC: STRConfig Peer " + lv_peer_num + " ,revised zookeeper.session.timeout " + value);
        }
        value = lv_config.getTrimmed("zookeeper.recovery.retry");
        if (LOG.isDebugEnabled()) LOG.debug("xDC: STRConfig Peer " + lv_peer_num + " ,zookeeper.recovery.retry " + value);
        if (  (zkRetry != null) && ((value == null) || (Integer.parseInt(value) > zkRetryInt))  ) {
             lv_config.set("zookeeper.recovery.retry", Integer.toString(zkRetryInt));
             value = lv_config.getTrimmed("zookeeper.recovery.retry");
             if (LOG.isDebugEnabled()) LOG.debug("xDC: STRConfig Peer " + lv_peer_num + " ,revised zookeeper.recovery.retry " + value);
        }

        //for peer cluster, set retry number as 1 to reduce the hung time described in M-16300
        int hbRetryInt = 1 ; //this should be safe
        String hbRetry = System.getenv("TM_STRCONFIG_PEER_HBASE_RETRY");
        if (hbRetry != null){
           hbRetryInt = Integer.parseInt(hbRetry.trim());
           if (LOG.isDebugEnabled()) LOG.debug("xDC: TM_STRCONFIG_PEER_HBASE_RETRY: " + hbRetryInt);
        }

        value = lv_config.getTrimmed("hbase.client.retries.number");
        if (LOG.isDebugEnabled()) LOG.debug("xDC: STRConfig Peer " + lv_peer_num + " ,hbase.client.retries.number" + value);
        if (  (hbRetry != null) && ((value == null) || (Integer.parseInt(value) > hbRetryInt))  ) {
             lv_config.set("hbase.client.retries.number", Integer.toString(hbRetryInt));
             value = lv_config.getTrimmed("hbase.client.retries.number");
             if (LOG.isDebugEnabled()) LOG.debug("xDC: STRConfig Peer " + lv_peer_num + " ,revised hbase.client.retries.number" + value);
        }

	add_peer(lv_config,
		 lv_peer_num, sup);

    }

    public static void initObjects(Configuration pv_config)
	throws IOException 
    {
	if (pv_config == null) {
	    return;
	}

	pv_config.set("hbase.hregion.impl", "org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion");
	//v_config.setInt("hbase.client.retries.number", 3);

        int zkTimeoutInt = 60000;
        String zkTimeout = System.getenv("TM_STRCONFIG_ZK_TIMEOUT");
        if (zkTimeout != null){
           zkTimeoutInt = Integer.parseInt(zkTimeout.trim());
           if (LOG.isDebugEnabled()) LOG.debug("xDC: TM_STRCONFIG_ZK_TIMEOUT: " + zkTimeoutInt);
        }

        int zkRetryInt = 1;
        String zkRetry = System.getenv("TM_STRCONFIG_ZK_RETRY");
        if (zkRetry != null){
           zkRetryInt = Integer.parseInt(zkRetry.trim());
           if (LOG.isInfoEnabled()) LOG.info("xDC: TM_STRCONFIG_ZK_RETRY: " + zkRetryInt);
        }

        int rpcTimeoutInt = 60000;
        String rpcTimeout = System.getenv("TM_STRCONFIG_RPC_TIMEOUT");
        if (rpcTimeout != null){
           rpcTimeoutInt = Integer.parseInt(rpcTimeout.trim());
           if (LOG.isInfoEnabled()) LOG.info("xDC: TM_STRCONFIG_RPC_TIMEOUT: " + rpcTimeoutInt);
        }
        
        int clientRetryInt = 3;  
        String clientRetry = System.getenv("TM_STRCONFIG_CLIENT_RETRIES_NUMBER");
        if (clientRetry != null){
           clientRetryInt = Integer.parseInt(clientRetry.trim());
           if (LOG.isInfoEnabled()) LOG.info("xDC: TM_STRCONFIG_RPC_TIMEOUT: " + clientRetryInt);
        }        

        String value = pv_config.getTrimmed("zookeeper.session.timeout");
        if (LOG.isDebugEnabled()) LOG.debug("STRConfig " + "zookeeper.session.timeout " + value);
        if (  (zkTimeout != null) && ((value == null) || (Integer.parseInt(value) > zkTimeoutInt))  ) {
             pv_config.set("zookeeper.session.timeout", Integer.toString(zkTimeoutInt));
             value = pv_config.getTrimmed("zookeeper.session.timeout");
             if (LOG.isInfoEnabled()) LOG.info("xDC: STRConfig " + "revised zookeeper.session.timeout " + value);
        }
        value = pv_config.getTrimmed("zookeeper.recovery.retry");
        if (LOG.isDebugEnabled()) LOG.debug("STRConfig " + "zookeeper.recovery.retry " + value);
        if (  (zkRetry != null) && ((value == null) || (Integer.parseInt(value) > zkRetryInt))  ) {
             pv_config.set("zookeeper.recovery.retry", Integer.toString(zkRetryInt));
             value = pv_config.getTrimmed("zookeeper.recovery.retry");
             if (LOG.isInfoEnabled()) LOG.info("xDC: STRConfig " + "revised zookeeper.recovery.retry " + value);
        }
        value = pv_config.getTrimmed("hbase.rpc.timeout");
        if (LOG.isDebugEnabled()) LOG.debug("STRConfig " + "hbase.rpc.timeout " + value);
        if (  (rpcTimeout != null) && ((value == null) || (Integer.parseInt(value) > rpcTimeoutInt))  ) {
             pv_config.set("hbase.rpc.timeout", Integer.toString(rpcTimeoutInt));
             value = pv_config.getTrimmed("hbase.rpc.timeout");
             if (LOG.isInfoEnabled()) LOG.info("xDC: STRConfig " + "revised hbase.rpc.timeout " + value);
        }
        value = pv_config.getTrimmed("hbase.client.retries.number");
        if (LOG.isDebugEnabled()) LOG.debug("STRConfig " + "hbase.client.retries.number " + value);
        if (  (value == null) || (Integer.parseInt(value) > clientRetryInt)  ) {
             pv_config.set("hbase.client.retries.number", Integer.toString(clientRetryInt));
             value = pv_config.getTrimmed("hbase.client.retries.number");
             if (LOG.isInfoEnabled()) LOG.info("xDC: STRConfig " + "revised hbase.client.retries.number " + value);
        }        

//      Add here for info, these settings should be defined from higher level (Traf Configuration ?)

//       pv_config.set("zookeeper.session.timeout", Integer.toString(30000));
//       pv_config.set("zookeeper.recovery.retry", Integer.toString(1));
//       pv_config.set("hbase.rpc.timeout", "30000");  

	peer_configs = new HashMap<Integer, Configuration>();
	peer_connections = new HashMap<Integer, Connection>();

	sv_dc_zk = new HBaseDCZK(pv_config);
	peer_info_list = sv_dc_zk.list_clusters();
	sv_traf_cluster_id = sv_dc_zk.get_my_id();
	if (sv_traf_cluster_id == null) {
	    sv_traf_cluster_id = "1";
	}
	sv_traf_cluster_id_int = Integer.parseInt(sv_traf_cluster_id);

        sv_traf_instance_id = "0";
	sv_traf_instance_id_int = Integer.parseInt(sv_traf_instance_id);

	if (LOG.isDebugEnabled()) LOG.debug("My cluster id: " + sv_traf_cluster_id + ", peer_info_list size: " +
			(peer_info_list == null ? "null " : peer_info_list.size()));
	pv_config.setInt("esgyn.cluster.id", sv_traf_cluster_id_int);

    }

    public static void initClusterConfigsZK(Configuration pv_config) 
	throws IOException 
    {
	if (LOG.isDebugEnabled()) LOG.debug("initClusterConfigsZK ENTRY");

	initObjects(pv_config);

	// Put myself in the list of configurations
	add_peer(pv_config, 0, true); // always need to set local connection

	try {

	    if (peer_info_list == null) {
		if (LOG.isDebugEnabled()) LOG.debug("initClusterConfigsZK: list_clusters returned null");
		return;
	    }

            String lv_local_safemode = System.getenv("XDC_LOCAL_SAFEMODE");
            if (lv_local_safemode != null)
                  sv_local_safemode = Integer.parseInt(lv_local_safemode);
            if (LOG.isDebugEnabled()) LOG.debug("XDC local safe mode: " + sv_local_safemode);

		if (LOG.isDebugEnabled()) LOG.debug("initClusterConfigsZK: peer_info_list size: " + peer_info_list.size());
	    for (PeerInfo lv_pi : peer_info_list.values()) {
		if (LOG.isTraceEnabled()) LOG.trace("initClusterConfigsZK: " + lv_pi);

		if (lv_pi.get_id().equals(sv_traf_cluster_id)) {
		    continue;
		}

                if (sv_local_safemode == 1) {
                     if (LOG.isWarnEnabled()) LOG.warn("Bypass XDC peer configuration, XDC local safe mode: " + sv_local_safemode);
                     continue;
                }

		add_peer(pv_config,
			 lv_pi.get_id(),
			 lv_pi.get_quorum(),
			 lv_pi.get_port(),
                         lv_pi.isSTRUp());

		if (sv_first_remote_peer_id == -1) {
		    sv_first_remote_peer_id = Integer.parseInt(lv_pi.get_id());
		}

		sv_configured_peer_count++;
		if (lv_pi.isSTRUp()) {
		    sv_sup_peer_count++;
		}
	    }

            if (LOG.isInfoEnabled()) LOG.info("initClusterConfigsZK:"
					  + " configured peer count: " + sv_configured_peer_count
					  + " sup peer count: " + sv_sup_peer_count
                      + " XDC local safe mode: " + sv_local_safemode
                      + " My cluster id: " + sv_traf_cluster_id
					  );
	}
	catch (Exception e) {
	    LOG.error("Exception while adding peer info to the config: ", e);
	}
	
    }

    public boolean shieldFromRemote(int pv_cluster_id) {
    if (peer_info_list == null){
       if (LOG.isTraceEnabled()) LOG.trace("getPeerInfo for cluster: " + pv_cluster_id + " peer_info_list is null");
       return false;
    }
    if (LOG.isTraceEnabled()) LOG.trace("getPeerInfo for cluster: " + pv_cluster_id + " peer_info_list size: " + peer_info_list.size());
    PeerInfo lv_pi = peer_info_list.get(pv_cluster_id);
    if (lv_pi == null) {
       if (LOG.isTraceEnabled()) LOG.trace("getPeerInfo for cluster: " + pv_cluster_id + " peer_info is null");
       return false;
    }
    return lv_pi.is_shield();
    }

    public PeerInfo getPeerInfo(int pv_cluster_id) {
    if (peer_info_list == null){
       if (LOG.isTraceEnabled()) LOG.trace("getPeerInfo for cluster: " + pv_cluster_id + " peer_info_list is null");
       return null;
    }
    if (LOG.isTraceEnabled()) LOG.trace("getPeerInfo for cluster: " + pv_cluster_id + " peer_info_list size: " + peer_info_list.size());
	PeerInfo lv_pi = peer_info_list.get(pv_cluster_id);


	return lv_pi;
    }

    public synchronized void setPeerStatus(int    pv_cluster_id,
					   String pv_status)
    {

	if (LOG.isTraceEnabled()) LOG.trace("setPeerStatus: "
					    + " cluster id: " + pv_cluster_id
					    + " status: " + pv_status
					    );

	if (pv_status == null) {
	    return;
	}

	PeerInfo lv_pi = getPeerInfo(pv_cluster_id);
	if (lv_pi != null) {
	    boolean previouslySTRUp = lv_pi.isSTRUp();
	    lv_pi.set_status(pv_status);
	    boolean nowSTRUp = lv_pi.isSTRUp();

            if (   (!nowSTRUp) && (sv_peer_connection_state == PEER_CONNECTION_UP)   ) {
                   sv_peer_connection_state = PEER_CONNECTION_DOWN;
                   if (LOG.isInfoEnabled()) LOG.info("setPeerStatus: " +
                                " nowSTRUP " + nowSTRUp + " peer connection " + sv_peer_connection_state +
                                " disable peer connectioon state " + sv_peer_connection_state );
            }

	    if (previouslySTRUp && ! nowSTRUp) {
		--sv_sup_peer_count;
	    }
	    else if (! previouslySTRUp && nowSTRUp) {
		++sv_sup_peer_count;
	    }
	}

	if (LOG.isInfoEnabled()) LOG.info("setPeerStatus: "
					  + " configured peer count: " + sv_configured_peer_count
					  + " sup peer count: " + sv_sup_peer_count
					  );
	return;
    }

    public String getPeerStatus(int pv_cluster_id)
    {
       return getPeerStatus( pv_cluster_id, false);
    }

    public String getPeerStatus(int pv_cluster_id, boolean complete_status)
    {

	if (LOG.isTraceEnabled()) LOG.trace("getPeerStatus"
					    + " cluster id: " + pv_cluster_id
					    );

	PeerInfo lv_pi = getPeerInfo(pv_cluster_id);
	if (lv_pi != null) {
	      lv_pi.set_complete_status(complete_status);
	    return lv_pi.get_status();
	}
	return "";
    }

    public int getConfiguredPeerCount() 
    {
	return sv_configured_peer_count;
    }

    public int getSupPeerCount() 
    {
	return sv_sup_peer_count;
    }

    public Configuration getPeerConfiguration(int pv_cluster_id, boolean pv_STR_should_be_up) 
    {
	if (pv_cluster_id == sv_traf_cluster_id_int) {
	    pv_cluster_id = 0;
	}

	if (pv_STR_should_be_up) {
	    PeerInfo lv_pi = getPeerInfo(pv_cluster_id == 0 ? sv_traf_cluster_id_int : pv_cluster_id);
	    if (lv_pi == null) {
		return null;
	    }
	    if (! lv_pi.isSTRUp()) {
		return null;
	    }
	} 

	return peer_configs.get(pv_cluster_id);
    }

    public Configuration getPeerConfiguration(int pv_cluster_id)
    {
	boolean lv_STR_should_be_up_flag;
	
	lv_STR_should_be_up_flag = true;

	if ((pv_cluster_id == 0) || 
	    (pv_cluster_id == sv_traf_cluster_id_int)) {
	    lv_STR_should_be_up_flag = false;
	}
	
	return getPeerConfiguration(pv_cluster_id, lv_STR_should_be_up_flag);
    }

    public Map<Integer, Configuration> getPeerConfigurations()
    {
	return peer_configs;
    }

    public Connection resetPeerConnection(int pv_peer_id) throws IOException
    {
        if (pv_peer_id == sv_traf_cluster_id_int) {
            pv_peer_id = 0;
        }

        peer_connections.get(pv_peer_id).close();
        Configuration lv_config = getPeerConfiguration(pv_peer_id, false);
        try {
           Connection lv_connection = ConnectionFactory.createConnection(lv_config);
           if (LOG.isTraceEnabled()) LOG.trace("Recreated connection for peer: " + pv_peer_id + " connection: " + lv_connection);
           peer_connections.put(pv_peer_id, lv_connection);
           sv_peer_connection_state = PEER_CONNECTION_UP;
           if (LOG.isTraceEnabled()) LOG.trace("Reset connection in the peer_connections map for cluster id: " + pv_peer_id);
        } catch (Exception e) {
            LOG.error("Exception while recreating the connection: " + e);
            sv_peer_connection_state = PEER_CONNECTION_DOWN;
            e.printStackTrace();
            LOG.error("cause: " + e.getCause());
        }
        return peer_connections.get(pv_peer_id);
    }

    public Connection addPeerConnection(int pv_peer_id) 
    {
        if (pv_peer_id == sv_traf_cluster_id_int) {
            LOG.error("Should not add local connection ");
            pv_peer_id = 0;
        }


        Configuration lv_config = getPeerConfiguration(pv_peer_id, false);
        try {
           Connection lv_connection = ConnectionFactory.createConnection(lv_config);
           if (LOG.isDebugEnabled()) LOG.debug("adding connection for peer: " + pv_peer_id + " connection: " + lv_connection);
           peer_connections.put(pv_peer_id, lv_connection);
           if (LOG.isTraceEnabled()) LOG.trace("add connection in the peer_connections map for cluster id: " + pv_peer_id);
           sv_peer_connection_state = PEER_CONNECTION_UP;
        } catch (Exception e) {
            LOG.error("Exception while adding peer connection: " + e);
            sv_peer_connection_state = PEER_CONNECTION_DOWN;
            e.printStackTrace();
            LOG.error("cause: " + e.getCause());
        }
        return peer_connections.get(pv_peer_id);
    }


    public Connection getPeerConnection(int pv_peer_id)
    {
	if (pv_peer_id == sv_traf_cluster_id_int) {
	    pv_peer_id = 0;
	}

	return peer_connections.get(pv_peer_id);
    }

    public Map<Integer, Connection> getPeerConnections()
    {
	return peer_connections;
    }

    public Map<Integer, PeerInfo> getPeerInfos()
    {
	return peer_info_list;
    }

    public Set<Integer> getPeerIds() 
    {
       return peer_configs.keySet();
    }
        
    public String getTrafClusterId()
    {
	return sv_traf_cluster_id;
    }

    public int getTrafClusterIdInt()
    {
       return Integer.parseInt(sv_traf_cluster_id);
    }

    public String getTrafInstanceId()
    {
	return sv_traf_instance_id;
    }

    public int getTrafInstanceIdInt()
    {
       return sv_traf_instance_id_int;
    }

    public int getFirstRemotePeerId()
    {
	return sv_first_remote_peer_id;
    }

    public int getPeerConnectionState()
    {
        return sv_peer_connection_state;
    }

    public void setPeerConnectionState(int value)
    {
        sv_peer_connection_state = value;
    }

    public static void setTrafodionNodeCount()
    {
	String lv_trafodion_node_count_string = System.getenv("TRAFODION_NODE_COUNT");
	if (lv_trafodion_node_count_string != null) {
	    sv_trafodion_node_count = Integer.parseInt(lv_trafodion_node_count_string);
	}
	else {
	    sv_trafodion_node_count = 0;
	}
	if (LOG.isDebugEnabled()) LOG.debug("TRAFODION_NODE_COUNT = " + sv_trafodion_node_count);
    }

    public int getTrafodionNodeCount()
    {
	return sv_trafodion_node_count;
    }

    public HBaseDCZK dczk() {
        return sv_dc_zk;
    }

    public static STRConfig getInstance() throws IOException, ZooKeeperConnectionException  {
        if (s_STRConfig == null) {
            final Configuration conf = HBaseConfiguration.create();
            return getInstance(conf);
        }
        return s_STRConfig;
    }

    // getInstance to return the singleton object for TransactionManager
    public synchronized static STRConfig getInstance(final Configuration conf) 
	throws IOException, ZooKeeperConnectionException 
    {
	if (s_STRConfig == null) {
	
	    s_STRConfig = new STRConfig(conf);
	}
	return s_STRConfig;
    }

    /**
     * @param conf
     * @throws ZooKeeperConnectionException
     */
    private STRConfig(final Configuration conf)
	throws ZooKeeperConnectionException, IOException 
    
    {
	setTrafodionNodeCount();
	
	initClusterConfigsZK(conf);

	if (sv_dc_zk != null) {
	    sv_dc_zk.watch_all();
	    XDCStatusWatcher lv_pw = new XDCStatusWatcher(sv_dc_zk.getZKW());
	    lv_pw.setDCZK(sv_dc_zk);
	    lv_pw.setSTRConfig(this);
	    sv_dc_zk.register_status_listener(lv_pw);
	}
    }

    public String toString()
    {
	StringBuilder lv_sb = new StringBuilder();
	String lv_str;
	lv_str = "Number of configured peers: " + sv_configured_peer_count;
	lv_sb.append(lv_str);
	for ( Map.Entry<Integer, Configuration> e : peer_configs.entrySet() ) {
	    lv_str = "\n======\nID: " + e.getKey() + "\n";
	    lv_sb.append(lv_str);
	    lv_str = ZK_QUORUM + ": " + e.getValue().get(ZK_QUORUM) + "\n";
	    lv_sb.append(lv_str);
	    lv_str = ZK_PORT + ": " + e.getValue().get(ZK_PORT);
	    lv_sb.append(lv_str);
	}

	return lv_sb.toString();
    }

    public static void main(String[] args) {
	STRConfig pSTRConfig = null;
	Configuration lv_config = HBaseConfiguration.create();
	try {
	    pSTRConfig = STRConfig.getInstance(lv_config);
	} catch (IOException ioe) {
	    System.out.println("IO Exception trying to get STRConfig instance: " + ioe);
	}
	
	System.out.println(pSTRConfig);
	System.out.println(pSTRConfig.getPeerStatus(pSTRConfig.getTrafClusterIdInt()));
	System.out.println(pSTRConfig.getPeerStatus(2));
	System.out.println(pSTRConfig.getFirstRemotePeerId());

	System.out.println("getPeerConfiguration(0): " + pSTRConfig.getPeerConfiguration(0));
	System.out.println("getPeerConfiguration(1): " + pSTRConfig.getPeerConfiguration(1));
	System.out.println("getPeerConfiguration(1, false): " + pSTRConfig.getPeerConfiguration(1, false));
	System.out.println("getPeerConfiguration(1, true): " + pSTRConfig.getPeerConfiguration(1, true));
	System.out.println("getPeerConfiguration(1, true): " + pSTRConfig.getPeerConfiguration(1, true));
	System.out.println("getPeerConfiguration(1, false): " + pSTRConfig.getPeerConfiguration(1, false));

	System.out.println("getPeerConfiguration(2): " + pSTRConfig.getPeerConfiguration(2));
	System.out.println("getPeerConfiguration(2, false): " + pSTRConfig.getPeerConfiguration(2, false));
	System.out.println("getPeerConfiguration(2, true): " + pSTRConfig.getPeerConfiguration(2, true));

	System.out.println("getPeerConnnection(0): " + pSTRConfig.getPeerConnection(0));
	System.out.println("getPeerConnnection(1): " + pSTRConfig.getPeerConnection(1));
	System.out.println("getPeerConnnection(2): " + pSTRConfig.getPeerConnection(2));
    }

}

