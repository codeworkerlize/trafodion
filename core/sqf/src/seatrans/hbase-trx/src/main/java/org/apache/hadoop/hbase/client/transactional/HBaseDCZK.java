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

import java.nio.charset.Charset;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Date;
import java.util.concurrent.TimeUnit;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Abortable;

import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.XDCStatusWatcher;

import org.apache.hadoop.hbase.pit.BackupRestoreClient;

import org.apache.log4j.PropertyConfigurator;
import org.apache.hadoop.hbase.coprocessor.transactional.message.RSMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockCleanXdcMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSConnection;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.XDC_STATUS;
import com.esgyn.common.LicenseHelper;


/**
 * Multi Data Center specific Zookeeper operations
 */
public class HBaseDCZK implements Abortable {

    public static void setupConf() {
    	String confFile = System.getProperty("trafodion.log4j.configFile");
    	if (confFile == null) {
	        System.setProperty("hostName", System.getenv("HOSTNAME"));
    		System.setProperty("trafodion.tools.log", System.getenv("TRAF_LOG") + "/trafodion.tools.java.${hostName}.log");
    		confFile = System.getenv("TRAF_CONF") + "/log4j.tools.config";
    	}
    	PropertyConfigurator.configure(confFile);
    }

    static final Log LOG = LogFactory.getLog(HBaseDCZK.class);

    private static final Charset CHARSET        = Charset.forName("UTF-8");

    public static final String m_traf_cluster_id_string  = "traf_cluster_id";
    public static final String m_traf_instance_id_string  = "traf_instance_id";
    public static final String m_lock_string           = "lock";
    public static final String m_clusters_string       = "clusters";
    public static final String m_quorum_string         = "quorum";
    public static final String m_port_string           = "port";
    public static final String m_status_string         = "status";
    public static final String m_peer_attribute_string = "peer_attribute";
    public static final String m_trafodion_up_string   = "trafodion_up";

    public static final String m_base_node            = "/trafodion/multi_dc";
    public static final String m_clusters_node        = m_base_node + "/" + m_clusters_string;
    public static final String m_traf_cluster_id_node   = m_base_node + "/" + m_traf_cluster_id_string;
    public static final String m_traf_instance_id_node   = m_base_node + "/" + m_traf_instance_id_string;
    public static final String m_lock_node            = m_base_node + "/" + m_lock_string;

    private             ZooKeeperWatcher              m_zkw;
    private             Configuration                 m_config;
	
    /**
     * @param conf
     * @throws Exception
     */
    public HBaseDCZK(final Configuration pv_config) 
	throws IOException 
    {
	if (LOG.isTraceEnabled()) LOG.trace("HBaseDCZK(conf) -- ENTRY");
	m_config = pv_config;
	this.m_zkw = new ZooKeeperWatcher(m_config, "DC", this, false);
    }
	
    /**
     * @param znode
     * @return znode data
     * @throws KeeperException
     */
    private byte [] get_znode_data (String znode) 
             throws IOException
    {
        boolean loopBack = false;
        do
        {
	try {
            loopBack = false;
	    return ZKUtil.getData(m_zkw, znode);
	}
        catch(InterruptedException ie) {
           loopBack = true;
        }
	catch(KeeperException.NoNodeException ke) {
	    LOG.debug(m_zkw.prefix("Unable to list children of znode " 
				   + znode + " because node does not exist (not an error)"));
	    return null;
	} 
        catch (KeeperException ke1) {
            throw new IOException(ke1);
        }
        } while (loopBack);
        return null;
    }
	
    /**
     * @param znode
     * @return List<String> of children nodes
     * @throws KeeperException
     */
    private List<String> get_znode_children(String pv_node) 
	throws IOException 
    {
        try {
	    return ZKUtil.listChildrenNoWatch(m_zkw, pv_node);
        }
        catch (KeeperException ze) {
           throw new IOException(ze);
        }
    }

    /**
     * @param toDelete
     * @throws KeeperException
     */
    public void delete_cluster_entry(String pv_cluster_id ) 
	throws IOException 
    {
	if (LOG.isDebugEnabled()) LOG.debug("delete_cluster_entry -- ENTRY -- key: " + pv_cluster_id);
        try {
	   ZKUtil.deleteNodeFailSilent(m_zkw, m_clusters_node + "/" + pv_cluster_id);
        } catch (KeeperException ze) {
            throw new IOException(ze);
        }
	if (LOG.isDebugEnabled()) LOG.debug("delete_cluster_entry -- EXIT ");
    }

    public ZooKeeperWatcher getZKW() 
    {
	return m_zkw;
    }

    public void register_status_listener(ZooKeeperListener pv_zkl) 
	throws IOException
    {
	if (pv_zkl == null) {
	    return;
	}

	m_zkw.registerListener(pv_zkl);
    }

    /**
     * @param cluster Id
     * @throws IOException
     */
    public void watch_status_znode(String pv_cluster_id)
	throws IOException
    {

	try {
	    String lv_cluster_id_node = m_clusters_node + "/" + pv_cluster_id;
	    String lv_status_node = lv_cluster_id_node + "/" + m_status_string;
	    if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:watch_status_znode " + lv_status_node);
	    ZKUtil.setWatchIfNodeExists(m_zkw, lv_status_node);
	}
	catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:watch_status_znode: ZKW. Error in setting watch: " 
				  + m_clusters_node + "/" + pv_cluster_id
				  + " , throwing IOException " + e
				  );
	}
    }

    /**
     * @param cluster Id
     * @throws IOException
     */
    public void watch_peer_attribute_znode(String pv_cluster_id)
	throws IOException
    {

	try {
	    String lv_cluster_id_node = m_clusters_node + "/" + pv_cluster_id;
	    String lv_status_node = lv_cluster_id_node + "/" + m_peer_attribute_string;
	    if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:watch_peer_attribute_znode " + lv_status_node);
	    ZKUtil.setWatchIfNodeExists(m_zkw, lv_status_node);
	}
	catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:watch_status_znode: ZKW. Error in setting watch: " 
				  + m_clusters_node + "/" + pv_cluster_id
				  + " , throwing IOException " + e
				  );
	}
    }

    /**
     * @param cluster Id
     * @throws IOException
     */
    public void watch_clusters_znode()
	throws IOException
    {
	
	if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:watch_clusters_znode ENTRY");
		 
	try {
	    ZKUtil.setWatchIfNodeExists(m_zkw, m_clusters_node);
	}
	catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:watch_clusters_znode: ZKW Unable to create watch on: " 
				  + m_clusters_node 
				  + " , throwing IOException " + e
				  );
	}
    }

    /**
     * @throws IOException
     */
    public void watch_status_all_clusters()
	throws IOException
    {
	
    if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:watch_status_all_clusters ENTRY");
		 
	Map<Integer, PeerInfo> lv_pi_list = list_clusters();
	
	if (lv_pi_list == null) {
	    return;
	}

	for (PeerInfo lv_pi : lv_pi_list.values()) {
	    watch_status_znode(lv_pi.get_id());
	    watch_peer_attribute_znode(lv_pi.get_id());
	}
    }

    /**
     * @throws IOException
     */
    public void watch_peer_attribute_all_clusters()
	throws IOException
    {
	
	if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:watch_peer_attribute_all_clusters ENTRY");
		 
	Map<Integer, PeerInfo> lv_pi_list = list_clusters();
	
	if (lv_pi_list == null) {
	    return;
	}

	for (PeerInfo lv_pi : lv_pi_list.values()) {
	    watch_peer_attribute_znode(lv_pi.get_id());
	}
    }

    public void watch_all() 
	throws IOException
    {
    if (LOG.isDebugEnabled()) LOG.debug("watch_all ENTRY");
	watch_clusters_znode();
	watch_status_all_clusters();
	watch_peer_attribute_all_clusters();
    }

    /**
     * @param cluster Id
     * @param quorum
     * @param port
     * @param status
     * @param peer_attribute
     * @throws IOException
     */
    public void set_peer_znode(String pv_cluster_id,
			       String pv_quorum, 
			       String pv_port,
			       String pv_status,
			       String pv_peer_attribute
			       )
	throws IOException 
    {

    if (LOG.isInfoEnabled()) LOG.info("HBaseDCZK:set_peer_znode: "
		 + " cluster_id : " + pv_cluster_id
		 + " quorum: " + pv_quorum
		 + " port: " + pv_port
		 + " status: " + pv_status
		 + " xdc attribute: " + pv_peer_attribute
		 );
		 
	try {

	    if (this.is_locked(pv_cluster_id)) {
		System.out.println("Could not update info of cluster id: " + pv_cluster_id
				   + " as it is locked.");
		
		return;
	    }

		Map<Integer, PeerInfo> peer_info_list = list_clusters();
		if(peer_info_list != null && peer_info_list.size() > 0 ) {
			for (Map.Entry<Integer,PeerInfo> entry : peer_info_list.entrySet()) {
				PeerInfo peerInfo = entry.getValue();
				String cluster_id = String.valueOf(entry.getKey());
				if(peerInfo.get_quorum().equalsIgnoreCase(pv_quorum)
						&& !cluster_id.equalsIgnoreCase(pv_cluster_id)) {
					delete_peer_znode(cluster_id);
				}
			}
		}

		String lv_cluster_id_node = m_clusters_node + "/" + pv_cluster_id;
	    ZKUtil.createWithParents(m_zkw, lv_cluster_id_node);

	    String lv_zk_node;
	    if ((pv_quorum != null) && (pv_quorum.length() > 0)) {
		lv_zk_node = lv_cluster_id_node + "/" + m_quorum_string;
		ZKUtil.createSetData(m_zkw, lv_zk_node, pv_quorum.getBytes(CHARSET));
	    }

	    if ((pv_port != null) && (pv_port.length() > 0)) {
		lv_zk_node = lv_cluster_id_node + "/" + m_port_string;
		ZKUtil.createSetData(m_zkw, lv_zk_node, pv_port.getBytes(CHARSET));
	    }

	    if ((pv_status != null) && (pv_status.length() > 0)) {
		lv_zk_node = lv_cluster_id_node + "/" + m_status_string;
		ZKUtil.createSetData(m_zkw, lv_zk_node, pv_status.getBytes(CHARSET));
	    }

	    if ((pv_peer_attribute != null) && (pv_peer_attribute.length() > 0)) {
		lv_zk_node = lv_cluster_id_node + "/" + m_peer_attribute_string;
		ZKUtil.createSetData(m_zkw, lv_zk_node, pv_peer_attribute.getBytes(CHARSET));
	    }

	}
	catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:set_peer_znode: ZKW Unable to create peer zNode: " 
				  + m_clusters_node + "/" + pv_cluster_id
				  + " , throwing IOException " + e
				  );
	}
    }

    /**
     * @param cluster Id
     * @param quorum
     * @param port
     * @param status
     * @throws IOException
     */
    public void set_trafodion_znode(String pv_cluster_id,
				    String pv_node_id,
				    String pv_trafodion_status_string
				    ) 
	throws IOException 
    {

	if (pv_cluster_id == null) {
	    if (LOG.isTraceEnabled()) LOG.trace("set_trafodion_znode, pv_cluster_id is null");
	    return;
	}

	if (pv_node_id == null) {
	    if (LOG.isTraceEnabled()) LOG.trace("set_trafodion_znode, pv_node_id is null");
	    return;
	}

	if (LOG.isInfoEnabled()) LOG.info("set_trafodion_znode: "
					  + " cluster_id : " + pv_cluster_id
					  + " node_id : "    + pv_node_id
					  + " node_data : "  + pv_trafodion_status_string
					  );
	
	try {
	    String lv_trafodion_up_node = m_clusters_node 
		+ "/" + pv_cluster_id
		+ "/" + m_trafodion_up_string;

	    ZKUtil.createWithParents(m_zkw, lv_trafodion_up_node);
	    
	    String lv_ephemeral_node_id_node = lv_trafodion_up_node
		+ "/" + pv_node_id;
	    ZKUtil.createEphemeralNodeAndWatch(m_zkw,
					       lv_ephemeral_node_id_node,
					       pv_trafodion_status_string.getBytes(CHARSET));
	} 
	catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:set_trafodion_znode: ZKW Unable to create zNode: " 
				  + m_clusters_node 
				  + "/" + pv_cluster_id
				  + "/" + pv_node_id
				  + " , throwing IOException " + e
				  );
	}
    }

    /**
     * @param cluster Id
     * @throws IOException
     */
    public PeerInfo get_peer_znode(String pv_cluster_id) 
	throws IOException 
    {

    if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:get_peer_znode: "
		 + " cluster_id : " + pv_cluster_id
		 );

	String lv_cluster_id_node = m_clusters_node + "/" + pv_cluster_id;


	    byte[] znode_data = get_znode_data(lv_cluster_id_node);
	    if (znode_data == null) {
		return null;
	    }

	    PeerInfo lv_pi = new PeerInfo();
	    lv_pi.set_id(pv_cluster_id);

	    String lv_quorum_node = lv_cluster_id_node + "/" + m_quorum_string;
	    lv_pi.set_quorum(get_znode_data(lv_quorum_node));

	    String lv_port_node = lv_cluster_id_node + "/" + m_port_string;
	    lv_pi.set_port(get_znode_data(lv_port_node));

	    String lv_status_node = lv_cluster_id_node + "/" + m_status_string;
	    
	    if(LOG.isDebugEnabled()){
	    	byte[] znode_status_data = get_znode_data(lv_status_node);
	        LOG.debug("HBaseDCZK: get_peer_znode"
             + " node: " + lv_status_node
		     + " value: " + (znode_status_data == null ? "null" : new String(znode_status_data))
		     );
	    }
	    lv_pi.set_status(get_znode_data(lv_status_node));

	    String lv_peer_attribute_node = lv_cluster_id_node + "/" + m_peer_attribute_string;
	    lv_pi.set_status(get_znode_data(lv_peer_attribute_node));

	    String lv_trafodion_up_node = lv_cluster_id_node + "/" + m_trafodion_up_string;
	    List<String> lv_list_nodes = get_znode_children(lv_trafodion_up_node);
	    if ((lv_list_nodes != null) && 
		(lv_list_nodes.size() > 0)) {
		lv_pi.setTrafodionStatus(true);
		if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK: get_peer_znode"
			 + " node: " + lv_trafodion_up_node
			 + " value: " + lv_list_nodes.size()
			 );
	    }

	    return lv_pi;

    }

    /**
     * @param cluster Id
     * @throws IOException
     */
    public boolean delete_peer_znode(String pv_cluster_id) 
	throws IOException 
    {
	if (LOG.isTraceEnabled()) LOG.trace("HBaseDCZK:delete_peer_znode: "
					    + " cluster_id : " + pv_cluster_id
					    );

	try {
	    String lv_myid = get_my_id();
	    
	    if (pv_cluster_id.equals(lv_myid)) {
		System.out.println("Cannot delete your own info.");
		return false;
	    }
	    
	    Map<Integer, PeerInfo> lv_pi_list = list_clusters();
	
	    if (lv_pi_list == null) {
		return false;
	    }

	    boolean lb_found = false;
	    for (PeerInfo lv_pi : lv_pi_list.values()) {
		if (lv_pi.get_id().equals(pv_cluster_id)) {
		    lb_found = true;
		}
	    }

	    if ( ! lb_found ) {
		System.out.println("peer: " 
				   + pv_cluster_id 
				   + " does not exist");
		return false;
	    }

	    ZKUtil.deleteNodeRecursively(m_zkw,
					 m_clusters_node + "/" + pv_cluster_id
					 );

	} catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:delete_peer_znode: ZKW, KeeperException trying to delete: " 
				  + pv_cluster_id
				  + " , throwing IOException " + e
				  );
	}

	return true;

    }

    /**
     * @param cluster Id
     * @throws IOException
     */
    public Map<Integer, PeerInfo> list_clusters() 
	throws IOException 
    {
    if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:list_clusters ENTRY");

	List<String> peer_node_strings         = new ArrayList<String>();
	Map<Integer, PeerInfo> peer_info_list  = new HashMap<Integer, PeerInfo>();
	
	peer_node_strings = get_znode_children(m_clusters_node);
	if (peer_node_strings == null) {
       if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:list_clusters returning null");
       return null;
	}

	for (String peer_node_string : peer_node_strings) {
	    int lv_id = Integer.parseInt(peer_node_string);
	    PeerInfo lv_pi = get_peer_znode(peer_node_string);
	    peer_info_list.put(lv_id, lv_pi);
	}

    if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:list_clusters returning list with " + peer_info_list.size() + " elements");
	return peer_info_list;
    }

    /**
     * @param cluster Id
     * push_to ZK information from this cluster to the provided cluster
     * @throws IOException
     */
    public void push_to_cluster(String   pv_cluster_id,
				PeerInfo pv_pi) 
	throws IOException 
    {
        if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:push_to_cluster"
		 + " cluster id: " + pv_cluster_id
		 + " data: " + pv_pi
		 );

	    STRConfig     lv_STRConfig = null;
	    Configuration lv_config  = null;

	    lv_STRConfig = STRConfig.getInstance(m_config);
	    lv_config = lv_STRConfig.getPeerConfiguration(Integer.parseInt(pv_cluster_id), false);
	    if (lv_config == null) {
		System.out.println("Peer ID: " + pv_cluster_id + " does not exist OR it has not been configured.");
		return;
	    }
		
	    HBaseDCZK lv_zk = new HBaseDCZK(lv_config);
	    
	    if (lv_zk.is_locked(pv_cluster_id)) {
		System.out.println("Could not push info of cluster id: " + pv_pi.get_id()
				   + " to cluster id: " + pv_cluster_id
				   + " as it is locked.");
	    }
	    else {
		lv_zk.set_peer_znode(pv_pi.get_id(),
				     pv_pi.get_quorum(), 
				     pv_pi.get_port(),
				     pv_pi.get_status(),
				     pv_pi.get_attribute_string()
				     );
	    }
    }

    public void push_to_clusters() 
	throws IOException 
    {
    if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:push_to_clusters ENTRY");

	Map<Integer, PeerInfo> lv_pi_list = list_clusters();
	
	if (lv_pi_list == null) {
	    return;
	}

	    String        lv_my_id   = get_my_id();
	    PeerInfo      lv_my_pi   = lv_pi_list.get(Integer.parseInt(lv_my_id));
	    
	    for (PeerInfo lv_pi : lv_pi_list.values()) {
		if (lv_pi.get_id().equals(lv_my_id)) {
		    continue;
		}
		
		push_to_cluster(lv_pi.get_id(), lv_my_pi);
	    }
    }

    /**
     * @param cluster Id
     * pulls ZK information from the cluster to the provided cluster
     * @throws IOException
     */
    public void pull_cluster(String pv_cluster_id)
	throws IOException 
    {
       if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:pull_cluster"
		 + " cluster id: " + pv_cluster_id
		 );

	    STRConfig     lv_STRConfig = null;
	    Configuration lv_config  = null;

	    lv_STRConfig = STRConfig.getInstance(m_config);
	    lv_config = lv_STRConfig.getPeerConfiguration(Integer.parseInt(pv_cluster_id), false);
	    if (lv_config == null) {
		System.out.println("Peer ID: " + pv_cluster_id + " does not exist OR it has not been configured.");
		return;
	    }
		
	    HBaseDCZK lv_zk = new HBaseDCZK(lv_config);

	    PeerInfo lv_pi = lv_zk.get_peer_znode(pv_cluster_id);

	    this.set_peer_znode(lv_pi.get_id(),
				lv_pi.get_quorum(), 
				lv_pi.get_port(),
				lv_pi.get_status(),
				lv_pi.get_attribute_string()
				);
    }
    
    // Communicates with each cluster and gets that cluster's config information
    public void pull_clusters() 
	throws IOException 
    {
        if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:pull_clusters ENTRY");


	    String        lv_my_id   = get_my_id();

	    if (this.is_locked(lv_my_id)) {
		System.out.println("Could not pull info for cluster id: " + lv_my_id
				   + " as it is locked.");

		return;
	    }

	    Map<Integer, PeerInfo> lv_pi_list = list_clusters();
	
	    if (lv_pi_list == null) {
		return;
	    }

	    for (PeerInfo lv_pi : lv_pi_list.values()) {
		if (lv_pi.get_id().equals(lv_my_id)) {
		    continue;
		}
		
		pull_cluster(lv_pi.get_id());
	    }
    }

    /**
     * @param traf_cluster_id
     * @throws IOException
     */
    public boolean is_locked(String pv_traf_cluster_id)
	throws IOException 
    {
    if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:is_locked: cluster ID: " + pv_traf_cluster_id);
	int lv_exists = 0;

	try {
	    lv_exists = ZKUtil.checkExists(m_zkw, m_lock_node);
	}
	catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:is_locked: ZKW Unable to check existence of znode: " 
				  + m_lock_node
				  + " , throwing IOException " + e
				  );
	}

	if (lv_exists != -1) {
	    return true;
	}

	return false;
    }
    

    /**
     * @param traf_cluster_id
     * @throws IOException
     */
    public void lock_db(String pv_traf_cluster_id)
	throws IOException 
    {
	if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:lock_db: cluster ID: " + pv_traf_cluster_id);

	try {
	    ZKUtil.createSetData(m_zkw, m_lock_node, pv_traf_cluster_id.getBytes(CHARSET));
	}
	catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:lock_db: ZKW Unable to create zNode: " 
				  + m_lock_node
				  + " for " + pv_traf_cluster_id
				  + " , throwing IOException " + e
				  );
	}
    }
    
    /**
     * @param traf_cluster_id
     * @throws IOException
     */
    public void unlock_db(String pv_traf_cluster_id)
	throws IOException 
    {
	if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:unlock_db: cluster ID: " + pv_traf_cluster_id);

	try {
	    ZKUtil.deleteNodeFailSilent(m_zkw, m_lock_node);
	}
	catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:unlock_db: ZKW Unable to delete zNode: " 
				  + m_lock_node 
				  + " for " + pv_traf_cluster_id
				  + " , throwing IOException " + e
				  );
	}
    }
    
    /**
     * @param traf_cluster_id
     * @throws IOException
     */
    public void set_my_id(String pv_traf_cluster_id)
	throws IOException 
    {
	if (LOG.isDebugEnabled()) LOG.debug("HBaseDCZK:set_my_id: cluster ID: " + pv_traf_cluster_id);
	String system_traf_cluster_id = System.getenv("TRAF_CLUSTER_ID");
	if(system_traf_cluster_id != null
			&& !pv_traf_cluster_id.equalsIgnoreCase(system_traf_cluster_id)) {
		throw new IOException("HBaseDCZK:set_my_id: Cannot set myid to : "
				+ pv_traf_cluster_id
				+ " , which is not TRAF_CLUSTER_ID.");
	}
	try {
	    ZKUtil.createSetData(m_zkw, m_traf_cluster_id_node, pv_traf_cluster_id.getBytes(CHARSET));
	    PeerInfo lv_pi = get_peer_znode(pv_traf_cluster_id);
	    if (lv_pi == null) {
		set_peer_znode(pv_traf_cluster_id,
			       new String(m_config.get( STRConfig.ZK_QUORUM )),
			       new String(m_config.get( STRConfig.ZK_PORT )),
			       new String("hupsup"),
			       new String("xup")
			       );
	    }
	} 
	catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:set_my_id: ZKW Unable to create zNode: " 
				  + pv_traf_cluster_id
				  + " , throwing IOException " + e
				  );
	}
    }

    /**
     * @return
     */
    public String get_my_id()
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("HBaseDCZK:get_my_id");

	byte[] b_traf_cluster_id = get_znode_data(m_traf_cluster_id_node);
	if (b_traf_cluster_id != null) {
	    String lv_cid = new String(b_traf_cluster_id);
	    if (LOG.isTraceEnabled()) LOG.trace("get_my_id id: " + lv_cid);
	    return lv_cid;
	}

	return null;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.Abortable#abort(java.lang.String, java.lang.Throwable)
     */
    @Override
	public void abort(String arg0, Throwable arg1) {
	// TODO Auto-generated method stub
		
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.Abortable#isAborted()
     */
    @Override
	public boolean isAborted() {
	// TODO Auto-generated method stub
	return false;
    }

    static void usage() {
	
	System.out.println("usage:");
	System.out.println("HBaseDCZK [<command> | <options>...]");
	System.out.println("<command>           : <   -setmyid <id>");
	System.out.println("                    :   | -getmyid");
	System.out.println("                    :   | -set <cluster info>");
	System.out.println("                    :   | -get <id>");
	System.out.println("                    :   | -list");
	System.out.println("                    :   | -delete <id>");
        System.out.println("                    :   | -down <id>");
        System.out.println("                    :   | -shield ");
        System.out.println("                    :   | -cdc ");
	System.out.println("                    :   | -push ");
	System.out.println("                    :   | -pull ");
	System.out.println("                    :   | -lock ");
	System.out.println("                    :   | -unlock >");
	System.out.println("<options>           : [ <peer info> | -h | -v ]");
	System.out.println("<cluster info>      : < <cluster id> [ <quorum info> | <port info> | <status info> ]... >");
	System.out.println("<cluster id>        : -id <id> ");
	System.out.println("<quorum info>       : -quorum <zookeeper quorum>");
	System.out.println("<port info>         : -port <zookeeper client port>");
	System.out.println("<status info>       : <STR status cmd> | <Peer attribute cmd>");
	System.out.println("<STR status cmd>    : -status <STR status>");
	System.out.println("<STR Status >       : <STR Up> | <STR Down>");
	System.out.println("<STR Up >           : sup");
	System.out.println("<STR Down >         : sdn");
	System.out.println("<Peer attribute cmd>: -peer_attribute <Peer attribute>");
	System.out.println("<Peer attribute >   : <No chk for chk&Put>");
	System.out.println("<No chk for chk&Put>: a01");
	System.out.println("<Trafodion Status>  : <Trafodion Up>(tup)| <Trafodion Down> (tdn)");
	System.out.println("<peer info>         : -peer <id>");
	System.out.println("                    :    With this option the command is executed at the specified peer.");
	System.out.println("                    :    (Defaults to the local cluster)");
	System.out.println("<id>                : A number between 1 and 100 (inclusive)");
	System.out.println("-h                  : Help (this output).");
	System.out.println("-v                  : Verbose output. ");

    }

    public static void main(String [] Args) throws Exception {

	boolean lv_retcode = true;

	boolean lv_verbose = false;
	boolean lv_test    = false;
	int     lv_peer_id = 0;

	String  lv_my_id      = new String();
	String  lv_id         = new String();
	String  lv_quorum     = new String();
	String  lv_port       = new String();
	String  lv_status     = new String();
	String  lv_peer_attribute = new String();
        String  lv_cdc         = new String();
        String  lv_snapshot         = new String();

	boolean lv_complete_status = false;

	boolean lv_cmd_set_my_id  = false;
	boolean lv_cmd_get_my_id  = false;
	boolean lv_cmd_set        = false;
	boolean lv_cmd_get        = false;
        boolean lv_cmd_down        = false;
	boolean lv_cmd_delete     = false;
	boolean lv_cmd_lock       = false;
	boolean lv_cmd_unlock     = false;
	boolean lv_cmd_list       = false;
	boolean lv_cmd_push       = false;
	boolean lv_cmd_pull       = false;
        boolean lv_cmd_shield       = false;
        boolean lv_cmd_cdc       = false;
        boolean lv_cmd_snapshot       = false;

        HBaseDCZK.setupConf();

        boolean lv_skip_next = false;
	int lv_index = 0;
	for (String lv_arg : Args) {
	    lv_index++;
            if (lv_skip_next) {
                lv_skip_next = false;
                continue;
            }
	    if (lv_arg.compareTo("-h") == 0) {
		usage();
		System.exit(0);
	    }
	    if (lv_arg.compareTo("-v") == 0) {
		lv_verbose = true;
	    }
	    else if (lv_arg.compareTo("-t") == 0) {
		lv_test = true;
	    }
	    else if (lv_arg.compareTo("-peer") == 0) {
		lv_peer_id = Integer.parseInt(Args[lv_index]); lv_skip_next = true;
		if (lv_verbose) System.out.println("Talk to Peer Cluster, ID: " + lv_peer_id);
	    }
	    else if (lv_arg.compareTo("-id") == 0) {
		lv_id =Args[lv_index]; lv_skip_next = true;
		int lv_integer_id = Integer.parseInt(lv_id);
		if ((lv_integer_id < 1) || (lv_integer_id > 100)) {
		    System.out.println("The id has to be between 1 and 100 (inclusive). Exitting...");
		    usage();
		    System.exit(1);
		}
		if (lv_verbose) System.out.println("Cluster ID: " + lv_id);
	    }
	    else if (lv_arg.compareTo("-status") == 0) {
		lv_status =Args[lv_index]; lv_skip_next = true;
		if (lv_verbose) System.out.println("Status: " + lv_status);
	    }
	    else if (lv_arg.compareTo("-peer_attribute") == 0) {
		lv_peer_attribute =Args[lv_index]; lv_skip_next = true;
		if (lv_verbose) System.out.println("XDC Attribute: " + lv_peer_attribute);
	    }
	    else if (lv_arg.compareTo("-quorum") == 0) {
		lv_quorum =Args[lv_index]; lv_skip_next = true;
		if (lv_verbose) System.out.println("Quorum: " + lv_quorum);
	    }
	    else if (lv_arg.compareTo("-port") == 0) {
		lv_port =Args[lv_index]; lv_skip_next = true;
                boolean lv_nfe = false;
                try {
                    Integer lv_port_number = Integer.parseInt(lv_port);
                    if (lv_port_number < 0) {
                        lv_nfe = true;
                    }
                }
                catch (NumberFormatException nfe) {
                    lv_nfe = true;
                }
                if (lv_nfe) {
                    System.out.println("Incorrect value for a port: " + lv_port + ". Please provide a positive number.");
                    System.exit(1);
                }
		if (lv_verbose) System.out.println("Port: " + lv_port);
	    }
	    else if (lv_arg.compareTo("-setmyid") == 0) {
		if (lv_verbose) System.out.print("Command: setmyid:");
		lv_my_id =Args[lv_index]; lv_skip_next = true;
		if ((lv_my_id != null) && (lv_my_id.length() > 0)) {
		    if (lv_verbose) System.out.println(lv_my_id);
		    int lv_integer_id = Integer.parseInt(lv_my_id);
		    if ((lv_integer_id < 1) || (lv_integer_id > 100)) {
			System.out.println("The id has to be between 1 and 100 (inclusive). Exitting...");
			usage();
			System.exit(1);
		    }
		    lv_cmd_set_my_id = true;
		}
		else {
		    System.out.println("Id not provided. Exitting...");
		    System.exit(1);
		}
	    }
	    else if (lv_arg.compareTo("-getmyid") == 0) {
		if (lv_verbose) System.out.println("Command: getmyid");
		lv_cmd_get_my_id = true;
	    }
	    else if (lv_arg.compareTo("-set") == 0) {
		if (lv_verbose) System.out.println("Command: set");
		lv_cmd_set = true;
	    }
	    else if (lv_arg.compareTo("-get") == 0) {
		if (lv_verbose) System.out.println("Command: get");
		lv_cmd_get = true;
		if (Args.length > lv_index) {
		    if (lv_verbose) System.out.println("Args.length: " + Args.length
						       + " lv_index: " + lv_index);
		    lv_id = Args[lv_index]; lv_skip_next = true;
		}
		if ((lv_id != null) && (lv_id.length() > 0)) {
		    if (lv_verbose) System.out.println("Provided id: " + lv_id);
		}
		else {
		    System.out.println("Id not provided. Getting this cluster's info...");
		}
	    }
            else if (lv_arg.compareTo("-down") == 0) {
                if (lv_verbose) System.out.println("Command: down");
                lv_cmd_down = true;
                if (Args.length > lv_index) {
                    if (lv_verbose) System.out.println("Args.length: " + Args.length
                                                       + " lv_index: " + lv_index);
                    lv_id = Args[lv_index]; lv_skip_next = true;
                }
                if ((lv_id != null) && (lv_id.length() > 0)) {
                    if (lv_verbose) System.out.println("Provided id: " + lv_id);
                }
                else {
                    System.out.println("Id not provided. Getting this cluster's info. Exitting...");
                    System.exit(1);
                }
            }
	    else if (lv_arg.compareTo("-delete") == 0) {
		if (lv_verbose) System.out.println("Command: delete");
		if (Args.length > lv_index) {
		    lv_id =Args[lv_index]; lv_skip_next = true;
		}
		if ((lv_id != null) && (lv_id.length() > 0)) {
		    lv_cmd_delete = true;
		}
		else {
		    System.out.println("Id not provided. Exitting...");
		    System.exit(1);
		}
	    }
            else if (lv_arg.compareTo("-cdc") == 0) {
                if (lv_verbose) System.out.println("Command: cdc");
                lv_cmd_cdc = true;
                if (Args.length > lv_index) {
                    if (lv_verbose) System.out.println("Args.length: " + Args.length
                                                       + " lv_index: " + lv_index);
                    lv_cdc = Args[lv_index]; lv_skip_next = true;
                }
                if ((lv_cdc != null) && (lv_cdc.length() > 0)) {
                    if (lv_verbose) System.out.println("Provided cdc setting: " + lv_cdc);
                }
                else {
                    System.out.println("CDC setting not provided. Exitting...");
                    System.exit(1);
                }
            }
            else if (lv_arg.compareTo("-snapshot") == 0) {
                if (lv_verbose) System.out.println("Command: snapshot");
                lv_cmd_snapshot = true;
                if (Args.length > lv_index) {
                    if (lv_verbose) System.out.println("Args.length: " + Args.length
                                                       + " lv_index: " + lv_index);
                    lv_snapshot = Args[lv_index]; lv_skip_next = true;
                }
                if ((lv_snapshot != null) && (lv_snapshot.length() > 0)) {
                    if (lv_verbose) System.out.println("Provided cdc setting: " + lv_snapshot);
                }
                else {
                    System.out.println("Snapshot setting not provided. Exitting...");
                    System.exit(1);
                }
            }
	    else if (lv_arg.compareTo("-list") == 0) {
		if (lv_verbose) System.out.println("Command: list");
		lv_cmd_list = true;
	    }
	    else if (lv_arg.compareTo("-lock") == 0) {
		if (lv_verbose) System.out.println("Command: lock");
		lv_cmd_lock = true;
	    }
	    else if (lv_arg.compareTo("-unlock") == 0) {
		if (lv_verbose) System.out.println("Command: unlock");
		lv_cmd_unlock = true;
	    }
	    else if (lv_arg.compareTo("-push") == 0) {
		if (lv_verbose) System.out.println("Command: push");
		lv_cmd_push = true;
	    }
	    else if (lv_arg.compareTo("-pull") == 0) {
		if (lv_verbose) System.out.println("Command: pull");
		lv_cmd_pull = true;
	    }
            else if (lv_arg.compareTo("-shield") == 0) {
                if (lv_verbose) System.out.println("Command: shield");
                lv_cmd_shield = true;
            }
	    else if (lv_arg.compareTo("-complete_status") == 0) {
		if (lv_verbose) System.out.println("Option: complete_status");
		lv_complete_status = true;
	    }
            else {
                System.out.println("Incorrect parameter: " + lv_arg + ". Exitting...");
                usage();
                System.exit(1);
            }
	}
	    
	STRConfig lv_STRConfig = null;
	Configuration lv_config = HBaseConfiguration.create();
	if (lv_peer_id > 0) {
	    try {
		System.setProperty("PEERS", String.valueOf(lv_peer_id));
		lv_STRConfig = STRConfig.getInstance(lv_config);
		lv_config = lv_STRConfig.getPeerConfiguration(lv_peer_id, false);
		if (lv_config == null) {
		    System.out.println("Peer ID: " + lv_peer_id + " does not exist OR it has not been configured.");
		    System.exit(1);
		}
	    }
	    catch (ZooKeeperConnectionException zke) {
		System.out.println("Zookeeper Connection Exception trying to get STRConfig instance: " + zke);
		System.exit(1);
	    }
	    catch (IOException ioe) {
		System.out.println("IO Exception trying to get STRConfig instance: " + ioe);
		System.exit(1);
	    }
	}

	try {
	    HBaseDCZK lv_zk = new HBaseDCZK(lv_config);
	    if (lv_id.length() <= 0) {
		lv_id = lv_zk.get_my_id();
		if (lv_verbose) System.out.println("my id: " + lv_id);
	    }
	    if (lv_cmd_set_my_id) {
		lv_zk.set_my_id(lv_my_id);
		if (lv_test) {
		    lv_zk.watch_all();
		    XDCStatusWatcher lv_pw = new XDCStatusWatcher(lv_zk.getZKW());
		    lv_pw.setDCZK(lv_zk);
		    lv_pw.setSTRConfig(lv_STRConfig);
		    lv_zk.register_status_listener(lv_pw);

		    System.out.println("Number of listeners: " + lv_zk.getZKW().getNumberOfListeners());
		    Scanner scanner = new Scanner(System.in);
		    System.out.print("Enter any key when done: ");
		    String userdata = scanner.next();
		    System.out.print("User entered: " + userdata);
		    scanner.close();
		}					
	    }
	    else if (lv_cmd_get_my_id) {
		lv_my_id = lv_zk.get_my_id();
		if (lv_my_id != null) {
		    System.out.println(lv_my_id);
		}
		else {
		    System.out.println("0");
		}
	    }
	    else if (lv_cmd_set) {
		if ((lv_status.length() > 0) &&
		    (lv_status.compareTo("sup") != 0) && 
		    (lv_status.compareTo("sdn") != 0)) {
		    System.out.println("Status string can only be sup or sdn");
		    System.exit(1);
		}
		if ((lv_peer_attribute.length() > 0) &&
		    (lv_peer_attribute.compareTo("a00") != 0) && 
		    (lv_peer_attribute.compareTo("a01") != 0) &&
		    (lv_peer_attribute.compareTo("a02") != 0) &&
                    (lv_peer_attribute.compareTo("a03") != 0) &&
                    (lv_peer_attribute.compareTo("b01") != 0) &&
		    (lv_peer_attribute.compareTo("-a01") != 0)&&
                    (lv_peer_attribute.compareTo("-a02") != 0)&&
                    (lv_peer_attribute.compareTo("-a03") != 0)&&
		    (lv_peer_attribute.compareTo("-b01") != 0)) {
		    System.out.println("Peer Attribute string can be: <a00 | [-]a01 | [-]a02 | [-]a03 |[-]b01>");
		    System.exit(1);
		}
		String lv_peer_updated_attr = lv_peer_attribute;
                PeerInfo lv_pi = lv_zk.get_peer_znode(lv_id);
		if ((lv_peer_attribute.length() > 0)) {
                    //need to get the attribute string from zk and append or overwrite it.
                    if (lv_pi != null) {
                        lv_pi.update_peer_attribute(lv_peer_attribute);
                        lv_peer_updated_attr = lv_pi.get_attribute_string();
                    }
		}
                if (lv_pi == null) {
                    if (lv_quorum.length() == 0) {
                        System.out.println("Please provide the quorum info.");
                        usage();
                        System.exit(1);
                    }
                    if (lv_port.length() == 0) {
                        lv_port = "2181";
                    }
                    if (lv_status.length() == 0) {
                        lv_status = "sup";
                    }
                }                    
		lv_zk.set_peer_znode(lv_id, lv_quorum, lv_port, lv_status, lv_peer_updated_attr);
	    }
	    else if (lv_cmd_get) {
		if (lv_verbose) System.out.println(lv_id);
		PeerInfo lv_pi = lv_zk.get_peer_znode(lv_id);
		if (lv_pi != null) {
		    lv_pi.set_complete_status(lv_complete_status);
		    System.out.println(lv_pi);
		}
	    }
            else if (lv_cmd_shield) {
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                lv_my_id = lv_zk.get_my_id();
                System.out.println(dateFormat.format(date) + ": Command to shield in local: " + lv_my_id + ", starts ...");

                String lv_peer_updated_attr = PeerInfo.PEER_ATTRIBUTE_SHIELD;
                PeerInfo lv_my_pi = lv_zk.get_peer_znode(lv_my_id);
                lv_my_pi.update_peer_attribute(PeerInfo.PEER_ATTRIBUTE_SHIELD);
                lv_peer_updated_attr = lv_my_pi.get_attribute_string();
                lv_zk.set_peer_znode(lv_my_id, null, null, null, lv_peer_updated_attr);

                date = new Date();
                System.out.println(dateFormat.format(date) + ": Command to shield in local: " + lv_my_id + ", completes");
            }
            else if (lv_cmd_down) {
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                System.out.println(dateFormat.format(date) + ": Command to down peer: " + lv_id + ", starts ...");
                lv_my_id = lv_zk.get_my_id();
                if (lv_my_id != null) {
                    System.out.println("My cluster id: "+ lv_my_id);
                }
                else {
                    System.out.println("0");
                }

                if (lv_my_id.equals(lv_id)) {
                    System.out.println("Can not down self. Exitting...");
                    System.exit(1);
                }
                PeerInfo lv_pi = lv_zk.get_peer_znode(lv_id);
                if (lv_pi != null) {
                    lv_pi.set_complete_status(lv_complete_status);
                    System.out.println("Peer xdc config: " + lv_pi);
                }
                else {
                    System.out.println("Peer: " + lv_id + " does not exist. Please provide the correct peer id. Exitting...");
                    System.exit(1);
                }
                if (!lv_pi.isSTRUp()) {
                    System.out.println("Peer: " + lv_id + " exists but is not in the UP state. Exitting...");
                    System.exit(1);
                }
                if (!lv_cmd_snapshot) {
                    System.out.println("xDC Marker: " + lv_snapshot + " has to be set in xdc down command. Exitting...");
                    System.exit(1);
                }

                System.out.println("Peer: " + lv_id + " exists and is in the correct state. Proceeding...");
                System.out.println("Set shield against remote access...");
                String lv_peer_updated_attr = PeerInfo.PEER_ATTRIBUTE_SHIELD;
                PeerInfo lv_my_pi = lv_zk.get_peer_znode(lv_my_id);
                lv_my_pi.update_peer_attribute(PeerInfo.PEER_ATTRIBUTE_SHIELD);
                lv_peer_updated_attr = lv_my_pi.get_attribute_string();
                lv_zk.set_peer_znode(lv_my_id, null, null, null, lv_peer_updated_attr);

                date = new Date();
                System.out.println(dateFormat.format(date) + ": Command to marker: ");

                System.out.println("Set the xDC CDC marker " + lv_snapshot + " ... ");

                Configuration config = HBaseConfiguration.create();

                try {
                     BackupRestoreClient brc = new BackupRestoreClient(config);
                     brc.setVerbose(lv_verbose);
                     brc.xdcMutationCaptureBegin(lv_snapshot);
                     if (lv_verbose) {
                             System.out.println("xdcMutation Marker " + lv_snapshot + " begins ...");
                     }
                    brc.xdcMutationCaptureEnd();
                }catch(Exception e) {
                      System.out.println("xdcMutation Marker failed " + e);
                      System.exit(1);
                }
                System.out.println("xdcMutation Marker succeeds ");

                TimeUnit.SECONDS.sleep(1);

                System.out.println("Set the STR status of peer...");

                date = new Date();
                System.out.println(dateFormat.format(date) + ": Command to set CDC");

                if (lv_cdc.equals("0")) { // skip CDC
                   lv_zk.set_peer_znode(lv_id, null, null, PeerInfo.STR_DOWN, PeerInfo.PEER_ATTRIBUTE_NO_CDC_XDC);
                }
                else { // do CDC
                   lv_zk.set_peer_znode(lv_id, null, null, PeerInfo.STR_DOWN, null);
                } 

                date = new Date();
                System.out.println(dateFormat.format(date) + ": Command to print xdc status");

                System.out.println("Cluster info after setting the STR status of peer " + lv_id);
                Map<Integer, PeerInfo> lv_pi_list = lv_zk.list_clusters();
                if (lv_pi_list != null) {
                    for (PeerInfo lv_pix : lv_pi_list.values()) {
                        lv_pix.set_complete_status(lv_complete_status);
                        System.out.print(lv_pix);
                        if ((lv_my_id != null) && 
                            (lv_pix.get_id() != null) && 
                            (lv_pix.get_id().equals(lv_my_id))) {
                            System.out.print("*");
                            if (lv_zk.is_locked(lv_my_id)) System.out.println(":L"); else System.out.println("");
                        }
                        else {
                            System.out.println("");
                        }
                    }
                }
                date = new Date();
                System.out.println(dateFormat.format(date) + ": Command to down peer: " + lv_id + ", completes");

                // lock clean xdc
                String stringLock = System.getenv("ENABLE_ROW_LEVEL_LOCK");
                stringLock = (stringLock == null) ? "0" : stringLock.trim();
                boolean hasLicense = LicenseHelper.isModuleOpen(LicenseHelper.Modules.ROW_LOCK);
                if (hasLicense && (stringLock.toUpperCase().equals("1") || stringLock.toUpperCase().equals("TRUE"))) {
                    try {
                        String host = "localhost";
                        int port = 8888;
                        try {
                            String stringPort = System.getenv("LM_LISTENER_PORT");
                            if (stringPort != null) {
                                port = Integer.parseInt(stringPort);
                            }
                        } catch(Exception e) {
                            System.out.println("The value of LM_LISTENER_PORT is invalid");
                            System.exit(1);
                        }

                        RSConnection rsConnection = new RSConnection(host, port);
                        LMLockCleanXdcMessage lmLockCleanXdcMessage = (LMLockCleanXdcMessage)RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_CLEAN_XDC);
                        lmLockCleanXdcMessage.setClusterId(Integer.parseInt(lv_id));
                        lmLockCleanXdcMessage.setXdcStatus(XDC_STATUS.XDC_PEER_DOWN);
                        rsConnection.send(lmLockCleanXdcMessage);
                        RSMessage responseMsg = rsConnection.receive();
                        rsConnection.close();
                        if (responseMsg.getMsgType() == RSMessage.MSG_TYP_OK) {
                            System.out.println("lock clean xdc down success");
                        } else if (responseMsg.getMsgType() == RSMessage.MSG_TYP_LOCK_CLEAN_XDC_NO_EXIST) {
                            System.out.println("lock clean xdc not start");
                        } else {
                            System.out.println("lock clean xdc down failed");
                            System.exit(1);
                        }
                    }  catch (IOException e) {
                        System.out.println("LM_LISTENER_PORT ERROR");
                        System.out.println("please export LM_LISTENER_PORT=xxxx in current env");
                        System.exit(1);
                    } catch (Exception e) {
                        System.out.println("lock clean xdc down failed");
                        System.exit(1);
                    }
                }

            }
	    else if (lv_cmd_delete) {
		if (lv_verbose) System.out.println(lv_id);
		lv_retcode = lv_zk.delete_peer_znode(lv_id);
		if (lv_retcode) {
		    System.out.println("Successfully deleted the info about the peer: " + lv_id);
		}		    
	    }
	    else if (lv_cmd_list) {
		Map<Integer, PeerInfo> lv_pi_list = lv_zk.list_clusters();
		if (lv_pi_list != null) {
		    lv_my_id = lv_zk.get_my_id();
		    for (PeerInfo lv_pi : lv_pi_list.values()) {
			lv_pi.set_complete_status(lv_complete_status);
			System.out.print(lv_pi);
			if ((lv_my_id != null) && 
			    (lv_pi.get_id() != null) && 
			    (lv_pi.get_id().equals(lv_my_id))) {
			    System.out.print("*");
			    if (lv_zk.is_locked(lv_my_id)) System.out.println(":L"); else System.out.println("");
			}
			else {
			    System.out.println("");
			}
		    }
		}
	    }
	    else if (lv_cmd_lock) {
		lv_my_id = lv_zk.get_my_id();
		if (lv_my_id != null) {
		    if (lv_verbose) System.out.println(lv_my_id);
		}
		else {
		    if (lv_verbose) System.out.println("0");
		}
		lv_zk.lock_db(lv_my_id);
	    }
	    else if (lv_cmd_unlock) {
		lv_my_id = lv_zk.get_my_id();
		if (lv_my_id != null) {
		    if (lv_verbose) System.out.println(lv_my_id);
		}
		else {
		    if (lv_verbose) System.out.println("0");
		}
		lv_zk.unlock_db(lv_my_id);
	    }
	    else if (lv_cmd_push) {
		lv_zk.push_to_clusters();
	    }
	    else if (lv_cmd_pull) {
		lv_zk.pull_clusters();
	    }
	    else {
		usage();
	    }
	}
	catch (Exception e)
	    {
		System.out.println("exception: " + e);
		System.exit(1);
	    }

	if (! lv_retcode) {
	    System.exit(1);
	}

	System.exit(0);
    }
}
