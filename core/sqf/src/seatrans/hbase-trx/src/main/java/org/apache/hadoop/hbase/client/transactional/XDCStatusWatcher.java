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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.HBaseDCZK;

/**
 * Watcher used to follow the creation and deletion of peer clusters.
 */
public class XDCStatusWatcher extends ZooKeeperListener {

    static final Log LOG = LogFactory.getLog(XDCStatusWatcher.class);

    private HBaseDCZK m_zk;
    private STRConfig m_str;

    /**
     * Construct a ZooKeeper event listener.
     */
    public XDCStatusWatcher(ZooKeeperWatcher pv_watcher) 
    {
	super(pv_watcher);
    }

    public void setDCZK(HBaseDCZK pv_zk) 
    {
	m_zk = pv_zk;
    }

    public void setSTRConfig(STRConfig pv_str)
    {
	m_str = pv_str;
    }

    /**
     * Called when a node has been deleted
     * @param path full path of the deleted node
     */
    public void nodeDeleted(String path) {
	if (LOG.isInfoEnabled()) LOG.info(path + " znode deleted");
	try {
        if (LOG.isInfoEnabled()) LOG.info("nodeDeleted calling watch_all: " + m_zk);
	    if (m_zk != null) m_zk.watch_all();
	}
	catch (Exception e)
	    {
		LOG.error("Exception raised by watch_all: ", e);
	    }
    }

    public PeerInfo getPeerInfo(String path)
    {
	if (LOG.isInfoEnabled()) LOG.info("getPeerInfo: "
					  + path
					  );
	if ( ! path.startsWith(HBaseDCZK.m_clusters_node)) {
	    return null;
	}

	String[] lv_elements = path.split("/");
	int lv_length = lv_elements.length;
	if (LOG.isInfoEnabled()) LOG.info("Number of elements:" + lv_length);

	if (lv_length <= 1) {
	    return null;
	}

	int lv_id_index = lv_length - 2;
	String lv_id = lv_elements[lv_id_index];
	if (LOG.isTraceEnabled()) LOG.trace("Node element[" + lv_id_index + "]: " + lv_id);
	try {
	    PeerInfo lv_pi = m_zk.get_peer_znode(lv_id);
	    lv_pi.set_complete_status(true);
	    if (lv_pi != null) {
		if (LOG.isInfoEnabled()) LOG.info("Peer info: " + lv_pi);
		return lv_pi;
	    }
	}
	catch (Exception e)
	    {
		LOG.error("Exception raised by get_peer_znode: ", e);
	    }

	return null;
    }

    void updateSTRConfig(PeerInfo pv_pi)
    {
	if (LOG.isInfoEnabled()) LOG.info("updateSTRConfig: PeerInfo: "
					  + ((pv_pi != null) ? pv_pi :"")
					  );
	if (m_str == null) {
	    if (LOG.isInfoEnabled()) LOG.info("updateSTRConfig: STRConfig is null");
	    return;
	}

	if (pv_pi == null) {
	    if (LOG.isInfoEnabled()) LOG.info("updateSTRConfig: pv_pi is null");
	    return;
	}

	String lv_id = pv_pi.get_id();
	if (lv_id == null) {
	    return;
	}

	int lv_integer_id = Integer.parseInt(lv_id);
	m_str.setPeerStatus(lv_integer_id,
			    pv_pi.get_status());

    }

    /**
     * Called when an existing node has changed data.
     * @param path full path of the updated node
     */
    public void nodeDataChanged(String path) {

	LOG.info(path + " znode changed");

	try {
        if (LOG.isInfoEnabled()) LOG.info("nodeDataChanged calling watch_all: " + m_zk);
	    if (m_zk != null) m_zk.watch_all();
	}
	catch (Exception e)
	    {
		LOG.error("Exception raised by watch_all: ", e);
	    }

	if (m_zk != null) {
	    PeerInfo lv_pi = getPeerInfo(path);
	    if (lv_pi == null) {
		return;
	    }

	    updateSTRConfig(lv_pi);
		
	}
    }

    /**
     * Called when an existing node has a child node added or removed.
     * @param path full path of the node whose children have changed
     */
    public void nodeChildrenChanged(String path) {

	try {
        if (LOG.isInfoEnabled()) LOG.info("nodeChildrenChanged calling watch_all: " + m_zk);
	    if (m_zk != null) m_zk.watch_all();
	}
	catch (Exception e)
	    {
		LOG.error("Exception raised by watch_all: ", e);
	    }

	if (LOG.isInfoEnabled()) LOG.info(path + " znode children changed");
	
    }
}
