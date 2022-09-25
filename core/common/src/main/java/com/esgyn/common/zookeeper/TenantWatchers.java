// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common.zookeeper;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.common.CGroupHelper;
import com.esgyn.common.TenantHelper;


public class TenantWatchers {
    private static final Logger _LOG = LoggerFactory.getLogger(TenantWatchers.class);
    private ZkClient zkc = null;
    
    public TenantWatchers() {
        try {
            zkc = new ZkClient();
            zkc.connect();
            initZkTenants();
        } catch (Exception e) {
            _LOG.error("Failed to create zookeeper tenant znode watchers", e);
        }
    }
    public class TenantWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();

                    List<String> children = zkc.getChildren(znode, new TenantWatcher());
                    if (!children.isEmpty()) {
                        for (String child : children) {
                            stat = zkc.exists(znode + "/" + child, false);
                            if (stat != null) {
                                data = zkc.getData(znode + "/" + child, new TenantDataWatcher(), stat);

                                if (_LOG.isDebugEnabled()) {
                                    _LOG.debug("TenantWatcher Add Tenant NodeChildrenChanged [" + child + "]");
                                    _LOG.debug(new String(data));
                                }
                            }
                        }
                    }
                    try {
                        CGroupHelper.refresh();
                    } catch (Exception e) {
                       _LOG.error("Failed to refresh tenant allocation after tenant update");
                    }                     
                } catch (Exception e) {
                    _LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    class TenantDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.NodeDataChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    data = zkc.getData(znode, new TenantDataWatcher(), stat);

                    if (_LOG.isDebugEnabled()) {
                        _LOG.debug("TenantDataWatcher EventType.NodeDataChanged [" + child + "]");
                        _LOG.debug(new String(data));
                    }
                    try {
                        CGroupHelper.refresh();
                    } catch (Exception e) {
                       _LOG.error("Failed to refresh tenant allocation after tenant update");
                    }                    
                } catch (Exception e) {
                    _LOG.error(e.getMessage(), e);
                }
            } /*else if (event.getType() == Event.EventType.NodeDeleted) {
                String znode = event.getPath();
                String child = znode.substring(znode.lastIndexOf('/') + 1);
                if (_LOG.isDebugEnabled())
                    _LOG.debug("TenantDataWatcher NodeDeleted [" + child + "]");
                try {
                    CGroupHelper.refresh();
                } catch (Exception e) {
                   _LOG.error("Failed to refresh tenant allocation after tenant delete");
                }
            }*/
        }
    }

    private void initZkTenants() throws Exception {
        String tenantsRootZnode = TenantHelper.TENANT_ZNODE_PARENT;
        if (_LOG.isDebugEnabled())
            _LOG.debug("initZkTenants " + tenantsRootZnode);
       
        Stat stat = null;
        byte[] data = null;
        List<String> children = null;

        children = zkc.getChildren(tenantsRootZnode, new TenantWatcher());
        if (!children.isEmpty()) {
            for (String child : children) {
                if (_LOG.isDebugEnabled())
                    _LOG.debug("child [" + child + "]");
                stat = zkc.exists(tenantsRootZnode + "/" + child, false);
                if (stat != null) {
                    data = zkc.getData(tenantsRootZnode + "/" + child, new TenantDataWatcher(), stat);
                    _LOG.debug(new String(data));
                }
            }
        }
    }

}
