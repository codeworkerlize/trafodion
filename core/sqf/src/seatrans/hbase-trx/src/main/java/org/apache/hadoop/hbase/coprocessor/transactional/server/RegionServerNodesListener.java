package org.apache.hadoop.hbase.coprocessor.transactional.server;

import java.util.List;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class RegionServerNodesListener extends ZooKeeperListener {
    private String znodeRS;
    private static Logger LOG = Logger.getLogger(RegionServerNodesListener.class);

    public RegionServerNodesListener(ZooKeeperWatcher watcher) {
        super(watcher);
        znodeRS = watcher.getConfiguration().get("zookeeper.znode.parent") + "/rs";
        watcher.registerListener(this);
        try {
            List<String> rsNames = ZKUtil.listChildrenAndWatchThem(watcher, znodeRS);
            LOG.info("RegionServerNodesListener: " + rsNames);
        } catch (KeeperException e) {
            LOG.error("RegionServerNodesListener", e);
        }
    }

    public void nodeChildrenChanged(String path) {
        if (path.equals(this.znodeRS)) {
            try {
                List<String> rsNames = ZKUtil.listChildrenAndWatchThem(watcher, znodeRS);
                LOG.info("RegionServerNodesListener.nodeChildrenChanged: " + rsNames);
            } catch (KeeperException e) {
                LOG.error("RegionServerNodesListener.nodeChildrenChanged", e);
            }
            RSServer rsServer = RSServer.getInstance();
            if (rsServer != null) {
                rsServer.listAllRegionServers(true);
            }
        }
    }
}
