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
/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trafodion.dcs.master;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.util.Bytes;

public class MasterLeaderElection {
    private static final Logger LOG = LoggerFactory.getLogger(MasterLeaderElection.class);

    private DcsMaster master = null;
    private String myZnode;
    private String parentDcsZnode;
    private FloatingIp floatingIp;
    private boolean bindfloatingIP = false;

    public MasterLeaderElection(DcsMaster master) throws IOException,
            InterruptedException, KeeperException {
        this.master = master;
        this.parentDcsZnode = master.getZKParentDcsZnode();

        try {
            this.floatingIp = FloatingIp.getInstance(master);
        } catch (Exception e) {
            LOG.error("Error creating class FloatingIp : {}.", e.getMessage(), e);
            floatingIp = null;
        }

        byte[] data = Bytes.toBytes(master.getServerName() + ":"
                + master.getServerIp() + ":" + master.getInstance());
        setNodePath(master.getZkClient().create(
                parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER_LEADER
                        + "/" + "leader-n_", data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL));
        elect();
    }

    private void setNodePath(String myZnode) {
        this.myZnode = myZnode;
    }

    private synchronized void elect() throws IOException, InterruptedException,
            KeeperException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Leader Election: Do Elect");
        }
        master.setIsLeader();

        if (bindfloatingIP) {
            return;
        }

        List<String> znodeList = master.getZkClient().getChildren(
                parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER_LEADER,
                new ElectionNodeWatcher());

        Collections.sort(znodeList);
        String bindfloatingIPZnode = parentDcsZnode
                + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER_LEADER + "/"
                + znodeList.get(0);

        if (myZnode.equals(bindfloatingIPZnode)) {
            bindfloatingIP = true;
        } else {
            bindfloatingIP = false;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Bind FloatingIP znode <{}>, my znode <{}>, is bindfloatingIP <{}>, znodeList <{}>",
                    bindfloatingIPZnode, myZnode, bindfloatingIP, znodeList);
        }

        //Bind floating IP
        if (bindfloatingIP) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Leader Election: I'm the Leader <{}>.", myZnode);
            }
            if (floatingIp != null) {
                try {
                    floatingIp.runScript();
                } catch (Exception e) {
                    LOG.error("Error invoking FloatingIp <{}>.", e.getMessage(), e);
                }
            }
        } else {
            if (LOG.isInfoEnabled()) {
                LOG.info("Leader Election: I do not need bind floating IP <{}>.", myZnode);
            }
        }
    }

    private class ElectionNodeWatcher implements Watcher {
        // watches /LEADER node's children changes.
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Node changed <{}>, electing a master bind floating IP.",
                            event.getPath());
                }
                try {
                    elect();
                } catch (IOException | InterruptedException | KeeperException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    public boolean isFollower() {
        return false;
    }

}
