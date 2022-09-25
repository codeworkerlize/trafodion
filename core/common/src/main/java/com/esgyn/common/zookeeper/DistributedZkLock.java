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

// same code as class org.trafodion.sql.DistributedLock to avoid
// a dependency on the SQL jar
// See also https://dzone.com/articles/distributed-lock-using

package com.esgyn.common.zookeeper;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Collections;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;
import org.apache.log4j.Logger;

public class DistributedZkLock {
    static Logger logger = Logger.getLogger(DistributedZkLock.class.getName());
    private final ZooKeeper zk;
    private final String lockBasePath;
    private final String lockName;
    private String lockPath;
    private long wait_timeout = 90000; //ms
    private int queuesize = 0;

    public DistributedZkLock(ZooKeeper zk, String lockBasePath, String lockName, boolean createIfNeeded)
            throws IOException {
        this.zk = zk;
        this.lockBasePath = lockBasePath;
        this.lockName = lockName;

        if (createIfNeeded) {
            try {
                if (zk.exists(lockBasePath, false) == null)
                    zk.create(lockBasePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    public boolean queue_head(List<String> nodes_for_this_lock) throws IOException{
    try {
        //select lock node with the same prefix as lockName, and add it into own_nodes;
        nodes_for_this_lock.clear();
        List<String> nodes = zk.getChildren(lockBasePath, true);
        for (String node_: nodes) {
            String node_name = node_.split("\\.")[0];
            if (node_name.equals(lockName))
                nodes_for_this_lock.add(node_);
        }
        Collections.sort(nodes_for_this_lock);
        queuesize = nodes_for_this_lock.size();
        if (lockPath.endsWith(nodes_for_this_lock.get(0)))
            return true;
        else 
            return false;
        } catch (KeeperException e) {
            throw new IOException (e);
        } catch (InterruptedException e) {
            throw new IOException (e);
        }
    }

    public int lock(long timeout) throws IOException {
        try {
            // lockPath will be different than (lockBasePath + "/" + lockName) becuase of the sequence number ZooKeeper appends
            if (logger.isDebugEnabled()) logger.debug("create znode");
            if (timeout >= 0)
              wait_timeout = timeout;

            lockPath = zk.create(lockBasePath + "/" + lockName + ".", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            List<String> nodes_for_this_lock = new ArrayList<String>();

            if (queue_head(nodes_for_this_lock))
                return 0;
                        
            //find the node previous to us
            String current_node = lockPath.substring(lockPath.lastIndexOf("/") + 1);
            String pre_node = nodes_for_this_lock.get(Collections.binarySearch(nodes_for_this_lock, current_node) - 1);
            final Object lock = new Object();
            synchronized(lock) {
                while(true) {
                    Stat stat = zk.exists(lockBasePath + "/" + pre_node, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                            synchronized (lock) {
                                lock.notifyAll();
                            }
                        }
                    });
                    if (null == stat) {
                       if (queue_head(nodes_for_this_lock))
                          return 0;
                        else
                          pre_node = nodes_for_this_lock.get(Collections.binarySearch(nodes_for_this_lock, current_node) - 1);
                    } else {
                         long start_time = System.currentTimeMillis();
                         lock.wait(wait_timeout);
                         long elapsed = System.currentTimeMillis() - start_time;
                         if(wait_timeout !=0 && elapsed >= wait_timeout) {
                             logger.error("lock timeout, queuesize is " + queuesize);
                             this.unlock();
                             return -1;
                         } 
                    }
                }
            }
        } catch (KeeperException e) {
            throw new IOException (e);
        } catch (InterruptedException e) {
            throw new IOException (e);
        }
    }

    public void unlock() throws IOException {
        try {
            if(lockPath != null)
              zk.delete(lockPath, -1);
            lockPath = null;
        } catch (KeeperException e) {
            throw new IOException (e);
        } catch (InterruptedException e) {
            throw new IOException (e);
        }
    }

    public void deleteBaseDir(int timeout) throws IOException {
        long start_time = System.currentTimeMillis();
        long elapsed_time = 0;
        boolean quiesced = false;
        try {
            while (!quiesced) {
                try {
                    zk.delete(lockBasePath, -1);
                    quiesced = true;
                }
                catch (KeeperException e) {
                    if (e.code() == KeeperException.Code.NOTEMPTY) {
                        // somebody is still using the directory, use a
                        // watcher to wait for it to empty
                        final Object lock = new Object();
                        synchronized(lock) {
                            while(true) {
                                try {
                                    List<String> children = zk.getChildren(lockBasePath, new Watcher() {
                                        @Override
                                        public void process(WatchedEvent event) {
                                            synchronized (lock) {
                                                lock.notifyAll();
                                            }
                                        }
                                    });
                                    if (children.size() > 0) {
                                        elapsed_time = System.currentTimeMillis() - start_time;
                                        long remaining_timeout = timeout;

                                        if (remaining_timeout > 0) {
                                            remaining_timeout -= elapsed_time;
                                            if (remaining_timeout <= 0) {
                                                String errMsg = "Timed out trying to remove lock base dir " +
                                                                lockBasePath + ", there are " +
                                                                Integer.toString(children.size()) +
                                                                " nodes remaining under that path";
                                                logger.error(errMsg);
                                                throw new IOException(errMsg);
                                            }
                                        }
                                        lock.wait(remaining_timeout);
                                    }
                                } catch (KeeperException e1) {
                                    throw new IOException(e1);
                                }
                            }
                        }
                    } // child nodes exist
                    else if (e.code() == KeeperException.Code.NONODE)
                        // somebody else already deleted the directory
                        quiesced = true;
                    else
                        throw new IOException(e);
                }
            } // while
        }
        catch (InterruptedException e) {
            throw new IOException (e);
        }
    }
}

