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

package org.trafodion.sql;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.util.Collections;
import java.sql.Timestamp;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.CreateMode;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

public class DistributedLock {
    static Logger logger = Logger.getLogger(DistributedLock.class.getName());
    private String lockName;
    private long wait_timeout = 90000; //ms
    private int queuesize = 0;
    private final String lockBasePath="/trafodion/traflock";
    private static boolean basePathCreated = false;
    private String lockPath;

    public DistributedLock() 
    {}

    public DistributedLock(String lockName) throws KeeperException, IOException
    {
        this.lockName = lockName;
    }
    public String lockName()
    {
        return lockName;
    }
  
    public ZooKeeper init() throws KeeperException, IOException
    {
       int retryCnt = 0;
       ZooKeeper zk = ZooKeeperConnection.getZKInstance();
       if (basePathCreated)
          return zk;
       do {
          try {
             if (zk.exists(lockBasePath, true) == null ) {
                byte[] bb = "trafDLock".getBytes();
                zk.create(lockBasePath,bb,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
             }
             basePathCreated = true;
          }
          catch(KeeperException.NodeExistsException e) {
              //this is normal
             basePathCreated = true;
          } catch (InterruptedException ie) {
             retryCnt++;
             if (retryCnt > 3) {
                throw new IOException(ie);
             }
          }
       } while (!basePathCreated);
       return zk;
    }

    public void getAllNodesForLockName(List<String> own_nodes, boolean addWatch) throws IOException
    {
      try {
        ZooKeeper zk = init();
        //select lock node with the same prefix as lockName, and add it into own_nodes;
        own_nodes.clear();
        List<String> nodes = zk.getChildren(lockBasePath, addWatch);
        for (String node_: nodes) {
            String node_name = node_.split("\\.")[0];
	    if (node_name.equals(lockName))
                {
                    own_nodes.add(node_);
                    if (logger.isDebugEnabled()) logger.debug("Adding distributed lock node :"+node_); 
                }
	}
	Collections.sort(own_nodes);

	} catch (KeeperException e) {
            throw new IOException (e);
        } catch (InterruptedException e) {
            throw new IOException (e);
        }
    }

    public boolean queue_head(List<String> own_nodes) throws IOException
    {
      try {
        getAllNodesForLockName(own_nodes, true);

	if (lockPath.endsWith(own_nodes.get(0)))
	    return true;
	else 
	    return false;
       } catch (IOException e) {
            throw e;
       }
    }

    public boolean observe() throws IOException
    {
      try {
        List<String> own_nodes = new ArrayList<String>();
        getAllNodesForLockName(own_nodes, false);

        //System.out.println("In observe(): sorted own_nodes= "+ own_nodes);

	return (own_nodes.size() > 0);

       } catch (IOException e) {
         throw e;
       }
    }

    public int lock(long timeout) throws IOException, KeeperException
    {
        if (lockName == null)
           throw new IOException("lockname is not given");
        ZooKeeper zk = init();
        try {
            // lockPath will be different than (lockBasePath + "/" + lockName) becuase of the sequence number ZooKeeper appends
            if (logger.isDebugEnabled()) logger.debug("create znode");
            if (timeout >= 0)
               wait_timeout = timeout;

            lockPath = zk.create(lockBasePath + "/" + lockName + ".", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            if (logger.isDebugEnabled())logger.debug("Getting distributed lock, lockPath :"+lockPath);  
            List<String> own_nodes = new ArrayList<String>();

            if (queue_head(own_nodes)) {
                return 0;
            }
			
            //find the node previous to us
            String current_node = lockPath.substring(lockPath.lastIndexOf("/") + 1);
            String pre_node = own_nodes.get(Collections.binarySearch(own_nodes, current_node) - 1);

            final Object lock = new Object();
            synchronized (lock) {
                while (true) {

                    Stat stat = zk.exists(lockBasePath + "/" + pre_node, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                            synchronized (lock) {
                                lock.notifyAll();
                            }
                        }
                    });
                    if (null == stat) {
                       if (queue_head(own_nodes))
                          return 0;
                        else
                          pre_node = own_nodes.get(Collections.binarySearch(own_nodes, current_node) - 1);
                    } else {
                         long start_time = System.currentTimeMillis();
                         long elapsed = wait_timeout;
                         while (true) {
                            try {
                               lock.wait(elapsed);
                               break;
                            } catch (InterruptedException e) {
                               elapsed = System.currentTimeMillis() - start_time;
                               if (wait_timeout !=0 && elapsed >= wait_timeout) 
                                  break;
                            }
                         }
                         elapsed = System.currentTimeMillis() - start_time;
                         if (wait_timeout !=0 && elapsed >= wait_timeout) {
                             logger.error("lock timeout, lockName is " + lockName +", queuesize is " + queuesize);
                             this.unlock();
                             return -1;
                         } 
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new IOException (e);
        }
    }

    // longHeldLock
    //
    // Try to obtain a lock. If the lock already exists, return the text data
    // stored with that lock.
    //
    // Use this method for applications that require a long-running lock (e.g.
    // hours), and where we don't care about indefinite postponement. An
    // example is UPDATE STATISTICS, where we use this kind of lock to avoid
    // two concurrent UPDATE STATISTICS commands against the same table.
    //
    // For applications requiring short-term locks and/or avoiding indefinite
    // postponement, use the lock method instead.
    //
    // Note: We do two operations from a Zookeeper perspective: 1. Create node,
    // and 2. getData (if the node already exists). We don't do these atomically,
    // so it is possible a node exists at create node time but is gone by getData
    // time. So we retry in that case. But just once, because our assumption is
    // that these locks are typically held for longer periods than this method
    // call.
    //
    // On return, we return null if we successfully obtained the
    // lock, or the data from the lock if the lock is already held. Or we
    // throw an exception for unusual conditions.

    public String longHeldLock(String data) throws IOException, KeeperException 
    {
        if (logger.isDebugEnabled()) logger.debug("DistributedLock.longHeldLock create znode");

        String lockKey = lockBasePath + "/" + lockName;
        ZooKeeper zk = init();
        int retries = 0;
        while (retries < 2) {
            try {            
                // use EPHEMERAL instead of EPHEMERAL_SEQUENTIAL because we don't care about indefinite
                // postponement (in contrast, the lock method uses EPHEMERIAL_SEQUENTIAL, which creates
                // a set of znodes with a sequence number appended to the node key, effectively creating
                // a queue)
                lockPath = zk.create(lockKey, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                return null;  // success!
            }
            catch (KeeperException e) {
                if (e.code() == KeeperException.Code.NODEEXISTS) {
                    Stat dummy = new Stat();
                    try {
                        byte[] storedData = zk.getData(lockKey,null,dummy);
                        String returnData = new String(storedData);
                        return returnData;  // node exists, return its data
                    }
                    catch (KeeperException f) {
                        if (f.code() == KeeperException.Code.NONODE) {
                            // the node existed when we tried to create it, but it doesn't exist
                            // now, so let's retry
                            retries++;
                        } else 
                            throw new IOException(f);
                    } catch (InterruptedException f) {
                        throw new IOException(f);
                    }                
                } else
                    throw new IOException(e);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
          
        // if we got here, we did the create-getData sequence twice and still didn't
        // get any data, which means nodes are being created and destroyed quickly, contrary
        // to assumption. So we will throw an exception

        throw new IOException("Failed to get a lock due to rapid node creation and destruction."); 

    }

    public void unlock() throws IOException, KeeperException 
    {
       ZooKeeper zk = init();
       int retryCnt = 0;
       
       while (lockPath != null) {
          try {
             zk.delete(lockPath, -1);
             if (logger.isDebugEnabled())logger.debug("Deleting distributed lock, lockPath :"+lockPath);
             lockPath = null;
          } catch (InterruptedException e) {
              retryCnt++;
              if (retryCnt > 3) {
                 logger.error("unlock is interrupted");
                 throw new IOException(e);
              }
          }
       }
    }

    public void clearlock() throws IOException, KeeperException 
    {
       ZooKeeper zk = init();
       int retryCnt = 0;

       List<String> own_nodes = new ArrayList<String>();
       getAllNodesForLockName(own_nodes, false);
       Iterator<String> it = own_nodes.iterator();
       
       while (it.hasNext()) {
          try {
             String s = lockBasePath + "/" + it.next();
             zk.delete(s, -1);
             if (logger.isDebugEnabled())logger.debug("Clearing distributed lock, lockPath :"+s);
          } catch (InterruptedException e) {
              retryCnt++;
              if (retryCnt > 3) {
                 logger.error("clearlock is interrupted");
                 throw new IOException(e);
              }
          }
       }
    }

    public void listNodes() throws IOException, KeeperException 
    {
      try {
        List<String> own_nodes = new ArrayList<String>();
        getAllNodesForLockName(own_nodes, false);

        System.out.print("lockBasePath=" + lockBasePath);
        System.out.println(", lockName=" + lockName);
        for (String node: own_nodes) {
           System.out.println("   node=" + node);
        }
	return;

       } catch (IOException e) {
         throw e;
       }
    }
}
