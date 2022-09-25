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

package org.apache.hadoop.hbase.pit;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.transactional.TransactionRegionLocation;
import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Zookeeper client-side communication class for the PIT
 */
public class HBasePitZK implements Abortable{
    public static final String rootNode = "/trafodion/recovery";
    private final String zNodeSnapshotMetaLockPath = "/trafodion/recovery/LOCK";
    static final Log LOG = LogFactory.getLog(HBasePitZK.class);

    static Object zkLock = new Object();        
    static ZooKeeperWatcher zooKeeper = null;
    private Configuration conf;
    private static final int maxRetryTimes = 3;

    /**
     * @param conf
     * @throws IOException
     */
    public HBasePitZK(final Configuration conf) throws IOException {
       this.conf = conf;
       if (LOG.isTraceEnabled()) LOG.trace("HBasePitZK(conf) -- ENTRY");
       synchronized (zkLock) {
          try {
             if (this.zooKeeper == null){
                this.zooKeeper = new ZooKeeperWatcher(conf, "PIT ZK Snapshot Meta Lock", this, true);
             }
             if (LOG.isInfoEnabled()) LOG.info("HBasePitZK zookeeper watcher created");
          } catch (IOException e) {
             LOG.error("pitZK: new ZK watcher exception ", e);
             throw new IOException(e);
          }
       }
    }

    private void setZooKeeper() throws IOException {
        try {
            zooKeeper.reconnectAfterExpiration();
            if (LOG.isInfoEnabled()) LOG.info("HBasePitZK zookeeper retry watcher reconnectAfterExpiration");
        } catch (Exception e) {
            LOG.error("pitZK: new ZK watcher exception ", e);
            throw new IOException(e);
        }

    }

    /**
     ** @param dataString zNodeKey = basePath + "/PIT;
     ** #throws IOException
     **/
    public void deleteSnapshotMetaLockNode() throws IOException {
        String zNodeSnapshotMetaLockPathKey = zNodeSnapshotMetaLockPath + "/SnapshotMetaLock";
        int retryTimes = 0;
        do {
           try {
              if (ZKUtil.checkExists(zooKeeper, zNodeSnapshotMetaLockPathKey) == -1) {
                 if (LOG.isTraceEnabled()) LOG.trace("deleteSnapshotMetaLockNode Trafodion znode path cleared (no deletion) " + zNodeSnapshotMetaLockPathKey);
                 return;
              }
              if (LOG.isTraceEnabled()) LOG.trace("deleteSnapshotMetaLockNode Trafodion znode path cleared (deletion) " + zNodeSnapshotMetaLockPathKey);
              ZKUtil.deleteNode(zooKeeper, zNodeSnapshotMetaLockPathKey);
              if (LOG.isInfoEnabled()) LOG.info("HBasePitZK snapshot meta zNode deleted");
              return;
           }  catch (KeeperException.SessionExpiredException e){
               //if SessionExpiredException, recreate zooKeeper
               setZooKeeper();
           }catch (KeeperException e) {
              LOG.error("pitZK:deleteSnapshotMetaLockNode znode exception ", e);
              throw new IOException("deleteSnapshotMetaLockNode: ZKW Unable to delete SnapshotMetaLock  zNode: " + zNodeSnapshotMetaLockPathKey +"  , throwing IOException ", e);
           }
        } while (retryTimes++ < maxRetryTimes);
    }

    /**
     ** @param dataString zNodeKey = basePath + "/PIT;
     ** #throws IOException
     **/
    public void cleanupSnapshotMetaLockNode() throws IOException {
        String zNodeSnapshotMetaLockPathKey = zNodeSnapshotMetaLockPath + "/SnapshotMetaLock";
        int retryTimes = 0;
        do {
           try {
              if (ZKUtil.checkExists(zooKeeper, zNodeSnapshotMetaLockPathKey) == -1) {
                 if (LOG.isTraceEnabled()) LOG.trace("cleanupSnapshotMetaLockNode Trafodion znode path cleared (no deletion) " + zNodeSnapshotMetaLockPathKey);
                 return;
              }
              if (LOG.isTraceEnabled()) LOG.trace("cleanupSnapshotMetaLockNode Trafodion znode path cleared (deletion) " + zNodeSnapshotMetaLockPathKey);
              ZKUtil.deleteNode(zooKeeper, zNodeSnapshotMetaLockPathKey);
              if (LOG.isInfoEnabled()) LOG.info("HBasePitZK snapshot meta zNode removeded");
              return;
           } catch (KeeperException.SessionExpiredException e){
               setZooKeeper();
           } catch (Exception e) {
              LOG.error("cleanupSnapshotMetaLockNode ignoring znode exception ", e);
           }
        } while (retryTimes++ < maxRetryTimes);
    }

    /**
     ** @param data
     ** #throws IOException
     **/
    public void createSnapshotMetaLockNode(byte [] data) throws IOException {
        String zNodeSnapshotMetaLockPathKey = zNodeSnapshotMetaLockPath + "/SnapshotMetaLock";
        int retryTimes = 0;
        do {
           try {
              if (ZKUtil.checkExists(zooKeeper, zNodeSnapshotMetaLockPathKey) == -1) {
                 if (LOG.isTraceEnabled()) LOG.trace("Trafodion SnapshotMetaLock does not exist " + zNodeSnapshotMetaLockPathKey);
                 ZKUtil.createWithParents(zooKeeper, zNodeSnapshotMetaLockPath);
              }
              ZKUtil.createAndWatch(zooKeeper, zNodeSnapshotMetaLockPathKey, data);
              if (LOG.isInfoEnabled()) LOG.info("HBasePitZK snapshot meta zNode created");
              return;
           } catch (KeeperException.SessionExpiredException e){
               setZooKeeper();
           } catch (KeeperException e) {
              LOG.error("pitZK: create snapshot meta lock znode exception ", e);
              throw new IOException("HBaseTmZK:SnapshotMetaLock: ZKW Unable to create SnapshotMetaLock zNode: " + zNodeSnapshotMetaLockPathKey + "  , throwing IOException ", e);
           }
        } while (retryTimes++ < maxRetryTimes);
    }

    /**
     * @param znode
     * @return
     * @throws KeeperException
     */
    public boolean isSnapshotMetaLocked() throws IOException {
        String zNodeSnapshotMetaLockPathKey = zNodeSnapshotMetaLockPath + "/SnapshotMetaLock";
        int retryTimes = 0;
        do {
            try {
                if (ZKUtil.checkExists(zooKeeper, zNodeSnapshotMetaLockPathKey) == -1) {
                    if (LOG.isTraceEnabled()) LOG.trace("isSnapshotMetaLocked Trafodion znode path does not exist " + zNodeSnapshotMetaLockPathKey);
                    return false;
                }
                if (LOG.isTraceEnabled()) LOG.trace("isSnapshotMetaLocked Trafodion znode path exists " + zNodeSnapshotMetaLockPathKey);
                return true;
            } catch (KeeperException.SessionExpiredException e) {
                setZooKeeper();
            } catch (KeeperException e) {
                LOG.error("pitZK: check snapshot meta lock znode exception ", e);
                throw new IOException("HBasePitZK:isSnapshotMetaLocked: ZKW Unable to check SnapshotMetaLock zNode: " + zNodeSnapshotMetaLockPathKey + "  , throwing IOException ", e);
            }
        } while (retryTimes++ < maxRetryTimes);
        return true;
    }
        

    /**
     * @param znode
     * @return
     * @throws KeeperException
     */
    private byte [] checkData (String znode) throws KeeperException, InterruptedException {
       return ZKUtil.getData(zooKeeper, znode);
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
       System.out.println("HBasePitZK [<options>...]");
       System.out.println("<options>         : [ <peer info> | -h | -v ]");
       System.out.println("<peer info>       : -peer <id>");
       System.out.println("                  :    With this option the command is executed at the specified peer.");
       System.out.println("                  :    (Defaults to the local cluster)");
       System.out.println("-h                : Help (this output).");
       System.out.println("-v                : Verbose output. ");

    }    

    public static void main(String [] Args) throws Exception {

       boolean lv_retcode = true;

       boolean lv_verbose = false;
       boolean lv_test    = false;
       int     lv_peer_id = 0;

       String  lv_my_id  = new String();

       int lv_index = 0;
       for (String lv_arg : Args) {
          lv_index++;
          if (lv_arg.compareTo("-h") == 0) {
             usage();
             System.exit(0);
          }
          if (lv_arg.compareTo("-v") == 0) {
             lv_verbose = true;
          }
          if (lv_arg.compareTo("-t") == 0) {
             lv_test = true;
          }
       }
            
       STRConfig lv_STRConfig = null;
       Configuration lv_config = HBaseConfiguration.create();
       if (lv_peer_id > 0) {
          try {
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

       HBasePitZK lv_tz = new HBasePitZK(lv_config);

       System.exit(0);
    }
}
 
