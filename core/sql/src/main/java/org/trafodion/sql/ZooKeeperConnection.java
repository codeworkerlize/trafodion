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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

// import zookeeper classes
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

public class ZooKeeperConnection {

   // declare zookeeper instance to access ZooKeeper ensemble
   private ZooKeeper zoo;
   final CountDownLatch connectedSignal = new CountDownLatch(1);

   private static Logger logger = Logger.getLogger(ZooKeeperConnection.class.getName());
   private static Configuration config = null;
   private static String zkServers = null;
   private static ZooKeeper zk = null;
   private static ZooKeeperConnection zkConn = null; 
   private static int zkSessionTimeout;

   static {
      config = TrafConfiguration.create(TrafConfiguration.HBASE_CONF);
      String[] zkquorum= config.getStrings("hbase.zookeeper.quorum");
      String[] zkport = config.getStrings("hbase.zookeeper.property.clientPort");
      zkSessionTimeout = config.getInt("zookeeper.session.timeout", 90000); // Default value is 90 secs
      //logger.info("zookeeper.session.timeout read as " + zkSessionTimeout);
      if (zkquorum != null) {
         zkServers= new String("");
         for (int i = 0; i < zkquorum.length ; i++) {
             zkServers +=zkquorum[i].trim() ;
             zkServers +=":"+zkport[0];
             if (i != zkquorum.length -1)
                zkServers += ",";
         }
      }
   }
 
   // Method to connect zookeeper ensemble.
   public ZooKeeper connect() throws IOException 
   {
      zoo = new ZooKeeper(zkServers,zkSessionTimeout,new Watcher() {
         public void process(WatchedEvent e) {
            if (e.getType() == Event.EventType.None) {
               switch (e.getState()) {
                  case SyncConnected:
                     connectedSignal.countDown();
                     break;
                  case Disconnected:
                     logger.warn("Zookeeper client dropped the current connection and reconnected. Undelivered events might have been lost. SessionTimeout="+zkSessionTimeout);
                     break;
                  case Expired:
                     logger.warn("Zookeeper connection expired, but reconnected. SessionTimeout="+zkSessionTimeout);
                     break;
                  default:
                     break;
               }
            }
         }
      });
      boolean connected = false;
      int retryCnt = 0;
      do {
         try {
             connectedSignal.await();
             connected = true;
         } catch (InterruptedException ie) {
            retryCnt++;
            if (retryCnt > 3) {
               throw new IOException(ie);
            }
         }
      } while (! connected); 
      return zoo;
   }

   // Method to disconnect from zookeeper server
   public void close() 
   {
      try {  
         zoo.close();
         logger.info("Zookeeper connection closed"+zoo);
      } catch (InterruptedException ie) {
          logger.error("ZooKeeper close was interrupted" + zoo,ie);
      } 
   }
   
   public static ZooKeeper getZKInstance() throws IOException
   {
      if (zkConn != null && zk != null && zk.getState() == ZooKeeper.States.CONNECTED)
         return zk;
      int retryCnt = 0;
      synchronized (ZooKeeperConnection.class) {
         while (retryCnt < 3) {
           try {
      	      if (zkConn == null) {
       	         zkConn = new ZooKeeperConnection();
                 zk = zkConn.connect();
              }
              if (zk.getState() == ZooKeeper.States.CONNECTED)
                 return zk;
              else {
                 retryCnt++;
                 zkConn.close();
                 zkConn = null;
              }
           }
           catch (IOException e) {
              if (retryCnt == 3) {
                 throw e;
 	      }
              retryCnt++;
              zkConn.close();
              zkConn = null;
           }
        }
      }
      return zk;
   }
}
