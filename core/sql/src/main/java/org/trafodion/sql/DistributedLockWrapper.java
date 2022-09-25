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
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import org.trafodion.sql.DistributedLock;
import org.trafodion.sql.TrafConfiguration;

public class DistributedLockWrapper 
{
  private static Logger logger = Logger.getLogger(DistributedLockWrapper.class.getName());
  private static Configuration config = null;
  private DistributedLock lock = null;
	
  static {
    String confFile = System.getProperty("trafodion.log4j.configFile");
    System.setProperty("hostName", System.getenv("HOSTNAME"));
    System.setProperty("trafodion.root", System.getenv("TRAF_HOME"));
    if (confFile == null) 
      confFile = System.getenv("TRAF_CONF") + "/log4j.sql.config";
      PropertyConfigurator.configure(confFile);
  }

  public boolean lock(String lockName, long timeout) throws IOException, KeeperException
  {
    if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.lock(" + lockName + ")");

    //remove the slash and dot in the path
    String tmp = lockName.replace("/","_");
    String nln = tmp.replace(".", "_");
    lock = new DistributedLock(nln);
    boolean rc = true;	

    try {
      //get lock first
      if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.lock() -- try to lock");
      if (timeout == 0)
        timeout = 10000 * 60; //timeout 10 min
      if (lock.lock(timeout) == -1) {
        throw new IOException("DistributedLockWrapper.lock() -- lock fail due to timeout");
      } 
    }
    catch (Exception e) {
      if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.lock() -- exception: ", e);
      unlock();
      rc = false;
      throw new Exception(e);
    }
    finally {
      if (rc == true)
        if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.lock() - success");
      return rc;	
    }
  }

  public String longHeldLock(String lockName, String data) throws IOException, KeeperException
  {
    if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.longHeldLock(" + lockName + ")");

    //remove the slash and dot in the path
    String tmp = lockName.replace("/","_");
    String nln = tmp.replace(".", "_");
    lock = new DistributedLock(nln);
    String result = null;
    
    try {
        if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.longHeldLock() -- try to lock");
        result = lock.longHeldLock(data);  
    }
    catch (Exception e) {
        if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.longHeldLock() -- exception: ", e);
        unlock();
        throw new IOException(e);
    }

    return result;
  }


  public void unlock() throws IOException
  {
    try {
      if (lock != null) {

        if (logger.isDebugEnabled()) 
           logger.debug("DistributedLockWrapper.unlock() -- try to unlock");

        lock.unlock();
      }
    }
    catch (Exception e) {
      if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.unlock() -- exception: ", e);
      throw new IOException(e);
    }
    finally {
      lock = null;
    }
  }

  public void clearlock() throws IOException
  {
    try {
      if (lock != null) {

        if (logger.isDebugEnabled()) 
           logger.debug("DistributedLockWrapper.clearlock() -- try to clearlock");

        lock.clearlock();
      }
    }
    catch (Exception e) {
      if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.clearlock() -- exception: ", e);
      throw new IOException(e);
    }
    finally {
      lock = null;
    }
  }

  public boolean observe(String lockName) throws IOException
  {
    if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.observe(" + lockName + ")");

    try {

      //remove the slash and dot in the path
      if ( lock == null ) {
         String tmp = lockName.replace("/","_");
         String nln = tmp.replace(".", "_");
         lock = new DistributedLock(nln);
      }

      if (lock != null) {

        if (logger.isDebugEnabled()) 
           logger.debug("DistributedLockWrapper.observe() called");

        return lock.observe();
      }
    }
    catch (Exception e) {
      if (logger.isDebugEnabled()) 
        logger.debug("DistributedLockWrapper.observe() -- exception: ", e);
      throw new IOException(e);
    }

    return false;
  }

  public void listNodes(String lockName) throws IOException
  {
    if (logger.isDebugEnabled()) logger.debug("DistributedLockWrapper.listNodes(" + lockName + ")");

    try {

      //remove the slash and dot in the path
      if ( lock == null ) {
         String tmp = lockName.replace("/","_");
         String nln = tmp.replace(".", "_");
         lock = new DistributedLock(nln);
      }

      if (lock != null) {

        if (logger.isDebugEnabled()) 
           logger.debug("DistributedLockWrapper.listNodes() called");

        lock.listNodes();
      }
    }
    catch (Exception e) {
      if (logger.isDebugEnabled()) 
        logger.debug("DistributedLockWrapper.listNodes() -- exception: ", e);
      throw new IOException(e);
    }

    return;
  }
}
