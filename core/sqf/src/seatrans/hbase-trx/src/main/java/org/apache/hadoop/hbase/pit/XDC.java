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

import org.apache.log4j.PropertyConfigurator;

import java.util.ArrayList;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.transactional.message.RSMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockCleanXdcMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSConnection;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.XDC_STATUS;
import java.io.IOException;

public class XDC
{
  private native int lockTMReq();
  private native int unlockTMReq();
  private native int sdnTranReq();
  static Configuration config;

  static {
    System.loadLibrary("stmlib");
  }
  
   void setupLog4j() {
      System.setProperty("trafodion.logdir", System.getenv("TRAF_LOG"));
      System.setProperty("hostName", System.getenv("HOSTNAME"));
      String confFile = System.getenv("TRAF_CONF")
           + "/log4j.xdc.config";
      PropertyConfigurator.configure(confFile);
      System.out.println("In seupLog4j" + ", confFile: " + confFile);
  }

  public XDC(){
     setupLog4j();
  }
  
  
  //NOTE: Export the following in your shell before you use this main program.
  //export LD_PRELOAD=$JAVA_HOME/jre/lib/amd64/libjsig.so:$TRAF_HOME/export/lib64d/libseabasesig.so
  static public void main(String[] args) {
    
    //if (LOG.isInfoEnabled()) LOG.info("XDC Client Utililty.");
    System.out.println("XDC Client Utililty."
		       );
    XDC xdc = new XDC();
    int rc = 0;
    boolean lv_verbose = false;
      
    for ( int i = 0; i < args.length; i++ )
    {
      if (args[i].compareTo("-v") == 0) {
	  lv_verbose = true;
      }
      if(args[i].equals("l"))
      {
        rc = xdc.lockTMReq();
        if(rc != 0) {
          //LOG.error("lockTMReq failed " + rc);
	      System.out.println("lockTMReq failed " + rc);
	      System.exit(1);
	    }
        else{
          //if (LOG.isInfoEnabled()) LOG.info("lockTMReq Success");
          System.out.println("lockTMReq Success");
        }
      }
      else if(args[i].equals("u"))
      {
        rc = xdc.unlockTMReq();
        if(rc != 0) {
          //LOG.error("unlockTMReq failed " + rc);
	      System.out.println("unlockTMReq failed " + rc);
	      System.exit(1);
	    }
        else{
          //if (LOG.isInfoEnabled()) LOG.info("lockTMReq Success");
          System.out.println("unlockTMReq Success");
        }
      }
      else if(args[i].equals("s"))
      {
        rc = xdc.sdnTranReq();
        if(rc != 0) {
          //LOG.error("sdnTranReq failed " + rc);
	      System.out.println("sdnTranReq failed " + rc);
	      System.exit(1);
	    }
        else{
          //if (LOG.isInfoEnabled()) LOG.info("sdnTranReq Success");
          System.out.println("sdnTranReq Success \n");
        }
      }
      else if(args[i].equals("m"))
      {
        String backupTag = args[++i].toString();
        config = HBaseConfiguration.create();
        
        try{
          BackupRestoreClient brc = new BackupRestoreClient(config);
          brc.setVerbose(lv_verbose);
          brc.xdcMutationCaptureBegin(backupTag);
          //if (LOG.isInfoEnabled()) LOG.info("xdcMutationCaptureBegin Success");
	      if (lv_verbose) {
	         System.out.println("xdcMutationCaptureBegin Success");
	      }
	  while (i < (args.length -1)) {
	      String tableName = args[++i].toString();
          //if (LOG.isInfoEnabled()) LOG.info("xdcMutationCaptureAddTable" + " table: " + tableName);
          if (lv_verbose) {
             System.out.println("xdcMutationCaptureAddTable"
				     + " table: " + tableName
				     );
	      }
	      brc.xdcMutationCaptureAddTable(backupTag, tableName);
	  }
          //if (LOG.isInfoEnabled()) LOG.info("xdcMutationCaptureAddTable(s) Success");
          if (lv_verbose) {
	        System.out.println("xdcMutationCaptureAddTable(s) Success");
	      }
          brc.xdcMutationCaptureEnd();
        }catch(Exception e) {
          //LOG.error("xdcMutationCapture failed ", e);
          System.out.println("xdcMutationCapture failed " + e);
          System.exit(1);
        }
        //if (LOG.isInfoEnabled()) LOG.info("xdcMutationCapture success");
        System.out.println("xdcMutationCapture success");
      }
      else if(args[i].equals("x"))
      {
        config = HBaseConfiguration.create();

        try{
          BackupRestoreClient brc = new BackupRestoreClient(config);
          brc.setVerbose(lv_verbose);
          brc.cleanUnneededMutations();
        }catch(Exception e) {
          //LOG.error("cleanXdcMutations failed ", e);
          System.out.println("cleanXdcMutations failed " + e);
          System.exit(1);
        }
        //if (LOG.isInfoEnabled()) LOG.info("cleanXdcMutations success");
        System.out.println("cleanXdcMutations success");
      }
      else if(args[i].equals("c"))
      {
        int lv_num_threads = Integer.parseInt(args[++i]);
        config = HBaseConfiguration.create();

        try{
          BackupRestoreClient brc = new BackupRestoreClient(config);
          brc.setVerbose(lv_verbose);
          brc.crashRecover(lv_num_threads);
        }catch(Exception e) {
          //LOG.error("cleanXdcMutations failed ", e);
          System.out.println("crashRecover failed " + e);
          System.exit(1);
        }
        //if (LOG.isInfoEnabled()) LOG.info("cleanXdcMutations success");
        System.out.println("crashRecover success");
      }
      else if(args[i].equals("b"))
      {
        int b_key = Integer.parseInt(args[++i]);
        int b_value = Integer.parseInt(args[++i]);
        int b_peer_id = Integer.parseInt(args[++i]);
        ArrayList<String> tableList=new ArrayList<String>();
        config = HBaseConfiguration.create();
        
        try {
          BackupRestoreClient brc = new BackupRestoreClient(config);
          brc.setVerbose(lv_verbose);
          //if (LOG.isInfoEnabled()) LOG.info("xdcBroadcastBegin Success <key,value,peer_id>: " + b_key + " " + b_value + " " + b_peer_id);
	      if (lv_verbose) {
	           System.out.println("xdcBroadcastBegin Success <key,value,peer_id>: " + b_key + " " + b_value + " " + b_peer_id);
	      }
	      while (i < (args.length -1)) {
	           String tableName = args[++i].toString();
	           //if (LOG.isInfoEnabled()) LOG.info("xdcBroadcast addTable: " + tableName);
	           if (lv_verbose) { 
                     System.out.println("xdcBroadcast addTable: " + tableName );
	            }
                tableList.add(tableName);
	            //brc.xdcMutationCaptureAddTable(backupTag, tableName);
	      } // while
          //if (LOG.isInfoEnabled()) LOG.info("xdcBroadcast addTable(s) Success " + tableList.size());
	      if (lv_verbose) {
	           System.out.println("xdcBroadcast addTable(s) Success " + tableList.size());
	      }
          if (b_key != 3) brc.xdcBroadcast(b_key, b_value, b_peer_id, tableList);
          else brc.xdcRecoveryBroadcast(b_key, b_value, b_peer_id, tableList);
        } catch(Exception e) {
          //LOG.error("xdcBroadcast failed ", e);
          System.out.println("xdcBroadcast failed " + e);
	      System.exit(1);
        }
        //if (LOG.isInfoEnabled()) LOG.info("xdcBroadcast  success");
	    System.out.println("xdcBroadcast success");
        //System.exit(0);
      } // else if
      else if(args[i].equals("r"))
      {

        int lv_peerId = Integer.parseInt(args[++i]);
	int lv_num_threads = Integer.parseInt(args[++i]);
        ArrayList<String> tableList=new ArrayList<String>();
        config = HBaseConfiguration.create();
        //if (LOG.isInfoEnabled()) LOG.info("xdc replay: " + " peerId: " + lv_peerId + " #threads: " + lv_num_threads);
        if (lv_verbose) {
	       System.out.println("xdc replay: "
			       + " peerId: " + lv_peerId
			       + " #threads: " + lv_num_threads
			       );
	    }
        
        try{
          BackupRestoreClient brc = new BackupRestoreClient(config);
          brc.setVerbose(lv_verbose);
          //if (LOG.isInfoEnabled()) LOG.info("xdc Replay Begin <peer_id, #threads>: " + lv_peerId + " " + lv_num_threads);

              if (lv_verbose) {
                   System.out.println("xdc Replay Begin <peer_id, #threads>: " + lv_peerId + " " + lv_num_threads);
              }
              while (i < (args.length -1)) {
                   String tableName = args[++i].toString();
                   //if (LOG.isInfoEnabled()) LOG.info("xdc Replay addTable: " + tableName);
                   if (lv_verbose) { 
                     System.out.println("xdc Replay addTable: " + tableName );
                    }
                    tableList.add(tableName);
              } // while
              //if (LOG.isInfoEnabled()) LOG.info("xdc Replay addTable(s) Success " + tableList.size());
              if (lv_verbose) {
                   System.out.println("xdc Replay addTable(s) Success " + tableList.size());
              }

          brc.catchupReplay(lv_num_threads, lv_peerId, tableList);
          //if (LOG.isInfoEnabled()) LOG.info("xdc catchup Replay Success");
          System.out.println("xdc catchup Replay Success");
        }catch(Exception e) {
          //LOG.error("xdc catchup Replay failed ", e);
          System.out.println("xdc catchup Replay failed " + e);
          String lv_exception_string = new String(e.getMessage());
          //LOG.error("XDC exception message: " + lv_exception_string);
          System.out.println("XDC exception message: " + lv_exception_string);
	      if (lv_exception_string.contains("getPriorSnapshotSet(key) Prior record not found")) {
            //LOG.error("As prior snapshot not found, exitting with error 0");
	        System.out.println("As prior snapshot not found, exitting with error 0");
	        System.exit(0);
	      }
          System.exit(1);
        }
      }  // else if
      else if(args[i].equals("k")) {
          // lock clean xdc
          int b_peer_id = Integer.parseInt(args[++i]);
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
              lmLockCleanXdcMessage.setClusterId(b_peer_id);
              lmLockCleanXdcMessage.setXdcStatus(XDC_STATUS.XDC_PEER_UP);
              rsConnection.send(lmLockCleanXdcMessage);
              RSMessage responseMsg = rsConnection.receive();
              rsConnection.close();
              if (responseMsg.getMsgType() == RSMessage.MSG_TYP_OK) {
                  System.out.println("lock clean xdc up success");
              } else if (responseMsg.getMsgType() == RSMessage.MSG_TYP_LOCK_CLEAN_XDC_NO_EXIST) {
                  System.out.println("lock clean xdc not start");
              }else {
                  System.out.println("lock clean xdc up failed");
                  System.exit(1);
              }
          } catch (IOException e) {
              System.out.println("LM_LISTENER_PORT ERROR");
              System.out.println("please export LM_LISTENER_PORT=xxxx in current env");
              System.exit(1);
          } catch (Exception e) {
              System.out.println("lock clean xdc up failed");
              System.exit(1);
          }
      }
    }

    if(args.length == 0)
      System.out.println("Usage: XDC [-v] <[l][u][s][m <backupName tableName>][r]> \n" + 
			 "\n" +
			 "l              : Lock TM \n" + 
			 "u              : Unlock TM \n" +
			 "s              : Drain SDN transactions \n" +
			 "m <backupName> : Mark begin for mutation capture \n" +
			 "x              : Clean mutations not associated with a backup \n" +
			 "c              : crash recover \n" +
			 "b key value <tables> : Broadcast <key,value> to transaction tables \n" +
			 "r <#parallel>  : Launch Catchup Replay \n" +
			 "-v             : Verbose"
			 );

  }
}
