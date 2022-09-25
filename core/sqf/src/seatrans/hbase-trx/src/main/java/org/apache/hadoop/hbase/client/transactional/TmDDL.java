/**
* @@@ START COPYRIGHT @@@
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
**/



package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class TmDDL {

   static final Log LOG = LogFactory.getLog(TmDDL.class);
   private Connection connection;
   private Configuration adminConf;
   private Connection adminConnection;
   private Configuration config;
   private int dtmid;
   private static final byte[] TDDL_FAMILY = Bytes.toBytes("tddlcf");
   private static final byte[] TDDL_CREATE = Bytes.toBytes("createList");
   private static final byte[] TDDL_CREATE_INCR = Bytes.toBytes("createIncrList");
   private static final byte[] TDDL_DROP = Bytes.toBytes("dropList");
   private static final byte[] TDDL_TRUNCATE = Bytes.toBytes("truncateList");
   private static final byte[] TDDL_STATE = Bytes.toBytes("state");
   private static Object tablePutLock;            // Lock for synchronizing table.put operations
   private static Table table;
   
   //TRAF_RESERVED_NAMESPACE5 namespace name in sql/common/ComSmallDefs.h.
   private static String tddlName = "TRAF_RSRVD_5:TRAFODION._DTM_.TDDL";

   public TmDDL (Configuration config) throws IOException {

      this.config = config;

      this.config.set("hbase.client.pause", "1");
      this.config.set("hbase.client.retries.number", "30");

      this.connection = ConnectionFactory.createConnection(this.config);
      //this.connection = connection;
      this.dtmid = Integer.parseInt(this.config.get("dtmid"));
      TableName tablename = TableName.valueOf(tddlName);

      if (LOG.isTraceEnabled()) LOG.trace("Enter TmDDL constructor for dtmid: " + dtmid);

      String rpcTimeout = System.getenv("HAX_ADMIN_RPC_TIMEOUT");
      if (rpcTimeout != null){
           adminConf = new Configuration(this.config);
           int rpcTimeoutInt = Integer.parseInt(rpcTimeout.trim());
           String value = adminConf.getTrimmed("hbase.rpc.timeout");
           adminConf.set("hbase.rpc.timeout", Integer.toString(rpcTimeoutInt));
           String value2 = adminConf.getTrimmed("hbase.rpc.timeout");
           LOG.info("HAX: ADMIN RPC Timeout, revise hbase.rpc.timeout from " + value + " to " + value2);
           adminConnection = ConnectionFactory.createConnection(adminConf);
      }
      else {
           adminConnection = this.connection;
      }

      Admin admin = connection.getAdmin();

      //First check if non-namespace TDDL table exists. return error if exist.
      boolean tDDLTableExists = admin.tableExists(TableName.valueOf("TRAFODION._DTM_.TDDL"));
      if(tDDLTableExists == true)
      {
          //This new version of software does not expect oldtddl to exist.
          //return error.
          LOG.error("TmDDL: Old version TRAFODION._DTM_.TDDL exists");
          throw new IOException("TmDDL: Old version TRAFODION._DTM_.TDDL exists");
          
      }
      
      tDDLTableExists = admin.tableExists(tablename);
      boolean loopExit = false;
      do
      {
        try {
           if (tDDLTableExists==false) {
              if (dtmid == 0){
                 HTableDescriptor desc = new HTableDescriptor(tablename);
                 desc.addFamily(new HColumnDescriptor(TDDL_FAMILY));
                 admin.createTable(desc);
              }
              else {
                 Thread.sleep(3000);  // Sleep 3 seconds to allow table creation
                 if (admin.tableExists(tablename) == false){
                     // still not present; try again
                     if (LOG.isTraceEnabled()) LOG.trace("TmDDL still not created for dtmid: " + dtmid);
                     continue;
                 }
              }
           }
           loopExit = true;
        }
        catch (InterruptedException ie) {}
        catch (IOException e) {
           loopExit = true;
           LOG.error("Exception at the time of creating TmDDL ", e);
           throw e;
        }
      } while (loopExit == false);

      tablePutLock = new Object();

      table = this.connection.getTable(tablename);
      admin.close();
   }

   public void putRow(final long transid, final String Operation, final String tableName) throws IOException {

        long threadId = Thread.currentThread().getId();
        if (LOG.isTraceEnabled()) LOG.trace("TmDDL putRow Operation, TxID: " + transid + " Thread ID:" + threadId 
                + " TableName:" + tableName + " Operation :" + Operation);
        byte [] value = null;
        StringBuilder tableString = null;
        Result r = null;
        Put p = new Put(Bytes.toBytes(transid));


        //Retrieve the row if it exists so we can append.
        Get g = new Get(Bytes.toBytes(transid));
        r = table.get(g);
        //Check and set State
        if(! r.isEmpty())
        {
            value = r.getValue(TDDL_FAMILY, TDDL_STATE);
        }
        if((value != null) && (value.length > 0) && Bytes.toString(value).equals("INVALID"))
        {
            LOG.error("TmDDL putRow on invalid Transaction TxID:" + transid);
            throw new RuntimeException("TmDDL putRow on invalid Transaction State TxID :" + transid);
        }
        else
        {
            p.addColumn(TDDL_FAMILY, TDDL_STATE, Bytes.toBytes("VALID"));
        }

        //Check and append table name
        if(Operation.equals("CREATE"))
        {
            if(! r.isEmpty())
            {
                value = r.getValue(TDDL_FAMILY, TDDL_CREATE);
            }
            if((value != null) && (value.length > 0) && Bytes.toString(value).equals("INVALID"))
            {
                LOG.error("TmDDL putRow on invalid Transaction TxID:" + transid);
                throw new RuntimeException("putRow on invalid Transaction State TxID:" + transid);
            }
            else
            {
                tableString = new StringBuilder();
                if(value != null && value.length > 0)
                {
                    tableString.append(Bytes.toString(value));
                    tableString.append(",");
                }
                tableString.append(tableName);
                p.addColumn(TDDL_FAMILY, TDDL_CREATE, Bytes.toBytes(tableString.toString()));
            }

        }
        
        //Check and append table name
        if(Operation.equals("CREATE_INCR"))
        {
            if(! r.isEmpty())
            {
                value = r.getValue(TDDL_FAMILY, TDDL_CREATE_INCR);
            }
            if((value != null) && (value.length > 0) && Bytes.toString(value).equals("INVALID"))
            {
                LOG.error("TmDDL putRow on invalid Transaction TxID:" + transid);
                throw new RuntimeException("putRow on invalid Transaction State TxID:" + transid);
            }
            else
            {
                tableString = new StringBuilder();
                if(value != null && value.length > 0)
                {
                    tableString.append(Bytes.toString(value));
                    tableString.append(",");
                }
                tableString.append(tableName);
                p.addColumn(TDDL_FAMILY, TDDL_CREATE_INCR, Bytes.toBytes(tableString.toString()));
            }

        }


        // Check and append table name
        if(Operation.equals("TRUNCATE"))
        {
            if(! r.isEmpty())
            {
                value = r.getValue(TDDL_FAMILY, TDDL_TRUNCATE);
            }
            if((value != null) && (value.length > 0) && Bytes.toString(value).equals("INVALID"))
            {
                LOG.error("putRow on invalid Transaction, TxID :" + transid);
                throw new RuntimeException("putRow on invalid Transaction State :" + transid);
            }
            else
            {
                tableString = new StringBuilder();
                if(value != null && value.length > 0)
                {
                    tableString.append(Bytes.toString(value));
                    tableString.append(",");
                }
                tableString.append(tableName);
                p.addColumn(TDDL_FAMILY, TDDL_TRUNCATE, Bytes.toBytes(tableString.toString()));
            }
        }

        //Check and append table name
        if(Operation.equals("DROP"))
        {
            if(! r.isEmpty())
            {
                value = r.getValue(TDDL_FAMILY, TDDL_DROP);
            }
            if((value != null) && (value.length > 0) && Bytes.toString(value).equals("INVALID"))
            {
                LOG.error("TmDDL putRow invalid Transaction state, TxID :" + transid);
                throw new RuntimeException("putRow on invalid Transaction State :" + transid);
            }
            else
            {
                tableString = new StringBuilder();
                if(value != null && value.length > 0)
                {
                    tableString.append(Bytes.toString(value));
                    tableString.append(",");
                }
                tableString.append(tableName);
                p.addColumn(TDDL_FAMILY, TDDL_DROP, Bytes.toBytes(tableString.toString()));
            }

        }

            synchronized (tablePutLock) {
                  if (LOG.isTraceEnabled()) LOG.trace("TmDDL table.put, TxID: " + transid + " Put:" + p );

                  table.put(p);
            } // End global synchronization
      if (LOG.isTraceEnabled()) LOG.trace("TmDDL putRow exit, TxId:" + transid);
   }

   public void setState(final long transid, final String state) throws IOException {
      long threadId = Thread.currentThread().getId();
      if (LOG.isTraceEnabled()) LOG.trace("TmDDL setState start in thread: " + threadId + " TxId:" + transid + " State:" + state);

      Put p = new Put(Bytes.toBytes(transid));
      p.addColumn(TDDL_FAMILY, TDDL_STATE, Bytes.toBytes(state));
      synchronized (tablePutLock) {
         if (LOG.isTraceEnabled()) LOG.trace("TmDDL setState method. table.put. TxId: " + transid + " Put:" + p );
         table.put(p);
      } // End global synchronization
      if (LOG.isTraceEnabled()) LOG.trace("TmDDL setState exit, TxID:" + transid);
   }

   public void getRow(final long lvTransid, StringBuilder state,
                       ArrayList<String> createList,
                       ArrayList<String> createIncrList,
                       ArrayList<String> dropList, 
                       ArrayList<String> truncateList)
              throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("TmDDL getRow start, TxID: " + lvTransid);
      String recordString = null;
      StringTokenizer st = null;
      byte [] value = null;
            Get g = new Get(Bytes.toBytes(lvTransid));
            Result r = table.get(g);

            if(! r.isEmpty())
            {
                value = r.getValue(TDDL_FAMILY, TDDL_CREATE);
                if(value != null && value.length > 0)
                {
                    recordString =  new String (Bytes.toString(value));
                    st = new StringTokenizer(recordString, ",");

                    while (st.hasMoreElements())
                    {
                        createList.add(st.nextToken());
                    }
                }
                
                value = r.getValue(TDDL_FAMILY, TDDL_CREATE_INCR);
                if(value != null && value.length > 0)
                {
                    recordString =  new String (Bytes.toString(value));
                    st = new StringTokenizer(recordString, ",");

                    while (st.hasMoreElements())
                    {
                        createIncrList.add(st.nextToken());
                    }
                }

                value = r.getValue(TDDL_FAMILY, TDDL_DROP);
                if(value != null && value.length > 0)
                {
                    recordString =  new String (Bytes.toString(value));
                    st = new StringTokenizer(recordString, ",");
                    while (st.hasMoreElements())
                    {
                        dropList.add(st.nextToken());
                    }
                }

                value = r.getValue(TDDL_FAMILY, TDDL_TRUNCATE);
                if(value != null && value.length > 0)
                {
                    recordString =  new String (Bytes.toString(value));
                    st = new StringTokenizer(recordString, ",");
                    while (st.hasMoreElements())
                    {
                        truncateList.add(st.nextToken());
                    }
                }

                value = r.getValue(TDDL_FAMILY, TDDL_STATE);
                if(value != null && value.length > 0)
                {
                    state.append(Bytes.toString(value));
                }
            }
    }

    public void getState(final long lvTransid, StringBuilder state) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("TmDDL getState start, TxID:" + lvTransid);
      byte [] value = null;
      short retry = 0;
      Result r = null;
      boolean loopBack = false;
      boolean succeed = false;
            
            Get g = new Get(Bytes.toBytes(lvTransid));

            do {
                     try {
                         r = table.get(g);
                         succeed = true;
                     }  catch (IOException e) {
                         LOG.warn("An error occurred while accessing TDDL ", e);
                         retry++;
                         if (retry >= 3) {
                               throw new IOException("Error in accessig TDDL"); // or runtime error ?
                        }
                        do {
                              try {
                                    loopBack = false;
                                    Thread.sleep(3000); // 3 seconds delay
                               } catch (InterruptedException ie) {
                                     loopBack = true;
                               }
                         } while (loopBack);
                         LOG.warn("An error occurred while accessing TDDL, retry,  count " + retry);
                         table.close();
                         this.connection.close();
                         this.connection = ConnectionFactory.createConnection(this.config);
                         table = this.connection.getTable(TableName.valueOf(tddlName));
                     }
            } while ((!succeed) && (retry < 3));

            if((r != null) && (! r.isEmpty()))
            {
                value = r.getValue(TDDL_FAMILY, TDDL_STATE);
                if(value != null && value.length > 0)
                {
                    state.append(Bytes.toString(value));
                }
                else
                {
                    state.append("INVALID");
                }
            }
            else
            {
                state.append("INVALID");
            }
    }

    public void deleteRow(final long lvTransid) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("TmDDL deleteRow start, TxID: " + lvTransid);
            Delete d = new Delete(Bytes.toBytes(lvTransid));
            table.delete(d);
    }
    
    public List<Long> getTxIdList(short tmID) throws IOException {
      byte [] value = null;
      Scan s = new Scan();
      s.setCaching(100);
      s.setCacheBlocks(false);
      ArrayList<Long> txIdList = new ArrayList<Long>();
      ResultScanner ss = table.getScanner(s);
      try{
        for (Result r : ss) {
        Long txid = Bytes.toLong(r.getRow());
        if(TransactionState.getNodeId(txid) != tmID)
        {
          //not owned by this tmID
          continue;
        }
        value = r.getValue(TDDL_FAMILY, TDDL_STATE);
        if((value != null) && (value.length > 0) && 
           (Bytes.toString(value).equals("VALID") ||
            Bytes.toString(value).equals("REDRIVE")))
          txIdList.add(txid);
        }
      }
      finally {
          ss.close();
      }
      
      return txIdList;
    }
    
 }
