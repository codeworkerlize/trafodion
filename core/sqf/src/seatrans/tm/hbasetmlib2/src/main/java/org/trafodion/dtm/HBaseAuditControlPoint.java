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

package org.trafodion.dtm;

import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;


public class HBaseAuditControlPoint {

    static final Log LOG = LogFactory.getLog(HBaseAuditControlPoint.class);
    private static long currControlPt;
    private Connection connection;
    private Configuration adminConf;
    private Connection adminConnection;
    private Configuration config;
    private static String CONTROL_POINT_TABLE_NAME;
    private static String NAME_SPACE;
    private static final byte[] CONTROL_POINT_FAMILY = Bytes.toBytes("cpf");
    private static final byte[] CP_NUM_AND_ASN_HWM = Bytes.toBytes("hwm");
    private Table table;
    private boolean useAutoFlush;
    private boolean disableBlockCache;
    private static boolean localizeTlog;
    private static int versions;
    private static STRConfig pSTRConfig = null;
    private int TlogRetryDelay;
    private int CtrlPtRetryCount;

    public static final int TM_SLEEP = 1000;      // One second
    public static final int TM_SLEEP_INCR = 5000; // Five seconds
    public static final int TM_RETRY_ATTEMPTS = 5;

    static{
       versions = 10;
       try {
          String versionsToKeep = System.getenv("TM_TX_CLEANUP_DELAY");
          if (versionsToKeep != null){
             versions = ((Integer.parseInt(versionsToKeep.trim()) > 10) ? Integer.parseInt(versionsToKeep.trim()) : 10);
          }
       }
       catch (NumberFormatException e) {
          LOG.error("TM_TX_CLEANUP_DELAY is not valid in ms.env");
       }
       LOG.info("TM_TX_CLEANUP_DELAY is " + versions);

       localizeTlog = false;
       try {
          String localizeTlogString = System.getenv("TM_TLOG_LOCALIZE_TLOG");
          if (localizeTlogString != null){
             localizeTlog = (Integer.parseInt(localizeTlogString.trim()) != 0);
             if (LOG.isTraceEnabled()) LOG.trace("localizeTlog != null");
          }
       }
       catch (NumberFormatException e) {
          LOG.error("TM_TLOG_LOCALIZE_TLOG is not valid in ms.env");
       }
       LOG.info("localizeTlog is " + localizeTlog);
    }

    public HBaseAuditControlPoint(Configuration config, Connection connection) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("Enter HBaseAuditControlPoint constructor()");
      this.config = config;
      this.connection = connection;
      CONTROL_POINT_TABLE_NAME = config.get("CONTROL_POINT_TABLE_NAME");
      NAME_SPACE = config.get("NAME_SPACE");
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(CONTROL_POINT_TABLE_NAME));
      HColumnDescriptor hcol = new HColumnDescriptor(CONTROL_POINT_FAMILY);

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

      try {
         pSTRConfig = STRConfig.getInstance(config);
      }
      catch (ZooKeeperConnectionException zke) {
         LOG.error("Zookeeper Connection Exception trying to get STRConfig instance: " , zke);
         throw new IOException(zke);
      }

      TlogRetryDelay = 3000; // 3 seconds
      try {
         String retryDelayS = System.getenv("TM_TLOG_RETRY_DELAY");
         if (retryDelayS != null){
            TlogRetryDelay = (Integer.parseInt(retryDelayS) > TlogRetryDelay ? Integer.parseInt(retryDelayS) : TlogRetryDelay);
         }
      }
      catch (NumberFormatException e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_RETRY_DELAY is not valid in ms.env");
      }

      CtrlPtRetryCount = 3;

      disableBlockCache = false;
      try {
         String blockCacheString = System.getenv("TM_TLOG_DISABLE_BLOCK_CACHE");
         if (blockCacheString != null){
             disableBlockCache = (Integer.parseInt(blockCacheString) != 0);
         if (LOG.isDebugEnabled()) LOG.debug("disableBlockCache != null");
         }
      }
      catch (NumberFormatException e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_DISABLE_BLOCK_CACHE is not valid in ms.env");
      }
      LOG.info("disableBlockCache is " + disableBlockCache);
      if (disableBlockCache) {
         hcol.setBlockCacheEnabled(false);
      }

      hcol.setMaxVersions(versions);

      desc.addFamily(hcol);

      useAutoFlush = true;
      String autoFlush = System.getenv("TM_TLOG_AUTO_FLUSH");
      if (autoFlush != null){
         useAutoFlush = (Integer.parseInt(autoFlush) != 0);
         if (LOG.isDebugEnabled()) LOG.debug("autoFlush != null");
      }
      LOG.info("useAutoFlush is " + useAutoFlush);
      Admin admin = adminConnection.getAdmin();
      boolean lvControlPointExists = admin.tableExists(TableName.valueOf(CONTROL_POINT_TABLE_NAME));
      if (LOG.isDebugEnabled()) LOG.debug("HBaseAuditControlPoint lvControlPointExists " + lvControlPointExists);
      currControlPt = -1;
      try {
         if (LOG.isInfoEnabled()) LOG.info("Creating the table " + CONTROL_POINT_TABLE_NAME);
         admin.createTable(desc);
         currControlPt = 1;
      }
      catch (TableExistsException e) {

         // make sure the table is enabled
         boolean retry = false;
         if (!admin.isTableEnabled(TableName.valueOf(CONTROL_POINT_TABLE_NAME))) {
            admin.enableTable(TableName.valueOf(CONTROL_POINT_TABLE_NAME));
         }
         do {
           retry = false;
           if (!admin.isTableEnabled(TableName.valueOf(CONTROL_POINT_TABLE_NAME))) {
             LOG.info("Table " + CONTROL_POINT_TABLE_NAME + " is not yet enabled" );
             retry = true;
           }
           if (retry)
             retry(TM_SLEEP);
         } while (retry);

         HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(CONTROL_POINT_TABLE_NAME));
         HColumnDescriptor hcd = htd.getFamily(CONTROL_POINT_FAMILY);
         int currVersions = hcd.getMaxVersions();
         if (currVersions == versions){
            LOG.info("Table " + CONTROL_POINT_TABLE_NAME + " already exists with the correct max versions setting: "
                     + versions );
         }
         else {
            LOG.info("Table " + CONTROL_POINT_TABLE_NAME + " already exists, modifying max versions from "
                     + currVersions + " to " + versions );

            // Make sure we set the number of versions correctly as currently configured
            admin.modifyTable(TableName.valueOf(CONTROL_POINT_TABLE_NAME), desc);
         }
      }

      try{
         table = connection.getTable(desc.getTableName());
      }
      catch(Exception e){
         LOG.error("HBaseAuditControlPoint Exception; ", e);
         throw new IOException("HBaseAuditControlPoint Exception; ",e);
      }

      if (localizeTlog){

         // We want to try to have the CONTROLPOINT region hosted by the RegionServer
         // residing on the same node at the DTM process that will write to it.
         List<HRegionInfo> ril = admin.getTableRegions(TableName.valueOf(CONTROL_POINT_TABLE_NAME));
         Iterator<HRegionInfo> it = ril.iterator();
         HRegionInfo hri = it.next();

         InetAddress ip = InetAddress.getLocalHost();
         String lvHost = ip.getHostName();
         ClusterStatus status = admin.getClusterStatus();
         Collection<ServerName> regionServers = status.getServers();
         for (ServerName sn : regionServers) {
            String host = sn.getHostname();
            LOG.info("HBaseAuditControlPoint host from RegionServer: " + host
                          + " host from getHostName: " + lvHost);
            if (host.compareTo(lvHost) == 0){
               LOG.info("HBaseAuditControlPoint moving " + CONTROL_POINT_TABLE_NAME
                          + " to server: " + sn.getServerName());
               admin.move(hri.getEncodedNameAsBytes(), Bytes.toBytes(sn.getServerName()));
            }
         }
      }

      // Make sure the table is enabled one last time
      boolean retry = false;
      if (!admin.isTableEnabled(TableName.valueOf(CONTROL_POINT_TABLE_NAME))) {
         admin.enableTable(TableName.valueOf(CONTROL_POINT_TABLE_NAME));
      }
      do {
        retry = false;
        if (!admin.isTableEnabled(TableName.valueOf(CONTROL_POINT_TABLE_NAME))) {
          LOG.info("Table " + CONTROL_POINT_TABLE_NAME + " is not yet enabled" );
          retry = true;
        }
        if (retry)
          retry(TM_SLEEP);
      } while (retry);

      admin.close();

      if (currControlPt == -1){
         if (LOG.isDebugEnabled()) LOG.debug("getting currControlPt for clusterId " + pSTRConfig.getTrafClusterIdInt()
                 + " instanceId " + pSTRConfig.getTrafInstanceIdInt());
         currControlPt = getCurrControlPt(pSTRConfig.getTrafClusterIdInt(), pSTRConfig.getTrafInstanceIdInt());
      }
      if (LOG.isDebugEnabled()) LOG.debug("currControlPt is " + currControlPt);
      return;
    }

    public int retry(int retrySleep) {
       boolean keepPolling = true;
       while (keepPolling) {
          try {
              Thread.sleep(retrySleep);
              keepPolling = false;
          } catch(InterruptedException ex) {
            // ignore the interruption and keep going
          }
       }
       return (retrySleep += TM_SLEEP_INCR);
    }

    public long getCurrControlPt(final int clusterId, final int instanceId) throws IOException {
       if (LOG.isTraceEnabled()) LOG.trace("getCurrControlPt:  start, clusterId " + clusterId + ", instanceId " + instanceId);
       long lvCpNum = 1;

       Get g = new Get(Bytes.toBytes((instanceId << 8) | clusterId));
       Result r = table.get(g);
       if (r.isEmpty())
          return lvCpNum;
       String value = new String(Bytes.toString(r.getValue(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM)));
       // In theory the above value is the latest version of the column
//       if (LOG.isDebugEnabled()) LOG.debug("getCurrControlPt for clusterId: " + clusterId + ", valueString is " + value);
       StringTokenizer stok = new StringTokenizer(value, ",");
       if (stok.hasMoreElements()) {
          String ctrlPtToken = stok.nextElement().toString();
          lvCpNum = Long.parseLong(ctrlPtToken, 10);
       }
       if (LOG.isTraceEnabled()) LOG.trace("getCurrControlPt value for clusterId: "
            + clusterId + " and InstanceId: " + instanceId + " is: " + lvCpNum);
       return lvCpNum;
    }

   public long putRecord(final int clusterId, final int instanceId, final long ControlPt, final long startingSequenceNumber) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("putRecord clusterId: " + clusterId + ", instanceId: " + instanceId
                    + ", startingSequenceNumber (" + startingSequenceNumber + ")");
      Put p = new Put(Bytes.toBytes((instanceId << 8 ) | clusterId));
      p.addColumn(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM,
    		  Bytes.toBytes(String.valueOf(ControlPt) + ","
    	               + String.valueOf(startingSequenceNumber)));
      boolean complete = false;
      int retries = 0;
      do {
         try {
       	    retries++;
            table.put(p);
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putRecord for cp: " + ControlPt + " on table "
                        + table.getName().getNameAsString());
            }
         }
         catch (IOException e){
             if (LOG.isTraceEnabled()) {
                LOG.trace("Retrying putRecord on control point: " + ControlPt + " on control point table "
                      + table.getName().getNameAsString() + " due to Exception ", e);
             }
             else {
                // Don't print the exception
                LOG.error("Retrying putRecord on control point: " + ControlPt + " on control point table "
                      + table.getName().getNameAsString() + " due to Exception ");
             }

             try {
                Thread.sleep(TlogRetryDelay); // 3 second default
             } catch (InterruptedException ie) {
             }

             try{
                connection.getRegionLocator(table.getName()).getRegionLocation(p.getRow(), true);
                //table.getRegionLocation(p.getRow(), true);
             }
             catch (Exception e2){
                LOG.error("putRecord returning -1; unable to locate region on control point: " + ControlPt + " on control point table "
                         + table.getName().getNameAsString() + " due to Exception ");
                if (LOG.isTraceEnabled()) LOG.trace("putRecord returning -1; unable to locate region on control point: " + ControlPt + " on control point table "
                        + table.getName().getNameAsString() + " due to Exception ", e2);
                return -1;
             }
             if (retries == CtrlPtRetryCount){
                LOG.error("putRecord returning -1 due to excessive retries on on control point table : "
                         + table.getName().getNameAsString() + " due to Exception; returning -1");
                return -1;
             }
         }
      } while (! complete && retries < CtrlPtRetryCount);  // default give up after 5 minutes
      if (LOG.isTraceEnabled()) LOG.trace("HBaseAuditControlPoint:putRecord returning " + ControlPt);
      return ControlPt;
   }

   public long getRecord(final int clusterId, final int instanceId, final String controlPt) throws IOException {

      if (LOG.isTraceEnabled()) LOG.trace("getRecord clusterId: " + clusterId + "instanceId: " + instanceId
                      + " controlPt: " + controlPt);
      long lvValue = -1;
      Get g = new Get(Bytes.toBytes((instanceId << 8) | clusterId));
      g.setMaxVersions(versions);  // will return last n versions of row
      g.addColumn(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);
      String ctrlPtToken;
      String asnToken;
         Result r = table.get(g);
         if (r.isEmpty())
            return lvValue;
         List<Cell> list = r.getColumnCells(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);  // returns all versions of this column
         for (Cell element : list) {
            StringTokenizer stok = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(element)), ",");
            if (stok.hasMoreElements()) {
               ctrlPtToken = stok.nextElement().toString();
//               if (LOG.isTraceEnabled()) LOG.trace("Parsing record for controlPt (" + ctrlPtToken + ")");
               asnToken = stok.nextElement().toString();
               if (Long.parseLong(ctrlPtToken, 10) == Long.parseLong(controlPt, 10)){
                  // This is the one we are looking for
                  lvValue = Long.parseLong(asnToken, 10);
                  if (LOG.isTraceEnabled()) LOG.trace("ASN value for controlPt: " + controlPt + " is: " + lvValue);
                  return lvValue;
               }
            }
            else {
               if (LOG.isTraceEnabled()) LOG.trace("No tokens to parse for controlPt (" + controlPt + ")");
            }
         }
         if (LOG.isTraceEnabled()) LOG.trace("all results scannned for clusterId: " + clusterId + ", but controlPt: " + controlPt + " not found");
      return lvValue;
   }

   public long getStartingAuditSeqNum(final int clusterId, final int instanceId) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("getStartingAuditSeqNum for clusterId: " + clusterId + " and instanceId " + instanceId);
      long lvAsn = 1;

      Get g = new Get(Bytes.toBytes((instanceId << 8) | clusterId));
      String asnToken;
      Result r = table.get(g);
      if (r.isEmpty())
         return lvAsn;
      String value = new String(Bytes.toString(r.getValue(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM)));
      // In theory the above value is the latestversion of the column
//      if (LOG.isDebugEnabled()) LOG.debug("getStartingAuditSeqNum for clusterId: " + clusterId + ", valueString is " + value);
      StringTokenizer stok = new StringTokenizer(value, ",");
      if (stok.hasMoreElements()) {
         stok.nextElement();  // skip the control point token
         asnToken = stok.nextElement().toString();
         lvAsn = Long.parseLong(asnToken, 10);
      }
      if (LOG.isTraceEnabled()) LOG.trace("getStartingAuditSeqNum for cluster " + clusterId + ", instanceId " + instanceId
             + " - exit returning " + lvAsn);
      return lvAsn;
    }

   public long getNextAuditSeqNum(int clusterId, int instanceId, int nid) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum for cluster " + clusterId + " instanceId: " + instanceId
                 + " node: " + nid);

      // We need to open the appropriate control point table and read the value from it
      Table remoteTable;
      String lv_tName = new String(NAME_SPACE + ":TRAFODION._DTM_.TLOG" + String.valueOf(nid) + "_CONTROL_POINT");
      remoteTable = connection.getTable(TableName.valueOf(lv_tName));

      long lvAsn = 1;

      try {
         Get g = new Get(Bytes.toBytes((instanceId << 8) | clusterId));
         if (LOG.isDebugEnabled()) LOG.debug("getNextAuditSeqNum attempting remoteTable.get");
         Result r = remoteTable.get(g);
         if (!r.isEmpty()){
            String value = new String(Bytes.toString(r.getValue(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM)));
            // In theory the above value is the latest version of the column
//            if (LOG.isDebugEnabled()) LOG.debug("getNextAuditSeqNum for clusterId: " + clusterId + ", valueString is " + value);
            StringTokenizer stok = new StringTokenizer(value, ",");
            if (stok.hasMoreElements()) {
               stok.nextElement();  // skip the control point token
               String asnToken = stok.nextElement().toString();
               lvAsn = Long.parseLong(asnToken, 10);
            }
         }
      } finally {
         remoteTable.close();
      }
      if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum for clusterId: "
              + clusterId + " is: " + (lvAsn + 1));
      return (lvAsn + 1);
   }


   public long doControlPoint(final int clusterId, final int instanceId, final long sequenceNumber, final boolean incrementCP) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("doControlPoint start for clusterId " + clusterId + " instanceId " + instanceId);

      if (incrementCP) {
        currControlPt++;
      }
      if (LOG.isTraceEnabled()) LOG.trace("doControlPoint interval (" + currControlPt + "), clusterId: " + clusterId + ", sequenceNumber (" + sequenceNumber+ ") try putRecord");
      try{
         long returnVal = 0;
         returnVal = putRecord(clusterId, instanceId, currControlPt, sequenceNumber);
         if (returnVal == -1 && incrementCP ){
            currControlPt--;
         }
      }
      catch (Exception e){
         LOG.error("doControlPoint - caught exception and unable to write control point record: ");
         if (LOG.isTraceEnabled()) LOG.trace("doControlPoint - caught exception and unable to write control point record: ", e);
         // If we incremented the currControlPt variable, then let's decrement before returning
         if (incrementCP) {
            currControlPt--;
         }
      }

      if (LOG.isTraceEnabled()) LOG.trace("doControlPoint - exit");
      return currControlPt;
   }

   public long bumpControlPoint(final int clusterId, final int instanceId, final int count) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("bumpControlPoint start, count: " + count);
      long currASN = -1;
         currControlPt = getCurrControlPt(clusterId, instanceId);
         currASN = getStartingAuditSeqNum(clusterId, instanceId);
         for ( int i = 0; i < count; i++ ) {
            currControlPt++;
            if (LOG.isTraceEnabled()) LOG.trace("bumpControlPoint putting new record " + (i + 1) + " for control point ("
                 + currControlPt + "), clusterId: " + clusterId + " instanceId: " + instanceId + ", ASN (" + currASN + ")");
            putRecord(clusterId, instanceId, currControlPt, currASN);
         }
      if (LOG.isTraceEnabled()) LOG.trace("bumpControlPoint - exit");
      return currASN;
   }

   public boolean deleteRecord(final long controlPoint) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord start for control point " + controlPoint);
      String controlPtString = new String(String.valueOf(controlPoint));

      Delete del = new Delete(Bytes.toBytes(controlPtString));
      if (LOG.isDebugEnabled()) LOG.debug("deleteRecord  (" + controlPtString + ") ");
      table.delete(del);
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord - exit");
      return true;
   }

   public boolean deleteAgedRecords(final int clusterId, final int instanceId, final long controlPoint) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteAgedRecords start - clusterId " + clusterId
               + " instanceId " + instanceId + " control point " + controlPoint);

      ArrayList<Delete> deleteList = new ArrayList<Delete>();
      Get g = new Get(Bytes.toBytes((instanceId << 8) | clusterId));
      g.setMaxVersions(versions);  // will return last n versions of row
      g.addColumn(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);
      String ctrlPtToken;
         Result r = table.get(g);
         if (r.isEmpty())
            return false;
         List<Cell> list = r.getColumnCells(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);  // returns all versions of this column
         for (Cell cell : list) {
            StringTokenizer stok = 
                    new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
            if (stok.hasMoreElements()) {
               ctrlPtToken = stok.nextElement().toString();
//               if (LOG.isTraceEnabled()) LOG.trace("Parsing record for controlPoint (" + ctrlPtToken + ")");
               if (Long.parseLong(ctrlPtToken, 10) <= controlPoint){
                  // This is one we are looking for
                  Delete del = new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), cell.getTimestamp());
                  deleteList.add(del);
                  if (LOG.isTraceEnabled()) LOG.trace("Deleting entry for ctrlPtToken: " + ctrlPtToken);
               }
            }
            else {
               if (LOG.isTraceEnabled()) LOG.trace("No tokens to parse for controlPoint (" + controlPoint + ")");
            }
         }
         if (LOG.isDebugEnabled()) LOG.debug("attempting to delete list with " + deleteList.size() + " elements");
         table.delete(deleteList);
      if (LOG.isTraceEnabled()) LOG.trace("deleteAgedRecords - exit");
      return true;
   }
   
   public String getTableName(){
      return CONTROL_POINT_TABLE_NAME;
   }
   
   public long getNthRecord(int clusterId, final int instanceId, int n) throws IOException{
      if (LOG.isTraceEnabled()) LOG.trace("getNthRecord start - clusterId " + clusterId + " instanceId " + instanceId + " n: " + n);

      Get g = new Get(Bytes.toBytes((instanceId << 8) | clusterId));
      g.setMaxVersions(n + 1);  // will return last n+1 versions of row just in case
      g.addColumn(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);
      String ctrlPtToken;
      long lvReturn = 1;
      Result r;
         try{
            r = table.get(g);
         }
         catch (Exception e){
            LOG.error("getNthRecord exception; returning 0 ");
            if (LOG.isTraceEnabled()) LOG.trace("getNthRecord exception; returning 0 ", e);
            return 0;
         }
         if (r.isEmpty())
            return lvReturn; 
         List<Cell> list = r.getColumnCells(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);  // returns all versions of this column
         int i = 0;
         for (Cell cell : list) {
            i++;
            StringTokenizer stok = 
                    new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
            if (stok.hasMoreElements()) {
               ctrlPtToken = stok.nextElement().toString();
//               if (LOG.isTraceEnabled()) LOG.trace("Parsing record for controlPoint (" + ctrlPtToken + ")");
               if ( i < n ){
//                  if (LOG.isTraceEnabled()) LOG.trace("Skipping record " + i + " of " + n + " for controlPoint" );
                  continue;
               }
               lvReturn = Long.parseLong(ctrlPtToken);;
               if (LOG.isTraceEnabled()) LOG.trace("getNthRecord exit - returning " + lvReturn);
               return lvReturn;
            }
            else {
               if (LOG.isTraceEnabled()) LOG.trace("No tokens to parse for " + i);
            }
         }
      if (LOG.isTraceEnabled()) LOG.trace("getNthRecord - exit returning 1");
      return 1;
   }
}

      
