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

//package org.apache.hadoop.hbase.client.transactional;
package org.apache.hadoop.hbase.pit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.pit.job.DependencyCheckUtils;
import org.apache.hadoop.hbase.pit.job.MutationReplayJob;
import org.apache.hadoop.hbase.pit.meta.AbstractSnapshotRecordMeta;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.protocol.SnapshotException;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.transactional.RMInterface;
import org.apache.hadoop.hbase.client.transactional.TransactionalTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.BroadcastResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportLaunchResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportLaunchRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportStatusRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportStatusResponse;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.exception.ExceptionUtils;

import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import com.google.protobuf.ServiceException;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.lang.ProcessBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.lang.Throwable;
import java.lang.Process;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.stream.Collector;

import org.apache.hadoop.hbase.pit.meta.SnapshotMetaEndRecord;
import org.apache.hadoop.hbase.pit.meta.SnapshotTagIndexRecord;
import org.apache.hadoop.util.ToolRunner;

//incr -true and sys -true = SYSTEM_PIT
//incr -false and sys -false = REGULAR
//incr -true and sys- false = INCREMENTAL
//incr -false and sys - true = SYSTEM
class BackupSet implements Serializable{
  private static final long serialVersionUID = -6864370808895151978L;
  String version;
  String extAttrs;
  ArrayList<TableSet> tableList;
  
  //either SYSTEM or NON-SYSTEM
  boolean sysBackup;
  
  //either imported or local
  boolean imported;
  
  //if NON-SYSTEM, whether REGULAR or INCREMENTAL
  boolean incremental;
  
  String timestamp;
  String backupTag;
  boolean validity;

  //This constructor is used during export/import.
  BackupSet(String bkpTag,
            ArrayList<TableSet> tlist,
            boolean incr,
            boolean sysbkp,
            boolean valid,
            String tstamp,
            String extAttr,
            boolean imp) {
    backupTag = bkpTag;
    tableList = (tlist != null)? new ArrayList<TableSet>(tlist): new ArrayList<TableSet>();
    incremental = incr;
    sysBackup = sysbkp;
    validity = valid;
    timestamp = tstamp;
    extAttrs = extAttr;
    imported = imp;
    version = "2.7.0";
  }
  

  ArrayList<TableSet> getTableList() {
    return tableList;
  }

  void resetTableList() {
    tableList = new ArrayList<TableSet>();
  }

  boolean isSysBackup() {
    return ((!incremental) && (sysBackup));
  }
  
  void setImported() {
    imported = true;
  }
  
  boolean isImported() {
    return imported;
  }

  boolean isIncremental() {
    return (incremental && (!sysBackup));
  }
  
  String getVersion() {
    return version;
  }
  
  String getBackupTag() {
    return backupTag;
  }

  String getTimeStamp() {
    return timestamp;
  }

  void setExtAttrs(String attrs) {
    extAttrs = attrs;
  }

  String getExtAttrs() {
    return extAttrs;
  }
  
  boolean isValid() {
    return validity;
  }
  
  //incr -true and sys -true = SYSTEM_PIT
  //incr -false and sys -false = REGULAR
  //incr -true and sys- false = INCREMENTAL
  //incr -false and sys - true = SYSTEM
  String getBackupType() {
    if(incremental && sysBackup) return "SYSTEM_PIT";
    if(incremental && (!sysBackup)) return "INCREMENTAL";
    if((!incremental) && (!sysBackup)) return "REGULAR";
    if((!incremental) && (sysBackup)) return "SYSTEM";
    return "INVALID";
  }
  
  boolean isValidVersion() {
    if(version.equals("2.7.0"))
      return true;
    else
      return false;
  }
  
  RecoveryRecord getRecoveryRecord(Configuration config) throws Exception {
    
    RecoveryRecord rr = null;
    if(isSysBackup() || (!isImported()))
      return rr;

    String hdfsRoot = config.get("fs.default.name");
    String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";

    Path backupsysRootPath = new Path(backupsysRoot);
    FileSystem backupsysRootFs = FileSystem.get(backupsysRootPath.toUri(), config);
            
    Path rrPath = new Path(backupsysRoot + "/" + backupTag, "recoveryRecord.bin");
    if(! backupsysRootFs.exists(rrPath)) {
       return rr;
    }
      
    //Read in recoveryRecord.bin
    FSDataInputStream rris = backupsysRootFs.open(rrPath);
    ObjectInputStream ois = new ObjectInputStream(rris);
    rr = (RecoveryRecord)ois.readObject();
    rr.setOpType("SANDBOX_RESTORE");
    ois.close();
    rris.close();
    
    return rr;
  }
  
  String displayString() {
    return " BackupTag: " + backupTag +
           " BackupType: " + getBackupType() + 
           " imported: " + imported +
           " version: " + version + 
           " extAttrs " + extAttrs +
           " TableListSize:" + String.valueOf(tableList.size());
      }
  }
  
  class BackupSetCompare implements Comparator<BackupSet> {
    // Used for sorting in ascending order of
    // roll number
    public int compare(BackupSet a, BackupSet b)  {
      return (a.getTimeStamp().compareTo(b.getTimeStamp()));
    }
  }

  class BackupSetReverseCompare implements Comparator<BackupSet> {
    // Used for sorting in descending order
    public int compare(BackupSet a, BackupSet b)  {
      return -1 * (a.getTimeStamp().compareTo(b.getTimeStamp()));
    }
  }

 class TableSet implements Serializable{
    
  private static final long serialVersionUID = -6542812921820584153L;
  String tableName;
  String snapshotName;
  String snapshotPath;
  ArrayList<String> lobSnapshotNameList;
  ArrayList<String> lobSnapshotPathList;
  ArrayList<String> mutationList;
  boolean incremental;
  ArrayList<String> brcSnapshotPathList;

  TableSet(String tblName, String snapName, String snapPath) {
    tableName = tblName;
    snapshotName = snapName;
    snapshotPath = snapPath;
    lobSnapshotNameList = new ArrayList<String>();
    lobSnapshotPathList = new ArrayList<String>();
    mutationList = new ArrayList<String>();
    incremental = false;
  }
  
  TableSet(String tblName, String snapName, String snapPath,
		  ArrayList<String> lobSnapshotNames, ArrayList<String> lobSnapshotPaths, ArrayList<String> mutations, boolean incrType) {
    tableName = tblName;
    snapshotName = snapName;
    snapshotPath = snapPath;
    lobSnapshotNameList = (lobSnapshotNames != null)? new ArrayList<String>(lobSnapshotNames): new ArrayList<String>();
    lobSnapshotPathList = (lobSnapshotPaths != null)? new ArrayList<String>(lobSnapshotPaths): new ArrayList<String>();
    mutationList = (mutations != null)? new ArrayList<String>(mutations): new ArrayList<String>();
    incremental = incrType;
  }
  
  String getTableName() {
    return tableName;
  }
  
  String getSnapshotPath() {
    return snapshotPath;
  }
  
  boolean isIncremental() {
    return incremental;
  }

  ArrayList<String> getLobSnapshotNames() {
    return lobSnapshotNameList;
  }

  int getLobSnapshotNameListSize() {
    if (lobSnapshotNameList == null){
       return 0;
    }
    else {
       return lobSnapshotNameList.size();
    }
  }

  ArrayList<String> getLobSnapshotPaths() {
    return lobSnapshotPathList;
  }

  int getLobSnapshotPathListSize() {
    if (lobSnapshotPathList == null){
      return 0;
    }
    else {
      return lobSnapshotPathList.size();
    }
  }

  ArrayList<String> getMutationList() {
    return mutationList;
  }
  int getMutationSize() {
    if (mutationList == null){
      return 0;
    }
    else{
      return mutationList.size();
    }
  }

  String displayString() {
    StringBuilder builder = new StringBuilder();
    builder.append("tableName: " + tableName +
            " snapshotName: " + snapshotName +
            " snapshotPath: " + snapshotPath +
            " incremental: " + incremental +
            " lobSnapshotNameListSize :" + getLobSnapshotNameListSize());
    for (int i = 0; i < getLobSnapshotNameListSize(); i++) {
       builder.append(" lobSnapshotName: " + lobSnapshotNameList.get(i));
    }
    builder.append(" lobSnapshotPathListSize :" + getLobSnapshotPathListSize());
    for (int i = 0; i < getLobSnapshotPathListSize(); i++) {
       builder.append(" lobSnapshotPath: " + lobSnapshotPathList.get(i));
    }
    builder.append(" mutationSize :" + getMutationSize());
    for (int i = 0; i < getMutationSize(); i++) {
       builder.append(" Mutation: " + mutationList.get(i));
    }
    return builder.toString();
  }

     public ArrayList<String> getBrcSnapshotPathList() {
         return brcSnapshotPathList;
     }

     public void setBrcSnapshotPathList(ArrayList<String> brcSnapshotPathList) {
         this.brcSnapshotPathList = brcSnapshotPathList;
     }
 }
  
 class TableSetCompare implements Comparator<TableSet> {
    // Used for sorting in ascending order of
    // roll number
    public int compare(TableSet a, TableSet b)  {
      return (a.getTableName().compareTo(b.getTableName()));
    }
  }

  
public class BackupRestoreClient implements Serializable
{
    private static final long serialVersionUID = 3361277481578700999L;
    private static String SNAPSHOT_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.SNAPSHOT";
    private static String MUTATION_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.MUTATION";
    private static String TRAF_BACKUP_MD_PREFIX = "_BACKUP_";
    private static String TRAF_CATALOG_PREFIX = "TRAFODION";

    private static List<String> HBASE_NATIVE_TABLES = Arrays.asList(
            "TRAF_RSRVD_5:TRAFODION._DTM_.BINLOG_READER",
            "ESG_TRAFODION._ORDER_SG_.ORDER_SEQ_GEN");

    private static final String MS_ENV_BR_EXPORT_QUEUE_NAME = "BR_EXPORT_QUEUE_NAME";
    private static final String MS_ENV_BR_IMPORT_QUEUE_NAME = "BR_EXPORT_QUEUE_NAME";
    private static final String BR_EXPORT_DEFAULT_QUEUE_NAME = "root.br.export";
    private static final String BR_IMPORT_DEFAULT_QUEUE_NAME = "root.br.import";


    public static final int BRC_SLEEP = 1000;      // One second
    public static final int BRC_SLEEP_INCR = 3000; // three seconds
    public static final int BRC_RETRY_ATTEMPTS = 5;

    public static final int GENERIC_SHIELD = 1;
    public static final int GENERIC_FLUSH_MUTATION_FILES = 2;
    public static final int GENERIC_RECOVERY_COMPLETE = 3;
    public static final int GENERIC_DEFER_SPLIT = 4;
    public static final int GENERIC_BLOCK_PHASE1 = 5;
    public static final int GENERIC_UNBLOCK_PHASE1 = 6;
    
    static Configuration config;
    static Connection connection;
    static Object adminConnLock = new Object();
    String lastError;
    String lastWarning;
    
    long timeIdVal = 1L;
    IdTm idServer;
    IdTmId timeId;
    private static int ID_TM_SERVER_TIMEOUT = 1000; // 1 sec
    private static long SNAPSHOT_MUTATION_FLUSH_KEY = 1L;

    private static SnapshotMeta sm;
    private static LobMeta lm;
    private static MutationMeta mm;
    private SnapshotMetaStartRecord smsr;
    int pit_thread = 1;
    private ExecutorService threadPool;

    boolean m_verbose;
    boolean pitEnabled;

    TransactionalTable ttable = null;

    private Map<String,String> preTagMap; //key is current restore tag, value is previous tag

    protected static boolean useFuzzyIBR = true;
    
    static final Log logger = LogFactory.getLog(BackupRestoreClient.class);

    static {
       String idtmTimeout = System.getenv("TM_IDTM_TIMEOUT");
       if (idtmTimeout != null){
         ID_TM_SERVER_TIMEOUT = Integer.parseInt(idtmTimeout.trim());
       }
       String useFuzzyIbrString = System.getenv("DTM_FUZZY_BACKUP");
       if (useFuzzyIbrString != null){
          useFuzzyIBR = (Integer.parseInt(useFuzzyIbrString.trim()) == 1) ? true : false;
       }
    }

    public BackupRestoreClient()
    {
        m_verbose = false;
        pitEnabled = false;
    }

    public BackupRestoreClient(Configuration conf) throws IOException
    {
      m_verbose = false;
      if (logger.isDebugEnabled())
          logger.debug("BackupRestoreClient.BackupRestoreClient(...) called.");
      config = conf;

      int retryCount = 0;
      boolean snapshotTableExists = false;
      while (retryCount < 3) {
         try{
            String hbaseRpcTimeout = System.getenv("HAX_BR_HBASE_RPC_TIMEOUT");
            int hbaseRpcTimeoutInt = 600000;
            if (hbaseRpcTimeout != null) {
                synchronized(adminConnLock) {
                    if (hbaseRpcTimeout != null) {
                        hbaseRpcTimeoutInt = Integer.parseInt(hbaseRpcTimeout.trim());
                    }
                } // sync
            }

            String value = config.getTrimmed("hbase.rpc.timeout");
            config.set("hbase.rpc.timeout", Integer.toString(hbaseRpcTimeoutInt));
            if (logger.isInfoEnabled())
                logger.info("HAX: BackupRestore HBASE RPC Timeout, revise hbase.rpc.timeout from "
                    + value + " to " + hbaseRpcTimeoutInt);

            connection = ConnectionFactory.createConnection(config);
            sm = new SnapshotMeta(conf);
            break;
         }
         catch(Exception ke){
            if (retryCount < 3){
               retryCount++;
               if (logger.isInfoEnabled()) logger.info("BackupRestoreClient... Exception in initialization, retrying ", ke);
               try {
                  Thread.sleep(1000); // sleep one seconds or until interrupted
               }
               catch (InterruptedException e) {
                  // ignore the interruption and keep going
               }
            }
            else{
               if (logger.isErrorEnabled()) logger.error("BackupRestoreClient... Exception in initialization ", ke);
               throw new IOException(ke);
            }
         }
      }

      retryCount = 0;
      boolean lobTableExists = false;
      while (retryCount < 3) {
         try{
            lm = new LobMeta(conf);
            break;
         }
         catch(Exception ke){
            if (retryCount < 3){
               retryCount++;
               if (logger.isInfoEnabled()) logger.info("BackupRestoreClient... Exception in initialization of LobMeta, retrying ", ke);
               try {
                  Thread.sleep(1000); // sleep one seconds or until interrupted
               }
               catch (InterruptedException e) {
                  // ignore the interruption and keep going
               }
            }
            else{
               if (logger.isErrorEnabled()) logger.error("BackupRestoreClient... Exception in initialization of LobMeta ", ke);
               throw new IOException(ke);
            }
         }
      }

      try {
         mm = new MutationMeta(conf);
      } catch (Exception e) {
         if (logger.isErrorEnabled()) logger.error("BackupRestoreClient... Exception creating MutationMeta object ", e);
         throw new IOException(e);
      }

      String envIdtmRetry = System.getenv("IDTM_RETRY_TIMES_IN_BR");
      int numRetries = (envIdtmRetry != null) ? Integer.parseInt(envIdtmRetry) : 60;

      timeId = new IdTmId();
      int retryCnt = 0;
      do
      {
        idServer = new IdTm(false);
        try {
          idServer.id(ID_TM_SERVER_TIMEOUT, timeId);
          break;
        } catch (IdTmException ide) {
          if (retryCnt++ < numRetries)
          {
            if (logger.isErrorEnabled())
              logger.error("idServer exception ", ide);
            retry(BRC_SLEEP);
            continue;
          }
          else
            throw new IOException(ide);
        }
      } while(true);

      timeIdVal = timeId.val;
      
      String pitRecov = System.getenv("TM_USE_PIT_RECOVERY");
      if (pitRecov != null) {
        pitEnabled = ( Integer.parseInt(pitRecov) > 0 );
      } else {
        pitEnabled = false;
      }

      // allocate ttable that will be used to update BREI progress table
      String tblName = new String("TRAF_RSRVD_2:TRAFODION._REPOS_.BREI_PROGRESS_STATUS_TABLE");
      ttable = new  TransactionalTable(tblName, connection);
      this.preTagMap = new HashMap<>();
    }

    public void setVerbose(boolean pv_verbose) {
      m_verbose = pv_verbose;
    }
    
    static void injectError(String errEnvvar) throws IOException {
      if (System.getenv(errEnvvar) != null) {
        if (logger.isErrorEnabled()) logger.error("Error injected, " + errEnvvar);
        throw new IOException("Error injected, " + errEnvvar );
      }
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
      if(logger.isDebugEnabled())
        logger.debug("retry retrySleep: " + retrySleep);
      
      return (retrySleep += BRC_SLEEP_INCR);
    }

    /**
     * ReplayEngineCallable  :  inner class for creating asynchronous requests
     */
    private abstract class BackupRestoreCallable implements Callable<Integer> {

        BackupRestoreCallable() {
        }

        public Integer doRestore(TableRecoveryGroup trg, long limitTimeStamp,
                                 boolean restoringFromSys,
                                 String backupTag) throws IOException {

            Admin admin = connection.getAdmin();
            String hdfsRoot = config.get("fs.default.name");
            Path hdfsRootPath = new Path(hdfsRoot);
            HdfsAdmin hdfsAdmin = new HdfsAdmin(hdfsRootPath.toUri(), config);
            List<LobMetaRecord> lobList = trg.getLobList();
            FileSystem destFs = FileSystem.get(hdfsRootPath.toUri(), config);
            RemoteIterator<LocatedFileStatus> bkpitr;
            String tableName = trg.getSnapshotRecord().getTableName();

            if (logger.isDebugEnabled()) logger.debug("ENTER doRestore backupTag " + backupTag
                    + " restoringFromSys " + restoringFromSys + " tableName " + tableName
                    + " limitTimeStamp " + limitTimeStamp + " trg " + trg);

            if (lobList.size() > 0) {
                if (logger.isDebugEnabled()) logger.debug("doRestore lobList.size() " + lobList.size() +
                        " backupTag " + backupTag + " tableName " + tableName);

                //Update hdfsUrl in the lob path if importing from remote location.
                if (restoringFromSys) {
                    for (LobMetaRecord lv_lobRec : lobList) {
                        String savedPath = lv_lobRec.getHdfsPath();
                        lv_lobRec.updateHdfsUrl(hdfsRoot);
                        if (logger.isInfoEnabled()) logger.info("doRestore " + " backupTag " + backupTag +
                                " updated lob path from: " + savedPath + " to: " + lv_lobRec.getHdfsPath());
                    }
                }

                Path lb_path = null;
                for (LobMetaRecord lv_lobRec : lobList) {
                    String lb_file = lv_lobRec.getHdfsPath();
                    try {
                        if (logger.isInfoEnabled())
                            logger.info("doRestore checking file existence for record " + lv_lobRec);
                        lb_path = new Path(lb_file);
                        bkpitr = destFs.listLocatedStatus(lb_path);
                        int j = 0;
                        while (bkpitr.hasNext()) {
                            j++;
                            bkpitr.next();
                        }
                        if (j > 0) {
                        } else {
                            throw new FileNotFoundException("doRestore restore directory exists but file not found ");
                        }
                    } catch (FileNotFoundException fnfe) {
                        if (logger.isErrorEnabled()) logger.error("Unable to locate directory " + lb_file
                                + ", will restore snapshot.  restoringFromSys " + restoringFromSys);

                        //Now restore the lob file.
                        String rootPathString = new String(lv_lobRec.getHdfsPath());
                        String sourcePathString;
                        if (restoringFromSys) {
                            sourcePathString = new String(hdfsRoot + "/user/trafodion/backupsys/"
                                    + backupTag + "/" + tableName.replace(":", "_") + "/" + lv_lobRec.getSnapshotName());
                        } else {
                            sourcePathString = new String(rootPathString + "/.snapshot/" + lv_lobRec.getSnapshotName());
                        }
                        String destPathString = new String(rootPathString);
                        final Path rootLobPath = new Path(rootPathString);
                        final Path sourceLobPath = new Path(sourcePathString);
                        final Path destLobPath = new Path(destPathString);
                        FileSystem fs = FileSystem.get(rootLobPath.toUri(), config);
                        ArrayList<Path> sourcePathList = new ArrayList<Path>();
                        FileStatus[] srcFileStatus = fs.listStatus(sourceLobPath);
                        Path[] srcPaths = FileUtil.stat2Paths(srcFileStatus);
                        for (Path path : srcPaths) {
                            sourcePathList.add(path);
                        }

                        try {
                            DistCpOptions distcpopts = new DistCpOptions(sourcePathList, destLobPath);
                            DistCp distcp = new DistCp(config, distcpopts);
                            distcp.execute();
                        } catch (Exception e) {
                            if (logger.isErrorEnabled()) logger.error("doRestore backupTag " +
                                    backupTag + " encountered exception :", e);
                            throw new IOException(e);
                        }
                    }
                }
            }

            try {
                long threadId = Thread.currentThread().getId();
                String snapshotPath = trg.getSnapshotRecord().getSnapshotPath();
                SnapshotMetaRecord tableMeta = trg.getSnapshotRecord();
                List<MutationMetaRecord> mutationList = trg.getMutationList();
                //if restoreToTS,
                //find the first less than limitTimeStamp brc snapshot meta record,use the snapshot path
                if(null != tableMeta.getBrcSnapshotList() && tableMeta.getBrcSnapshotList().size() > 0){
                    SnapshotMetaRecord brcRecord = null;
                    List<SnapshotMetaRecord> brcSnapshotList = tableMeta.getBrcSnapshotList();
                    Collections.sort(brcSnapshotList, new Comparator<SnapshotMetaRecord>() {
                        @Override
                        public int compare(SnapshotMetaRecord s1, SnapshotMetaRecord s2) {
                            return Long.compare(s2.getKey(), s1.getKey());
                        }
                    });
                    for (SnapshotMetaRecord iterBrcRecord : brcSnapshotList) {
                        if(iterBrcRecord.getKey() > tableMeta.getKey() && iterBrcRecord.getKey() <= limitTimeStamp){
                            brcRecord = iterBrcRecord;
                            break;
                        }
                    }
                    //if mutation LargestCommitId less than brc key,remove
                    if(null != brcRecord){
                        snapshotPath = brcRecord.getSnapshotPath();
                        Iterator<MutationMetaRecord> iterator = mutationList.iterator();
                        while (iterator.hasNext()){
                            if(iterator.next().getLargestCommitId() < brcRecord.getKey()){
                                iterator.remove();
                            }
                        }
                    }
                }
                if (logger.isInfoEnabled()) logger.info("ENTRY doRestore thread " + threadId + " backupTag " +
                        backupTag + " path " + snapshotPath);

                //admin.restoreSnapshot(snapshotPath);
                // if we use cloneSnapshot, we do not need to enable table
                //admin.cloneSnapshot(snapshotPath, TableName.valueOf(tableName));
                String snapTableName = snapshotPath + "," + tableName;
                admin.restoreSnapshot(snapTableName);

                // if we just disable table and does not delete table before we restore snapshot,
                // we should enable table the moment after we restore snapshot.
                if (admin.isTableDisabled(TableName.valueOf(tableName)))
                    admin.enableTable(TableName.valueOf(tableName));

                if (mutationList.size() > 0) {
                    //replay mutations.
                    if (logger.isInfoEnabled()) logger.info("doRestore mutation thread " + threadId + " backupTag " +
                            backupTag + " Calling ReplayEngine mutation size: " + mutationList.size());

                    //Update hdfsUrl in mutationPathString if importing from remote
                    //location.
                    if (restoringFromSys) {
                        for (MutationMetaRecord mmr : mutationList) {
                            String savedPath = mmr.getMutationPath();
                            mmr.updateHdfsUrl(hdfsRoot);
                            if (logger.isInfoEnabled()) logger.info("doRestore " + " backupTag " + backupTag +
                                    " updated mutationPath from: " + savedPath + " to: " + mmr.getMutationPath());
                        }
                    }

                    List<String> mutationPathList = new ArrayList<>(mutationList.size());
                    for (MutationMetaRecord metaRecord : mutationList) {
                        mutationPathList.add(metaRecord.getMutationPath());
                    }
                    List<LobMetaRecord> emptylobList = new ArrayList<LobMetaRecord>();
                    try {
                        new ReplayEngine(mutationList,
                                emptylobList,
                                trg.getSnapshotRecord().getTableName(),
                                limitTimeStamp,
                                /*numParallel*/pit_thread,
                                /*pv_peer_id*/ 0,
                                /*m_verbose*/ false);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                }

                if (logger.isInfoEnabled())
                    logger.info("EXIT doRestore thread " + threadId + " path " + snapshotPath);

            } finally {
                admin.close();
            }
            return 0;
        }

        public Integer doRestore(TableSet trg,
                                 String backupTag) throws IOException {

            Admin admin = connection.getAdmin();
            try {
                long threadId = Thread.currentThread().getId();
                String snapshotPath = trg.getSnapshotPath();
                if (logger.isInfoEnabled()) logger.info("ENTRY doRestore thread " + threadId + " backupTag " +
                        backupTag + " path " + snapshotPath);

                admin.restoreSnapshot(snapshotPath + "," + trg.getTableName());

                if (logger.isInfoEnabled())
                    logger.info("EXIT doRestore thread " + threadId + " path " + snapshotPath);

            } finally {
                admin.close();
            }
            return 0;
        }

        public Integer doRestore(String table, long limit) throws IOException {
            Admin admin = connection.getAdmin();

            SnapshotMeta lv_sm = null;
            MutationMeta lv_mm = null;
            ArrayList<MutationMetaRecord> mutationList = null;
            List<LobMetaRecord> emptylobList = new ArrayList<LobMetaRecord>();
            try {
                long threadId = Thread.currentThread().getId();
                lv_sm = new SnapshotMeta(config);
                lv_mm = new MutationMeta(config);
                SnapshotMetaRecord lv_smr = lv_sm.getCurrentSnapshotRecord(table);
                String snapshotPath = lv_smr.getSnapshotPath();
                if (logger.isInfoEnabled()) logger.info("ENTRY doRestore for crash recovery, thread " + threadId
                        + " table " + table + " path " + snapshotPath);

                if (logger.isInfoEnabled()) logger.info("doRestore disabling table " + table + ", thread " + threadId);
                try {
                    admin.disableTable(TableName.valueOf(table));
                } catch (TableNotFoundException tnfe) {
                    // Ignore
                }

                if (logger.isInfoEnabled()) logger.info("doRestore deleting table " + table + ", thread " + threadId);
                try {
                    admin.deleteTable(TableName.valueOf(table));
                } catch (TableNotFoundException tnfe) {
                    // Ignore
                }

                if (logger.isInfoEnabled()) logger.info("ENTRY doRestore for crash recovery, thread " + threadId
                        + " table " + table + " path " + snapshotPath);

                admin.restoreSnapshot(snapshotPath + "," + table);

                if (logger.isDebugEnabled())
                    logger.debug("doRestore for crash recovery restored snapshot thread " + threadId + " path " + snapshotPath);

                // Now play get and replay the mutations
                mutationList = lv_mm.getMutationsFromSnapshot(lv_smr.getKey(), -1L /* mutationStartTimeNano */, table
                        , false /* skip pending */, true, 0L, false);
                new ReplayEngine(mutationList,
                        emptylobList,
                        table,
                        limit,
                        /*numParallel*/pit_thread,
                        /*pv_peer_id*/ 0,
                        /*m_verbose*/ false);
                if (logger.isInfoEnabled())
                    logger.info("EXIT doRestore thread " + threadId + " path " + snapshotPath);
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                admin.close();
            }
            return 0;
        }

        public Integer doDelete(TableName tableName, String backupTag, Admin admin) throws IOException {
            try {
                long threadId = Thread.currentThread().getId();
                if (logger.isInfoEnabled()) logger.info("ENTRY doDelete thread " + threadId + " backupTag " +
                        backupTag + " TableName " + tableName.toString());

                if (admin.isTableEnabled(tableName))
                    admin.disableTable(tableName);
                // in fact we does not need to delete table while we restore snapshot,
                // disable this table is enough
                // admin.deleteTable(tableName);

                if (logger.isInfoEnabled())
                    logger.info("EXIT doDelete thread " + threadId + " TableName " + tableName.toString());
            } finally {

            }
            return 0;
        }

        public Integer doLobSnapshot(HdfsAdmin hdfsAdmin,
                                     FileSystem rootFs,
                                     String tableFileName,
                                     String sourceHdfsLoc,
                                     String snaphotName,
                                     String backuptag,
                                     boolean systemBackup)
                throws Exception {

            try {
                long threadId = Thread.currentThread().getId();
                if (logger.isInfoEnabled()) logger.info("ENTER doLobSnapshot backupTag " + backuptag +
                        " Thread ID " + threadId +
                        " TableName " + tableFileName +
                        " sourceHdfsLoc " + sourceHdfsLoc +
                        " snaphotName " + snaphotName +
                        " systemBackup " + systemBackup
                );

                boolean retry = true;
                int retryCount = 0;
                int retrySleep = BRC_SLEEP;
                boolean snapComplete = false;
                Path sourceHdfsPath = new Path(sourceHdfsLoc);

                //Wait for  completion.
                do {
                    try {
                        retryCount++;
                        rootFs.createSnapshot(sourceHdfsPath, snaphotName);
                        retry = false;
                        if (retryCount > 1) {
                            if (logger.isErrorEnabled()) logger.error("doLobSnapshot snapshot attempt " + retryCount
                                    + " successful on file: " + sourceHdfsPath);
                        }
                    } catch (FileNotFoundException fnfe) {
                        if (retryCount < BRC_RETRY_ATTEMPTS) {
                            retry = true;
                            if (logger.isErrorEnabled())
                                logger.error("doLobSnapshot retrying snapshot due to exception on file: "
                                        + sourceHdfsLoc + " snaphotName: " + snaphotName + " ", fnfe);
                        } else {
                            if (logger.isErrorEnabled())
                                logger.error("doLobSnapshot snapshot failed after " + BRC_RETRY_ATTEMPTS
                                        + " attempts rethrowing exception on file: "
                                        + sourceHdfsLoc + " snaphotName: " + snaphotName + " ", fnfe);
                            throw new IOException(fnfe.getMessage());
                        }
                    } catch (SnapshotException sse) {
                        if (retryCount < BRC_RETRY_ATTEMPTS) {
                            retry = true;
                            if (logger.isErrorEnabled())
                                logger.error("doLobSnapshot enabling snapshot and retrying due to exception on file: "
                                        + sourceHdfsLoc + " snaphotName: " + snaphotName + " ", sse);
                            try {
                                hdfsAdmin.allowSnapshot(sourceHdfsPath);
                            } catch (IOException ioe) {
                                if (logger.isErrorEnabled()) logger.error("doLobSnapshot sourceHdfsLoc "
                                        + sourceHdfsLoc + " not enabled for snapshot ", ioe);
                                throw ioe;
                            }
                        } else {
                            if (logger.isErrorEnabled())
                                logger.error("doLobSnapshot snapshot failed after " + BRC_RETRY_ATTEMPTS
                                        + " attempts rethrowing exception on file: "
                                        + sourceHdfsLoc + " snaphotName: " + snaphotName + " ", sse);
                            throw new IOException(sse.getMessage());
                        }
                    }

                    if (retry) {
                        retrySleep = retry(retrySleep);
                    }
                } while (retry && (retryCount <= BRC_RETRY_ATTEMPTS));

                if (logger.isDebugEnabled())
                    logger.debug("EXIT doLobSnapshot thread " + threadId + " file " + sourceHdfsLoc
                            + " snaphotName " + snaphotName);
            } catch (Exception e) {
                if (logger.isErrorEnabled()) logger.error("doLobSnapshot encountered exception on file: "
                        + sourceHdfsLoc + " snaphotName: " + snaphotName + " ", e);
                throw new IOException(e);
            } finally {
                //hdfsAdmin.close();
            }
            return 0;
        }

        public Integer doSnapshot(String hbaseTableName,
                                  String snapshotName,
                                  String snapshotPath,
                                  String backuptag,
                                  boolean incrementalSnapshot,
                                  boolean systemBackup,
                                  boolean sqlMetaTable,
                                  boolean userGenerated,
                                  Set<Long> lobSnapshots,
                                  boolean incrementalTable)
                throws Exception {

            Admin admin = connection.getAdmin();
            try {
                long threadId = Thread.currentThread().getId();
                if (logger.isInfoEnabled()) logger.info("ENTER doSnapshot backupTag " + backuptag +
                        " Thread ID " + threadId +
                        " TableName " + hbaseTableName +
                        " SnapshotPath " + snapshotPath +
                        " SnapshotName " + snapshotName +
                        " incrementalSnapshot " + incrementalSnapshot +
                        " systemBackup " + systemBackup +
                        " sqlMetaTable " + sqlMetaTable +
                        " userGenerated " + userGenerated +
                        " lobSnapshotSize " + lobSnapshots.size() +
                        " incrementalTable " + incrementalTable
                );

                if (!admin.tableExists(TableName.valueOf(hbaseTableName))) {
                    if (logger.isInfoEnabled()) logger.info("doSnapshot thread " + threadId + " backupTag " +
                            backuptag + " TableName " + hbaseTableName +
                            " Table does not exist");
                    admin.close();
                    return 0;
                }

                if (pitEnabled) {
                    //flush regions to mutation capture.
                    admin.flush(TableName.valueOf(hbaseTableName));
                }

                if (!systemBackup) {
                    long timeIdVal = getIdTmVal();
                    updateSnapshotMeta(timeIdVal, backuptag, incrementalSnapshot, userGenerated, incrementalTable,
                            sqlMetaTable, hbaseTableName, snapshotName, snapshotPath, lobSnapshots, "Default");
                }

                boolean retry = true;
                int retryCount = 0;
                int retrySleep = BRC_SLEEP;
                boolean snapComplete = false;

                //Wait for  completion.
                do {
                    try {
                        retryCount++;
                        admin.snapshot(snapshotPath, TableName.valueOf(hbaseTableName));
                        retry = false;
                        if (retryCount > 1) {
                            if (logger.isErrorEnabled()) logger.error("doSnapshot snapshot attempt " + retryCount
                                    + " successful on table: " + hbaseTableName);
                        }
                    } catch (FileNotFoundException fnfe) {
                        if (retryCount < BRC_RETRY_ATTEMPTS) {
                            retry = true;
                            if (logger.isErrorEnabled())
                                logger.error("doSnapshot retrying snapshot due to exception on table: "
                                        + hbaseTableName + " path: " + snapshotPath + " ", fnfe);
                        } else {
                            if (logger.isErrorEnabled())
                                logger.error("doSnapshot snapshot failed after " + BRC_RETRY_ATTEMPTS
                                        + " attempts rethrowing exception on table: "
                                        + hbaseTableName + " path: " + snapshotPath + " ", fnfe);
                            throw new IOException(fnfe.getMessage());
                        }
                    }
                    if (retry)
                        retrySleep = retry(retrySleep);

                } while (retry && (retryCount <= BRC_RETRY_ATTEMPTS));

                if (!incrementalSnapshot) {
                    // Mark all the 'PENDING' mutations for this table so they are
                    // superseded with this FULL backup
                    List<MutationMetaRecord> mutationList = mm.getMutationsFromUserTag(hbaseTableName, "PENDING");
                    ListIterator<MutationMetaRecord> mListIterator = mutationList.listIterator();
                    while (mListIterator.hasNext()) {
                        MutationMetaRecord mmRec = mListIterator.next();

                        // It's possible the mutation records have changed underneath us, so
                        // we must check the shadow table for the latest version of the record
                        MutationMetaRecord mmRecShadow = mm.getMutationRecord(mmRec.getRowKey(), true /* readShadow */);
                        if (mmRecShadow != null) {
                            mmRec = mmRecShadow;
                            if (logger.isDebugEnabled())
                                logger.debug("doSnapshot mmRec updated from shadow table " + mmRec);
                        }
                        mmRec.setUserTag("SUPERSEDED");
                        mmRec.setSupersedingFullSnapshot(timeIdVal);
                        if (logger.isDebugEnabled())
                            logger.debug("doSnapshot updated superseded mutation rec " + mmRec);
                        mm.putMutationRecord(mmRec, true /* blindWrite */);
                    }
                }

                if (logger.isInfoEnabled())
                    logger.info("EXIT doSnapshot thread " + threadId + " TableName " + hbaseTableName + " snapshotPath " + snapshotPath);
            } catch (Exception e) {
                if (logger.isErrorEnabled()) logger.error("doSnapshot encountered exception on table: "
                        + hbaseTableName + " path: " + snapshotPath + " ", e);
                throw new IOException(e);
            } finally {
                admin.close();
            }
            return 0;
        }

        public Integer doXDCBroadcast(int key, int value, int peer_id, String hbaseTableName)
                throws Exception {

            int lv_status = 0;
            boolean flag = (value == 0) ? false : true;

            if (key != GENERIC_RECOVERY_COMPLETE) { // not reocvery complete request
                try {
                    TransactionalTable tTable = new TransactionalTable(hbaseTableName, connection);
                    lv_status = tTable.broadcastRequest(key, flag);
                    tTable.close();
                    if (lv_status != 0) {
                        throw new IOException("BRC xdc request fails with status " + lv_status);
                    }
                } catch (Exception e) {
                    if (!(e instanceof TableNotFoundException)) {
                        if (logger.isErrorEnabled()) logger.error("do XDC Broadcast encountered exception on table: "
                                + hbaseTableName + " ", e);
                        throw new IOException(e);
                    } else {
                        if (logger.isWarnEnabled())
                            logger.warn("do XDC Broadcast ignoring TableNotFoundException on table: "
                                    + hbaseTableName);
                    }
                }
            }
            return lv_status;
        }

        public Integer doBroadcast(String hbaseTableName,
                                   String snapshotName,
                                   String snapshotPath,
                                   String backuptag,
                                   boolean incrementalSnapshot,
                                   boolean systemBackup,
                                   boolean sqlMetaTable,
                                   boolean userGenerated,
                                   Set<Long> lobSnapshots,
                                   boolean incrementalTable)
                throws Exception {

            Admin admin = connection.getAdmin();
            try {
                long threadId = Thread.currentThread().getId();
                if (logger.isInfoEnabled()) logger.info("ENTER doBroadcast backupTag " + backuptag +
                        " Thread ID " + threadId +
                        " TableName " + hbaseTableName +
                        " SnapshotPath " + snapshotPath +
                        " SnapshotName " + snapshotName +
                        " incrementalSnapshot " + incrementalSnapshot +
                        " systemBackup " + systemBackup +
                        " sqlMetaTable " + sqlMetaTable +
                        " userGenerated " + userGenerated +
                        " lobSnapshotSize " + lobSnapshots.size() +
                        " incrementalTable " + incrementalTable
                );

                if (!incrementalSnapshot)
                    throw new IOException("doBroadcast only applicable to incr backup & incr table combination");

                //broadcast to table here
                int GENERIC_FLUSH_MUTATION_FILES = 2; //Must be same as defined in TrxRegionEndPoint.java
                RMInterface rmiTable = new RMInterface(hbaseTableName, connection, false);
                rmiTable.broadcastRequest(GENERIC_FLUSH_MUTATION_FILES);

                //update snapshotMeta here.  This procedure creates a SnapshotMetaRecord or
                // SnapshotMetaIncrementalRecord internally
                long keyVal = getIdTmVal();
                updateSnapshotMeta(keyVal, backuptag, incrementalSnapshot, userGenerated /* userGenerated */,
                        incrementalTable,
                        sqlMetaTable /*sqlMetaData */, hbaseTableName, snapshotName, snapshotPath, lobSnapshots, "Default");

                // Mark all the 'PENDING' mutations for this table so they are
                // associated with this backup
                List<MutationMetaRecord> mutationList = mm.getAllMutationsFromUserTag(hbaseTableName, "PENDING");

                if (logger.isInfoEnabled())
                    logger.info("doBroadcast mutationList, size:" + mutationList.size());
                ListIterator<MutationMetaRecord> mListIterator = mutationList.listIterator();
                while (mListIterator.hasNext()) {
                    MutationMetaRecord mmRec = mListIterator.next();

                    // It's possible the mutation records have changed underneath us, so
                    // we must check the shadow table for the latest version of the record
                    MutationMetaRecord mmRecShadow = mm.getMutationRecord(mmRec.getRowKey(), true /* readShadow */);
                    if (mmRecShadow != null) {
                        mmRec = mmRecShadow;
                        mmRec.setUserTag(backuptag);
                        mmRec.setBackupSnapshot(keyVal);
                        mm.putShadowRecord(mmRec);
                        if (logger.isDebugEnabled()) {
                            logger.debug("doBroadcast mmRec updated from shadow table " + mmRec);
                        }
                    }

                    mmRec.setUserTag(backuptag);
                    mmRec.setBackupSnapshot(keyVal);
                    if (logger.isDebugEnabled()) {
                        logger.debug("doBroadcast updated mutation file for tag " + backuptag + " " + mmRec);
                    }
                    mm.putMutationRecord(mmRec, true /* blindWrite */);
                }

                if (logger.isDebugEnabled())
                    logger.debug("EXIT doBroadcast thread " + threadId + " TableName " + hbaseTableName + " snapshotPath " + snapshotPath);
            } catch (Exception e) {
                if (logger.isErrorEnabled()) logger.error("doBroadcast encountered exception on table: "
                        + hbaseTableName + " ", e);
                throw new IOException(e);
            } finally {
                admin.close();
            }
            return 0;
        }

        public Integer broadcastHiatus(String hbaseTableName) throws Exception {

            //Admin admin = connection.getAdmin();
            try {
                long threadId = Thread.currentThread().getId();
                if (logger.isInfoEnabled()) logger.info("ENTER broadcastHiatus TableName: " + hbaseTableName);

                //broadcast to table here
                int GENERIC_FLUSH_MUTATION_FILES = 2; //Must be same as defined in TrxRegionEndPoint.java
                //RMInterface rmiTable = new RMInterface(hbaseTableName, connection, false);
                //rmiTable.broadcastRequest(GENERIC_FLUSH_MUTATION_FILES);
                TransactionalTable tTable = new TransactionalTable(hbaseTableName, connection);
                tTable.broadcastRequest(GENERIC_FLUSH_MUTATION_FILES, false);
                tTable.close();

                if (logger.isDebugEnabled())
                    logger.debug("EXIT broadcastHiatus thread " + threadId
                            + " TableName " + hbaseTableName);
            } catch (Exception e) {
                if (logger.isErrorEnabled()) logger.error("broadcastHiatus encountered exception on table: "
                        + hbaseTableName + " ", e);
                throw new IOException(e);
            } finally {
                //admin.close();
            }
            return 0;
        }

        // For mutations and lobs of table, we just need to use DistCp
        public Integer distCpObjects(List<Path> sourceObjects,
                                     String destObjectsPath,
                                     int mappers, boolean override) throws Exception {
            final Path destPath = new Path(destObjectsPath);
            DistCpOptions distcpOpts = new DistCpOptions(sourceObjects, destPath);
            distcpOpts.setMaxMaps(mappers);
            distcpOpts.setOverwrite(override);
            String yarnQueueName = System.getenv(MS_ENV_BR_EXPORT_QUEUE_NAME);
            yarnQueueName = AbstractSnapshotRecordMeta.isEmpty(yarnQueueName) ? BR_EXPORT_DEFAULT_QUEUE_NAME : yarnQueueName;
            config.set("mapreduce.job.queuename", yarnQueueName);
            DistCp distcp = new DistCp(config, distcpOpts);
            distcp.execute();
            return 0;
        }

        // For snapshots we should use an improved version of
        // org.apache.hadoop.hbase.snapshot.EsgynExportSnapshot, which
        // supports batch processing of snapshots
        public Integer doExportSnapshots(String snapshots,
                                         String destSnapshotPath,
                                         int mappers, boolean override)
                throws Exception {
            long start = System.currentTimeMillis();
            if (logger.isInfoEnabled()) {
                logger.info("ENTER doExportSnapshots Snapshots: " + snapshots +
                        " destSnapshotPath: " + destSnapshotPath + " mappers: " +
                        mappers + " override:" + override);
            }

            try {
                ProcessBuilder pb;
                // get fullpath of hbase exe.
                String hbaseExe = System.getenv("HBASE_HOME");
                if (hbaseExe == null) {
                    hbaseExe = "hbase";
                } else {
                    hbaseExe = hbaseExe + "/" + "bin" + "/" + "hbase";
                }
                String yarnQueueName = System.getenv(MS_ENV_BR_EXPORT_QUEUE_NAME);
                yarnQueueName = AbstractSnapshotRecordMeta.isEmpty(yarnQueueName) ? BR_EXPORT_DEFAULT_QUEUE_NAME : yarnQueueName;
                config.set("mapreduce.job.queuename", yarnQueueName);
                // use list to ProcessBuilder
                List<String> argsList = new ArrayList<>(Arrays.asList(hbaseExe,
                        "org.apache.hadoop.hbase.snapshot.EsgynExportSnapshot",
                        "-Dmapreduce.job.queuename=" + yarnQueueName,
                        "-snapshot", snapshots,
                        "-copy-to", destSnapshotPath,
                        "-chmod", "755",
                        "-mappers", String.valueOf(mappers)));
                if (override) {
                    argsList.add("-overwrite");
                }
                if (logger.isInfoEnabled()) {
                    logger.info("ENTER doExportSnapshots sh: " + argsList);
                }
                pb = new ProcessBuilder(argsList);

                //redirect child process error output to std output.
                pb.redirectErrorStream(true);
                Process process = pb.start();

                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

                StringBuilder builder = new StringBuilder();
                String line = null;
                while ((line = reader.readLine()) != null) {
                    builder.append(line);
                    builder.append(System.getProperty("line.separator"));
                }

                //Wait for child completion.
                int rc = process.waitFor();
                String rcStr = builder.toString();

                if (logger.isDebugEnabled())
                    logger.debug("doExportSnapshots rc:" + rc + " exportLog: " + rcStr);

                if ((rc != 0) || rcStr.contains("ERROR") || rcStr.contains("FATAL")) {
                    if (logger.isErrorEnabled())
                        logger.error("doExportSnapshots FAILED with error:" + rc + " Error Detail: " + rcStr);
                    throw new IOException("doExportSnapshots FAILED with error:" + rc + " Error Detail: " + rcStr);
                }
            } catch (Exception e) {
                if (logger.isErrorEnabled())
                    logger.error("doExportSnapshots encountered exception: ", e);
                throw new IOException(e);
            } finally {
                long end = System.currentTimeMillis();
                if (logger.isInfoEnabled())
                    logger.info("doExportSnapshots total Elapsed Time: " + (end - start) + " ms.");
            }
            return 0;
        }

        public Integer doExport(String backupTag,
                                String dest,
                                String snapshotPath,
                                String snapshotName,
                                String destSnapshotPath,
                                List<MutationMetaRecord> mutationList,
                                String destMutationPath,
                                List<LobMetaRecord> lobList,
                                ArrayList<String> destLobPaths,
                                boolean skipExportSnapshot
        ) throws Exception {

            long threadId = Thread.currentThread().getId();
            try {

                if (logger.isInfoEnabled()) logger.info("ENTER doExport backupTag " + backupTag +
                        " Thread ID " + threadId + " Destination " + dest +
                        " SnapshotTableName " + snapshotName +
                        " SnapshotName " + snapshotPath +
                        " Destination Snapshot path " + destSnapshotPath +
                        " MutationList size " + mutationList.size() +
                        " Destination Mutation path " + destMutationPath +
                        " LobList size " + lobList.size() +
                        " Destination Lob paths " + destLobPaths +
                        " skipExportSnapshot " + skipExportSnapshot
                );

                String destDirectory = destSnapshotPath;
                destDirectory = destDirectory.substring(0, destSnapshotPath.lastIndexOf('/'));

                if (!skipExportSnapshot) {
                    String hadoopType = System.getenv("HADOOP_TYPE");
                    ProcessBuilder pb;
                    String hbaseExe;
                    if (hadoopType != null) {
                        //if null, it is generally cluster env, use default hbase exe
                        hbaseExe = "hbase";
                    } else {
                        //if null, then work station env.
                        hbaseExe = System.getenv("HBASE_HOME");
                        hbaseExe = hbaseExe + "/" + "bin" + "/" + "hbase";
                    }

                    String mappers = System.getenv("EXPORT_IMPORT_MAPPERS");

                    pb = new ProcessBuilder(hbaseExe,
                            "org.apache.hadoop.hbase.snapshot.ExportSnapshot",
                            "-snapshot", snapshotPath,
                            "-copy-to", destSnapshotPath,
                            "-chmod", "755",
                            "-mappers", (mappers != null) ? mappers : "16");

                    //redirect child process error output to std output.
                    pb.redirectErrorStream(true);
                    Process process = pb.start();

                    BufferedReader reader =
                            new BufferedReader(new InputStreamReader(process.getInputStream()));

                    StringBuilder builder = new StringBuilder();
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        builder.append(line);
                        builder.append(System.getProperty("line.separator"));
                    }

                    //Wait for child completion.
                    int rc = process.waitFor();
                    String rcStr = builder.toString();

                    if (logger.isDebugEnabled())
                        logger.debug("doExport Thread ID " + threadId + " backupTag " + backupTag +
                                " rc:" + rc + " exportLog: " + rcStr);

                    if ((rc != 0) || rcStr.contains("ERROR") || rcStr.contains("FATAL")) {
                        if (logger.isErrorEnabled())
                            logger.error("doExport thread " + threadId + " backupTag " + backupTag +
                                    " FAILED with error:" + rc + " Error Detail: " + rcStr);
                        throw new IOException("doExport thread " + threadId + " FAILED with error:" + rc + " Error Detail: " + rcStr);
                    }
                }

                ArrayList<Path> sourcePathList = new ArrayList<Path>();
                for (int i = 0; i < mutationList.size(); i++) {
                    MutationMetaRecord mutationRecord = mutationList.get(i);
                    String mutationPathString = mutationRecord.getMutationPath();
                    if (logger.isInfoEnabled()) logger.info("doExport mutationPathString " + mutationPathString);
                    Path mutationPath = new Path(mutationPathString);
                    sourcePathList.add(mutationPath);
                }
                if (sourcePathList.size() > 0) {
                    final Path destMutationPathPath = new Path(destMutationPath);
                    if (logger.isInfoEnabled())
                        logger.info("doExport mutation Thread ID " + threadId + " backupTag " + backupTag +
                                " TableName " + snapshotName
                                + " sourcePathList: " + sourcePathList + " Destination : " + destMutationPathPath);
                    DistCpOptions distcpopts = new DistCpOptions(sourcePathList, destMutationPathPath);
                    DistCp distcp = new DistCp(config, distcpopts);
                    distcp.execute();
                }

                sourcePathList = new ArrayList<Path>();
                for (int i = 0; i < lobList.size(); i++) {
                    LobMetaRecord lobRecord = lobList.get(i);
                    String lobPathString = lobRecord.getHdfsPath() + "/.snapshot/" + lobRecord.getSnapshotName();
                    Path lobPath = new Path(lobPathString);
                    if (logger.isInfoEnabled())
                        logger.info("doExport lob file Thread ID " + threadId + " backupTag " + backupTag +
                                " TableName " + snapshotName
                                + " lobPath: " + lobPathString);
                    sourcePathList.add(lobPath);
                }
                if (sourcePathList.size() > 0) {
                    final Path destLobPath = new Path(dest);
                    if (logger.isInfoEnabled())
                        logger.info("doExport mutation Thread ID " + threadId + " backupTag " + backupTag +
                                " TableName " + snapshotName
                                + " sourcePathList: " + sourcePathList + " Destination : " + dest);
                    DistCpOptions distcpopts = new DistCpOptions(sourcePathList, destLobPath);
                    DistCp distcp = new DistCp(config, distcpopts);
                    distcp.execute();
                }

            } catch (Exception e) {
                if (logger.isErrorEnabled()) logger.error("doExport thread " + threadId + " backupTag " +
                        backupTag + " encountered exception :", e);
                throw new IOException(e);
            } finally {
                if (logger.isDebugEnabled())
                    logger.debug("EXIT doExport thread " + threadId + " backupTag " +
                            backupTag + " TableName " + snapshotName);
            }
            return 0;
        }

        // For snapshots we should use an improved version of
        // org.apache.hadoop.hbase.snapshot.EsgynImportSnapshot, which
        // supports batch processing of snapshots
        public Integer doImportSnapshots(String snapshots,
                                         String fullPathSnaps,
                                         String destPath,
                                         boolean override,
                                         int mappers) throws Exception {
            long start = System.currentTimeMillis();
            if (logger.isInfoEnabled()) {
                logger.info("ENTER doImportSnapshots snapshots: " + snapshots +
                        " destPath: " + destPath +
                        " fullPathSnaps: " + fullPathSnaps +
                        " override: " + override +
                        " mappers: " + mappers);
            }
            try {
                String hadoopType = System.getenv("HADOOP_TYPE");
                String hbaseUser = System.getenv("HBASE_USER");
                if (hbaseUser == null)
                    hbaseUser = "hbase";

                // get fullpath of hbase exe.
                String hbaseExe = System.getenv("HBASE_HOME");
                if (hbaseExe == null)
                    hbaseExe = "hbase";
                else
                    hbaseExe = hbaseExe + "/" + "bin" + "/" + "hbase";

                // get queue name from ms.env
                String yarnQueueName = System.getenv(MS_ENV_BR_IMPORT_QUEUE_NAME);
                yarnQueueName = AbstractSnapshotRecordMeta.isEmpty(yarnQueueName) ? BR_IMPORT_DEFAULT_QUEUE_NAME : yarnQueueName;
                List<String> argsList = new ArrayList<String>(Arrays.asList(hbaseExe,
                        "org.apache.hadoop.hbase.snapshot.EsgynImportSnapshot",
                        "-Dmapreduce.job.queuename=" + yarnQueueName,
                        "-snapshot", snapshots,
                        "-copy-to", destPath,
                        "-copy-from", fullPathSnaps,
                        "-mappers", String.valueOf(mappers)));

                if (hadoopType != null)
                    argsList.addAll(0, Arrays.asList("sudo", "-u", hbaseUser));

                if (override)
                    argsList.add("-overwrite");

                if (logger.isInfoEnabled()) {
                    logger.info("ENTER doImportSnapshots args: " + argsList);
                }

                ProcessBuilder pb = new ProcessBuilder(argsList);

                //redirect child process error output to stdout.
                pb.redirectErrorStream(true);
                Process process = pb.start();

                //getInputStream reads stdout pipe.
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(process.getInputStream()));

                StringBuilder builder = new StringBuilder();
                String line = null;
                while ((line = reader.readLine()) != null) {
                    builder.append(line);
                    builder.append(System.getProperty("line.separator"));
                }

                //Wait for child completion.
                int rc = process.waitFor();
                String rcStr = builder.toString();
                if (logger.isDebugEnabled())
                    logger.debug("doImportSnapshots rc:" + rc + " importLog: " + rcStr);

                if ((rc != 0) || rcStr.contains("ERROR") || rcStr.contains("FATAL")) {
                    if (logger.isErrorEnabled())
                        logger.error("doImportSnapshots FAILED with error:" + rc + " Error Detail: " + rcStr);
                    throw new Exception("doImportSnapshots FAILED with error:" + rc + " Error Detail: " + rcStr);
                }
            } catch (Exception e) {
                if (logger.isErrorEnabled())
                    logger.error("doImportSnapshots encountered exception: ", e);
                throw new IOException(e);
            } finally {
                long end = System.currentTimeMillis();
                if (logger.isInfoEnabled())
                    logger.info("doImportSnapshots total Elapsed Time: " + (end - start) + " ms.");
            }
            return 0;
        }

        public Integer doImport(String backupTag,
                                boolean incrementalBackup,
                                String srcRoot,
                                TableSet tableset,
                                boolean override,
                                int mappers) throws Exception {
            long threadId = Thread.currentThread().getId();
            try {
                if (logger.isInfoEnabled()) {
                    logger.info("ENTER doImport backupTag " + backupTag +
                            " ThreadID " + threadId +
                            " srcRoot " + srcRoot +
                            " TableSet " + tableset.displayString()
                    );
                }

                //mutation import
                if (logger.isInfoEnabled()) logger.info("doImport mutation Thread ID " + threadId + " backupTag " +
                        backupTag + " Mutation size: " + tableset.getMutationSize() + " lob size: " + tableset.getLobSnapshotNameListSize());

                ArrayList<Path> sourcePathList = new ArrayList<Path>();
                if (tableset.getMutationSize() > 0) {
                    ArrayList<String> mutationlist = tableset.getMutationList();
                    for (int i = 0; i < mutationlist.size(); i++) {
                        String mutation = mutationlist.get(i);
                        //String mutationPathString = srcRoot + "/" + tableset.getTableName().replace(":", "_") + "/" + mutation;
                        String mutationPathString = srcRoot + "/.incrments/" + mutation;
                        if (logger.isInfoEnabled())
                            logger.info("doImport mutation " + mutation + " mutationPathString " + mutationPathString);
                        Path mutationPath = new Path(mutationPathString);
                        sourcePathList.add(mutationPath);
                    }
                }
                if (sourcePathList.size() > 0) {
                    String hdfsRoot = config.get("fs.default.name");
                    String PITRoot = hdfsRoot + "/user/trafodion/PIT/" + tableset.getTableName().replace(":", "_") + "/cdc/";

                    Path destRootPath = new Path(PITRoot);

                    if (logger.isInfoEnabled()) logger.info("doImport mutation Thread ID " + threadId + " backupTag " +
                            backupTag + " sourcePathList: " + sourcePathList +
                            " Destination : " + destRootPath);
                    DistCpOptions distcpopts = new DistCpOptions(sourcePathList, destRootPath);
                    distcpopts.setMaxMaps(mappers);
                    if (override)
                        distcpopts.setOverwrite(true);
                    String yarnQueueName = System.getenv(MS_ENV_BR_IMPORT_QUEUE_NAME);
                    yarnQueueName = AbstractSnapshotRecordMeta.isEmpty(yarnQueueName) ? BR_IMPORT_DEFAULT_QUEUE_NAME : yarnQueueName;
                    config.set("mapreduce.job.queuename", yarnQueueName);
                    DistCp distcp = new DistCp(config, distcpopts);
                    distcp.execute();
                }

                sourcePathList = new ArrayList<Path>();
                if (tableset.getLobSnapshotNameListSize() > 0) {
                    ArrayList<String> lobNameList = tableset.getLobSnapshotNames();
                    ArrayList<String> lobPathList = tableset.getLobSnapshotPaths();
                    for (int i = 0; i < lobNameList.size(); i++) {
                        String lobName = lobNameList.get(i);
                        String lobPathString = srcRoot + "/" + backupTag + "/" + lobName;
                        if (logger.isInfoEnabled())
                            logger.info("doImport lob for table " + tableset.getTableName() + " lobPathString " + lobPathString);
                        Path lobPath = new Path(lobPathString);
                        sourcePathList.add(lobPath);
                    }
                }

                if (sourcePathList.size() > 0) {
                    String hdfsRoot = config.get("fs.default.name");
                    String PITRoot = hdfsRoot + "/user/trafodion/backupsys/"
                            + backupTag + "/" + tableset.getTableName().replace(":", "_");

                    Path destRootPath = new Path(PITRoot);

                    if (logger.isInfoEnabled()) logger.info("doImport lob Thread ID " + threadId + " backupTag " +
                            backupTag + " sourcePathList: " + sourcePathList +
                            " Destination : " + destRootPath);
                    DistCpOptions distcpopts = new DistCpOptions(sourcePathList, destRootPath);
                    distcpopts.setMaxMaps(mappers);
                    String yarnQueueName = System.getenv(MS_ENV_BR_IMPORT_QUEUE_NAME);
                    yarnQueueName = AbstractSnapshotRecordMeta.isEmpty(yarnQueueName) ? BR_IMPORT_DEFAULT_QUEUE_NAME : yarnQueueName;
                    config.set("mapreduce.job.queuename", yarnQueueName);
                    DistCp distcp = new DistCp(config, distcpopts);
                    if (override)
                        distcpopts.setOverwrite(true);
                    distcp.execute();
                }
            } catch (Exception e) {
                if (logger.isErrorEnabled())
                    logger.error("doImport thread " + threadId + " encountered exception :", e);
                throw new IOException(e);
            } finally {
                if (logger.isDebugEnabled())
                    logger.debug("EXIT doImport thread " + threadId + " TableName " + tableset.getTableName());
            }
            return 0;
        }

        public Integer doDeleteBackup(TableSet t) throws IOException {

            Admin admin = connection.getAdmin();
            try {
                long threadId = Thread.currentThread().getId();
                if (logger.isInfoEnabled())
                    logger.info("ENTRY doDeleteBackup TableSet thread " + threadId + " TableSet " + t.displayString());

                try {
                    String snapshotPath = t.getSnapshotPath();
                    admin.deleteSnapshot(snapshotPath);
                } catch (SnapshotDoesNotExistException se) {
                    if (logger.isInfoEnabled()) logger.info("doDeleteBackup snapshot does not exist, ignoring");
                }

                //Now delete the mutations associated with this tableSet.
                String hdfsRoot = config.get("fs.default.name");
                Path hdfsRootPath = new Path(hdfsRoot);
                String PITRoot = hdfsRoot + "/user/trafodion/PIT/" + t.getTableName().replace(":", "_") + "/cdc/";
                FileSystem fs = FileSystem.get(hdfsRootPath.toUri(), config);

                ArrayList<String> mutationList = t.getMutationList();
                for (String mutation : mutationList) {
                    String mutationPathString = PITRoot + mutation;
                    Path mutationPath = new Path(mutationPathString);
                    if (logger.isDebugEnabled())
                        logger.debug("doDeleteBackup deleting mutation file at " + mutationPath);

                    if (fs.exists(mutationPath))
                        fs.delete(mutationPath, false);
                    else
                        if (logger.isWarnEnabled()) logger.warn("mutation file: " + mutationPath + " not exists.");
                }
                if (logger.isDebugEnabled())
                    logger.debug("EXIT doDeleteBackup thread " + threadId);
            } finally {
                admin.close();
            }
            return 0;
        }

    } //callable

    private int sudoImportStatus(final String snapshotName,
                                  final String tag) throws IOException {

      if (logger.isInfoEnabled()) logger.info("Enter sudoImportStatus " +
                  " snapshotName :" + snapshotName +
                  " tag :" + tag );

      Batch.Call<TrxRegionService, ImportStatusResponse> callable =
        new Batch.Call<TrxRegionService, ImportStatusResponse>() {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ImportStatusResponse> rpcCallback =
          new BlockingRpcCallback<ImportStatusResponse>();
        
        @Override
        public ImportStatusResponse call(TrxRegionService instance) throws IOException {
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportStatusRequest.Builder builder =
        ImportStatusRequest.newBuilder();
        
        builder.setSnapshotName(ByteString.copyFromUtf8(snapshotName));
        builder.setTag(ByteString.copyFromUtf8(tag));
        
        instance.importStatus(controller, builder.build(), rpcCallback);
          return rpcCallback.get();
        }
        };
        
        int rc = -1;
        Map<byte[],ImportStatusResponse> resultmap = null;
        TransactionalTable ttable = null;
        try {
          ttable = new TransactionalTable(Bytes.toBytes("TRAF_RSRVD_1:TRAFODION._MD_.VERSIONS"), connection);
          resultmap = ttable.coprocessorService(TrxRegionService.class,
              HConstants.EMPTY_START_ROW,
              null,
              callable);

          if(resultmap == null) {
            throw new IOException("sudoImportStatus result is null");
          }

          for (ImportStatusResponse result : resultmap.values()){
            if (result.getHasException()){
              throw new IOException(result.getException());
            }
            rc = result.getResult();
            if (rc == -1) {
              throw new IOException("sudoImportStatus result is : " + rc);
            } else {
              return rc;  // rc is either 0(complete) or 1(waiting completion) 
            }
          }   
        } catch (IOException e) {
          if (logger.isErrorEnabled()) logger.error("ERROR while calling sudoImportStatus ", e);
          throw new IOException("ERROR while calling sudoImportStatus ", e);
        } catch (ServiceException s) {
          throw new IOException("ERROR while calling sudoImportStatus ", s);
        } catch (Throwable t) {
          throw new IOException("ERROR while calling sudoImportStatus " , t);
        } finally {
          ttable.close();
        }

        return rc;
    }
    
    private void sudoImport(final String snapshotName,
                              final String tag,
                              final String copyTo,
                              final String copyFrom,
                              final String mappers) throws IOException {
      
      if (logger.isInfoEnabled()) logger.info("Enter sudoImport " +
               " snapshotName :" + snapshotName +
               " tag :" + tag +
               " copyto : " + copyTo +
               " copyFrom : " + copyFrom +
               " mappers : " + mappers);

      Batch.Call<TrxRegionService, ImportLaunchResponse> callable =
      new Batch.Call<TrxRegionService, ImportLaunchResponse>() {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ImportLaunchResponse> rpcCallback =
        new BlockingRpcCallback<ImportLaunchResponse>();

        @Override
        public ImportLaunchResponse call(TrxRegionService instance) throws IOException {
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportLaunchRequest.Builder builder =
          ImportLaunchRequest.newBuilder();
        
        builder.setSnapshotName(ByteString.copyFromUtf8(snapshotName));
        builder.setCopyTo(ByteString.copyFromUtf8(copyTo));
        builder.setCopyFrom(ByteString.copyFromUtf8(copyFrom));
        builder.setMappers(ByteString.copyFromUtf8(mappers));
        builder.setTag(ByteString.copyFromUtf8(tag));
        
        instance.importLaunch(controller, builder.build(), rpcCallback);
        return rpcCallback.get();
       }
      };

      Map<byte[],ImportLaunchResponse> resultmap = null;
      TransactionalTable ttable = null;
      try {
        ttable = new TransactionalTable(Bytes.toBytes("TRAF_RSRVD_1:TRAFODION._MD_.VERSIONS"), connection);
        resultmap = ttable.coprocessorService(TrxRegionService.class,
                    HConstants.EMPTY_START_ROW,
                    null,
                    callable);

        if(resultmap == null) {
          throw new IOException("sudoImport result is null");
        }
        
        for (ImportLaunchResponse result : resultmap.values()){
          if (result.getHasException()){
            throw new IOException(result.getException());
          }
          if (result.getResult() != 0) {
            throw new IOException("sudoImport result is : " + result.getResult());
          }
          if (logger.isInfoEnabled()) logger.info("sudoImport completed success tag :" + tag + " snapshotName :" + snapshotName);
        }   
      } catch (IOException e) {
        if (logger.isErrorEnabled()) logger.error("ERROR while calling sudoImport ", e);
        throw new IOException("ERROR while calling sudoImport ", e);
      } catch (ServiceException s) {
        throw new IOException("ERROR while calling sudoImport ", s);
      } catch (Throwable t) {
        throw new IOException("ERROR while calling sudoImport " , t);
      } finally {
        ttable.close();
      }
      
      
      //Now check status for completion.
      boolean retry = false;
      int retryCount = 0;
      int retrySleep = BRC_SLEEP;
      int rc = 0;
      
      //Wait for  completion.
      do {
        rc = sudoImportStatus(snapshotName, tag);
        if(rc == 0) {
          retry = false;
        }
        else if (rc == 1) {
          if(retryCount == BRC_RETRY_ATTEMPTS) {
            //lock held by different tag
            throw new IOException("sudoImport unable to complete import in given time");
          }
          retry = true;
        }
        else {
          throw new IOException("sudoImport error.");
        }
        if (retry) 
          retrySleep = retry(retrySleep);
      }while (retry && retryCount++ <= BRC_RETRY_ATTEMPTS);
    }      

    public void operationLock(String tag) throws Exception {
      
      if (logger.isInfoEnabled()) logger.info("ENTRY operationLock tag: " + tag);
      Admin admin = connection.getAdmin();
      String snapshotMeta = "SNAPSHOT_META__" + tag.replace(":", "_");
      String mutationMeta = "MUTATION_META__" + tag.replace(":", "_");
      boolean smlock = false;
      boolean snapSnapshotMeta = false;
      boolean snapMutationMeta = false;
      SnapshotMetaLockRecord record = null;
      boolean retry = false;
      int retryCount = 0;
      int retrySleep = BRC_SLEEP;
      
      try {
        
        do {
          //first check if locked
          record = sm.getLockRecord();
          if (record == null){
            //no lock
            break;
          }
          else if (!record.getLockHeld()){
            //no lock;
            break;
          }
          else if(record.getUserTag().equals(tag)) {
            //lock held by same tag.
            //reentrant possible.
            return;
          }
          else {
            //lock held by someone else.
            if(retryCount == BRC_RETRY_ATTEMPTS) {
              //lock held by different tag
              throw new IOException("operationLock failed. Lock held by tag: " +
                                     record.getUserTag());
            }
            retry = true;
          }
          if (retry) 
            retrySleep = retry(retrySleep);
        }while (retry && retryCount++ <= BRC_RETRY_ATTEMPTS);
        
        sm.lock(tag);
        
        if (logger.isInfoEnabled()) logger.info("operationLock success tag: " + tag);
        
        //Now proceed further.
        admin.snapshot(snapshotMeta, TableName.valueOf(SNAPSHOT_TABLE_NAME));
        snapSnapshotMeta = true;
        
        admin.snapshot(mutationMeta, TableName.valueOf(MUTATION_TABLE_NAME));
        snapMutationMeta = true;
        
      }catch(IOException e) {
         if (logger.isErrorEnabled()) logger.error("operationLock exception", e);
         
         try {
           //Depending on where the exception got raised, reverse the 
           //operations.
           if(snapMutationMeta){
              try {
                 admin.deleteSnapshot(mutationMeta);
              }
              catch(Exception se){
                 if (logger.isErrorEnabled()) logger.error("operationLock ignoring exception deleting mutation meta snapshot ", se);
              }
           }
           if(snapSnapshotMeta){
              try {
                 admin.deleteSnapshot(snapshotMeta);
              }
              catch(Exception se){
                 if (logger.isErrorEnabled()) logger.error("operationLock ignoring exception deleting snapshot meta snapshot ", se);
              }
           }
           if(smlock){
              try {
                 sm.unlock(tag);
              }
              catch(Exception ue){
                 if (logger.isErrorEnabled()) logger.error("operationLock attempting lock cleanup for tag " + tag);
                 sm.cleanupLock(tag);
              }
           }
         }catch (IOException ie) {
           if (logger.isErrorEnabled()) logger.error("operationLock cleanup exception", ie);
           e.addSuppressed(ie);
         }
         throw new IOException(e);
      }finally {
        admin.close();
      }
      if (logger.isInfoEnabled()) logger.info("EXIT operationLock tag: " + tag);
    }

    public boolean xdcLock() throws Exception {

      String tag = "XDC_PEER_UP";
      boolean smlock = false;
      SnapshotMetaLockRecord record = null;
      boolean retry = false;
      int retryCount = 0;
      int retrySleep = BRC_SLEEP;
      if (logger.isInfoEnabled()) logger.info("ENTRY xdcLock tag: " + tag);

      try {

         do {
            retry = false;

            //first check if locked
            record = sm.getLockRecord();
            if (record == null){
               //no lock
               break;
            }
            else if (!record.getLockHeld()){
               //no lock;
               break;
            }
            else if(record.getUserTag().equals(tag)) {
               //lock held by same tag.
               //reentrant possible.
               return true;
            }
            else {
               //lock held by someone else.
               if(retryCount % 20 == 0) {
                  //lock held by different tag
                  if (logger.isInfoEnabled()) logger.info("xdcLock lock held by existing Backup/Restore operation for tag: " + record.getUserTag() + "; waiting");
               }
               retry = true;
            }
            if (retry){
               retry(BRC_SLEEP); // sleep the default amount without increasing
               retryCount++;
            }
         }while (retry);

         sm.lock(tag);

         if (logger.isInfoEnabled()) logger.info("xdcLock success tag: " + tag);

      }catch(IOException e) {
         if (logger.isErrorEnabled()) logger.error("xdcLock exception", e);

         try {
            //Depending on where the exception got raised, reverse the
            //operations.
            if(smlock){
               try {
                  sm.unlock(tag);
               }
               catch(Exception ue){
                  if (logger.isErrorEnabled()) logger.error("xdcLock attempting lock cleanup for tag " + tag);
                  sm.cleanupLock(tag);
               }
            }
         }catch (IOException ie) {
            if (logger.isErrorEnabled()) logger.error("xdcLock cleanup exception", ie);
            e.addSuppressed(ie);
         }
         throw new IOException(e);
      }finally {
      }
      if (logger.isInfoEnabled()) logger.info("EXIT xdcLock tag: " + tag);
      return smlock;
    }

    public void operationUnlock(String tag, boolean recoverMeta) throws Exception {
        operationUnlock(tag, recoverMeta, false);
    }

    public void operationUnlock(String tag, boolean recoverMeta, boolean cleanup) throws Exception {
      if (cleanup)
          cleanuplock(tag);
          
      if (logger.isInfoEnabled()) logger.info("ENTRY operationUnlock tag: " + tag + " recoverMeta: " + recoverMeta);
      Admin admin = connection.getAdmin();
      String snapshotMeta = "SNAPSHOT_META__" + tag.replace(":", "_");
      String mutationMeta = "MUTATION_META__" + tag.replace(":", "_");
      TableName snapshotTable = TableName.valueOf(SNAPSHOT_TABLE_NAME);
      TableName mutationTable = TableName.valueOf(MUTATION_TABLE_NAME);

      try {
        
        //first check if I am the current holder
        //of the lock. 
        SnapshotMetaLockRecord record = sm.getLockRecord();
        if (record == null){
          //if(logger.isDebugEnabled())
          if (logger.isInfoEnabled()) logger.info("operationUnlock record not found. tag: " + tag);
          return;
        }
        else if (!record.getLockHeld()){
          //if(logger.isDebugEnabled())
          if (logger.isInfoEnabled()) logger.info("operationUnlock SnapshotMeta already unlocked. tag: " + tag);
          return;
        }
        else if(!record.getUserTag().equals(tag)) {
          throw new IOException("operationUnlock called by wrong tag: " + tag +
                                " lock held by tag: " + record.getUserTag());
        }
        
        //Reaching here means, lock is valid.
        if (recoverMeta) {
          String uid = tag +"_"+ System.currentTimeMillis();
          if (logger.isInfoEnabled())
              logger.info("operationUnlock disable snapshot " + uid);
          if(admin.isTableEnabled(snapshotTable)) {
            admin.disableTable(snapshotTable);
          } else {
            //Throw this exception since something is wrong here.
            //These tables should never be in disabled state.
            throw new IOException("SNAPSHOT table already disabled! Tag:" +
                                   tag);
          }
          if (logger.isInfoEnabled())
              logger.info("operationUnlock restoreSnapshot snapshot " + uid);
          admin.restoreSnapshot(snapshotMeta + "," + SNAPSHOT_TABLE_NAME);
          if (logger.isInfoEnabled())
              logger.info("operationUnlock enableTable snapshot " + uid);
          enableTable(snapshotTable, admin);
          if (logger.isInfoEnabled())
              logger.info("operationUnlock disableTable mutation " + uid);
          if (admin.isTableEnabled(mutationTable)) {
            admin.disableTable(mutationTable);
          } else {
            //Throw this exception since something is wrong here.
            //These tables should never be in disabled state.
            throw new IOException("MUTATION table already disabled! Tag:" +
                                  tag);
          }
          admin.restoreSnapshot(mutationMeta + "," + MUTATION_TABLE_NAME);
          enableTable(mutationTable, admin);
          if (logger.isInfoEnabled())
              logger.info("operationUnlock enableTable mutation-end " + uid);
        }
        try{
           sm.unlock(tag);
        }
        catch (Exception e){
           sm.cleanupLock(tag);
        }

        if (logger.isInfoEnabled()) logger.info("operationUnlock success tag: " + tag);

        //now delete the snapshots
        admin.deleteSnapshot(snapshotMeta);
        admin.deleteSnapshot(mutationMeta);

      }catch(IOException e) {
        if (logger.isErrorEnabled()) logger.error("operationUnlock exception", e);
        throw new IOException(e);
      }finally {
          if(recoverMeta){
              enableTable(snapshotTable, admin);
              enableTable(mutationTable, admin);
          }
        admin.close();
      }
      if (logger.isInfoEnabled()) logger.info("EXIT operationUnlock tag: " + tag);
    }

    //enable table with retry
    private void enableTable(TableName tableName, Admin admin) throws IOException {
        int retryCnt = 0;
        do {
            retryCnt++;
            try {
                if (!admin.isTableEnabled(tableName)) {
                    admin.enableTable(tableName);
                    break;
                } else {
                    break;
                }
            } catch (Exception e) {
                retry(BRC_SLEEP);
            }
        } while (retryCnt < 10);
        if (retryCnt == 10) {
            //Throw this exception since something is wrong here.
            //These tables should never be in disabled state.
            throw new IOException("operationUnlock " + tableName + " is not enabled ");
        }
    }

    public void cleanuplock(String tag) throws Exception {
       if (logger.isInfoEnabled()) logger.info("ENTRY cleanuplock tag: " + tag);

       Admin admin = connection.getAdmin();
       String snapshotMeta = "SNAPSHOT_META__" + tag.replace(":", "_");
       String mutationMeta = "MUTATION_META__" + tag.replace(":", "_");

       try {

         //first check if I am the current holder of the lock.
         SnapshotMetaLockRecord record = sm.getLockRecord();
         if (record == null){
            if (logger.isInfoEnabled()) logger.info("cleanuplock record not found. tag: " + tag);
         }
         else if (!record.getLockHeld()){
            if (logger.isInfoEnabled()) logger.info("cleanuplock SnapshotMeta already unlocked. tag: " + tag);
         }
         else if(!record.getUserTag().equals(tag)) {
             throw new IOException("cleanuplock called by wrong tag: " + tag +
                               " lock held by tag: " + record.getUserTag());
         }

         if(admin.isTableEnabled(TableName.valueOf(SNAPSHOT_TABLE_NAME))) {
            admin.disableTable(TableName.valueOf(SNAPSHOT_TABLE_NAME));
         } else {
            if (logger.isInfoEnabled()) logger.info("SNAPSHOT table already disabled! Tag:" + tag);
         }
         try{
            admin.restoreSnapshot(snapshotMeta + "," + SNAPSHOT_TABLE_NAME);
            admin.enableTable(TableName.valueOf(SNAPSHOT_TABLE_NAME));
         }
         catch (Exception e){
            if (logger.isInfoEnabled()) logger.info("cleanuplock ignoring exception restoring SnapshotMeta for tag: " + tag + " ", e);
            admin.enableTable(TableName.valueOf(SNAPSHOT_TABLE_NAME));
         }
         int retryCnt = 0;
         boolean retry = false;
         do {
            retry = false;
            if (!admin.isTableEnabled(TableName.valueOf(SNAPSHOT_TABLE_NAME))) {
               retry = true;
               retryCnt++;
            }
            if (retry)
               retry(BRC_SLEEP);
         } while (retry && (retryCnt < 10));
         if (retryCnt == 10){
             //Throw this exception since something is wrong here.
             //These tables should never be in disabled state.
             throw new IOException("cleanuplock " + SNAPSHOT_TABLE_NAME + " is not enabled ");
          }

         if (admin.isTableEnabled(TableName.valueOf(MUTATION_TABLE_NAME))) {
            admin.disableTable(TableName.valueOf(MUTATION_TABLE_NAME));
         } else {
            if (logger.isInfoEnabled()) logger.info("MUTATION table already disabled! Tag:" + tag);
         }
         try{
            admin.restoreSnapshot(mutationMeta + "," + MUTATION_TABLE_NAME);
            admin.enableTable(TableName.valueOf(MUTATION_TABLE_NAME));
         }
         catch (Exception e){
            if (logger.isInfoEnabled()) logger.info("cleanuplock ignoring exception restoring MutationMeta for tag: " + tag + " ", e);
            admin.enableTable(TableName.valueOf(MUTATION_TABLE_NAME));
         }
         retryCnt = 0;
         do {
            retry = false;
            if (!admin.isTableEnabled(TableName.valueOf(MUTATION_TABLE_NAME))) {
              retry = true;
              retryCnt++;
            }
            if (retry)
              retry(BRC_SLEEP);
         } while (retry && (retryCnt < 10));
         if (retryCnt == 10){
            //Throw this exception since something is wrong here.
            //These tables should never be in disabled state.
            throw new IOException("cleanuplock " + MUTATION_TABLE_NAME + " is not enabled ");
         }

         sm.cleanupLock(tag);

         try {
            admin.deleteSnapshot(mutationMeta);
         }
         catch(Exception se){
            if (logger.isErrorEnabled()) logger.error("cleanuplock ignoring exception deleting mutation meta snapshot ", se);
         }

         try {
            admin.deleteSnapshot(snapshotMeta);
         }
         catch(Exception se){
            if (logger.isErrorEnabled()) logger.error("cleanuplock ignoring exception deleting snapshot meta snapshot ", se);
         }

       }catch(IOException e) {
          if (logger.isErrorEnabled()) logger.error("cleanuplock exception", e);
          throw new IOException(e);
       }finally {
       }
       if (logger.isInfoEnabled()) logger.info("EXIT cleanuplock tag: " + tag);
    }

    public boolean isTagExistInMeta(String tag) throws Exception {

      try {
        SnapshotMetaStartRecord smsr = sm.getSnapshotStartRecord(tag);
        if(smsr != null) {
          return true; //tag exists.
        }else {
          return false;//tag does not exist.
        }
      }catch (Exception e) {
        String exceptionString = ExceptionUtils.getStackTrace(e);
        if (exceptionString.contains("Record not found")) {
          return false; //tag does not exist.
        }
        else { 
         throw new Exception(e);
        }
      }
    }

    public boolean isTagExistBackupSys(String tag) throws Exception {

      String hdfsRoot = config.get("fs.default.name");
      String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";

      //check if backupMeta exists.
      Path destRootPath = new Path(backupsysRoot);
      FileSystem destFs = FileSystem.get(destRootPath.toUri(), config);
      RemoteIterator<LocatedFileStatus> bkpitr;
      try{
         bkpitr = destFs.listLocatedStatus(destRootPath);
      }
      catch(FileNotFoundException fnfe){
         if (logger.isErrorEnabled()) logger.error("Unable to locate file " + destRootPath + " ", fnfe);
         throw fnfe;
      }
      
      while (bkpitr.hasNext()) {
        //Note folder name is tag name.
        LocatedFileStatus bkpFolder = bkpitr.next();
        String bkpFolderName = bkpFolder.getPath().getName();
        String bkpTag = bkpFolderName.substring(bkpFolderName.lastIndexOf("/") + 1);
        
        if (bkpFolder.isDirectory() && bkpTag.equals(tag))
          return true;  //tag exists
      }
      
      return false; //tag does not exist.
    }

    private ArrayList<BackupSet> getBackupListFromBackupSys(boolean includeTableSet) throws Exception {

      if (logger.isInfoEnabled()) logger.info("ArrayList<BackupSet> [getBackupFromBackupSys()] includeTableSet " + includeTableSet);
      String hdfsRoot = config.get("fs.default.name");
      String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";

      Path backupsysRootPath = new Path(backupsysRoot);
      FileSystem backupsysRootFs = FileSystem.get(backupsysRootPath.toUri(), config);
      RemoteIterator<LocatedFileStatus> bkpitr = backupsysRootFs.listLocatedStatus(backupsysRootPath); 
      
      ArrayList<BackupSet> backupList = new ArrayList<BackupSet>();
      while (bkpitr.hasNext()) {
        //Note folder name is tag name.
        LocatedFileStatus bkpFolder = bkpitr.next();
        String bkpFolderName = bkpFolder.getPath().getName();

        Path backupMetaPath = new Path(backupsysRoot + "/" + bkpFolderName, "backupMeta.bin");
        if(! backupsysRootFs.exists(backupMetaPath)) {
          //do not throw exception. Import may be in progress.
          //Just ignore.
          //throw new IOException("Backup Meta " + bkpFolderName + " does not exist.");
          continue;
        }

        //Read in backupMeta.bin
        BackupSet bkpSet = null;
        try {
          FSDataInputStream bkpmetaIs = backupsysRootFs.open(backupMetaPath);
          ObjectInputStream ois = new ObjectInputStream(bkpmetaIs);
          bkpSet = (BackupSet)ois.readObject();
          ois.close();
          bkpmetaIs.close();
        } catch (IOException e) {
          String exceptionString = ExceptionUtils.getStackTrace(e);
          if (! exceptionString.contains("EOFException")) {
             //any exception other than EOFException throw exception.
            throw new IOException(e);
          }
          //ignore error since import may be in progress.
          if (logger.isWarnEnabled())
              logger.warn("getBackupListFromBackupSys Backup Name: " + bkpFolderName +
                      "encountered exception : ", e);
          continue;
        }

        if (logger.isInfoEnabled()) logger.info("getBackupListFromBackupSys backupTag: " + bkpSet.getBackupTag() +
                    " Backup Type: " + bkpSet.getBackupType() +
                    " Validity: " + (bkpSet.isValid() ? "VALID" : "INVALID"));

        if (! includeTableSet){
           // TableList is not needed for this operation and consumes memory
           bkpSet.resetTableList();
        }
        backupList.add(bkpSet);
      }

      return backupList;
    }

    static public String getPriorTagFromBackupSys(long timeId) throws Exception {
       long start = System.currentTimeMillis();
       if (logger.isInfoEnabled()) logger.info("getPriorTagFromBackupSys timeId " + timeId);
       String hdfsRoot = config.get("fs.default.name");
       String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";
       String returnTag = null;

       Path backupsysRootPath = new Path(backupsysRoot);
       FileSystem backupsysRootFs = FileSystem.get(backupsysRootPath.toUri(), config);
       RemoteIterator<LocatedFileStatus> bkpitr = backupsysRootFs.listLocatedStatus(backupsysRootPath);

       ArrayList<RecoveryRecord> recordList = new ArrayList<>();
       while (bkpitr.hasNext()) {
          //Note folder name is tag name.
          LocatedFileStatus bkpFolder = bkpitr.next();
          String bkpFolderName = bkpFolder.getPath().getName();

            Path backupMetaPath = new Path(backupsysRoot + "/" + bkpFolderName, "recoveryRecord.bin");
            if (!backupsysRootFs.exists(backupMetaPath)) {
                //do not throw exception. Import may be in progress.
                //Just ignore.
                continue;
            }

            //Read in RecoveryRecord.bin
            try {
                FSDataInputStream bkpmetaIs = backupsysRootFs.open(backupMetaPath);
                ObjectInputStream ois = new ObjectInputStream(bkpmetaIs);
                RecoveryRecord rr = (RecoveryRecord)ois.readObject();
                recordList.add(rr);
                ois.close();
                bkpmetaIs.close();
            } catch (IOException e) {
                String exceptionString = ExceptionUtils.getStackTrace(e);

                if (!exceptionString.contains("EOFException")) {
                    //any exception other than EOFException throw exception.
                    throw new IOException(e);
                }
                //ignore error since import may be in progress.
                if (logger.isWarnEnabled())
                    logger.warn("getPriorTagFromBackupSys Backup Name: " + bkpFolderName +
                        "encountered exception : ", e);
                continue;
            }
        }

        Collections.sort(recordList, new Comparator<RecoveryRecord>() {
            @Override
            public int compare(RecoveryRecord o1, RecoveryRecord o2) {
                return o1.getSnapshotStartRecord().getKey() > o2.getSnapshotStartRecord().getKey() ? 1 : -1;
            }
        });

        //first set return tag is before tag,if find after start key break
        for (RecoveryRecord rr : recordList) {
            long startKey = rr.getSnapshotStartRecord().getKey();
            if (startKey < timeId){
                returnTag = rr.getTag();
            }else if (startKey >= timeId){
                returnTag = rr.getTag();
                break;
            }
        }

        if (returnTag == null) {
            if (logger.isErrorEnabled())
                logger.error("Exception in getPriorTagFromBackupSys(timeId).  Record " + timeId + " not found");
            throw new Exception("Exception in getPriorTagFromBackupSys(timeId).  Record " + timeId + " not found");
        }
        long end = System.currentTimeMillis();
        if (logger.isInfoEnabled()) logger.info("getPriorTagFromBackupSys returning backupTag: " + returnTag + " " +
                "total Elapsed time  is "+(end - start));

        return returnTag;
    }

    private String getExtAttrsFromBackupSys(String backupTag) throws Exception {

       if (logger.isInfoEnabled()) logger.info(" BackupSet [getExtAttrsFromBackupSys] backupTag " + backupTag);
       BackupSet bkpset = null;
       String attrs = "";
       String hdfsRoot = config.get("fs.default.name");
       String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";

       Path backupsysRootPath = new Path(backupsysRoot);
       FileSystem backupsysRootFs = FileSystem.get(backupsysRootPath.toUri(), config);
       Path backupMetaPath = new Path(backupsysRoot + "/" + backupTag, "backupMeta.bin");
       if(! backupsysRootFs.exists(backupMetaPath)) {
          return attrs; // Empty
       }

       //Read in backupMeta.bin
       try{
          FSDataInputStream bkpmetaIs = backupsysRootFs.open(backupMetaPath);
          ObjectInputStream ois = new ObjectInputStream(bkpmetaIs);
          bkpset = (BackupSet)ois.readObject();
          ois.close();
          bkpmetaIs.close();
          attrs = bkpset.getExtAttrs();
       } catch (Exception e){
          String exceptionString = ExceptionUtils.getStackTrace(e);
          if (logger.isErrorEnabled()) logger.error("getExtAttrsFromBackupSys exception " + exceptionString);
       }

       return attrs;
   }

    private BackupSet getBackupFromBackupSys(String backupTag)
                                            throws Exception {

      if (logger.isInfoEnabled()) logger.info(" BackupSet [getBackupFromBackupSys] backupTag " + backupTag);
      BackupSet bkpset = null;
      String hdfsRoot = config.get("fs.default.name");
      String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";

      Path backupsysRootPath = new Path(backupsysRoot);
      FileSystem backupsysRootFs = FileSystem.get(backupsysRootPath.toUri(), config);
      Path backupMetaPath = new Path(backupsysRoot + "/" + backupTag, "backupMeta.bin");
      if(! backupsysRootFs.exists(backupMetaPath)) {
        return bkpset; //null;
      }
      
      //Read in backupMeta.bin
      FSDataInputStream bkpmetaIs = backupsysRootFs.open(backupMetaPath);
      ObjectInputStream ois = new ObjectInputStream(bkpmetaIs);
      bkpset = (BackupSet)ois.readObject();
      ois.close();
      bkpmetaIs.close();
      
      return bkpset;
    }
    
    private void deleteBackupFromBackupSys(String backupTag) throws Exception {
      if (logger.isInfoEnabled()) logger.info("[deleteBackupFromBackupSys] backupTag " + backupTag);

      ExecutorService tPool = Executors.newFixedThreadPool(pit_thread);
      
      try {
        String hdfsRoot = config.get("fs.default.name");
        String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";
        
        Path backupsysRootPath = new Path(backupsysRoot);
        FileSystem backupsysRootFs = FileSystem.get(backupsysRootPath.toUri(), config);
        Path backupMetaPath = new Path(backupsysRoot + "/" + backupTag, "backupMeta.bin");
        if(! backupsysRootFs.exists(backupMetaPath)) {
          throw new IOException("Backup Meta for" + backupTag + " does not exist.");
        }
        
        //Read in backupMeta.bin
        FSDataInputStream bkpmetaIs = backupsysRootFs.open(backupMetaPath);
        ObjectInputStream ois = new ObjectInputStream(bkpmetaIs);
        BackupSet bkpset = (BackupSet)ois.readObject();
        ois.close();
        bkpmetaIs.close();
        
        //Iterate through backupMeta
        ArrayList<TableSet>  tableSetList = bkpset.getTableList();
        
        //determine threads
        Admin admin = connection.getAdmin();
        ClusterStatus cstatus = admin.getClusterStatus();
        this.pit_thread = cstatus.getServersSize();
        admin.close();
  
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tPool);
        
        int loopCount = 0;
        //Iterate through tableSet.
        for (final TableSet t : tableSetList) {
          compPool.submit(new BackupRestoreCallable() {
            public Integer call() throws Exception {
            return doDeleteBackup(t);
              }
            });
          loopCount++;
         } //for
        
        // simply to make sure they all complete, no return codes necessary at the moment
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
          int returnValue = compPool.take().get(); 
        }
        
        //now delete the folder in backupsys directory
        Path backupPath = new Path(backupsysRoot + "/" + backupTag);

        backupsysRootFs.delete(backupPath, true);
      } finally {
        if(tPool != null)
          tPool.shutdownNow();
      }
      return;
    }

    //////////////////////////////////////////////////////////////////////
    // tables:     list of fully qualified tables(namespace:cat.sch.tab)
    //             to be backed up.
    // backuptag:  backup tag
    //
    //////////////////////////////////////////////////////////////////////
    public boolean systemBackup(Object[] tables,
                                Object[] hdfsDirs,
                                final String backuptag,
                                final String extAttr,
                                int numParallelThreads)
        throws  MasterNotRunningException,
                IOException,
                Exception {

      if (logger.isInfoEnabled()) logger.info("[SYSTEMBACKUP] backupTag " + backuptag +
                  " tables :" + tables.length +
                  " extAttr :" + extAttr +
                  " threads:" + numParallelThreads);
      
      //First check if this tag already exists and error accordingly.
      
      String hdfsRoot = config.get("fs.default.name");
      String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";

      //check if backupMeta exists.
      Path destRootPath = new Path(backupsysRoot);
      Path hdfsRootPath = new Path(hdfsRoot);
      FileSystem destFs = FileSystem.get(destRootPath.toUri(), config);
      String destBackupTag = backupsysRoot + "/" + backuptag;
      Path backupMetafile = new Path(destBackupTag, "backupMeta.bin");
      Path lobPath = new Path("/user/trafodion/lobs");
      final HdfsAdmin hdfsAdmin = new HdfsAdmin ( hdfsRootPath.toUri(), config);
      final FileSystem hdfsRootFs = FileSystem.get(hdfsRootPath.toUri(), config);
      if(destFs.exists(backupMetafile)) {
        throw new IOException("Backup tag " + backuptag + " already exists.");
      }

      //if numParallelThreads is zero, default it to the number of 
      //region servers in the cluster.
      if(numParallelThreads <= 0)
      {
        Admin admin = connection.getAdmin();
        ClusterStatus cstatus = admin.getClusterStatus();
        numParallelThreads = cstatus.getServersSize();
        admin.close();
      }
      this.pit_thread = numParallelThreads;
      
      //setup thread pool.
      ExecutorService tPool = Executors.newFixedThreadPool(pit_thread == 0 ? 1 : pit_thread);
      CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tPool);

      try {
        // For each table, generate snapshot name.
        // snapshotName is internal name for reference( mostly not needed anymore ?)
        // snapshotPath is actual name of snapshot.
        int loopCount = 0;
        long startId = timeIdVal = getIdTmVal();
        ArrayList<TableSet>  tableSetList = new ArrayList<TableSet>();
        for (int i = 0; i < tables.length; i++) {
          final String hbaseTableName = (String) tables[i];
  
          // snapshot name cannot contain ":".
          // If the src name is prefixed by "namespace:", replace
          // ":" by "_"
          final String snapshotName = hbaseTableName.replace(":", "_") + 
                                      "_SNAPSHOT_" + 
                                      backuptag +
                                      "_" +
                                      String.valueOf(startId);
          
          final Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());

          // Each table may have n LOB columns.  Each LOB column is an hdfs file that
          // requires a snapshot because we can't capture the changes in mutation files
          StringTokenizer st = new StringTokenizer((String) hdfsDirs[i], ",");
//          StringTokenizer st = new StringTokenizer("", ",");
          int numLocs = 0;
          if (st.hasMoreTokens()){
             // Need to make sure the /user/trafodion/lobs directory allows snapshots
             try{
                hdfsAdmin.allowSnapshot(lobPath);
             }
             catch(IOException ioe){
                if (logger.isErrorEnabled()) logger.error("[SYSTEMBACKUP] lobPath "
                              + lobPath + " not enabled for snapshot ", ioe);
                throw ioe;
             }
          }
          while (st.hasMoreTokens()){
             numLocs++;
             final String tmpHdfsDir = st.nextToken().trim();
             final String srcHdfsDir = hdfsRoot + tmpHdfsDir;
             Path srcHdfsDirPath = new Path(srcHdfsDir);

             if(! hdfsRootFs.exists(srcHdfsDirPath)) {
                if (logger.isErrorEnabled()) logger.error("[SYSTEMBACKUP] source hdfs path "
                       + srcHdfsDirPath + " does not exist ");

                continue;
             }

             final String lobSnapshotName = LobMetaRecord.createPath(srcHdfsDir +
                                "_LOB_" + backuptag + "_" + String.valueOf(startId));

             long key = getIdTmVal();
             String lobSnapshotPath = srcHdfsDir;

             updateLobMeta(key, lm.getMetaVersion(), hbaseTableName, lobSnapshotName,
                   -1L /* fileSize */, backuptag, tmpHdfsDir, "dummy",
                   true /* inLocalFS */, false /* archived */, "null" /* snapshotArchivePath */);

             // Keep track of the lobs so we can add the set to the snapshot record for the table
             lobSnapshots.add(key);

             compPool.submit(new BackupRestoreCallable() {
                 public Integer call() throws Exception {
                     return doLobSnapshot(  hdfsAdmin,
                             hdfsRootFs,
                             hbaseTableName,
                             tmpHdfsDir,
                             lobSnapshotName,
                             backuptag,
                             false /* systemBackup */);

                 }
             });

          }
          if (logger.isDebugEnabled())
              logger.debug("[SYSTEMBACKUP] doLobSnapshot sent " + numLocs + " times");

          // simply to make sure they all complete, no return codes necessary at the moment
          for (int k = 0; k < numLocs; k++) {
             int returnValue = compPool.take().get();
          }

          //generate a unique name for snapshot
          final String snapshotPath = SnapshotMetaRecord.createPath(snapshotName) + "._SYS_";
          TableSet ts = new TableSet(hbaseTableName,
                                     snapshotName,
                                     snapshotPath);
          tableSetList.add(ts);
          if (logger.isInfoEnabled()) logger.info("systemBackup backupTag " + backuptag + " Table: " +
                      ts.displayString());
          
          compPool.submit(new BackupRestoreCallable() {
            public Integer call() throws Exception {
            return doSnapshot(  hbaseTableName,
                                snapshotName,
                                snapshotPath,
                                backuptag,
                                false, /* incrementalSnapshot */
                                true,  /* systemBackup */
                                false, /* sqlMetaData */
                                true,  /* user generated */
                                lobSnapshots,
                                true); /* incrementalTable */   // TODO need to pass value in
              }
            });
          loopCount++;
        }// for
        if(logger.isDebugEnabled())
          logger.debug("systemBackup doSnapshot sent " + loopCount + " times");
        // simply to make sure they all complete, no return codes necessary at the moment
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
          int returnValue = compPool.take().get(); 
        }
        
        //Now update the system backup location with this meta information.
        FSDataOutputStream bkmetaos = destFs.create(backupMetafile);
        timeIdVal = getIdTmVal();
        byte [] asciiTime = new byte[40];
        int timeout = ID_TM_SERVER_TIMEOUT;
        boolean cb = false;
        IdTm cli = new IdTm(cb);
        cli.idToStr(timeout, timeIdVal, asciiTime);
        String timeStamp = Bytes.toString(asciiTime);
        
        //cut off timestamp milli seconds before display.
        timeStamp = timeStamp.substring(0, 19);
        
        // replace space delimiter between date and time with colon (:)
        timeStamp = timeStamp.replace(' ', ':');

        BackupSet bkpSet = new BackupSet( backuptag,
                                          tableSetList,
                                          false,    /*incremental*/
                                          true,     /*sys backup */
                                          true,     /*validity*/
                                          timeStamp,/*timestamp*/
                                          extAttr,  /* extendedAttributes */
                                          false);   /*imported*/

        
        ObjectOutputStream oos = new ObjectOutputStream(bkmetaos);
        oos.writeObject(bkpSet);
        oos.close();
        bkmetaos.close();

      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("systemBackup exception : ", e);
        throw new Exception(e);
      } finally {
        if(tPool != null)
          tPool.shutdownNow();
      }
      return true;
    }

    public byte[][] systemRestore(final String backuptag, 
                                  boolean showObjects,
                                  int numParallelThreads) throws Exception {

      if (logger.isInfoEnabled()) logger.info("[SYSTEMRESTORE]: " +
                  "backupTag " + backuptag +
                  "showObjects " + showObjects +
                  "numThreads " + numParallelThreads);

      //check if backupMeta exists.
      String hdfsRoot = config.get("fs.default.name");
      String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";
      Path backupsysRootPath = new Path(backupsysRoot);
      FileSystem backupsysRootFs = FileSystem.get(backupsysRootPath.toUri(), config);
      String destBackupTag = backupsysRoot + "/" + backuptag;
      Path backupMetafilePath = new Path(destBackupTag, "backupMeta.bin");
      if(! backupsysRootFs.exists(backupMetafilePath)) {
        throw new IOException("Backup tag " + backuptag + " does not exist.");
      }
      
      //Read in backupMeta.bin
      FSDataInputStream bkpmetaIs = backupsysRootFs.open(backupMetafilePath);
      ObjectInputStream ois = new ObjectInputStream(bkpmetaIs);
      BackupSet bkpSet = (BackupSet)ois.readObject();
      ois.close();
      bkpmetaIs.close();
      
      
      
      //check if backupMeta.bin is valid.
      if(!bkpSet.isValidVersion())
        throw new IOException("Backup tag:" + backuptag + 
                              " Version:" + bkpSet.getVersion() +
                              " is not compatible.");

      if(!bkpSet.isSysBackup())
        throw new IOException("Backup tag " + backuptag + " is not a System backup.");
      
      
      //now get the table list and proceed to restore.
      ArrayList<TableSet>  tableSetList = bkpSet.getTableList();
      Collections.sort(tableSetList, new TableSetCompare());
      
      byte[][] restoredObjList = null;
      int restoredEntry = 0;
      
      if(showObjects) {
        //populate restoreObjList and return
        restoredObjList = new byte[tableSetList.size()][];
        for (final TableSet t : tableSetList) {
          restoredObjList[restoredEntry] = t.getTableName().getBytes();
          restoredEntry++;
        } //for
        return restoredObjList;
      }
      
      try {
        restoredObjList = new byte[tableSetList.size()][];
        restoredEntry = 0;
        for (TableSet t : tableSetList) {
          String hbaseTableName = t.getTableName();
          restoredObjList[restoredEntry] = hbaseTableName.getBytes();
          restoredEntry++;

          if (logger.isInfoEnabled()) logger.info("systemRestore backupTag " + backuptag +
                      " Table:" + t.displayString());
        }


        if(tableSetList.size() == 0)
          return null;

        //if numParallelThreads is zero, default it to the number of 
        //region servers in the cluster.
        if(numParallelThreads <= 0)
        {
          Admin admin = connection.getAdmin();
          ClusterStatus cstatus = admin.getClusterStatus();
          numParallelThreads = cstatus.getServersSize();
          admin.close();
        }
        this.pit_thread = numParallelThreads;

        if (logger.isInfoEnabled())
            logger.info("systemRestore broadcast mutation flush before restore ");
        xdcBroadcastTableSet(GENERIC_FLUSH_MUTATION_FILES, 0, 0 /* local peer */, tableSetList);

        threadPool = Executors.newFixedThreadPool(pit_thread == 0 ? 1 : pit_thread);
        int loopCount = 0;
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);
       
        // For each table, in the list restore snapshot
        for (final TableSet t : tableSetList) {
         // Send work to thread
         compPool.submit(new BackupRestoreCallable() {
                 public Integer call() throws Exception {
                     return doRestore(t, backuptag);
                 }
             });
         loopCount++;
        } // for-loop
           
        // simply to make sure they all complete, no return codes necessary at the moment
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
          int returnValue = compPool.take().get(); 
        }

      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("Restore exception in system restore : ", e);
        throw new IOException(e);
      }
      finally{
       if(threadPool != null)
         threadPool.shutdownNow();
      }
       
      return restoredObjList;
    }

   public boolean validateBackupObjects(Object[] tables,
                                        Object[] incrBackupEnabled,
                                        final String backuptag, 
                                        String backupType)
       throws  MasterNotRunningException,
               IOException,
               Exception,
               SnapshotCreationException,
               InterruptedException {

	   if (logger.isInfoEnabled()) logger.info("[validateBackupObjects] ENTRY backuptag "
	         + (backuptag == null ? "null" : backuptag));
       //initial checks
       if(isTagExistInMeta(backuptag) || isTagExistBackupSys(backuptag)) {
           throw new IOException("Backup tag " + backuptag + " already exists.");
       }
       
       boolean incremental_bkup = backupType.equals("INCREMENTAL");
       //Initial check
       if(incremental_bkup) {
           if(incrBackupEnabled == null )
               throw new IOException("incremental_bkup but incrBackupEnabled is null");
           
           if(tables.length != incrBackupEnabled.length)
               throw new IOException("incremental_bkup but table list and incr list differ");
       }

       for (int i = 0; i < tables.length; i++) {
           final String hbaseTableName = (String) tables[i];
           boolean incrementalSnapshot = 
               (incremental_bkup && (((String)incrBackupEnabled[i]).startsWith("Y")));
       }

       return true;
   }

   //////////////////////////////////////////////////////////////////////
   // tables:     list of fully qualified tables(namespace:cat.sch.tab)
   //             to be backed up.
   // tableflags: list of flags containing 'Y' or 'N' depending on
   //             whether the corresponding object in 'tables' has
   //             incremental backup enabled.
   //             First character indicates incremental table.
   //             Second character indicates whether table name
   //             includes backup Tag name included with it.
   // hdfsDirs:   Lob information
   // backuptag:  backup tag
   // extendedAttributes: special attributes assigned to backup
   // backupType: type of backup: REGULAR, INCREMENTAL, SYSTEM
   // numParellelThreads: number threads to use for backup
   //////////////////////////////////////////////////////////////////////
    public boolean backupObjects(Object[] tables,
                                 Object[] tableflags,
                                 Object[] hdfsDirs,
                                 final String backuptag,
                                 String extendedAttributes,
                                 String backupType,
                                 int numParallelThreads,
                                 int progressUpdateDelay)
        throws  MasterNotRunningException,
                IOException,
                Exception,
                SnapshotCreationException,
                InterruptedException {
      if (logger.isInfoEnabled()) logger.info("[BACKUPOBJECTS] backuptag " + backuptag +
                  " backupType " + backupType + " extendedAttributes " + extendedAttributes +
                  " tables " + String.valueOf((tables != null ? tables.length:0)) +
                  " tableFlags " + String.valueOf((tableflags != null ? tableflags.length:0)) +
                  " hdfsDirs " + String.valueOf((hdfsDirs != null ? hdfsDirs.length:0)) +
                  " threads " + String.valueOf(numParallelThreads));

      boolean operationLocked = false;

      String hdfsRoot = config.get("fs.default.name");
      Path hdfsRootPath = new Path(hdfsRoot);
      final HdfsAdmin hdfsAdmin = new HdfsAdmin ( hdfsRootPath.toUri(), config);
      final FileSystem hdfsRootFs = FileSystem.get(hdfsRootPath.toUri(), config);

      //Branch out to perform system backup if backupType is FULL.
      //System backup is completely independent of snapshotMeta updates.
      //We only need to make sure the tag provided is not duplicate.
      if(backupType.equals("SYSTEM")) {

         return systemBackup(tables, hdfsDirs, backuptag, extendedAttributes, numParallelThreads);
      }

      ArrayList<String> flushTables = new ArrayList<String>();
      List<String> warningTables = new ArrayList<String>();
      boolean incremental_bkup = backupType.equals("INCREMENTAL");

      //if numParallelThreads is zero, default it to the number of 
      //region servers in the cluster.
      if(numParallelThreads <= 0)
      {
        Admin admin = connection.getAdmin();
        ClusterStatus cstatus = admin.getClusterStatus();
        numParallelThreads = cstatus.getServersSize();
        admin.close();
      }
      this.pit_thread = numParallelThreads;

      //setup thread pool.
      ExecutorService tPool = Executors.newFixedThreadPool(pit_thread == 0 ? 1 : pit_thread);
      CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tPool);

      try {
        //From this point onwards, lock and preserve SNAPSHOT_META and MUTATION_META
        //tables until the operation is successful.
        if (logger.isInfoEnabled()) logger.info("backupObjects calling OperationLock(" + backuptag + ")");
        operationLock(backuptag);
        operationLocked = true;

        //Always get idTm after obtaining operationLock.
        long startId = timeIdVal = getIdTmVal();

        // Initialize Full SnapshotMetaStartRecord
        initializeSnapshotMeta(timeIdVal, backuptag, incremental_bkup, extendedAttributes, backupType);

        // For each table, generate snapshot name.
        // snapshotName is internal name for reference( mostly not needed anymore ?)
        // snapshotPath is actual name of snapshot.
        int loopCount = 0;
        for (int i = 0; i < tables.length; i++) {
          final String hbaseTableName = (String) tables[i];
          boolean lvIncrementalSnapshot = (incremental_bkup && (((String)tableflags[i]).startsWith("Y")));
          final boolean sqlMetaTable = (tableflags!= null)? ((String)tableflags[i]).endsWith("Y") : false;
          final boolean incrementalTable = (((String)tableflags[i]).startsWith("Y"));
          final Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());

          // If this is a REGULAR backup and we are running in crash recovery mode we update the
          // list of tables to flush based on the incremental attribute
//          if ( /* (crash recovery mode) && */ (incrementalTable) && (! sqlMetaTable)) {
          if (incrementalTable) {
             flushTables.add(hbaseTableName);
          }

          // Each table may have n LOB columns.  Each LOB column is an hdfs file that
          // requires a snapshot because we can't capture the changes in mutation files
          // Make sure the destination directory exists

          StringTokenizer st = new StringTokenizer((String) hdfsDirs[i], ",");
//          StringTokenizer st = new StringTokenizer("", ",");
          int numLocs = 0;
          while (st.hasMoreTokens()){
             String tmpHdfsDir = st.nextToken().trim();
             final String srcHdfsDir = hdfsRoot + tmpHdfsDir;
             Path srcHdfsDirPath = new Path(srcHdfsDir);

             if(! hdfsRootFs.exists(srcHdfsDirPath)) {
                if (logger.isErrorEnabled()) logger.error("[BACKUPOBJECTS] srcHdfsDirPath "
                       + srcHdfsDirPath + " does not exist ");

                continue;
             }
             else {
                try {
                   // We must ensure the directory is snapshotable.  This may need to be done
                   // by an administrator outside of backup/restore
                   if (logger.isErrorEnabled()) logger.error("[BACKUPOBJECTS] allowing snapshots for path "
                            + srcHdfsDirPath);

                   hdfsAdmin.allowSnapshot(srcHdfsDirPath);
                }
                catch(IOException ioe){
                   if (logger.isErrorEnabled()) logger.error("[BACKUPOBJECTS] srcHdfsDirPath "
                            + srcHdfsDirPath + " not enabled for snapshot ", ioe);
                }
             }

             final String lobSnapshotName = LobMetaRecord.createPath(srcHdfsDir +
                                "_LOB_" + backuptag + "_" + String.valueOf(startId));

             long key = getIdTmVal();
             final String snapshotPath = srcHdfsDir;

             updateLobMeta(key, lm.getMetaVersion(), hbaseTableName, lobSnapshotName,
                   -1L /* fileSize */, backuptag, snapshotPath, "dummy",
                   true /* inLocalFS */, false /* archived */, "null" /* snapshotArchivePath */);

             // Keep track of the lobs so we can add the set to the snapshot record for the table
             lobSnapshots.add(key);

             compPool.submit(new BackupRestoreCallable() {
                 public Integer call() throws Exception {
                     return doLobSnapshot(  hdfsAdmin,
                             hdfsRootFs,
                             hbaseTableName,
                             snapshotPath,
                             lobSnapshotName,
                             backuptag,
                             false /* systemBackup */);

                 }
             });
             numLocs++;

          }
          if (logger.isDebugEnabled())
              logger.debug("backupObjects doLobSnapshot/doBroadcast sent " + numLocs + " times");

          // simply to make sure they all complete, no return codes necessary at the moment
          for (int k = 0; k < numLocs; k++) {
             int returnValue = compPool.take().get();
          }

          // snapshot name cannot contain ":".
          // If the src name is prefixed by "namespace:", replace
          // ":" by "_"
          final String snapshotName = hbaseTableName.replace(":", "_") + 
                                      "_SNAPSHOT_" + 
                                      backuptag +
                                      "_" +
                                      String.valueOf(startId);

          final String snapshotPath = SnapshotMetaRecord.createPath(snapshotName);

          if(lvIncrementalSnapshot) {
             // If we are doing an incrementalSnapshot, but the real snapshot was system
             // generated, then we delete the system generated snapshot and associated
             // mutations before creating a new user generated snapshot.
             SnapshotMetaRecord lvSnapshotRec = sm.getCurrentSnapshotRecord(hbaseTableName);
             if (lvSnapshotRec != null){
                if (lvSnapshotRec.isSkipTag() ||
                    (lvSnapshotRec.getHiatusTime() != -1L)){

                   lvIncrementalSnapshot = false;
                   List<MutationMetaRecord> orphanedMutations = mm.getMutationsFromSnapshot(lvSnapshotRec.getKey(),
                                         lvSnapshotRec.getMutationStartTime(), hbaseTableName, false /* skipPending
                                         */, true , 0L, false);
                   warningTables.add(hbaseTableName);
                   if (logger.isInfoEnabled()) logger.info("backupObjects performing full snapshot for backupTag " + backuptag
                       + " Prior snapshot tag: " + lvSnapshotRec.getUserTag()
                       + " userGenerated: " + lvSnapshotRec.getUserGenerated()
                       + " hiatusTime: " + lvSnapshotRec.getHiatusTime() + " for " + hbaseTableName
                       + "  There are " + orphanedMutations.size() + " orphaned mutations");
                   if (lvSnapshotRec.getHiatusTime() != -1L){
                      // In the case of a hiatus record, we must leave the record in place
                      // because it's possible to restore to that record, but we
                      // make sure to perform a regular snapshot instead of incremental
                   }
                   else {
                      // We need to delete the system generated snapshot and make
                      // sure we do a full snapshot for this table.
                      // Need to delete the mutations?
                      SnapshotMeta.deleteRecord(lvSnapshotRec.getKey());
                   }
                }
             }
             else {
                // There is no prior snapshotRecord, so we need to perform one now
                lvIncrementalSnapshot = false;
                warningTables.add(hbaseTableName);
                if (logger.isInfoEnabled()) logger.info("backupObjects performing full snapshot for backupTag " + backuptag
                     + " because prior snapshot does not exist for " + hbaseTableName);
             }
          }

          final boolean incrementalSnapshot = lvIncrementalSnapshot;
          if(incrementalSnapshot) {
             compPool.submit(new BackupRestoreCallable() {
                public Integer call() throws Exception {
                return doBroadcast( hbaseTableName,
                                    snapshotName,
                                    snapshotPath,
                                    backuptag,
                                    incrementalSnapshot,
                                    false,        /* sysBackup never true here*/
                                    sqlMetaTable, /* sqlMetaData */
                                    true,         /* user generated */
                                    lobSnapshots,
                                    incrementalTable);
                }
            });

          } else {
            compPool.submit(new BackupRestoreCallable() {
              public Integer call() throws Exception {
              return doSnapshot(  hbaseTableName,
                                  snapshotName,
                                  snapshotPath,
                                  backuptag,
                                  incrementalSnapshot,
                                  false,
                                  sqlMetaTable, /* sqlMetaData */
                                  true,         /* user generated */
                                  lobSnapshots,
                                  incrementalTable);
                }
              });
          }
          
          loopCount++;
        }// for
        
        if (logger.isDebugEnabled())
          logger.debug("backupObjects doSnapshot/doBroadcast sent " + loopCount + " times");
        // simply to make sure they all complete, no return codes necessary at the moment
        int tagOperCount = getBREIOperCount("BACKUP", backuptag);
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
          int returnValue = compPool.take().get(); 
          updateBREIprogress("BACKUP", backuptag, loopCount, loopIndex+1,
                             progressUpdateDelay, tagOperCount, false);
        }
      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("backupObjects exception : ", e);

        //operation unlock here since already locked.
        try {
          if(operationLocked){
            operationLocked = false;
            if (logger.isInfoEnabled()) logger.info("backupObjects calling OperationUnlock(" + backuptag + ", true) because of previous exception");
            operationUnlock(backuptag, true/*recoverMeta*/);
          }
        }catch (IOException ie) {
          e.addSuppressed(ie);
        }
        throw new IOException(e);
      } finally {
        if(tPool != null)
            tPool.shutdownNow();
        //No need of operationUnlock here.
        //It is performed in finalizeBackup().
      }

      if(flushTables.size() > 0){
         if (logger.isInfoEnabled()) logger.info("backupObjects updating mutation flush list with "
             + flushTables.size() + " tables for tag "+ backuptag);
         SnapshotMutationFlushRecord smfr = sm.getFlushRecord();
         if (smfr == null){
            // Record is not there yet, so create a starting point
            smfr = new SnapshotMutationFlushRecord(SNAPSHOT_MUTATION_FLUSH_KEY,
                          SnapshotMetaRecordType.getVersion(), 0 /* flushInterval */, 0L /* timeIdVal */, flushTables);
         }
         else {
            // Update existing record with current table list
            smfr.setTableList(flushTables);
         }
         sm.putRecord(smfr);
      }

      // Complete snapshotMeta update.
      //Moving this to finalizeBackup called from SQL.
      //completeSnapshotMeta();
      ListIterator<String> tabIterator = warningTables.listIterator();
      if (tabIterator.hasNext()){
         StringBuilder tabNames = new StringBuilder();
         while (tabIterator.hasNext()){
            tabNames.append(" ");
            tabNames.append(tabIterator.next());
         }
         lastWarning = new String("Regular backup implemented because prior backup "
                                  + "does not exist for incremental tables:" + tabNames.toString());
         if (logger.isWarnEnabled()) logger.warn("backupObjects " + backuptag + " " + lastWarning);
         //return false;
      }

      // close ttable
      if (ttable != null)
          ttable.close();

      if (logger.isInfoEnabled()) logger.info("[BACKUPOBJECTS] complete for backuptag " + backuptag);

      return true;
    }

    //////////////////////////////////////////////////////////////////////
    // table:      Name of fully qualified table(namespace:cat.sch.tab)
    //             Following a RESTORE operation, we need to create a
    //             dummy snapshot record and then trigger mutatiom file
    //             close so that new mutations are associated with the new
    //             snapshot record.
    //////////////////////////////////////////////////////////////////////
    public boolean newSnapshotRecAndMutationClose(ArrayList<TableRecoveryGroup> trgList)
         throws  MasterNotRunningException,
                 IOException,
                 Exception,
                 InterruptedException {
       if (logger.isInfoEnabled()) logger.info("newSnapshotRecAndMutationClose " +
                   " tables: " + trgList.size());

       SnapshotMetaRecord lvSnapshotRec = null;
       for (final TableRecoveryGroup t : trgList) {
          final String hbaseTableName = t.getSnapshotRecord().getTableName();

          if (HBASE_NATIVE_TABLES.contains(hbaseTableName)) {
              if (logger.isInfoEnabled()) logger.info("table: " + hbaseTableName + " is a hbase native table.");
              continue;
          }
          long key = getIdTmVal();
          long systemTime = EnvironmentEdgeManager.currentTime();

          Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
          Set<Long> dependentSnapshots = Collections.synchronizedSet(new HashSet<Long>());
          lobSnapshots.add(-1L);
          dependentSnapshots.add(-1L);
          lvSnapshotRec = new SnapshotMetaRecord(key,
                           -1L /* supercedingSnapshot */,
                           -1L /* restoredSnapshot */,
                           -1L /* restoreStartTime */,
                           -1L /* hiatusTime */,
                           hbaseTableName,
                           "AfterRestore" /* userTag */,
                           "noName" /* snapshotName */,
                           "null" /* snapshotPath */,
                           false /* userGenerated */,
                           true /* incrementalTable */,
                           false /* sqlMetaData */,
                           true /* inLocalFS */,
                           false /* excluded */,
                           systemTime /* mutationStartTime */,
                           "null" /* snapshotArchivePath */,
                           "EmptyStatements" /* Meta Statements */,
                           1,
                           lobSnapshots,
                           dependentSnapshots);

          sm.putRecord(lvSnapshotRec);

          if (logger.isInfoEnabled()) logger.info("newSnapshotRecAndMutationClose created new snapshot record " + lvSnapshotRec);

       } // for

       // All Snapshot records created, now flush and close mutations so new mutations are associated
       // against the new record
       try {
          xdcBroadcastTRG(GENERIC_FLUSH_MUTATION_FILES, 0, 0 /* local peer */, trgList);

          if (logger.isDebugEnabled())
             logger.debug("newSnapshotRecAndMutationClose GENERIC_FLUSH_MUTATION_FILES sent");

       } catch (Exception e) {
         if (logger.isErrorEnabled()) logger.error("newSnapshotRecAndMutationClose exception : ", e);
         throw new IOException(e);
       }

       if (logger.isDebugEnabled())
          logger.debug("newSnapshotRecAndMutationClose EXIT");
       return true;
    }

    //////////////////////////////////////////////////////////////////////
    // table:      Name of fully qualified table(namespace:cat.sch.tab)
    //             needing a hiatusRecord.
    //////////////////////////////////////////////////////////////////////
    public boolean setHiatus(String table,
                             boolean lockOperation,
                             boolean createSnapIfDoesNotExist,
                             boolean ignoreSnapIfDoesNotExist,
                             int numParallelThreads)
         throws  MasterNotRunningException,
                 IOException,
                 Exception,
                 SnapshotCreationException,
                 InterruptedException {
       if (logger.isInfoEnabled()) logger.info("setHiatus lockOperation: " + lockOperation +
                   " table: " + table +
                   " createSnapIfDoesNotExist " + createSnapIfDoesNotExist +
                   " ignoreSnapIfDoesNotExist"  + ignoreSnapIfDoesNotExist +
                   " threads: " + String.valueOf(numParallelThreads));

       boolean operationLocked = false;
       
       //if numParallelThreads is zero, default it to the number of
       //region servers in the cluster.
       if(numParallelThreads <= 0)
       {
         Admin admin = connection.getAdmin();
         ClusterStatus cstatus = admin.getClusterStatus();
         numParallelThreads = cstatus.getServersSize();
         admin.close();
       }
       this.pit_thread = numParallelThreads;

       //setup thread pool.
       ExecutorService tPool = Executors.newFixedThreadPool(pit_thread == 0 ? 1 : pit_thread);
       CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tPool);
       final String hbaseTableName = table;

       try {
         if (lockOperation){
            //From this point onwards, lock and preserve SNAPSHOT_META and MUTATION_META
            //tables until the operation is successful.
            operationLock("hiatus");
            operationLocked = true;
         }
         compPool.submit(new BackupRestoreCallable() {
             public Integer call() throws Exception {
                 return broadcastHiatus(hbaseTableName);
             }
         });

         if (logger.isDebugEnabled())
           logger.debug("setHiatus broadcastHiatus sent");

         // simply to make sure they all complete, no return codes necessary at the moment
         int returnValue = compPool.take().get();

         //Always get idTm after broadcast is complete
         long hiatusTime = getIdTmVal();

         // We need to update the current snapshot record
         SnapshotMetaRecord lvSnapshotRec = sm.getCurrentSnapshotRecord(hbaseTableName);
         if (lvSnapshotRec != null) {
            if (logger.isInfoEnabled()) logger.info("setHiatus setting hiatus for record " + hbaseTableName);
            lvSnapshotRec.setHiatusTime(hiatusTime);
            // Make sure the hiatus time is saved in the SnapshotTable
            sm.putRecord(lvSnapshotRec);
         }
         if (logger.isInfoEnabled()) logger.info("setHiatus could not find snapshot for " + hbaseTableName);
         long key = hiatusTime;
         hiatusTime = getIdTmVal();
         long systemTime = EnvironmentEdgeManager.currentTime();
         Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
         Set<Long> dependentSnapshots = Collections.synchronizedSet(new HashSet<Long>());
         lobSnapshots.add(-1L);
         dependentSnapshots.add(-1L);
         lvSnapshotRec = new SnapshotMetaRecord(key,
                               -1L /* supercedingSnapshot */,
                               -1L /* restoredSnapshot */,
                               -1L /* restoreStartTime */,
                               hiatusTime,
                               table,
                               "AlterIncremental" /* userTag */,
                               "noName" /* snapshotName */,
                               "null" /* snapshotPath */,
                               false /* userGenerated */,
                               true /* incrementalTable */,
                               false /* sqlMetaData */,
                               true /* inLocalFS */,
                               false /* excluded */,
                               systemTime /* mutationStartTime */,
                               "null" /* snapshotArchivePath */,
                               "EmptyStatements" /* Meta Statements */,
                               1,
                               lobSnapshots,
                               dependentSnapshots);

         // Make sure the hiatus time is saved in the SnapshotTable
         sm.putRecord(lvSnapshotRec);

         if (logger.isInfoEnabled()) logger.info("setHiatus created new snapshot record " + lvSnapshotRec);
       } catch (Exception e) {
         if (logger.isErrorEnabled()) logger.error("setHiatus exception : ", e);

         if (operationLocked){
            //operation unlock here since already locked.
            try {
              operationLocked = false;
              operationUnlock("hiatus", true/*recoverMeta*/);
            }catch (IOException ie) {
              e.addSuppressed(ie);
            }
         }
         throw new IOException(e);
       } finally {
         if(tPool != null)
           tPool.shutdownNow();
         
         if (operationLocked){
           //operation unlock here since already locked.
           //Exception caught by caller. Log.error already
           //logged in operationUnlock(). No need of recover
           //meta since it is normal exit.
           operationUnlock("hiatus", false/*recoverMeta*/);
           operationLocked = false;
         }
       }
      
      if (logger.isDebugEnabled())
        logger.debug("setHiatus EXIT");
      return true;
    }


    //////////////////////////////////////////////////////////////////////
    // table:      Name of fully qualified table(namespace:cat.sch.tab)
    //             we wantt o clear hiatus record on.
    //////////////////////////////////////////////////////////////////////
    public boolean clearHiatus(String table)
         throws  MasterNotRunningException,
                 IOException,
                 Exception,
                 SnapshotCreationException,
                 InterruptedException {
       if (logger.isInfoEnabled()) logger.info("clearHiatus start for table: " + table);

       final String hbaseTableName = table;

       // We need to update the current snapshot record
       SnapshotMetaRecord lvSnapshotRec = sm.getCurrentSnapshotRecord(hbaseTableName);
       if (lvSnapshotRec != null){
          if (logger.isInfoEnabled()) logger.info("clearHiatus clearing hiatus for record " + hbaseTableName);
          lvSnapshotRec.setHiatusTime(-1L);
       }
       else {
          if (logger.isInfoEnabled()) logger.info("clearHiatus could not find snapshot for " + hbaseTableName);
          throw new IOException("clearHiatus could not find snapshot for " + hbaseTableName);
       }

       // Make sure the record is saved in the SnapshotTable
       sm.putRecord(lvSnapshotRec);

       if (logger.isDebugEnabled())
           logger.debug("clearHiatus EXIT");
       return true;
    }

    //////////////////////////////////////////////////////////////////////
    // tables:     list of fully qualified tables(namespace:cat.sch.tab)
    //             for which snapshot is to be created.
    //             This snapshot will be the base on which further
    //             incremental backups could be done.
    //             Specified tables must be enabled for incremental backup.
    //
    // Return:     true, if success. false, if error.
    //////////////////////////////////////////////////////////////////////
    public boolean createSnapshotForIncrBackup(Object[] tables)
        throws  MasterNotRunningException,
                IOException,
                Exception, 
                SnapshotCreationException,
                InterruptedException {

      final String backupTag =  "NON_TX_SNAPSHOT" + "_" + String.valueOf(getIdTmVal()) + "_BRC_";
      final String nullAttributes =  "nullAttributes";
      return createSnapshotForIncrBackup(tables, nullAttributes, backupTag);
    }

    public boolean createSnapshotForIncrBackup(Object[] tables, final String extendedAttributes, final String backupTag)
        throws  MasterNotRunningException,
                IOException,
                Exception, 
                SnapshotCreationException,
                InterruptedException {
      // backup tag name cannot contain ":".
      // If the src name is prefixed by "namespace:", replace
      // ":" by "_"
      // TODO: Modified by heng.guo to solve Mantis#15102
      String srcTagName = backupTag;
      if (srcTagName.contains(":"))
          srcTagName = srcTagName.replace(":", "_");

      final String tagName = srcTagName;

      boolean operationLocked = false;
      
      ExecutorService tPool = Executors.newFixedThreadPool(pit_thread == 0 ? 1 : pit_thread);

      //setup thread pool completion service.
      CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tPool);

      if (logger.isInfoEnabled()) logger.info("createSnapshotForIncrBackup snapshots for backupTag " + tagName +
                  " Tables : " + String.valueOf( tables != null? tables.length : 0));

      try {
        
        operationLock(tagName);
        operationLocked = true;
        
        //get id just after lock. We have id to be as close to initializeSnapshotMeta.
        long startId = timeIdVal = getIdTmVal();

        // Initialize Full SnapshotMetaStartRecord
        initializeSnapshotMeta(timeIdVal, tagName, /* incrementalBackup */ false, extendedAttributes, "SUBSET");
        final Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());

        // For each table, generate snapshot name.
        // snapshotName is internal name for reference( mostly not needed anymore ?)
        // snapshotPath is actual name of snapshot.
        int loopCount = 0;
        for (Object t : tables) {
          
          final String hbaseTableName = (String)t;
          final boolean sqlMetaTable = false;

          // snapshot name cannot contain ":".
          // If the src name is prefixed by "namespace:", replace
          // ":" by "_"
          final String snapshotName = hbaseTableName.replace(":", "_") + 
                                      "_SNAPSHOT_" + 
                                      tagName +
                                      "_" +
                                      String.valueOf(startId);
          
          final String snapshotPath = SnapshotMetaRecord.createPath(snapshotName);
            
          compPool.submit(new BackupRestoreCallable() {
            public Integer call() throws Exception {
            return doSnapshot(  hbaseTableName,
                                snapshotName,
                                snapshotPath,
                                tagName,
                                false,          /*incrementalSnapshot*/
                                false,          /*systemBkp*/
                                sqlMetaTable,   /*sqlMetaData */
                                false,          /*userGenerated*/
                                lobSnapshots,
                                true);          /*incrementalTable */
              }
            });
          loopCount++;
        }// for
        
        // simply to make sure they all complete, no return codes necessary at the moment
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
          int returnValue = compPool.take().get(); 
        }
        
        // Complete snapshotMeta update.
        completeSnapshotMeta();
        
      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("createSnapshotForIncrBackup exception : ", e);
        
        //operation unlock here since already locked.
        try {
          if(operationLocked) {
            operationLocked = false;
            operationUnlock(tagName, true/*recoverMeta*/);
          }
        }catch (IOException ie) {
          e.addSuppressed(ie);
        }
        throw new IOException(e);
      } finally {
        if(tPool != null)
          tPool.shutdownNow();
        if(operationLocked) {
          operationUnlock(tagName, false/*recoverMeta*/);
          operationLocked = false;
        }
      }
 
      return true;
    }

    //private method called from restoreObjects, supports parallel threads.
    //This method is internally called.
    private void backupTables(ArrayList<TableRecoveryGroup> restoreTableList,
                              final String extendedAttributes, final String backupTag
                              ) throws Exception {

      if (logger.isInfoEnabled()) logger.info("backupTables start for tag: " + backupTag);

      //Keep the tag unique so that there is no duplicates.
      if(isTagExistInMeta(backupTag) || isTagExistBackupSys(backupTag)) {
          throw new IOException("backupTables Backup tag " + backupTag + " already exists.");
      }

      String hdfsRoot = config.get("fs.default.name");
      Path hdfsRootPath = new Path(hdfsRoot);
      final HdfsAdmin hdfsAdmin = new HdfsAdmin ( hdfsRootPath.toUri(), config);
      final FileSystem hdfsRootFs = FileSystem.get(hdfsRootPath.toUri(), config);

      long startId = timeIdVal = getIdTmVal();

      //setup thread pool completion service.
      CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);

      try {
        // Initialize Full SnapshotMetaStartRecord
        initializeSnapshotMeta(timeIdVal, backupTag, /* incrementalBackup */ false, extendedAttributes, "SUBSET");
        
        // For each table, generate snapshot name.
        // snapshotName is internal name for reference( mostly not needed anymore ?)
        // snapshotPath is actual name of snapshot.
        int loopCount = 0;
        for (TableRecoveryGroup t : restoreTableList) {
          
          SnapshotMetaRecord rec = t.getSnapshotRecord();
          final String hbaseTableName = rec.getTableName();
          final boolean sqlMetaTable = rec.getSqlMetaData();
          final boolean incrementalTable = rec.getIncrementalTable();
          ArrayList<SnapshotMetaIncrementalRecord> incSnapshots = t.getSnapshotIncrementalList();
          Set<Long> newLobIds = new HashSet<Long>();
          List<LobMetaRecord> origLogRecs = t.getLobList();
          int lobLoopCount = 0;

          // For each Lob file we need to generate a new snapshot using the actual path
          for(LobMetaRecord lrec : origLogRecs){
             // Get the lobrecord so we can determine the source lob file
             if (logger.isInfoEnabled()) logger.info("backupTables creating new lob record for: " + lrec.getKey());
             final String srcHdfsDir = lrec.getHdfsPath();
             Path srcHdfsDirPath = new Path(srcHdfsDir);

             if(! hdfsRootFs.exists(srcHdfsDirPath)) {
                if (logger.isInfoEnabled()) logger.info("backupTables table " + hbaseTableName + " tag " + backupTag + " srcHdfsDirPath "
                        + srcHdfsDirPath + " does not exist ");

                continue;
             }

             try{
                hdfsAdmin.allowSnapshot(srcHdfsDirPath);
             }
             catch(IOException ioe){
                 if (logger.isErrorEnabled()) logger.error("backupTables srcHdfsDirPath "
                              + srcHdfsDirPath + " not enabled for snapshot ", ioe);
                 throw ioe;
             }
             final String lobSnapshotName = LobMetaRecord.createPath(srcHdfsDir +
                     "_LOB_" + backupTag + "_" + String.valueOf(startId));

             long key = getIdTmVal();
             final String lobSnapshotPath = srcHdfsDir;

             updateLobMeta(key, lm.getMetaVersion(), hbaseTableName, lobSnapshotName,
                   -1L /* fileSize */, backupTag, lobSnapshotPath, "dummy",
                   true /* inLocalFS */, false /* archived */, "null" /* snapshotArchivePath */);

             // Keep track of the lobs so we can add the set to the snapshot record for the table
             newLobIds.add(key);

             compPool.submit(new BackupRestoreCallable() {
                 public Integer call() throws Exception {
                     return doLobSnapshot(  hdfsAdmin,
                             hdfsRootFs,
                             hbaseTableName,
                             lobSnapshotPath,
                             lobSnapshotName,
                             backupTag,
                             false /* systemBackup */);

                 }
             });
             lobLoopCount++;
          }

          if (logger.isDebugEnabled())
              logger.debug("backupTables doLobSnapshot sent " + origLogRecs.size() + " times");

          // simply to make sure they all complete, no return codes necessary at the moment
          for (int k = 0; k < lobLoopCount; k++) {
             int returnValue = compPool.take().get();
          }

          // snapshot name cannot contain ":".
          // If the src name is prefixed by "namespace:", replace
          // ":" by "_"
          final String snapshotName = hbaseTableName.replace(":", "_") + 
                                      "_SNAPSHOT_" + 
                                      backupTag +
                                      "_" +
                                      String.valueOf(startId);
          
          final String snapshotPath = SnapshotMetaRecord.createPath(snapshotName);
          final Set<Long> lobSnapshotIds = newLobIds;

          compPool.submit(new BackupRestoreCallable() {
            public Integer call() throws Exception {
            return doSnapshot(  hbaseTableName,
                                snapshotName,
                                snapshotPath,
                                backupTag,
                                false,          /*incrementalSnapshot*/
                                false,          /*systemBkp*/
                                sqlMetaTable,   /*sqlMetaData */
                                false,          /*userGenerated*/
                                lobSnapshotIds, /* lobSnapshots */
                                incrementalTable); /*incrementalTable*/
              }
            });
          loopCount++;
        }// for
        
        // simply to make sure they all complete, no return codes necessary at the moment
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
          int returnValue = compPool.take().get(); 
        }
      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("backupTables exception : ", e);
        throw new IOException(e);
      } finally {
      }
      
      //if testing inject error, throws exception.
      injectError("RESTORE_ERR_INJECT_J1");

      // Complete snapshotMeta update.
      completeSnapshotMeta();

      if (logger.isInfoEnabled()) logger.info("backupTables end for tag: " + backupTag);
    }
    
    //private method called from restoreObjects, supports parallel threads.
    //backupTag is only for logging purposes to help during debug.
    private void deleteTables(ArrayList<TableRecoveryGroup> restoreTableList,
                              final String backupTag
                              ) throws Exception {
      
      if (logger.isInfoEnabled()) logger.info("ENTRY deleteTables for backupTag " + backupTag +
                  " num of tables: " + restoreTableList.size());
      
      int loopCount = 0;
      CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);
      
      final Admin admin = connection.getAdmin();
      try {
        for (TableRecoveryGroup t : restoreTableList) {
          final String tname = t.getSnapshotRecord().getTableName();
          final TableName tableName = TableName.valueOf(tname);
          if (admin.tableExists(tableName)) {
            // Send work to a thread for work
            compPool.submit(new BackupRestoreCallable() {
              public Integer call() throws Exception {
                  return doDelete(tableName, backupTag, admin);
              }
            });
            loopCount++;
          }
        }

        // simply to make sure they all complete, no return codes necessary at the moment
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
            int returnValue = compPool.take().get(); 
        }
        if (logger.isInfoEnabled()) logger.info("EXIT deleteTables for backupTag " + backupTag +
                    " num of tables: " + restoreTableList.size());
      } catch (Exception e) {
          if (logger.isErrorEnabled()) logger.error("deleteTables exception : ", e);
          throw e;
      } finally {
          admin.close();
      }
    }

    //private method called from restoreObjects, supports parallel threads.
    //backupTag only for logging purpose.
    private void restoreTables(ArrayList<TableRecoveryGroup> restoreTableList,
                               final long limitTimeStamp,
                               final boolean restoringFromSys,
                               final String backupTag,
                               final int progressUpdateDelay,
                               boolean restoreOnlyMD
    ) throws Exception {
        long threadId = Thread.currentThread().getId();
        if (logger.isInfoEnabled())
            logger.info("ENTRY restoreTables for backupTag " + backupTag + " num of tables: " + restoreTableList.size() + " thread " + threadId);

        int loopCount = 0;
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);

        // For each table, in the list restore snapshot
        try {
            String nameSpace = null;
            for (final TableRecoveryGroup t : restoreTableList) {
                String hbaseTableName = t.getSnapshotRecord().getTableName();
                String space = null;
                int prefix = hbaseTableName.indexOf(":");
                if (prefix != -1) {
                    space = hbaseTableName.substring(0, prefix);
                }

                if (space != null && !space.equals(nameSpace)) {
                    Admin admin = connection.getAdmin();
                    try {
                        NamespaceDescriptor nameDesc = admin.getNamespaceDescriptor(space);
                        if (nameDesc != null) {
                            if (logger.isInfoEnabled()) {
                                logger.info("namespace:" + space + " is exists.");
                            }
                        }
                    } catch (NamespaceNotFoundException e) {
                        if (logger.isInfoEnabled()) {
                            logger.info("namespace:" + space + " is not exists, then we need to create it.");
                        }
                        admin.createNamespace(NamespaceDescriptor.create(space).build());
                    } finally {
                        nameSpace = space;
                        admin.close();
                    }
                }
                // 提前检查MR依赖，不存在则上传依赖x
                DependencyCheckUtils.checkDependency(config);
                // Send work to thread
                compPool.submit(new BackupRestoreCallable() {
                    public Integer call() throws Exception {
                        return doRestore(t, limitTimeStamp, restoringFromSys,
                                backupTag);
                    }
                });
                loopCount++;
            } // for-loop

            // simply to make sure they all complete, no return codes necessary at the moment
            int tagOperCount = getBREIOperCount("RESTORE", backupTag);
            for (int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                int returnValue = compPool.take().get();
                updateBREIprogress("RESTORE", backupTag, loopCount, loopIndex + 1,
                        progressUpdateDelay, tagOperCount, restoreOnlyMD);
            }
            if (logger.isInfoEnabled())
                logger.info("EXIT restoreTables for backupTag " + backupTag + " num of tables: " + restoreTableList.size() + " thread " + threadId);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) logger.error("restoreTables exception : ", e);
            throw new IOException(e);
        }
    }
    
    //This method is called by sql before restore.
    public String getBackupType(String backuptag) throws Exception {
        if (logger.isInfoEnabled()) logger.info("[GETBACKUPTYPE] backupTag " + backuptag);
        String backupType = "";
        SnapshotMetaStartRecord snapshotStartRecord = null;
        
        try {
          snapshotStartRecord = sm.getSnapshotStartRecord(backuptag);
          if (snapshotStartRecord != null) {
            if (logger.isInfoEnabled()) logger.info("getBackupType(1) returning " + backuptag + " " + snapshotStartRecord.getBackupType());
            return snapshotStartRecord.getBackupType();
          }
          return backupType;
        }catch (Exception e) {
          String exceptionString = ExceptionUtils.getStackTrace(e);
          if (! exceptionString.contains("Record not found")) {
             //any exception other than records not found throw error.
            throw new IOException(e);
          }
        }
        
        //Now check backupsys. Reaching here means the tag was not 
        //found in snapshotmeta.
        BackupSet bkpset = getBackupFromBackupSys(backuptag);
        if(bkpset != null) {
          if (logger.isInfoEnabled()) logger.info("getBackupType(2) returning " + backuptag + " " + bkpset.getBackupType());
          return bkpset.getBackupType();
        }
        
        if (logger.isInfoEnabled()) logger.info("getBackupType(3) returning " + backuptag + " " + backupType);
        
        return backupType;
    }

    public String getBackupStatus(String backuptag) throws Exception {
      String backupStatus = "NOT FOUND";
      SnapshotMetaStartRecord snapshotStartRecord = null;
      
      try {
        snapshotStartRecord = sm.getSnapshotStartRecord(backuptag);
        if (snapshotStartRecord != null) {
          backupStatus = snapshotStartRecord.getSnapshotComplete() ? "VALID" : "INVALID";
          return backupStatus;
        }
        return backupStatus;
      }catch (Exception e) {
        String exceptionString = ExceptionUtils.getStackTrace(e);
        if (! exceptionString.contains("Record not found")) {
           //any exception other than records not found throw error.
          throw new IOException(e);
        }
      }
      
      //Now check backupsys. Reaching here means the tag was not 
      //found in snapshotmeta.
      BackupSet bkpset = getBackupFromBackupSys(backuptag);
      if(bkpset != null)
        return (bkpset.isValid()? "VALID" : "INVALID");

      return backupStatus;
    }
    
    private boolean searchForObject(String tblName,
                                    Object[] schemas,
                                    Object[] tables)
    {
        if ((schemas == null) && (tables == null))
            return true;

        if (logger.isTraceEnabled()) logger.trace("searchForObject ENTRY searching for "
            + tblName + " among " + (schemas == null ? 0 : schemas.length) + " schemas and "
            + (tables == null ? 0 : tables.length) + " tables ");

        // remove namespace from tblName, if present
        int colonPos = tblName.indexOf(':');
        String tblNameWithoutNameSpace = null;
        if (colonPos >= 0) {
            tblNameWithoutNameSpace = tblName.substring(colonPos+1);
        }
        else
            tblNameWithoutNameSpace = tblName;

        // search for object in tables list
        if (tables != null)
            {
                boolean found = false;
                for (int i = 0; (!found && (i < tables.length)); i++) {
                    if (logger.isTraceEnabled()) logger.trace("searchForObject ENTRY searching for "
                            + tblName + " against table " + (String)tables[i]);
                    if (tblNameWithoutNameSpace.equals((String) tables[i]))
                        {
                            found = true;
                        }
                }
                
                if (found){
                    if (logger.isTraceEnabled()) logger.trace("searchForObject found the table in the table list and returning true");
                    return true;
                }
            }

        // search for object in schema list
        if (schemas != null)
            {
                // tblNameWithoutNameSpace is of form: cat.sch.tab
                // Remove .tab from it 
                int dotPos = tblNameWithoutNameSpace.lastIndexOf('.');
                dotPos = tblNameWithoutNameSpace.lastIndexOf('.', dotPos+1);
                String schName = tblNameWithoutNameSpace.substring(0, dotPos);

                boolean found = false;
                for (int i = 0; (!found && (i < schemas.length)); i++) {
                    if (logger.isTraceEnabled()) logger.trace("searchForObject ENTRY searching for "
                            + tblName + " against schema " + (String)schemas[i]);
                    if (schName.equals((String) schemas[i]))
                        {
                            found = true;
                        }
                }
                
                if (found){
                    if (logger.isTraceEnabled()) logger.trace("searchForObject found the table in the schema list and returning true");
                    return true;
                }
            }

        if (logger.isTraceEnabled()) logger.trace("searchForObject table not found and returning false");
        return false;
    }

    ///////////////////////////////////////////////////////////////////////////
    // backuptag:   Public interface to get the extendedAttributes information saved
    //              at the time the backup was created.  This contains things
    //              like the userId/userName who issued the backup command as
    //              well as the SQL meta version.  This is a black box to us and
    //              is interpreted entired by SQL.
    ///////////////////////////////////////////////////////////////////////////
    public String getExtendedAttributes(String backuptag) throws Exception{
       if (logger.isTraceEnabled()) logger.trace("[getExtendedAttributes] backupTag:" + backuptag);
       if (backuptag.length() == 0){
          IOException ioe = new IOException("[getExtendedAttributes] called with empty backuptag ");
          if (logger.isInfoEnabled()) logger.info( "Error in [getExtendedAttributes] ", ioe);
          throw ioe;
       }

       try{
          SnapshotMetaStartRecord smsr = sm.getSnapshotStartRecord(backuptag);
          return smsr.getExtendedAttributes();
       }
       catch (FileNotFoundException fnfe){
          // Not in SnapshotMeta.  Look to see if we have the tag in backupsys
          if (logger.isTraceEnabled()) logger.trace("[getExtendedAttributes] calling getExtAttrsFromBackupSys for backupTag:" + backuptag);
          return getExtAttrsFromBackupSys(backuptag);
       }
    }

    // Temp API to be used until SQL calls below API with restoreToTS as a String
    public byte[][] restoreObjects(String backuptag,
            Object[] schemas,
            Object[] tables,
            boolean restoreToTS,
            boolean showObjects,
            boolean saveObjects,
            boolean restoreSavedObjects,
            int numParallelThreads,
            int progressUpdateDelay) throws Exception {

        if (logger.isInfoEnabled()) logger.info( "[RESTOREOBJECTS] (default) :" +
                " backupTag: " + backuptag +
                " schemas: " + ((schemas != null)? String.valueOf(schemas.length): "0") +
                " tables:" + ((tables != null)? String.valueOf(tables.length): "0" )+
                " restoreToTS:" + restoreToTS +
                " showObjects:" + showObjects +
                " saveObjects:" + saveObjects +
                " restoreSavedObjects:" + restoreSavedObjects +
                " numParallel:" + String.valueOf(numParallelThreads));

        String restoreTimeString = null;
        if (restoreToTS){
           restoreTimeString = new String(backuptag);
           backuptag = getPriorBackupTag(restoreTimeString);
           if (logger.isInfoEnabled()) logger.info( "[RESTOREOBJECTS] (default) retrieved backupTag: "
               + backuptag + " for timeString " + restoreTimeString);
        }

        return restoreObjects(backuptag,
                    schemas,
                    tables,
                    restoreTimeString,
                    showObjects,
                    saveObjects,
                    restoreSavedObjects,
                    numParallelThreads,
                    progressUpdateDelay);
    }


    ///////////////////////////////////////////////////////////////////////////
    // backuptag:   tag for the backup to be restored
    // schemas:     list of schemas to be restored. 
    // tables:      list of tables to be restores.
    //              If both schemas and tables are null, then restore all.
    // restoreToTS: If not null, restore to the timestamp specified of
    //              the form:  YYYY-MM-DD hh:mm:ss.ffffff
    // showObjects: Return list of objs to be restored without restoring them
    // saveObjects: Save current objects specified in schemas/tables before
    //              restoring them from snapshots.
    // restoreSavedObjects: restore objects that were previously saved with
    //                      saveObjects specification
    // numParallelThreads:  number of parallel threads to be used to restore.
    //                      1, use serial restore.
    //                      0, use system determined number of threads.
    //                      > 1, use specified number of parallem threads.
    //
    // Return values: null in case of an error or there are no objects to be
    //                restored.
    //                list of objects to be restored, if showObjects is true,
    //                or list of objects that were restored.
    ///////////////////////////////////////////////////////////////////////////
    // A note here on when OperationLock() method is called on various 
    // combination of flags that is passed by the caller. 
    // 
    // showObjects is true, it is read only operation. No operationLock needed.
    //
    // saveObjects is true, this is set by caller to indicate "save a copy" of 
    // objects being restored so that in case of error, recovery can be 
    // performed. In this case, if no error, then finalizeRestore() method is 
    // called by caller. Hence operationLock() can be called here and 
    // OperationUnlock() is called in finalizeRestore. There is a use case where
    // caller does not want to saveObjects but simply restore non-user tables. 
    // In this case, finalizeRestore() is not called by the caller. Hence
    // in this case, operationUnlock() needs to be called before returning.
    //
    // restoreSavedObjects is true, this is a recovery operation. A saved copy
    // of user objects is being restored. It is likely operationLock() was
    // already called before. There is no need to unlock till finalizeRestore()
    // is called.
    ///////////////////////////////////////////////////////////////////////////

    public byte[][] restoreObjects(String backuptag,
                                   Object[] schemas,
                                   Object[] tables,
                                   String restoreToTimeString,
                                   boolean showObjects,
                                   boolean saveObjects,
                                   boolean restoreSavedObjects,
                                   int numParallelThreads,
                                   int progressUpdateDelay) throws Exception {

        if (logger.isInfoEnabled()) logger.info("[RESTOREOBJECTS] :" +
                " backupTag: " + backuptag +
                " schemas: " + ((schemas != null) ? String.valueOf(schemas.length) : "0") +
                " tables:" + ((tables != null) ? String.valueOf(tables.length) : "0") +
                " restoreToTimeString:" + ((restoreToTimeString != null) ? restoreToTimeString : "null") +
                " showObjects:" + showObjects +
                " saveObjects:" + saveObjects +
                " restoreSavedObjects:" + restoreSavedObjects +
                " numParallel:" + String.valueOf(numParallelThreads));

        byte[][] restoredObjList = null;
        int restoredEntry = 0;
        RecoveryRecord rr;
        long limitTimeStamp = -1L;
        boolean operationLocked = false;
        boolean restoringFromSys = false;
        long restoreTS = 0L;
        SnapshotMetaStartRecord lvPreStartRec = null;
        SnapshotMetaStartRecord lvPostStartRec = null;
        String precedingTag = null;
        String followingTag = null;
        boolean restoreToTS = ((restoreToTimeString != null) &&
                (restoreToTimeString.length() > 0));

        boolean restoreOnlyMD = false;
        if ((schemas != null) && (schemas.length == 1)) {
            restoreOnlyMD = isBackupSchema((String)schemas[0], backuptag);
            if (restoreOnlyMD) {
                logger.info("restore backup schema MD for tag: " + backuptag);
            }
        }

        try {
            if (restoreToTS) {

                // First we need to know which SQL metadata to restore, so we need
                // to find the following incremental snapshot
                restoreTS = strToId(restoreToTimeString);
                if (logger.isInfoEnabled())
                    logger.info("restoreObjects(1) restoreToTimeString " + restoreToTimeString + " restoreTS " + restoreTS);
            }

            //Check if this is restore from tag in backupsys
            if (isTagExistBackupSys(backuptag)) {
                if (logger.isInfoEnabled()) logger.info("restoreObjects(2) tag exists in BackupSys.");

                //read BackupSet
                BackupSet bkpset = getBackupFromBackupSys(backuptag);

                if (bkpset == null)
                    throw new IOException("RestoreObjects BackupSet is null");

                if (bkpset.isSysBackup())
                    return systemRestore(backuptag, showObjects, numParallelThreads);

                rr = bkpset.getRecoveryRecord(config);
                if (rr == null) {
                    throw new IOException("RestoreObjects RecoveryRecord is null");
                }

                restoringFromSys = true;

                if (!showObjects) {
                    // It's possible that this imported backup set was dropped locally and has the
                    // mutation files listed as PENDING in the local meta.  Since we are restoring
                    // this set we will reestablish these meta records so they can be used during replay
                    rr.writeMutationRecords(config);
                }
            } else {
                if (logger.isInfoEnabled()) logger.info("restoreObjects(3) tag not exists in BackupSys.");
                //tag must exist in snapshotMeta.

                //first check if restoring SAVE objects.
                if (restoreSavedObjects && saveObjects) {
                    throw new Exception("Cannot Save existing Objects when restoring Saved Objects");
                }

                //update backuptag to SAVE if necessary and use it consistently.
                backuptag = restoreSavedObjects ? (backuptag + "_SAVE_") : backuptag;
                String bkupStatus = getBackupStatus(backuptag);
                if (restoreSavedObjects &&
                        ((bkupStatus.equals("INVALID")) ||
                                (bkupStatus.equals("NOT FOUND")))) {
                    //return at least one entry indicating skipped.
                    //returning null indicates error.
                    //this situation is not an error because saved objects is
                    //invalid means current master copy of the objects is assumed
                    //unchanged. SQL will skip restoring this saved copy and unlock
                    //the restore operation.
                    String restSkip = backuptag + " Status is INVALID or NOT FOUND. Restore skipped";
                    restoredObjList = new byte[1][];
                    restoredObjList[0] = restSkip.getBytes();

                    if (logger.isInfoEnabled()) logger.info("restoreObjects " + restSkip);

                    return restoredObjList;
                }

                //Now retrieve RR for bkpTag
                // We need to pass in showObjects here so we know if the RecoveryRecord
                // is basically a read-only object for display or if it needs to be
                // persistent in the meta tables.
                if ((!showObjects) && (!restoreSavedObjects)) {
                    if (logger.isInfoEnabled())
                        logger.info("restoreObjects(4) calling operationLock(backupTag): " + backuptag);
                    operationLock(backuptag);
                    operationLocked = true;
                }

                rr = new RecoveryRecord(config, backuptag, "RESTORE",
                        /* adjust */ true,
                        /* force */ false,
                        /* restoreTS */ restoreTS,
                        showObjects);
            }
            String preTag = preTagMap.get(backuptag);

            //pit restore to a regular backup
            if(restoreToTS && null != preTag) {
                logger.info("restoreObjects-rb-pit restoreToTS using two tag pre " + preTag + " after " + backuptag + " " +
                        "restoreTS " + restoreTS);
                RecoveryRecord preRR = getRecoveryRecordByTag(preTag, restoreTS, showObjects);
                //merge preRR and rr
                Map<String, TableRecoveryGroup> preMap = preRR.getRecoveryTableMap();
                Map<String, TableRecoveryGroup> afterMap = rr.getRecoveryTableMap();

                //metadata schema name is TRAF_RSRVD_3:TRAFODION._BACKUP_002_00212504560673702527_.OBJECT_PRIVILEGE
                String metadataTagSchema = "_BACKUP_" + backuptag + "_";
                for (Map.Entry<String, TableRecoveryGroup> entry : afterMap.entrySet()) {
                    String tableName = entry.getKey();
                    //meta data table only use after tag
                    if (tableName.contains(metadataTagSchema)) {
                        continue;
                    }
                    TableRecoveryGroup afterGroup = entry.getValue();
                    TableRecoveryGroup preGroup = preMap.get(tableName);
                    if(logger.isDebugEnabled()) {
                        logger.debug("restoreObjects-rb-pit tableName " + tableName + " preGroup  "+ preGroup +"   afterGroup " + afterGroup);
                    }
                    SnapshotMetaRecord afterRoot = afterGroup.getSnapshotRecord();
                    //if snapshot time greater than restoreTS,use previous snapshot previous mutation and after tag shadow mutation
                    // if(afterRoot.getKey() >= restoreTS) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("restoreObjects-rb-pit using two tag mutations tableName " + tableName);
                    }
                    if (null != preGroup) {
                        SnapshotMetaRecord preRoot = preGroup.getSnapshotRecord();
                        List<MutationMetaRecord> mmList = afterGroup.getMutationList();
                        mmList.addAll(preGroup.getMutationList());
                        List<LobMetaRecord> lobList = afterGroup.getLobList();
                        lobList.addAll(preGroup.getLobList());
                        afterGroup = new TableRecoveryGroup(preRoot,
                                new ArrayList<SnapshotMetaIncrementalRecord>()
                                , new ArrayList<>(lobList), new ArrayList<>(mmList));
                        afterMap.put(tableName, afterGroup);
                    }
                    //  }
                }
            }
            // This step to get limitTimeStamp is very critical.  Because there is a procession of time
            // between all of the snapshots captured for the tables we need to use an incremental
            // backup to get a transactionally consistent database when we to not lock the database
            // for transactions (fuzzy backup).  In this case, we use the start time of the incremental backup
            if (useFuzzyIBR && rr.getIncrementalBackup()) {
                limitTimeStamp = rr.getStartTime();
            } else {
                limitTimeStamp = rr.getEndTime();
            }
            if (restoreToTS) {
                limitTimeStamp = restoreTS;
                if (logger.isInfoEnabled())
                    logger.info("restoreObjects using tag " + rr.getTag() + " and restoreTS " + restoreTS);
            }

            //This map is complete set belonging to the tag.
            Map<String, TableRecoveryGroup> recoveryTableMap = rr.getRecoveryTableMap();

            if (recoveryTableMap.size() == 0)
                return null;

            //Build a list which is a subset requested by this restore.
            ArrayList<TableRecoveryGroup> restoreTableList = new ArrayList<TableRecoveryGroup>();

            for (Map.Entry<String, TableRecoveryGroup> tableEntry : recoveryTableMap.entrySet()) {
                String tableName = tableEntry.getKey();
                if (searchForObject(tableName, schemas, tables) == true) {
                    restoreTableList.add(tableEntry.getValue());
                } else {
                    // This table is not in the restore set, so we can delete the TableGroup
                    // from the RecoveryRecord.
                    rr.removeTable(tableName);
                }
            }

            //this sort is needed to display consistent output in logs.
            Collections.sort(restoreTableList, new TableRecoveryGroupCompare());

            restoredObjList = new byte[restoreTableList.size()][];
            restoredEntry = 0;
            for (TableRecoveryGroup t : restoreTableList) {
                String hbaseTableName = t.getSnapshotRecord().getTableName();
                restoredObjList[restoredEntry] = hbaseTableName.getBytes();
                restoredEntry++;

                //Log it only if modifying something to keep the log
                //less confusing.
                if (showObjects == false) {
                    if (logger.isInfoEnabled()) logger.info("restoreObjects backupTag " + backuptag +
                            " TableName: " + hbaseTableName +
                            " SnapshotName: " + t.getSnapshotRecord().getSnapshotPath() +
                            " MutationSize: " + t.getMutationList().size());
                }
            }

            if (showObjects == true) {
                return restoredObjList;
            }

            if (restoreTableList.size() == 0)
                return null;

            //if numParallelThreads is zero, default it to the number of
            //region servers in the cluster.
            if (numParallelThreads <= 0) {
                Admin admin = connection.getAdmin();
                ClusterStatus cstatus = admin.getClusterStatus();
                numParallelThreads = cstatus.getServersSize();
                admin.close();
            }

            this.pit_thread = numParallelThreads;

            //setup thread pool at class level for use by
            //deleteTables and restoreTables.
            threadPool = Executors.newFixedThreadPool(pit_thread == 0 ? 1 : pit_thread);

            Future completeRestoreFuture = null;
            if (!showObjects){

                if (logger.isInfoEnabled())
                    logger.info("restoreObjects calling newSnapshotRecAndMutationClose ");

                newSnapshotRecAndMutationClose(restoreTableList);
                if (logger.isInfoEnabled())
                    logger.info("restoreObjects calling newSnapshotRecAndMutationClose-end ");
                // Now complete the restore operation
                final boolean restoreToTSFinal = restoreToTS;
                final boolean restoreSavedObjectsFinal = restoreSavedObjects;
                final Configuration configFinal = config;
                final RecoveryRecord rrFinal = rr;
                completeRestoreFuture = threadPool.submit(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        logger.info("restoreObjects calling completeRestore-start");
                        rrFinal.completeRestore(configFinal, restoreSavedObjectsFinal, restoreToTSFinal);
                        logger.info("restoreObjects calling completeRestore-end");
                        return 1;
                    }
                });
            }

            //From this point onwards, lock the operation
            //if not locked already, lock it now. This can be a sandbox restore.
            //if restoreSavedObjects, this is a recovery operation, no need to
            //lock again. Operation must be locked already in this case.
            if ((!operationLocked) && (!restoreSavedObjects)) {
                if (logger.isInfoEnabled())
                    logger.info("restoreObjects(5) calling operationLock(backupTag): " + backuptag);
                operationLock(backuptag);
                operationLocked = true;
            }

            //Take a snapshot backup of existing tables
            //before deleting these tables. Applicable only
            //in case of SUBSET restore. SQL sets this flag
            //only in case of SUBSET backup type.
            if (saveObjects) {
                if (logger.isInfoEnabled())
                    logger.info("restoreObjects(6) calling backupTables: " + backuptag + "_SAVE_");
                backupTables(restoreTableList, rr.getSnapshotStartRecord().getExtendedAttributes(), backuptag + "_SAVE_");
            }

            //Drop the tables in this list.
            if (logger.isInfoEnabled()) logger.info("restoreObjects(7) calling deleteTables: " + backuptag);
            deleteTables(restoreTableList, backuptag);

            //Restore the tables in this list.
            if (logger.isInfoEnabled()) logger.info("restoreObjects(8) calling restoreTables: " + backuptag);
            restoreTables(restoreTableList, limitTimeStamp, restoringFromSys,
                    backuptag, progressUpdateDelay, restoreOnlyMD);

            if (null != completeRestoreFuture) {
                completeRestoreFuture.get();
            }

            //if testing, inject error, throws exception.
            //This is to simulate replay engine failure.
            //Goal is backup that gets locked due to this failure
            //can successfully unlock and restore the SAVED_ objects.
            injectError("RESTORE_ERR_INJECT_J2");

            //Once restore is complete(may involve lot of mutation replay),
            //perform a regular backup of the same set, initiated internally
            //from here. This snapshot would be the starting point for new mutations
            //capture.
            //if restoreSavedObjects then no need to perform this operation.
            //Perform this operation only if restoring from backupsys. There is no
            //need of this starting point if restoring from snapshotmeta.
      /* TODO heng.guo: Mantis-17185 M-17184
      // In fact, we does not need to backup after we restore from backupsys.
      if((!restoreSavedObjects) && restoringFromSys) {
        if (logger.isInfoEnabled()) logger.info("restoreObjects(9) calling backupTables: " + backuptag);
        long id = getIdTmVal();
        String btagRestored =  backuptag + "_" + String.valueOf(timeIdVal) + "_RESTORED_";
        backupTables(restoreTableList, rr.getSnapshotStartRecord().getExtendedAttributes(), btagRestored);
      }
      */

        } catch (Throwable e) {
            if (logger.isErrorEnabled()) logger.error("Restore Engine exception in restore snapshot : ", e);
	    //shutdown threadPool first
	    if (threadPool != null) {
		threadPool.shutdownNow();
	    }
            //operation unlock here since already locked.
            //recoverMeta is true because any exception in this method assumes
            //meta has changed. However, this is very important, if saveObjects is
            //true( save objects being replaced) or sql is restoring saved objects
            //(restoreSavedObjects), we do not want to recover meta as part of this
            //exception. If saveObjects is true or restoreSavedObjects is true,
            //recoverMeta will happen as part of finalizeRestore(on success or CLEANUP)
            //call from SQL once the SQL side transaction is committed and cleanup is
            //invoked.
            try {
                if (operationLocked && (!saveObjects) && (!restoreSavedObjects)) {
                    operationLocked = false;
                    if (logger.isInfoEnabled())
                        logger.info("restoreObjects(10) calling operationUnlock(backupTag, true): " + backuptag);
                    operationUnlock(backuptag, true/*recoverMeta*/);
                }
            } catch (IOException ie) {
                e.addSuppressed(ie);
            }
            throw new IOException(e);
        } finally {
            if (threadPool != null)
                threadPool.shutdownNow();

            //if saveObjects is true, then operation is unlocked in
            //finalizeRestore() method subsequently. This is because SQL needs to
            //commit the transaction followed by calling finalizeRestore(). In other
            //words, when saveObjects is true, changes made in this method and SQL side
            //are interdependent.
            //We are assuming here that when control reaches here, the intended saveObjects
            //operation is successful and this restoreObjects method is successful.
            //Any other error conditions is assumed handled in the exception handler
            //above.
            //
            //For all other cases, where saveObjects is not true, unlock here.
            //Saveobjects is true for restore of user tables.
            //Saveobjects is false for restore of backup sql meta tables.
            //RecoverMeta is false, since it is not error condition.
            //Any exceptions here is returned to caller. No need to try catch again.
            //In case of exceptions handled in exception handler above, operationLocked
            //will likely be unlocked before reaching here.
            if (operationLocked && (!saveObjects) && (!restoreSavedObjects)) {
                if (logger.isInfoEnabled())
                    logger.info("restoreObjects(11) calling operationLock(backupTag, false): " + backuptag);
                operationUnlock(backuptag, false/*recoverMeta*/);
                operationLocked = false;
            }
        }
        if (logger.isInfoEnabled()) logger.info("[RESTOREOBJECTS] EXIT");
        // close ttable
        if (ttable != null)
            ttable.close();

        return restoredObjList;
    }

    public boolean finalizeBackup(final String backuptag, 
                                  String backupType)
        throws  MasterNotRunningException,
                IOException,
                Exception, 
                SnapshotCreationException,
                InterruptedException {
      
        // Complete snapshotMeta update.
        if (logger.isInfoEnabled()) logger.info("finalizeBackup  backupTag: " + backuptag +
                    " backupType: " + backupType);
        
        //only applicable to subsets
        //Note SYSTEM_PIT is actually REGULAR type without sql box
        //driven from backup system command.
        if(backupType.equals("REGULAR") || 
           backupType.equals("INCR") ||
           backupType.equals("SYSTEM_PIT")) {
          completeSnapshotMeta();
          if (logger.isInfoEnabled()) logger.info("finalizeBackup calling operationUnlock (tag, false) ; backupTag: " + backuptag);
          operationUnlock(backuptag, false/*recoverMeta*/);
        }
        return true;
    }

    private RecoveryRecord getRecoveryRecordByTag(String backuptag, Long restoreTS, boolean showObjects) throws Exception {
        RecoveryRecord rr = null;
        if(isTagExistBackupSys(backuptag)) {
            if (logger.isInfoEnabled()) logger.info("restoreObjects(2) tag exists in BackupSys.");

            //read BackupSet
            BackupSet bkpset = getBackupFromBackupSys(backuptag);

            if(bkpset == null)
                throw new IOException("RestoreObjects BackupSet is null");

            rr = bkpset.getRecoveryRecord(config);
            if(rr == null) {
                throw new IOException("RestoreObjects RecoveryRecord is null");
            }
        }
        else {
            if (logger.isInfoEnabled()) logger.info("restoreObjects(3) tag not exists in BackupSys.");
            //tag must exist in snapshotMeta.

            rr = new RecoveryRecord(config, backuptag, "RESTORE",
                    /* adjust */ true,
                    /* force */ false,
                    /* restoreTS */ restoreTS,
                    showObjects);
        }
        return rr;
    }

    //Currently finalizeRestore is overloaded. It is called for two
    //different purposes. 
    //1. if Restore operation is successful, this method is called
    //   as final stage cleanup. OperationUnlock(tag, false) called in the end
    //   does not reinstate the saved meta files. This is expected behavior.
    //2. if a backup operation fails or a restore fails, this method is
    //   called to perform cleanup. If it is restore that failed , then SQL
    //   will first restoreObjects( tag_SAVE_) objects before calling 
    //   finalizeRestore here to do final cleanup. If backup operation failed,
    //   OperationUnlock(tag, true) must be called to reinstate the saved meta
    //   files.
    public boolean finalizeRestore(final String backuptag, 
                                  String backupType)
        throws  MasterNotRunningException,
                IOException,
                Exception, 
                SnapshotCreationException,
                InterruptedException {
      
      if (logger.isInfoEnabled()) logger.info("[FINALIZERESTORE]  backupTag: " + backuptag +
                   " backupType: " + backupType);
 
      //Could be SYSTEM, SYSTEM_PIT.
      if(backupType.contains("SYSTEM")) {
    	  //nothing needs to be done in this case since restore of these types of
        //backups goes through different path that requires entire system 
        //initialize, dropped to begin with.
    	  return true;
      }
      
      //Delete the SAVEed backup as part of original restore
      //only applicable to subsets. The _SAVE_ objects are already restored
      //driven by SQL by calling restoreObjects(restoreSavedObjects = true).
      //If records not found, it is likely finalizeRestore is called CLEANUP mode
      //in response to failed restore(_SAVE_ may or may not be present depending 
      //on restore failure happening at different steps).
      try {
        if (logger.isInfoEnabled()) logger.info("finalizeRestore deleting SAVED objects backupTag: " +
                     backuptag+"_SAVE_");

        deleteBackup(backuptag+"_SAVE_", false, false, false, true);
      }catch (Exception e) {
    	String exceptionString = ExceptionUtils.getStackTrace(e);
        if (! exceptionString.contains("Record not found")) {
           //any exception other than records not found throw exception.
          throw new IOException(e);
        }
      }
      
      if(backupType.contains("CLEANUP")) {
    	  //This operationUnlock is called in failure case of 
          //backup or restore. saved meta needs to be recovered( recoverMeta is true).
        if (logger.isInfoEnabled()) logger.info("finalizeRestore calling operationUnlock(backupTag,true): " + backuptag);
        operationUnlock(backuptag, true /*recoverMeta*/);
      }
      else {
    	  //This operationUnlock is called in success case of 
    	  //backup or restore. saved meta is not recovered( recoverMeta is false).
        if (logger.isInfoEnabled()) logger.info("finalizeRestore calling operationUnlock(backupTag, false): " + backuptag);
        operationUnlock(backuptag, false /*recoverMeta*/);
      }
      
      return true;
    }
      
    //Note: ReplayEngine will determine the most recent full backup
    //      to start replay from. No need of passing full backup tag.
    public void catchupReplay(int numParallel, int pv_peer_id,  ArrayList<String> tableList) throws Exception {

      // First, let's get a lock to make sure we don't compete with backup
      boolean lockAcquired = false;
      try{
         lockAcquired = xdcLock();
      }
      catch (Exception le){
         if (logger.isErrorEnabled()) logger.error("catchupReplay exception acquiring lock: ", le);
         throw new IOException(le);
      }

      timeIdVal = getIdTmVal(); //choose latest timeId to catch up to.
      if (numParallel <= 1) numParallel = 8;

      // no need for pit_parallel, the min pool for xdc replay is set to 8
      
      if (logger.isDebugEnabled())
        logger.debug("catchupReplay timeIdVal :" + timeIdVal );
      boolean lv_skip_snapshot = true;
      try {
		new ReplayEngine(timeIdVal,
						    numParallel, 
						    pv_peer_id, 
						    lv_skip_snapshot, 
						    m_verbose,
                            tableList);
      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("catchupReplay Exception: ", e);
        throw new IOException(e);
      }
      
    }
    
      public byte[][] restoreToTimeStamp(String timestamp, int numParallel,
                                   boolean showObjects) throws Exception {
      if (logger.isInfoEnabled()) logger.info("[RESTORETOTIMESTAMP] Timestamp: " + timestamp + " concurrent threads: " + numParallel +
                  " showObjects " + showObjects);
      
      byte[][] restoredObjList = null;
      int restoredEntry = 0;
      IdTmId idtmid = new IdTmId();

      //2019-01-26:17:26:22 convert to 2019-01-26 17:26:22
      if(timestamp.charAt(10) == ':') {
        String beginstr = timestamp.substring(0,10);
        String endstr = timestamp.substring(11);
        timestamp = beginstr + " " + endstr;
        if (logger.isInfoEnabled()) logger.info("restoreToTimeStamp adjusted timestamp beginstr:"
                    + beginstr + " endstr:" + endstr + " timestamp: " 
                    + timestamp);
      }
      
      if (!timestamp.contains(".")) {
        timestamp = timestamp + ".000000";
      }
      idServer.strToId(ID_TM_SERVER_TIMEOUT, idtmid, timestamp);
      if (logger.isInfoEnabled()) logger.info("restoreToTimeStamp idtmid :" + idtmid.val + " Timestamp : " + timestamp );

      //generate list of objects that will be restored.
      RecoveryRecord recoveryRecord = new RecoveryRecord(idtmid.val);
      
      Map<String, TableRecoveryGroup> recoveryTableMap = recoveryRecord.getRecoveryTableMap();

      if (recoveryTableMap.size() == 0)
        return restoredObjList;
        
      restoredObjList = new byte[recoveryTableMap.size()][];
      for (Map.Entry<String, TableRecoveryGroup> tableEntry : recoveryTableMap.entrySet()) {
        String hbaseTableName = tableEntry.getKey();
        TableRecoveryGroup t = tableEntry.getValue();
        restoredObjList[restoredEntry] = hbaseTableName.getBytes();
        restoredEntry++;
          
        if (logger.isInfoEnabled()) logger.info("restoreToTimestamp objects " +
                    " TableName: " + hbaseTableName +
                    " SnapshotName: " + t.getSnapshotRecord().getSnapshotPath() +
                    " MutationSize: " + t.getMutationList().size());
      }

      if (showObjects == true) {
        return restoredObjList;
      }

      //Proceed to restore.
      int pit_parallel = 0;
      String pitParallel = System.getenv("TM_PIT_PARALLEL");
      if (pitParallel != null) {
    	  pit_parallel = Integer.parseInt(pitParallel);
    	  if(logger.isDebugEnabled()) logger.debug("PIT parallel value set to: " + pit_parallel);
      }
      if (logger.isInfoEnabled()) logger.info("PIT parallel value set to: " + pit_parallel);

      try {
          if (numParallel <= 1) {
	        new ReplayEngine(idtmid.val, /* peerId */ 0, /* skip_snapshot */ false, /* verbose */ false);
          }
          else {
               if (pit_parallel == 0)  {
		          new ReplayEngine(idtmid.val, numParallel, /* peerId */ 0, /* skip_snapshot */ false, /* verbose */ false);
               }
               else {
		          new ReplayEngine(idtmid.val, numParallel, pit_parallel,  /* peerId */ 0, /* skip_snapshot */ false, /* verbose */ false);
               }
          }
      } catch (Exception e) {
          if (logger.isErrorEnabled()) logger.error("restoreToTimeStamp Timestamp: " + timestamp + " Exception: ", e);
          throw new IOException(e);
      }
      
      return restoredObjList;
    }

    public void deleteRecoveryRecord(RecoveryRecord rr) throws Exception {
       if (logger.isDebugEnabled())
          logger.debug("deleteRecoveryRecord ENTRY : " + rr);
       FileSystem fs = FileSystem.get(config);
       Admin admin = connection.getAdmin();
       String hdfsRoot = config.get("fs.default.name");
       Path hdfsRootPath = new Path(hdfsRoot);
       final HdfsAdmin hdfsAdmin = new HdfsAdmin ( hdfsRootPath.toUri(), config);
       final FileSystem hdfsRootFs = FileSystem.get(hdfsRootPath.toUri(), config);
       List<Long> snapshotDeletes = new ArrayList<>();
       try {
          Map<String, TableRecoveryGroup> recoveryTableMap = rr.getRecoveryTableMap();
          if (logger.isDebugEnabled())
              logger.debug("deleteRecoveryRecord recoveryTableMap size is " + recoveryTableMap.size());
          for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet()){
              String tableName = tableEntry.getKey();
              TableRecoveryGroup tableGroup = tableEntry.getValue();
              if (logger.isDebugEnabled())
                  logger.debug("deleteRecoveryRecord got TableGroup " + tableGroup);

              boolean foundIncremental = false;
              // If there is an incremental record for this table, then we leave the mutations
              if (tableGroup.getSnapshotIncrementalList() != null){
                 List<SnapshotMetaIncrementalRecord> smil = tableGroup.getSnapshotIncrementalList();
                 ListIterator<SnapshotMetaIncrementalRecord> smilIterator = smil.listIterator();
                 while (smilIterator.hasNext()){
                    foundIncremental = true;
                    SnapshotMetaIncrementalRecord smir = smilIterator.next();
                    // This is an incremental incremental snapshot record.
                    // We can delete it from the SNAPSHOT meta table
                    // and in the incremental list for the tableGroup, but
                    // we do not remove the mutations until we are deleting the
                    // root snapshot.
                    if (smir.getUserTag().equals(rr.getTag())){
                       if (logger.isDebugEnabled())
                            logger.debug("deleteRecoveryRecord leaving mutations, but deleting IncrementalRecord from table " + smir);
                       snapshotDeletes.add(smir.getKey());
                       smilIterator.remove();
                    }
                    else {
                       if (logger.isDebugEnabled())
                            logger.debug("deleteRecoveryRecord  tag " + rr.getTag()
                              + " leaving incremental record for tag " + smir.getUserTag()
                              + " from table " + smir.getTableName());
                    }
                 }
              } // if (rr.getSnapshotIncrementalList() != null){

              if (! foundIncremental){
                 // Now we remove the physical snapshot and SnapshotMetaRecord from the META
                 SnapshotMetaRecord tableMeta = tableGroup.getSnapshotRecord();
                 if (logger.isDebugEnabled())
                    logger.debug("deleteRecoveryRecord got SnapshotMetaRecord " + tableMeta);

                 // First remove any LOB snapshots
                 if (tableMeta.getNumLobs() > 0){
                    Set<Long> lobSnaps = tableMeta.getLobSnapshots();
                    for (Long currLobKey : lobSnaps){
                       try{
                          if (currLobKey ==  -1L){
                             continue;
                          }
                          LobMetaRecord lobRec = lm.getLobMetaRecord(currLobKey);
                          if (lobRec != null){
                             String hdfsPathStr = lobRec.getHdfsPath();
                             String lobSnapshotName = lobRec.getSnapshotName();
                             Path lobSnapPath = new Path(hdfsPathStr);
                             hdfsRootFs.deleteSnapshot(lobSnapPath, lobSnapshotName);
                             if (logger.isInfoEnabled()) logger.info("Deleted LOB Snapshot " + lobSnapshotName + " Table Name: " + tableName);
                             lm.deleteLobMetaRecord(currLobKey);
                          }
                       }
                       catch(SnapshotDoesNotExistException se){
                          if (logger.isDebugEnabled())
                             logger.debug("deleteRecoveryRecord LOB snapshot does not exist, ignoring");
                       }
                    }
                 }

                 String snapshotPath = tableMeta.getSnapshotPath();
                 if (logger.isDebugEnabled())
                    logger.debug("deleteRecoveryRecord got path " + snapshotPath);

                 try{
                    admin.deleteSnapshot(snapshotPath);
                    if (logger.isInfoEnabled()) logger.info("Deleted Snapshot " + snapshotPath + " Table Name: " + tableName);
                 }
                 catch(SnapshotDoesNotExistException se){
                    if (logger.isDebugEnabled())
                        logger.debug("deleteRecoveryRecord snapshot does not exist, ignoring");
                 }

                 if (logger.isDebugEnabled())
                    logger.debug("deleteRecoveryRecord deleting snapshotRecord " + tableMeta.getKey());
                 snapshotDeletes.add(tableMeta.getKey());
                 // All the mutation files hung of this SnapshotRecord need to be
                 // hung off the previous snapshot record.
                 List<MutationMetaRecord> mutationList = tableGroup.getMutationList();

                 if (mutationList.size() > 0){

                    if (logger.isDebugEnabled())logger.debug("deleteRecoveryRecord : "
                        + mutationList.size() + " mutation files for " + tableName);
                    SnapshotMetaRecord newTableMeta = sm.getRelativeSnapshotRecord(tableName,
                                                     tableMeta.getKey(), true /* before */);
                    if (logger.isDebugEnabled())logger.debug("deleteRecoveryRecord : "
                            + " found new SnapshotMetaRecord " + newTableMeta);
                    if (newTableMeta != null){
                       for (int i = 0; i < mutationList.size(); i++) {
                          MutationMetaRecord mutationRecord = mutationList.get(i);

                          // It's possible the mutation records have changed underneath us, so
                          // we must check the shadow table for the latest version of the record
                          MutationMetaRecord mmRecShadow = mm.getMutationRecord(mutationRecord.getRowKey(), true /*
                          readShadow */);
                          if (mmRecShadow != null){
                             mutationRecord = mmRecShadow;
                             if (logger.isDebugEnabled())logger.debug("deleteRecoveryRecord mutationRecord updated from shadow table " + mutationRecord);
                          }

                          mutationRecord.setBackupSnapshot(newTableMeta.getKey());
                          mutationRecord.setPriorIncrementalSnapshot(-1L);
                          mutationRecord.setUserTag(newTableMeta.getUserTag());
                          mm.putMutationRecord(mutationRecord, true /* blind write */);
                       }
                    }
                    else {
                       // There is no prior SnapshotMetaRecord, so these mutations are not useful
                       for (int i = 0; i < mutationList.size(); i++) {
                          MutationMetaRecord mutationRecord = mutationList.get(i);
                          String mutationPathString = mutationRecord.getMutationPath();
                          Path mutationPath = new Path (new URI(mutationPathString).getPath());

                          // Delete mutation file
                          if (logger.isDebugEnabled())
                             logger.debug("deleteRecoveryRecord deleting mutation file without prior snapshot at "
                                            + mutationPath);
                          if (fs.exists(mutationPath))
                            fs.delete(mutationPath, false);
                          else
                            if (logger.isWarnEnabled()) logger.warn("mutation file: " + mutationPath + " not exists.");

                          // Delete mutation record
                          if (logger.isDebugEnabled())
                             logger.debug("deleteRecoveryRecord deleting mutationMetaRecord without prior snapshot "
                                            + mutationRecord);
                          MutationMeta.deleteMutationRecord(mutationRecord.getRowKey());
                       }
                    }
                 } //if (mutationList.size() > 0){
              } // if (! foundIncremental)
          } // for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet())

          //delete records
          if(snapshotDeletes.size() > 0){
              SnapshotMeta.deleteRecordsKeys(snapshotDeletes);
          }
       }
        catch (Exception e) {
          if (logger.isErrorEnabled()) logger.error("deleteRecoveryRecord Exception occurred " ,e);
          e.printStackTrace();
          throw new IOException(e);
       }
       finally{
         admin.close();
       }
    }

    public boolean deleteBackup(String timestamp) throws Exception {
      if (logger.isDebugEnabled())
         logger.debug("deleteBackup Timestamp: " + timestamp);
      int timeout = ID_TM_SERVER_TIMEOUT;
      boolean cb = false;
      IdTm cli = new IdTm(cb);
      IdTmId idtmid = new IdTmId();
      cli.strToId(timeout, idtmid, timestamp);
      if (logger.isDebugEnabled())
        logger.debug("deleteBackup idtmid: " + idtmid.val + " Timestamp: " + timestamp );
      RecoveryRecord rr = new RecoveryRecord(idtmid.val);
      deleteRecoveryRecord(rr);
//      sm.deleteSnapshotStartRecord(tag);
      return true;
    }

    public boolean deleteBackup(String backuptag, boolean ts,
        boolean cascade, boolean force) throws Exception {

      return deleteBackup(backuptag, ts, cascade, force, false);
          
    }

    public byte[][] getBackupNameLink(String backupTag) throws Exception {
        List<String> tagNameList = new ArrayList<>(64);
        List<SnapshotTagIndexRecord> deleteTagList = getBackupLink(backupTag);
        for (SnapshotTagIndexRecord indexRecord : deleteTagList) {
            tagNameList.add(SnapshotTagIndexRecord.getTagName(indexRecord.getKey()));
        }

        byte[][] backupTagsList = new byte[tagNameList.size()][];
        int index = 0;
        for (String name : tagNameList) {
            backupTagsList[index] = name.getBytes();
            index++;
        }
        return backupTagsList;
    }

    private List<SnapshotTagIndexRecord> getBackupLink(String backupTag) throws Exception {
        List<SnapshotTagIndexRecord> deleteTagList = new ArrayList<>(64);
        // checking tagName in C program
        SnapshotTagIndexRecord tagIndex = sm.getSnapshotTagIndex(backupTag);
        if (tagIndex == null) {
            return deleteTagList;
        }
        // scan sm table, get index
        List<SnapshotTagIndexRecord> indexList = sm.getSnapshotTagIndexList(null, null);
        if (indexList.size() == 0) {
            return deleteTagList;
        }
        /*
         * Can be replaced with lambda expressions, but EsgynDB does not support JDK1.8.
         * Sort to determine the tag to be deleted.
         */
        indexList.sort(new Comparator<SnapshotTagIndexRecord>() {
            @Override
            public int compare(SnapshotTagIndexRecord o1, SnapshotTagIndexRecord o2) {
                return Long.compare(o1.getStartKey(), o2.getStartKey());
            }
        });

        deleteTagList.add(tagIndex);
        // Delete all increments between the current full backup and the next full backup
        for (SnapshotTagIndexRecord tagIndexRecord : indexList) {
            if (tagIndex.getStartKey() >= tagIndexRecord.getStartKey()) { continue; }
            SnapshotMetaStartRecord startRecord = sm.getSnapshotStartRecord(tagIndexRecord.getStartKey());
            if (startRecord == null) { continue; }
            // field incrementalBackup is false, tag is regular
            if (!startRecord.getIncrementalBackup()) { break; }
            deleteTagList.add(tagIndexRecord);
        }
        return deleteTagList;
    }

    private boolean deleteBackupForce(String backupTag) throws Exception {
        if (logger.isInfoEnabled())
            logger.info("deleteBackupForce start, backupTag:" + backupTag);
        List<SnapshotTagIndexRecord> deleteTagList = getBackupLink(backupTag);
        if (deleteTagList.size() == 0) { return false; }
        Admin hbaseAdmin = connection.getAdmin();
        List<String> tagNameList = new ArrayList<>(64);
        List<String> hbaseSnapshotNameList = new ArrayList<>(deleteTagList.size());
        List<Delete> snapshotMetaDeleteList = new ArrayList<>(deleteTagList.size());
        for (SnapshotTagIndexRecord indexRecord : deleteTagList) {
            String tagName = SnapshotTagIndexRecord.getTagName(indexRecord.getKey());
            tagNameList.add(tagName);
            for (Map.Entry<Long,String> entry : sm.getSnapshotMetaList(indexRecord.getStartKey(), indexRecord.getEndKey()).entrySet()) {
                // startRowKey and stopRow need be deleted after this
                snapshotMetaDeleteList.add(new Delete(Bytes.toBytes(entry.getKey())));
                if (AbstractSnapshotRecordMeta.getRecordTypeByValue(entry.getValue()) == SnapshotMetaRecordType.SNAPSHOT_RECORD) {
                    try {
                        org.apache.hadoop.hbase.pit.meta.SnapshotMetaRecord record = new org.apache.hadoop.hbase.pit.meta.SnapshotMetaRecord(entry.getValue());
                        hbaseSnapshotNameList.add(record.getSnapshotPath());
                    } catch (IOException e) {
                        if (logger.isErrorEnabled())
                            logger.error("column value has illegal type");
                    }
                }
            }
        }
        // add brc record
        Pair<byte[],byte[]> brcRange = sm.getRegularRangeByTagName(backupTag);
        List<byte[]> brcRowKeyList = sm.getBrcRowKey(brcRange.getFirst(), brcRange.getSecond());
        if (logger.isInfoEnabled())
            logger.info("deleteBackupForce brcRowKeyList: " + brcRowKeyList.size());
        for (byte[] rowKey : brcRowKeyList) {
            snapshotMetaDeleteList.add(new Delete(rowKey));
        }
        if (logger.isInfoEnabled())
            logger.info("deleteBackupForce deleteTagList: " + tagNameList);
        // scan mutation table
        Map<byte[],String> mutationMap = mm.getMutationMetaByTag(tagNameList);
        List<Delete> mutationMetaDeleteList = new ArrayList<>(deleteTagList.size());
        List<String> mutationHdfsPathList = new ArrayList<>(deleteTagList.size());

        for (Map.Entry<byte[],String> entry : mutationMap.entrySet()) {
            mutationMetaDeleteList.add(new Delete(entry.getKey()));
            mutationHdfsPathList.add(entry.getValue());
        }

        if (logger.isInfoEnabled())
            logger.info("deleteBackupForce delete start, snapshotMetaDeleteList:" + snapshotMetaDeleteList.size()
                + "\nhbaseSnapshotNameList:" + hbaseSnapshotNameList.size() + "\nmutationHdfsPathList:" + mutationHdfsPathList.size());

        try {
            long time1 = System.currentTimeMillis();
            SnapshotMeta.deleteRecords(snapshotMetaDeleteList);
            long time2 = System.currentTimeMillis();
            if (logger.isInfoEnabled())
                logger.info("SnapshotMeta.deleteRecords cost : " + (time2 - time1));
            for (String snapshotName : hbaseSnapshotNameList) {
                try {
                    hbaseAdmin.deleteSnapshot(snapshotName);
                } catch (Exception e) {
                    if (logger.isWarnEnabled())
                        logger.warn("hbaseAdmin.deleteSnapshot failed, snapshotName:" +snapshotName +"Exception:\n" + e);
                }
            }
            long time3 = System.currentTimeMillis();
            if (logger.isInfoEnabled())
                logger.info("hbaseAdmin.deleteSnapshot cost : " + (time3 - time2));
            for (String tagName : tagNameList) {
                hbaseAdmin.disableTables("TRAF_RSRVD_3:TRAFODION\\._BACKUP_" + tagName + "_\\..+");
                hbaseAdmin.deleteTables("TRAF_RSRVD_3:TRAFODION\\._BACKUP_" + tagName + "_\\..+");
            }
            long time4 = System.currentTimeMillis();
            if (logger.isInfoEnabled())
                logger.info("hbaseAdmin.deleteTable cost : " + (time4 - time3));
            for (String filePath : mutationHdfsPathList) {
                try {
                    FileSystem.get(config).delete(new Path(new URI(filePath).getPath()), false);
                } catch (Exception e) {
                    if (logger.isWarnEnabled())
                        logger.warn("delete mutation file failed, filePath:" + filePath +"Exception:\n" + e);
                }
            }
            long time5 = System.currentTimeMillis();
            if (logger.isInfoEnabled())
                logger.info("FileSystem.get(config).delete cost : " + (time5 - time4));
            // finally, delete startKey and endKey
            List<Delete> startAndStopDeleteList = new ArrayList<>(deleteTagList.size());
            for (SnapshotTagIndexRecord indexRecord : deleteTagList) {
                startAndStopDeleteList.add(new Delete(Bytes.toBytes(indexRecord.getStartKey())));
                startAndStopDeleteList.add(new Delete(Bytes.toBytes(indexRecord.getEndKey())));
                // delete tag index
                startAndStopDeleteList.add(new Delete(Bytes.toBytes(indexRecord.getKey())));
            }
            if (logger.isInfoEnabled())
                logger.info("deleteBackupForce , mutationMetaDeleteList:" + mutationMetaDeleteList.size() +
                    ", startAndStopDeleteList:" + startAndStopDeleteList.size());
            MutationMeta.deleteRecords(mutationMetaDeleteList);
            SnapshotMeta.deleteRecords(startAndStopDeleteList);
            if (logger.isInfoEnabled())
                logger.info("deleteRecords cost : " + (System.currentTimeMillis() - time5));
        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error("deleteBackupForce, delete failed," + e);
            return false;
        } finally {
            hbaseAdmin.close();
        }

        return true;
    }

    private boolean deleteBackup(String backuptag, boolean ts,
                                boolean cascade, boolean force,
                                boolean skipOperationLockUnlock) throws Exception {
        long startTime = System.currentTimeMillis();
        // backup tag name cannot contain ":".
        // If the src name is prefixed by "namespace:", replace
        // ":" by "_"
        // TODO: Modified by heng.guo to solve Mantis#15102
        if (backuptag.contains(":"))
            backuptag = backuptag.replace(":", "_");

        if (logger.isInfoEnabled()) logger.info("[DELETEBACKUP] backup: " + backuptag +
                    " timestamp: " + ts +
                    " cascade: " + cascade +
                    " force: " + force +
                    " skipLock: " + skipOperationLockUnlock);

        boolean operationLocked = false;
        
        FileSystem fs = FileSystem.get(config);

        //ts indicates, delete all backups up to the timestamp
        if(ts)  {
          // Convert the string timestamp to a long
          int timeout = ID_TM_SERVER_TIMEOUT;
          boolean cb = false;
          IdTm cli = new IdTm(cb);
          IdTmId idtmid = new IdTmId();
          cli.strToId(timeout, idtmid, backuptag);

          // get all backup snapshots 
          ArrayList<SnapshotMetaStartRecord> snapshotStartList = null;
          snapshotStartList = sm.getAllPriorSnapshotStartRecords(idtmid.val);

          for (SnapshotMetaStartRecord s : snapshotStartList) {
            String userTag = s.getUserTag();
            // 'drop backup' doesn't allow 'show objects' syntax, so showOnly must be false
            RecoveryRecord rr = new RecoveryRecord(config, userTag, "DROP",
                   /* adjust */ true,
                   force,
                   /* restoreTS */ 0L,
                   /* showOnly */ false);
            deleteRecoveryRecord(rr);
            sm.deleteSanpShotStartRecord(rr.getSnapshotStartRecord().getKey());
          }
        }
        else  {

          //Check if this tag exists in backupsys and delete from there.
          //tags that exist in backupsys are imported or system backups.
          //no need to resolve dependencies. For now enforce force option. 
          //cascade option not applicable.
          if(isTagExistBackupSys(backuptag)) {
            
            //if(!force)   //FOR NOW TBD
            //  throw new IOException("Imported backups can only be dropped with force option");
            
            deleteBackupFromBackupSys(backuptag);
            
            return true;
          }
          
          try {
              // force is true, ts is false,delete
              if (force) {
                  long startDeleteTime = System.currentTimeMillis();
                  if (!deleteBackupForce(backuptag)) {
                      throw new IOException("force delete fail!");
                  }
                  if (logger.isInfoEnabled())
                      logger.info("deleteBackupForce cost:" + (System.currentTimeMillis() - startDeleteTime));
              } else {
                  if(!skipOperationLockUnlock) {
                      operationLock(backuptag);
                      operationLocked = true;
                  }
                  // 'drop backup' doesn't allow 'show objects' syntax, so showOnly must be false
                  RecoveryRecord rr = new RecoveryRecord(config, backuptag, "DROP",
                          /* adjust */ true,
                          force,
                          /* restoreTS */ 0L,
                          /* showOnly */ false);

                  // Need to make sure that none of the snapshots have dependent incremental backups
                  List<SnapshotMetaRecord> updateSnapshotMetaRecords = new ArrayList<>();
                  Map<String, TableRecoveryGroup> tableMap =  rr.getRecoveryTableMap();
                  for (Map.Entry<String, TableRecoveryGroup> tableEntry :  tableMap.entrySet()) {
                      String tableName = tableEntry.getKey();
                      TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();
                      Set<Long> dependentSnapshots = tableRecoveryGroup.getSnapshotRecord().getDependentSnapshots();
                      if (tableRecoveryGroup.getSnapshotRecord().getUserTag().equals(backuptag)) {
                          // We are trying to delete this snapshot, not one of the incrementals
                          Iterator<Long> dependencyIterator = dependentSnapshots.iterator();
                          List<SnapshotMetaIncrementalRecord> irs = sm.getIncrementalSnapshotRecords(dependentSnapshots);
                          Map<Long,SnapshotMetaIncrementalRecord> irsMap = new HashMap<>();
                          for (SnapshotMetaIncrementalRecord ir : irs) {
                              irsMap.put(ir.getKey(),ir);
                          }
                          while (dependencyIterator.hasNext()){
                              Long ss = dependencyIterator.next();
                              if (ss.longValue() != -1L){
                                  // We found a dependent snapshot, so this snapshot can't be deleted.
                                  // First, let's make sure the snapshot actually exists
                                  SnapshotMetaIncrementalRecord ir = irsMap.get(ss.longValue());
                                  if (ir == null){
                                      // We have a dependency listed, but that record is not in the SNAPSHOT table.
                                      // Let's log a warning event and allow the delete
                                      if (logger.isErrorEnabled()) logger.error("deleteBackup BackupTag: " + backuptag
                                              + " has a table " + tableName + " with dependent backup for record " + ss.longValue()
                                              + ", but that record is not found; continuing ");
                                      dependencyIterator.remove();
                                      if(dependentSnapshots.isEmpty()){
                                          dependentSnapshots.add(-1L);
                                      }
                                      tableRecoveryGroup.getSnapshotRecord().setDependentSnapshots(dependentSnapshots);
                                      updateSnapshotMetaRecords.add(tableRecoveryGroup.getSnapshotRecord());
                                      continue;
                                  }
                                  // Let's get the tag for the dependent snapshot so it makes sense to the user
                                  Exception e = new Exception("deleteBackup BackupTag: " + backuptag + " has a table "
                                          + tableName + " with dependent backup " + ir.getUserTag()
                                          + ".  Total dependencies: " + dependentSnapshots.size());
                                  if (logger.isErrorEnabled()) logger.error("Exception in deleteBackup ", e);
                                  throw e;
                              }
                          }
                      }
                      // Remove our dependency from the root
                      if (logger.isDebugEnabled())
                          logger.debug("deleteBackup looking for incremental backups for  "
                                  + tableRecoveryGroup.getSnapshotRecord().getTableName());
                      ArrayList<SnapshotMetaIncrementalRecord> incrSnapshotList = tableRecoveryGroup.getSnapshotIncrementalList();
                      if (incrSnapshotList != null){
                          ListIterator<SnapshotMetaIncrementalRecord> listIterator = incrSnapshotList.listIterator();
                          SnapshotMetaIncrementalRecord targetRec = null;
                          while (listIterator.hasNext()){
                              SnapshotMetaIncrementalRecord ir = listIterator.next();
                              if (ir == null){
                                  if (logger.isDebugEnabled()) logger.debug("deleteBackup removing null incremental record for tag " + backuptag
                                          + " and table "+ tableRecoveryGroup.getSnapshotRecord().getTableName());
                                  listIterator.remove();
                                  continue;
                              }
                              if (ir.getUserTag().equals(backuptag)){
                                  targetRec = ir;
                                  long rootKey = targetRec.getRootSnapshotId();
                                  SnapshotMetaRecord rootRecord = sm.getSnapshotRecord(rootKey);
                                  if (rootRecord == null) {
                                      IOException ioe =  new IOException("NullPointerException on rootRecord ");
                                      if (logger.isErrorEnabled()) logger.error("Error deleteBackup rootRecord record not found in SNAPSHOT table, but is listed in incrementalRecord "
                                              + targetRec + " ", ioe);
                                  }
                                  else {
                                      dependentSnapshots = rootRecord.getDependentSnapshots();
                                      if (logger.isDebugEnabled())
                                          logger.debug("deleteBackup removing dependency "
                                                  + targetRec.getKey() + " from " + rootRecord.getTableName());
                                      dependentSnapshots.remove(targetRec.getKey());
                                      if (dependentSnapshots.isEmpty()){
                                          // We removed the last dependency.  We can signify no dependencies
                                          // by adding key -1.
                                          dependentSnapshots.add(-1L);
                                          if (logger.isDebugEnabled())
                                              logger.debug("deleteBackup adding dependency -1 to " + rootRecord.getTableName());
                                      }
                                      rootRecord.setDependentSnapshots(dependentSnapshots);
                                      updateSnapshotMetaRecords.add(rootRecord);
                                  }
                              } //if (ir.getUserTag().equals(backuptag)){
                          } // while (listIterator.hasNext()){
                      } // if (incrSnapshotList != null){

                      // Now look for any LOB objects to delete

                      if (tableRecoveryGroup.getMutationList().size() > 0){
                          if (logger.isDebugEnabled())
                              logger.debug("deleteBackup deleting excluded mutations for  " + tableRecoveryGroup);
                          List<MutationMetaRecord>mutationList = tableRecoveryGroup.getMutationList();
                          ListIterator<MutationMetaRecord> mListIterator = mutationList.listIterator();
                          while (mListIterator.hasNext()){
                              MutationMetaRecord mmRec = mListIterator.next();

                              if (logger.isTraceEnabled()) logger.trace("deleteBackup removing mutation "
                                      + mmRec);
                              String mutationPathString = mmRec.getMutationPath();
                              Path mutationPath = new Path (new URI(mutationPathString).getPath());

                              if (logger.isDebugEnabled())
                                  logger.debug("deleteBackup deleting mutation file at " + mutationPath);

                              if (fs.exists(mutationPath))
                                fs.delete(mutationPath, false);
                              else
                                if (logger.isWarnEnabled()) logger.warn("mutation file: " + mutationPath + " not exists.");

                              // Delete mutation record
                              if (logger.isDebugEnabled())
                                  logger.debug("deleteBackup deleting mutationMetaRecord " + mmRec);
                              MutationMeta.deleteMutationRecord(mmRec.getRowKey());
                          }
                          if (mutationList.size() > 0){
                              SnapshotMetaRecord root = tableRecoveryGroup.getSnapshotRecord();
                              SnapshotMetaRecord prevRoot = sm.getRelativeSnapshotRecord(root.getTableName(), root.getKey(), true);
                              if (prevRoot != null){
                                  long timeIdVal = getIdTmVal();
                                  prevRoot.setHiatusTime(timeIdVal);
                                  if (logger.isDebugEnabled())
                                      logger.debug("deleteBackup setting hiatus on previous record because mutations have been deleted from current root " + prevRoot);
                                  updateSnapshotMetaRecords.add(prevRoot);
                              }
                          }
                      }
                  } // for (Map.Entry<String, TableRecoveryGroup> tableEntry :  tableMap.entrySet())

                  //batch put snapshotmetarecords
                  if(updateSnapshotMetaRecords.size() > 0){
                      sm.batchPutSnapshotRecords(updateSnapshotMetaRecords);
                  }
                  long startKey = rr.getSnapshotStartRecord().getKey();
                  deleteRecoveryRecord(rr);

                  // Snapshot
                  sm.deleteSanpShotStartRecord(startKey);
              }
          } catch (Exception e) {
            try {
              if(operationLocked){
                operationLocked = false;
                operationUnlock(backuptag, true /*recover*/);
              }
            }catch (IOException ie) {
              e.addSuppressed(ie);
            }
            throw new IOException(e);
          } finally {
            if (!force) {
                if (operationLocked) {
                    operationUnlock(backuptag, false /*recover*/);
                    operationLocked = false;
                }
            }
          }
        }
        if (logger.isInfoEnabled())
            logger.info("deleteBackup cost:" + (System.currentTimeMillis() - startTime));
        return true;
    }
    
    private boolean isTableIncremental(String TableName, List<SnapshotMetaIncrementalRecord>  smirList) {
      
      for (int i = 0; i < smirList.size(); i++) {
        SnapshotMetaIncrementalRecord smir = smirList.get(i);
        if(smir.getTableName().equals(TableName))
            return true;
      }
      return false;
    }
    

    private boolean exportSysBackup(final String backuptag, String dest,
                                int numParallelThreads, boolean override) 
                                throws Exception {

      //Now validate destination.
      ExecutorService tPool = null;

      String hdfsRoot = config.get("fs.default.name");
      String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";
      
      try {
        //Validate source.
        //Source file system access setup.
        Path srcRoot = new Path(backupsysRoot);
        FileSystem srcFs = FileSystem.get(srcRoot.toUri(), config);
        String srcBackupTag = backupsysRoot + "/" + backuptag;
          
        //validate backupMeta exists at source.
        Path srcBackupMetafile = new Path(srcBackupTag, "backupMeta.bin");
        if(!srcFs.exists(srcBackupMetafile))
          throw new IOException("Backup location is invalid: " + srcBackupTag);
          
        //Next read in source backupMeta.
        FSDataInputStream is = srcFs.open(srcBackupMetafile);
        ObjectInputStream ois = new ObjectInputStream(is);
        BackupSet bkpset = (BackupSet)ois.readObject();
        ois.close();
        is.close();  
        
        if(!bkpset.isSysBackup()) {
          throw new IOException("Backup tag: " + backuptag + " is not system backup.");
        }
        
        if(bkpset.isImported()) {
          throw new IOException("Backup tag: " + backuptag + " is already imported backup.");
        }
      
        if(dest.endsWith("/"))
          dest = dest.substring(0,dest.length() -1);

        Path destRootPath = new Path(dest);
        FileSystem destFs = FileSystem.get(destRootPath.toUri(), config);
        String destBackupTag = dest + "/" + backuptag;
        Path backupMetafile = new Path(destBackupTag, "backupMeta.bin");
        if(destFs.exists(backupMetafile)) {
           if (logger.isDebugEnabled()) logger.debug("Files exist for tag " + backuptag + " at: " + destBackupTag);
           if (override){
              deleteDir(destBackupTag, true /*recursive */);
           }
           else{
              throw new IOException("Backup already exists at this location: " + backupMetafile);
           }
           if (logger.isDebugEnabled()) logger.debug("Files deleted for tag " + backuptag + " at: " + destBackupTag);
        }

        FSDataOutputStream bkpsetos = destFs.create(backupMetafile);

        pit_thread = numParallelThreads;
        pit_thread = (pit_thread == 0) ? 1 : pit_thread;
        if (logger.isInfoEnabled()) logger.info("exportBackup parallel threads : " + pit_thread);

        tPool = Executors.newFixedThreadPool(pit_thread);
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tPool);
        int loopCount = 0;

        ArrayList<TableSet> tableSetList = bkpset.getTableList();

        //get tableSet from sysBackup.
        for (final TableSet t : tableSetList) {
          String tableName = t.getTableName();
          final String snapshotPath = t.getSnapshotPath();
          final ArrayList<String> lobPaths = t.getLobSnapshotPaths();
          final String snapshotName = t.getTableName();
          final String destSnapshotPathFinal = destBackupTag + "/" + snapshotPath;
          boolean incrementalType = t.isIncremental();  //this must be false.
          final boolean skipExportSnapshotFinal = false;
          final List<MutationMetaRecord> mutationList = new ArrayList<MutationMetaRecord>();
          final List<LobMetaRecord> lobList = new ArrayList<LobMetaRecord>();
          final String destBackupTagFinal = destBackupTag;

          compPool.submit(new BackupRestoreCallable() {
            public Integer call() throws Exception {
            return doExport(backuptag,
                            destBackupTagFinal,
                            snapshotPath,
                            snapshotName,
                            destSnapshotPathFinal,
                            mutationList,
                            "",     /*destMutationPath*/
                            lobList,
                            lobPaths,
                            skipExportSnapshotFinal);
              }
          });
          loopCount++;
        } //for

        // simply to make sure they all complete, no return codes necessary at the moment
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
          int returnValue = compPool.take().get(); 
        }  

        ObjectOutputStream oos = new ObjectOutputStream(bkpsetos);
        oos.writeObject(bkpset);
        oos.close();
        bkpsetos.close();
        
      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("exportBackup Exception occurred " ,e);
        throw new IOException(e);
      } finally {
        if(tPool != null)
          tPool.shutdownNow();
      }
      return true;
    }
    
    public boolean exportBackup(final String backuptag, String dest,
                                int numParallelThreads, final boolean override,
                                int progressUpdateDelay)
        throws Exception {

      if (logger.isInfoEnabled()) logger.info("[EXPORTBACKUP] backup: " + backuptag +
                  " destination: " + dest +
                  " threads: " + numParallelThreads +
                  " override: " + override);

      // First check to ensure we are not exporting to an internal location that could cause future problems
      if ((dest.contains("/user/trafodion/backupsys")) ||
          (dest.contains("/user/trafodion/lobs")) ||
          (dest.contains("/user/trafodion/PIT")) ||
          (dest.contains("/user/trafodion/.hiveStats")) ||
          (dest.contains("/user/trafodion/rstats")) ||
          (dest.contains("/user/trafodion/udr")) ||
          (dest.contains("/user/trafodion/hive"))) {
         throw new IOException ("Export backup prohibited to privileged internal Trafodion directory: " + dest);
      }

      if( isTagExistBackupSys(backuptag)) {
        return exportSysBackup(backuptag, dest, numParallelThreads, override);
      }
      
      if(!isTagExistInMeta(backuptag)) {
        throw new IOException("Backup tag " + backuptag + " does not exist.");
      }
      
      ExecutorService tPool = null;
      byte [] asciiTime = new byte[40];
      int timeout = ID_TM_SERVER_TIMEOUT;
      boolean cb = false;
      IdTm cli = new IdTm(cb);
      String hdfsRoot = config.get("fs.default.name");

      try {
        RecoveryRecord rr = new RecoveryRecord(config, backuptag,
                "EXPORT",
                /* adjust */ true,
                /* force */ false,
                /* restoreTS */ 0L,
                /* showOnly */ true);

        //collect basic info from this backup.
        boolean incrementalBackup = rr.getSnapshotStartRecord().getIncrementalBackup();
        boolean validity = rr.getSnapshotStartRecord().getSnapshotComplete();
        
        //timestamp
        long key = rr.getSnapshotStartRecord().getCompletionTime();
        cli.idToStr(timeout, key, asciiTime);
        String timeStamp = Bytes.toString(asciiTime);
        timeStamp = timeStamp.substring(0, 19);

        // replace space delimiter between date and time with colon (:)
        timeStamp = timeStamp.replace(' ', ':');

        Map<String, TableRecoveryGroup> recoveryTableMap = rr.getRecoveryTableMap();
        if (logger.isDebugEnabled())
            logger.debug("exportBackup recoveryTableMap size is " + recoveryTableMap.size());

        // First we need to broadcast to the affected tables to ensure they close the current mutation
        // file so we can ensure we don't miss any mutations
        int GENERIC_FLUSH_MUTATION_FILES = 2; //Must be same as defined in TrxRegionEndPoint.java
        ArrayList<String> tableList = new ArrayList<String>();
        for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet()) {
            String tableName = tableEntry.getKey();
            tableList.add(tableName);
        }
        if (logger.isInfoEnabled())
            logger.info("exportBackup broadcast mutation flush before export ");
        xdcBroadcast(GENERIC_FLUSH_MUTATION_FILES, 0, 0 /* local peer */, tableList);

        //remove trailing '/' if present.
        if(dest.endsWith("/"))
          dest = dest.substring(0,dest.length() -1);
        
        //create a reference txt file that lists all snapshots of this backup.
        Path destRootPath = new Path(dest);
        FileSystem destFs = FileSystem.get(destRootPath.toUri(), config);
        String destBackupTag = dest + "/" + backuptag;
        Path recovrec = new Path(destBackupTag, "recoveryRecord.bin");
        Path backupMetafile = new Path(destBackupTag, "backupMeta.bin");
        String destSnapshotRootDir = dest + "/.snapshots";
        String destIncrmentalFileDir = dest + "/.incrments";
        if(destFs.exists(backupMetafile)) {
          if (logger.isDebugEnabled()) logger.debug("[EXPORTBACKUP] Files exist for tag " + backuptag + " at: " + destBackupTag);
          if (override){
             deleteDir(destBackupTag, true /*recursive */);
          }
          else{
             throw new IOException("Backup already exists at this location: " + backupMetafile);
          }
        }
        //First serialize and write out recoveryRecord.
        FSDataOutputStream rros = destFs.create(recovrec);
        ObjectOutputStream oos = new ObjectOutputStream(rros);
        oos.writeObject(rr);
        oos.close();
        rros.close();
        
        //Secondly create the backupMeta info.
        //Write contents after successful export.
        FSDataOutputStream bkpsetos = destFs.create(backupMetafile);
        String envMappers = System.getenv("EXPORT_IMPORT_MAPPERS");
        int mappers = (envMappers != null) ? Integer.parseInt(envMappers) : 16;
        pit_thread = numParallelThreads;
        pit_thread = (pit_thread == 0) ? 1 : pit_thread;
        
        tPool = Executors.newFixedThreadPool(pit_thread);
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tPool);
        int loopCount = 0;
        ArrayList<TableSet> tableSetList = new ArrayList<TableSet>();

        List<String> allSnapshots = new ArrayList<String>();
        List<Path> allMutations = new ArrayList<Path>();
        List<Path> allLobs = new ArrayList<Path>();
        for (Map.Entry<String, TableRecoveryGroup> tableEntry : recoveryTableMap.entrySet()) {
          String tableName = tableEntry.getKey();
          TableRecoveryGroup tableGroup = tableEntry.getValue();

          //Export snapshot
          SnapshotMetaRecord tableMeta = tableGroup.getSnapshotRecord();
          final boolean incrementalTable = tableMeta.getIncrementalTable();
          final String snapshotPath = tableMeta.getSnapshotPath();
          final String snapshotName = tableMeta.getSnapshotName();
          final List<MutationMetaRecord> mutationList = tableGroup.getMutationList();
          final List<LobMetaRecord> lobList = tableGroup.getLobList();
          final List<SnapshotMetaRecord> brcSnapshotList = tableMeta.getBrcSnapshotList();
          ArrayList<String> tableSetBrcList = new ArrayList<>();

          if (logger.isInfoEnabled()) {
             logger.info("exportBackup tableName: " + tableName
                                 + " snapshotName: " + snapshotName
                                 + " snapshotPath: " + snapshotPath
                                 + " mutationList: " + mutationList
                                 + " lobList: " + lobList);
          }

          String destSnapshotPath = destSnapshotRootDir + "/" + snapshotPath;
          boolean skipExportSnapshot = destFs.exists(new Path(destSnapshotPath));

          // if override is true,can't skip
          if (!skipExportSnapshot || override) {
              allSnapshots.add(snapshotPath);
          }

          //add brc snapshot path to allSnapshots list
          if(null != brcSnapshotList && brcSnapshotList.size() > 0){
              for (SnapshotMetaRecord brcRecord : brcSnapshotList) {
                  String brcSnapshotPath = brcRecord.getSnapshotPath();
                  tableSetBrcList.add(brcSnapshotPath);
                  destSnapshotPath = destSnapshotRootDir + "/" + brcSnapshotPath;
                  skipExportSnapshot = destFs.exists(new Path(destSnapshotPath));
                  // if override is true,can't skip
                  if (!skipExportSnapshot || override) {
                      allSnapshots.add(brcSnapshotPath);
                  }
              }
          }

          ArrayList<String> mutations = new ArrayList<String>();
          for (int i = 0; i < mutationList.size(); i++) {
            MutationMetaRecord mutationRecord = mutationList.get(i);
            String mutationPathString = mutationRecord.getMutationPath();
            int mutationPathOffsetIdx = mutationPathString.indexOf("/cdc/");
            String mutation = mutationPathString.substring(mutationPathOffsetIdx + 5);
            if (logger.isInfoEnabled())
               logger.info("exportBackup mutationPathString: " + mutationPathString);
            mutations.add(mutation);

            String destMutationPath = destIncrmentalFileDir + "/" + mutation;
            if (!destFs.exists(new Path(destMutationPath))) {
              if (!mutationPathString.startsWith(hdfsRoot)) {
                  //@see MutationMetaRecord.updateHdfsUrl
                  mutationPathString = mutationPathString.substring(mutationPathString.indexOf("/user/trafodion/PIT"));
                  mutationPathString = hdfsRoot + mutationPathString;
              }
              Path mutationPath  = new Path(mutationPathString);
              allMutations.add(mutationPath);
            }
          }

          ArrayList<String> lobSnapNames = new ArrayList<String>();
          ArrayList<String> lobSnapPaths = new ArrayList<String>();
          for (int i = 0; i < lobList.size(); i++) {
            LobMetaRecord lobRecord = lobList.get(i);
            String lobFile = lobRecord.getSnapshotName();
            String lobPath = lobRecord.getHdfsPath();
            int pathOffsetIdx = lobPath.indexOf("/user/trafodion/lobs/");
            String lob = lobPath.substring(pathOffsetIdx + 21);

            if (logger.isInfoEnabled())
                logger.info("exportBackup lobPath: " + lob + " lobFile: " + lobFile);
            lobSnapNames.add(lobFile);
            lobSnapPaths.add(lob);

            String lobPathString = lobPath + "/.snapshot/" + lobFile;
            Path lobFilePath = new Path(lobPathString);
            allLobs.add(lobFilePath);
          }
          TableSet ts = new TableSet(tableName, snapshotName, snapshotPath,
                 lobSnapNames, lobSnapPaths, mutations, incrementalTable);
          ts.setBrcSnapshotPathList(tableSetBrcList);
          if (lobSnapNames.size() > 0){
            if (logger.isInfoEnabled())
              logger.info("\nexportBackup creating TableSet with lobs: " + ts.displayString() + "\n");
          }
          tableSetList.add(ts);
          loopCount++;
        } //for

        //check mutation file can read
        int readCheckSize = checkMutationFile(allMutations, backuptag);
        if(readCheckSize > 0){
           String msg =
                  "exportBackup Exception occurred caused by mutation file Trailer error, size "+ readCheckSize;
           if (logger.isErrorEnabled())
               logger.error(msg);
           throw new IOException(msg);
        }

        final int mappersFinal = mappers;
        // for all snapshots we use hbase org.apache.hadoop.hbase.snapshot.EsgynExportSnapshot
        String inputSnapshots = null;
        int batchCount = 0;
        final String destDirFinal = destSnapshotRootDir;
        for (int i = 0; i < allSnapshots.size(); i++) {
          if (inputSnapshots == null)
            inputSnapshots = allSnapshots.get(i);
          else
            inputSnapshots += "," + allSnapshots.get(i);
          if ((((i+1)%mappers) == 0) || (i == allSnapshots.size() - 1)) {
            final String inputSnapshotsFinal = inputSnapshots;
            if (logger.isInfoEnabled()) {
              int batchSize = 0;
              if (i == (allSnapshots.size() - 1))
                batchSize = (batchCount == 0) ? loopCount : (loopCount % mappers);
              else
                batchSize = mappers;
              logger.info("doExportSnapshots size: " + batchSize);
            }
            compPool.submit(new BackupRestoreCallable() {
              @Override
              public Integer call() throws Exception {
                return doExportSnapshots(inputSnapshotsFinal, destDirFinal, mappersFinal, override);
              }
            });
            batchCount++;
            inputSnapshots = null;
          }
        }

        // for all mutations and lobs, we just need to use DistCp
        if (allMutations.size() > 0) {
          final String destIncrDirFinal = destIncrmentalFileDir;
          final List<Path> allMutationsFinal = allMutations;
          if (logger.isInfoEnabled()) logger.info("distCpObjects mutations size: " + allMutationsFinal.size());
          compPool.submit(new BackupRestoreCallable() {
            @Override
            public Integer call() throws Exception {
              return distCpObjects(allMutationsFinal, destIncrDirFinal, mappersFinal, override);
            }
          });
          batchCount++;
        }

        if (allLobs.size() > 0) {
          final String destLobDirFinal = destBackupTag;
          final List<Path> allLobsFinal = allLobs;
          if (logger.isInfoEnabled()) logger.info("distCpObjects lobs size: " + allLobsFinal.size());
          compPool.submit(new BackupRestoreCallable() {
            @Override
            public Integer call() throws Exception {
              return distCpObjects(allLobsFinal, destLobDirFinal, mappersFinal, override);
            }
          });
          batchCount++;
        }

        // simply to make sure they all complete, no return codes necessary at the moment
        int tagOperCount = getBREIOperCount("EXPORT", backuptag);
        int numCurrProcess = 0;
        double realProgress = 0.0D;
        double incrProgress = loopCount * 1.0D / batchCount;
        for (int loopIndex = 0; loopIndex < batchCount; loopIndex++) {
          Future returnValue = compPool.poll(getExportImportTimeoutSeconds(), TimeUnit.SECONDS);
          if(null == returnValue){
              throw new IOException("export mr task is timeout");
          }
          returnValue.get();
          realProgress = (loopIndex == batchCount -1) ? loopCount : (realProgress + incrProgress);
          numCurrProcess = (int)realProgress;
          updateBREIprogress("EXPORT", backuptag, loopCount, numCurrProcess, progressUpdateDelay, tagOperCount, false);
        }

        final String extAtrributes = rr.getSnapshotStartRecord().getExtendedAttributes();

        //Write out BackupMeta now.
        BackupSet bkpset = new BackupSet( backuptag,
                                          tableSetList,
                                          incrementalBackup,  /*incr*/
                                          false,              /*sysBkp*/
                                          validity,           /*validity*/
                                          timeStamp,          /*timestamp*/
                                          extAtrributes,      /* extended Attributes */
                                          false);             /*imported*/ 
        
        ObjectOutputStream oos2 = new ObjectOutputStream(bkpsetos);
        oos2.writeObject(bkpset);
        oos2.close();
        bkpsetos.close();

        int checkResult = checkExportByBackupSet(bkpset, destRootPath);
        if (checkResult != 0) {
          if (logger.isErrorEnabled()) logger.error("exportBackup Exception occurred caused by check result error.");
          throw new IOException("exportBackup Exception occurred caused by check result error");
        }
      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("exportBackup Exception occurred " ,e);
        throw new IOException(e);
      } finally {
        if(tPool != null)
          tPool.shutdownNow();
      }

      return true;
    }
    
    public int checkExportByBackupSet(BackupSet bkpset, Path destPath) {
      long start = System.currentTimeMillis();
      CheckSnapshot snapshot = new CheckSnapshot(new Path(destPath.toString()));
      snapshot.init();

      // total mutation path list
      List<String> mutationList = new ArrayList<>();
      // total normal path list
      List<String> normalList = new ArrayList<>();
      // total incremental path list
      List<String> incrementalList = new ArrayList<>();

      Map<String, String> tableNameMap = new HashMap<>();

      for (TableSet tableSet : bkpset.getTableList()) {
          String tbName = tableSet.getTableName();
          if (tableSet.incremental) {
              incrementalList.add(tableSet.getSnapshotPath());
              //@See ImportCallable#doImport
              tbName = tbName.replace(":", "_");
          } else {
              normalList.add(tableSet.getSnapshotPath());
          }
          tableNameMap.put(tableSet.getSnapshotPath(), tbName);
          mutationList.addAll(tableSet.getMutationList());
      }

      //check mutation is exist
      List<String> mutationCheckResult = snapshot.check(mutationList);
      //check normal is exist
      List<String> normalCheckResult = snapshot.check(normalList);

      //check incremental is exist
      List<String> incrementalResult = snapshot.check(incrementalList);
      String fileName = "exportbackup-check.log";
      //write log file
      try {
          String logFile = System.getenv("TRAF_LOG") + "/" + fileName;
          BufferedWriter out = new BufferedWriter(new FileWriter(logFile, true));

          out.write("\n\ntag: " + bkpset.getBackupTag() + "\ttype: " + bkpset.getBackupType());
          out.write("\nexportTime: "+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
          out.write("\n------mutation-total: " + mutationList.size() + " error-size: "+mutationCheckResult.size()+
                    "---\nerror:\n");
          if(mutationCheckResult.size() > 0) {
              for (String str : mutationCheckResult) {
                  out.write(str+"\n");
              }
          }
          out.write("\n------normal-total: " + normalList.size()  + " error-size: " + normalCheckResult.size() +
                    "---\nerror:\n");
          if(normalCheckResult.size() > 0) {
              for (String str : normalCheckResult) {
                      out.write("tableName: " + tableNameMap.get(str) + " snapshot: " + str+"\n");
              }
          }
          out.write("\n------incremental-total: " + incrementalList.size() + " error-size: " +incrementalResult.size() +
                   "---\nerror:\n");

          if(incrementalResult.size() > 0) {
              for (String str : incrementalResult) {
                    out.write("tableName: " + tableNameMap.get(str) + " snapshot: " + str + "\n");
              }
          }
          out.write("Total Elapsed Time: " + (System.currentTimeMillis() - start) + " ms");
          out.close();
      } catch (Exception e) {
          if (logger.isErrorEnabled())
              logger.error("checkExportByBackupSet error\t" + e.getMessage(), e);
      }
      if (logger.isInfoEnabled())
          logger.info("check result mutationCheckResult:" + mutationCheckResult.size()+" normalCheckResult:"
              + normalCheckResult.size()+" incrementalResult:"+incrementalResult.size());
      return mutationCheckResult.size() + normalCheckResult.size() + incrementalResult.size();
    }

    public int checkMutationFile(List<Path> mutationList, String tag) {
        long start = System.currentTimeMillis();
        //check mutation file is right
        List<String> mutationCloseCheck = CheckSnapshot.checkMutationClose(mutationList, config);
        if(!mutationCloseCheck.isEmpty()){
            try {
                String fileName = "exportbackup-check.log";
                String logFile = System.getenv("TRAF_LOG") + "/" + fileName;
                BufferedWriter out = new BufferedWriter(new FileWriter(logFile, true));
                out.write("\n\ntag: " + tag);
                out.write("\n------mutation-total: " + mutationList.size() + " file-error-size: "+mutationCloseCheck.size()+
                        "---\nerror:\n");
                for (String s : mutationCloseCheck) {
                    if (logger.isErrorEnabled())
                        logger.error(s);
                    out.write(s+"\n");
                }
                out.write("\nexportTime: "+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                out.write("\ncheckMutationFile Total Elapsed Time: " + (System.currentTimeMillis() - start) + " ms");
                out.close();
            } catch (Exception e){
                if (logger.isErrorEnabled())
                    logger.error("checkMutationFile error tag: " + tag, e);
            }
        }
        if (logger.isInfoEnabled())
            logger.info("checkMutationFile Total Elapsed Time: " + (System.currentTimeMillis() - start) + " ms" + " total" +
                "-size: " + mutationList.size());
        return mutationCloseCheck.size();
    }
      
    public boolean importBackup(final String backuptag, String source,
                                int numParallelThreads, final boolean override,
                                int progressUpdateDelay)
        throws Exception {

      if (logger.isInfoEnabled()) logger.info("[IMPORTBACKUP] backup: " + backuptag +
                  " Source location: " + source +
                  " threads: " + String.valueOf(numParallelThreads) +
                  " override: " + override);
      
      //initial checks
      if(isTagExistInMeta(backuptag)) {
        throw new IOException("Backup " + backuptag + " already exists in meta.");
      }

      if (isTagExistBackupSys(backuptag)) {
         if (override) {
            String importDest = new String("/user/trafodion/backupsys/" + backuptag);
            Path dirPath = new Path(importDest );
            FileSystem fs = FileSystem.get(dirPath.toUri(), config);
            if (logger.isInfoEnabled()) logger.info("importBackup backup: " + backuptag +
                    " already exists; deleting files in destination directory " + importDest);
            fs.delete(dirPath, true /*recursive */);
         }
         else {
            throw new IOException("Backup " + backuptag + " already imported.");
         }
      }

      ExecutorService tPool= null;
      try {
        //remove trailing '/' if present.
        if(source.endsWith("/"))
          source = source.substring(0,source.length() -1);
        
        //Source file system access setup.
        Path srcRoot = new Path(source);
        FileSystem srcFs = FileSystem.get(srcRoot.toUri(), config);
        String srcBackupTag = source + "/" + backuptag;
        
        //validate backupMeta exists at source.
        Path srcBackupMetafile = new Path(srcBackupTag, "backupMeta.bin");
        if(!srcFs.exists(srcBackupMetafile))
          throw new IOException("Backup location is invalid");

        //Next read in backupMeta.
        FSDataInputStream is = srcFs.open(srcBackupMetafile);
        ObjectInputStream ois = new ObjectInputStream(is);
        BackupSet bkpset = (BackupSet)ois.readObject();
        ois.close();
        is.close();

        //destination is the backupsys in local system
        //setup access to this filesystem
        String hdfsRoot = config.get("fs.default.name");
        String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";
        Path backupsysRootPath = new Path(backupsysRoot);
        FileSystem backupsysFs = FileSystem.get(backupsysRootPath.toUri(), config);
        String destBackupTag = backupsysRoot + "/" + backuptag;

        //if backupMeta indicates non system backup, first 
        //transfer the recoveryRecord to backupsys location.
        if(! bkpset.isSysBackup()) {
          //For now read in and write out. Later we can just copy this file. TBD.
          Path srcrecovrec = new Path(srcBackupTag, "recoveryRecord.bin");
          FSDataInputStream rris = srcFs.open(srcrecovrec);
          ObjectInputStream ois2 = new ObjectInputStream(rris);
          RecoveryRecord rr = (RecoveryRecord)ois2.readObject();
          ois2.close();
          rris.close();

          //Now write this back to backupsys location.
          Path destrecovrec = new Path(destBackupTag, "recoveryRecord.bin");
          FSDataOutputStream rros = backupsysFs.create(destrecovrec);
          ObjectOutputStream oos = new ObjectOutputStream(rros);
          oos.writeObject(rr);
          oos.close();
          rros.close();
        }

        //perform real import from here.
        FileSystem destFs = FileSystem.get(config);
        final String hbaseRoot = config.get("hbase.rootdir");
        FileStatus hrootStatus = destFs.getFileStatus(new Path(hbaseRoot));
        
        ArrayList<TableSet>  tableSetList = bkpset.getTableList();
       
        if(tableSetList.size() <= 0)
          throw new IOException("Import objects are Empty");
        
        
        pit_thread = numParallelThreads;
        pit_thread = (pit_thread == 0) ? 1 : pit_thread;
        if (logger.isInfoEnabled()) logger.info("importBackup parallel threads : " + pit_thread);
        
        tPool = Executors.newFixedThreadPool(pit_thread);
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tPool);
        
        int loopCount = tableSetList.size();
        final boolean incrBkp = bkpset.isIncremental();
        final String srcLocation = source;

        String envMappers = System.getenv("EXPORT_IMPORT_MAPPERS");
        int mappers = (envMappers != null) ? Integer.parseInt(envMappers) : 16;
        final int mappersFinal = mappers;
        List<String> allSnapshots = new ArrayList<String>();

        Admin admin = connection.getAdmin();
        List<SnapshotDescription> existSnapshots = admin.listSnapshots();
        Set<String> snapshotSet = new HashSet<String>(existSnapshots.size());
        for (SnapshotDescription snapshotDescription : existSnapshots) {
            snapshotSet.add(snapshotDescription.getName());
        }
        // collect the import snapshots
        for (TableSet table : tableSetList) {
            List<String> brcSnapshotPathList = table.getBrcSnapshotPathList();
            if(null != brcSnapshotPathList && brcSnapshotPathList.size() > 0){
                for (String brcSnapshotPath : brcSnapshotPathList) {
                    if (snapshotSet.contains(brcSnapshotPath) && !override){
                        continue;
                    }
                    allSnapshots.add(brcSnapshotPath);
                }
            }
            String snapshotPath = table.getSnapshotPath();
            // Check if table snapshot import can be skipped.
            // if this snapshot is already exist in this cluster,
            // and we choose not to override, then we skip it
            if (snapshotSet.contains(snapshotPath) && !override)
                continue;
            allSnapshots.add(snapshotPath);
        }
        admin.close();

        // for all snapshots we use hbase org.apache.hadoop.hbase.snapshot.EsgynImportSnapshot
        String inputSnapshots = null;
        String inputSrcFullPathSnaps = null;
        int batchCount = 0;
        final String destDirFinal = hbaseRoot;
          for (int i = 0; i < allSnapshots.size(); i++) {
              String snapshot = allSnapshots.get(i);
              String fullSrcSnapshot = source + "/.snapshots/" + snapshot;
              if (inputSnapshots == null) {
                  inputSnapshots = snapshot;
                  inputSrcFullPathSnaps = fullSrcSnapshot;
              } else {
                  inputSnapshots += "," + snapshot;
                  inputSrcFullPathSnaps += "," + fullSrcSnapshot;
              }

              if ((((i+1)%mappers) == 0) || (i == allSnapshots.size() - 1)) {
                  final String inputSnapshotsFinal = inputSnapshots;
                  final String inputSrcFullPathSnapsFinal = inputSrcFullPathSnaps;
                  if (logger.isInfoEnabled()) {
                      int batchSize = 0;
                      if (i == (allSnapshots.size() - 1))
                          batchSize = (batchCount == 0) ? loopCount : (loopCount % mappers);
                      else
                          batchSize = mappers;
                      logger.info("doImportSnapshots size: " + batchSize);
                  }
                  compPool.submit(new BackupRestoreCallable() {
                      public Integer call() throws Exception {
                          return doImportSnapshots(inputSnapshotsFinal, inputSrcFullPathSnapsFinal,
                                  destDirFinal, override, mappersFinal);
                      }
                  });
                  batchCount++;
                  inputSnapshots = null;
                  inputSrcFullPathSnaps = null;
              }
          }

        // we use doImport just to DictCp mutations and lobs
        for (final TableSet t : tableSetList) {
            if (t.getMutationSize() > 0 || t.getLobSnapshotNameListSize() > 0) {
                compPool.submit(new BackupRestoreCallable() {
                    public Integer call() throws Exception {
                        return doImport(backuptag, incrBkp, srcLocation, t, override, mappersFinal);
                    }
                });
                batchCount++;
            }
         } //for

        // simply to make sure they all complete, no return codes necessary at the moment
        int tagOperCount = getBREIOperCount("IMPORT", backuptag);
        int numCurrProcess = 0;
        double realProgress = 0.0D;
        double incrProgress = loopCount * 1.0D / batchCount;
        for (int loopIndex = 0; loopIndex < batchCount; loopIndex++) {
            Future returnValue = compPool.poll(getExportImportTimeoutSeconds(), TimeUnit.SECONDS);
            if(null == returnValue){
                throw new IOException("import mr task is timeout");
            }
            returnValue.get();
            realProgress = (loopIndex == batchCount -1) ? loopCount : (realProgress + incrProgress);
            numCurrProcess = (int)realProgress;
            updateBREIprogress("IMPORT", backuptag, loopCount, numCurrProcess,
                             progressUpdateDelay, tagOperCount, false);
        }
        
        //Now that everything is imported, update backupMeta and 
        //write it out to backupsys folder.
        Path backupMetafile = new Path(destBackupTag, "backupMeta.bin");
        FSDataOutputStream bkmetaos = backupsysFs.create(backupMetafile);
        bkpset.setImported();
        ObjectOutputStream oos = new ObjectOutputStream(bkmetaos);
        oos.writeObject(bkpset);
        oos.close();
        bkmetaos.close();
       }
       catch (Exception e) {
         if (logger.isErrorEnabled()) logger.error("ImportBackup Exception occurred " ,e);
         throw new IOException(e);
       }
       finally{
         if(tPool != null)
           tPool.shutdownNow();
       }

       return true;
      } 
    

    public boolean exportOrImportBackup(String backuptag, 
                                        boolean isExport,
                                        boolean override,
                                        String location,
                                        int numParallelThreads,
                                        int progressUpdateDelay) 
                                        throws Exception {

      //check if location is fully qualified.
      if(! location.startsWith("hdfs:"))
        throw new IOException("exportOrImportBackup location must be fully qualified hdfs address.");

      boolean retcode = false;
      if (isExport) {
        // export code need to go here.
        retcode =  exportBackup(backuptag, location, numParallelThreads,
                                override, progressUpdateDelay);
      } else {
        // import code need to go here
        retcode = importBackup(backuptag, location, numParallelThreads,
                               override, progressUpdateDelay);
      }

       // close ttable
      if (ttable != null)
          ttable.close();
        
       return retcode;
    }

    static public long getIdTmVal() throws Exception {
      IdTmId LvId;
      IdTm idServer = new IdTm(false);
      LvId = new IdTmId();
      idServer.id(ID_TM_SERVER_TIMEOUT, LvId);
      return LvId.val;
    }

    static public String idToStr(long key) throws Exception {

      byte [] asciiTime = new byte[40];
      int timeout = ID_TM_SERVER_TIMEOUT;
      IdTm idServer = new IdTm(false);
      idServer.idToStr(timeout, key, asciiTime);
      String timeStamp = Bytes.toString(asciiTime);

      // replace space delimiter between date and time with colon (:)
      timeStamp = timeStamp.replace(' ', ':');
      return timeStamp;
    }

    static final SimpleDateFormat idToStrFormat = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
    static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //idToStr return GMT 2021-09-21:12:18:47 convert to GMT+8
    public static String idToTimeStr(long timestamp) {
        String timestampStr = "";
        try {
            idToStrFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            Date date = idToStrFormat.parse(idToStr(timestamp));
            return dateFormat.format(date);
        } catch (Exception e) {
        }
        return timestampStr;
    }

    static public long strToId(String asciiTime) throws Exception {

      int timeout = ID_TM_SERVER_TIMEOUT;
      IdTm idServer = new IdTm(false);
      IdTmId idtmid = new IdTmId();

      if (logger.isInfoEnabled()) logger.info("strToId entry timeout " + timeout + " asciiTime " + asciiTime);

      // convert 2019-10-06:11:42:57 tp 2019-10-06 11:42:57
      if(asciiTime.charAt(10) == ':') {
         String beginstr = asciiTime.substring(0,10);
         String endstr = asciiTime.substring(11);
         asciiTime = beginstr + " " + endstr;
      }

      if (!asciiTime.contains(".")) {
         asciiTime = asciiTime + ".000000";
      }

      if (logger.isInfoEnabled()) logger.info("strToId timeout " + timeout + " asciiTime " + asciiTime);

      idServer.strToId(timeout, idtmid, asciiTime);

      return idtmid.val;
    }

    static public String getPriorBackupTag(String asciiTime) throws Exception {

       if (logger.isInfoEnabled()) logger.info("getPriorBackupTag entry asciiTime " + asciiTime);
       long timeId = strToId(asciiTime);
       return getPriorBackupTag(timeId);
    }

    static public String getPriorBackupTag(long timeId) throws Exception {
        SnapshotMetaStartRecord lv_smsr = sm.getPriorStartRecord(timeId);
        return (lv_smsr == null) ? null : lv_smsr.getUserTag();
    }

    static public String getFollowingBackupTag(long timeId) throws Exception {
        SnapshotMetaStartRecord lv_smsr = sm.getFollowingStartRecord(timeId);
        return (lv_smsr == null) ? null : lv_smsr.getUserTag();
    }

    /**
     * 1. pit timestamp must between int two tag
     * 2. after tag must be incremental tag
     * 3. two tag must be same root SnapshotMetaRecord
     * @param asciiTime
     * @return
     * @throws Exception
     */
    public String getRestoreToTsBackupTag(String asciiTime) throws Exception {
        if (logger.isInfoEnabled()) logger.info("getRestoreToTsBackupTag entry asciiTime " + asciiTime);
        preTagMap.clear();
        long timeId = strToId(asciiTime);
        Set<String> importTagSet = new HashSet<>();
        Set<String> metaTagSet = new HashSet<>();
        List<SnapshotMetaStartRecord> allStartTagList = new ArrayList<>();
        try {
            // in listSnapshotStartRecords() method, if result is empty,the method throw Exception,It's not necessary
            allStartTagList = sm.listSnapshotStartRecords();
        } catch (Exception e){
            if (!e.getMessage().contains("Prior record not found")) {
                throw e;
            }
        }
        if (logger.isInfoEnabled()) logger.info("getRestoreToTsBackupTag listSnapshotStartRecords timeId " + timeId
                + " size: " + allStartTagList.size());

        for (SnapshotMetaStartRecord startRecord : allStartTagList) {
            metaTagSet.add(startRecord.getUserTag());
        }

        String hdfsRoot = config.get("fs.default.name");
        String backupsysRoot = hdfsRoot + "/user/trafodion/backupsys";

        Path backupsysRootPath = new Path(backupsysRoot);
        FileSystem backupsysRootFs = FileSystem.get(backupsysRootPath.toUri(), config);

        //import RecoveryRecord maybe a lot, cache limit
        int max_cache = 10;
        Map<String, RecoveryRecord> importCache = new HashMap<>(max_cache);
        RemoteIterator<LocatedFileStatus> bkpitr = backupsysRootFs.listLocatedStatus(backupsysRootPath);

        List<SnapshotMetaStartRecord> importStartTagList = new ArrayList<>();
        while (bkpitr.hasNext()) {
            //Note folder name is tag name.
            LocatedFileStatus bkpFolder = bkpitr.next();
            String bkpFolderName = bkpFolder.getPath().getName();
            Path backupMetaPath = new Path(backupsysRoot + "/" + bkpFolderName, "recoveryRecord.bin");
            if (!backupsysRootFs.exists(backupMetaPath)) {
                continue;
            }

            RecoveryRecord recoveryRecord = getRecoveryRecordByPath(backupsysRootFs, backupMetaPath);
            if (null != recoveryRecord) {
                importStartTagList.add(recoveryRecord.getSnapshotStartRecord());
                if (importCache.size() < max_cache) {
                    importCache.put(recoveryRecord.getTag(), recoveryRecord);
                }
                importTagSet.add(recoveryRecord.getTag());
            }
        }

        if (logger.isInfoEnabled()) logger.info("getRestoreToTsBackupTag importTag timeId " + timeId
                + " size: " + importStartTagList.size());

        allStartTagList.addAll(importStartTagList);
        Collections.sort(allStartTagList, new Comparator<SnapshotMetaStartRecord>() {
            @Override
            public int compare(SnapshotMetaStartRecord s1, SnapshotMetaStartRecord s2) {
                return Long.compare(s1.getKey(), s2.getKey());
            }
        });
        SnapshotMetaStartRecord preview = null, after = null;
        for (SnapshotMetaStartRecord startRecord : allStartTagList) {
            long key = startRecord.getIncrementalBackup() ? startRecord.getKey() : startRecord.getCompletionTime();
            if (key < timeId) {
                preview = startRecord;
            } else if (key >= timeId) {
                after = startRecord;
                break;
            }
        }
        //preview and after must be same root and after must incremental backup
        if (logger.isInfoEnabled())
            logger.info("getRestoreToTsBackupTag preview : " + preview + " after : " + after);

        if (null == preview || null == after) {
            throw new IOException("timestamp " + idToTimeStr(timeId) + " has to be between two tags");
        }

        //the after tag is regular backup, must query preview tag
        if (!after.getIncrementalBackup()) {
            if (logger.isInfoEnabled()) logger.info("getRestoreToTsBackupTag exit asciiTime " + asciiTime
                    + " after is regular  tag " + after.getUserTag());
            preTagMap.put(after.getUserTag(), preview.getUserTag());
            return after.getUserTag();
        }
        List<SnapshotMetaRecord> previewSnapList = null, afterSnapList = null;
        if (importTagSet.contains(preview.getUserTag())) {
            previewSnapList = getRecordFromImportRecord(importCache, preview.getUserTag(), backupsysRootFs, backupsysRoot);
        } else if (metaTagSet.contains(preview.getUserTag())) {
            previewSnapList = sm.getAdjustedSnapshotSet(preview.getKey(), new StringBuilder());
        }

        if (importTagSet.contains(after.getUserTag())) {
            afterSnapList = getRecordFromImportRecord(importCache, after.getUserTag(), backupsysRootFs, backupsysRoot);
        } else if (metaTagSet.contains(after.getUserTag())) {
            afterSnapList = sm.getAdjustedSnapshotSet(after.getKey(), new StringBuilder());
        }

        if (logger.isInfoEnabled())
            logger.info("getRestoreToTsBackupTag afterSnapList : " + (null != afterSnapList ? afterSnapList.size() : null)
                + " previewSnapList :  " + (null != previewSnapList ? previewSnapList.size() : null));

        if (null != previewSnapList && null != afterSnapList) {
            Map<String, Long> previewMap = new HashMap<>();
            for (SnapshotMetaRecord record : previewSnapList) {
                previewMap.put(record.getTableName(), record.getKey());
            }
            boolean sameRoot = false;
            for (SnapshotMetaRecord record : afterSnapList) {
                Long existValue = previewMap.get(record.getTableName());
                if (null != existValue && existValue.equals(record.getKey())) {
                    if (logger.isInfoEnabled())
                        logger.info("getRestoreToTsBackupTag sameroot check success : " + record.getTableName());
                    sameRoot = true;
                    break;
                }
            }
            if (logger.isInfoEnabled()) logger.info("getRestoreToTsBackupTag exit asciiTime " + asciiTime
                    + " sameRoot " + sameRoot + " afterTag " + after.getUserTag());
            if (sameRoot) {
                return after.getUserTag();
            } else {
                throw new IOException("the basic snapshots of the two tags should be the same");
            }
        }

        if (logger.isInfoEnabled()) logger.info("getRestoreToTsBackupTag exit asciiTime " + asciiTime + " end " +
                "return null");
        return null;
    }

    /**
     * first query in cache,then query from /user/trafodion/backupsys/tagName/recoveryRecord.bin
     *
     * @param importCache
     * @param tag
     * @param backupsysRootFs
     * @param backupsysRoot
     * @return
     * @throws Exception
     */
    private List<SnapshotMetaRecord> getRecordFromImportRecord(Map<String, RecoveryRecord> importCache, String tag,
                                                               FileSystem backupsysRootFs, String backupsysRoot) throws Exception {
        RecoveryRecord tmpRecord = importCache.get(tag);
        if (null == tmpRecord) {
            Path backupMetaPath = new Path(backupsysRoot + "/" + tag, "recoveryRecord.bin");
            tmpRecord = getRecoveryRecordByPath(backupsysRootFs, backupMetaPath);
        }
        if (null != tmpRecord) {
            List<SnapshotMetaRecord> list = new ArrayList<>();
            Map<String, TableRecoveryGroup> map = tmpRecord.getRecoveryTableMap();
            for (Map.Entry<String, TableRecoveryGroup> entry : map.entrySet()) {
                list.add(entry.getValue().getSnapshotRecord());
            }
            return list;
        }
        return null;
    }

    /**
     * get RecoveryRecord in /user/trafodion/backupsys/tagName/recoveryRecord.bin
     *
     * @param backupsysRootFs
     * @param path
     * @return
     * @throws Exception
     */
    private RecoveryRecord getRecoveryRecordByPath(FileSystem backupsysRootFs, Path path) throws Exception {
        try {
            FSDataInputStream bkpmetaIs = backupsysRootFs.open(path);
            ObjectInputStream ois = new ObjectInputStream(bkpmetaIs);
            RecoveryRecord rr = (RecoveryRecord) ois.readObject();
            ois.close();
            bkpmetaIs.close();
            return rr;
        } catch (IOException e) {
            String exceptionString = ExceptionUtils.getStackTrace(e);

            if (!exceptionString.contains("EOFException")) {
                //any exception other than EOFException throw exception.
                throw new IOException(e);
            }
            //ignore error since import may be in progress.
            if (logger.isWarnEnabled())
                logger.warn("getRecoveryRecordByPath Backup Name: " + path.getName() +
                    "encountered exception : ", e);
            return null;
        }
    }

    public void initializeSnapshotMeta(long timeIdVal, String backuptag, boolean incrementalBackup,
          String extendedAttributes, String backupType) throws Exception {
      long systemTime = EnvironmentEdgeManager.currentTime();
      smsr = new SnapshotMetaStartRecord(timeIdVal, backuptag, incrementalBackup, extendedAttributes, backupType, systemTime);
      sm.initializeSnapshot(timeIdVal, backuptag, incrementalBackup, extendedAttributes, backupType, systemTime);
      return;
    }

    public void updateSnapshotMeta(long timeIdVal, String backuptag, boolean incrementalSnapshot,
            boolean userGenerated, boolean incrementalTable, boolean sqlMetaData, String tableName,
            String snapshotName, String snapshotPath, Set<Long> lobSnapshots, String snapshotArchivePath)
            throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("updateSnapshotMeta backuptag " + backuptag + " incrementalSnapshot " + incrementalSnapshot
                    + " userGenerated " + userGenerated + " incrementalTable " + incrementalTable
                    + " lobSnapshotsSize " + lobSnapshots.size()
                    + " tableName " + tableName);

            SnapshotMetaRecord rootRecord = null;

            if (incrementalSnapshot){
               // This is meant to be an incremental backup and there is no snapshot
               // associate with it, so we need to look up the root snapshot
               // for this table and put that in the parentSnapshot parameter
               // for the incremental record we are about to create.
               long rootSnapshotId = sm.getCurrentSnapshotId(tableName);
               long parentSnapshotId = rootSnapshotId;

               if (rootSnapshotId != timeIdVal) {
                  if (logger.isDebugEnabled())
                      logger.debug("updateSnapshotMeta backuptag " + backuptag + " incrementalSnapshot " + incrementalSnapshot
                               + " tableName " + tableName + " timeId " + timeIdVal + " rootSnapshotId " + rootSnapshotId);
                  // We need to record the dependency of the incremental snapshot we are
                  // about to create on the parent record so that it can't be deleted.
                  rootRecord = sm.getSnapshotRecord(rootSnapshotId);
                  Set<Long> dependentSnapshots = rootRecord.getDependentSnapshots();
                  if (dependentSnapshots.size() == 1) {
                     if (dependentSnapshots.contains(-1L)){
                        dependentSnapshots.remove(-1L);
                     }
                  }
                  dependentSnapshots.add(timeIdVal);
                  rootRecord.setDependentSnapshots(dependentSnapshots);
                  sm.putRecord(rootRecord);

                  // We need to establish a chain of related incremental backups,
                  // so we will get the current active incremental record so we
                  // can chain a child off of it.
                  SnapshotMetaIncrementalRecord currentSMIR = sm.getPriorIncrementalSnapshotRecord(tableName, /* includeExcluded */ false);
                  if (currentSMIR != null){
                     // It is possible the last incremental record we have just retrieved is not
                     // for this same root, so if it's not, we must ignore it.
                     if (currentSMIR.getRootSnapshotId() == rootSnapshotId){
                        currentSMIR.setChildSnapshotId(timeIdVal);
                        parentSnapshotId = currentSMIR.getKey();
                        sm.putRecord(currentSMIR);
                     }
                  }
               }
               else {
                  if (logger.isDebugEnabled())
                       logger.debug("updateSnapshotMeta backuptag " + backuptag + " incrementalSnapshot " + incrementalSnapshot
                               + " tableName " + tableName + " timeId matches currentSnapshot ");
               }
               // Mutations start right after the prior incremental snapshot record, even if it is now excluded.  If there isn't
               // a prior incremental record, then mutations start right after the parent record.
               SnapshotMetaIncrementalRecord priorSMIR = sm.getPriorIncrementalSnapshotRecord(tableName, /* includeExcluded */ true);
               long mutationStartTime = (priorSMIR != null ? priorSMIR.getKey() : parentSnapshotId);
               SnapshotMetaIncrementalRecord smir = new SnapshotMetaIncrementalRecord(timeIdVal,
                        SnapshotMetaRecordType.getVersion(),
                        rootSnapshotId,
                        parentSnapshotId,
                        /* childSnapshot */ -1L,
                        /* mutationStartTime */ mutationStartTime,
                        tableName,
                        backuptag,
                        snapshotName,
                        snapshotPath,
                        true /*inLocalFS */,
                        false /* excluded */,
                        false /* archived */,
                        snapshotArchivePath,
                        "EmptyStatements" /* Meta Statements */,
                        lobSnapshots.size(),
                        lobSnapshots);

               if (logger.isDebugEnabled())
                  logger.debug("updateSnapshotMeta backuptag " + backuptag + " putting smir record " + smir);

               sm.putRecord(smir);
            }
            else {
               long systemTime = EnvironmentEdgeManager.currentTime();
               Set<Long> dependentSnapshots = Collections.synchronizedSet(new HashSet<Long>());
               dependentSnapshots.add(-1L);
               SnapshotMetaRecord smr = new SnapshotMetaRecord(timeIdVal,
                                         -1L /* supercedingSnapshot */,
                                         -1L /* restoredSnapshot */,
                                         -1L /* restoreStartTime */,
                                         -1L /* hiatusTime */,
                                         tableName,
                                         backuptag,
                                         snapshotName,
                                         snapshotPath,
                                         userGenerated,
                                         incrementalTable,
                                         sqlMetaData,
                                         true /* inLocalFS */,
                                         false /* excluded */,
                                         systemTime /* mutationStartTime */,
                                         snapshotArchivePath,
                                         "EmptyStatements" /* Meta Statements */,
                                         lobSnapshots.size(), /*numLobs */
                                         lobSnapshots,
                                         dependentSnapshots);

               if (logger.isDebugEnabled())
                   logger.debug("updateSnapshotMeta putting smr record " + smr);
               sm.putRecord(smr);
            }
    }

    public void updateLobMeta(long timeIdVal, int version, String tableName, String snapshotName,
               long fileSize, String backuptag, String snapshotPath, String sourceDirectory,
               boolean inLocalFS, boolean archived, String snapshotArchivePath) throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("updateLobMeta backuptag " + backuptag + " snapshotName " + snapshotName
                    + " snapshotPath " + snapshotPath + " sourceDirectory " + sourceDirectory
                    + " tableName " + tableName);

        LobMetaRecord lobRecord = new LobMetaRecord(timeIdVal, version, tableName, snapshotName,
                   fileSize, backuptag, snapshotPath, sourceDirectory,
                   inLocalFS, archived, snapshotArchivePath);

        if (logger.isDebugEnabled())
                  logger.debug("updateLobMeta backuptag " + backuptag + " putting lob record " + lobRecord);

        lm.putLobMetaRecord(lobRecord, true /* blindWrite */);
    }

    public void completeSnapshotMeta() throws Exception {
        timeIdVal = getIdTmVal();
        long systemTime = EnvironmentEdgeManager.currentTime();
        smsr.setCompletionTime(timeIdVal);
        smsr.setCompletionWallTime(systemTime);
        smsr.setSnapshotComplete(true);
        if (logger.isDebugEnabled())
            logger.debug("completeSnapshotMeta putting smsr record " + smsr);
        sm.putRecord(smsr);
        // insert a new record used to mark completion of backup
        SnapshotMetaEndRecord endRecord = new SnapshotMetaEndRecord(
                timeIdVal,
                smsr.getVersion(),
                smsr.getKey(),
                smsr.getUserTag()
        );
        sm.putRecordMeta(endRecord);
        // insert a index record used to speed up query snapshot record 
        SnapshotTagIndexRecord tagIndexRecord = new SnapshotTagIndexRecord(
                smsr.getUserTag(),
                smsr.getVersion(),
                smsr.getKey(),
                timeIdVal
        );
        sm.putRecordMeta(tagIndexRecord);

        if(!smsr.getIncrementalBackup()) {
            long start = System.currentTimeMillis();
            ArrayList<SnapshotMetaRecord> snapshotMetas = sm.getAdjustedSnapshotSet(smsr.getKey(), new StringBuilder());
            Set<String> tableNamesSet = new HashSet<>();
            List<MutationMetaRecord> mutationList = new ArrayList<>();
            //1 pending data exists in the mutation table and shadow table, in doSnapshot method mutation pending
            // data change to SUPERSEDED and put to shadow table
            //2 root snapshot may be the current tag or the previous regular tag
            //3 change userTag pending to current tag
            for (SnapshotMetaRecord snapshotMeta : snapshotMetas) {
                List<MutationMetaRecord> mmlist = mm.getMutationsFromUserTag(snapshotMeta.getTableName(),"PENDING",
                        true);
                if (!mmlist.isEmpty()) {
                    for (MutationMetaRecord record : mmlist) {
                        mutationList.add(record);
                        tableNamesSet.add(snapshotMeta.getTableName());
                    }
                }
            }
            if (!tableNamesSet.isEmpty()) {
                xdcBroadcast(GENERIC_FLUSH_MUTATION_FILES, 0, 0, new ArrayList<>(tableNamesSet));
                List<MutationMetaRecord> updateMutations = new ArrayList<>();
                for (MutationMetaRecord updateMutation : mutationList) {
                    //meta data maybe update in xdcBroadcast, reload it
                    updateMutation = mm.getMutationRecord(updateMutation.getRowKey(), true);
                    updateMutation.setUserTag(smsr.getUserTag());
                    updateMutations.add(updateMutation);
                }
                mm.batchPut(updateMutations, true);
            }
            long end = System.currentTimeMillis();
            logger.info("full backup close tag " + smsr.getUserTag() + " time " + (end - start));
        }

    }

    public ArrayList<SnapshotMetaRecord> listLatestBackup() throws Exception {
        ArrayList<SnapshotMetaRecord> snapshotList = null;
            snapshotList = sm.getPriorSnapshotSet();
        return snapshotList;
    }
 
    public ArrayList<SnapshotMetaRecord> getBackedupSnapshotList(String backuptag,
                                          StringBuilder backupType)
      throws Exception {
        ArrayList<SnapshotMetaRecord> snapshotList = null;
        snapshotList = sm.getPriorSnapshotSet(backuptag, backupType);
        return snapshotList;
    }

    public Map<String, TableRecoveryGroup> getRestoreTableList(String backuptag,
                                                  StringBuilder backupType,
                                                  boolean showOnly)
      throws Exception {
        RecoveryRecord rr = new RecoveryRecord(config,
                                              backuptag,
                                              "RESTORE",
                                              /* adjust */ true,
                                              /* force */ false,
                                              /* restoreTS */ 0L,
                                              showOnly);
        Map<String, TableRecoveryGroup> recoveryTableMap = rr.getRecoveryTableMap();
        return recoveryTableMap;
    }

 
    public byte [][] listAllBackups(boolean shortFormat, boolean reverseOrder) throws Exception {
      if (logger.isDebugEnabled())
          logger.debug("listAllBackups called shortFormat " + shortFormat + " reverseOrder " + reverseOrder);

      byte[][] backupList = null;
      int maxTagLen = 0;
      int maxTypeLen = 0;
      ArrayList<BackupSet> backupSetList = new ArrayList<BackupSet>();

      byte [] asciiTime = new byte[40];
      Admin admin = connection.getAdmin();

      int retryCnt = 0;
      boolean retry = false;
      do {
        retry = false;
        if (!admin.isTableEnabled(TableName.valueOf(SNAPSHOT_TABLE_NAME))) {
          retry = true;
          retryCnt++;
        }
        if (retry)
          retry(BRC_SLEEP);
      } while (retry && retryCnt < 10);
      if (retryCnt == 10){
         //Throw this exception since something is wrong here.
         //These tables should never be in disabled state.
         throw new IOException("listAllBackups " + SNAPSHOT_TABLE_NAME + " is not enabled ");
      }

      //Collect all backups from snapshotMeta.
      try {
        ArrayList<SnapshotMetaStartRecord> snapshotStartList = null;
        snapshotStartList = sm.listSnapshotStartRecords();
        if (reverseOrder){
          Collections.sort(snapshotStartList, new SnapshotMetaStartRecordReverseKeyCompare());
        }

        if (logger.isDebugEnabled())
          logger.debug("listAllBackups snapshotStartList.size() :" + snapshotStartList.size()
                         + " reverseOrder: " + reverseOrder);
          
        //Eliminate _SAVE_,_BRC_, _RESTORED_, SNAPSHOT_PEER_ backups and determine size to allocate.
        for (SnapshotMetaStartRecord s : snapshotStartList) {
          String userTag = s.getUserTag();
          
          //collect backup details and store it in backupSet.
          //Do this early so that it gets logged.
          long key;
          if (s.getIncrementalBackup()){
             // For incremental backups we display the start time
             key = s.getKey();
          }
          else {
             // Regular backups log the completion time
             key = s.getCompletionTime();
          }
          idServer.idToStr(ID_TM_SERVER_TIMEOUT, key, asciiTime);
          String timeStamp = Bytes.toString(asciiTime);
          timeStamp = timeStamp.substring(0, 26);
          
          //log every backup including _SAVE_ for debugging purposes.
          if (logger.isInfoEnabled()) logger.info("listAllBackups Backup Name: " + userTag +
                      " key: " + key +
                      " Backup Type: " + s.getBackupType() +
                      " Validity: " + (s.getSnapshotComplete() ? "VALID" : "INVALID") +
                      " TimeStamp " + timeStamp);

          if (( userTag.endsWith("_SAVE_")      ||
                userTag.endsWith("_BRC_")       ||
                userTag.endsWith("_RESTORED_")  ||
                userTag.startsWith("SNAPSHOT_PEER_")) &&
                (shortFormat == false))
              continue;
            
          //cut off timestamp milli seconds before display.
          timeStamp = timeStamp.substring(0, 19);

          // replace space delimiter between date and time with colon (:)
          timeStamp = timeStamp.replace(' ', ':');

          //Currently this is temporarly work around to accomodate four different
          //backup types. In the future, a bit mask would be appropriate.
          //incr -true and sys -true = SYSTEM_PIT
          //incr -false and sys -false = REGULAR
          //incr -true and sys- false = INCREMENTAL
          //incr -false and sys - true = SYSTEM
          boolean incrBkpType = s.getBackupType().equals("INCREMENTAL")? true: false;
          boolean systemBkpType = s.getBackupType().equals("SYSTEM_PIT")? true: false;
          if(systemBkpType)
            incrBkpType = true;
          
          boolean validity = s.getSnapshotComplete();
          String extAttrs = s.getExtendedAttributes();

          BackupSet bset = new BackupSet(s.getUserTag(),
                                         null,
                                         incrBkpType,
                                         systemBkpType,           /*sysBkp*/
                                         validity,                /*validity*/
                                         timeStamp,               /*timestamp*/
                                         extAttrs,                /* extended Atrributes */
                                         false);                  /*imported*/
          bset.resetTableList();
          backupSetList.add(bset);
        }
      }catch (Exception e) {
        String exceptionString = ExceptionUtils.getStackTrace(e);
        if (! exceptionString.contains("listSnapshotStartRecords() Prior record not found")) {
           //any exception other than records not found throw error.
          throw new IOException(e);
        }
      }
      finally{
         admin.close();
      }
        
      //collect backups from backupsys directory
      ArrayList<BackupSet> bkpSetListFromBkpSys = getBackupListFromBackupSys( /* includeTableSet */ false);
      for( BackupSet bk : bkpSetListFromBkpSys) {
        backupSetList.add(bk);
      }
        
      if (reverseOrder){
         Collections.sort(backupSetList, new BackupSetReverseCompare());
      }
      else{
         Collections.sort(backupSetList, new BackupSetCompare());
      }

      if(backupSetList.size() > 0) {
         backupList = new byte[backupSetList.size()][];
      }
  
      //From here it is formatting and display.
      int i =0;
      String concatStringFullRow;
      byte[] b = null;

      for (BackupSet bs : backupSetList) {
        String backupTag = bs.getBackupTag();
        String backupType = bs.getBackupType();
        boolean bkpValid = bs.isValid();
        String timeStamp = bs.getTimeStamp();
        maxTagLen = bs.getBackupTag().length();

        int typelen = bs.getBackupType().length() + 1;
        if(bs.isImported())
            typelen = typelen + "(IMPORTED)".length();
        maxTypeLen = typelen;
          
        //append IMPORTED to backupType
        if(bs.isImported())
          backupType = backupType + "(IMPORTED)";
          
          //append STANDALONE to backupType
          // TBD
          
        //skip display of internal tags except incase of shortFormat.
        if((backupTag.endsWith("_SAVE_") || backupTag.endsWith("_TM_") ||
            backupTag.endsWith("_BRC_") || backupTag.endsWith("_RESTORED_") ||
            backupTag.startsWith("SNAPSHOT_PEER_")) &&
           (shortFormat == false))
           continue;
          
        if (shortFormat == true) {
            concatStringFullRow = String.format("%s", backupTag);
        } 
        else {
// Note the following will be replaced so that the attributes are returned in 1 call,
            concatStringFullRow = String.format("%-" + maxTagLen + "s %-" + maxTypeLen + "s %-10s %-19s",
                                                backupTag,
                                                timeStamp,
                                                bkpValid? "VALID" : "INVALID",
                                                backupType);

// This section to be uncommented when we want to retrieve the extended attibues in one call.
// Requires SQL to parse the returned data before displaying.
//            String rowWithAttributes = String.format("%-" + maxTagLen + "s %-" + maxTypeLen + "s %-10s %-19s",
//                                                backupTag,
//                                                timeStamp,
//                                                bkpValid? "VALID" : "INVALID",
//                                                backupType);
//
//            concatStringFullRow = new String(rowWithAttributes + " " + bs.getExtAttrs());
        }
          
        if (logger.isDebugEnabled())
            logger.debug("listAllBackups  : " + concatStringFullRow);
        b = concatStringFullRow.getBytes();
        backupList[i++] = b;
      }
      return backupList;
    }

    public String getLastError() {
        return lastError;
    }

    public String getLastWarning() {
        return lastWarning;
    }

    public boolean release() throws IOException {
        if (logger.isInfoEnabled()) logger.info("BackupRestoreClient release called.");

        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }

            if (sm != null) {
                sm.close();
                sm = null;
            }

            if (lm != null) {
                lm.close();
                lm = null;
            }

            if (mm != null) {
                mm.close();
                mm = null;
            }

            if (config != null)
                config = null;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }
       
    public boolean xdcMutationCaptureBegin(String xdcTag) throws Exception {
      // get timestamp
      timeIdVal = getIdTmVal();
      if (logger.isInfoEnabled())logger.info("xdcMutationCaptureBegin started; id " + timeIdVal);

      // Initialize Full SnapshotMetaStartRecord
      initializeSnapshotMeta(timeIdVal, xdcTag, /* incrementalBackup */ false, "nullAttributes", "xdcMutationType");

      return true;
    }

    public boolean xdcMutationCaptureAddTable(String xdcTag, String tableName) throws Exception {
      
      // get timestamp
      timeIdVal = getIdTmVal();

      // snapshot name cannot contain ":".
      // If the src name is prefixed by "namespace:", replace
      // ":" by "_"
      String snapshotName = new String(tableName.replace(":", "_") + "_snapshot");
      String snapshotPath = SnapshotMetaRecord.createPath(snapshotName);
      Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());

//TEMP need to fix
boolean incrementalSnapshot = false;

      updateSnapshotMeta(timeIdVal, 
			 xdcTag,
			 incrementalSnapshot,
	         true /* userGenerated */,
	         false /* incrementalTable */,
	         false /* sqlMetaData */,
			 tableName,
			 snapshotName,
			 snapshotPath,
			 lobSnapshots,
			 "Default");
      
      return true;
  }

    public boolean xdcMutationCaptureEnd() throws Exception {
      
      // Complete snapshotMeta update.
      completeSnapshotMeta();
      
      return true;
  }

    public void dumpRecovery(final String backuptag, boolean adjust, boolean showOnly) throws Exception {

      try {
        
        RecoveryRecord rr = new RecoveryRecord(config,
                                               backuptag,
                                               "EXPORT",
                                               adjust,
                                               /* force */ false,
                                               /* restoreTS */ 0L,
                                               showOnly);

        
        Map<String, TableRecoveryGroup> recoveryTableMap = rr.getRecoveryTableMap();

        int loopCount = 0;
        
        for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet()) {            
          String tableName = tableEntry.getKey();
          TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();

          SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
          final String snapshotPath = tableMeta.getSnapshotPath();
          final String snapshotName = tableMeta.getSnapshotName();
        
        
          List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();
          if (logger.isInfoEnabled()) {
              logger.info("tableName: " + tableName +
                      "snapshotName: " + snapshotName +
                      "snapshotPath: " + snapshotPath +
                      "Mutation list size: " + mutationList.size());
          }
          for (int i = 0; i < mutationList.size(); i++) {
             MutationMetaRecord mutationRecord = mutationList.get(i);
             String mutationPathString = mutationRecord.getMutationPath();
             if (logger.isInfoEnabled()) logger.info("mutation : " + mutationPathString);
          }
        }
       
      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("dumpRecovery Exception occurred " ,e);
        throw e;
      } finally {
      }
    } 
  
     public void dumpBackupMeta(final String backuptag, String source) throws Exception {

      try {
        
        //remove trailing '/' if present.
        if(source.endsWith("/"))
          source = source.substring(0,source.length() -1);
        String fullSrc = source + "/" + backuptag;
        
        Path srcRoot = new Path(fullSrc);
        FileSystem srcFs = FileSystem.get(srcRoot.toUri(), config);
        Path backupMetaFilePath = new Path(fullSrc + "/" + "backupMeta.bin");
        if(!srcFs.exists(backupMetaFilePath))
          throw new IOException("Backup location is invalid");
        
        FSDataInputStream is = srcFs.open(backupMetaFilePath);
        ObjectInputStream ois = new ObjectInputStream(is);
        BackupSet bkpset = (BackupSet)ois.readObject();
        ois.close();
        is.close();

        if (logger.isInfoEnabled()) logger.info("BackupSet : " + bkpset.displayString());
        ArrayList<TableSet>  tableSetList = bkpset.getTableList();
          
        if(tableSetList.size() <= 0)
          throw new IOException("dumpBackupMeta objects are Empty");
        
        for (final TableSet t : tableSetList) {
          if (logger.isInfoEnabled()) logger.info(" TableSet : " + t.displayString());
        }
      } catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("dumpBackupMeta Exception occurred " ,e);
        throw e;
      } finally {
      }
    }
     
     public void dumpRemoteRecoveryRecord(final String backuptag, String source) throws Exception {

	     try {
	         //remove trailing '/' if present.
	         if(source.endsWith("/"))
	           source = source.substring(0,source.length() -1);
	         String fullSrc = source + "/" + backuptag;
	         
	         Path srcRoot = new Path(fullSrc);
	         FileSystem srcFs = FileSystem.get(srcRoot.toUri(), config);
	         Path recoveryRecordFilePath = new Path(fullSrc + "/" + "recoveryRecord.bin");
	         if(!srcFs.exists(recoveryRecordFilePath))
	           throw new IOException("Backup location is invalid");
	         
	         FSDataInputStream is = srcFs.open(recoveryRecordFilePath);
	         ObjectInputStream ois = new ObjectInputStream(is);
	         RecoveryRecord rr = (RecoveryRecord)ois.readObject();
	         ois.close();
	         is.close();
	         
	       Map<String, TableRecoveryGroup> recoveryTableMap = rr.getRecoveryTableMap();
	
	       int loopCount = 0;
	       
	       for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet()) {            
	         String tableName = tableEntry.getKey();
	         TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();
	
	         SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
	         final String snapshotPath = tableMeta.getSnapshotPath();
	         final String snapshotName = tableMeta.getSnapshotName();
	       
	         List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();
             if (logger.isInfoEnabled()) {
                logger.info("tableName: " + tableName +
                      "snapshotName: " + snapshotName +
                      "snapshotPath: " + snapshotPath +
                      "Mutation list size: " + mutationList.size());
             }
	         for (int i = 0; i < mutationList.size(); i++) {
	            MutationMetaRecord mutationRecord = mutationList.get(i);
	            String mutationPathString = mutationRecord.getMutationPath();
	            if (logger.isInfoEnabled()) logger.info("mutation : " + mutationPathString);
             }
	       }
	      
	     } catch (Exception e) {
	       if (logger.isErrorEnabled()) logger.error("dumpRemoteRecoveryRecord Exception occurred " ,e);
	       throw e;
	     } finally {
	     }
   } 

    public void unlock(final String backuptag) throws Exception {

      try {
         if (logger.isInfoEnabled()) logger.info("unlock : " + backuptag);
         sm.unlock(backuptag);
      } catch (Exception e) {
         if (logger.isErrorEnabled()) logger.error("unlock : " + backuptag + " exception " ,e);
         throw e;
      } finally {
      }
    }

    public String lockHolder() throws Exception{
       if (logger.isInfoEnabled()) 
          logger.info("ENTRY lockHolder ");

       if (logger.isInfoEnabled()) logger.info("lockHolder start ");
       try {
          String backupTag="";
          SnapshotMetaLockRecord record = sm.getLockRecord();
          if (record ==  null)
            return backupTag;
          else if (!record.getLockHeld())
            return backupTag;
          return record.getUserTag();
       } catch (Exception e) {
          if (logger.isErrorEnabled()) logger.error("lockHolder exception " ,e);
          throw e;
       } finally {
       }
    }

    public void checkLock() throws Exception {
       if (logger.isInfoEnabled()) logger.info("checkLock start ");
       try {
          SnapshotMetaLockRecord record = sm.getLockRecord();
          if (record ==  null){
             if (logger.isInfoEnabled()) logger.info("Lock record not found: SnapshotMeta is unlocked ");
          }
          else if (!record.getLockHeld()){
             if (logger.isInfoEnabled()) logger.info("SnapshotMeta is unlocked ");
          }
          else {
             if (logger.isInfoEnabled()) logger.info("SnapshotMeta is locked for " + record.getUserTag());
          }
       } catch (Exception e) {
          if (logger.isErrorEnabled()) logger.error("checkLock exception " ,e);
          throw e;
       } finally {
       }
    }

    //public method called from XDC, supports parallel threads.
    //broadcast to transaction table for <key,value> or <command,action>
    public void xdcBroadcastTRG(final int key, final int value, final int peer_id,
                                ArrayList<TableRecoveryGroup> tableRecoveryGroupList
                               ) throws Exception {

       ArrayList<String> tableList = new ArrayList<String>();
       for (TableRecoveryGroup tableEntry : tableRecoveryGroupList) {
          tableList.add(tableEntry.getSnapshotRecord().getTableName());
       }

       this.xdcBroadcast(key, value, peer_id, tableList);

    }

    //public method called from XDC, supports parallel threads.
    //broadcast to transaction table for <key,value> or <command,action>
    public void xdcBroadcastTableSet(final int key, final int value, final int peer_id,
                                ArrayList<TableSet> tableSetList
                               ) throws Exception {

       ArrayList<String> tableList = new ArrayList<String>();
       for (TableSet tableSetEntry : tableSetList) {
          tableList.add(tableSetEntry.getTableName());
       }

       this.xdcBroadcast(key, value, peer_id, tableList);

    }

    //public method called from XDC, supports parallel threads.
    //broadcast to transaction table for <key,value> or <command,action>
    public void xdcBroadcast(final int key, final int value, final int peer_id,
                             ArrayList<String> tableList
    ) throws Exception {
        if (logger.isInfoEnabled()) logger.info("ENTRY xdc broadcast Tables for <key,value,peer_id> " +
                key + " " + value + " " + peer_id + " num of tables: " + tableList.size());

        List<String> existTableList = new ArrayList<>();
        Admin admin = connection.getAdmin();
        if (admin == null) {
            existTableList = tableList;
        }
        else {
            for (final String table : tableList) {
                try {
                    if (admin.isTableEnabled(TableName.valueOf(table))) {
                        if (HBASE_NATIVE_TABLES.contains(table)) {
                            if (logger.isInfoEnabled()) logger.info("table: " + table + " is a hbase native table.");
                            continue;
                        }
                        existTableList.add(table);
                    } else {
                        if (logger.isWarnEnabled())
                            logger.warn("table " + table + " is disable");
                    }
                } catch (TableNotFoundException e){
                    continue;
                }
            }
        }
        ExecutorService xdcThreadPool = Executors.newFixedThreadPool(Math.max((existTableList.size() / 100 + 2), 16));
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(xdcThreadPool);
        int loopCount = 0;

        for (final String table : existTableList) {
            try {
                // Send work to thread
                compPool.submit(new BackupRestoreCallable() {
                    public Integer call() throws Exception {
                        return doXDCBroadcast(key, value, peer_id, table);
                    }
                });
                loopCount++;
            } catch (Exception e) {
                if (!(e instanceof TableNotFoundException)) {
                    if (logger.isWarnEnabled()) logger.warn("BRC xdc broadcast Tables exception : ", e);
                    xdcThreadPool.shutdown();
                    throw new IOException(e);
                }
            }
        } // for
        // simply to make sure they all complete, no return codes necessary at the moment
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
            try {
                int returnValue = compPool.take().get();
            } catch (Exception e) {
                if (e.toString().contains("TableNotFoundException")) {
                    if (logger.isInfoEnabled()) logger.info("BRC xdc broadcast TableNotFoundException");
                } else if (e.toString().contains("Coprocessor result is null, retries exhausted")) {
                    if (logger.isInfoEnabled()) logger.info("BRC xdc broadcast ignoring null coprocessor result");
                } else {
                    if (logger.isErrorEnabled())
                        logger.error("BRC xdc broadcast Tables exception retrieving results : ", e);
                    xdcThreadPool.shutdown();
                    throw new IOException(e);
                }
            }
        }
        xdcThreadPool.shutdown();
        if (m_verbose) {
            System.out.println("BRC xdc broadcast to tables with size " + tableList.size());
        }
    }

    //public method called from XDC, supports parallel threads.
    //broadcast to transaction table for <key,value> or <command,action>
    public void xdcRecoveryBroadcast(final int key, final int value, final int peer_id,
                                ArrayList<String> tableList
                               ) throws Exception {

      boolean lv_done = false;
      int lv_status = 0;
      boolean flag = (value == 0) ? false : true;
      int retryCount = 0;
      String tableName = null;

      if (logger.isInfoEnabled())
          logger.info("ENTRY xdc recovery broadcast Tables for <key,value,peer_id> " +
                 key + " " + value + " " + peer_id + " num of tables: " + tableList.size());

      try {
            STRConfig pSTRConfig = null;
            Configuration lv_config = HBaseConfiguration.create();     
             pSTRConfig = STRConfig.getInstance(lv_config);
             lv_config = pSTRConfig.getPeerConfiguration(peer_id, false);
             if (lv_config == null) {
                    System.out.println("Peer ID: " 
                              + peer_id + " does not exist OR it has not been configured for synchronization.");
                    System.exit(1);
             } 
            pSTRConfig.addPeerConnection(peer_id);
            for (final String hbaseTableName : tableList) {
                    tableName = hbaseTableName;
                    TransactionalTable tTable = new TransactionalTable(hbaseTableName, 
                                             pSTRConfig.getPeerConnection(peer_id));
                    lv_done = false;
                    while (!lv_done) {
                              if (m_verbose) {
                                   System.out.println("BRC xdc broadcast request,"
                                                   + " peer: " + peer_id + " table: " + hbaseTableName
                                                   + " command: " + key + " flag: " + flag);
                             }
                              lv_done = true;
                              retryCount++;
                              lv_status = tTable.broadcastRequest(key, flag);
                              if (m_verbose) {
                                        System.out.println("broadcast request, status: " + lv_status);
                              }
                              if ((lv_status != 0) && (retryCount <= 10)) {
                                        System.out.println("Retry broadcast request in 3 seconds");
                                        lv_done = false;
                                        try {
                                                  Thread.sleep(1000);
                                         } catch (InterruptedException ie) {
                                         }
                              }
                              else if ((lv_status != 0) && (retryCount > 10)) {
                                  if (logger.isErrorEnabled())
                                      logger.error("do XDC Broadcast encountered exception on table: "
                                                              + hbaseTableName);
                                  System.out.println("Retry broadcast recovery request fails in 10 tries ... ");
                                  throw new IOException("BRC xdc recovery request retry exhausted");
                              }
                    } // while
                    tTable.close();
           } // for-loop     
      }catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("BRC xdc broadcast Tables exception : ", e);
        System.out.println("do XDC Broadcast encountered exception on table: " + tableName);
        throw new IOException(e);
      }
      System.out.println("BRC xdc recovery broadcast to tables with size " + tableList.size());

    }  

    //public method called for testing
    public void mutationrange(final long systemTime, final long startTime, final long endTime) throws Exception {
      if (logger.isInfoEnabled())
          logger.info("ENTRY mutationrange for systemTime: " + systemTime + " startTime: " + startTime + " endTime: " + endTime);

      ArrayList<MutationMetaRecord> mutationList = new ArrayList<MutationMetaRecord>();
      try {
         mutationList = mm.getMutationsFromRange(systemTime, startTime, endTime);
         ListIterator<MutationMetaRecord> mutationIter;
         for (mutationIter = mutationList.listIterator(); mutationIter.hasNext();) {
            MutationMetaRecord tmpRecord = mutationIter.next();
            String tmpTable = tmpRecord.getTableName();
         }
      }catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("BRC getMutationsFromRange exception : ", e);
        throw new IOException(e);
      }
    }

    //public method called for testing
    public void cleanUnneededMutations() throws Exception {
      if (logger.isInfoEnabled())
          logger.info("ENTRY cleanUnneededMutations ");
      if (m_verbose){
         System.out.println("ENTRY cleanUnneededMutations ");
      }

      FileSystem fs = FileSystem.get(config);
      ArrayList<MutationMetaRecord> mutationList = new ArrayList<MutationMetaRecord>();
      try {
         mutationList = mm.getUnneededMutations();
         ListIterator<MutationMetaRecord> mutationIter;
         for (mutationIter = mutationList.listIterator(); mutationIter.hasNext();) {
            MutationMetaRecord mutationRecord = mutationIter.next();
            String mutationPathString = mutationRecord.getMutationPath();
            Path mutationPath = new Path (new URI(mutationPathString).getPath());

            // Delete mutation file
            if (logger.isDebugEnabled())
               logger.debug("cleanUnneededMutations deleting unneeded mutation file at " + mutationPath);
             if (m_verbose) {
                System.out.println("cleanUnneededMutations deleting unneeded mutation file at " + mutationPath);
             }
             if (fs.exists(mutationPath))
               fs.delete(mutationPath, false);
             else
               if (logger.isWarnEnabled()) logger.warn("mutation file: " + mutationPath + " not exists.");

             // Delete mutation record
             if (logger.isDebugEnabled())
                logger.debug("deleteRecoveryRecord deleting unneeded mutationMetaRecord " + mutationRecord);
             if (m_verbose) {
                System.out.println("deleteRecoveryRecord deleting unneeded mutationMetaRecord " + mutationRecord);
             }
             MutationMeta.deleteMutationRecord(mutationRecord.getRowKey());
          }

      }catch (Exception e) {
        if (logger.isErrorEnabled()) logger.error("BRC cleanUnneededMutations exception : ", e);
        throw new IOException(e);
      }
      if (m_verbose){
         System.out.println("BRC cleanUnneededMutations returned " + mutationList.size() + " records");
      }
    }

    public static boolean deleteDir(String dirPathStr, boolean recursive) throws IOException
    {
       if (logger.isDebugEnabled())
          logger.debug("deleteDir(" + dirPathStr + " recursive " + recursive + ")");

       Path dirPath = new Path(dirPathStr );
       FileSystem fs = FileSystem.get(dirPath.toUri(), config);
       FileStatus[] fileStatus;
       if (fs.isDirectory(dirPath)){
          fileStatus = fs.listStatus(dirPath);
       }
       else{
          throw new IOException("The path " + dirPath + " is not a directory");
       }
       FileStatus aFileStatus;
       if (fileStatus != null) {
          for (int i = 0; i < fileStatus.length; i++)
          {
             aFileStatus = fileStatus[i];
             if (! aFileStatus.isDirectory()) {
                String pathName =  aFileStatus.getPath().toString();
                String filenameParts[] = pathName.split(dirPathStr);
                //if (filenameParts.length == 2 && filenameParts[1].startsWith(startingFileName))
                if (logger.isDebugEnabled())
                    logger.debug("deleteDir deleting file " + aFileStatus.getPath().toString());
                fs.delete(aFileStatus.getPath());
             }
             else{
                if (logger.isDebugEnabled())
                    logger.debug("deleteDir deleting directory " + aFileStatus.getPath().toString());
                fs.delete(aFileStatus.getPath(), recursive);
             }
          }
       }
       return true;
    }

    // API for special crash processing where tables are restored and mutations replayed to a safe interval
    public void crashRecover(int numParallelThreads) throws IOException{

       if (m_verbose) {
           System.out.println("[CRASHRECOVER]: " + "numThreads " + numParallelThreads);
       }
       if (logger.isInfoEnabled()) logger.info("[CRASHRECOVER]: " + "numThreads " + numParallelThreads);

       try{
          SnapshotMutationFlushRecord flushRec = sm.getNthFlushRecord(2);  // Assume this record should be safely flushed
          ArrayList<String> recovTables = flushRec.getTableList();
          final long limitId = flushRec.getCommitId();

          threadPool = Executors.newFixedThreadPool(numParallelThreads == 0 ? 1 : numParallelThreads);
          int loopCount = 0;
          CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);

          // For each table, in the list restore snapshot
          for (final String t : recovTables) {
             // Send work to thread
             compPool.submit(new BackupRestoreCallable() {
                   public Integer call() throws Exception {
                      return doRestore(t, limitId);
                   }
               });
             loopCount++;
          } // for-loop

          // simply to make sure they all complete, no return codes necessary at the moment
          for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
             int returnValue = compPool.take().get();
          }

       } catch (Exception e) {
          if (logger.isErrorEnabled()) logger.error("Restore exception in crashRecover: ", e);
          throw new IOException(e);
       }
       finally{
          if(threadPool != null)
          threadPool.shutdownNow();
       }
       if (m_verbose) {
          System.out.println("[CRASHRECOVER]: exit");
       }
       if (logger.isInfoEnabled()) logger.info("[CRASHRECOVER]: exit");

    }

    /**
     * used to determine whether the schema is a backup metadata schema
     * @param schmaName
     * @param backupTag
     * @return
     */
    private boolean isBackupSchema(String schmaName, String backupTag) {
        String backupSchema = TRAF_CATALOG_PREFIX + "." + TRAF_BACKUP_MD_PREFIX + backupTag + "_";
        if (backupSchema.equals(schmaName))
            return true;
        else
            return false;
    }

    // this table is created in method CmpSeabaseDDL::createProgressTable
    // in file sql/sqlcomp/CmpSeabaseBackup.cpp.
    // Column names and other info used here need to be in sync with
    // column info created in progress table in that method.
    private boolean updateBREIprogress(
            String oper, String tag,
            int numTotalTables, int numCurrTables,
            int progressUpdateDelay,
            int tagOperCount, boolean restoreOnlyMD)
            throws Exception {
        final Put put;
        byte[] family = null;
        byte[] qualifier = null;
        byte[] colName, colValue;

        if (connection == null) {
            throw new Exception("connection cannot be null");
        }

        if (ttable == null) {
            throw new Exception("ttable cannot be null");
        }

        if (progressUpdateDelay == -1) // do not update
            return true;

        // current time as juliantimestamp
        Instant instant = Instant.now();
        long instantJTS = instant.toEpochMilli() * 1000;
        long COM_EPOCH_TIMESTAMP = 210866760000000000L;
        instantJTS = instantJTS + COM_EPOCH_TIMESTAMP;

        // Step type "BackupObjects","RestoreObjects","ExportObjects",
        // "ImportObjects" must be the same as used in file 
        // sqlcomp/CmpSeabaseBackup.cpp when method upsert/updateProgressTable
        // is called.
        String stepText = null;
        if (oper == "BACKUP")
            stepText = new String("BackupObjects");
        else if (oper == "RESTORE") {
            if (restoreOnlyMD)
                stepText = new String("RestoreMetadata");
            else
                stepText = new String("RestoreObjects");
        } else if (oper == "EXPORT")
            stepText = new String("ExportObjects");
        else if (oper == "IMPORT")
            stepText = new String("ImportObjects");

        // create rowID by blankpadding columns to the declared length
        // in progress table.
        String rowID = String.format("%-20s%-64s%-40s%-8s",
                oper, tag, stepText, tagOperCount);
        put = new Put(rowID.getBytes());

        // update column ENDTIME (Hbase Col Qualifier 7)
        family = Bytes.toBytes("#1");
        colName = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN).put((byte) 7).array();
        colValue = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(instantJTS).array();
        put.add(family, colName, colValue);

        // update column PARAM1 (numTotalTables) (HBase Col Qualifier 8)
        family = Bytes.toBytes("#1");
        colName = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN).put((byte) 8).array();
        colValue = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(numTotalTables).array();
        put.add(family, colName, colValue);

        // update column PARAM2 (numCurrTables) // (HBase Col Qualifier 9)
        family = Bytes.toBytes("#1");
        colName = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN).put((byte) 9).array();
        colValue = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(numCurrTables).array();
        put.add(family, colName, colValue);

        // update column PERC // (HBase Col Qualifier 10)
        family = Bytes.toBytes("#1");
        colName = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN).put((byte) 10).array();
        int perc = (numCurrTables * 100) / numTotalTables;
        colValue = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(perc).array();
        put.add(family, colName, colValue);

        long timeIdVal = getIdTmVal();
        ttable.putRegionTx(timeIdVal, put, true /* autoCommit */, false /* recoveryToPitMode */, "");

        return true;
    }

    private int getBREIOperCount(String oper, String tag) throws Exception {
        long start = System.currentTimeMillis();
        String stepText = "Initialize";

        // rowKey is : %-20s%-64s%-40s-%d (oper, tag, stepText, tag_oper_cnt)
        String partRowKey = String.format("%-20s%-64s%-40s", oper, tag, stepText);
        Scan scan = new Scan();
        scan.setRowPrefixFilter(Bytes.toBytes(partRowKey));
        scan.setCaching(100);
        scan.setCacheBlocks(false);

        ResultScanner scanner = ttable.getScanner(scan, 0);
        int result = 0;
        for (Result iter : scanner)
            result++;

        long end = System.currentTimeMillis();
        if (logger.isInfoEnabled())
            logger.info("[getBREIOperCount] tag oper count: " + result + " for tag: " + tag + " and cost: " + (end - start));
        scanner.close();

        return result;
    }

    static public void main(String[] args) {
      
      boolean lv_verbose = false;
      String preload = System.getenv("LD_PRELOAD");
      if (preload == null){
         System.out.println("\n*** LD_PRELOAD not configured.  Should \"export LD_PRELOAD=${JAVA_HOME}/jre/lib/${JRE_LIB_DIR}/libjsig.so:${TRAF_HOME}/export/lib${SQ_MBTYPE}/libseabasesig.so\" ***\n");
         System.exit(1);
      }
      else{
         System.out.println("\n*** LD_PRELOAD configured: " + preload + " ***\n");
      }

      for ( int i = 0; i < args.length; i++ )
      {
        if (args[i].compareTo("-v") == 0) {
        lv_verbose = true;
        }
        if(args[i].equals("import"))
        {
          String backupTag = args[++i].toString();
          config = HBaseConfiguration.create();
          
          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            String srcLoc = args[++i].toString();
            int parallelThreads = Integer.valueOf(args[++i]);
            brc.importBackup(backupTag, srcLoc, parallelThreads, false, -1);
            if (lv_verbose) {
              System.out.println("importBackup Success");
            }
          }catch(Exception e) {
            System.out.println("importBackup failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("idTmId"))
        {
          config = HBaseConfiguration.create();
          String timeString = null;

          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            long id = brc.getIdTmVal();
            System.out.println("idTm returned " + id);
            timeString = brc.idToStr(id);
            System.out.println("idToStr returned timeString " + timeString);
            long id2 = brc.strToId(timeString);
            System.out.println("strToId (" + timeString + ") returned id " + id2);
            System.out.println("difference is " + (id2 - id));
          }catch(Exception e) {
            System.out.println("idTmId failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("idToStr"))
        {
          config = HBaseConfiguration.create();
          long id = Long.valueOf(args[++i]);
          String timeString = null;

          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            timeString = brc.idToStr(id);
            System.out.println("idToStr (" + id + ") returned timeString " + timeString);
          }catch(Exception e) {
            System.out.println("idToStr failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("strToId"))
        {
          config = HBaseConfiguration.create();
          String timestamp = args[++i];
          long id = 0L;

          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            id = brc.strToId(timestamp);
            System.out.println("strToId returned id " + id);
          }catch(Exception e) {
            System.out.println("strToId failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("sysbackup"))
        {
          config = HBaseConfiguration.create();

          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            String backupTag =  args[++i].toString();
            String backupType = "SYSTEM";
            Object [] backupTableList = new Object[1];
            backupTableList[0] = args[++i].toString();
            brc.backupObjects(backupTableList, 
                              null,
                              null,
                              backupTag, 
                              "", /* Extended Attributes */
                              backupType,
                              1 /*parallel threads */,
                              -1);
            if (lv_verbose) {
              System.out.println("Backup Success");
            }
          }catch(Exception e) {
            System.out.println("Backup failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("export"))
        {
          String backupTag = args[++i].toString();
          config = HBaseConfiguration.create();

          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            String destLoc = args[++i].toString();
            int parallelThreads = Integer.valueOf(args[++i]);
            brc.exportBackup(backupTag, destLoc, parallelThreads, false, -1);
            if (lv_verbose) {
              System.out.println("exportBackup Success");
            }
          }catch(Exception e) {
            System.out.println("exportBackup failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("dumprr"))
        {
          String backupTag = args[++i].toString();
          config = HBaseConfiguration.create();
          
          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            String adjust = args[++i].toString();
            
            brc.dumpRecovery(backupTag, adjust.equals("y"), /* showOnly */ true);
            if (lv_verbose) {
              System.out.println("dumpRecovery Success");
            }
          }catch(Exception e) {
            System.out.println("dumpRecovery failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("mutationrange"))
        {
          long systemTime = Long.parseLong(args[++i].toString());
          long startTime = Long.parseLong(args[++i].toString());
          long endTime = Long.parseLong(args[++i].toString());
          config = HBaseConfiguration.create();

          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);

            brc.mutationrange(systemTime, startTime, endTime);
            if (lv_verbose) {
              System.out.println("mutationrange success");
            }
          }catch(Exception e) {
            System.out.println("mutationrange failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("getMutationRecord"))
        {
          String key = args[++i];
          config = HBaseConfiguration.create();

          try{
            MutationMeta mm = new MutationMeta(config);
            MutationMetaRecord mmr = mm.getMutationRecord(key);
            System.out.println("getMutationRecord success for record: " + mmr);
          }catch(Exception e) {
            System.out.println("getMutationRecord failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("getSnapshotRecord"))
        {
          long key = Long.parseLong(args[++i].toString());
          config = HBaseConfiguration.create();
          SnapshotMeta sm = null;
          try{
            sm = new SnapshotMeta(config);
          }catch(Exception e) {
            System.out.println("getSnapshotRecord failed creating SmapshotMeta " + e);
            System.exit(1);
          }

          // There are various types of records in the Snapshot meta table.  Try them all
          try{
            // Try a start record
            System.out.println("getSnapshotRecord looking for start record with key: " + key);
            SnapshotMetaStartRecord smsr = sm.getSnapshotStartRecord(key);
            if (smsr != null){
               System.out.println("getSnapshotRecord success for start record: " + smsr);
               System.exit(0);
            }
          }catch(Exception e) {
            System.out.println("getSnapshotRecord failed with exception on start record " + e);
            System.exit(1);
          }

          try{
            // Try a meta record
            System.out.println("getSnapshotRecord looking for meta record with key: " + key);
            SnapshotMetaRecord smr = sm.getSnapshotRecord(key);
            if (smr != null){
              System.out.println("getSnapshotRecord success for meta record: " + smr);
              System.exit(0);
            }
          }catch(Exception e) {
            System.out.println("getSnapshotRecord failed with exception on meta record " + e);
            System.exit(1);
          }

          try{
            // Try an incremental record
            System.out.println("getSnapshotRecord looking for an incremental record with key: " + key);
            SnapshotMetaIncrementalRecord smir = sm.getIncrementalSnapshotRecord(key);
            if (smir != null){
              System.out.println("getSnapshotRecord success for incremental record: " + smir);
              System.exit(0);
            }
          }catch(Exception e) {
            System.out.println("getSnapshotRecord failed with exception on incremental record " + e);
            System.exit(1);
          }

          if (key == 0L){
            try{
              // Try a lock record
              System.out.println("getSnapshotRecord looking for the lock record with key: " + key);
              SnapshotMetaLockRecord smlr = sm.getLockRecord();
              if (smlr != null){
                System.out.println("getSnapshotRecord success for lock record: " + smlr);
                System.exit(0);
              }
            }catch(Exception e) {
              System.out.println("getSnapshotRecord failed with exception on lock record " + e);
              System.exit(1);
            }
          }

          if (key == 1L){
            try{
              // Try a Mutation Flush record
              int n = 0;
              if (i < args.length){
                 n = ++i;
              }
              System.out.println("getSnapshotRecord looking for the mutation flush record with n " + n);
              SnapshotMutationFlushRecord smfr = sm.getNthFlushRecord(n);
              if (smfr != null){
                System.out.println("getSnapshotRecord success for flush record: " + smfr);
                System.exit(0);
              }
            }catch(Exception e) {
              System.out.println("getSnapshotRecord failed with exception on flush record " + e);
              System.exit(1);
            }
          }
          System.out.println("getSnapshotRecord failed to find a record with key " + key);
          System.exit(0);
        }
        else if(args[i].equals("restore"))
        {
          String backupTag = args[++i].toString();
          config = HBaseConfiguration.create();
          
          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            Object [] tableList = new Object[1];
            tableList[0] = args[++i].toString();

            brc.restoreObjects(backupTag, null, tableList,
                               /* restoreToTS */ false,
                               /* showObjects */ false,
                               /* saveObjects */ false,
                               /* restoreSavedObjects */ false,
                               /* number Threads */ 1,
                               -1);
            if (lv_verbose) {
              System.out.println("restore Success");
            }
          }catch(Exception e) {
            System.out.println("restore failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("restoretotime"))
        {
          String backupTag = args[++i].toString();
          config = HBaseConfiguration.create();
          if (!backupTag.contains(".")) {
        	  backupTag = backupTag + ".999999";
          }
          
          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            
            Object [] tableList = new Object[1];
            //tableList[0] = args[++i].toString();

            brc.restoreToTimeStamp(backupTag, 1, false);
            if (lv_verbose) {
              System.out.println("restoreToTimeStamp Success");
            }
          }catch(Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            System.out.println("restoreToTimeStamp failed " + sw.toString());
            System.exit(1);
          }
        }
        else if(args[i].equals("dumpbackupmeta"))
        {
          String backupTag = args[++i].toString();
          config = HBaseConfiguration.create();
          
          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            String source = args[++i].toString();
            
            brc.dumpBackupMeta(backupTag, source);
            if (lv_verbose) {
              System.out.println("dumpBackupMeta Success");
            }
          }catch(Exception e) {
            System.out.println("dumpBackupMeta failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("dumpremoterr"))
        {
          String backupTag = args[++i].toString();
          config = HBaseConfiguration.create();
          
          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            String source = args[++i].toString();
            
            brc.dumpRemoteRecoveryRecord(backupTag, source);
            if (lv_verbose) {
              System.out.println("dumpremoterr Success");
            }
          }catch(Exception e) {
            System.out.println("dumpremoterr failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("listall"))
        {
          config = HBaseConfiguration.create();
          
          try{
            BackupRestoreClient brc = new BackupRestoreClient(config);
            byte[][] backupList = brc.listAllBackups(false, false);
            for (int j = 0; j < backupList.length; j++) {
                final String displaystr = Bytes.toString(backupList[j]);
              System.out.println(displaystr);
            }
          }catch(Exception e) {
            System.out.println("listall failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("current"))
        {
          config = HBaseConfiguration.create();

          try{
            SnapshotMeta sm = new SnapshotMeta(config);
            SnapshotMetaRecord rec = sm.getCurrentSnapshotRecord("TRAF_regr:TRAFODION.SCH0511.T1INCR");
            System.out.println(" Rec is " + rec);
          }catch(Exception e) {
            System.out.println("current failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("prior"))
        {
          config = HBaseConfiguration.create();
          String timestamp = args[++i];
          long id = 0L;

          //2019-01-26:17:26:22 convert to 2019-01-26 17:26:22
          if(timestamp.charAt(10) == ':') {
            String beginstr = timestamp.substring(0,10);
            String endstr = timestamp.substring(11);
            timestamp = beginstr + " " + endstr;
          }

          if (!timestamp.contains(".")) {
            timestamp = timestamp + ".000000";
          }

          BackupRestoreClient brc = null;
          try{
            brc = new BackupRestoreClient(config);
            id = brc.strToId(timestamp);
            System.out.println("strToId returned id " + id);
          }catch(Exception e) {
            System.out.println("strToId failed " + e);
            System.exit(1);
          }

          try{
            SnapshotMeta sm = new SnapshotMeta(config);
            String tag = brc.getPriorBackupTag(id);
            System.out.println(" Tag is " + tag);
          }catch(Exception e) {
            System.out.println("prior failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("priorbsys"))
        {
          config = HBaseConfiguration.create();
          String timestamp = args[++i];
          long id = 0L;

          //2019-01-26:17:26:22 convert to 2019-01-26 17:26:22
          if(timestamp.charAt(10) == ':') {
            String beginstr = timestamp.substring(0,10);
            String endstr = timestamp.substring(11);
            timestamp = beginstr + " " + endstr;
          }

          if (!timestamp.contains(".")) {
            timestamp = timestamp + ".000000";
          }

          BackupRestoreClient brc = null;
          try{
            brc = new BackupRestoreClient(config);
            id = brc.strToId(timestamp);
            System.out.println("strToId returned id " + id);
          }catch(Exception e) {
            System.out.println("strToId failed " + e);
            System.exit(1);
          }

          try{
            SnapshotMeta sm = new SnapshotMeta(config);
            String tag = brc.getRestoreToTsBackupTag(timestamp);
            System.out.println(" Tag is " + tag);
          }catch(Exception e) {
            System.out.println("priorbsys failed " + e);
            System.exit(1);
          }
        }
        else if(args[i].equals("checklock"))
        {
          config = HBaseConfiguration.create();

          try{
             BackupRestoreClient brc = new BackupRestoreClient(config);
             brc.checkLock();
          }
          catch(Exception e){
             System.out.println("checkLock failed " + e);
             System.exit(1);
          }
        }
        else if(args[i].equals("lock"))
        {
          String tag = args[++i].toString();
          config = HBaseConfiguration.create();

          try{
             SnapshotMeta lvSM = new SnapshotMeta(config);
             lvSM.lock(tag);
          }
          catch(Exception e){
             System.out.println("lock failed " + e);
             System.exit(1);
          }
        }
        else if(args[i].equals("xdcLock"))
        {
          config = HBaseConfiguration.create();

          try{
             BackupRestoreClient brc = new BackupRestoreClient(config);
             brc.xdcLock();
          }
          catch(Exception e){
             System.out.println("xdclock failed " + e);
             System.exit(1);
          }
        }
        else if(args[i].equals("unlock"))
        {
          config = HBaseConfiguration.create();
          String backupTag = args[++i].toString();

          try{
             BackupRestoreClient brc = new BackupRestoreClient(config);
             brc.unlock(backupTag);
          }
          catch(Exception e){
             System.out.println("unlock " + backupTag + " failed " + e);
             System.exit(1);
          }
        }
        else if(args[i].equals("cleanuplock"))
        {
          config = HBaseConfiguration.create();
          String backupTag = args[++i].toString();

          try{
             BackupRestoreClient brc = new BackupRestoreClient(config);
             brc.cleanuplock(backupTag);
          }
          catch(Exception e){
             System.out.println("cleanuplock " + backupTag + " failed " + e);
             System.exit(1);
          }
        }
        else if(args[i].equals("delete"))
        {
          config = HBaseConfiguration.create();
          String backupTag = args[++i].toString();
          String cascade = args[++i].toString();
          String force = args[++i].toString();
          String skipLockUnlock = args[++i].toString();
          try{
             BackupRestoreClient brc = new BackupRestoreClient(config);
             brc.deleteBackup(backupTag, false, cascade.equals("y"), 
                              force.equals("y"), skipLockUnlock.equals("y"));
          }
          catch(Exception e){
             System.out.println("delete " + backupTag + " failed " + e);
             System.exit(1);
          }
        }
        else if(args[i].equals("operationUnlock"))
        {
          config = HBaseConfiguration.create();
          String backupTag = args[++i].toString();
          String recoverMeta = args[++i].toString();
          try{
             BackupRestoreClient brc = new BackupRestoreClient(config);
             brc.operationUnlock(backupTag, recoverMeta.equals("y"));
          }
          catch(Exception e){
             System.out.println("operationUnlock " + backupTag + " failed " + e);
             System.exit(1);
          }
        }
        else if(args[i].equals("translate"))
        {
          config = HBaseConfiguration.create();
          String hash = args[++i].toString();
          String tableName = "Not Found";
          try{
             SnapshotMeta sm = new SnapshotMeta(config);
             tableName = sm.translate(hash);
             System.out.println("\nhash " + hash + " translated to table " + tableName);
          }
          catch(Exception e){
             System.out.println("\ntranslate " + hash + " failed " + e);
             System.exit(1);
          }
        }
        System.exit(0);
      }
    }

    private Long getExportImportTimeoutSeconds(){
        String timeOutStr = System.getenv("BR_EXPORT_IMPORT_TIMEOUT");
        return null != timeOutStr ? Long.parseLong(timeOutStr) : 3600L;
    }
}
