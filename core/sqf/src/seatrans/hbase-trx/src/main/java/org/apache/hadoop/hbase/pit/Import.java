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

import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.transactional.RMInterface;
import org.apache.hadoop.hbase.client.transactional.TransactionalTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.BroadcastResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportLaunchResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportLaunchRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportStatusRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ImportStatusResponse;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.exception.ExceptionUtils;

import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.HConstants;
import com.google.protobuf.ServiceException;
import com.google.protobuf.ByteString;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Iterator;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.lang.ProcessBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Comparator;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.lang.Throwable;

import org.apache.commons.io.FileUtils;
import java.io.File;
import org.apache.hadoop.hbase.pit.RecoveryRecord;
import org.apache.hadoop.hbase.pit.MutationMeta;
import org.apache.hadoop.hbase.pit.MutationMetaRecord;
import org.apache.hadoop.hbase.pit.SnapshotMetaRecordCompare;
import org.apache.hadoop.hbase.pit.SnapshotMetaRecordReverseKeyCompare;
import org.apache.hadoop.hbase.pit.SnapshotMeta;
import org.apache.hadoop.hbase.pit.SnapshotMetaRecord;
import org.apache.hadoop.hbase.pit.SnapshotMetaStartRecord;
import org.apache.hadoop.hbase.pit.TableRecoveryGroup;

import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import java.lang.Process;
import java.io.FileReader;

public class Import implements Serializable
{
    private static final long serialVersionUID = 3361277481578700999L;
//    private static String SNAPSHOT_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.SNAPSHOT";
//    private static String MUTATION_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.MUTATION";
    
    public static final int BRC_SLEEP = 1000;      // One second
    public static final int BRC_SLEEP_INCR = 3000; // three seconds
    public static final int BRC_RETRY_ATTEMPTS = 5;
    
    static Configuration config;
    static Connection connection;
    static Configuration adminConf;
    static Connection adminConnection;
    static Object adminConnLock = new Object();

    String lastError;
    String lastWarning;
    
    private static final int ID_TM_SERVER_TIMEOUT = 1000;
    private static SnapshotMeta sm;
    private static MutationMeta mm;
//    private static SnapshotMetaRecord smr;
    private static SnapshotMetaStartRecord smsr;
//    private static SnapshotMetaIncrementalRecord smir;
    HBaseAdmin restoreAdmin;
    int pit_thread = 1;
    private ExecutorService threadPool;

    boolean m_verbose;
    boolean pitEnabled; 
    
    static final Log logger = LogFactory.getLog(Import.class);
    public Import()
    {
      m_verbose = false;
      pitEnabled = false;
    }

    public Import(Configuration conf) throws IOException
    {
      m_verbose = false;
      if (logger.isDebugEnabled())
          logger.debug("Import::Import(...) called.");
      config = conf;
      connection = ConnectionFactory.createConnection(config);

      String rpcTimeout = System.getenv("HAX_ADMIN_RPC_TIMEOUT");
      if (rpcTimeout != null){
           synchronized(adminConnLock) {
              if (adminConnection == null) {
                adminConf = new Configuration(config);
                int rpcTimeoutInt = Integer.parseInt(rpcTimeout.trim());
                String value = adminConf.getTrimmed("hbase.rpc.timeout");
                adminConf.set("hbase.rpc.timeout", Integer.toString(rpcTimeoutInt));
                String value2 = adminConf.getTrimmed("hbase.rpc.timeout");
                logger.info("HAX: ADMIN RPC Timeout, revise hbase.rpc.timeout from " + value + " to " + value2);
                adminConnection = ConnectionFactory.createConnection(adminConf);
              }
           } //sync
      }
      else {
           adminConnection = connection;
      }

    }

    
/**
   * ReplayEngineCallable  :  inner class for creating asynchronous requests
   */
  private abstract class ImportCallable implements Callable<Integer>  {

	  	ImportCallable(){}
        
        
        public Integer doImport(String backupTag,
                                boolean incrementalBackup,
                                String srcRoot,
                                TableSet tableset,
                                String importType
            ) throws Exception  {

          long threadId = Thread.currentThread().getId();
          try {
          
          logger.info("ENTER doImport Backup Tag: " + backupTag +
            " Thread ID " + threadId +
            " srcRoot: " + srcRoot +
            " TableSet:" + tableset.displayString()
            );

          final String hbaseRoot = config.get("hbase.rootdir");
          String snapshotPath = tableset.getSnapshotPath();
          String fullSrc = srcRoot + "/" + backupTag + "/" + snapshotPath;
          if(tableset.isIncremental()) {
            fullSrc = srcRoot + "/" + tableset.getTableName().replace(":", "_") + "/" + snapshotPath;
          }
          boolean skipImportSnapshot = false;
          //Check if table snapshot import can be skipped.
          if(incrementalBackup && tableset.isIncremental()) {
            //hbase check snapshot exist by this name.
            Admin admin = connection.getAdmin();
            List<SnapshotDescription> sdl = new ArrayList<SnapshotDescription>();
            sdl = admin.listSnapshots();
            if (! sdl.isEmpty()) {
              for (SnapshotDescription sd : sdl) {
                if (sd.getName().compareTo(snapshotPath) == 0) {
                  skipImportSnapshot = true;
                  break;
                }
              }
            }
            admin.close();
          }
          
          if(!skipImportSnapshot) {
            String hadoopType = System.getenv("HADOOP_TYPE");
            String hbaseExe = null;
            if (hadoopType != null) {
            	hbaseExe = "hbase";
            } else {
                hbaseExe = System.getenv("HBASE_HOME");
                if(hbaseExe != null) {
                	// then work station env.
                	hbaseExe = hbaseExe + "/" + "bin" + "/" + "hbase";
                } else {
                	//just use default.
                	hbaseExe = "/" + "usr" + "/" + "bin" + "/" + "hbase";
                }
            }
            
            if (logger.isDebugEnabled())
            	logger.debug("doImport hbaseExe used : " + hbaseExe);
            
            ProcessBuilder pb = null;
            String mappers = System.getenv("EXPORT_IMPORT_MAPPERS");
            
        	if(importType.equals("imp_sudohbase")) {
        		if (logger.isDebugEnabled())
        			logger.debug("doImport options" + " sudo" + " -u" + " hbase");
        		
        		pb = new ProcessBuilder("sudo", "-u", "hbase",
        							hbaseExe,
                                    "org.apache.hadoop.hbase.snapshot.ExportSnapshot",
                                    "-snapshot", snapshotPath ,
                                    "-copy-to", hbaseRoot,
                                    "-copy-from", fullSrc,
                                    "-mappers", (mappers != null)? mappers : "16");
        	
        	}else if(importType.equals("imp_hbase")) {
				if (logger.isDebugEnabled())
					logger.debug("doImport options" + " hbaseExe");
				
				pb = new ProcessBuilder(hbaseExe,
		                "org.apache.hadoop.hbase.snapshot.ExportSnapshot",
		                "-snapshot", snapshotPath ,
		                "-copy-to", hbaseRoot,
		                "-copy-from", fullSrc,
		                "-mappers", (mappers != null)? mappers : "16");
        		
        	}else if(importType.equals("imp_shell")) {
        		if (logger.isDebugEnabled())
        			logger.debug("doImport options" + "/bin/bash -c");
        		
        		String cmd = hbaseExe + " org.apache.hadoop.hbase.snapshot.ExportSnapshot" +
        					 " -snapshot " + snapshotPath +
                             " -copy-to " + hbaseRoot +
                             " -copy-from " + fullSrc +
                             " -mappers" + " 16";
        		pb = new ProcessBuilder("/bin/bash", "-c",
        				                cmd);
        		
        	}
        
          //redirect child process error output to stdout.  
          pb.redirectErrorStream(true);
          
          Process process = pb.start();
          
          //getInputStream reads stdout pipe. 
          BufferedReader reader = 
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        
          StringBuilder builder = new StringBuilder();
          String line = null;
          while ( (line = reader.readLine()) != null) {
            builder.append(line);
            builder.append(System.getProperty("line.separator"));
          }
        
        
          //Wait for child completion.
          int rc = process.waitFor();
          String rcStr = builder.toString();
          
          if(logger.isDebugEnabled())
            logger.debug("doImport Thread ID " + threadId + " rc:" + rc + " importLog: " + rcStr);
          
          if((rc != 0 ) || rcStr.contains("ERROR") || rcStr.contains("FATAL")) {
            logger.error("doImport thread " + threadId + " FAILED with error:" + rc + " Error Detail: " + rcStr);
            throw new Exception("doImport thread " + threadId + " FAILED with error:" + rc + " Error Detail: " + rcStr);
      	  }
            
          } //if(!skipImportSnapshot)
          
          //mutation import
          logger.info("doImport mutation Thread ID " + threadId + " Mutation size: " + tableset.getMutationSize());
          if(tableset.getMutationSize() > 0) {
            ArrayList<Path> sourcePathList = new ArrayList<Path>();
            ArrayList<String> mutationlist = tableset.getMutationList(); 
            for (int i = 0; i < mutationlist.size(); i++) {
            String mutation = mutationlist.get(i);
            String mutationPathString = srcRoot + "/" + tableset.getTableName().replace(":", "_") + "/" + mutation;
            Path mutationPath = new Path (mutationPathString);
            sourcePathList.add(mutationPath);
            }
            
            String hdfsRoot = config.get("fs.default.name");
            String PITRoot = hdfsRoot + "/user/trafodion/PIT/" + tableset.getTableName().replace(":", "_") + "/cdc/";

            Path destRootPath = new Path(PITRoot);
            
            logger.info("doImport mutation Thread ID " + threadId + " sourcePathList: " + sourcePathList + 
                " Destination : " + destRootPath);
            DistCpOptions distcpopts = new DistCpOptions(sourcePathList, destRootPath);
            DistCp distcp = new DistCp(config, distcpopts);
            distcp.execute();
          }
          
          } catch( Exception e) {
            logger.error("doImport thread " + threadId + " encountered exception :" , e);
            throw new IOException(e);
          } finally {
            if (logger.isDebugEnabled())
              logger.debug("EXIT doImport thread " + threadId + " TableName " + tableset.getTableName());
          }
          return 0;
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
         logger.error("Unable to locate file " + destRootPath + " ", fnfe);
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
    
    public boolean importBackup(final String backuptag, String source,
                                int numParallelThreads, boolean override,
                                final String importType) 
        throws Exception {
      
      logger.info("[IMPORTBACKUP] backup: " + backuptag +
                  " Source location: " + source +
                  " threads: " + String.valueOf(numParallelThreads) +
                  " importType: " + importType +
                  " override: " + override);
      
      //initial checks
      //if(isTagExistInMeta(backuptag) || isTagExistBackupSys(backuptag)) {
      if(isTagExistBackupSys(backuptag)) {
        throw new IOException("Backup " + backuptag + " already exists.");
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
        logger.info("importBackup parallel threads : " + pit_thread);
        
        tPool = Executors.newFixedThreadPool(pit_thread);
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tPool);
        
        int loopCount = 0;
        final boolean incrBkp = bkpset.isIncremental();
        final String srcLocation = source;
        //Iterate through tableSet.
        for (final TableSet t : tableSetList) {
          compPool.submit(new ImportCallable() {
            public Integer call() throws Exception {
            return doImport(backuptag,
                            incrBkp,
                            srcLocation,
                            t,
                            importType);
              }
            });
          compPool.take().get(); 
          loopCount++;
         } //for
        
        loopCount = 0;
        
        // simply to make sure they all complete, no return codes necessary at the moment
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
          int returnValue = compPool.take().get(); 
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
         logger.error("ImportBackup Exception occurred " ,e);
         throw new IOException(e);
       }
       finally{
         if(tPool != null)
           tPool.shutdownNow();
       }
        
       return true;
      } 
    
    

    static public void main(String[] args) {
      
      for ( int i = 0; i < args.length; i++ )
      {
        if(args[i].equals("import"))
        {
          String backupTag = args[++i].toString();
          config = HBaseConfiguration.create();
          
          try{
            Import brc = new Import(config);
            String srcLoc = args[++i].toString();
            String importType = args[++i].toString();
            int parallelThreads = Integer.valueOf(args[++i]);
            brc.importBackup(backupTag, srcLoc, parallelThreads, false, importType);
          }catch(Exception e) {
            System.out.println("Import failed " + e);
            System.exit(1);
          }
        }
        System.exit(0);
      }
    }
}
