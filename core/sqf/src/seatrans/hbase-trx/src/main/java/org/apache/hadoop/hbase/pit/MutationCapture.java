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

package org.apache.hadoop.hbase.pit;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.KeyValue;

import org.apache.hadoop.hbase.HRegionInfo;

import org.apache.hadoop.hbase.regionserver.transactional.TrxTransactionState;
import org.apache.hadoop.hbase.regionserver.transactional.TrxTransactionState.WriteAction;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;


import org.apache.hadoop.hbase.pit.MutationMeta;
import org.apache.hadoop.hbase.pit.MutationMetaRecord;
import org.apache.hadoop.hbase.pit.SnapshotMeta;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.pit.HBasePitZK;

public class MutationCapture {

  static final Log LOG = LogFactory.getLog(MutationCapture.class);
  private static final String COMMITTED_TXNS_KEY = "1_COMMITED_TXNS_KEY";
  private int PIT_max_txn_mutation_per_KV = 10; // many KV in file, each one has 10 txn, no impact on file size
  private int PIT_max_txn_mutation_per_FILE = 10000; // 100K transaction branch
  private long PIT_max_size_mutation_per_FILE = 128000000; // 128 MB size
  private boolean useReentrantLock = false;

  Configuration config;
  HRegionInfo regionInfo;
  FileSystem fs = null;
  HFileContext context;

  // PIT Mutation Capturer
  ByteArrayOutputStream mutationOutput;
  HFileWriterV2 mutationWriter = null;
  long mutationCount;
  long mutationTotalCount;
  long mutationTotalSize;
  long mutationSet;
  long totalPuts = 0;
  long totalDeletes = 0;
  String currentFileKey = "";
  long smallestCommitId = -1;
  long largestCommitId = -1;
  private Object mutationOpLock = new Object();
  Path currentPITPath;
  long currentSnapshotId = 0;
  long incrementalSnapshotId = 0;
  boolean skip_CDC = false;
  SnapshotMetaRecord currentSnapshot = null;

  private String hdfsRoot;
  private String PITRoot;
  private static Object mutationMetaLock = new Object();
  static IdTmId timeId = null;
  static IdTm idServer = null;
  private static int ID_TM_SERVER_TIMEOUT = 1000; // 1 sec

  static MutationMeta meta = null;
  static SnapshotMeta snapMeta = null;

 ReentrantLock reentrantOpLock = new ReentrantLock();
 static ReentrantLock reentrantMetaLock = new ReentrantLock();

   private HBasePitZK pitZK;
  
  public static final int PIT_MUTATION_CREATE = 1;
  public static final int PIT_MUTATION_APPEND = 2;
  public static final int PIT_MUTATION_FLUSH = 3;
  public static final int PIT_MUTATION_CLOSE = 4;
  public static final int PIT_MUTATION_ROLLOVER = 5;
  public static final int PIT_MUTATION_CLOSE_STOP = 6;

  public static final int PIT_MUTATION_WRITER_CREATE = 1;
  public static final int PIT_MUTATION_WRITER_FLUSH = 2;
  public static final int PIT_MUTATION_WRITER_CLOSE = 3;
  public static final int PIT_MUTATION_WRITER_APPEND = 4;
  public static final int PIT_MUTATION_WRITER_CLOSE_STOP = 5;

  public static final int SYNCHRONIZED    = 4;   // 00100
  
  
  public MutationCapture (Configuration conf, FileSystem f, HFileContext cont, HRegionInfo rInfo,
                                               int txnPerKV, int txnPerFile, long sizePerFile, boolean configuredPITRecoveryHA)  throws IOException{
     
	  PIT_max_txn_mutation_per_KV = txnPerKV;
	  PIT_max_txn_mutation_per_FILE = txnPerFile;
	  PIT_max_size_mutation_per_FILE = sizePerFile;
      this.config = conf;
      this.pitZK = new HBasePitZK(this.config);
      this.regionInfo = rInfo;
      this.fs = f;
      this.context = cont;
      this.skip_CDC = false;
      // this is to use ReentrantLock to timeout incoming requests to avoid deadlock in RS handlers
      this.useReentrantLock = configuredPITRecoveryHA;
      hdfsRoot = conf.get("fs.default.name");
      PITRoot = hdfsRoot + "/user/trafodion/PIT/" + regionInfo.getTable().toString().replace(":", "_") + "/cdc/";
      if (LOG.isTraceEnabled()) LOG.trace("PIT MutationCapture root dir " + PITRoot); 
      if (LOG.isInfoEnabled()) LOG.info("PIT MutationCapture rollover attributes for region " + regionInfo.getEncodedName() + " are " + 
		   " Max Txn per KV " + this.PIT_max_txn_mutation_per_KV +
		   " Max Txn per FILE " + this.PIT_max_txn_mutation_per_FILE +
     		   " Max Size per FILE " + this.PIT_max_size_mutation_per_FILE + 
                   " Use ReentrantLock " + this.useReentrantLock );
      String idtmTimeout = System.getenv("TM_IDTM_TIMEOUT");
      if (idtmTimeout != null){
         ID_TM_SERVER_TIMEOUT = Integer.parseInt(idtmTimeout.trim());
      }

      if (LOG.isTraceEnabled()) LOG.trace("MutationCapture constructor() completes");
      return;
  
  
  } 

  public void setSkipCDC(boolean flag) {

       this.skip_CDC = flag;
   }
      
  // Transactional Mutation Capturer
   
  public void txnMutationBuilder(TrxTransactionState ts, int mutationOpTimer, int mutationMetaTimer) throws IOException {
      
      // build a tsBuild based input argument: Trx Transaction State
      int iPut = 0;
      int iDelete = 0;
      
      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationTSBuilder -- Transaction Id " + ts.getTransactionId() + 
                                                          " commitId " + ts.getCommitId() + " skip CDC " + skip_CDC);

      if (skip_CDC) return;

      try {
	  
      TransactionMutationMsg.Builder tmBuilder =  TransactionMutationMsg.newBuilder();
      tmBuilder.setTxId(ts.getTransactionId());
      tmBuilder.setTableName(regionInfo.getTable().getNameAsString());
      tmBuilder.setStartId(ts.getStartId());
      tmBuilder.setCommitId(ts.getCommitId());
      tmBuilder.setTableCDCAttr(ts.getMutationClient());
      
      // since mutation write is non-force, Region Oberserver may need to get commit-id from ts supplied by TLOG
      // interface to generate mutation for in-doubt txn. Also commit HLOG record should also include commit-id
      // to build correct mutation (may need a different type of mutation KVs)

      for (WriteAction wa : ts.getWriteOrdering()) {
         if (wa.getPut() != null) {
            tmBuilder.addPutOrDel(true);
            tmBuilder.addPut(ProtobufUtil.toMutation(MutationType.PUT, new Put(wa.getPut())));
            iPut++;
         }
         else {
            if (! wa.getIgnoreDelete()){ // Don't apply delete if we subsequently have a put for the same key
               tmBuilder.addPutOrDel(false);
               tmBuilder.addDelete(ProtobufUtil.toMutation(MutationType.DELETE, new Delete(wa.getDelete())));
               iDelete++;
            }
         }
      } // for WriteAction loop
      
      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationTSBuilder -- Transaction Id " + ts.getTransactionId() + 
	                       " has " + iPut + " put " + iDelete + " delete ");
      
      // now just append the mutation into the output buffer
      mutationBufferOp(PIT_MUTATION_APPEND, null, tmBuilder, iPut, iDelete,
                               mutationOpTimer, mutationMetaTimer, ts.getMutationClient());
      
      } catch(IOException e) {
          LOG.error("PIT CDC Exception: mutation builder -- Transaction Id " + ts.getTransactionId() + " stack ", e);
          throw new IOException(e);
      }

  }

  public void mutationBufferOp(int op, Path writePath, TransactionMutationMsg.Builder tmBuilder, int iput, int idelete,
                                                                                  int mutationOpTimer, int mutationMetaTimer, int mClient)  throws IOException {

     mutationBufferOp(op, writePath, tmBuilder, false, iput, idelete, mutationOpTimer, mutationMetaTimer, mClient) ;

  }

  public void mutationBufferOp(int op, Path writePath, TransactionMutationMsg.Builder tmBuilder, boolean ha, int iput, int idelete,
                                                                                 int mutationOpTimer, int mutationMetaTimer, int mClient)  throws IOException {
      IOException ioe = null;
      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationBufferOp: operation " + op + " skip CDC " + skip_CDC + 
                                              " op lock timer " + mutationOpTimer + " meta lock timer " + mutationMetaTimer +
                                              " mClient " + mClient);

      if (skip_CDC) return;

      // use reentrantLock to regulate time-out based incoming requests to break possible deadlock with limit RS handlers and meta query/update
      if (useReentrantLock) {
          if (mutationOpTimer > 0) {
                 try {
                      boolean flag = reentrantOpLock.tryLock(mutationOpTimer, TimeUnit.MILLISECONDS);
                      if (flag) {
                          try {
                               performMutationBufferOp(op, writePath, tmBuilder, ha, iput, idelete, mutationMetaTimer, mClient);
                          } catch (IOException e1) {
                               LOG.trace("PIT CDC Exception: mutationBufferOp: reentrantOpLock perform Buffer Op exception " + e1);
                               ioe = e1;
                          } finally {
                              reentrantOpLock.unlock();
                              LOG.trace("PIT CDC Exception: mutationBufferOp: reentrantOpLock unlock");
                              if (ioe != null) throw new IOException(ioe);
                          }
                          LOG.trace("PIT CDC Exception: PIT mutationBufferOp: reentrantOpLock complete");
                      }
                     else {
                              LOG.trace("PIT CDC Exception: mutationBufferOp: reentrantOpLock time out");
                              throw new IOException("Trafodion CDC operation exception");
                     }
                 }
                 catch (InterruptedException e3) {
                              LOG.trace("PIT CDC Exception: mutationBufferOp: reentrantOpLock interrupted" + e3);
                              throw new IOException(e3);
                 }
                 catch (IOException e2) {
                     throw new IOException(e2);
                 }
          } // timer
          else {
                 reentrantOpLock.lock();
                 try {
                     performMutationBufferOp(op, writePath, tmBuilder, ha, iput, idelete, mutationMetaTimer, mClient);
                 }
                 catch (IOException e) {
                      LOG.error("PIT CDC Exception: mutationBufferAction: Exception " + op + " stack ", e);
                      ioe = e;
                  }
                 finally {
                       if(LOG.isTraceEnabled()) LOG.trace("PIT CDC Exception: mutationBufferOp: reentrantOpLock release");
                       reentrantOpLock.unlock();
                       if (ioe != null) throw new IOException(ioe);
                 }
           }
      } // if block
      else { // use old sync mechanism
          synchronized (mutationOpLock) {
              try {
                     performMutationBufferOp(op, writePath, tmBuilder, ha, iput, idelete, mutationMetaTimer, mClient);
              } catch(IOException e) {
                  //LOG.error("PIT CDC Exception: mutationBufferAction: Exception " + op + " stack ", e);
                  throw new IOException(e);
              } 
          } // synchronized
      } // else block

  }

  public void performMutationBufferOp(int op, Path writePath, TransactionMutationMsg.Builder tmBuilder, boolean ha, int iput, int idelete,
                                                                                 int mutationMetaTimer, int mClient)  throws IOException {
      
          try {

          if(LOG.isTraceEnabled()) LOG.trace("PIT performMutationBufferOp: operation " + op + " skip CDC " + skip_CDC + 
                                                " meta lock timer " + mutationMetaTimer + " mClient " + mClient);
      
          switch (op) {
              case PIT_MUTATION_CREATE: { // create the mutation file based on passed writePath
                  mutationWriterAction(PIT_MUTATION_WRITER_CREATE /* reset output stream */, writePath, null, ha, mutationMetaTimer, mClient);
                  break;
              }
              case PIT_MUTATION_APPEND: { // append mutation into local buffer
                  if (currentFileKey.equals("")) { // need to new mutation file
                      mutationWriterAction(PIT_MUTATION_WRITER_CREATE /* reset output stream */, null, null, ha, mutationMetaTimer, mClient); // create mutation file based on snapshot meta
                      if (skip_CDC) break;
                  } 
                  mutationWriterAction(PIT_MUTATION_WRITER_APPEND /* append */, null, tmBuilder, ha, iput, idelete, mutationMetaTimer, mClient);
                  
                  // PIT test, let's say do write-to-KV every 5 txn
                  if (mutationCount >= this.PIT_max_txn_mutation_per_KV) {
                     mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha, mutationMetaTimer, mClient );  
                  }
                  
                  // We can close the mutation file when
                  //    a) total mutation size is too large
                  //    b) system event such as pre-flush by HBase (memory store is going to flush)
                  //    c) per request (e.g. during full snapshot)
                  //    d) number of txn branches 
                  //    e) by timer through lease (optional)
                  // PIT test, let's say do close&rollover every 20 txn
                  if ((!ha) && ((mutationTotalCount >= this.PIT_max_txn_mutation_per_FILE) || 
                       (mutationTotalSize >= this.PIT_max_size_mutation_per_FILE))) { 
                     if (pitZK.isSnapshotMetaLocked()) {
                        if(LOG.isDebugEnabled()) LOG.error("PIT mutation file rollover deferred due to concurrent PIT Meta operation " + op);
                     }
                     else {
                        mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha, mutationMetaTimer, mClient);
                        mutationWriterAction(PIT_MUTATION_WRITER_CLOSE /* close mutation file */, null, null, ha, mutationMetaTimer, mClient);
                        //mutationWriterAction(PIT_MUTATION_WRITER_CREATE /* new mutation file and writer */, null, null, ha);
                        currentFileKey = ""; // reset currentFileKey to allow later file creation for new append request
                        mutationWriter = null;
                     }
                  }
                  break;
              }
              case PIT_MUTATION_FLUSH: { // append local mutation buffer to writer      
              mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* flush current mutation buffer */, null, null, ha, mutationMetaTimer, mClient);
                  break;
              }
              case PIT_MUTATION_CLOSE: { // append local buffer to writer and close current mutation file
                  mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha, mutationMetaTimer, mClient );  
                  mutationWriterAction(PIT_MUTATION_WRITER_CLOSE /* close mutation file */, null, null, ha, mutationMetaTimer, mClient );
                  currentFileKey = ""; // reset currentFileKey to allow later file creation for new append request
                  mutationWriter = null;
                  break;
              }
              case PIT_MUTATION_ROLLOVER: { // op 4 + new next mutation file immediately
                  mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha, mutationMetaTimer, mClient );  
                  mutationWriterAction(PIT_MUTATION_WRITER_CLOSE /* close mutation file */, null, null, ha, mutationMetaTimer, mClient);  
                  mutationWriterAction(PIT_MUTATION_WRITER_CREATE /* new mutation file and writer */, null, null, ha, mutationMetaTimer, mClient );
                  break;
              }
              case PIT_MUTATION_CLOSE_STOP: { // append local buffer to writer and close current mutation file
                  mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha, mutationMetaTimer, mClient );  
                  mutationWriterAction(PIT_MUTATION_WRITER_CLOSE_STOP /* close mutation file */, null, null, ha, mutationMetaTimer, mClient );
                  currentFileKey = ""; // reset currentFileKey to allow later file creation for new append request
                  mutationWriter = null;
                  break;
              }
              default: {
                  if(LOG.isTraceEnabled()) LOG.trace("PIT perfromMutationBufferOp: invalid operation " + op);
              }
          } // switch
          } catch(IOException e) {
              LOG.error("PIT CDC Exception: performMutationBufferOpAction " + op + " stack ", e);
              throw new IOException(e);
          } 
  }


 public void mutationWriterAction(int action, Path writePath, TransactionMutationMsg.Builder tmBuilder, int metaTimer, int mClient) throws IOException {

     mutationWriterAction(action, writePath, tmBuilder, false, 0, 0, metaTimer, mClient);

  }

 public void mutationWriterAction(int action, Path writePath, TransactionMutationMsg.Builder tmBuilder, boolean ha, int metaTimer, int mClient) throws IOException {

     mutationWriterAction(action, writePath, tmBuilder, ha, 0, 0, metaTimer, mClient);

  }
  

  public void syncMutationWriterAction(int action, Path writePath, TransactionMutationMsg.Builder tmBuilder, boolean ha,
                                                              int iput, int idelete, int metaTimer, int mClient) throws IOException {

     IOException ioe = null;
     if(LOG.isTraceEnabled()) LOG.trace("PIT syncMutationWriterAction: action " + action + " meta lock timer " + metaTimer);

      // use reentrantLock to regulate time-out based incoming requests to break possible deadlock with limit RS handlers and meta query/update
      if (useReentrantLock) {
          if (metaTimer > 0) {
                 try {
                      boolean flag = reentrantMetaLock.tryLock(metaTimer, TimeUnit.MILLISECONDS);
                      if (flag) {
                          try {
                               performSyncMutationWriterAction(action, writePath, tmBuilder, ha, iput, idelete, metaTimer, mClient);
                          } catch (IOException e1) {
                               LOG.trace("PIT sync mutationWriterAction: reentrantMetaLock perform Writer action exception " + e1);
                               ioe = e1;
                          } finally {
                              reentrantMetaLock.unlock();
                              LOG.trace("PIT sync mutationWriterAction: reentrantMetaLock unlock");
                              if (ioe != null) throw new IOException(ioe);
                          }
                          LOG.trace("PIT sync mutationWriterAction: reentrantMetaLockk complete");
                      }
                     else {
                              LOG.trace("PIT sync mutationWriterAction: reentrantMetaLock time out");
                              throw new IOException("Trafodion CDC operation exception");
                     }
                 }
                 catch (InterruptedException e3) {
                              LOG.trace("PIT sync mutationWriterAction: reentrantMetaLock interrupted" + e3);
                              throw new IOException(e3);
                 }
                 catch (IOException e2) {
                     throw new IOException(e2);
                 }
          } // timer
          else {
                 reentrantMetaLock.lock();
                 try {
                     performSyncMutationWriterAction(action, writePath, tmBuilder, ha, iput, idelete, metaTimer, mClient);
                 }
                 catch (IOException e) {
                      LOG.error("PIT CDC Exception: sync mutationWriterAction: Exception " + action + " stack ", e);
                      ioe = e;
                  }
                 finally {
                       if(LOG.isTraceEnabled()) LOG.trace("PIT CDC Exception: sync mutationWriterAction: reentrantMetaLock release");
                       reentrantMetaLock.unlock();
                       if (ioe != null) throw new IOException(ioe);
                 }
           }
      } // if block
      else { // use old sync mechanism
          synchronized (mutationMetaLock) {
              try {
                     performSyncMutationWriterAction(action, writePath, tmBuilder, ha, iput, idelete, metaTimer, mClient);
              } catch(IOException e) {
                  //LOG.error("PIT CDC Exception: sync mutationWriterAction: Exception " + action + " stack ", e);
                  throw new IOException(e);
              } 
          } // synchronized
      } // else block

  }


  public void mutationWriterAction(int action, Path writePath, TransactionMutationMsg.Builder tmBuilder, boolean ha,
                                                              int iput, int idelete, int metaTimer, int mClient) throws IOException {
   
      byte [] bSet;
      byte [] bKey;
      MutationMetaRecord mmr;
      boolean test = false;
      
      
      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: action " + action);
      
      try {
      
      switch (action) {
	  case PIT_MUTATION_WRITER_CREATE: { // create next transactional mutation file and associated HBaseWriter
               syncMutationWriterAction(action, writePath, tmBuilder, ha,  iput, idelete, metaTimer, mClient);
               break;
          }
	  case PIT_MUTATION_WRITER_FLUSH: { // append and write output buffer out
	      // for the key by a sequence incremental number (mutationSet), the number is reset when rollover
	      if (mutationCount > 0) {
		  mutationSet++;
	          bSet = Bytes.toBytes (mutationSet);
	          bKey = concat(Bytes.toBytes(COMMITTED_TXNS_KEY), bSet);
                  byte [] firstByte = mutationOutput.toByteArray();
                  mutationWriter.append(new KeyValue(bKey, Bytes.toBytes("cf"), Bytes.toBytes("qual"), firstByte));
	          if(LOG.isInfoEnabled()) LOG.info("PIT mutationWriterAction: action flush " + action + " " +
		         regionInfo.getTable().toString() + "-e-" + regionInfo.getEncodedName() +
                         " mutationCount " + mutationCount + " mutationSet " + mutationSet + 
		         " all mutations in KV size " + firstByte.length + " mutationTotalCount " + mutationTotalCount);
	          mutationTotalSize = mutationTotalSize + firstByte.length;
	          mutationOutput = new ByteArrayOutputStream(); // reset the buffer for next KV (batch of TSBuilder cells)
	          mutationCount = 0;
	      }
	      else {
		  if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: append, no mutation no flush & no close "
		      + regionInfo.getTable().toString() + "-e-" + regionInfo.getEncodedName() +
                      " FileKey " + currentFileKey + " Path " + writePath);
	      }
	      break;
          }
	  case PIT_MUTATION_WRITER_CLOSE: { 
               syncMutationWriterAction(action, writePath, tmBuilder, ha,  iput, idelete, metaTimer, mClient);
               break;
	  }
	  case PIT_MUTATION_WRITER_CLOSE_STOP: { // close  transactional mutation file, driven by chore thread (timer), preClose, preFlush, snapshot req      
		      
//	      if (mutationWriter != null) {
              if ((mutationWriter != null) && (!currentFileKey.equals(""))) {
                  try {
                  mutationWriter.close(); 
                  }
                 catch(IOException e) {
                       LOG.error("PIT CDC Exception: mutation file close " + regionInfo.getTable().toString());
                       test = true;
                       throw e;
                  }
                  mutationWriter = null;
      
	          if(LOG.isInfoEnabled()) LOG.info("PIT mutationWriterAction: close with stop " + action + " " +
		                        regionInfo.getTable().toString() + "-e-" + regionInfo.getEncodedName() + 
                                        " FileKey " + currentFileKey +
		                        " PIT mutation file path " + currentPITPath +
		                        " with smallest commitId " + smallestCommitId +
		                        " with largest commitId " + largestCommitId +
		                        " number of total Txn Protos " + mutationTotalCount + 
		                        " total txn mutation size " + mutationTotalSize + 
                                        " total puts " + totalPuts + " total deletes " + totalDeletes);
	      } // writer is not null
	         
	      currentFileKey = "";
                  mutationWriter = null;
              currentPITPath = null;
              mutationCount = 0;
              mutationTotalCount = 0;
              mutationTotalSize = 0;
              totalPuts = totalDeletes = 0;
              smallestCommitId = -1;
              largestCommitId = -1;
              mutationSet = 0;
              break;
	  }	  
	  case PIT_MUTATION_WRITER_APPEND: { // append the mutation into output buffer with delimeter
              tmBuilder.build().writeDelimitedTo(mutationOutput);
	      if ((smallestCommitId == -1) || (smallestCommitId > tmBuilder.getCommitId()))
		             smallestCommitId = tmBuilder.getCommitId();
	      if ((largestCommitId == -1) || (largestCommitId < tmBuilder.getCommitId()))
		             largestCommitId = tmBuilder.getCommitId();
	      mutationCount++;
	      mutationTotalCount++;
              totalPuts = totalPuts + iput;
              totalDeletes = totalDeletes + idelete;
              if ((mutationCount % 50) == 0) { // print every 50 times
	           if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: buffered " + action + " " +
		         regionInfo.getTable().toString() + "-e-" + regionInfo.getEncodedName() + 
                         " FileKey " + currentFileKey +
                         " PIT mutation file path " + currentPITPath +
		         " mutationCount " + mutationCount + " mutationSet " + mutationSet +
		         " smallest CommitId " + smallestCommitId + 
		         " largest CommitId " + largestCommitId + 
                         " total puts " + totalPuts + " total deletes " + totalDeletes);
              }
              break;
	  }
	  default: { // bad parameter
	      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: invalid action " + action);
	  }
      } // switch     
      } catch(Exception e) {
          LOG.error("PIT CDC Exception: mutationWriterAction " + action + " stack ", e);
          if (test) LOG.error("PIT CDC Exception: path: mc file close 5 ");
          else { 
                    LOG.error("PIT CDC Exception: path: others 5 "); 
                    throw new IOException(e);
           }
      }
  }

  public void performSyncMutationWriterAction(int action, Path writePath, TransactionMutationMsg.Builder tmBuilder, boolean ha,
                                                              int iput, int idelete, int metaTimer, int mClient) throws IOException {
   
      byte [] bSet;
      byte [] bKey;
      MutationMetaRecord mmr;
      boolean test = false;
      
      if(LOG.isInfoEnabled()) LOG.info("PIT MC, perform sync mutationWriterAction: action " + action + 
                             " region " + regionInfo.getRegionNameAsString() );
      
      try {
      
      switch (action) {
          case PIT_MUTATION_WRITER_CREATE: { // create next transactional mutation file and associated HBaseWriter

              if (!ha) { // not called by Observer HA recovery
                  if (snapMeta == null) {
               if(LOG.isTraceEnabled()) LOG.trace("to create snapshot meta ...");
                       try {
                  snapMeta = new SnapshotMeta(this.config);
               } catch (Exception e){
                  LOG.error("PIT CDC Exception: creating new snapshot meta: ", e);
                  throw new IOException(e);
               }
                       if(LOG.isTraceEnabled()) LOG.trace("snapshot meta created successfully");
                      }

               if (meta == null) {
                 if(LOG.isTraceEnabled()) LOG.trace("to create mutation meta ...");
                         try {
                    meta = new MutationMeta(this.config);
                 } catch (Exception e){
                    LOG.error("PIT CDC Exception: creating new mutation meta: ", e);
                    throw new IOException(e);
                 }
                         if(LOG.isTraceEnabled()) LOG.trace("mutation meta created successfully");
                   }
              
                   try {
                         currentSnapshot = snapMeta.getCurrentSnapshotRecord(regionInfo.getTable().toString());
                         if ((currentSnapshot == null) || currentSnapshot.isSkipTag()) {
                             if ((mClient & SYNCHRONIZED) == SYNCHRONIZED)  { // if SYNC table then assign 999 as default snapshot id
                                currentSnapshotId = 999; // Note. the tmTableCDCAttr = SYNCHRONIZED only when table is SYNC && XDC_DOWN
                             }
                             else {
                                 skip_CDC = true;
                                 currentSnapshotId = -1;
                             }
                         }
                         else {
                            currentSnapshotId = currentSnapshot.getKey();
                         }
                         if(LOG.isTraceEnabled()) LOG.trace("Traf CDC get current snapshot id " + currentSnapshotId + " table " + regionInfo.getTable().toString());
                       } catch (Exception e){
                          LOG.error("PIT CDC Exception: get current snapshot id failed " + regionInfo.getTable().toString() + " Exception: ", e);
                          currentSnapshotId = 999; //special snapshotId
                   }

                   if(LOG.isInfoEnabled()) LOG.info("PIT MC, get current snapshot id " + currentSnapshotId +
                                             " table " + regionInfo.getTable().toString());


                  if (skip_CDC) {
                         if(LOG.isInfoEnabled()) LOG.info("PIT no initial backup on " + regionInfo.getTable().toString() + " skip CDC and cached skip flag ");
                         return;
                   }

                   try {
                      SnapshotMetaIncrementalRecord incRec = snapMeta.getPriorIncrementalSnapshotRecord(regionInfo.getTable().toString(),
                                                                                              /* includeExcluded */ true);
                      if (incRec != null){
                     incrementalSnapshotId = incRec.getKey();
                     if(LOG.isTraceEnabled()) LOG.trace("PIT get incremental snapshot id "
                          + incrementalSnapshotId + " " + regionInfo.getTable().toString());
                      }
                      else {
                     incrementalSnapshotId = -1;
                     if(LOG.isTraceEnabled()) LOG.trace("PIT setting incrementalSnapshotId to -1 because "
                          + "no prior incremental snapshot found for " + regionInfo.getTable().toString());
                      }
               } catch (Exception e){
                  LOG.error("PIT CDC Exception: get incremental snapshot id failed " + regionInfo.getTable().toString() + " Exception: ", e);
                  incrementalSnapshotId = -1; // None
               }
             } // called by HA
             else {
                currentSnapshotId = 999; //special snapshotId
             }

         // snapshot name cannot contain ":". If the src name is prefixed by "namespace:",
         // replace ":" with "_"
         String pathName = regionInfo.getTable().toString().replace(":", "_");

         if(LOG.isInfoEnabled()) LOG.info("PIT MC, set MC file with path  " + pathName +
                           " for region " + regionInfo.getTable().toString() + " now, get the unique key for mutation meta record " );

         boolean done = false;
         while (! done) {
            long oldKey = System.nanoTime();
            currentFileKey = regionInfo.getTable().toString() +"_"+oldKey;
            if(LOG.isTraceEnabled()) LOG.trace("PIT intend to create mutation meta file ... ");
            currentPITPath = writePath = new Path(PITRoot +
                  pathName + "-snapshot-" + Long.toString(currentSnapshotId) +
                  "-e-" + regionInfo.getEncodedName() +
                  "-" + oldKey);
            if(LOG.isTraceEnabled()) LOG.trace("Creating mutation meta file using writePath "
                      + writePath);

            if (!ha) {
               try {
                  mmr = new MutationMetaRecord(currentFileKey, regionInfo.getTable().toString(),
                              currentSnapshotId, incrementalSnapshotId,
                              -1 /* backupSnapshot */,
                              -1 /* supersedingullSnapshot */,
                                  0 /* smallestCommitId */,
                                  0 /* largestCommitId */,
                                  512 /* fileSize */,
                                  "PENDING" /* userTag */,
                              regionInfo.getEncodedName(), // region encoded Name as String
                              writePath.toString(), // mutation file path as String
                              true /* inLocalFS */,
                              false /* archived */,
                              "test" /* archive location */);
                  if(LOG.isTraceEnabled()) LOG.trace("mutation meta record created successfully");
                  if (meta ==  null){
                     LOG.error ("mutation meta is null");
                  }
                  String retVal = meta.putMutationRecord(mmr, /* blindWrite */ false);
                  if (!retVal.equals(currentFileKey)) {
                     if(LOG.isInfoEnabled()) LOG.info("PITMC, Mutation putMutationRecord returned " + retVal
                               + "  Recreating MutationMetaRecord and retrying");
                  }
                  else {
                     done = true;
                     if(LOG.isInfoEnabled()) LOG.info("PIT MC, Mutation Record Put Create : key " + mmr.getRowKey() +
                                  " snapshot id " + currentSnapshotId +
                                  " incremental snapshot id " + incrementalSnapshotId +
                                  " table " + regionInfo.getTable().toString() +
                                  " region encoded name " + regionInfo.getRegionNameAsString() +
                                  " path " + writePath.toString() +
                          " inLocalFS: " + true +
                          " archived: " + false + " archive location: test" );
                  }

                } catch (Exception exc) {
                  LOG.error("PIT CDC Exception: put mutation record exception during mutation file creation ", exc);
                  throw new IOException(exc);
                       //throw new IOException("put mutation record exception ");
                }
              }
              else {
                      if(LOG.isTraceEnabled()) LOG.trace("PIT skip put mutation record during HA mutation generation");
              }
            } // while (! done)      

         if(LOG.isInfoEnabled()) LOG.info("PIT MC, finalize MC with path  " + writePath +
                           " for region " + regionInfo.getTable().toString() + " now, do the HFile create " );
                                           
              // 2.1) create writer (~ action 1) 
              if(LOG.isTraceEnabled()) LOG.trace("PIT creating mutationWriter with path " + writePath);
              mutationWriter = (HFileWriterV2) HFile.getWriterFactory(config,
                        new CacheConfig(config)).withPath(fs, writePath).withFileContext(context).create();
          mutationOutput = new ByteArrayOutputStream(); // reset the buffering
              mutationCount = 0;
              mutationTotalCount = 0;
                  mutationTotalSize = 0;
                  totalPuts = totalDeletes = 0;
              smallestCommitId = -1;
              largestCommitId = -1;
              mutationSet = 0;
          
              if(LOG.isInfoEnabled()) LOG.info("PIT MC, mutationWriterAction: create " + action +
                       " Table " + regionInfo.getTable().toString() + "-e-" + regionInfo.getEncodedName() +
                       " FileKey " + currentFileKey + " Path " + writePath);
              break;
          }
          case PIT_MUTATION_WRITER_CLOSE: { // close  transactional mutation file, driven by chore thread (timer), preClose, preFlush, snapshot req      
              // close transactional mutation file, driven by chore thread (timer), preClose, preFlush, snapshot req
              // close the writer to force content to be written to disk and make mutation immutable
              // set currentFileKey = -1 (so next mutation append will have to create the next file and writer)
              
              //        writePath = new Path("/hbase/PIT/mutation/" +
              //        //regionInfo.getTable().toString() + /* "-" + regionInfo.getEncodedName() + */
              //        //"-" + Long.toString(timeKey));
              //        "mutation-reader"); // for reader to test a special named mutation file
                      
//              if (mutationWriter != null) {
              if ((mutationWriter != null) && (!currentFileKey.equals(""))) {
                  try {
                  mutationWriter.close(); 
                  }
                 catch(IOException e) {
                       LOG.error("PIT MC, CDC Exception: mutation file close  " + regionInfo.getTable().toString());
                       test = true;
                       throw e;
                  }
                  mutationWriter = null; 
                  if(LOG.isInfoEnabled()) LOG.info("PIT MC, writer closed for region " + regionInfo.getRegionNameAsString() );

                  if (currentSnapshotId == 999) {
                      try {
                         currentSnapshot = snapMeta.getCurrentSnapshotRecord(regionInfo.getTable().toString());
                         if ((currentSnapshot == null) || currentSnapshot.isSkipTag()) {
                             LOG.info("PIT CDC: current snapshot record not found during PIT Mutation Writer Close; setting sid == 999 "
                                     + regionInfo.getTable().toString());
                            currentSnapshotId = 999;
                         }
                         else {
                           currentSnapshotId = currentSnapshot.getKey();
                           if(LOG.isTraceEnabled()) LOG.trace("PIT get current snapshot id " + currentSnapshotId +
                                                               " Table " + regionInfo.getTable().toString());  
                         }
                       } catch (Exception e){
                               LOG.error("PIT CDC Exception: get current snapshot id failed during PIT Mutation Writer Close when sid == 999 "
                                                     + regionInfo.getTable().toString() + " Exception: ", e);
                               currentSnapshotId = 999;
                       }
                   }

                  // update meta after writer close
                  if(LOG.isInfoEnabled()) LOG.info("PIT MC, writer closed for region " + regionInfo.getRegionNameAsString() +
                                                        " update meta " );
         
                  try {
                       mmr = new MutationMetaRecord(currentFileKey, regionInfo.getTable().toString(), 
                                           currentSnapshotId, incrementalSnapshotId, -1 /* backupSnapshot */,
                                           -1 /* supersedingullSnapshot */,
                       smallestCommitId, largestCommitId, mutationTotalSize, // for commitId rang & fileSize
                           "PENDING", // userTag updated by BackupRestoreClient during backup operation
                                           regionInfo.getEncodedName(), // region encoded Name as String
                                           currentPITPath.toString(), // mutation file path as String
                                           true /* inLocalFS */,
                       false /* archived */, "test" /* archive location */);
                       if(LOG.isTraceEnabled()) LOG.trace("mutation meta record created successfully");
                       meta.putMutationRecord(mmr, /* blindWrite */ true);
                       if(LOG.isTraceEnabled()) LOG.trace("PITMutation Record Put Close : key " + currentFileKey +
                               " snapshot id " + currentSnapshotId +
                               " incremental snapshot id " + incrementalSnapshotId +
                               " table " + regionInfo.getTable().toString() + 
                               " region encoded name " + regionInfo.getRegionNameAsString() +
                               " path " + currentPITPath +
                       " inLocalFS: " + true +
                       " archived: " + false + " archive location: test" );
                   
                   } catch (Exception exc) {
                               LOG.error("PIT CDC Exception: put mutation record exception during mutation file close ", exc);
                               throw new IOException(exc);
                       //throw new IOException("put mutation record exception ");
                    }

                    if(LOG.isInfoEnabled()) LOG.info("PIT MC, mutationWriterAction: close with " + action + " " +
                                        regionInfo.getTable().toString() + "-e-" + regionInfo.getEncodedName() + 
                                        " FileKey " + currentFileKey +
                                        " PIT mutation file path " + currentPITPath +
                                        " with smallest commitId " + smallestCommitId +
                                        " with largest commitId " + largestCommitId +
                                        " number of total Txn Protos " + mutationTotalCount + 
                                        " total txn mutation size " + mutationTotalSize + 
                                        " total puts " + totalPuts + " total deletes " + totalDeletes);
              } // writer is not null
                 
              currentFileKey = "";
              mutationWriter = null;
              currentPITPath = null;
              mutationCount = 0;
              mutationTotalCount = 0;
              mutationTotalSize = 0;
              totalPuts = totalDeletes = 0;
              smallestCommitId = -1;
              largestCommitId = -1;
              mutationSet = 0;
              break;
          }
          default: { // bad parameter
              if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: invalid action " + action);
          }
      } // switch     
      } catch(Exception e) {
          LOG.error("PIT CDC Exception: mutationWriterAction: Exception " + action + " stack ", e);
          if(LOG.isInfoEnabled()) LOG.info("PIT CDC Exception: mutationWriterAction: close with " + action + " " +
                                        regionInfo.getTable().toString() + "-e-" + regionInfo.getEncodedName() + 
                                        " FileKey " + currentFileKey +
                                        " PIT mutation file path " + currentPITPath +
                                        " with smallest commitId " + smallestCommitId +
                                        " with largest commitId " + largestCommitId +
                                        " number of total Txn Protos " + mutationTotalCount + 
                                        " total txn mutation size " + mutationTotalSize + 
                                        " total puts " + totalPuts + " total deletes " + totalDeletes);

          if (test) LOG.error("PIT CDC Exception: path : mc file close 3 ");
          else { 
                    LOG.error("PIT CDC Exception: path: others 3 "); 
                    throw new IOException(e);
           }

      }
  }

  
  public void addPut(TransactionMutationMsg.Builder tmBuilder, Put put) throws IOException {
      try {
         tmBuilder.addPut(ProtobufUtil.toMutation(MutationType.PUT, new Put(put)));
      } catch(IOException e) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          LOG.error(sw.toString());
      }
  }
  
  public void addDelete(TransactionMutationMsg.Builder tmBuilder, Delete del) throws IOException {
      try {
         tmBuilder.addDelete(ProtobufUtil.toMutation(MutationType.DELETE, new Delete(del)));
      } catch(IOException e) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          LOG.error(sw.toString());
      }
  }
  
  // concatenate several byte[]
  byte[] concat(byte[]...arrays) {
       // Determine the length of the result byte array
       int totalLength = 0;
       for (int i = 0; i < arrays.length; i++)  {
           totalLength += arrays[i].length;
       }

       // create the result array
       byte[] result = new byte[totalLength];

       // copy the source arrays into the result array
       int currentIndex = 0;
       for (int i = 0; i < arrays.length; i++)  {
           System.arraycopy(arrays[i], 0, result, currentIndex, arrays[i].length);
           currentIndex += arrays[i].length;
       }
       return result;
  }
   
}

