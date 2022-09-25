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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.lang.management.ManagementFactory;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.client.TableConfiguration;//for PatchClientScanner
import org.apache.hadoop.hbase.client.PatchClientScanner;//for PatchClientScanner
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;//for PatchClientScanner
import org.apache.hadoop.hbase.client.ClusterConnection;//for PatchClientScanner
import org.apache.hadoop.hbase.client.TrafParallelClientScanner;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.BroadcastRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.BroadcastResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRegionTxRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRegionTxResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRegionTxRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRegionTxResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteMultipleTransactionalRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteMultipleTransactionalResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteMultipleNonTransactionalRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteMultipleNonTransactionalResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteRegionTxRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteRegionTxResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteTransactionalRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteTransactionalResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetMultipleTransactionalRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetMultipleTransactionalResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetTransactionalRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetTransactionalResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.LockRequiredRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.LockRequiredResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutRegionTxRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutRegionTxResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutMultipleTransactionalRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutMultipleTransactionalResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutMultipleNonTransactionalRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutMultipleNonTransactionalResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.exception.LockTimeOutException;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.exception.DeadLockException;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.exception.LockCancelOperationException;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.exception.LockNotEnoughResourcsException;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;//for PatchClientScanner
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.regionserver.transactional.SingleVersionDeleteNotSupported;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.NotServingRegionException;

import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;

import java.util.concurrent.ThreadPoolExecutor;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.apache.hadoop.hbase.client.transactional.ATRConfig;

/**
 * Table with transactional support.
 */
public class TransactionalTable extends HTable {
    static final Log LOG = LogFactory.getLog(TransactionalTable.class);
    private static final String REGION_NAME_MISMATCH_EXCEPTION = "RegionNameMismatchException";
    static Configuration       config;
    private Connection connection = null;
    static ThreadPoolExecutor threadPool = null;
    static int                 retries = 3;
    static int                 delay = 1000;
    static int                 lockdelay = 100;
    static int                 lockRetries = 10000;
    static int                 regionNotReadyDelay = 1000;
    static long                capValue = 0;
    static byte[]              pidByte;
    static String              envdisable_unique = null;
    String                     tName= null;

    private String retryErrMsg = "Coprocessor result is null, retries exhausted for table " + this.getName().getNameAsString();

    //this scanner implement fixes and improvements over the regular client
    //scanner, and will be deprecated or modified as HBase implements these fixes
    // in future releases.
    // This feature can be enabled using hbase config: hbase.trafodion.patchclientscanner.enabled (true or false)
    static boolean             usePatchScanner;
    private RpcRetryingCallerFactory rpcCallerFactory;
    private RpcControllerFactory rpcControllerFactory;
    static private int replicaCallTimeoutMicroSecondScan;
    private static boolean enableRowLevelLock = false;
    private static Integer costTh = -1;
    private static Integer costAllTh = -1;
    private static String  PID;
    private static long    transId = -1;
    private static long    lockCost = 0;
    private static double[] costSum = new double[20];
    private static long[]  callCount = new long[20]; 

    // the 4 above privates are needed for PatchScannerClient because they are private in HTable
    // therefore not accessible here. So I keep reference at the TransactionTable level so they
    // can be used to invoke contructor of PatchClientScanner
    static {
       Configuration config = HBaseConfiguration.create();
       threadPool = HTable.getDefaultExecutor(config);
       lockdelay = config.getInt("hbase.client.forupdatelock.delay", 100);  //default 100ms
       lockRetries = config.getInt("hbase.client.forupdatelock.retries", 10000); //default 10000 times , ie 1000 seconds
       
       String useTTRetry = System.getenv("TM_TRANSACTIONAL_TABLE_RETRY");
       if (useTTRetry != null)
          retries = Integer.parseInt(useTTRetry);
       if (LOG.isInfoEnabled()) LOG.info("Transactional Table retry count: " + retries);
       RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
       int pid = Integer.valueOf(runtimeMXBean.getName().split("@")[0]).intValue();
       ByteBuffer bufferPid = ByteBuffer.allocate(Integer.BYTES);
       bufferPid.putInt(pid);
       pidByte = bufferPid.array();
       envdisable_unique = System.getenv("DISABLE_UNIQUE");

        String envEnableRowLevelLock = System.getenv("ENABLE_ROW_LEVEL_LOCK");
        if (envEnableRowLevelLock != null) {
            try {
                enableRowLevelLock = (Integer.parseInt(envEnableRowLevelLock.trim()) == 0) ? false : true;
            } catch (Exception e) {
            }
        }

       if (enableRowLevelLock) {
           String lockRetriesStr = System.getenv("LOCK_CLIENT_RETRIES_TIMES");
           if (lockRetriesStr != null) {
               try {
                   lockRetries = Integer.parseInt(lockRetriesStr);
               } catch (Exception e){}
           } else {
               lockRetries = 20;
           }
           LOG.info("LOCK_CLIENT_RETRIES_TIMES: " + lockRetries);
       }

       String costThreshold = System.getenv("RECORD_TIME_COST_HBASE");
       if (costThreshold != null && false == costThreshold.trim().isEmpty())
         costTh = Integer.parseInt(costThreshold);

       costThreshold = System.getenv("RECORD_TIME_COST_HBASE_ALL");
       if (costThreshold != null && false == costThreshold.trim().isEmpty())
         costAllTh = Integer.parseInt(costThreshold);

       PID = ManagementFactory.getRuntimeMXBean().getName();
       PID = PID.split("@")[0];

       if (LOG.isInfoEnabled()) LOG.info("Transactional Table retry count: " + retries);       
    }
    static private ExecutorService threadPoolPut;
    static private boolean threadPoolPutInit = false;
    static private boolean threadPoolAuxInit = false;

    //temp solution to M-20892
    //when the target region belong to a table with no primary key
    //need to set keepMutationRows to true, so it will allow CDC to get mutation
    private boolean keepMutationRows = ATRConfig.instance().isATRXDCEnabled();

    private abstract class TransactionalTableCallable implements Callable<Integer> {
            final TransactionState transactionState;
            final long savepointId;
            final long pSavepointId;
            final String regionName;
            final List<Put> rowsInSameRegion;
            final List<Put> rowsDone;
            Map.Entry<TransactionRegionLocation, List<Put>> entry;
            TransactionRegionLocation location;
            long transactionId;
            TransactionalTable tt;
            final String queryContext;

            TransactionalTableCallable (TransactionalTable ptt,
                                    final TransactionState ptransactionState,
                                    final long psavepointId,
                                    final long ppsavepointId,
                                    final String pregionName, 
                                    final List<Put> prowsInSameRegion,
                                    Map.Entry<TransactionRegionLocation, List<Put>> pentry,
                                    TransactionRegionLocation plocation,
                                    long ptransactionId,
                                    List<Put> prowsDone,
                                    final String queryContext) {

            tt = ptt;
            transactionState = ptransactionState;
            savepointId = psavepointId;
            pSavepointId = ppsavepointId;
            regionName = pregionName;
            rowsInSameRegion = prowsInSameRegion;
            entry = pentry;
            location = plocation;
            transactionId = ptransactionId;
            rowsDone = prowsDone;
            this.queryContext = queryContext;
        }

        public Integer doPutX() throws IOException {
           if (LOG.isDebugEnabled()) LOG.debug("Inside doPutX() : transactionState " + transactionState +
                      "savepointId " + savepointId + "regionName " + regionName + "rowsInSameRegion " + rowsInSameRegion +
                      "entry " + entry + "location " + location + "transactionId " + transactionId);
 
           Batch.Call<TrxRegionService, PutMultipleTransactionalResponse> callable =
                new Batch.Call<TrxRegionService, PutMultipleTransactionalResponse>() {
                     ServerRpcController controller = new ServerRpcController();
                     BlockingRpcCallback<PutMultipleTransactionalResponse> rpcCallback =
                          new BlockingRpcCallback<PutMultipleTransactionalResponse>();

               @Override
               public PutMultipleTransactionalResponse call(TrxRegionService instance) throws IOException {
                   org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutMultipleTransactionalRequest.Builder builder = PutMultipleTransactionalRequest.newBuilder();
                   builder.setTransactionId(transactionState.getTransactionId());
                   builder.setStartId(transactionState.getStartId());
                   builder.setSavepointId(savepointId);
                   builder.setPSavepointId(pSavepointId);
                   builder.setRegionName(ByteString.copyFromUtf8(regionName));
                   builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
                   if( keepMutationRows == true)
                     builder.setKeepOldRow(true);
                   else
                     builder.setKeepOldRow(false);
                   for (Put put : rowsInSameRegion){
                      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);
                      builder.addPut(m1);
                   }
  
                   instance.putMultiple(controller, builder.build(), rpcCallback);
                   return rpcCallback.get();
               }
           };
           PutMultipleTransactionalResponse result = null;
           try {
               int retryCount = 0;
               int lockRetryCount = 0;
               //will later add support for write lock, so this change is required in future
               int retryTotal = TransactionalTable.retries; //default retry number
               boolean retry = false;
               do {
               long timeCost = System.currentTimeMillis();
               boolean isLockException = false;
               Iterator<Map.Entry<byte[], PutMultipleTransactionalResponse>> it= tt.coprocessorService(TrxRegionService.class, 
                                            entry.getValue().get(0).getRow(),
                                            entry.getValue().get(0).getRow(),                                             
                                            callable)
                                            .entrySet().iterator();
                    if(it.hasNext()) {
                        result = it.next().getValue();
                        retry = false;
                    }

                    if (costTh >= 0 && result != null) {
                      long cost1 = result.getCoproSTime() - timeCost;
                      long cost3 = System.currentTimeMillis();
                      timeCost = cost3 - timeCost;
                      costSum[0] += timeCost;
                      callCount[0]++;
                      if (timeCost >= costTh) {
                        long cost2 = result.getCoproETime() - result.getCoproSTime();
                        cost3 -= result.getCoproETime();
                        LOG.warn("TransactionalTableCallable doPutX copro PID " + PID + " txID " + transactionState.getTransactionId() + " CC " + callCount[0] + " ATC " + (costSum[0] / callCount[0]) +  " TTC " + costSum[0]  + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + regionName);
                      }
                    }

                    if(result == null || result.getException().contains("closing region")
                         || result.getException().contains("NewTransactionStartedBefore")
                         || isLockException(result.getException())) {
                         retryTotal = TransactionalTable.retries;
                         if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
                             if (LOG.isTraceEnabled()) LOG.trace("put <List> retrying because region is recovering trRegion ["
                                 + location.getRegionInfo().getEncodedName() + "], endKey: "
                                 + Hex.encodeHexString(location.getRegionInfo().getEndKey())
                                 + " and transaction [" + transactionId + "]");

                             Thread.sleep(TransactionalTable.regionNotReadyDelay);
                        }
			else if(result != null && result.getException().contains("get for update lock timeout")) {
				Thread.sleep(TransactionalTable.lockdelay);
	                        lockCost += TransactionalTable.lockdelay;
                                lockRetryCount++;
                                isLockException = true;
			}
                        else if (result != null && isLockException(result.getException())) {
                            isLockException = true;
                            lockRetryCount++;
			    if (transactionState.getCancelOperation())
			      throw new LockCancelOperationException("transaction " + transactionId + " is canceled!");
                        }
                        else{
		             retryTotal = TransactionalTable.retries;
                             Thread.sleep(TransactionalTable.delay);
                        }
                        retry = true;
                        transactionState.setRetried(true);
			if (result == null || !isLockException) 
                           retryCount++;
                   }
               } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
           } catch (Throwable e) {
               if (LOG.isErrorEnabled()) LOG.error("ERROR while calling putMultipleTransactional ",e);
               throw new IOException("ERROR while calling putMultipleTransactional ",e);
          }
          if(result == null) {
              throw new IOException(retryErrMsg);
          } else if (result.hasException()) {
            if (result.getException().contains("LockTimeOutException")) {
              throw new LockTimeOutException(result.getException());
            } else if (result.getException().contains("DeadLockException")) {
                throw new DeadLockException(result.getException());
            } else if (result.getException().contains("LockNotEnoughResourcsException")) {
                throw new LockNotEnoughResourcsException(result.getException());
            }
            throw new IOException(result.getException());
          }
          rowsDone.addAll(rowsInSameRegion);

          return 1;
       }
    }

    private abstract class TransactionalTableDeletexCallable implements Callable<Integer> {
        final TransactionState transactionState;
        final long savepointId;
        final long pSavepointId;
        final String regionName;
        final List<Delete> deletesInSameRegion;
        final boolean noConflictCheckForIndex;
        Map.Entry<TransactionRegionLocation, List<Delete>> dEntry;
        TransactionRegionLocation location;
        long transactionId;
        TransactionalTable tt;
        final String queryContext;

     TransactionalTableDeletexCallable (TransactionalTable ptt,
                final TransactionState ptransactionState,
                final long psavepointId,
                final long ppsavepointId,
                final boolean pNoConflictCheckForIndex,
                final String pregionName,
                final List<Delete> rowsInSameRegion,
                Map.Entry<TransactionRegionLocation, List<Delete>> entry,
                TransactionRegionLocation plocation,
                final String queryContext) {

       tt = ptt;
       transactionState = ptransactionState;
       savepointId = psavepointId;
       pSavepointId = ppsavepointId;
       regionName = pregionName;
       deletesInSameRegion = rowsInSameRegion;
       dEntry = entry;
       location = plocation;
       noConflictCheckForIndex = pNoConflictCheckForIndex;
       transactionId = transactionState.getTransactionId();
       this.queryContext = queryContext;
    }

    public Integer doDeleteSetX() throws IOException {
      if (LOG.isDebugEnabled()) LOG.debug("Inside doDeleteSetX() : transactionState " + transactionState +
             " savepointId " + savepointId + " noConflictCheckForIndex " + noConflictCheckForIndex +
             " regionName " + regionName + " deletesInSameRegion " + deletesInSameRegion +
             " dEntry " + dEntry + " location " + location + " transactionId " + transactionId);

        Batch.Call<TrxRegionService, DeleteMultipleTransactionalResponse> callable =
             new Batch.Call<TrxRegionService, DeleteMultipleTransactionalResponse>() {
                  ServerRpcController controller = new ServerRpcController();
                  BlockingRpcCallback<DeleteMultipleTransactionalResponse> rpcCallback =
                       new BlockingRpcCallback<DeleteMultipleTransactionalResponse>();

            @Override
            public DeleteMultipleTransactionalResponse call(TrxRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteMultipleTransactionalRequest.Builder builder = DeleteMultipleTransactionalRequest.newBuilder();
                builder.setTransactionId(transactionState.getTransactionId());
                builder.setStartId(transactionState.getStartId());
                builder.setSavepointId(savepointId);
                builder.setPSavepointId(pSavepointId);
                builder.setNoConflictCheckForIndex(noConflictCheckForIndex);
                builder.setRegionName(ByteString.copyFromUtf8(regionName));
                builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
                if( keepMutationRows == true)
                  builder.setKeepOldRow(true);
                else
                  builder.setKeepOldRow(false);

                for (Delete delete : deletesInSameRegion){
                   MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, delete);
                   builder.addDelete(m1);
                }

                instance.deleteMultiple(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
            }
        };

       DeleteMultipleTransactionalResponse result = null;
       try {
           int retryCount = 0;
           int retryTotal = TransactionalTable.retries; //default retry number
           boolean retry = false;
           int lockRetryCount = 0;
          do {
             long timeCost = System.currentTimeMillis();
             boolean isLockException = false;
             Iterator<Map.Entry<byte[], DeleteMultipleTransactionalResponse>> it= tt.coprocessorService(TrxRegionService.class,
                   dEntry.getValue().get(0).getRow(),
                   dEntry.getValue().get(0).getRow(),
                   callable)
                   .entrySet().iterator();

             if(it.hasNext()) {
                result = it.next().getValue();
                retry = false;
             }

             if (costTh >= 0 && result != null) {
               long cost1 = result.getCoproSTime() - timeCost;
               long cost3 = System.currentTimeMillis();
               timeCost = cost3 - timeCost;
               costSum[1] += timeCost;
               callCount[1]++;
               if (timeCost >= costTh) {
                 long cost2 = result.getCoproETime() - result.getCoproSTime();
                 cost3 -= result.getCoproETime();
                 LOG.warn("TransactionalTableDeletexCallable doDeleteSetX copro PID " + PID + " txID " + transactionState.getTransactionId() + " CC " + callCount[1] + " ATC " + (costSum[1] / callCount[1]) + " TTC " + costSum[1] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + regionName);
               }
             }

             if(result == null || result.getException().contains("closing region")
                     || result.getException().contains("NewTransactionStartedBefore")
                     || isLockException(result.getException())) {
                if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
                   if (LOG.isTraceEnabled()) LOG.trace("delete <List> retrying because region is recovering trRegion ["
                              + location.getRegionInfo().getEncodedName() + "], endKey: "
                              + Hex.encodeHexString(location.getRegionInfo().getEndKey())
                              + " and transaction [" + transactionId + "]");

                   Thread.sleep(TransactionalTable.regionNotReadyDelay);
                }
                else if (result != null && isLockException(result.getException())) {
                    lockRetryCount++;
                    isLockException = true;
                    if (transactionState.getCancelOperation())
                        throw new LockCancelOperationException("transaction " + transactionId + " is canceled!");
                }
                else{
                   Thread.sleep(TransactionalTable.delay);
                }
                retry = true;
                transactionState.setRetried(true);
                if (result == null || !isLockException)
                    retryCount++;
             }
          } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
       } catch (Throwable e) {
          if (LOG.isErrorEnabled()) LOG.error("ERROR while calling DeleteMultipleTransactional ",e);
          throw new IOException("ERROR while calling DeleteMultipleTransactional ",e);
       }
       if(result == null) {
           throw new IOException(retryErrMsg);
       } else if (result.hasException()) {
           if (result.getException().contains("LockTimeOutException")) {
              throw new LockTimeOutException(result.getException());
            } else if (result.getException().contains("DeadLockException")) {
                throw new DeadLockException(result.getException());
            } else if (result.getException().contains("LockNotEnoughResourcsException")) {
                throw new LockNotEnoughResourcsException(result.getException());
            }
            throw new IOException(result.getException());
       }

       return 1;
      }
    }

    private abstract class TransactionalTableGetSetXCallable implements Callable<GetMultipleTransactionalResponse> {
        final TransactionState transactionState;
        final long savepointId;
        final long pSavepointId;
        final boolean skipReadConflict;
        final boolean waitOnSelectForUpdate;
        final int isolationLevel;
        final int lockMode;
        final String regionName;
        final List<Get> getsInSameRegion;
        Map.Entry<TransactionRegionLocation, List<Get>> gEntry;
        TransactionRegionLocation location;
        long transactionId;
        TransactionalTable tt;
        final String queryContext;

    TransactionalTableGetSetXCallable (TransactionalTable ptt,
            final TransactionState ptransactionState,
            final long psavepointId,
            final long ppsavepointId,
            final int pIsolationLevel,
            final int pLockMode,
            final boolean pskipReadConflict,
            final boolean pWaitOnSelectForUpdate,
            final String pregionName,
            final List<Get> rowsInSameRegion,
            Map.Entry<TransactionRegionLocation, List<Get>> entry,
            TransactionRegionLocation plocation,
            long ptransactionId,
            final String queryContext) {

       tt = ptt;
       transactionState = ptransactionState;
       savepointId = psavepointId;
       pSavepointId = ppsavepointId;
       isolationLevel = pIsolationLevel;
       lockMode = pLockMode;
       skipReadConflict = pskipReadConflict;
       waitOnSelectForUpdate = pWaitOnSelectForUpdate;
       regionName = pregionName;
       getsInSameRegion = rowsInSameRegion;
       gEntry = entry;
       location = plocation;
       transactionId = ptransactionId;
       this.queryContext = queryContext;
    }

    public GetMultipleTransactionalResponse doGetSetX() throws IOException {
       if (LOG.isDebugEnabled()) LOG.debug("Inside doGetSetX() : transactionState " + transactionState +
                   " savepointId " + savepointId + " gets size: " + getsInSameRegion.size() +
                   " isolationLevel " + isolationLevel + " lockMode " + lockMode +
                   " skipReadConflict " + skipReadConflict + " waitOnSelectForUpdate " + waitOnSelectForUpdate +
                   " location " + location + " transactionId " + transactionId);

       Batch.Call<TrxRegionService, GetMultipleTransactionalResponse> callable =
            new Batch.Call<TrxRegionService, GetMultipleTransactionalResponse>() {
                  ServerRpcController controller = new ServerRpcController();
                  BlockingRpcCallback<GetMultipleTransactionalResponse> rpcCallback =
                       new BlockingRpcCallback<GetMultipleTransactionalResponse>();

            @Override
            public GetMultipleTransactionalResponse call(TrxRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetMultipleTransactionalRequest.Builder builder = GetMultipleTransactionalRequest.newBuilder();
                builder.setTransactionId(transactionState.getTransactionId());
                builder.setStartId(transactionState.getStartId());
                builder.setSavepointId(savepointId);
                builder.setPSavepointId(pSavepointId);
                builder.setIsolationLevel(isolationLevel);
                builder.setLockMode(lockMode);
                builder.setSkipScanConflict(skipReadConflict);
                builder.setWaitOnSelectForUpdate(waitOnSelectForUpdate);
                builder.setRegionName(ByteString.copyFromUtf8(regionName));
                builder.setQueryContext(ByteString.copyFromUtf8(queryContext));

                for (Get get : getsInSameRegion){
                   builder.addGet(ProtobufUtil.toGet(get));
                }

                instance.getMultiple(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
            }
        };
       GetMultipleTransactionalResponse result = null;
       try {
          int retryCount = 0;
          int retryTotal = TransactionalTable.retries; //default retry number
          boolean retry = false;
          int lockRetryCount = 0;
          do {
            long timeCost = System.currentTimeMillis();
            boolean isLockException = false;
            Iterator<Map.Entry<byte[], GetMultipleTransactionalResponse>> it = tt.coprocessorService(TrxRegionService.class,
                                         gEntry.getValue().get(0).getRow(),
                                         gEntry.getValue().get(0).getRow(),
                                         callable)
                                         .entrySet().iterator();
            if(it.hasNext()) {
                result = it.next().getValue();
                retry = false;
            }

            if (costTh >= 0 && result != null) {
              long cost1  = result.getCoproSTime() - timeCost;
              long cost3  = System.currentTimeMillis();
              timeCost    = cost3 - timeCost;
              costSum[2] += timeCost;
              callCount[2]++;
              if (timeCost > costTh) {
                long cost2 = result.getCoproETime() - result.getCoproSTime();
                cost3 -= result.getCoproETime();
                LOG.warn("TransactionalTableGetSetXCallable doGetSetX copro PID " + PID + " txID " + transactionState.getTransactionId() + " CC " + callCount[2] + " ATC " + (costSum[2] / callCount[2]) + " TTC " + costSum[2] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + regionName);
              }
            }

            if(result == null || result.getException().contains("closing region")
                    || result.getException().contains("NewTransactionStartedBefore")
                    || isLockException(result.getException())) {
                if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
                    if (LOG.isTraceEnabled()) LOG.trace("get <List> retrying because region is recovering trRegion ["
                              + location.getRegionInfo().getEncodedName() + "], endKey: "
                              + Hex.encodeHexString(location.getRegionInfo().getEndKey())
                              + " and transaction [" + transactionId + "]");

                    Thread.sleep(TransactionalTable.regionNotReadyDelay);
                }
                else if (result != null && isLockException(result.getException())) {
                    isLockException = true;
                    lockRetryCount++;
                    if (transactionState.getCancelOperation())
                        throw new LockCancelOperationException("transaction " + transactionId + " is canceled!");
                }
                else{
                    Thread.sleep(TransactionalTable.delay);
                }
                retry = true;
                transactionState.setRetried(true);
                if (result == null || !isLockException)
                    retryCount++;
            }
            else{
                // We will return result;
            }
          } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
        } catch (Throwable e) {
            if (LOG.isErrorEnabled()) LOG.error("ERROR while calling GetMultipleTransactional ",e);
            throw new IOException("ERROR while calling GetMultipleTransactional ",e);
       }
       if(result == null) {
           throw new IOException(retryErrMsg);
       } else if (result.hasException()) {
           if (result.getException().contains("LockTimeOutException")) {
              throw new LockTimeOutException(result.getException());
            } else if (result.getException().contains("DeadLockException")) {
                throw new DeadLockException(result.getException());
            } else if (result.getException().contains("LockNotEnoughResourcsException")) {
                throw new LockNotEnoughResourcsException(result.getException());
            }
            else if(result.getException().contains("get for update lock timeout")){
		throw new IOException("get for update lock timeout") ; 
            }
            throw new IOException(result.getException());
       }

       return result;
    }
   }

    private abstract class TransactionalTablePutNonTxnCallable implements Callable<Integer> {
            final List<Put> rowsInSameRegion;
            Map.Entry<TransactionRegionLocation, List<Put>> entry;
            TransactionRegionLocation location;
            final String regionName;
             long nonTransactionId;
             long commitId;
            TransactionalTable tt;
             long flags;
            int totalNum;

            TransactionalTablePutNonTxnCallable (TransactionalTable ptt,
                                    final List<Put> prowsInSameRegion,
                                    Map.Entry<TransactionRegionLocation, List<Put>> pentry,
                                    TransactionRegionLocation plocation, final String pregionName,
                                    long pNonTransactionId, long pCommitId, long pflags, int tn) {

            tt = ptt;
            rowsInSameRegion = prowsInSameRegion;
            entry = pentry;
            location = plocation;
            nonTransactionId = pNonTransactionId;
            commitId = pCommitId;
            flags = pflags;
            regionName = pregionName;
            totalNum = tn;
        }

        public Integer doPutNonX() throws IOException {
           if (LOG.isDebugEnabled()) LOG.debug("Inside doPutNonX() : rowsInSameRegion " + rowsInSameRegion +
                      "entry " + entry + "location " + location + "transactionId " + nonTransactionId);
 
           Batch.Call<TrxRegionService, PutMultipleNonTransactionalResponse> callable =
                new Batch.Call<TrxRegionService, PutMultipleNonTransactionalResponse>() {
                     ServerRpcController controller = new ServerRpcController();
                     BlockingRpcCallback<PutMultipleNonTransactionalResponse> rpcCallback =
                          new BlockingRpcCallback<PutMultipleNonTransactionalResponse>();

               @Override
               public PutMultipleNonTransactionalResponse call(TrxRegionService instance) throws IOException {
                   org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutMultipleNonTransactionalRequest.Builder builder = PutMultipleNonTransactionalRequest.newBuilder();

                   builder.setNonTransactionId(nonTransactionId);
                   builder.setCommitId(commitId);
                   builder.setFlags(flags);
                   builder.setRegionName(ByteString.copyFromUtf8(regionName));
                   builder.setTotalNum(totalNum);
                   for (Put put : rowsInSameRegion){
                      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);
                      builder.addPut(m1);
                   }
  
                   instance.putMultipleNonTxn(controller, builder.build(), rpcCallback);
                   return rpcCallback.get();
               }
           };
           PutMultipleNonTransactionalResponse result = null;
           try {
               int retryCount = 0;
               int retryTotal = TransactionalTable.retries; //default retry number
               boolean retry = false;
               int lockRetryCount = 0;
               do {
               long timeCost = System.currentTimeMillis();
               boolean isLockException = false;
               Iterator<Map.Entry<byte[], PutMultipleNonTransactionalResponse>> it= tt.coprocessorService(TrxRegionService.class, 
                                            entry.getValue().get(0).getRow(),
                                            entry.getValue().get(0).getRow(),                                             
                                            callable)
                                            .entrySet().iterator();
                    if(it.hasNext()) {
                        result = it.next().getValue();
                        retry = false;
                    }

                    if (costTh >= 0 && result != null) {
                      long cost1  = result.getCoproSTime() - timeCost;
                      long cost3  = System.currentTimeMillis();
                      timeCost    = cost3 - timeCost;
                      costSum[3] += timeCost;
                      callCount[3]++;
                      if (timeCost >= costTh) {
                        long cost2 = result.getCoproETime() - result.getCoproSTime();
                        cost3 -= result.getCoproETime();
                        LOG.warn(regionName + " TransactionalTablePutNonTxnCallable doPutNonX copro PID " + PID + " txID " + nonTransactionId + " CC " + callCount[3] + " ATC " + (costSum[3] / callCount[3]) + " TTC " + costSum[3] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + regionName);
                      }
                    }

                    if(result == null || result.getException().contains("closing region")
                         || result.getException().contains("NewTransactionStartedBefore")
                         || isLockException(result.getException())) {
                         if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
                             if (LOG.isTraceEnabled()) LOG.trace("put <List> retrying because region is recovering trRegion ["
                                 + location.getRegionInfo().getEncodedName() + "], endKey: "
                                 + Hex.encodeHexString(location.getRegionInfo().getEndKey())
                                 + " and transaction [" + nonTransactionId + "]");

                              Thread.sleep(TransactionalTable.regionNotReadyDelay);
                        }
                         else if (result != null && isLockException(result.getException())) {
                             isLockException = true;
                             lockRetryCount++;
                         }
                        else{
                              Thread.sleep(TransactionalTable.delay);
                        }
                        retry = true;
                        //transactionState.setRetried(true);
                        if (result == null || !isLockException)
                            retryCount++;
                   }
               } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
           } catch (Throwable e) {
               if (LOG.isErrorEnabled()) LOG.error("ERROR while calling putMultipleNonTransactional ",e);
               throw new IOException("ERROR while calling putMultipleNonTransactional ",e);
          }
          if(result == null) {
              throw new IOException(retryErrMsg);
          } else if (result.hasException()) {
            if (result.getException().contains("LockTimeOutException")) {
              throw new LockTimeOutException(result.getException());
            } else if (result.getException().contains("DeadLockException")) {
                throw new DeadLockException(result.getException());
            } else if (result.getException().contains("LockNotEnoughResourcsException")) {
                throw new LockNotEnoughResourcsException(result.getException());
            }
            throw new IOException(result.getException());
          }

          return 1;
       } // doPutNonX

    }

    private abstract class TransactionalTableDeleteNonTxnCallable implements Callable<Integer> {
            final List<Delete> rowsInSameRegion;
            Map.Entry<TransactionRegionLocation, List<Delete>> entry;
            TransactionRegionLocation location;
            final String regionName;
             long nonTransactionId;
            TransactionalTable tt;
             long commitId;
             long flags;

            TransactionalTableDeleteNonTxnCallable (TransactionalTable ptt,
                                    final List<Delete> prowsInSameRegion,
                                    Map.Entry<TransactionRegionLocation, List<Delete>> pentry,
                                    TransactionRegionLocation plocation, final String pregionName,
                                    long pNonTransactionId, long pCommitId, long pflags) {

            tt = ptt;
            rowsInSameRegion = prowsInSameRegion;
            entry = pentry;
            location = plocation;
            regionName = pregionName;
            nonTransactionId = pNonTransactionId;
            commitId = pCommitId;
            flags = pflags;

        }

        public Integer doDeleteNonX() throws IOException {
           if (LOG.isDebugEnabled()) LOG.debug("Inside doDeleteNonX() : rowsInSameRegion " + rowsInSameRegion +
                      "entry " + entry + "location " + location + "transactionId " + nonTransactionId);
 
           Batch.Call<TrxRegionService, DeleteMultipleNonTransactionalResponse> callable =
                new Batch.Call<TrxRegionService, DeleteMultipleNonTransactionalResponse>() {
                     ServerRpcController controller = new ServerRpcController();
                     BlockingRpcCallback<DeleteMultipleNonTransactionalResponse> rpcCallback =
                          new BlockingRpcCallback<DeleteMultipleNonTransactionalResponse>();

               @Override
               public DeleteMultipleNonTransactionalResponse call(TrxRegionService instance) throws IOException {
                   org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteMultipleNonTransactionalRequest.Builder builder = DeleteMultipleNonTransactionalRequest.newBuilder();

                   builder.setNonTransactionId(nonTransactionId);
                   builder.setCommitId(commitId);
                   builder.setFlags(flags);
                   builder.setRegionName(ByteString.copyFromUtf8(regionName));
                   for (Delete del : rowsInSameRegion){
                      MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, del);
                      builder.addDelete(m1);
                   }
  
                   instance.deleteMultipleNonTxn(controller, builder.build(), rpcCallback);
                   return rpcCallback.get();
               }
           };
           DeleteMultipleNonTransactionalResponse result = null;
           try {
               int retryCount = 0;
               int retryTotal = TransactionalTable.retries; //default retry number
               boolean retry = false;
               int lockRetryCount = 0;
               do {
               long timeCost = System.currentTimeMillis();
               boolean isLockException = false;
               Iterator<Map.Entry<byte[], DeleteMultipleNonTransactionalResponse>> it= tt.coprocessorService(TrxRegionService.class, 
                                            entry.getValue().get(0).getRow(),
                                            entry.getValue().get(0).getRow(),                                             
                                            callable)
                                            .entrySet().iterator();
                    if(it.hasNext()) {
                        result = it.next().getValue();
                        retry = false;
                    }

                    if (costTh >= 0 && result != null) {
                      long cost1  = result.getCoproSTime() - timeCost;
                      long cost3  = System.currentTimeMillis();
                      timeCost    = cost3 - timeCost;
                      costSum[4] += timeCost;
                      callCount[4]++;
                      if (timeCost >= costTh) {
                        long cost2 = result.getCoproETime() - result.getCoproSTime();
                        cost3 -= result.getCoproETime();
                        LOG.warn(" TransactionalTableDeleteNonTxnCallable doDeleteNonX copro PID " + PID + " txID " + nonTransactionId + " CC " + callCount[4] + " ATC " + (costSum[4] / callCount[4]) + " TTC " + costSum[4] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + regionName);
                      }
                    }

                    if(result == null || result.getException().contains("closing region")
                         || result.getException().contains("NewTransactionStartedBefore")
                         || isLockException(result.getException())) {
                         if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
                             if (LOG.isTraceEnabled()) LOG.trace("put <List> retrying because region is recovering trRegion ["
                                 + location.getRegionInfo().getEncodedName() + "], endKey: "
                                 + Hex.encodeHexString(location.getRegionInfo().getEndKey())
                                 + " and transaction [" + nonTransactionId + "]");

                              Thread.sleep(TransactionalTable.regionNotReadyDelay);
                        }
                         else if (result != null && isLockException(result.getException())) {
                             isLockException = true;
                             lockRetryCount++;
                         }
                        else{
                              Thread.sleep(TransactionalTable.delay);
                        }
                        retry = true;
                        //transactionState.setRetried(true);
                        if (result == null || !isLockException)
                            retryCount++;
                   }
               } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
           } catch (Throwable e) {
               if (LOG.isErrorEnabled()) LOG.error("ERROR while calling deleteMultipleNonTransactional ",e);
               throw new IOException("ERROR while calling deleteMultipleNonTransactional ",e);
          }
          if(result == null) {
              throw new IOException(retryErrMsg);
          } else if (result.hasException()) {
            if (result.getException().contains("LockTimeOutException")) {
              throw new LockTimeOutException(result.getException());
            } else if (result.getException().contains("DeadLockException")) {
                throw new DeadLockException(result.getException());
            } else if (result.getException().contains("LockNotEnoughResourcsException")) {
                throw new LockNotEnoughResourcsException(result.getException());
            }
            throw new IOException(result.getException());
          }

          return 1;
       } // doDeleteNonX

    }

    private static synchronized void clearStatsIfNewTrans(long tid) {
      if (transId != tid && tid != -1) {
         long count = 0, cost = 0;
         for (int idx = 0; idx < 20; idx++) {
           count += callCount[idx];
           cost += costSum[idx];
           costSum[idx] = 0;
           callCount[idx] = 0;
         }
         if (transId != -1 && costAllTh >= 0 && cost >= costAllTh) {
           LOG.warn("TransactionalTable summary PID " + PID + " txID " + transId  + " TCC " + count + " TTC " + cost + " TLTC " + lockCost);
         }
         lockCost = 0;
         transId = tid;
      }
    }

    private void setPatchclientscannerAttributes() throws IOException
    {
       config = this.getConnection().getConfiguration();
       usePatchScanner = config.getBoolean("hbase.trafodion.patchclientscanner.enabled", false);
       // duplicated from TableConfiguration since it cannot be instantiated by lack of public constructor
       replicaCallTimeoutMicroSecondScan = config.getInt("hbase.client.replicaCallTimeout.scan", 1000000); 
    }
    
    /**
     * @param tableName
     * @throws IOException
     */
    public TransactionalTable(final String tableName, Connection conn) throws IOException {
        this(Bytes.toBytes(tableName), conn);        
       //check if this is a table that do not have primary key
       keepMutationRows = ATRConfig.instance().isATRXDCEnabled();
    }
/*
    //added for pacthClientScanner
    public TransactionalTable(TableName tableName,
                 final ClusterConnection connection,
                     final TableConfiguration tableConfig,
                     final RpcRetryingCallerFactory rpcCallerFactory,
                     final RpcControllerFactory rpcControllerFactory,
                     final ExecutorService pool) throws IOException {
       super(tableName, connection, pool);
       this.rpcCallerFactory = rpcCallerFactory;
       this.rpcControllerFactory = rpcControllerFactory;
       this.connection = connection;
       if (!threadPoolPutInit)
       {
          String poolPutSizeStr = System.getenv("TMCLIENT_POOL_PUT_SIZE");
          int intPutThreads = 16;  //default
          if (poolPutSizeStr != null)
             intPutThreads = Integer.parseInt(poolPutSizeStr.trim());
          threadPoolPut = Executors.newFixedThreadPool(intPutThreads);
          threadPoolPutInit = true;
       }

       
       setPatchclientscannerAttributes();
    }
*/
    // For TransactionManager
    public TransactionalTable(TableName tableName, Connection conn, ExecutorService auxThreads, int auxPoolSize) throws IOException {
       super(tableName, conn, threadPool);
       this.connection = conn;
       this.tName = tableName.getNameAsString();
       if (!threadPoolAuxInit)
       {
          auxThreads = Executors.newFixedThreadPool(auxPoolSize);
          threadPoolAuxInit = true;
       }
    }

    public TransactionalTable(final byte[] tableName, Connection conn) throws IOException {
       super(tableName, conn, threadPool);
       this.connection = conn;
       this.tName = this.getName().getNameAsString();
       if (!threadPoolPutInit)
       {
           String poolPutSizeStr = System.getenv("TMCLIENT_POOL_PUT_SIZE");
           int intPutThreads = 16;  //default
           if (poolPutSizeStr != null){
              intPutThreads = Integer.parseInt(poolPutSizeStr.trim());
           }
           threadPoolPut = Executors.newFixedThreadPool(intPutThreads);
           threadPoolPutInit = true;
       }
       setPatchclientscannerAttributes();
       keepMutationRows = ATRConfig.instance().isATRXDCEnabled();
    }

    private String normalizeTableName(String t){
      String tbns[] = t.split("\\.");
      int size = tbns.length;
      if(size < 2) return null;
      //last two parts schema and table name
      //String ret =  tbns[size-2] + "." + tbns[size-1];
      String schemaName = tbns[size-2].startsWith("\"") ? tbns[size-2].replaceAll("\"", "") : tbns[size-2];
      String tableName = tbns[size-1].startsWith("\"") ? tbns[size-1].replaceAll("\"", "") : tbns[size-1];
      String ret = schemaName +"."+tableName;
      return ret;
    }

    protected void addLocation(final TransactionState transactionState, HRegionLocation location) {
      if (LOG.isTraceEnabled()) LOG.trace("addLocation HRegionLocation ENTRY");
      TransactionRegionLocation trRegion = new TransactionRegionLocation(location.getRegionInfo(),
           location.getServerName(), 0);
      addLocation(transactionState, trRegion);
      if (LOG.isTraceEnabled()) LOG.trace("addLocation HRegionLocation EXIT");
    }

    protected void addLocation(final TransactionState transactionState, TransactionRegionLocation location) {
        if (LOG.isTraceEnabled()) LOG.trace("addLocation TransactionRegionLocation ENTRY");
        if (transactionState.addRegion(location)){
            if (LOG.isTraceEnabled()) LOG.trace("addLocation added region [" + location.getRegionInfo().getRegionNameAsString() + " endKey: "
                    + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " to TS. Beginning txn " + transactionState.getTransactionId() + " on server");
        }
        if (LOG.isTraceEnabled()) LOG.trace("addLocation TransactionRegionLocation EXIT");
    }

    /**
     * Method for getting data from a row
     * 
     * @param get the Get to fetch
     * @return the result
     * @throws IOException
     * @since 0.20.0
     */

    public Result get(final TransactionState transactionState, final long savepointId, final long pSavepointId,
                      final int lockMode, final boolean skipConflictAccess, final boolean waitOnSelectForUpdate, final Get get, final String queryContext) throws IOException {

        return get(transactionState, savepointId, pSavepointId, /* readCommitted */ 10, lockMode, skipConflictAccess, waitOnSelectForUpdate, get, /* migrate */ true, queryContext);

    }

    public Result get(final TransactionState transactionState, final long savepointId, final long pSavepointId, final int lockMode, final Get get, final String queryContext) throws IOException {

        return get(transactionState, savepointId, pSavepointId, /* readCommitted */ 10, lockMode, /* skipConflictAccess */ false, false, get, /* migrate */ true, queryContext);

    }

    public Result get(final TransactionState transactionState, final long savepointId, final long pSavepointId, final int isolationLevel, final int lockMode, final boolean skipConflictAccess, final boolean waitOnSelectForUpdate,
            final Get get, final boolean bool_addLocation, final String queryContext) throws IOException {

      if (LOG.isDebugEnabled()) LOG.debug("Enter TransactionalTable.get skipConflictAccess " + skipConflictAccess
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);      
      
      GetTransactionalResponse result = null;   
      try {
        int retryCount = 0;
	int retryTotal = TransactionalTable.retries; //default retry number
        boolean retry = false;
        boolean refresh = false;
        int lockRetryCount = 0;

        clearStatsIfNewTrans(transactionState.getTransactionId());
        do {
          boolean isLockException = false;
          HRegionLocation location = getConnection().getRegionLocator(this.getName()).getRegionLocation(get.getRow(), refresh);
          final String regionName = location.getRegionInfo().getRegionNameAsString();
          Batch.Call<TrxRegionService, GetTransactionalResponse> callable =
          new Batch.Call<TrxRegionService, GetTransactionalResponse>() {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<GetTransactionalResponse> rpcCallback =
            new BlockingRpcCallback<GetTransactionalResponse>();

            @Override
            public GetTransactionalResponse call(TrxRegionService instance) throws IOException {
            org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetTransactionalRequest.Builder builder = GetTransactionalRequest.newBuilder();
            builder.setGet(ProtobufUtil.toGet(get));
            builder.setTransactionId(transactionState.getTransactionId());
            builder.setStartId(transactionState.getStartId());
            builder.setSavepointId(savepointId);
            builder.setPSavepointId(pSavepointId);
            builder.setIsolationLevel(isolationLevel);
            builder.setLockMode(lockMode);
            builder.setSkipScanConflict(skipConflictAccess);
            builder.setRegionName(ByteString.copyFromUtf8(regionName));
            builder.setWaitOnSelectForUpdate( waitOnSelectForUpdate);
            builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetTransactionalRequest req = builder.build();

            instance.get(controller, req, rpcCallback);
            return rpcCallback.get();
           }
          };

          long timeCost = System.currentTimeMillis(); 
          refresh = false;
          Iterator<Map.Entry<byte[], TrxRegionProtos.GetTransactionalResponse>> it = super.coprocessorService(TrxRegionService.class, 
                                                                                                              get.getRow(), 
                                                                                                              get.getRow(), 
                                                                                                              callable)
                                                                                                              .entrySet().iterator();
          if(it.hasNext()) {
            result = it.next().getValue();
            retry = false;
          } 

          if (costTh >= 0 && result != null) {
            long cost1 = result.getCoproSTime() - timeCost;
            long cost3 = System.currentTimeMillis();
            timeCost = cost3 - timeCost;
            costSum[5] += timeCost;
            callCount[5]++;
            if (timeCost >= costTh) {
              long cost2 = result.getCoproETime() - result.getCoproSTime();
              cost3 -= result.getCoproETime();
              LOG.warn("get copro PID " + PID + " txID " + transactionState.getTransactionId() + " CC " + callCount[5] + " ATC " + (costSum[5] / callCount[5]) + " TTC " + costSum[5] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + this.getName());
            }
          }

          if(result == null || result.getException().contains("closing region")
                            || result.getException().contains("get for update lock timeout")
                            || result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)
                            || result.getException().contains("NewTransactionStartedBefore")
                            || isLockException(result.getException())) {
            if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
               if (LOG.isTraceEnabled()) LOG.trace("Get retrying because region is recovering,"
                      + " transaction [" + transactionState.getTransactionId() + "]");

               retryTotal = TransactionalTable.retries;
               Thread.sleep(TransactionalTable.regionNotReadyDelay);
            }
            else if(result != null && result.getException().contains("get for update lock timeout")) {
               Thread.sleep(TransactionalTable.lockdelay);
               lockCost += TransactionalTable.lockdelay;
               isLockException = true;
               lockRetryCount++;
            }
            else if (result != null && isLockException(result.getException())) {
                isLockException = true;
                lockRetryCount++;
                if (transactionState.getCancelOperation())
                    throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
            }
            else if (result != null && result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
               if (LOG.isDebugEnabled())
                 LOG.debug("TransactionalTable.get will retry due to RegionNameMismatchException, region's name is: " + regionName);
               retryTotal = TransactionalTable.retries;
               refresh = true;
            }
            else{
               retryTotal = TransactionalTable.retries;
               Thread.sleep(TransactionalTable.delay);
            }
            retry = true;
            transactionState.setRetried(true);
            if (result == null || !isLockException)
                retryCount++;
          }
          if (bool_addLocation && false == retry) addLocation(transactionState, location);
        } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
      } catch (Throwable e) {
        if (LOG.isErrorEnabled()) LOG.error("ERROR while calling getTransactional ", e);
        throw new IOException("ERROR while calling getTransactional ", e);
      } 
      
      if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.get "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);  
             
      if(result == null) {
          throw new NotServingRegionException(retryErrMsg);
      } else if(result.hasException()) {
        if (result.getException().contains("LockTimeOutException")) {
          throw new LockTimeOutException(result.getException());
        } else if (result.getException().contains("DeadLockException")) {
            throw new DeadLockException(result.getException());
        } else if (result.getException().contains("LockNotEnoughResourcsException")) {
            throw new LockNotEnoughResourcsException(result.getException());
        }
        throw new IOException(result.getException());
      }
      return ProtobufUtil.toResult(result.getResult());      
    }
    
    /**
     * @param delete
     * @throws IOException
     * @since 0.20.0
     */
    public void delete(final TransactionState transactionState,
                        final long savepointId, final long pSavepointId,
                        final Delete delete, final String queryContext) throws IOException {
      delete(transactionState, savepointId, pSavepointId, delete, true, false, queryContext);
    }

    public void delete(final TransactionState transactionState,
                        final long savepointId, final long pSavepointId, final Delete delete,
                        final boolean bool_addLocation, final boolean noConflictCheckForIndex, final String queryContext) throws IOException {
    
        if (LOG.isDebugEnabled()) LOG.debug("Enter TransactionalTable.delete "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);   
             
        SingleVersionDeleteNotSupported.validateDelete(delete);

        byte[] row = delete.getRow();
        DeleteTransactionalResponse result = null; 
        try {
          int retryCount = 0;
          int retryTotal = TransactionalTable.retries; //default retry number
          boolean retry = false;
          boolean refresh = false;
          int lockRetryCount = 0;

          clearStatsIfNewTrans(transactionState.getTransactionId());
          do {
            boolean isLockException = false;
            HRegionLocation location = getConnection().getRegionLocator(this.getName()).getRegionLocation(delete.getRow(), refresh);
            final String regionName = location.getRegionInfo().getRegionNameAsString();

            Batch.Call<TrxRegionService, DeleteTransactionalResponse> callable =
                new Batch.Call<TrxRegionService, DeleteTransactionalResponse>() {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<DeleteTransactionalResponse> rpcCallback =
                new BlockingRpcCallback<DeleteTransactionalResponse>();

                @Override
                public DeleteTransactionalResponse call(TrxRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteTransactionalRequest.Builder builder = DeleteTransactionalRequest.newBuilder();  
                builder.setTransactionId(transactionState.getTransactionId());
                builder.setStartId(transactionState.getStartId());
                builder.setSavepointId(savepointId);
                builder.setPSavepointId(pSavepointId);
                builder.setRegionName(ByteString.copyFromUtf8(regionName));
                builder.setNoConflictCheckForIndex(noConflictCheckForIndex);
                builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
                if( keepMutationRows == true) 
                  builder.setKeepOldRow(true);
                else
                  builder.setKeepOldRow(false);

                MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, delete);
                builder.setDelete(m1);
                instance.delete(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
              }
            };

            refresh = false;
            long timeCost = System.currentTimeMillis();
            Iterator<Map.Entry<byte[], DeleteTransactionalResponse>> it = super.coprocessorService(TrxRegionService.class, 
                                              row, 
                                              row, 
                                              callable)
                                              .entrySet().iterator();
            if(it.hasNext()) {
              result = it.next().getValue();
              retry = false;
            }

            if (costTh >= 0 && result != null) {
              long cost1 = result.getCoproSTime() - timeCost;
              long cost3 = System.currentTimeMillis();
              timeCost = cost3 - timeCost;
              costSum[6] += timeCost;
              callCount[6]++;
              if (timeCost >= costTh) {
                long cost2 = result.getCoproETime() - result.getCoproSTime();
                cost3 -= result.getCoproETime();
                LOG.warn(this.getName() + " delete copro PID " + PID + " txID " + transactionState.getTransactionId() + " CC " + callCount[6] + " ATC " + (costSum[6] / callCount[6]) + " TTC " + costSum[6] + " TC " + timeCost + " : " + cost1 + " : " + cost2 + " : " + cost3);
              }
            }

            if(result == null || result.getException().contains("closing region")
                              || result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)
                              || result.getException().contains("NewTransactionStartedBefore")
                              || isLockException(result.getException())) {
              if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
                 if (LOG.isTraceEnabled()) LOG.trace("Delete retrying because region is recovering,"
                          + " transaction [" + transactionState.getTransactionId() + "]");

                 Thread.sleep(TransactionalTable.regionNotReadyDelay);
              }
              else if (result != null && isLockException(result.getException())) {
                  isLockException = true;
                  lockRetryCount++;
                  if (transactionState.getCancelOperation())
                      throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
              }
              else if (result != null && result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
                if (LOG.isDebugEnabled())
                  LOG.debug("TransactionalTable.delete will retry due to RegionNameMismatchException, region's name is: " + regionName);
                refresh = true;
              }
              else{
                 Thread.sleep(TransactionalTable.delay);
              }
              retry = true;
              transactionState.setRetried(true);
              if (result == null || !isLockException)
                  retryCount++;
            }
            if (bool_addLocation && false == retry) addLocation(transactionState, location);
          } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
        } catch (Throwable t) {
          if (LOG.isErrorEnabled()) LOG.error("ERROR while calling delete ",t);
          throw new IOException("ERROR while calling coprocessor ",t);
        } 
        
      if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.delete "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);          
        
        if(result == null) {
            throw new NotServingRegionException(retryErrMsg);
        } else if(result.hasException()) {
          if (result.getException().contains("LockTimeOutException")) {
            throw new LockTimeOutException(result.getException());
          } else if (result.getException().contains("DeadLockException")) {
              throw new DeadLockException(result.getException());
          } else if (result.getException().contains("LockNotEnoughResourcsException")) {
              throw new LockNotEnoughResourcsException(result.getException());
          }
          throw new IOException(result.getException());
        }
    }

    /**
     * Method for broadcating a request to all regions of a table
     *
     * @param int requestType
     * @param boolean requestBool
     * @return the result
     * @throws IOException
     */

    public int broadcastRequest(final int requestType, final boolean requestBool) throws IOException {

      if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.broadcastRequest");

      Batch.Call<TrxRegionService, BroadcastResponse> callable =
      new Batch.Call<TrxRegionService, BroadcastResponse>() {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<BroadcastResponse> rpcCallback =
        new BlockingRpcCallback<BroadcastResponse>();

        @Override
        public BroadcastResponse call(TrxRegionService instance) throws IOException {
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.BroadcastRequest.Builder builder = BroadcastRequest.newBuilder();
        builder.setRequestType(requestType);
        builder.setRequestBool(requestBool);
        long bcost = System.currentTimeMillis();
        instance.broadcastRequest(controller, builder.build(), rpcCallback);
        BroadcastResponse response = rpcCallback.get();
        if (costTh >= 0 && response != null) {
          long cost1 = response.getCoproSTime() - bcost;
          long cost3 = System.currentTimeMillis();
          bcost = cost3 - bcost;
          costSum[7] += bcost;
          callCount[7]++;
          if (bcost >= costTh) {
            long cost2 = response.getCoproETime() - response.getCoproSTime();
            cost3 -= response.getCoproETime();
            LOG.warn("broadcastRequest each copro PID " + PID + " CC " + callCount[7] + " ATC " + (costSum[7] / callCount[7]) + " TTC " + costSum[7] + " TC " + bcost + " " + cost1 + " " + cost2 + " " + cost3);
          }
        }
        return response;
       }
      };

      BroadcastResponse result = null;
      BroadcastResponse savedResult = null;
      try {
        int retryCount = 0;
        int retryTotal = TransactionalTable.retries; //default retry number
        boolean retry = false;
        do {
          savedResult = null;
          if (LOG.isTraceEnabled()) LOG.trace("broadcastRequest sending request with retryCount " + retryCount );
          long timeCost = System.currentTimeMillis();
          Iterator<Map.Entry<byte[], TrxRegionProtos.BroadcastResponse>> it = super.coprocessorService(TrxRegionService.class,
                  HConstants.EMPTY_START_ROW,
                  HConstants.EMPTY_END_ROW,
                  callable).entrySet().iterator();
          if (costTh >= 0) {
            timeCost = System.currentTimeMillis() - timeCost;
            if (timeCost >= costTh)
              LOG.warn(this.getName() + " broadcastRequest copro PID " + PID + " TC " + timeCost);
          }

          int resultCount = 0;
          while(it.hasNext()) {
            resultCount++;
            result = it.next().getValue();
            if (LOG.isTraceEnabled()) LOG.trace("broadcastRequest result " + resultCount + " is " + result );
            retry = false;

            if(result == null || result.getHasException() || (result.getResult() != 0)) {
              if (result.getHasException()){
                 Exception e = new Exception(result.getException());
                 if (LOG.isErrorEnabled()) LOG.error("Exception while calling broadcastRequest and retrying " + result.getException() + " ", e);
              }
              else if (result.getResult() != 0) {
                 if (LOG.isErrorEnabled()) LOG.error("Unsuccessful result from broadcastRequest, retrying " + result.getResult());
              }
              savedResult = result;
              if (LOG.isTraceEnabled()) LOG.trace("broadcastRequest retrying ");
              Thread.sleep(TransactionalTable.delay);
              retry = true;
              retryCount++;
            }
          }
        } while (retryCount < retryTotal && retry == true);
      } catch (Throwable e) {
        if (LOG.isErrorEnabled()) LOG.error("ERROR while calling broadcastRequest ", e);
	if (e instanceof org.apache.hadoop.hbase.TableNotFoundException) {
	    if (LOG.isErrorEnabled()) LOG.error("Caught Table Not Found Exception");
	    throw new org.apache.hadoop.hbase.TableNotFoundException("ERROR broadcastRequest, Table Not Found");
	}
	else
	    throw new IOException("ERROR while calling broadcastRequest ", e);
      }
      if(result == null)
        throw new NotServingRegionException(retryErrMsg);        
      else if((savedResult != null) && savedResult.hasException())
        throw new IOException(savedResult.getException());
      else if(savedResult != null)
        return (savedResult.getResult());
      else
        return (result.getResult());
    }

    public void deleteRegionTx(final long tid, final Delete delete, final boolean autoCommit, final boolean recoveryToPitMode, final String queryContext) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("TransactionalTable.deleteRegionTx ENTRY, autoCommit: "
                + autoCommit + " recoveryToPitMode: " + recoveryToPitMode);
        SingleVersionDeleteNotSupported.validateDelete(delete);

        byte[] row = delete.getRow();
        DeleteRegionTxResponse result = null; 
        try {
          int retryCount = 0;
          int retryTotal = TransactionalTable.retries; //default retry number
          boolean retry = false;
          boolean refresh = false;
          int lockRetryCount = 0;

          clearStatsIfNewTrans(tid);
          do {
            boolean isLockException = false;
            final String regionName = getConnection().getRegionLocator(this.getName()).
                                      getRegionLocation(delete.getRow(), refresh).getRegionInfo().getRegionNameAsString();

            Batch.Call<TrxRegionService, DeleteRegionTxResponse> callable =
                new Batch.Call<TrxRegionService, DeleteRegionTxResponse>() {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<DeleteRegionTxResponse> rpcCallback =
                new BlockingRpcCallback<DeleteRegionTxResponse>();

              @Override
              public DeleteRegionTxResponse call(TrxRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteRegionTxRequest.Builder builder = DeleteRegionTxRequest.newBuilder();
                builder.setTid(tid);
                if (recoveryToPitMode){
                   builder.setCommitId(tid);
                }
                else{
                   builder.setCommitId(-1);
                }
                builder.setAutoCommit(autoCommit);
                builder.setRegionName(ByteString.copyFromUtf8(regionName));

                MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, delete);
                builder.setDelete(m1);
                builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
                instance.deleteRegionTx(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
              }
             };

            refresh = false;
            long timeCost = System.currentTimeMillis();
            Iterator<Map.Entry<byte[], DeleteRegionTxResponse>> it =
                 super.coprocessorService(TrxRegionService.class, row, row, callable).entrySet().iterator();
            if(it.hasNext()) {
              result = it.next().getValue();
              retry = false;
            }

            if (costTh >= 0 && result != null) {
              long cost1  = result.getCoproSTime() - timeCost;
              long cost3  = System.currentTimeMillis();
              timeCost    = cost3 - timeCost;
              costSum[8] += timeCost;
              callCount[8]++;
              if (timeCost >= costTh) {
                long cost2 = result.getCoproETime() - result.getCoproSTime();
                cost3 -= result.getCoproETime();
                LOG.warn("deleteRegionTx copro PID " + PID + " CC " + callCount[8] + " ATC " + (costSum[8] / callCount[8]) + " TTC " + costSum[8] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + this.getName());
              }
            }

            if(result == null || result.getException().contains("closing region")
                              || result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)
                              || isLockException(result.getException())) {
              if (result != null && result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
                if (LOG.isDebugEnabled())
                  LOG.debug("TransactionalTable.deleteRegionTx will retry due to RegionNameMismatchException, region's name is: " + regionName);
                refresh = true;
              }
              else if (result != null && isLockException(result.getException())) {
                  isLockException = true;
                  lockRetryCount++;
              }
              else
                Thread.sleep(TransactionalTable.delay);
              retry = true;
              if (result == null || !isLockException)
                  retryCount++;
            }
          } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
        } catch (Throwable t) {
          if (LOG.isErrorEnabled()) LOG.error("ERROR while calling deleteRegionTx ",t);
          throw new IOException("ERROR while calling deleteRegionTx",t);
        } 
        if(result == null) {
            throw new NotServingRegionException(retryErrMsg);
        } else if(result.hasException()) {
          if (result.getException().contains("LockTimeOutException")) {
            throw new LockTimeOutException(result.getException());
          } else if (result.getException().contains("DeadLockException")) {
              throw new DeadLockException(result.getException());
          } else if (result.getException().contains("LockNotEnoughResourcsException")) {
              throw new LockNotEnoughResourcsException(result.getException());
          }
          throw new IOException(result.getException());
        }
        // deleteRegionTx is void, may not need to check result
        if (LOG.isTraceEnabled()) LOG.trace("TransactionalTable.deleteRegionTx EXIT");
    }

    /**
     * Commit a Put to the table.
     * <p>
     * If autoFlush is false, the update is buffered.
     * 
     * @param put
     * @throws IOException
     * @since 0.20.0
     */
    public synchronized void put(final TransactionState transactionState, final long savepointId, final long pSavepointId, final Put put, final String queryContext) throws IOException {
      put(transactionState, savepointId, pSavepointId, put, true, false, queryContext);

    }

    public synchronized void put(final TransactionState transactionState, final long savepointId, final long pSavepointId, final Put put, final boolean bool_addLocation, final boolean noConflictCheckForIndex, final String queryContext) throws IOException {

      if (LOG.isDebugEnabled()) LOG.debug("Enter TransactionalTable.put "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);  
             
    PutTransactionalResponse result = null; 
    try {
      int retryCount = 0;
      int lockRetryCount = 0;
      int retryTotal = TransactionalTable.retries; //default retry number
      boolean retry = false;
      boolean refresh = false;

      clearStatsIfNewTrans(transactionState.getTransactionId());
      do {
        boolean isLockException = false;
        HRegionLocation location = getConnection().getRegionLocator(this.getName()).getRegionLocation(put.getRow(), refresh);
        final String regionName = location.getRegionInfo().getRegionNameAsString();

        Batch.Call<TrxRegionService, PutTransactionalResponse> callable =
        new Batch.Call<TrxRegionService, PutTransactionalResponse>() {
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<PutTransactionalResponse> rpcCallback =
              new BlockingRpcCallback<PutTransactionalResponse>();
          @Override
          public PutTransactionalResponse call(TrxRegionService instance) throws IOException {
            org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest.Builder builder = PutTransactionalRequest.newBuilder();
            builder.setTransactionId(transactionState.getTransactionId());
            builder.setStartId(transactionState.getStartId());
            builder.setSavepointId(savepointId);
            builder.setPSavepointId(pSavepointId);
            builder.setRegionName(ByteString.copyFromUtf8(regionName));
            builder.setNoConflictCheckForIndex(noConflictCheckForIndex);
            if( keepMutationRows == true)
              builder.setKeepOldRow(true);
            else
              builder.setKeepOldRow(false);

            builder.setQueryContext(ByteString.copyFromUtf8(queryContext));

            MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);
            builder.setPut(m1);

            instance.put(controller, builder.build(), rpcCallback);
            return rpcCallback.get();
          }
        };

        refresh = false;
        long timeCost = System.currentTimeMillis();
        Iterator<Map.Entry<byte[], PutTransactionalResponse>> it= super.coprocessorService(TrxRegionService.class, 
                                                                                          put.getRow(), 
                                                                                          put.getRow(), 
                                                                                          callable)
                                                                                          .entrySet().iterator();
        if(it.hasNext()) {
          result = it.next().getValue();
          retry = false;
        }

        if (costTh >= 0 && result != null) {
          long cost1  = result.getCoproSTime() - timeCost;
          long cost3  = System.currentTimeMillis();
          timeCost    = cost3 - timeCost;
          costSum[9] += timeCost;
          callCount[9]++;
          if (timeCost >= costTh) {
            long cost2 = result.getCoproETime() - result.getCoproSTime();
            cost3 -= result.getCoproETime();
            LOG.warn("put copro PID " + PID + " txID " + transactionState.getTransactionId() + " CC " + callCount[9] + " ATC " + (costSum[9] / callCount[9]) + " TTC " + costSum[9] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + this.getName());
          }
        }

        if(result == null || result.getException().contains("closing region")
                          || result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)
                          || result.getException().contains("get for update lock timeout")
                          || result.getException().contains("NewTransactionStartedBefore")
                          || isLockException(result.getException())) {
          if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
             if (LOG.isTraceEnabled()) LOG.trace("Put retrying because region is recovering,"
                      + " transaction [" + transactionState.getTransactionId() + "]");

             Thread.sleep(TransactionalTable.regionNotReadyDelay);
          }
          else if (result != null && isLockException(result.getException())) {
              isLockException = true;
              lockRetryCount++;
              if (transactionState.getCancelOperation())
                   throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
          }
          else if (result != null && result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
            if (LOG.isDebugEnabled())
              LOG.debug("TransactionalTable.put will retry due to RegionNameMismatchException, region's name is: " + regionName);
            refresh = true;
          }
          else if (result != null && result.getException().contains("get for update lock timeout")) {
		Thread.sleep(TransactionalTable.lockdelay);
                lockCost += TransactionalTable.lockdelay;
                isLockException = true;
                lockRetryCount++;
          }
          else{
             Thread.sleep(TransactionalTable.delay);
          }
          retry = true;
          transactionState.setRetried(true);
          if(result == null || !isLockException) 
            retryCount++;
        }
        if (bool_addLocation && false == retry) addLocation(transactionState, location);
      } while(retryCount < retryTotal && retry == true && lockRetryCount < TransactionalTable.lockRetries);
    } catch (Throwable e) {
      if (LOG.isErrorEnabled()) LOG.error("ERROR while calling putTransactional ", e);
      throw new IOException("ERROR while calling putTransactional ", e);
    }    
    
      if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.put "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);      
    
    if(result == null) {
        throw new NotServingRegionException(retryErrMsg);
    } else if(result.hasException()) {
      if (result.getException().contains("LockTimeOutException")) {
        throw new LockTimeOutException(result.getException());
      } else if (result.getException().contains("DeadLockException")) {
          throw new DeadLockException(result.getException());
      } else if (result.getException().contains("LockNotEnoughResourcsException")) {
          throw new LockNotEnoughResourcsException(result.getException());
      }
      throw new IOException(result.getException());
    }
    
    // put is void, may not need to check result
    if (LOG.isTraceEnabled()) LOG.trace("TransactionalTable.put EXIT");
  }

   public synchronized void putRegionTx(final long tid, final Put put, final boolean autoCommit, final boolean recoveryToPitMode, final String queryContext) throws IOException{
        if (LOG.isTraceEnabled()) LOG.trace("TransactionalTable.putRegionTx ENTRY, autoCommit: "
               + autoCommit + " recoveryToPitMode: " + recoveryToPitMode);

      PutRegionTxResponse result = null; 
      clearStatsIfNewTrans(tid);
      try {
        int retryCount = 0;
        int retryTotal = TransactionalTable.retries; //default retry number
        boolean retry = false;
        boolean refresh = false;
        int lockRetryCount = 0;
        do {
          boolean isLockException = false;
          final String regionName = getConnection().getRegionLocator(this.getName())
                                    .getRegionLocation(put.getRow(), refresh).getRegionInfo().getRegionNameAsString();

          Batch.Call<TrxRegionService, PutRegionTxResponse> callable =
            new Batch.Call<TrxRegionService, PutRegionTxResponse>() {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<PutRegionTxResponse> rpcCallback =
                new BlockingRpcCallback<PutRegionTxResponse>();
            @Override
            public PutRegionTxResponse call(TrxRegionService instance) throws IOException {
              org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutRegionTxRequest.Builder builder = PutRegionTxRequest.newBuilder();
              builder.setTid(tid);
              if (recoveryToPitMode){
                 builder.setCommitId(tid);
              }   
              else{
                 builder.setCommitId(-1);
              }
              builder.setAutoCommit(autoCommit);
              builder.setRegionName(ByteString.copyFromUtf8(regionName));
              builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
              MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);
              builder.setPut(m1);

              instance.putRegionTx(controller, builder.build(), rpcCallback);
              return rpcCallback.get();
            }
          };

          refresh = false;
          long timeCost = System.currentTimeMillis();
          Iterator<Map.Entry<byte[], PutRegionTxResponse>> it= super.coprocessorService(TrxRegionService.class, 
                                                                                            put.getRow(), 
                                                                                            put.getRow(), 
                                                                                            callable)
                                                                                            .entrySet().iterator();
          if(it.hasNext()) {
            result = it.next().getValue();
            retry = false;
          }

          if (costTh >= 0 && result != null) {
            long cost1   = result.getCoproSTime() - timeCost;
            long cost3   = System.currentTimeMillis();
            timeCost     = cost3 - timeCost;
            costSum[10] += timeCost;
            callCount[10]++;
            if (timeCost >= costTh) {
              long cost2 = result.getCoproETime() - result.getCoproSTime();
              cost3 -= result.getCoproETime();
              LOG.warn("putRegionTx copro PID " + PID + " CC " + callCount[10] + " ATC " + (costSum[10] / callCount[10]) + " TTC " + costSum[10] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + this.getName());
            }
          }

          if(result == null || result.getException().contains("closing region")
                            || result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)
                            || isLockException(result.getException())) {
            if (result != null && result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
              if (LOG.isDebugEnabled())
                LOG.debug("TransactionalTable.putRegionTx will retry due to RegionNameMismatchException, region's name is: " + regionName);
              refresh = true;
            }
            else if (result != null && isLockException(result.getException())) {
                isLockException = true;
                lockRetryCount++;
            }
            else {
              Thread.sleep(TransactionalTable.delay);
            }
            retry = true;
            if (result == null || !isLockException)
                retryCount++;
          }

        } while(retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
      } catch (Throwable e) {
        if (LOG.isErrorEnabled()) LOG.error("ERROR while calling putRegionTx ", e);
        throw new IOException("ERROR while calling coprocessor in putRegionTx ", e);
      }    
      if(result == null) {
          throw new NotServingRegionException(retryErrMsg);
      } else if(result.hasException()) {
        if (result.getException().contains("LockTimeOutException")) {
          throw new LockTimeOutException(result.getException());
        } else if (result.getException().contains("DeadLockException")) {
            throw new DeadLockException(result.getException());
        } else if (result.getException().contains("LockNotEnoughResourcsException")) {
            throw new LockNotEnoughResourcsException(result.getException());
        }
        throw new IOException(result.getException());
      }

      // put is void, may not need to check result
      if (LOG.isTraceEnabled()) LOG.trace("TransactionalTable.putRegionTx EXIT");

  }

  public synchronized ResultScanner getScanner(final TransactionState transactionState, final long savepointId, final long pSavepointId, final int isolationLevel,
                                               final int lockMode, final boolean skipConflictAccess, final Scan scan, final String queryContext) throws IOException {

    if (LOG.isDebugEnabled()) LOG.debug("Enter TransactionalTable.getScanner "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName + " skipConflictAccess " + skipConflictAccess + " scan ");  
             
    if (scan.getCaching() <= 0) {
        scan.setCaching(getScannerCaching());
    }

    if (scan.getBatch() > 0 && scan.isSmall()) {
        LOG.error("getScanner for transaction " + transactionState.getTransactionId()
                + " Small scan should not be used with batching");
        throw new IllegalArgumentException("Small scan should not be used with batching");
    }
    Long value = (long) -1;
    TransactionalScanner scanner = new TransactionalScanner(this, transactionState, lockMode, skipConflictAccess, scan, value, savepointId, pSavepointId, isolationLevel, queryContext);
    if (LOG.isTraceEnabled()){
        String startRow = (Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW) ?
                "INFINITE" : Hex.encodeHexString(scan.getStartRow()));
        String stopRow = (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW) ?
                "INFINITE" : Hex.encodeHexString(scan.getStopRow()));
       LOG.trace("Exit TransactionalTable.getScanner for transaction "
            + transactionState.getTransactionId() + " scan startRow=" + startRow + ", stopRow=" + stopRow);
    }
    
      if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.getScanner "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);      
    
    return scanner;         
  }

  public boolean checkAndDeleteRegionTx(final long tid,	final byte[] row,
		  final byte[] family, final byte[] qualifier, final byte[] value,
          final Delete delete, final boolean autoCommit, final boolean recoveryToPitMode, final String queryContext) throws IOException {
    if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.checkAndDeleteRegionTx row: " + row
                + " family: " + family + " qualifier: " + qualifier + " value: " + value
                + " autoCommit: " + autoCommit + " recoveryToPitMode: " + recoveryToPitMode);
    if (!Bytes.equals(row, delete.getRow())) {
       throw new IOException("checkAndDeleteRegionTx action's getRow must match the passed row");
    }
    String regionName = getConnection().getRegionLocator(this.getName()).getRegionLocation(delete.getRow()).getRegionInfo().getRegionNameAsString();
    if(regionName == null) 
    	throw new IOException("Null regionName");

    CheckAndDeleteRegionTxResponse result = null;
    try {
       int retryCount = 0;
       int retryTotal = TransactionalTable.retries; //default retry number
       boolean retry = false;
       boolean refresh = false;
       int lockRetryCount = 0;

       clearStatsIfNewTrans(tid);
       do {
         boolean isLockException = false;
         final String rName = getConnection().getRegionLocator(this.getName())
                                      .getRegionLocation(delete.getRow(), refresh).getRegionInfo().getRegionNameAsString();
         Batch.Call<TrxRegionService, CheckAndDeleteRegionTxResponse> callable =
             new Batch.Call<TrxRegionService, CheckAndDeleteRegionTxResponse>() {
           ServerRpcController controller = new ServerRpcController();
           BlockingRpcCallback<CheckAndDeleteRegionTxResponse> rpcCallback =
             new BlockingRpcCallback<CheckAndDeleteRegionTxResponse>();

           public CheckAndDeleteRegionTxResponse call(TrxRegionService instance) throws IOException {
             org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRegionTxRequest.Builder builder = CheckAndDeleteRegionTxRequest.newBuilder();
             builder.setTid(tid);
             if (recoveryToPitMode){
               builder.setCommitId(tid);
             }
             else{
               builder.setCommitId(-1);
             }
             builder.setRegionName(ByteString.copyFromUtf8(rName));
             builder.setRow(HBaseZeroCopyByteString.wrap(row));
             builder.setAutoCommit(autoCommit);
             if(family != null)
               builder.setFamily(HBaseZeroCopyByteString.wrap(family));
             else
               builder.setFamily(HBaseZeroCopyByteString.wrap(new byte[]{}));

             if(qualifier != null)
               builder.setQualifier(HBaseZeroCopyByteString.wrap(qualifier));
             else
               builder.setQualifier(HBaseZeroCopyByteString.wrap(new byte[]{}));
             if(value != null)
               builder.setValue(HBaseZeroCopyByteString.wrap(value));
             else
               builder.setValue(HBaseZeroCopyByteString.wrap(new byte[]{}));

             MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, delete);
             builder.setDelete(m1);
             builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
             instance.checkAndDeleteRegionTx(controller, builder.build(), rpcCallback);
             return rpcCallback.get();
           }
         };

          refresh = false;
          long timeCost = System.currentTimeMillis();
          Iterator<Map.Entry<byte[], CheckAndDeleteRegionTxResponse>> it = super.coprocessorService(TrxRegionService.class, 
                         delete.getRow(), 
                         delete.getRow(), 
                         callable)
                        .entrySet()
                        .iterator();
          if(it.hasNext()) {
            result = it.next().getValue();
            retry = false;
          }

          if (costTh >= 0 && result != null) {
            long cost1 = result.getCoproSTime() - timeCost;
            long cost3 = System.currentTimeMillis();
            timeCost = cost3 - timeCost;
            costSum[11] += timeCost;
            callCount[11]++;
            if (timeCost >= costTh) {
              long cost2 = result.getCoproETime() - result.getCoproSTime();
              cost3 -= result.getCoproETime();
              LOG.warn("checkAndDeleteRegionTx copro PID " + PID + " CC " + callCount[11] + " ATC " + (costSum[11] / callCount[11]) + " TTC " + costSum[11] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + this.getName());
            }
          }

          if(result == null || result.getException().contains("closing region")
                            || result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)
                            || isLockException(result.getException())) {
            if (result != null && result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
              if (LOG.isDebugEnabled())
                LOG.debug("TransactionalTable.checkAndDeleteRegionTx will retry due to RegionNameMismatchException, region's name is: " + regionName);
              refresh = true;
            }
            else if (result != null && isLockException(result.getException())) {
                lockRetryCount++;
                isLockException = true;
            }
            else {
              Thread.sleep(TransactionalTable.delay);
            }
            retry = true;
            if (result == null || !isLockException)
                retryCount++;
          }
        } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
      } catch (Throwable e) {
        if (LOG.isErrorEnabled()) LOG.error("ERROR while calling checkAndDeleteRegionTx ",e);
        throw new IOException("ERROR while calling checkAndDeleteRegionTx",e);
      }
      if(result == null) {
          throw new NotServingRegionException(retryErrMsg);
      } else if(result.hasException()) {
        if (result.getException().contains("LockTimeOutException")) {
          throw new LockTimeOutException(result.getException());
        } else if (result.getException().contains("DeadLockException")) {
            throw new DeadLockException(result.getException());
        } else if (result.getException().contains("LockNotEnoughResourcsException")) {
            throw new LockNotEnoughResourcsException(result.getException());
        }
        throw new IOException(result.getException());
      }
      return result.getResult();
  }

  public boolean checkAndDelete(final TransactionState transactionState,
                      final long savepointId, final long pSavepointId,
                      final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value,
                      final Delete delete, final boolean skipCheck, final String queryContext) throws IOException {
    if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.checkAndDelete row: " + row
        + " family: " + family + " qualifier: " + qualifier + " value: " + value + " skipChek " + skipCheck);

    if (LOG.isDebugEnabled()) LOG.debug("Enter TransactionalTable.checkAndDelete "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);  
             
        
    if (!Bytes.equals(row, delete.getRow())) {
            throw new IOException("Action's getRow must match the passed row");
    }
    String regionName = getConnection().getRegionLocator(this.getName()).getRegionLocation(delete.getRow()).getRegionInfo().getRegionNameAsString();
    if(regionName == null) 
    	throw new IOException("Null regionName");

      CheckAndDeleteResponse result = null;
      try {
        int retryCount = 0;
        int retryTotal = TransactionalTable.retries; //default retry number
        boolean retry = false;
        boolean refresh = false;
        int lockRetryCount = 0;

        clearStatsIfNewTrans(transactionState.getTransactionId());
        do {
          boolean isLockException = false;
          final String rName = getConnection().getRegionLocator(this.getName())
                                       .getRegionLocation(delete.getRow(), refresh).getRegionInfo().getRegionNameAsString();
          Batch.Call<TrxRegionService, CheckAndDeleteResponse> callable =
              new Batch.Call<TrxRegionService, CheckAndDeleteResponse>() {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<CheckAndDeleteResponse> rpcCallback =
              new BlockingRpcCallback<CheckAndDeleteResponse>();

            @Override
            public CheckAndDeleteResponse call(TrxRegionService instance) throws IOException {
              org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRequest.Builder builder = CheckAndDeleteRequest.newBuilder();
              builder.setTransactionId(transactionState.getTransactionId());
              builder.setSavepointId(savepointId);
              builder.setPSavepointId(pSavepointId);
              builder.setStartId(transactionState.getStartId());
              builder.setRegionName(ByteString.copyFromUtf8(rName));
              builder.setRow(HBaseZeroCopyByteString.wrap(row));
              builder.setSkipCheck(skipCheck);
              builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
              if(family != null)
                builder.setFamily(HBaseZeroCopyByteString.wrap(family));
              else
                builder.setFamily(HBaseZeroCopyByteString.wrap(new byte[]{}));

              if(qualifier != null)
                builder.setQualifier(HBaseZeroCopyByteString.wrap(qualifier));
              else
                builder.setQualifier(HBaseZeroCopyByteString.wrap(new byte[]{}));
              if(value != null)
                builder.setValue(HBaseZeroCopyByteString.wrap(value));
              else
                builder.setValue(HBaseZeroCopyByteString.wrap(new byte[]{}));

              MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, delete);
              builder.setDelete(m1);

              instance.checkAndDelete(controller, builder.build(), rpcCallback);
              return rpcCallback.get();
            }
          };

          refresh = false;
          long timeCost = System.currentTimeMillis();
          Iterator<Map.Entry<byte[], CheckAndDeleteResponse>> it = super.coprocessorService(TrxRegionService.class, 
                                                                                            delete.getRow(), 
                                                                                            delete.getRow(), 
                                                                                            callable)
                                                                                            .entrySet()
                                                                                            .iterator();
          if(it.hasNext()) {
            result = it.next().getValue();
            retry = false;
          }

          if (costTh >= 0 && result != null) {
            long cost1   = result.getCoproSTime() - timeCost;
            long cost3   = System.currentTimeMillis();
            timeCost     = cost3 - timeCost;
            costSum[12] += timeCost;
            callCount[12]++;
            if (timeCost >= costTh) {
              long cost2 = result.getCoproETime() - result.getCoproSTime();
              cost3 -= result.getCoproETime();
              LOG.warn("checkAndDelete copro PID " + PID + " txID " + transactionState.getTransactionId() + " CC " + callCount[12] + " ATC " + (costSum[12] / callCount[12]) + " TTC " + costSum[12] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + this.getName());
            }
          }

          if(result == null || result.getException().contains("closing region")
                            || result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)
                            || result.getException().contains("NewTransactionStartedBefore")
                            || isLockException(result.getException())) {
            if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
               if (LOG.isTraceEnabled()) LOG.trace("CheckAndDelete retrying because region is recovering, "
                             + " transaction [" + transactionState.getTransactionId() + "]");

               Thread.sleep(TransactionalTable.regionNotReadyDelay);
            }
            else if (result != null && result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
              if (LOG.isDebugEnabled())
                LOG.debug("TransactionalTable.checkAndDelete will retry due to RegionNameMismatchException, region's name is: " + regionName);
              refresh = true;
            }
            else if (result != null && isLockException(result.getException())) {
                isLockException = true;
                lockRetryCount++;
                if (transactionState.getCancelOperation())
                    throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
            }
            else{
               Thread.sleep(TransactionalTable.delay);
            }
            retry = true;
            transactionState.setRetried(true);
            if (result == null || !isLockException)
                retryCount++;
          }
        } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
      } catch (Throwable e) {
        if (LOG.isErrorEnabled()) LOG.error("ERROR while calling checkAndDelete ",e);
        throw new IOException("ERROR while calling checkAndDelete ",e);
      }
      
      if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.checkAndDelete "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);        
      
      if(result == null) {
          throw new NotServingRegionException(retryErrMsg);
      } else if(result.hasException()) {
        if (result.getException().contains("LockTimeOutException")) {
          throw new LockTimeOutException(result.getException());
        } else if (result.getException().contains("DeadLockException")) {
            throw new DeadLockException(result.getException());
        } else if (result.getException().contains("LockNotEnoughResourcsException")) {
            throw new LockNotEnoughResourcsException(result.getException());
        }
        throw new IOException(result.getException());
      }
      return result.getResult();
   }
    
    public boolean checkAndPut(final TransactionState transactionState, final long savepointId, final long pSavepointId,
       	                       final byte[] row, final byte[] family, final byte[] qualifier,
                               final byte[] value, final Put put, final boolean skipCheck, final int nodeId, final String queryContext) throws IOException {

      if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.checkAndPut row: " + Hex.encodeHexString(row)
                                          + " put.row " + Hex.encodeHexString(put.getRow()) + " family: " + family + " qualifier: " + qualifier
                                          + " value: " + value + " skipCheck " + skipCheck);
      if (LOG.isDebugEnabled()) LOG.debug("Enter TransactionalTable.checkAndPut "
                                          + " transaction id " + transactionState.getTransactionId()
                                          + " table " + this.tName); 				
				
      if (!Bytes.equals(row, put.getRow())) {
        throw new IOException("Action's getRow must match the passed row");
      }

      CheckAndPutResponse result = null;
      try {
        int retryCount = 0;
        int retryTotal = TransactionalTable.retries; //default retry number
        boolean retry = false;
        boolean refresh = false;
        int lockRetryCount = 0;

        clearStatsIfNewTrans(transactionState.getTransactionId());
        do {
          boolean isLockException = false;
          final String regionName = getConnection().getRegionLocator(this.getName())
                                                   .getRegionLocation(put.getRow(), refresh).getRegionInfo().getRegionNameAsString();
          if (LOG.isTraceEnabled()) LOG.trace("checkAndPut, region name: " + regionName);

          Batch.Call<TrxRegionService, CheckAndPutResponse> callable =
              new Batch.Call<TrxRegionService, CheckAndPutResponse>() {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<CheckAndPutResponse> rpcCallback =
              new BlockingRpcCallback<CheckAndPutResponse>();

            @Override
            public CheckAndPutResponse call(TrxRegionService instance) throws IOException {
              org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRequest.Builder builder = CheckAndPutRequest.newBuilder();
              builder.setTransactionId(transactionState.getTransactionId());
              if (LOG.isTraceEnabled()) LOG.trace("checkAndPut, seting request startid: " + transactionState.getStartId());
              builder.setStartId(transactionState.getStartId());
              builder.setSavepointId(savepointId);
              builder.setPSavepointId(pSavepointId);
              builder.setRegionName(ByteString.copyFromUtf8(regionName));
              builder.setRow(HBaseZeroCopyByteString.wrap(row));
              builder.setSkipCheck(skipCheck);
                if( keepMutationRows == true)
                  builder.setKeepOldRow(true);
                else
                  builder.setKeepOldRow(false);

              if (LOG.isTraceEnabled()) LOG.trace("TransactionalTable.checkAndPut  transaction id: " + transactionState.getTransactionId() + "envdisable_unique: " + (envdisable_unique == null ? "null" : "not null") 
                  + "pidByte: " + Hex.encodeHexString(pidByte) + " nodeId: " + nodeId + " capValue: " + capValue);
              if (envdisable_unique == null) {
                  long innerCapValue = 0;
                  synchronized(TransactionalTable.class) {
                      innerCapValue = capValue;
                      capValue++;
                      if (capValue < 0) {
                          capValue = 0;
                      }
                  }
              byte[] uniqueByte = checkAndPutUniqueGenerate(nodeId, innerCapValue);
              if (LOG.isTraceEnabled()) LOG.trace("TransactionalTable.checkAndPut  transaction id: " + transactionState.getTransactionId() + "uniqueByte: " + Hex.encodeHexString(uniqueByte)
                  + " nodeId: " + nodeId + " capValue: " + capValue);
              builder.setCapValue(HBaseZeroCopyByteString.wrap(uniqueByte));
              } else {
                  builder.setCapValue(HBaseZeroCopyByteString.wrap(new byte[]{}));
              }
              builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
              if (family != null)
                builder.setFamily(HBaseZeroCopyByteString.wrap(family));
              else
                builder.setFamily(HBaseZeroCopyByteString.wrap(new byte[]{}));
              if (qualifier != null )
                builder.setQualifier(HBaseZeroCopyByteString.wrap(qualifier));
              else
                builder.setQualifier(HBaseZeroCopyByteString.wrap(new byte[]{}));
              if (value != null)
                builder.setValue(HBaseZeroCopyByteString.wrap(value));
              else
                builder.setValue(HBaseZeroCopyByteString.wrap(new byte[]{}));
              //Put p = new Put(ROW1);
              MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);
              builder.setPut(m1);

              instance.checkAndPut(controller, builder.build(), rpcCallback);
              return rpcCallback.get();
            }
          };

          refresh = false;
          long timeCost = System.currentTimeMillis();
          Iterator<Map.Entry<byte[], CheckAndPutResponse>> it = super.coprocessorService(TrxRegionService.class, 
                                                                                      put.getRow(), 
                                                                                      put.getRow(), 
                                                                                      callable)
                                                                                      .entrySet()
                                                                                      .iterator();
          if(it.hasNext()) {
            result = it.next().getValue();
            retry = false;
          }

          if (costTh >= 0 && result != null) {
            long cost1   = result.getCoproSTime() - timeCost;
            long cost3   = System.currentTimeMillis();
            timeCost     = cost3 - timeCost;
            costSum[13] += timeCost;
            callCount[13]++;
            if (timeCost >= costTh) {
              long cost2 = result.getCoproETime() - result.getCoproSTime();
              cost3 -= result.getCoproETime();
              LOG.warn("checkAndPut copro PID " + PID + " txID " + transactionState.getTransactionId() + " CC " + callCount[13] + " ATC " + (costSum[13] / callCount[13]) + " TTC " + costSum[13] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + this.getName());
            }
          }

          if(result == null || result.getException().contains("closing region")
                            || result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)
                            || result.getException().contains("NewTransactionStartedBefore")
                            || isLockException(result.getException())) {
            if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
               if (LOG.isTraceEnabled()) LOG.trace("CheckAndPut retrying because region is recovering ,"
                       + " transaction [" + transactionState.getTransactionId() + "]");

               Thread.sleep(TransactionalTable.regionNotReadyDelay);
            }
            else if (result != null && result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
              if (LOG.isDebugEnabled())
                LOG.debug("TransactionalTable.checkAndPut will retry due to RegionNameMismatchException, region's name is: " + regionName);
              refresh = true;
            }
            else if (result != null && isLockException(result.getException())) {
                isLockException = true;
                lockRetryCount++;
                if (transactionState.getCancelOperation())
                    throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
            }
            else {
               Thread.sleep(TransactionalTable.delay);
            }
            retry = true;
            transactionState.setRetried(true);
            if (result == null || !isLockException)
                retryCount++;
          }
        } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
      } catch (Throwable e) {
        if ((e instanceof OutOfMemoryError))
          {
            throw new OutOfMemoryError(e.getMessage());
          }
        else
          {
            if (LOG.isErrorEnabled()) LOG.error("ERROR while calling checkAndPut ",e);
            throw new IOException("ERROR while calling checkAndPut ",e);
          }
      }
      
      if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.checkAndPut "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);      

      if(result == null) {
          throw new NotServingRegionException(retryErrMsg);
      } else if(result.hasException()) {
        if (result.getException().contains("LockTimeOutException")) {
          throw new LockTimeOutException(result.getException());
        } else if (result.getException().contains("DeadLockException")) {
            throw new DeadLockException(result.getException());
        } else if (result.getException().contains("LockNotEnoughResourcsException")) {
            throw new LockNotEnoughResourcsException(result.getException());
        }
        throw new IOException(result.getException());
      }
      boolean retResult = result.getResult();
      if (retResult == false) {
          LOG.warn("TransactionalTable.checkAndPut row: " + " transaction id " + transactionState.getTransactionId() + ", row " + Bytes.toStringBinary(row) + ", row in hex " + Hex.encodeHexString(row) + " ,table " + this.tName);
      }

      return retResult;
    }

    /**
     * CheckAndPut to the table using a region transaction, not the DTM.
     * This is valid for single row, single table operations only.
     * After the checkAndPut operation the region performs conflict checking
     * and prepare processing automatically.  If the autoCommit flag is
     * true, the region also commits the region transaction before returning 
     * <p>
     * If autoFlush is false, the update is buffered.
     * 
     * @param tsId       // Id to be used by the region as a transId
     * @param row
     * @param family
     * @param qualifier
     * @param value
     * @param put
     * @param autoCommit // should the region transaction be committed 
     * @throws IOException
     * @since 2.1.0
     */
    public boolean checkAndPutRegionTx(final long tid, final byte[] row,
                                       final byte[] family, final byte[] qualifier, final byte[] value,
                                       final Put put, final boolean autoCommit, final boolean recoveryToPitMode, final String queryContext) throws IOException{
  
      if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.checkAndPutRegionTx row: " + row
                                           + " family: " + family + " qualifier: " + qualifier
                                           + " value: " + value + " autoCommit: " + autoCommit
                                           + " recoveryToPitMode: " + recoveryToPitMode);

      if (!Bytes.equals(row, put.getRow())) {
        throw new IOException("Action's getRow must match the passed row");
      }

      CheckAndPutRegionTxResponse result = null;
      try {
        int retryCount = 0;
        int retryTotal = TransactionalTable.retries; //default retry number
        boolean retry = false;
        boolean refresh = false;
        int lockRetryCount = 0;

        clearStatsIfNewTrans(tid);
        do {
          boolean isLockException = false;
          final String regionName = getConnection().getRegionLocator(this.getName()).
                                    getRegionLocation(put.getRow(), refresh).getRegionInfo().getRegionNameAsString();
          if (LOG.isTraceEnabled()) LOG.trace("checkAndPutRegionTx, region name: " + regionName);

          Batch.Call<TrxRegionService, CheckAndPutRegionTxResponse> callable =
              new Batch.Call<TrxRegionService, CheckAndPutRegionTxResponse>() {
                  ServerRpcController controller = new ServerRpcController();
                  BlockingRpcCallback<CheckAndPutRegionTxResponse> rpcCallback =
                    new BlockingRpcCallback<CheckAndPutRegionTxResponse>();

                  @Override
                  public CheckAndPutRegionTxResponse call(TrxRegionService instance) throws IOException {
                  org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRegionTxRequest.Builder builder = CheckAndPutRegionTxRequest.newBuilder();
                  builder.setTid(tid);
                  if (recoveryToPitMode){
                    builder.setCommitId(tid);
                  }
                  else{
                    builder.setCommitId(-1);
                  }
                  builder.setRegionName(ByteString.copyFromUtf8(regionName));
                  builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
                  builder.setRow(HBaseZeroCopyByteString.wrap(row));
                  if (family != null)
                    builder.setFamily(HBaseZeroCopyByteString.wrap(family));
                  else
                    builder.setFamily(HBaseZeroCopyByteString.wrap(new byte[]{}));
                  if (qualifier != null )
                    builder.setQualifier(HBaseZeroCopyByteString.wrap(qualifier));
                  else
                    builder.setQualifier(HBaseZeroCopyByteString.wrap(new byte[]{}));
                  if (value != null)
                    builder.setValue(HBaseZeroCopyByteString.wrap(value));
                  else
                    builder.setValue(HBaseZeroCopyByteString.wrap(new byte[]{}));
                  //Put p = new Put(ROW1);
                  MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);
                  builder.setPut(m1);
                  builder.setAutoCommit(autoCommit);

                  instance.checkAndPutRegionTx(controller, builder.build(), rpcCallback);
                  return rpcCallback.get();
                }
            };

          refresh = false;
          long timeCost = System.currentTimeMillis();
          Iterator<Map.Entry<byte[], CheckAndPutRegionTxResponse>> it = super.coprocessorService(TrxRegionService.class, 
                                                                                                 put.getRow(), 
                                                                                                 put.getRow(), 
                                                                                                 callable)
                                                                                                 .entrySet()
                                                                                                 .iterator();
          if(it.hasNext()) {
            result = it.next().getValue();
            retry = false;
          }

          if (costTh >= 0 && result != null) {
            long cost1   = result.getCoproSTime() - timeCost;
            long cost3   = System.currentTimeMillis();
            timeCost     = cost3 - timeCost;
            costSum[14] += timeCost;
            callCount[14]++;
            if (timeCost >= costTh) {
              long cost2 = result.getCoproETime() - result.getCoproSTime();
              cost3 -= result.getCoproETime();
              LOG.warn(this.getName() + " checkAndPutRegionTx copro PID " + " CC " + callCount[14] + " ATC " + (costSum[14] / callCount[14]) + " TTC " + costSum[14] + PID + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + this.getName());
            }
          }

          if(result == null || result.getException().contains("closing region")
                            || result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)
                            || isLockException(result.getException())) {
            if (result != null && result.getException().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
              if (LOG.isDebugEnabled())
                LOG.debug("TransactionalTable.checkAndPutRegionTx will retry due to RegionNameMismatchException, region's name is: " + regionName);
              refresh = true;
            }
            else if (result != null && isLockException(result.getException())) {
                isLockException = true;
                lockRetryCount++;
            }
            else {
              Thread.sleep(TransactionalTable.delay);
            }
            retry = true;
            if (result == null || !isLockException)
                retryCount++;
          }
        } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
      } catch (Throwable e) {
        if (LOG.isErrorEnabled()) LOG.error("ERROR while calling checkAndPutRegionTx ",e);
        if ((e instanceof OutOfMemoryError)){
          throw new OutOfMemoryError(e.getMessage());
        }
        else {
          throw new IOException("ERROR while calling checkAndPutRegionTx ",e);
        }
      }
      if(result == null) {
          throw new NotServingRegionException(retryErrMsg);
      } else if(result.hasException()) {
        if (result.getException().contains("LockTimeOutException")) {
          throw new LockTimeOutException(result.getException());
        } else if (result.getException().contains("DeadLockException")) {
          throw new DeadLockException(result.getException());
        } else if (result.getException().contains("LockNotEnoughResourcsException")) {
          throw new LockNotEnoughResourcsException(result.getException());
        }
        throw new IOException(result.getException());
      }

      boolean retResult = result.getResult();
      if (retResult == false) {
          LOG.warn("TransactionalTable.checkAndPutRegionTx row: " + " tid " + tid + ", row " + Bytes.toStringBinary(row) + ", row in hex " + Hex.encodeHexString(row) + " ,table " + this.tName);
      }
      return retResult;          
    }

   /**
    * @param transactionState
    * @param deletes
    * @throws IOException
    */
    public void delete(final TransactionState transactionState, final long savepointId, final long pSavepointId,
                        List<Delete> deletes, final boolean noConflictCheckForIndex, final String queryContext) throws IOException {
      int retryCount = 0;
      int retryTotal = TransactionalTable.retries; //default retry number
      boolean retry = false;
      int lockRetryCount = 0;

      do {
        boolean isLockException = false;
        try {
          delete(transactionState, savepointId, pSavepointId, deletes, noConflictCheckForIndex, retry, queryContext);
          retry = false;
        } catch (IOException ex) {
          String errorMsg = null;
          if (ex.getCause() != null) {
              errorMsg = ex.getCause().toString();
          } else {
              errorMsg = ex.getMessage();
          }
          if (retryCount < retryTotal
              && ex.getMessage().equals(REGION_NAME_MISMATCH_EXCEPTION)) {
            if (LOG.isDebugEnabled())
              LOG.debug("TransactionalTable.deletes will retry due to RegionNameMismatchException");
            retry = true;
          }
          else if (retryCount < retryTotal && isLockException(errorMsg)) {
              retry = true;
              isLockException = true;
              lockRetryCount++;
              if (transactionState.getCancelOperation())
                  throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
          }
          else {
            throw ex;
          }
          if (!isLockException) 
              retryCount++;
        }
      } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
    }

    public void delete(final TransactionState transactionState, final long savepointId, final long pSavepointId,
   			List<Delete> deletes, final boolean noConflictCheckForIndex, boolean refresh, final String queryContext) throws IOException {

       long transactionId = transactionState.getTransactionId();
       if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.deletes "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName + " list size: " + deletes.size());

       CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPoolPut);
       int loopCount = 0;
       int delError = 0;

       if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.delete[] <List> size: " + deletes.size() + ", transid: " + transactionId);
       // collect all rows from same region

       final Map<TransactionRegionLocation, List<Delete>> rows = new HashMap<TransactionRegionLocation, List<Delete>>();
       HRegionLocation hlocation = null;
       TransactionRegionLocation location = null;
       List<Delete> list = null;
       int size = 0;

       if (refresh) {
         this.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, refresh);
       }
       clearStatsIfNewTrans(transactionId);
       for (Delete del : deletes) {
          hlocation = this.getRegionLocation(del.getRow(), false);
          location = new TransactionRegionLocation(hlocation.getRegionInfo(), hlocation.getServerName());
          if (LOG.isTraceEnabled()){
             byte[] startKey = location.getRegionInfo().getStartKey();
             byte[] endKey = location.getRegionInfo().getEndKey();
             String sKey = ((startKey != null) ?
                      (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL");
             String eKey = ((endKey != null) ?
                      (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL");

             LOG.trace("delete <List> with trRegion ["
                    + location.getRegionInfo().getEncodedName()
                    + "], startKey " + sKey + ", endKey: " + eKey
                    + " and transaction [" + transactionId + "], get number: " + size);
          }

          if (!rows.containsKey(location)) {
             list = new ArrayList<Delete>();
             rows.put(location, list);
          } else {
             list = rows.get(location);
          }
          list.add(del);
          size++;
       }

       if (LOG.isTraceEnabled()) LOG.trace("Deletes region in parallel bucket size " + rows.size());
       for (Map.Entry<TransactionRegionLocation, List<Delete>> entry : rows.entrySet()) {
          final List<Delete> rowsInSameRegion = entry.getValue();
          final String regionName = entry.getKey().getRegionInfo().getRegionNameAsString();

          compPool.submit(new TransactionalTableDeletexCallable((org.apache.hadoop.hbase.client.transactional.TransactionalTable)this,transactionState, savepointId, pSavepointId,
                                   noConflictCheckForIndex, regionName, rowsInSameRegion, entry, location, queryContext) {
              public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("T")) {
					  Thread.currentThread().setName("TransactionalTable-" + Thread.currentThread().getName());
				  }

                 return doDeleteSetX();
              }
            });
          loopCount++;
	   }

       for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
          if (LOG.isDebugEnabled()) LOG.debug("doDeleteSetX results loopIndex " + loopIndex);
          boolean loopExit = false;
          do
          {
             try {
                 delError = compPool.take().get();
                 loopExit = true;
             }
             catch (Throwable e) {
                if (LOG.isErrorEnabled()) LOG.error("ERROR while calling deleteMultipleTransactional ",e);
                String errMsg = e.toString();
                if (errMsg.contains("LockTimeOutException")) {
                  throw new LockTimeOutException("ERROR while calling deleteMultipleTransactional " + errMsg);
                } else if (errMsg.contains("DeadLockException")) {
                    throw new DeadLockException("ERROR while calling deleteMultipleTransactional " + errMsg);
                } else if (errMsg.contains("LockNotEnoughResourcsException")) {
                    throw new LockNotEnoughResourcsException("ERROR while calling deleteMultipleTransactional " + errMsg);
                }

                if (e.getCause() instanceof IOException) {
                  throw ((IOException)e.getCause());
                }
                if (e instanceof IOException) {
                  throw ((IOException)e);
                }
                throw new IOException("ERROR while calling deleteMultipleTransactional ",e);
             }
          } while (loopExit == false);
        }

        if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.delete <list> "
               + " transaction id " + transactionState.getTransactionId()
               + " table " + this.tName);

   }


        /**
         * Put a set of rows region by region
         * 
         * @param transactionState
         * @param puts
         * @throws IOException
         */
        public void put(final TransactionState transactionState, final long savepointId, final long pSavepointId,
                        final List<Put> puts, final boolean noConflictCheckForIndex, final String queryContext) throws IOException {
          int retryCount = 0;
          int retryTotal = TransactionalTable.retries; //default retry number
          boolean retry = false;
          List<Put> putsDone = new ArrayList<Put>();
          int lockRetryCount = 0;

          do {
            boolean isLockException = false;
            try {
              put(transactionState, savepointId, pSavepointId, puts, noConflictCheckForIndex, putsDone, retry, queryContext);
              retry = false;
            } catch (IOException ex) {
              String errorMsg = null;
              if (ex.getCause() != null) {
                   errorMsg = ex.getCause().toString();
              } else {
                  errorMsg = ex.getMessage();
              }
              if (retryCount < retryTotal
                  && ex.getMessage().equals(REGION_NAME_MISMATCH_EXCEPTION)) {
                if (LOG.isDebugEnabled())
                  LOG.debug("TransactionalTable.puts will retry due to RegionNameMismatchException");
                retry = true;
                puts.removeAll(putsDone);
                putsDone.clear();
              }
              else if (retryCount < retryTotal && isLockException(errorMsg)) {
                  lockRetryCount++;
                  isLockException = true;
                  retry = true;
                  puts.removeAll(putsDone);
                  putsDone.clear();
                  if (transactionState.getCancelOperation())
                      throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
              }
              else {
                throw ex;
              }
              if (!isLockException)
                  retryCount++;
            }
          } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
        }

        private void put(final TransactionState transactionState, final long savepointId, final long pSavepointId,
                        final List<Put> puts, final boolean noConflictCheckForIndex, List<Put> putsDone, boolean refresh, final String queryContext) throws IOException {
                long transactionId = transactionState.getTransactionId();
                // collect all rows from same region
                
            if (LOG.isDebugEnabled()) LOG.debug("Enter TransactionalTable.puts "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName + " size: " + puts.size());                
                
                final Map<TransactionRegionLocation, List<Put>> rows = new HashMap<TransactionRegionLocation, List<Put>>();
                HRegionLocation hlocation = null;
                TransactionRegionLocation location = null;
                List<Put> list = null;
                int size = 0;

                if (refresh) {
                  this.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, refresh);
                }
                clearStatsIfNewTrans(transactionId);
                for (Put put : puts) {
                        hlocation = this.getRegionLocation(put.getRow(), false);
                        location = new TransactionRegionLocation(hlocation.getRegionInfo(), hlocation.getServerName());
                if (LOG.isTraceEnabled()) LOG.trace("put <List> with trRegion [" + location.getRegionInfo().getEncodedName() + "], endKey: "
                  + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " and transaction [" + transactionId + "], put number: " + size);
                        if (!rows.containsKey(location)) {
                if (LOG.isTraceEnabled()) LOG.trace("put adding new <List> for region [" + location.getRegionInfo().getRegionNameAsString() + "], endKey: "
                  + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " and transaction [" + transactionId + "], put number: " + size);
                                list = new ArrayList<Put>();
                                rows.put(location, list);
                        } else {
                                list = rows.get(location);
                        }
                        list.add(put);
      size++;
                }

                if (LOG.isTraceEnabled()) LOG.trace("Puts region by region bucket size " + rows.size());                
                for (Map.Entry<TransactionRegionLocation, List<Put>> entry : rows.entrySet()) {
                        final List<Put> rowsInSameRegion = entry.getValue();
                        final String regionName = entry.getKey().getRegionInfo().getRegionNameAsString();
                        Batch.Call<TrxRegionService, PutMultipleTransactionalResponse> callable =
                new Batch.Call<TrxRegionService, PutMultipleTransactionalResponse>() {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<PutMultipleTransactionalResponse> rpcCallback =
                new BlockingRpcCallback<PutMultipleTransactionalResponse>();

              @Override
              public PutMultipleTransactionalResponse call(TrxRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutMultipleTransactionalRequest.Builder builder = PutMultipleTransactionalRequest.newBuilder();
                builder.setTransactionId(transactionState.getTransactionId());
                builder.setStartId(transactionState.getStartId());
                builder.setSavepointId(savepointId);
                builder.setPSavepointId(pSavepointId);
                builder.setRegionName(ByteString.copyFromUtf8(regionName));
                builder.setNoConflictCheckForIndex(noConflictCheckForIndex);
                builder.setQueryContext(ByteString.copyFromUtf8(queryContext));

                if( keepMutationRows == true)
                  builder.setKeepOldRow(true);
                else
                  builder.setKeepOldRow(false);

                for (Put put : rowsInSameRegion){
                  MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);
                  builder.addPut(m1);
                }

                instance.putMultiple(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
              }
            };
            PutMultipleTransactionalResponse result = null;
      try {
        int retryCount = 0;
        int retryTotal = TransactionalTable.retries; //default retry number
        boolean retry = false;
        int lockRetryCount = 0;
        do {
          boolean isLockException = false;
          long timeCost = System.currentTimeMillis();
          Iterator<Map.Entry<byte[], PutMultipleTransactionalResponse>> it= super.coprocessorService(TrxRegionService.class, 
                                            entry.getValue().get(0).getRow(),
                                            entry.getValue().get(0).getRow(),                                             
                                            callable)
                                            .entrySet().iterator();
          if(it.hasNext()) {
            result = it.next().getValue();
            retry = false;
          }

          if (costTh >= 0 && result != null) {
            long cost1   = result.getCoproSTime() - timeCost;
            long cost3   = System.currentTimeMillis();
            timeCost     = cost3 - timeCost;
            costSum[15] += timeCost;
            callCount[15]++;
            if (timeCost >= costTh) {
              long cost2 = result.getCoproETime() - result.getCoproSTime();
              cost3 -= result.getCoproETime();
              LOG.warn("multiple put copro PID " + PID + " txID " + transactionState.getTransactionId() + " CC " + callCount[15] + " ATC " + (costSum[15] / callCount[15]) + " TTC " + costSum[15] + " TC " + timeCost + " " + cost1 + " " + cost2 + " " + cost3 + " " + this.getName());
            }
          }

          if(result == null || result.getException().contains("closing region")
                       || result.getException().contains("NewTransactionStartedBefore")
                       || isLockException(result.getException())) {
            if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
               if (LOG.isTraceEnabled()) LOG.trace("put <List> retrying because region is recovering trRegion ["
                      + location.getRegionInfo().getEncodedName() + "], endKey: "
                      + Hex.encodeHexString(location.getRegionInfo().getEndKey())
                      + " and transaction [" + transactionId + "]");

               Thread.sleep(TransactionalTable.regionNotReadyDelay);
            }
            else if (result != null && isLockException(result.getException())) {
                lockRetryCount++;
                isLockException = true;
                if (transactionState.getCancelOperation())
                    throw new LockCancelOperationException("transaction " + transactionId + " is canceled!");
            }
            else{
                Thread.sleep(TransactionalTable.delay);
            }
            retry = true;
            transactionState.setRetried(true);
            if (result == null || !isLockException)
                retryCount++;
          }
        } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
      } catch (Throwable e) {
        if (LOG.isErrorEnabled()) LOG.error("ERROR while calling putMultipleTransactional ",e);
        throw new IOException("ERROR while calling putMultipleTransactional ",e);
      }
      
      if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.puts "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);       
      
      if(result == null) {
          throw new NotServingRegionException(retryErrMsg);
      } else if (result.hasException()) {
        if (result.getException().contains("LockTimeOutException")) {
          throw new LockTimeOutException(result.getException());
        } else if (result.getException().contains("DeadLockException")) {
            throw new DeadLockException(result.getException());
        } else if (result.getException().contains("LockNotEnoughResourcsException")) {
            throw new LockNotEnoughResourcsException(result.getException());
        }
        throw new IOException(result.getException());
      }
      putsDone.addAll(rowsInSameRegion);
     }
                }

   	/**
	 * Put a set of rows regios in parallel
	 * 
	 * @param transactionState
	 * @param puts
	 * @throws IOException
	 */
        public void put_rowset(final TransactionState transactionState, final long savepointId, final long pSavepointId,
                        final List<Put> puts, final String queryContext) throws IOException {
          int retryCount = 0;
          int retryTotal = TransactionalTable.retries; //default retry number
          boolean retry = false;
          List<Put> putsDone = new ArrayList<Put>();
          int lockRetryCount = 0;

          do {
            boolean isLockException = false;
            try {
              put_rowset(transactionState, savepointId, pSavepointId, puts, putsDone, retry, queryContext);
              retry = false;
            } catch (IOException ex) {
              String errorMsg = null;
              if (ex.getCause() != null) {
                  errorMsg = ex.getCause().toString();
              } else {
                    errorMsg = ex.getMessage();
              }
              if (retryCount < retryTotal
                  && ex.getMessage().equals(REGION_NAME_MISMATCH_EXCEPTION)) {
                if (LOG.isDebugEnabled())
                  LOG.debug("TransactionalTable.put_rowset will retry due to RegionNameMismatchException");
                retry = true;
                puts.removeAll(putsDone);
                putsDone.clear();
              }
              else if (retryCount < retryTotal && isLockException(errorMsg)) {
                  lockRetryCount++;
                  isLockException = true;
                  retry = true;
                  puts.removeAll(putsDone);
                  putsDone.clear();
                  if (transactionState.getCancelOperation())
                      throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
              }
              else {
                throw ex;
              }
              if (!isLockException)
                  retryCount++;
            }
          } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
        }

	private void put_rowset(final TransactionState transactionState, final long savepointId, final long pSavepointId,
			final List<Put> puts, List<Put> putsDone, boolean refresh, final String queryContext) throws IOException {

       long transactionId = transactionState.getTransactionId();
                
            if (LOG.isDebugEnabled()) LOG.debug("Enter TransactionalTable.put_rowset "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName + " list size: " + puts.size());
             
  CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPoolPut);
  int loopCount = 0;
  int putError = 0;
		if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.put[] <List> size: " + puts.size() + ", transid: " + transactionId);
		// collect all rows from same region
		final Map<TransactionRegionLocation, List<Put>> rows = new HashMap<TransactionRegionLocation, List<Put>>();
		HRegionLocation hlocation = null;
                TransactionRegionLocation location = null;
		List<Put> list = null;
                int size = 0;

                if (refresh) {
                  this.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, refresh);
                }
                clearStatsIfNewTrans(transactionId);
		for (Put put : puts) {
			hlocation = this.getRegionLocation(put.getRow(), false);
                        location = new TransactionRegionLocation(hlocation.getRegionInfo(), hlocation.getServerName());
                if (LOG.isTraceEnabled()) LOG.trace("put <List> with trRegion [" + location.getRegionInfo().getEncodedName() + "], endKey: "
                  + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " and transaction [" + transactionId + "], put number: " + size);
			if (!rows.containsKey(location)) {
                if (LOG.isTraceEnabled()) LOG.trace("put adding new <List> for region [" + location.getRegionInfo().getRegionNameAsString() + "], endKey: "
                  + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " and transaction [" + transactionId + "], put number: " + size);
				list = new ArrayList<Put>();
				rows.put(location, list);
			} else {
				list = rows.get(location);
			}
			list.add(put);
      size++;
		}

    if (LOG.isTraceEnabled()) LOG.trace("Puts region in parallel bucket size " + rows.size());  
    Map<TransactionRegionLocation, List<Put>> rowsDone = new HashMap<TransactionRegionLocation, List<Put>>();
    for (Map.Entry<TransactionRegionLocation, List<Put>> entry : rows.entrySet()) {
     final List<Put> rowsInSameRegion = entry.getValue();
     final String regionName = entry.getKey().getRegionInfo().getRegionNameAsString();
     List<Put> tmpList = new ArrayList<Put>();

     rowsDone.put(entry.getKey(), tmpList);
     compPool.submit(new TransactionalTableCallable((org.apache.hadoop.hbase.client.transactional.TransactionalTable)this,transactionState, savepointId, pSavepointId, regionName, rowsInSameRegion,
                     entry, location, transactionId, tmpList, queryContext) {
          public Integer call() throws IOException {
              if (LOG.isDebugEnabled()) LOG.debug("before doPutX() : transactionState " + transactionState +
                      "savepointId " + savepointId + "regionName " + regionName + "rowsInSameRegion " + rowsInSameRegion +
                      "location " + location + "transactionId " + transactionId);
				  if (!Thread.currentThread().getName().startsWith("T")) {
					  Thread.currentThread().setName("TransactionalTable-" + Thread.currentThread().getName());
				  }

                 return doPutX();
              }
      });


      loopCount++;
    }

     IOException nameMismatchEx = null;
     for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
         if (LOG.isDebugEnabled()) LOG.debug("doPutX results loopIndex " + loopIndex);
         boolean loopExit = false;
         do
         {
            try {
                putError = compPool.take().get();
                loopExit = true; 
            } 
            catch (Throwable e) {
               if (e.getCause() instanceof IOException
                   && e.getCause().getMessage().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
                  nameMismatchEx = (IOException)e.getCause();
                  loopExit = true;
               }
               else if (e instanceof IOException
                   && e.getMessage().contains(REGION_NAME_MISMATCH_EXCEPTION)) {
                  nameMismatchEx = (IOException)e;
                  loopExit = true;
               }
               else {
                  if (LOG.isErrorEnabled()) LOG.error("ERROR while calling putMultipleTransactional ",e);
                  throw new IOException("ERROR while calling putMultipleTransactional ",e);
               }
            }
         } while (loopExit == false);
       }

      if (null != nameMismatchEx) {
        for (Map.Entry<TransactionRegionLocation, List<Put>> entry : rowsDone.entrySet()) {
          List<Put> rowsInRegion = entry.getValue();
          putsDone.addAll(rowsInRegion);
        }
        throw nameMismatchEx;
      }
 
      if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.put_rowset "
             + " transaction id " + transactionState.getTransactionId()
             + " table " + this.tName);        
       
    }

  public boolean lockRequired(final TransactionState transactionState, final long savepointId, final long pSavepointId, String tableName, final int lockMode, boolean registerRegion, final String queryContext) throws IOException {
    try {
      if (registerRegion) {
        //this.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, true);
        List<HRegionLocation> regionList = connection.getRegionLocator(this.getName()).getAllRegionLocations();

        transactionState.clearParticipatingRegions();
        for (HRegionLocation regionLocation : regionList) {
          final String regionName = regionLocation.getRegionInfo().getRegionNameAsString();
          if (!regionName.contains(tableName)) {
              continue;
          } 
          TransactionRegionLocation transactionRegionLocation = new TransactionRegionLocation(regionLocation.getRegionInfo(), regionLocation.getServerName());
          addLocation(transactionState, transactionRegionLocation);
          transactionState.registerLocation(transactionRegionLocation);
          
          if(LOG.isTraceEnabled()) LOG.trace("lockRequired registerLocation regionName:" + regionName + " regionLocation:" + regionLocation.getRegionInfo().getEncodedName());
        }
      }

      Batch.Call<TrxRegionService, LockRequiredResponse> callable =
          new Batch.Call<TrxRegionService, LockRequiredResponse>() {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<LockRequiredResponse> rpcCallback =
                new BlockingRpcCallback<LockRequiredResponse>();
            @Override
            public LockRequiredResponse call(TrxRegionService instance) throws IOException {
              org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.LockRequiredRequest.Builder builder = LockRequiredRequest.newBuilder();
              //              builder.setRegionName(ByteString.copyFromUtf8(regionName));
              builder.setLockMode(lockMode);
              builder.setTransactionId(transactionState.getTransactionId());
              builder.setSavepointId(savepointId);
              builder.setPSavepointId(pSavepointId);
              builder.setQueryContext(ByteString.copyFromUtf8(queryContext));
              instance.lockRequired(controller, builder.build(), rpcCallback);
              return rpcCallback.get();
            }
          };

      LockRequiredResponse result = null;

      int retryCount = 0;
      int retryTotal = TransactionalTable.retries; //default retry number
      boolean retry = false;
      int lockRetryCount = 0;
      do {
        boolean isLockException = false;
        Iterator<Map.Entry<byte[], LockRequiredResponse>> it = super.coprocessorService(TrxRegionService.class,
                                                                                        HConstants.EMPTY_START_ROW,
                                                                                        HConstants.EMPTY_END_ROW,
                                                                                        callable).entrySet().iterator();
        int resultCount = 0;
        while (it.hasNext()) {
          resultCount++;
          result = it.next().getValue();
          if (LOG.isTraceEnabled()) LOG.trace("lockRequiredRequest result " + resultCount + " is " + result );
          retry = false;
          
          if(result == null || result.getException().contains("closing region")
             || result.getException().contains("NewTransactionStartedBefore")
             || isLockException(result.getException())) {
            if (result != null && result.getException().contains("NewTransactionStartedBefore")) {
              if (LOG.isTraceEnabled()) LOG.trace("lockRequired retrying because region is recovering ,"
                                                  + " transaction [" + transactionState.getTransactionId() + "]");
              
              Thread.sleep(TransactionalTable.regionNotReadyDelay);
            }
            else if (result != null && isLockException(result.getException())) {
                isLockException = true;
                lockRetryCount++;
                if (transactionState.getCancelOperation())
                    throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
            }
            else{
              Thread.sleep(TransactionalTable.delay);
            }
            retry = true;
            transactionState.setRetried(true);
            if (result == null || !isLockException)
                retryCount++;
          }
        }
      } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
      
      if(result == null) {
          throw new IOException(retryErrMsg);
      } else if (result.hasException()) {
        if (result.getException().contains("LockTimeOutException")) {
          throw new LockTimeOutException(result.getException());
        } else if (result.getException().contains("DeadLockException")) {
            throw new DeadLockException(result.getException());
        } else if (result.getException().contains("LockNotEnoughResourcsException")) {
            throw new LockNotEnoughResourcsException(result.getException());
        }
        throw new IOException(result.getException());
      }
      
    } catch (LockTimeOutException ltoe) {
      if (LOG.isErrorEnabled()) LOG.error("ERROR while calling lockRequired ", ltoe);
      throw new LockTimeOutException("ERROR while calling lockRequired ");
    } catch (DeadLockException ltoe) {
        if (LOG.isErrorEnabled()) LOG.error("ERROR while calling lockRequired ", ltoe);
        throw new DeadLockException("ERROR while calling lockRequired ");
    } catch (LockNotEnoughResourcsException ltoe) {
        if (LOG.isErrorEnabled()) LOG.error("ERROR while calling lockRequired ", ltoe);
            throw new LockNotEnoughResourcsException("ERROR while calling lockRequired ");
    } catch (Throwable e) {
      if (LOG.isErrorEnabled()) LOG.error("ERROR while calling lockRequired ",e);
            throw new IOException("ERROR while calling lockRequired ",e);
    }
    return true;
  }
  
        /**
         * Put a set of rows nontransactionally regios in parallel
         * 
         * @param puts
         * @throws IOException
         */
        public void put_nontxn_rowset(final long nonTransactionId, final long commitId, final long flags, final List<Put> puts) throws IOException {
          int retryCount = 0;
          int retryTotal = TransactionalTable.retries; //default retry number
          boolean retry = false;
          int lockRetryCount = 0;

          do {
            boolean isLockException = false;
            try {
              put_nontxn_rowset(nonTransactionId, commitId, flags, puts, retry);
              retry = false;
            } catch (IOException ex) {
              String errorMsg = null;
              if (ex.getCause() != null) {
                  errorMsg = ex.getCause().toString();
              } else {
                  errorMsg = ex.getMessage();
              }
              if (retryCount < retryTotal
                  && ex.getMessage().equals(REGION_NAME_MISMATCH_EXCEPTION)) {
                if (LOG.isDebugEnabled())
                  LOG.debug("TransactionalTable.put_nontxn_rowset[] will retry due to RegionNameMismatchException");
                retry = true;
              }
              else if (retryCount < retryTotal && isLockException(errorMsg)) {
                  lockRetryCount++;
                  isLockException = true;
                  retry = true;
              }
              else {
                throw ex;
              }
              if (!isLockException)
                  retryCount++;
            }
          } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
        }

        private void put_nontxn_rowset(final long nonTransactionId, final long commitId, final long flags,
                                       final List<Put> puts, boolean refresh) throws IOException {

                CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPoolPut);
                int loopCount = 0;
                int putError = 0;
                if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.put_nontxn_rowset[] <List> size: " + puts.size() 
                            + ", nonTransid: " + nonTransactionId + " flags: " + flags);
                // collect all rows from same region
                final Map<TransactionRegionLocation, List<Put>> rows = new HashMap<TransactionRegionLocation, List<Put>>();
                HRegionLocation hlocation = null;
                TransactionRegionLocation location = null;
                List<Put> list = null;
                int size = 0;

                if (refresh) {
                  this.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, refresh);
                }
                clearStatsIfNewTrans(nonTransactionId);
                for (Put put : puts) {
                        hlocation = this.getRegionLocation(put.getRow(), false);
                        location = new TransactionRegionLocation(hlocation.getRegionInfo(), hlocation.getServerName());
                        if (LOG.isTraceEnabled()) LOG.trace("put <List> with trRegion [" + location.getRegionInfo().getEncodedName() + "], endKey: "
                                     + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " and transaction [" + nonTransactionId + "], put number: " + size);
                        if (!rows.containsKey(location)) {
                               if (LOG.isTraceEnabled()) LOG.trace("put adding new <List> for region [" + location.getRegionInfo().getRegionNameAsString() + "], endKey: "
                                                + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " and transaction [" + nonTransactionId + "], put number: " + size);
                                list = new ArrayList<Put>();
                                rows.put(location, list);
                        } else {
                                list = rows.get(location);
                        }
                        list.add(put);
                       size++;
                }

                if (LOG.isTraceEnabled()) LOG.trace("Non Txn Puts region in parallel bucket size " + rows.size());  

                int totalNum = rows.size();

                for (Map.Entry<TransactionRegionLocation, List<Put>> entry : rows.entrySet()) {
                        final List<Put> rowsInSameRegion = entry.getValue();
                        final String regionName = entry.getKey().getRegionInfo().getRegionNameAsString();

                        compPool.submit(new TransactionalTablePutNonTxnCallable((org.apache.hadoop.hbase.client.transactional.TransactionalTable)this,
                                          rowsInSameRegion, entry, location, regionName, nonTransactionId, commitId, flags, totalNum) {
                            public Integer call() throws IOException {
								if (!Thread.currentThread().getName().startsWith("T")) {
									Thread.currentThread().setName("TransactionalTable-" + Thread.currentThread().getName());
								}

                                       if (LOG.isDebugEnabled()) LOG.debug("before doPutNonX() : rowsInSameRegion " + rowsInSameRegion +
                                               "entry " + entry + "location " + location + " nonTransactionId " + nonTransactionId + " commitId " + commitId + " flags " + flags);
                                       return doPutNonX();
                            }
                        });

                        loopCount++;
                  }

                  for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
                         if (LOG.isDebugEnabled()) LOG.debug("doPutNonX results loopCount " + loopCount);
                         boolean loopExit = false;
                         do
                         {
                                 try {
                                     putError = compPool.take().get();
                                     loopExit = true; 
                                  } 
                                 catch (Throwable e) {
                                     if (LOG.isErrorEnabled()) LOG.error("ERROR while calling putMultipleNonTransactional ",e);
                                     if (e.getCause() instanceof IOException)
                                       throw ((IOException)e.getCause());
                                     if (e instanceof IOException)
                                       throw (IOException)e;
                                     throw new IOException("ERROR while calling putMultipleNonTransactional ",e);
                                 }
                          } while (loopExit == false);
                   }
    }

        /**
         * Delete a set of rows nontransactionally regios in parallel
         * 
         * @param puts
         * @throws IOException
         */
        public void delete_nontxn_rowset(final long nonTransactionId, final long commitId, final long flags, final List<Delete> deletes) throws IOException {
          int retryCount = 0;
          int retryTotal = TransactionalTable.retries; //default retry number
          boolean retry = false;
          int lockRetryCount = 0;

          do {
            boolean isLockException = false;
            try {
              delete_nontxn_rowset(nonTransactionId, commitId, flags, deletes, retry);
              retry = false;
            } catch (IOException ex) {
              String errorMsg = null;
              if (ex.getCause() != null) {
                  errorMsg = ex.getCause().toString();
              } else {
                  errorMsg = ex.getMessage();
              }
              if (retryCount < retryTotal
                  && ex.getMessage().equals(REGION_NAME_MISMATCH_EXCEPTION)) {
                if (LOG.isDebugEnabled())
                  LOG.debug("TransactionalTable.delete_nontxn_rowset[] will retry due to RegionNameMismatchException");
                retry = true;
              }
              else if (retryCount < retryTotal && isLockException(errorMsg)) {
                  lockRetryCount++;
                  isLockException = true;
                  retry = true;
              }
              else {
                throw ex;
              }
              if (!isLockException)
                  retryCount++;
            }
          } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);
        }

        private void delete_nontxn_rowset(final long nonTransactionId, final long commitId, final long flags, final List<Delete> deletes, boolean refresh) throws IOException {

                CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPoolPut);
                int loopCount = 0;
                int putError = 0;
                if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.delete_nontxn_rowset[] <List> size: " + deletes.size() 
                            + ", nonTransid: " + nonTransactionId + " flags: " + flags);
                // collect all rows from same region
                final Map<TransactionRegionLocation, List<Delete>> rows = new HashMap<TransactionRegionLocation, List<Delete>>();
                HRegionLocation hlocation = null;
                TransactionRegionLocation location = null;
                List<Delete> list = null;
                int size = 0;

                if (refresh) {
                   this.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, refresh);
                }
                clearStatsIfNewTrans(nonTransactionId);
                for (Delete del:deletes) {
                        hlocation = this.getRegionLocation(del.getRow(), false);
                        location = new TransactionRegionLocation(hlocation.getRegionInfo(), hlocation.getServerName());
                        if (LOG.isTraceEnabled()) LOG.trace("delete <List> with trRegion [" + location.getRegionInfo().getEncodedName() + "], endKey: "
                                     + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " and transaction [" + nonTransactionId + "], delete number: " + size);
                        if (!rows.containsKey(location)) {
                               if (LOG.isTraceEnabled()) LOG.trace("delete adding new <List> for region [" + location.getRegionInfo().getRegionNameAsString() + "], endKey: "
                                                + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " and transaction [" + nonTransactionId + "], delete number: " + size);
                                list = new ArrayList<Delete>();
                                rows.put(location, list);
                        } else {
                                list = rows.get(location);
                        }
                        list.add(del);
                       size++;
                }

                if (LOG.isTraceEnabled()) LOG.trace("Non Txn Deletes region in parallel bucket size " + rows.size());  
                for (Map.Entry<TransactionRegionLocation, List<Delete>> entry : rows.entrySet()) {
                        final List<Delete> rowsInSameRegion = entry.getValue();
                        final String regionName = entry.getKey().getRegionInfo().getRegionNameAsString();

                        compPool.submit(new TransactionalTableDeleteNonTxnCallable((org.apache.hadoop.hbase.client.transactional.TransactionalTable)this,
                                          rowsInSameRegion, entry, location, regionName, nonTransactionId, commitId, flags) {
                            public Integer call() throws IOException {
								if (!Thread.currentThread().getName().startsWith("T")) {
									Thread.currentThread().setName("TransactionalTable-" + Thread.currentThread().getName());
								}

                                       if (LOG.isDebugEnabled()) LOG.debug("before doDeleteNonX() : rowsInSameRegion " + rowsInSameRegion +
                                               "entry " + entry + "location " + location + " nonTransactionId " + nonTransactionId + " commitId " + commitId + " flags " + flags);
                                       return doDeleteNonX();
                            }
                        });

                        loopCount++;
                  }

                  for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
                         if (LOG.isDebugEnabled()) LOG.debug("doDeleteNonX results loopCount " + loopCount);
                         boolean loopExit = false;
                         do
                         {
                                 try {
                                     putError = compPool.take().get();
                                     loopExit = true; 
                                  } 
                                 catch (Throwable e) {
                                     if (LOG.isErrorEnabled()) LOG.error("ERROR while calling deleteMultipleNonTransactional ",e);
                                     if (e.getCause() instanceof IOException)
                                       throw ((IOException)e.getCause());
                                     if (e instanceof IOException)
                                       throw (IOException)e;
                                     throw new IOException("ERROR while calling deleteMultipleNonTransactional ",e);
                                   }
                          } while (loopExit == false);
                   }
    }

    /**
     * Get a set of rows from regions in parallel
     *
     * @param transactionState
     * @param gets
     * @throws IOException
     */
    public Result[] get_rowset(final TransactionState transactionState,
                    final long savepointId, final long pSavepointId,
                    final int isolationLevel, final int lockMode,
                    final boolean waitOnSelectForUpdate, final boolean skipReadConflict, final List<Get> gets, final String queryContext) throws IOException {
      int retryCount = 0;
      boolean retry = false;
      int retryTotal = TransactionalTable.retries; //default retry number
      Result [] returnResults = null;
      int lockRetryCount = 0;

      do {
        boolean isLockException = false;
        try {
          returnResults = get_rowset(transactionState, savepointId, pSavepointId, isolationLevel,
                                    lockMode, waitOnSelectForUpdate, skipReadConflict,
                                    gets, retry, queryContext);
          retry = false;
        } catch (IOException ex) {
          String errorMsg = null;
          if (ex.getCause() != null) {
              errorMsg = ex.getCause().toString();
          } else {
              errorMsg = ex.getMessage();
          }
          if (retryCount < retryTotal
              && ex.getMessage().equals(REGION_NAME_MISMATCH_EXCEPTION)) {
            if (LOG.isDebugEnabled())
                 LOG.debug("TransactionalTable.get_rowset will retry due to RegionNameMismatchException");
            retry = true;
          }
          else if (retryCount < retryTotal && isLockException(errorMsg)) {
              lockRetryCount++;
              isLockException = true;
              retry = true;
              if (transactionState.getCancelOperation())
                  throw new LockCancelOperationException("transaction " + transactionState.getTransactionId() + " is canceled!");
          }
          else {
            throw ex;
          }
          if (!isLockException)
              retryCount++;
        }
      } while (retryCount < retryTotal && retry == true && lockRetryCount < lockRetries);

      return returnResults;
    }

    private Result[] get_rowset(final TransactionState transactionState, final long savepointId, final long pSavepointId, final int isolationLevel, final int lockMode,
                    final boolean waitOnSelectForUpdate, final boolean skipReadConflict, final List<Get> gets, boolean refresh, final String queryContext) throws IOException {

      long transactionId = transactionState.getTransactionId();
      if (LOG.isDebugEnabled()) LOG.debug("Enter TransactionalTable.get_rowset "
            + " transaction id " + transactionState.getTransactionId()
            + " savepointId " + savepointId + " waitOnSelectForUpdate " + waitOnSelectForUpdate
            + " isolationLevel " + isolationLevel + " lockMode "
            + " table " + this.tName + " list size: " + gets.size());

      CompletionService<GetMultipleTransactionalResponse> compPool = new ExecutorCompletionService<GetMultipleTransactionalResponse>(threadPoolPut);
      int loopCount = 0;
      if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.get[] <List> size: " + gets.size() + ", transid: " + transactionId);

      // collect all rows from same region
      final Map<TransactionRegionLocation, List<Get>> locMap = new HashMap<TransactionRegionLocation, List<Get>>();
      HRegionLocation hlocation = null;
      TransactionRegionLocation location = null;
      List<Get> list = null;
      int size = 0;

      if (refresh) {
         this.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, refresh);
      }
      clearStatsIfNewTrans(transactionId);
      for (Get get : gets) {
         hlocation = this.getRegionLocation(get.getRow(), false);
         location = new TransactionRegionLocation(hlocation.getRegionInfo(), hlocation.getServerName());
         if (LOG.isTraceEnabled()){
             byte[] startKey = location.getRegionInfo().getStartKey();
             byte[] endKey = location.getRegionInfo().getEndKey();
             String sKey = ((startKey != null) ?
                     (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL");
             String eKey = ((endKey != null) ?
                     (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL");

             LOG.trace("get_rowset <List> with trRegion ["
                   + location.getRegionInfo().getEncodedName()
                   + "], startKey " + sKey + ", endKey: " + eKey
                   + " and transaction [" + transactionId + "], get number: " + size);
         }
         if (!locMap.containsKey(location)) {
            if (LOG.isTraceEnabled()) LOG.trace("get_rowset adding new <List> for region [" + location.getRegionInfo().getRegionNameAsString() + "], endKey: "
                  + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " and transaction [" + transactionId + "], get number: " + size);
            list = new ArrayList<Get>();
            locMap.put(location, list);
         } else {
            list = locMap.get(location);
         }
         list.add(get);
         size++;
      }

      if (LOG.isTraceEnabled()) LOG.trace("get_rowset region in parallel bucket size " + locMap.size());
      for (Map.Entry<TransactionRegionLocation, List<Get>> entry : locMap.entrySet()) {
         final List<Get> rowsInSameRegion = entry.getValue();
         final String regionName = entry.getKey().getRegionInfo().getRegionNameAsString();

         compPool.submit(new TransactionalTableGetSetXCallable((org.apache.hadoop.hbase.client.transactional.TransactionalTable)this,
                         transactionState, savepointId, pSavepointId, isolationLevel, lockMode, skipReadConflict, waitOnSelectForUpdate, regionName,
                         rowsInSameRegion, entry, location, transactionId, queryContext) {
             public GetMultipleTransactionalResponse call() throws IOException {
				 if (!Thread.currentThread().getName().startsWith("T")) {
					 Thread.currentThread().setName("TransactionalTable-" + Thread.currentThread().getName());
				 }

                return doGetSetX();
             }
           });
         loopCount++;
      }

      GetMultipleTransactionalResponse response = null;
      Result [] returnResults = new Result[size];
      int resultIndex = 0;
      for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
         if (LOG.isDebugEnabled()) LOG.debug("doGetSetX results loopIndex " + loopIndex);
         boolean loopExit = false;
         do {
            try {
               response = compPool.take().get();
               loopExit = true;
               for (int n = 0; n < response.getCount(); n++){
                  Result resultFromGet = ProtobufUtil.toResult(response.getResult(n));
                  returnResults[resultIndex] = resultFromGet;
                  resultIndex++;
               }
            }
            catch (Throwable e) {
               if (LOG.isErrorEnabled()) LOG.error("ERROR while calling getMultipleTransactional ",e);
               if (e.getCause() instanceof IOException)
                 throw ((IOException)e.getCause());
               if (e instanceof IOException)
                 throw (IOException)e;
               throw new IOException("ERROR while calling getMultipleTransactional ",e);
            }
         } while (loopExit == false);
      }

      if (LOG.isDebugEnabled()) LOG.debug("Exiting TransactionalTable.get_rowset "
                 + " transaction id " + transactionState.getTransactionId()
                 + " rows requested: " + size + " rows returned: " + resultIndex
                 + " table " + this.tName);
      return returnResults;

    }

    public HRegionLocation getRegionLocation(byte[] row, boolean f)
                                  throws IOException {
        return getConnection().getRegionLocator(this.getName()).getRegionLocation(row, f);
    }
    public void close()  throws IOException 
    { 
      super.close(); 
    }
        public void setAutoFlush(boolean autoFlush, boolean b)
    {
        super.setAutoFlush(autoFlush, b);
    }
    public org.apache.hadoop.conf.Configuration getConfiguration()
    {
        return super.getConfiguration();
    }
    public void flushCommits()
                  throws IOException {
         super.flushCommits();
    }
    public byte[][] getEndKeys()
                    throws IOException
    {
        return getConnection().getRegionLocator(this.getName()).getEndKeys();
    }
    public byte[][] getStartKeys() throws IOException
    {
        return getConnection().getRegionLocator(this.getName()).getStartKeys();
    }
    public void setWriteBufferSize(long writeBufferSize) throws IOException
    {
        super.setWriteBufferSize(writeBufferSize);
    }
    public long getWriteBufferSize()
    {
        return super.getWriteBufferSize();
    }
    public byte[] getTableName()
    {
        return super.getTableName();
    }
    public ResultScanner getScanner(Scan scan, float DOPparallelScanner) throws IOException
    {
        if (scan.isSmall() || DOPparallelScanner == 0){
           if (usePatchScanner){
               if (scan.isSmall())
                     return super.getScanner(scan);//the current patch is for lease timeout, does not affect small scanner
               else
                     return new PatchClientScanner(getConfiguration(), scan, getName(), (ClusterConnection)this.getConnection(),
                                                   this.rpcCallerFactory, this.rpcControllerFactory,
                                                   threadPool, replicaCallTimeoutMicroSecondScan);
           }
           else
               return super.getScanner(scan);
        } else
            return new TrafParallelClientScanner(this.getConnection(), scan, getName(), DOPparallelScanner);
    }
    public Result get(Get g) throws IOException
    {
        return super.get(g);
    }

    public Result[] get( List<Get> g) throws IOException
    {
        return super.get(g);
    }
    public void delete(Delete d) throws IOException
    {
        super.delete(d);
    }
    public void delete(List<Delete> deletes) throws IOException
    {
        super.delete(deletes);
    }
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException
    {
        return super.checkAndPut(row,family,qualifier,value,put);
    }
    public void put(Put p) throws IOException
    {
        super.put(p);
    }
    public void put(List<Put> p) throws IOException
    {
        super.put(p);
    }
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,  Delete delete) throws IOException
    {
        return super.checkAndDelete(row,family,qualifier,value,delete);
    }

    private boolean isLockException(String errorMsg) {
        if (errorMsg == null || errorMsg.equals("")) {
            return false;
        }
        return ((errorMsg.contains("LockTimeOutException") &&
                !errorMsg.contains("for transaction rollbacked"))
                ||
                (errorMsg.contains("FailedToLockException") &&
                 !errorMsg.contains("for region split") &&
                 !errorMsg.contains("for region move")));
    }

    private byte[] checkAndPutUniqueGenerate(int nodeId, long checkValue) {
        ByteBuffer bufferNid = ByteBuffer.allocate(Integer.BYTES);
        bufferNid.putInt(nodeId);
        byte[] nidByte = bufferNid.array();
        
        ByteBuffer bufferValue = ByteBuffer.allocate(Long.BYTES);
        bufferValue.putLong(checkValue);
        byte[] valueByte = bufferValue.array();
        
        byte[] UniqueByte = new byte[nidByte.length+pidByte.length+valueByte.length];  
        System.arraycopy(nidByte, 0, UniqueByte, 0, nidByte.length);  
        System.arraycopy(pidByte, 0, UniqueByte, nidByte.length, pidByte.length);
        System.arraycopy(valueByte, 0, UniqueByte, nidByte.length+pidByte.length, valueByte.length);  
        return UniqueByte;
    }


    public static void main(String[] Args) {

	String  lv_pattern = new String("");
	boolean lv_verbose = false;
	boolean lv_checktable = false;
	int     lv_peer_id = 0;
	int     lv_command = 0;
	boolean lv_flag = false;
	boolean lv_done = false;
	int     lv_num_params = Args.length;
	int     lv_status = 0;

	int lv_index = 0;
	for (String lv_arg : Args) {
	    lv_index++;

	    if (lv_arg.compareTo("-v") == 0) {
		lv_verbose = true;
	    }
	    else if (lv_arg.compareTo("-t") == 0) {
		lv_checktable = true;
		if (lv_index >= lv_num_params) {
		    System.out.println("Table name parameter not provided");
		    System.exit(1);
		}
		lv_pattern = Args[lv_index];
	    }
	    else if (lv_arg.compareTo("-p") == 0) {
		lv_peer_id = Integer.parseInt(Args[lv_index]);
	    }
	    else if (lv_arg.compareTo("-c") == 0) {
		lv_command = Integer.parseInt(Args[lv_index]);
	    }
	    else if (lv_arg.compareTo("-f") == 0) {
		lv_flag = Integer.parseInt(Args[lv_index]) == 1;
	    }
	}

	STRConfig pSTRConfig = null;
	Configuration lv_config = HBaseConfiguration.create();
	try {
	    pSTRConfig = STRConfig.getInstance(lv_config);
	    lv_config = pSTRConfig.getPeerConfiguration(lv_peer_id, false);
	    if (lv_config == null) {
		System.out.println("Peer ID: " 
				   + lv_peer_id 
				   + " does not exist OR it has not been configured for synchronization."
				   );
		System.exit(1);
	    }
	    
	    TransactionalTable lv_table = new TransactionalTable(lv_pattern, 
								 pSTRConfig.getPeerConnection(lv_peer_id));
	    while (!lv_done) {
		if (lv_verbose) {
		    System.out.println("broadcast request,"
				       + " peer: " + lv_peer_id
				       + " table: " + lv_pattern
				       + " command: " + lv_command
				       + " flag: " + lv_flag
				       );
		}
		lv_done = true;
		lv_status = lv_table.broadcastRequest(lv_command, lv_flag);
		if (lv_verbose) {
		    System.out.println("broadcast request, status: " + lv_status);
		}
		if (lv_command == 3) {
		    if (lv_status != 0) {
			System.out.println("Retry broadcast request in 15 seconds");
			lv_done = false;
			try {
			    Thread.sleep(15000);
			} catch (InterruptedException ie) {
			}
		    }
		}
	    }
		lv_table.close();
	} 
	catch (org.apache.hadoop.hbase.TableNotFoundException tnfe) {
	    System.out.println("Table Not Found: " + lv_pattern);
	    System.exit(2);
	}
	catch (IOException ioe) {
	    System.out.println("IO Exception: " + ioe);
	    System.exit(1);
	}

	System.exit(lv_status);
    }
	
}
