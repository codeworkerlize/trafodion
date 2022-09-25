package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;
import io.esgyn.ipc.*;
import io.esgyn.utils.*;


import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.ServerName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.Durability;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.TrxEnvironmentEdgeManager;

import org.apache.hadoop.ipc.RemoteException;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.transactional.*;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;


import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.client.transactional.STRConfig;

import com.google.protobuf.ServiceException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Transaction Manager. Responsible for committing transactions.
 */
public class MTransactionManager {

    // Singleton MTransactionManager class
    private static MTransactionManager g_MTransactionManager = null;
    static final Logger LOG = LogManager.getLogger(MTransactionManager.class.getName());
    public AlgorithmType TRANSACTION_ALGORITHM;

    static String sv_config_property_ampool_coprocessor_class = "io.esgyn.ampool.coprocessor.class";
    static String sv_esgyn_coprocessor_class_default = "io.esgyn.coprocessor.AmpoolTransactions";
    static String sv_esgyn_coprocessor_class = 
    MConnection.getMConfiguration().get(sv_config_property_ampool_coprocessor_class,
					    sv_esgyn_coprocessor_class_default);
    
    private boolean batchRegionServer = false;
    private int RETRY_ATTEMPTS;
    private final TransactionLogger transactionLogger;
    private JtaXAResource xAResource;
    private TmDDL tmDDL;
    private boolean batchRSMetricsFlag = false;
    private boolean recoveryToPitMode = false;
    Configuration     config;

    public static final int TM_SLEEP = 1000;      // One second
    public static final int TM_SLEEP_INCR = 5000; // Five seconds
    public static final int TM_RETRY_ATTEMPTS = 5;

    private IdTm idServer;
    private static final int ID_TM_SERVER_TIMEOUT = 1000;
    private static final int ABORT_EXCEPTION_DELAY = 30000;
    private static final int ABORT_EXCEPTION_RETIRES = 30;
    private boolean recoverToPitMode = false;

    private Map<String,Long> batchRSMetrics = new ConcurrentHashMap<String, Long>();
    private long regions = 0;
    private long regionServers = 0;
    private int metricsCount = 0;

    static ExecutorService    cp_tpe;

    private STRConfig pSTRConfig        = null;
    private int       my_cluster_id     = 0;

    public enum AlgorithmType{
	MVCC, SSCC
	    }
    
    // getInstance to return the singleton object for MTransactionManager
    public synchronized static MTransactionManager getInstance(final Configuration conf) 
	throws KeeperException, InterruptedException, IOException {
	if (g_MTransactionManager == null) {
	    g_MTransactionManager = new MTransactionManager(conf);
	}
	return g_MTransactionManager;
    }
    
    public void init(final TmDDL tmddl) throws IOException 
    {
	this.init();
	this.config = HBaseConfiguration.create();
	this.tmDDL = tmddl;
    }

    public void init() throws IOException
    {
    }

    /**
     * MTransactionManagerCallable  :  inner class for creating asynchronous requests
     */
    private abstract class MTransactionManagerCallable implements Callable<Integer> {

        MTransactionState transactionState = null;
        EsgynMServerLocation  m_location = null;
        MTable m_mtable = null;
        byte[] m_start_key = null;

        MTransactionManagerCallable(MTransactionState pv_txState,
				    EsgynMServerLocation pv_location) 
	    throws IOException 
	    {

		transactionState = pv_txState;
		this.m_location = pv_location;

		m_mtable = MConnection.getTable(pv_location.getTableName());
		
		if (LOG.isTraceEnabled()) LOG.trace("MTransactionManagerCallable Ctor"
						    + ", txState: " + pv_txState
						    + ", location: " + m_location
						    );
		
		if (m_location.getStartKey() == null) {
		    m_start_key = AmpoolUtils.getStartRowKey(m_mtable, 0);
		}
		else {
		    m_start_key = m_location.getStartKey();
		}

	    }

	/**
	 * Method  : doCommitX
	 * Return  : Always 0, can ignore
	 * Purpose : Call commit for a given regionserver
	 */
	public Integer doCommitX(
				 final long pv_transaction_id,
				 final long commitId,
				 final int participantNum,
				 final boolean ignoreUnknownTransaction) 
	    throws CommitUnsuccessfulException, IOException 
	    {

	    boolean retry = false;
	    boolean refresh = false;

	    int retryCount = 0;
	    int retrySleep = TM_SLEEP;

	    do {
		try {
		    if (LOG.isDebugEnabled()) LOG.debug("doCommitX" 
							+ ", txId: " + pv_transaction_id
							+ ", commitId: " + commitId
							+ ", participantNum: " + participantNum
							+ ", ignoreUnknownTransaction: " + ignoreUnknownTransaction
							);

		    AmpoolIPC lv_sr = new AmpoolIPC();
		    lv_sr.m_request_type = AmpoolIPC.RequestType.CommitTransaction;

		    AmpoolIPC.CommitRequest lv_cpr = new AmpoolIPC.CommitRequest();
		    lv_cpr.m_transaction_id = pv_transaction_id;
		    lv_cpr.m_participant_num = 1;
		    lv_sr.m_request = lv_cpr;

		    MExecutionRequest request = new MExecutionRequest();
		    request.setArguments(lv_sr);

		    List<Object> result = null;

		    try {
			if (ignoreUnknownTransaction){
			    if(LOG.isDebugEnabled())
				LOG.debug("doCommitX -- Recovery Redrive before coprocessorService" 
					  + ", txId: " + pv_transaction_id 
					  + ", ignoreUnknownTransaction: " + ignoreUnknownTransaction 
					  + ", table: " +  m_mtable.getName() 
					  + ", m_start_key: " + new String(m_start_key, "UTF-8") 
					  + ", location :" + m_location
					  );
			}

			result = m_mtable
			    .coprocessorService(sv_esgyn_coprocessor_class,
						"cpcall", 
						m_start_key,
						request);
		    } 
		    /*
		    catch (ServiceException se) {
			String msg = new String ("ERROR: doCommitX" 
						 + ", txId: " + pv_transaction_id 
						 + ", participantNum " + participantNum
						 );
			LOG.error(msg, se);
			throw new RetryTransactionException(msg,se);
		    }
		    */
		    catch (Throwable e) {
			String msg = new String ("doCommitX: ERROR calling coprocessor"
						 + ", txId: " + pv_transaction_id 
						 + ", participantNum " + participantNum
						 );
			LOG.error(msg, e);
			//TBD
			if (e.getMessage().contains("Retry")) {
			    throw new RetryTransactionException(e.getMessage());
			}

			throw new DoNotRetryIOException(msg,e);
		    }

		    if(result.size() == 0) {
			LOG.error("doCommitX, received incorrect result" 
				  + ", size: " + result.size() 
				  + ", txId: " + pv_transaction_id 
				  + ", location: " + m_location
				  );
			refresh = true;
			retry = true;
		    }
		    else if(result.size() == 1){
			// size is 1
			Object lv_rp_obj = result.get(0);
			if (LOG.isTraceEnabled()) LOG.trace("CP response object: " + lv_rp_obj);
			if (lv_rp_obj instanceof AmpoolIPC.CommitResponse) {
			    AmpoolIPC.CommitResponse lv_rp_cpr = (AmpoolIPC.CommitResponse) lv_rp_obj;
			    if (LOG.isTraceEnabled()) LOG.trace("Commit response: " 
								+ " status: " + lv_rp_cpr.m_status
								+ ", has_exception: " + lv_rp_cpr.m_has_exception
								);
			    if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
				if (LOG.isTraceEnabled()) LOG.trace("Commit response exception: "
								    + lv_rp_cpr.m_exception);
				lv_rp_cpr.m_exception.printStackTrace();
			    }
			    else {
				if (LOG.isTraceEnabled()) LOG.trace("Commit request status code: " 
								    + lv_rp_cpr.m_status_code
								    );
			    }
			}
			/*
			for (CommitResponse cresponse : result.values()){
			    if(cresponse.getHasException()) {
				String exceptionString = new String (cresponse.getException());
				LOG.error("doCommitX - exceptionString: " + exceptionString);
				if (exceptionString.contains("UnknownTransactionException")) {
				    if (ignoreUnknownTransaction == true) {
					if (LOG.isTraceEnabled()) LOG.trace("doCommitX, ignoring UnknownTransactionException in cresponse");
				    }
				    else {
					LOG.error("doCommitX, coprocessor UnknownTransactionException: " + cresponse.getException());
					throw new UnknownTransactionException(cresponse.getException());
				    }
				}
				else if (exceptionString.contains("org.apache.hadoop.hbase.exceptions.FailedSanityCheckException")) {
				    throw new org.apache.hadoop.hbase.exceptions.FailedSanityCheckException(cresponse.getException());
				}
				else {
				    if (LOG.isTraceEnabled()) LOG.trace("doCommitX coprocessor exception: " + cresponse.getException());
				    throw new RetryTransactionException(cresponse.getException());
				}
			    }
			}
			*/
			retry = false;
		    }
		    /*
		    else {
			for (CommitResponse cresponse : result.values()){
			    if(cresponse.getHasException()) {
				String exceptionString = new String (cresponse.getException());
				LOG.error("doCommitX - exceptionString: " + exceptionString);
				if (exceptionString.contains("UnknownTransactionException")) {
				    if (ignoreUnknownTransaction == true) {
					if (LOG.isTraceEnabled()) LOG.trace("doCommitX, ignoring UnknownTransactionException in cresponse");
				    }
				    else {
					LOG.error("doCommitX, coprocessor UnknownTransactionException: " + cresponse.getException());
					throw new UnknownTransactionException(cresponse.getException());
				    }
				}
				else if (exceptionString.contains("org.apache.hadoop.hbase.exceptions.FailedSanityCheckException")) {
				    throw new org.apache.hadoop.hbase.exceptions.FailedSanityCheckException(cresponse.getException());
				}
				else if(exceptionString.contains("Asked to commit a non-pending transaction")) {
				    if (LOG.isTraceEnabled()) LOG.trace("doCommitX, ignoring 'commit non-pending transaction' in cresponse");
				}
				else {
				    if (LOG.isTraceEnabled()) LOG.trace("doCommitX coprocessor exception: " + cresponse.getException());
				    throw new RetryTransactionException(cresponse.getException());
				}
			    }
			}
			retry = false;
			} */
		}
		catch (UnknownTransactionException ute) {
		    LOG.error("Got unknown exception in doCommitX by participant " + participantNum
			      + ", txId: " + pv_transaction_id
			      , ute
			      );
		    transactionState.requestPendingCountDec(true);
		    throw ute;
		}
		catch (org.apache.hadoop.hbase.exceptions.FailedSanityCheckException fsce) {
		    LOG.error("doCommitX, txId: " + pv_transaction_id + " non-retriable error: " + fsce);
		    refresh = false;
		    retry = false;
		    throw fsce;
		}
		catch (RetryTransactionException rte) {
		    if(retryCount == RETRY_ATTEMPTS) {
			LOG.error("Exceeded retry attempts (" + retryCount + ") in doCommitX for, txId: " + pv_transaction_id);
			// We have received our reply in the form of an exception,
			// so decrement outstanding count and wake up waiters to avoid
			// getting hung forever
			transactionState.requestPendingCountDec(true);
			throw new CommitUnsuccessfulException("Exceeded retry attempts (" + retryCount + ") in doCommitX for, txId: " +
							      pv_transaction_id, rte);
		    }
		    LOG.error("doCommitX retrying, txId: " + pv_transaction_id + " due to Exception: ", rte);
		    refresh = true;
		    retry = true;
		}

		retryCount++;

		if (retryCount < RETRY_ATTEMPTS && retry == true) {
		    try {
			Thread.sleep(retrySleep);
		    } catch(InterruptedException ex) {
			Thread.currentThread().interrupt();
		    }

		    retrySleep += TM_SLEEP_INCR;
		}

	    } while (retryCount < RETRY_ATTEMPTS && retry == true);

	    // We have received our reply so decrement outstanding count
	    transactionState.requestPendingCountDec(false);

	    if (LOG.isTraceEnabled()) LOG.trace("doCommitX -- EXIT txid: " + pv_transaction_id);
	    return 0;
	}

	/**
	 * Method  : doPrepareX
	 *           pv_transaction_id - transaction identifier
	 *           pv_location -
	 * Return  : Commit vote (yes, no, read only)
	 * Purpose : Call prepare for a given regionserver
	 */
	public Integer doPrepareX(final long pv_transaction_id, 
				  final int pv_participant_num, 
				  final EsgynMServerLocation pv_location)
	    throws IOException, CommitUnsuccessfulException 
	    {

	    if (LOG.isDebugEnabled()) LOG.debug("doPrepareX -- ENTRY"
						+ ", txId: " + pv_transaction_id
						+ ", participant_num: " + pv_participant_num 
						+ ", table: " + m_mtable.getName() 
						+ ", m_start_key: " + new String(m_start_key, "UTF-8") 
						+ ", location: " + pv_location
						);
	    int commitStatus = 0;
	    boolean refresh = false;
	    boolean retry = false;
	    int retryCount = 0;
	    int retrySleep = TM_SLEEP;
	    
	    do {
		try {

		    AmpoolIPC lv_sr = new AmpoolIPC();
		    lv_sr.m_request_type = AmpoolIPC.RequestType.PrepareTransaction;
			
		    AmpoolIPC.CommitRequestRequest lv_cpr = new AmpoolIPC.CommitRequestRequest();
		    lv_cpr.m_transaction_id = pv_transaction_id;
		    lv_cpr.m_participant_num = pv_participant_num;
		    lv_sr.m_request = lv_cpr;
			
		    MExecutionRequest request = new MExecutionRequest();
		    request.setArguments(lv_sr);
		    
		    List<Object> result = null;

		    try {
			result = m_mtable.coprocessorService(
							     sv_esgyn_coprocessor_class,
							     "cpcall",
							     m_start_key, 
							     request);
		    } 
		    /*
		    catch (ServiceException se) {
			String errMsg  = new String("doPrepareX coprocessor error for " 
						    + " txid: " + pv_transaction_id
						    );
			LOG.error(errMsg, se);
			throw new RetryTransactionException("Unable to call prepare, coprocessor error", se);
		    }
		    */
		    catch (Throwable e) {
			String errMsg  = new String("doPrepareX coprocessor error" 
						    + ", txId: " + pv_transaction_id
						    + ", participant_num: " + pv_participant_num 
						    + ", table: " + m_mtable.getName() 
						    + ", location: " + pv_location
						    );
			LOG.error(errMsg, e);
			//TBD
			if (e.getMessage().contains("Retry")) {
			    throw new RetryTransactionException(e.getMessage());
			}
			throw new CommitUnsuccessfulException("Unable to call prepare, coprocessor error", e);
		    }

		    if(result.size() == 0)  {
			LOG.error("doPrepareX(MVCC), received incorrect result" 
				  + ", size: " + result.size()
				  + ", txId: " + pv_transaction_id
				  + ", participant_num: " + pv_participant_num 
				  + ", table: " + m_mtable.getName() 
				  + ", location: " + pv_location
				  );
			refresh = true;
			retry = false;
		    }
		    else if(result.size() == 1) {
			// size is 1
			Object lv_rp_obj = result.get(0);
			if (LOG.isTraceEnabled()) LOG.trace("CP response object: " + lv_rp_obj);
			if (lv_rp_obj instanceof AmpoolIPC.CommitRequestResponse) {
			    AmpoolIPC.CommitRequestResponse lv_rp_cpr = (AmpoolIPC.CommitRequestResponse) lv_rp_obj;
			    if (LOG.isTraceEnabled()) LOG.trace("Prepare response: " 
								+ ", message status: " + lv_rp_cpr.m_status
								+ ", has_exception: " + lv_rp_cpr.m_has_exception
								);
			    if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
				if (LOG.isTraceEnabled()) LOG.trace("Prepare response exception: "
								    + lv_rp_cpr.m_exception);
				lv_rp_cpr.m_exception.printStackTrace();
			    }
			    else {
				if (LOG.isTraceEnabled()) LOG.trace("Prepare request status code: " 
								    + lv_rp_cpr.m_status_code
								    );
				commitStatus = lv_rp_cpr.m_status_code;
			    }
			}
			/*
			for (CommitRequestResponse cresponse : result.values()){
			    // Should only be one result
			    int value = cresponse.getResult();
			    commitStatus = value;
			    if(cresponse.getHasException()) {
				if(transactionState.hasRetried() &&
				   cresponse.getException().contains("encountered unknown transactionID")) {
				    retry = false;
				    commitStatus = TransactionalReturn.COMMIT_OK_READ_ONLY;
				}
				else {
				    if (LOG.isTraceEnabled()) LOG.trace("doPrepareX coprocessor exception: " + cresponse.getException());
				    throw new RetryTransactionException(cresponse.getException());
				}
			    }

			    if(value == TransactionalReturn.COMMIT_RESEND) {
				// Handle situation where repeated region is in list due to different m_end_keys
				int count = 0;
				for(EsgynMServerLocation trl : this.transactionState.getParticipatingRegions()) {
				    if(trl.getTableName()
				       .compareTo(pv_location.getTableName()) == 0
				       &&
				       Arrays.equals(trl.getStartKey(),
						     pv_location.getStartKey())) {
					count++;
				    }
				}
				if(count > 1) {
				    commitStatus = TransactionalReturn.COMMIT_OK;
				    retry = false;
				}
				else {
				    try {
					// Pause for split to complete and retry
					Thread.sleep(100);
				    } catch(InterruptedException ex) {
					Thread.currentThread().interrupt();
				    }
				    retry = true;
				}
			    }
			    else {
				retry = false;
			    }
			}
			*/
		    }
		    /*
		    else {
			for(CommitRequestResponse cresponse : result.values()) {
			    if(cresponse.getResult() == TransactionalReturn.COMMIT_UNSUCCESSFUL_FROM_COPROCESSOR ||
			       cresponse.getResult() == TransactionalReturn.COMMIT_CONFLICT ||
			       cresponse.getResult() == TransactionalReturn.COMMIT_UNSUCCESSFUL ||
			       commitStatus == 0) {
				commitStatus = cresponse.getResult();

				if(cresponse.getHasException()) {
				    if(cresponse.getException().contains("encountered unknown transactionID")) {
					retry = false;
					commitStatus = TransactionalReturn.COMMIT_OK_READ_ONLY;
				    }
				    else {
					if (LOG.isTraceEnabled()) LOG.trace("doPrepareX coprocessor exception: " +
									    cresponse.getException());
					throw new RetryTransactionException(cresponse.getException());
				    }
				}
			    }
			}

			if(commitStatus == TransactionalReturn.COMMIT_OK ||
			   commitStatus == TransactionalReturn.COMMIT_OK_READ_ONLY ||
			   commitStatus == TransactionalReturn.COMMIT_RESEND) {
			    commitStatus = TransactionalReturn.COMMIT_OK;
			}
			retry = false;
		    }
		    */
		}
		catch(RetryTransactionException rte) {
		    if (retryCount == RETRY_ATTEMPTS){
			LOG.error("Exceeded retry attempts in doPrepareX: " + retryCount, rte);
			// We have received our reply in the form of an exception,
			// so decrement outstanding count and wake up waiters to avoid
			// getting hung forever
			transactionState.requestPendingCountDec(true);
			throw new CommitUnsuccessfulException("Exceeded retry attempts in doPrepareX: " + retryCount, rte);
		    }
		    LOG.error("doPrepareX participant " + pv_participant_num + " retrying transaction "
			      + pv_transaction_id + " due to Exception: " , rte);
		    refresh = true;
		    retry = true;
		}

		retryCount++;

		if (retryCount < RETRY_ATTEMPTS && retry == true) {
		    try {
			Thread.sleep(retrySleep);
		    } catch(InterruptedException ex) {
			Thread.currentThread().interrupt();
		    }

		    retrySleep += TM_SLEEP_INCR;
		}

	    } while (retryCount < RETRY_ATTEMPTS && retry == true);

	    if (LOG.isTraceEnabled()) LOG.trace("CommitStatus" 
						+ ", txId: " + pv_transaction_id 
						+ ", status : " + commitStatus
						+ ", tablename: " + m_mtable.getName()
						);
	    boolean canCommit = true;
	    boolean readOnly = false;

	    switch (commitStatus) {
	    case TransactionalReturn.COMMIT_OK:
		break;
	    case TransactionalReturn.COMMIT_RESEND:
	    case TransactionalReturn.COMMIT_OK_READ_ONLY:
		transactionState.addRegionToIgnore(pv_location); // No need to doCommit for read-onlys
		readOnly = true;
		break;
	    case TransactionalReturn.COMMIT_UNSUCCESSFUL_FROM_COPROCESSOR:
		if (LOG.isTraceEnabled()) 
		    LOG.trace("Received COMMIT_UNSUCCESSFUL_FROM_COPROCESSOR return code from requestCommit " 
			      + commitStatus 
			      + ", txId: " + pv_transaction_id);
	    case TransactionalReturn.COMMIT_CONFLICT:
	    case TransactionalReturn.COMMIT_UNSUCCESSFUL:
		canCommit = false;
		transactionState.addRegionToIgnore(pv_location); // No need to re-abort.
		break;
	    default:
		LOG.warn("Received invalid return code from requestCommit " 
			 + commitStatus 
			 + ", txId: " + pv_transaction_id 
			 + ", throwing CommitUnsuccessfulException"
			 );
		throw new CommitUnsuccessfulException("Unexpected return code from prepareCommit: " + commitStatus);
	    }

	    if (!canCommit) {
		// track regions which indicate they could not commit for better diagnostics
		LOG.warn("Region [" + pv_location + "] votes "
			 +  "to abort" + (readOnly ? " Read-only ":"") + " transaction "
			 + transactionState.getTransactionId());
		if(commitStatus == TransactionalReturn.COMMIT_CONFLICT)
		    return TransactionManager.TM_COMMIT_FALSE_CONFLICT;
		return TransactionManager.TM_COMMIT_FALSE;
	    }

	    if (readOnly)
		return TransactionManager.TM_COMMIT_READ_ONLY;

	    return TransactionManager.TM_COMMIT_TRUE;
	}

	/**
	 * Method  : doAbortX
	 *           pv_transaction_id - transaction identifier
	 * Return  : Ignored
	 * Purpose : Call abort for a given regionserver
	 */
	public Integer doAbortX(
				final long pv_transaction_id, 
				final int pv_participant_num, 
				final boolean dropTableRecorded) 
	    throws IOException
	{
	    if(LOG.isDebugEnabled()) LOG.debug("doAbortX -- ENTRY"
					       + ", txId: " + pv_transaction_id 
					       + ", participantNum: " + pv_participant_num 
					       );
	    boolean retry = false;
	    boolean refresh = false;
	    int retryCount = 0;
            int retrySleep = TM_SLEEP;

	    AmpoolIPC lv_sr = new AmpoolIPC();
	    lv_sr.m_request_type = AmpoolIPC.RequestType.AbortTransaction;

	    AmpoolIPC.AbortTransactionRequest lv_cpr = new AmpoolIPC.AbortTransactionRequest();
	    lv_cpr.m_transaction_id = pv_transaction_id;
	    lv_cpr.m_participant_num = pv_participant_num;
	    lv_sr.m_request = lv_cpr;
	    
            MExecutionRequest request = new MExecutionRequest();
	    request.setArguments(lv_sr);

	    do {
		try {
		    List<Object> result = null;

		    try {
			if (LOG.isTraceEnabled()) LOG.trace("doAbortX -- before coprocessorService"
							    + ", txId: " + pv_transaction_id 
							    + ", participant_num: " + pv_participant_num 
							    + ", table: " + m_mtable.getName() 
							    + ", m_start_key: " + Bytes.toString(m_start_key)
							    + ", location: " + m_location
							    );

			result = m_mtable.coprocessorService(
							     sv_esgyn_coprocessor_class,
							     "cpcall",
							     m_start_key, 
							     request);
		    }
		    catch (Throwable t) {
			String msg = "ERROR occurred while calling doAbortX coprocessor service"
			    + ", txId: " + pv_transaction_id 
			    + ", participant_num: " + pv_participant_num 
			    + ", table: " + m_mtable.getName() 
			    + ", m_start_key: " + new String(m_start_key, "UTF-8") 
			    + ", location: " + m_location
			    ;
			LOG.error(msg,  t);
			//TBD
			if (t.getMessage().contains("Retry")) {
			    throw new RetryTransactionException(t.getMessage());
			}
			throw new DoNotRetryIOException(msg, t);
		    }

		    if(result.size() == 0) {
			LOG.error("doAbortX, received 0 region results"
				  + ", txId: " + pv_transaction_id 
				  + ", participant_num: " + pv_participant_num 
				  + ", table: " + m_mtable 
				  + ", m_start_key: " + new String(m_start_key, "UTF-8") 
				  );
			refresh = true;
			retry = true;
		    }
		    else {
			if (result.size() > 0) {
			    Object lv_rp_obj = result.get(0);
			    if (LOG.isTraceEnabled()) LOG.trace("CP response object: " + lv_rp_obj);
			    if (lv_rp_obj instanceof AmpoolIPC.AbortTransactionResponse) {
				AmpoolIPC.AbortTransactionResponse lv_rp_cpr = (AmpoolIPC.AbortTransactionResponse) lv_rp_obj;
				if (LOG.isTraceEnabled()) LOG.trace("Abort response" 
								    + ", status: " + lv_rp_cpr.m_status
								    + ", has_exception: " + lv_rp_cpr.m_has_exception
								    );
				if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
				    LOG.error("doAbortX - Abort response has exception"
					      + ", txId: " + pv_transaction_id 
					      + ", participantNum: " + pv_participant_num 
					      + ", exception: " + lv_rp_cpr.m_exception
					      , lv_rp_cpr.m_exception
					      );
				    //TBR				    lv_rp_cpr.m_exception.printStackTrace();
				}
				else {
				    if (LOG.isDebugEnabled()) LOG.debug("doAbortX, Abort response"
									+ ", status code: " + lv_rp_cpr.m_status_code
									+ ", txId: " + pv_transaction_id 
									+ ", participantNum: " + pv_participant_num 
									);
				}
			    }
			}
			/*
			for (AbortTransactionResponse cresponse : result.values()) {
			    if(cresponse.getHasException()) {
				String exceptionString = new String (cresponse.getException());
				String errMsg = new String("Abort of, txId: " + pv_transaction_id
							   + " participantNum: " + pv_participant_num 
							   + " threw Exception: " + exceptionString);
				LOG.error(errMsg);
				if(exceptionString.contains("UnknownTransactionException")) {
				    throw new UnknownTransactionException(errMsg);
				}
				throw new RetryTransactionException(cresponse.getException());
			    }
			}
			*/
			retry = false;
		    }
		}
		catch (RetryTransactionException rte) {
		    if (rte.toString().contains("Asked to commit a non-pending transaction ")) {
			LOG.error(" doAbortX will not retry"
				  + ", txId: " + pv_transaction_id 
				  , rte);
			refresh = false;
			retry = false;
		    }
		    if (retryCount == RETRY_ATTEMPTS) {
			String errMsg = "Exceeded retry attempts in doAbortX: " + retryCount;
			LOG.error(errMsg, rte); 
			throw new DoNotRetryIOException(errMsg, rte);
		    }
		    else {
			LOG.error("doAbortX retrying" 
				  + ", txId: " + pv_transaction_id 
				  + ", participantNum: " + pv_participant_num 
				  ,rte
				  );
			refresh = true;
			retry = true;
		    }
		}

		if (LOG.isWarnEnabled()) LOG.warn("doAbortX"
						  + ", retry-count: " + retryCount
						  + ", retry-flag: " + retry
						  + ", txId: " + pv_transaction_id 
						  + ", participantNum: " + pv_participant_num 
						  );
		refresh = false;

		retryCount++;

	    } while (retryCount < RETRY_ATTEMPTS && retry == true);

	    // We have received our reply so decrement outstanding count
	    transactionState.requestPendingCountDec(false);

	    // forget the transaction if all replies have been received.
	    //  otherwise another thread will do it
	    //      if (transactionState.requestAllComplete())
	    //      {
	    //      }
	    if(LOG.isTraceEnabled()) LOG.trace("doAbortX -- EXIT txID: " + pv_transaction_id);
	    return 0;
	}

	public Integer doAbortX(final List<EsgynMServerLocation> locations, 
				final long transactionId, 
				final int participantNum) throws IOException{
	    //TBD
	    return 0;
	}


    } // MTransactionManagerCallable

    private void checkException(MTransactionState ts, 
				List<EsgynMServerLocation> locations, 
				List<String> exceptions) throws IOException 
    {
	if(LOG.isTraceEnabled()) LOG.trace("checkException -- ENTRY txid: " + ts.getTransactionId());
	ts.clearRetryRegions();
	Iterator<String> exceptionIt = exceptions.iterator();
	StringBuilder logException = new StringBuilder();
	for(int i=0; i < exceptions.size(); i++) {
	    String exception = exceptionIt.next();
	    if(exception.equals(BatchException.EXCEPTION_OK.toString())) {
		continue;
	    }
	    else if (exception.equals(BatchException.EXCEPTION_UNKNOWNTRX_ERR.toString()) ||
		     exception.equals(BatchException.EXCEPTION_NORETRY_ERR.toString())) {
		// No need to add to retry list, throw exception if not ignoring
		logException.append("Encountered " + exception + " on region: " +
				    locations.get(i).toString());
		throw new DoNotRetryIOException(logException.toString());
	    }
	    else if (exception.equals(BatchException.EXCEPTION_RETRY_ERR.toString()) ||
		     exception.equals(BatchException.EXCEPTION_REGIONNOTFOUND_ERR.toString())) {
		if(LOG.isWarnEnabled()) LOG.warn("Encountered batch error, adding region to retry list: " +
						 locations.get(i).toString());
		ts.addRegionToRetry(locations.get(i));
	    }
	    if(logException.length() > 0) {
		throw new RetryTransactionException(logException.toString());
	    }
	}
	if(LOG.isTraceEnabled()) LOG.trace("checkException -- EXIT txid: " + ts.getTransactionId());

    }

    /**
     * threadPool - pool of thread for asynchronous requests
     */
    ExecutorService threadPool;

    /**
     * @param conf
     * @throws ZooKeeperConnectionException
     */
    private MTransactionManager(final Configuration conf) throws KeeperException, IOException, InterruptedException {
        this(LocalTransactionLogger.getInstance(), conf);

        int intThreads = 16;
        String retryAttempts = System.getenv("TMCLIENT_RETRY_ATTEMPTS");
        String numThreads = System.getenv("TM_JAVA_THREAD_POOL_SIZE");
        String numCpThreads = System.getenv("TM_JAVA_CP_THREAD_POOL_SIZE");
        String useSSCC = System.getenv("TM_USE_SSCC");
        String batchRSMetricStr = System.getenv("TM_BATCH_RS_METRICS");
        String batchRS = System.getenv("TM_BATCH_REGIONSERVER");
        String usePIT = System.getenv("TM_USE_PIT_RECOVERY");

        if( usePIT != null){
	    this.recoveryToPitMode = (Integer.parseInt(usePIT) == 1) ? true : false;
        }
        if (LOG.isTraceEnabled()) LOG.trace("TM_USE_PIT_RECOVERY: " + this.recoveryToPitMode);

        if (batchRSMetricStr != null)
	    batchRSMetricsFlag = (Integer.parseInt(batchRSMetricStr) == 1) ? true : false;

        if (batchRS != null)
            batchRegionServer = (Integer.parseInt(batchRS) == 1)? true : false;

        if (retryAttempts != null)
            RETRY_ATTEMPTS = Integer.parseInt(retryAttempts);
        else
            RETRY_ATTEMPTS = TM_RETRY_ATTEMPTS;

        if (numThreads != null)
            intThreads = Integer.parseInt(numThreads);

        int intCpThreads = intThreads;
        if (numCpThreads != null)
            intCpThreads = Integer.parseInt(numCpThreads);

        TRANSACTION_ALGORITHM = AlgorithmType.MVCC;
        if (useSSCC != null)
	    TRANSACTION_ALGORITHM = (Integer.parseInt(useSSCC) == 1) ? AlgorithmType.SSCC :AlgorithmType.MVCC ;

        try {
	    idServer = new IdTm(false);
        }
        catch (Exception e){
	    LOG.error("Exception creating new IdTm: " + e);
        }

        threadPool = Executors.newFixedThreadPool(intThreads);

	cp_tpe = Executors.newFixedThreadPool(intCpThreads);

	pSTRConfig = STRConfig.getInstance(conf);
	
	if (pSTRConfig != null) {
	    my_cluster_id = pSTRConfig.getTrafClusterIdInt();
	}
    }

    /**
     * @param transactionLogger
     * @param conf
     * @throws ZooKeeperConnectionException
     */
    protected MTransactionManager(final TransactionLogger transactionLogger, final Configuration conf)
	throws KeeperException, IOException, InterruptedException {
        this.transactionLogger = transactionLogger;        
    }


    /**
     * Called to start a transaction.
     *
     * @return new transaction state
     */
    public MTransactionState beginTransaction() {
        long pv_transaction_id = transactionLogger.createNewTransactionLog();
        if (LOG.isTraceEnabled()) LOG.trace("Beginning transaction " + pv_transaction_id);
        return new MTransactionState(pv_transaction_id);
    }

    /**
     * Called to start a transaction with transactionID input
     *
     * @return new transaction state
     */
    public MTransactionState beginTransaction(long pv_transaction_id) 
	throws IdTmException 
    {

	if (LOG.isTraceEnabled()) LOG.trace("Enter beginTransaction, txid: " + pv_transaction_id);

	MTransactionState ts = new MTransactionState(pv_transaction_id);
	long startIdVal = -1;

	// Set the startid
	if (ts.islocalTransaction() && 
	    ((TRANSACTION_ALGORITHM == AlgorithmType.SSCC) || (recoveryToPitMode))) {
	    IdTmId startId;
	    try {
		startId = new IdTmId();
		if (LOG.isTraceEnabled()) LOG.trace("beginTransaction (local) getting new startId");
		idServer.id(ID_TM_SERVER_TIMEOUT, startId);
		if (LOG.isTraceEnabled()) LOG.trace("beginTransaction (local) idServer.id returned: " + startId.val);
	    } catch (IdTmException exc) {
		LOG.error("beginTransaction (local) : IdTm threw exception ", exc);
		throw new IdTmException("beginTransaction (local) : IdTm threw exception ", exc);
	    }
	    startIdVal = startId.val;
	}
	else {
	    if (LOG.isTraceEnabled()) LOG.trace("beginTransaction NOT retrieving new startId");
	}

	ts.setStartId(startIdVal);
	if (LOG.isDebugEnabled()) LOG.debug("beginTransaction"
					    + ", txId: " + ts.getTransactionId()
					    + ", startId: " + startIdVal
					    );
	return ts;
    }
    
    /**
     * Prepare to commit a transaction.
     *
     * @param transactionState
     * @return commitStatusCode (see {@link TransactionalRegionInterface})
     * @throws IOException
     * @throws CommitUnsuccessfulException
     */
    public int prepareCommit(final MTransactionState transactionState)
	throws CommitUnsuccessfulException, IOException 
    {
	if (LOG.isDebugEnabled()) LOG.debug("Enter prepareCommit"
					    + ", txId: " + transactionState.getTransactionId()
					    + ", #participants: " + transactionState.getParticipatingRegions().size() 
					    + ", global transaction: " + !transactionState.islocalTransaction()
					    );

	boolean allReadOnly = true;
	int loopCount = 0;

	// (need one CompletionService per request for thread safety, can share pool of threads
	CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);

	for (EsgynMServerLocation location : transactionState.getParticipatingRegions()) {

	    loopCount++;
	    final EsgynMServerLocation myLocation = location;
	    final int lvParticipantNum = loopCount;

	    if(LOG.isTraceEnabled()) LOG.trace("MTransactionManager.prepareCommit" 
					       + ", txId: " + transactionState.getTransactionId()
					       + ", location: " + location
					       );

	    compPool.submit(new MTransactionManagerCallable(transactionState, location) {
		    public Integer call() throws IOException, CommitUnsuccessfulException {
			return doPrepareX(transactionState.getTransactionId(), 
					  lvParticipantNum, 
					  myLocation);
		    }
		});
	}

	// loop to retrieve replies
	int commitError = 0;
	for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
	    boolean loopExit = false;
	    Integer canCommit = null;
	    do
		{
		    try {
			canCommit = compPool.take().get();
			loopExit = true; 
		    } 
		    catch (InterruptedException ie) {}
		    catch (ExecutionException e) {
			throw new CommitUnsuccessfulException(e);
		    }
		} while (loopExit == false);
	    switch (canCommit) {
	    case TransactionManager.TM_COMMIT_TRUE:
		allReadOnly = false;
		break;
	    case TransactionManager.TM_COMMIT_READ_ONLY:
		break;
	    case TransactionManager.TM_COMMIT_FALSE_CONFLICT:
		commitError = TransactionalReturn.COMMIT_CONFLICT;
		break;
	    case TransactionManager.TM_COMMIT_FALSE:
		// Commit conflict takes precedence
		if(commitError != TransactionalReturn.COMMIT_CONFLICT)
		    commitError = TransactionalReturn.COMMIT_UNSUCCESSFUL;
		break;
	    default:
		LOG.error("Unexpected value of canCommit in prepareCommit (during completion processing): " + canCommit);
		commitError = TransactionalReturn.COMMIT_UNSUCCESSFUL;;
	    }
	}

	if(commitError != 0)
	    return commitError;

	return allReadOnly ? TransactionalReturn.COMMIT_OK_READ_ONLY:
	    TransactionalReturn.COMMIT_OK;

    }

    /**
     * Try and commit a transaction. This does both phases of the 2-phase protocol: prepare and commit.
     *
     * @param transactionState
     * @throws IOException
     * @throws CommitUnsuccessfulException
     */
    public void tryCommit(final MTransactionState transactionState)
        throws CommitUnsuccessfulException, UnsuccessfulDDLException, IOException {
        long startTime = TrxEnvironmentEdgeManager.currentTime();
        if (LOG.isTraceEnabled()) LOG.trace("Attempting to commit, txId: " + transactionState.toString());
        int status = prepareCommit(transactionState);

        if (status == TransactionalReturn.COMMIT_OK) {
	    if (LOG.isTraceEnabled()) LOG.trace("doCommit txid:" + transactionState.getTransactionId());

	    doCommit(transactionState);
        } else if (status == TransactionalReturn.COMMIT_OK_READ_ONLY) {
            // no requests sent for fully read only transaction
	    transactionState.completeSendInvoke(0);
        } else if (status == TransactionalReturn.COMMIT_UNSUCCESSFUL) {
	    // We have already aborted at this point
	    throw new CommitUnsuccessfulException();
        }
        if (LOG.isTraceEnabled()) LOG.trace("Committed transaction [" + transactionState.getTransactionId() + "] in ["
					    + ((TrxEnvironmentEdgeManager.currentTime() - startTime)) + "]ms");
    }

    public void retryCommit(final MTransactionState transactionState, final boolean ignoreUnknownTransaction) throws IOException {
	if(LOG.isTraceEnabled()) LOG.trace("retryCommit -- ENTRY -- txid: " + transactionState.getTransactionId());
	synchronized(transactionState.getRetryRegions()) {
	    List<EsgynMServerLocation> completedList = new ArrayList<EsgynMServerLocation>();
	    int loopCount = 0;
	    for (EsgynMServerLocation location : transactionState.getRetryRegions()) {
		loopCount++;
		final int participantNum = loopCount;
		if(LOG.isTraceEnabled()) LOG.trace("retryCommit retrying commit for, txId: "
						   + transactionState.getTransactionId() + ", participant: " + participantNum + ", region "
						   + location.toString());
		threadPool.submit(new MTransactionManagerCallable(transactionState,
								  location
								  ) {
			public Integer call() throws CommitUnsuccessfulException, IOException {

			    return doCommitX(
					     transactionState.getTransactionId(),
					     transactionState.getCommitId(),
					     participantNum,
					     ignoreUnknownTransaction);
			}
		    });
		completedList.add(location);
            }
            transactionState.getRetryRegions().removeAll(completedList);
        }
	if(LOG.isTraceEnabled()) LOG.trace("retryCommit -- EXIT -- txid: " + transactionState.getTransactionId());
    }

    public void retryAbort(final MTransactionState transactionState) throws IOException {
	if(LOG.isTraceEnabled()) LOG.trace("retryAbort -- ENTRY -- txid: " + transactionState.getTransactionId());
	synchronized(transactionState.getRetryRegions()) {
	    List<EsgynMServerLocation> completedList = new ArrayList<EsgynMServerLocation>();
	    int loopCount = 0;
	    for (EsgynMServerLocation location : transactionState.getRetryRegions()) {
		loopCount++;
		final int participantNum = loopCount;
		if(LOG.isTraceEnabled()) LOG.trace("retryAbort retrying abort for, txId: "
						   + transactionState.getTransactionId() + ", participant: "
						   + participantNum + ", region: " + location.toString());
		//TBD : The parameter '0' to getPeerConnections().get() may need to be something else
		threadPool.submit(new MTransactionManagerCallable(transactionState, 
								  location
								  ) {
			public Integer call() throws CommitUnsuccessfulException, IOException {

			    return doAbortX(
					    transactionState.getTransactionId(), 
					    participantNum, 
					    location.isTableRecodedDropped());
			}
		    });
		completedList.add(location);
	    }
	    transactionState.getRetryRegions().removeAll(completedList);
	}
	if(LOG.isTraceEnabled()) LOG.trace("retryAbort -- EXIT -- txid: " + transactionState.getTransactionId());
    }
    /**
     * Do the commit. This is the 2nd phase of the 2-phase protocol.
     *
     * @param transactionState
     * @throws CommitUnsuccessfulException
     */
    public void doCommit(final MTransactionState transactionState)
        throws CommitUnsuccessfulException, UnsuccessfulDDLException, IOException {
	if (LOG.isTraceEnabled()) LOG.trace("doCommit [" + transactionState.getTransactionId() +
					    "] ignoreUnknownTransaction not supplied");
	doCommit(transactionState, false);
    }

    /**
     * Do the commit. This is the 2nd phase of the 2-phase protocol.
     *
     * @param transactionState
     * @param ignoreUnknownTransaction
     * @throws CommitUnsuccessfulException
     */
    public void doCommit(final MTransactionState transactionState, final boolean ignoreUnknownTransaction)
	throws CommitUnsuccessfulException, UnsuccessfulDDLException, IOException {
        int loopCount = 0;

	{
	    // non batch-rs

	    if (LOG.isTraceEnabled()) LOG.trace("Committing [" + transactionState.getTransactionId() +
						"] ignoreUnknownTransactionn: " + ignoreUnknownTransaction);

	    if (LOG.isTraceEnabled()) LOG.trace("sending commits for ts: " + transactionState + ", with commitId: " 
						+ transactionState.getCommitId());
	    int participants = transactionState.getParticipantCount() - transactionState.getRegionsToIgnore();
	    if (LOG.isTraceEnabled()) LOG.trace("Committing [" + transactionState.getTransactionId() + "] with " + participants + " participants" );
	    // (Asynchronously send commit
	    for (EsgynMServerLocation location : transactionState.getParticipatingRegions()) {
		if (LOG.isTraceEnabled()) LOG.trace("sending commits ... [" + transactionState.getTransactionId() + "]");
		if (location.getReadOnly()) {
		    continue;
		}

		loopCount++;
		final int participantNum = loopCount;

		threadPool.submit(new MTransactionManagerCallable(transactionState, 
								  location
								  ) {
			public Integer call() throws CommitUnsuccessfulException, IOException {
			    return doCommitX(transactionState.getTransactionId(),
					     transactionState.getCommitId(),
					     participantNum,
					     ignoreUnknownTransaction);
			}
		    });
	    }

	    // all requests sent at this point, can record the count
	    transactionState.completeSendInvoke(loopCount);
	}

    }

    public void doCommitDDL(final MTransactionState transactionState) throws UnsuccessfulDDLException
    {
	return;
    }

    /**
     * Abort a transaction.
     *
     * @param transactionState
     * @throws IOException
     */
    public void abort(final MTransactionState transactionState) throws IOException, UnsuccessfulDDLException {
	if(LOG.isDebugEnabled()) LOG.debug("Abort -- ENTRY txID: " + transactionState.getTransactionId());
        int loopCount = 0;

	transactionState.setStatus(TransState.STATE_ABORTED);
	// (Asynchronously send aborts
	if (batchRegionServer && (TRANSACTION_ALGORITHM == AlgorithmType.MVCC)) {
	    ServerName servername;
	    List<EsgynMServerLocation> regionList;
	    Map<ServerName, List<EsgynMServerLocation>> locations = new HashMap<ServerName, List<EsgynMServerLocation>>();

	    for (EsgynMServerLocation location : transactionState.getParticipatingRegions()) {
		if (location.getReadOnly()) {
		    continue;
		}

		servername = location.getServerName();

		if(!locations.containsKey(servername)) {
		    regionList = new ArrayList<EsgynMServerLocation>();
		    locations.put(servername, regionList);
		}
		else {
		    regionList = locations.get(servername);
		}
		regionList.add(location);
	    }
	    for(final Map.Entry<ServerName, List<EsgynMServerLocation>> entry : locations.entrySet()) {
		loopCount++;
		final int lv_participant = loopCount;
		threadPool.submit(new MTransactionManagerCallable(transactionState,
								  entry.getValue().iterator().next()
								  ) {
			public Integer call() throws IOException {
			    if (LOG.isTraceEnabled()) LOG.trace("before abort()" 
								+ ", txId: " + transactionState.getTransactionId()
								);

			    return doAbortX(entry.getValue(),
					    transactionState.getTransactionId(),
					    lv_participant);
			}
		    });
	    }
	    transactionState.completeSendInvoke(loopCount);
	}
	else {
    	
	    loopCount = 0;
	    for (EsgynMServerLocation location : transactionState.getParticipatingRegions()) {
		if (location.getReadOnly()) {
		    continue;
		}
		try {
		    loopCount++;
		    final int participantNum = loopCount;

		    if(LOG.isTraceEnabled()) LOG.trace("Submitting abort" 
						       + ", txId: " + transactionState.getTransactionId() 
						       + ", participant: " + participantNum 
						       + ", region: " + location.toString()
						       );
		    threadPool.submit(new MTransactionManagerCallable(transactionState, 
								      location
								      ) {
			    public Integer call() throws IOException {
				return doAbortX(transactionState.getTransactionId(), 
						participantNum, 
						location.isTableRecodedDropped());
			    }
			});
		} catch (IllegalArgumentException iae) {
		    loopCount--;
		    LOG.error("exception in abort: " + iae 
			      + " Reducing loopCount to: " + loopCount 
			      + " Trans: " + transactionState 
			      + "  location: " + location);
		}
	    }

	    // all requests sent at this point, can record the count
	    transactionState.completeSendInvoke(loopCount);
	}

        if(LOG.isTraceEnabled()) LOG.trace("Abort -- EXIT txID: " + transactionState.getTransactionId());

    }

    void abortDDL(final MTransactionState transactionState) throws UnsuccessfulDDLException
    {
	return;
    }

    /*
    public synchronized JtaXAResource getXAResource() {
        if (xAResource == null) {
            xAResource = new JtaXAResource(this);
        }
        return xAResource;
    }
    */

    public void registerRegion(final MTransactionState pv_transactionState, 
			       EsgynMServerLocation pv_location)
	throws IOException
    {

        if (LOG.isTraceEnabled()) LOG.trace("registerRegion ENTRY"
					    + ", transactionState:" + pv_transactionState
					    + ", location: " + pv_location);

        if(pv_transactionState.addRegion(pv_location)){

	    if ((pv_location.peerId != 0) && (pv_location.peerId != this.my_cluster_id)) {
		pv_transactionState.setHasRemotePeers(true);
		if (LOG.isTraceEnabled()) LOG.trace("registerRegion: txn has RemotePeer" 
						    + ", this cluster ID: " + this.my_cluster_id
						    + ", participant cluster ID: " + pv_location.peerId);
	    }

        }
        else {
	    if (LOG.isTraceEnabled()) LOG.trace("registerRegion" 
						+ ", txId: " + pv_transactionState.getTransactionId()
						+ ", region previously added: " + pv_location
						);
        }

    }

    /* 

    /**
     * @param hostnamePort
     * @param regionArray
     * @param tmid
     * @return
     * @throws Exception

    public List<Long> recoveryRequest (String hostnamePort, byte[] regionArray, int tmid) throws DeserializationException, IOException {
        if (LOG.isTraceEnabled()) LOG.trace("recoveryRequest -- ENTRY TM" + tmid);
        HRegionInfo regionInfo = null;
        HTable table = null;
	regionInfo = HRegionInfo.parseFrom(regionArray);

        final String regionName = regionInfo.getRegionNameAsString();
        final int tmID = tmid;
        if (LOG.isTraceEnabled()) LOG.trace("MTransactionManager:recoveryRequest regionInfo encoded name: [" + regionInfo.getEncodedName() + "]" + " hostname " + hostnamePort);
        Batch.Call<TrxRegionService, RecoveryRequestResponse> callable =
	    new Batch.Call<TrxRegionService, RecoveryRequestResponse>() {
	    ServerRpcController controller = new ServerRpcController();
	    BlockingRpcCallback<RecoveryRequestResponse> rpcCallback =
	    new BlockingRpcCallback<RecoveryRequestResponse>();

	    @Override
	    public RecoveryRequestResponse call(TrxRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.RecoveryRequestRequest.Builder rbuilder = RecoveryRequestRequest.newBuilder();
                rbuilder.setTransactionId(-1);
                rbuilder.setRegionName(ByteString.copyFromUtf8(regionName));
                rbuilder.setTmId(tmID);

                instance.recoveryRequest(controller, rbuilder.build(), rpcCallback);
                return rpcCallback.get();
	    }
	};

	// Working out the begin and end keys
	byte[] m_start_key = regionInfo.getStartKey();
	byte[] m_end_key = regionInfo.getEndKey();

	if(m_end_key != HConstants.EMPTY_END_ROW)
	    m_end_key = TransactionManager.binaryIncrementPos(m_end_key, -1);
            
	//TBD : The parameter '0' to getPeerConnections().get() may need to be something else
	table = new HTable(regionInfo.getTable(), (Connection) pSTRConfig.getPeerConnections().get(0), cp_tpe);
         
	Map<byte[], RecoveryRequestResponse> rresult = null;   
	try {
	    rresult = m_mtable.coprocessorService(TrxRegionService.class, m_start_key, m_end_key, callable);
	}
	catch (ServiceException se) {
	    LOG.error("Exception thrown when calling coprocessor: ", se);
	    throw new IOException("Problem with calling coprocessor, no regions returned result", se);
	}
	catch (Throwable t) {
	    LOG.error("Exception thrown when calling coprocessor: ", t);
	    throw new IOException("Problem with calling coprocessor, no regions returned result", t);
	}

        Collection<RecoveryRequestResponse> results = rresult.values();
        RecoveryRequestResponse[] resultArray = new RecoveryRequestResponse[results.size()];
        results.toArray(resultArray);

        if(resultArray.length == 0) {
            m_mtable.close();
            throw new IOException("Problem with calling coprocessor, no regions returned result");
        }

        //return tri.recoveryRequest(regionInfo.getRegionName(), tmid);
        m_mtable.close();
        if (LOG.isTraceEnabled()) LOG.trace("recoveryRequest -- EXIT TM" + tmid);

        return resultArray[0].getResultList();
    }
    */
}

