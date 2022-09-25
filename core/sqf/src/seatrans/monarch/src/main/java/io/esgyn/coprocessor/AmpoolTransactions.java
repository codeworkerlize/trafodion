package io.esgyn.coprocessor;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.filter.KeyOnlyFilter;

import io.esgyn.ipc.AmpoolIPC;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.Stoppable;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
//import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.transactional.MemoryUsageException;
import org.apache.hadoop.hbase.client.transactional.OutOfOrderProtocolException;
import org.apache.hadoop.hbase.client.transactional.UnknownTransactionException;
import org.apache.hadoop.hbase.client.transactional.BatchException;
import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;

import org.apache.hadoop.hbase.regionserver.LeaseException;
import org.apache.hadoop.hbase.regionserver.LeaseListener;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TrxEnvironmentEdgeManager;

import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;

public class AmpoolTransactions extends MCoprocessor {

    static final Logger LOG = LogManager.getLogger(AmpoolTransactions.class.getName());

    MCoprocessorContext m_cp_context = null;
    //start
    protected Map<Long, Long> transactionsByIdTest = null;
    ConcurrentHashMap<String, Object> transactionsByIdTestz = null;

    // Map of recent transactions that are COMMIT_PENDING or COMMITED keyed 
    // by their sequence number

    static protected ConcurrentHashMap<String, Object> all_commitedTransactionsBySequenceNumber = new ConcurrentHashMap<String, Object>();
    private SortedMap<Long, AmpoolTransactionState> commitedTransactionsBySequenceNumber;

    // Collection of transactions that are COMMIT_PENDING
    static protected ConcurrentHashMap<String, Object> all_commitPendingTransactions = new ConcurrentHashMap<String, Object>();
    private Set<AmpoolTransactionState> commitPendingTransactions;

    // an in-doubt transaction list during recovery WALEdit replay
    static protected ConcurrentHashMap<String, Object> all_indoubtTransactionsById = new ConcurrentHashMap<String, Object>();
    private Map<Long, List<WALEdit>> indoubtTransactionsById;

    // list of transactions to check for stale scanners
    static protected ConcurrentHashMap<String, Object> all_cleanScannersForTransactions = new ConcurrentHashMap<String, Object>();
    private List<Long> cleanScannersForTransactions;
    
    // an in-doubt transaction list count by TM id
    static protected ConcurrentHashMap<String, Object> all_indoubtTransactionsCountByTmid = new ConcurrentHashMap<String, Object>();
    private Map<Integer, Integer> indoubtTransactionsCountByTmid = new TreeMap<Integer,Integer>();

    // Concurrent map for transactional region scanner holders
    // Protected by synchronized methods
    /* TBD  private ConcurrentHashMap<Long,
       TransactionalRegionScannerHolder> scanners =
       new ConcurrentHashMap<Long, TransactionalRegionScannerHolder>();
    */

    // Atomic values to manage region scanners
    private AtomicLong performScannerId = new AtomicLong(0);
    private AtomicLong nextSequenceId = new AtomicLong(0);

    private Object commitCheckLock = new Object();
    private Object recoveryCheckLock = new Object();
    private Object editReplay = new Object();
    private static Object stoppableLock = new Object();
    private int reconstructIndoubts = 0; 
    //temporary THLog getSequenceNumber() replacement
    private AtomicLong nextLogSequenceId = new AtomicLong(0);
    public AtomicLong controlPointEpoch = new AtomicLong(1);
    private final int oldTransactionFlushTrigger = 0;
    private final Boolean splitDelayEnabled = false;
    private final Boolean doWALHlog = false;
    static Leases transactionLeases = null;
    //  CleanOldTransactionsChore cleanOldTransactionsThread;
    //  static MemoryUsageChore memoryUsageThread = null;
    Stoppable stoppable = new StoppableImplementation();
    static Stoppable stoppable2 = new StoppableImplementation();
    private int cleanTimer = 5000; // Five minutes
    private int memoryUsageTimer = 60000; // One minute   
    private int regionState = 0; 
    private Path recoveryTrxPath = null;
    private int cleanAT = 0;
  
    private long[] commitCheckTimes   = new long[50];
    private long[] hasConflictTimes   = new long[50];
    private long[] putBySequenceTimes = new long[50];
    private long[] writeToLogTimes    = new long[50];

    private AtomicInteger  timeIndex               =    new AtomicInteger (0);
    private AtomicInteger  totalCommits            =    new AtomicInteger (0);
    private AtomicInteger  writeToLogOperations    =    new AtomicInteger (0);
    private AtomicInteger  putBySequenceOperations =    new AtomicInteger (0);
    private long   totalCommitCheckTime =    0;
    private long   totalConflictTime    =    0;
    private long   totalPutTime         =    0;
    private long   totalWriteToLogTime  =    0;
    private long   minCommitCheckTime   =    1000000000;
    private long   maxCommitCheckTime   =    0;
    private double avgCommitCheckTime   =    0;
    private long   minConflictTime      =    1000000000;
    private long   maxConflictTime      =    0;
    private double avgConflictTime      =    0;
    private long   minPutTime           =    1000000000;
    private long   maxPutTime           =    0;
    private double avgPutTime           =    0;
    private long   minWriteToLogTime    =    1000000000;
    private long   maxWriteToLogTime    =    0;
    private double avgWriteToLogTime    =    0;

    private AmpoolRegionInfo regionInfo = null;
    private String m_regionName = null;
    private boolean m_isTrafodionMetadata = false;
    //  private TransactionalRegion t_Region = null;
    private FileSystem fs = null;
    //  private RegionCoprocessorHost rch = null;
    private WAL tHLog = null;
    private AtomicBoolean blockAll = new AtomicBoolean(false);
    private AtomicBoolean blockNonPhase2 = new AtomicBoolean(false);
    private AtomicBoolean blockNewTrans = new AtomicBoolean(false);
    private boolean configuredEarlyLogging = false;
    private boolean configuredConflictReinstate = false;
    private static Object zkRecoveryCheckLock = new Object();
    private static ZooKeeperWatcher zkw1 = null;

    STRConfig pSTRConfig;

    String lv_hostName;
    int lv_port;
    private static String zNodePath = "/trafodion/recovery/";
    private static final String COMMITTED_TXNS_KEY = "1_COMMITED_TXNS_KEY";
    private static final String TXNS_BY_ID_KEY = "2_TXNS_BY_ID_KEY";
    //  private HFileContext context = new HFileContextBuilder().withIncludesTags(false).build();

    private static final int MINIMUM_LEASE_TIME = 7200 * 1000;
    private static final int LEASE_CHECK_FREQUENCY = 1000;
    private static final int DEFAULT_SLEEP = 60 * 1000;
    private static final int DEFAULT_MEMORY_THRESHOLD = 100; // 100% memory used
    private static final int DEFAULT_MEMORY_SLEEP = 15 * 1000;
    private static final boolean DEFAULT_MEMORY_WARN_ONLY = true;        
    private static final boolean DEFAULT_MEMORY_PERFORM_GC = false;
    private static final int DEFAULT_ASYNC_WAL = 1;
    private static final boolean DEFAULT_SKIP_WAL = false;
    private static final boolean DEFAULT_COMMIT_EDIT = false;
    private static final boolean DEFAULT_SUPPRESS_OOP = false;
    private static final String SLEEP_CONF = "hbase.transaction.clean.sleep";
    private static final String LEASE_CONF  = "hbase.transaction.lease.timeout";
    private static final String MEMORY_THRESHOLD = "hbase.transaction.memory.threshold";
    private static final String MEMORY_WARN_ONLY = "hbase.transaction.memory.warn.only";
    private static final String MEMORY_CONF = "hbase.transaction.memory.sleep";
    private static final String MEMORY_PERFORM_GC = "hbase.transaction.memory.perform.GC";
    private static final String CONF_ASYNC_WAL  = "hbase.trafodion.async.wal";
    private static final String CONF_SKIP_WAL  = "hbase.trafodion.skip.wal";
    private static final String CONF_COMMIT_EDIT  = "hbase.trafodion.full.commit.edit";
    private static final String SUPPRESS_OOP = "hbase.transaction.suppress.OOP.exception";
    private static final String CHECK_ROW = "hbase.transaction.check.row";
    protected static int transactionLeaseTimeout = 0;
    private static int scannerLeaseTimeoutPeriod = 0;
    private static int scannerThreadWakeFrequency = 0;
    private static int memoryUsageThreshold = DEFAULT_MEMORY_THRESHOLD;
    private static boolean memoryUsagePerformGC = DEFAULT_MEMORY_PERFORM_GC;
    private static boolean memoryUsageWarnOnly = DEFAULT_MEMORY_WARN_ONLY;
    private static int asyncWal = DEFAULT_ASYNC_WAL;
    private static boolean skipWal = DEFAULT_SKIP_WAL;
    private static boolean fullEditInCommit = DEFAULT_COMMIT_EDIT;
    //  private static MemoryMXBean memoryBean = null;
    private static float memoryPercentage = 0;
    private static boolean memoryThrottle = false;
    private static boolean suppressOutOfOrderProtocolException = DEFAULT_SUPPRESS_OOP;
    //  private Configuration config;
    private static boolean checkRowBelongs = true;

    // Transaction state defines
    private static final int COMMIT_OK = 1;
    private static final int COMMIT_OK_READ_ONLY = 2;
    private static final int COMMIT_UNSUCCESSFUL_FROM_COPROCESSOR = 3;
    private static final int COMMIT_CONFLICT = 5;
    private static final int COMMIT_RESEND = 6;

    private static final int CLOSE_WAIT_ON_COMMIT_PENDING = 1000;
    private static final int MAX_COMMIT_PENDING_WAITS = 10;
    private Thread ChoreThread = null;
    private static Thread ChoreThread2 = null;
    //private static Thread ScannerLeasesThread = null;
    private static Thread TransactionalLeasesThread = null;

    public static final int TS_ACTIVE = 0;
    public static final int TS_COMMIT_REQUEST = 1;
    public static final int TS_COMMIT = 2;
    public static final int TS_ABORT = 3;
    public static final int TS_CONTROL_POINT_COMMIT = 4;

    public static final int REGION_STATE_RECOVERING = 0;
    public static final int REGION_STATE_START = 2;

    public static final String trxkeyEPCPinstance = "EPCPinstance";
    // TBD Maybe we should just use HashMap to improve the performance, ConcurrentHashMap could be too strict
    static ConcurrentHashMap<String, Object> transactionsEPCPMap;

    //end

    static protected ConcurrentHashMap<String, Object> all_transactionsById = new ConcurrentHashMap<String, Object>();
    protected ConcurrentHashMap<String, AmpoolTransactionState> transactionsById;
    
    static long m_count = 0; //number of times a cp call is made

    EsgynAmpoolRegion m_region;

    ConcurrentHashMap<String, AmpoolTransactionState>
	get_region_transactionsById(AmpoolRegionInfo p_region_info) 
    {
	if (LOG.isDebugEnabled()) LOG.debug("Enter: AmpoolTransactions.get_region_transactionsById()");

	String lv_region_key = p_region_info.getEncodedName();
	Object lv_obj = all_transactionsById.get(lv_region_key);
	if (lv_obj == null) {
	    if (LOG.isDebugEnabled()) LOG.debug("AmpoolTransactions: get_region_transactionsById" 
						+ ", created a new hashmap" 
						+ ", region: " + lv_region_key
						);
	    lv_obj = new ConcurrentHashMap<String, AmpoolTransactionState>();
	    all_transactionsById.put(lv_region_key, lv_obj);
	}

	return (ConcurrentHashMap<String, AmpoolTransactionState>) lv_obj;
    }

    SortedMap<Long, AmpoolTransactionState>
	get_region_commitedTransactionsBySequenceNumber(AmpoolRegionInfo p_region_info) 
    {
	if (LOG.isDebugEnabled()) LOG.debug("Enter: AmpoolTransactions.get_region_commitedTransactionsBySequenceNumber()");
	
	String lv_region_key = p_region_info.getEncodedName();
	Object lv_obj = all_commitedTransactionsBySequenceNumber.get(lv_region_key);
	if (lv_obj == null) {
	    if (LOG.isDebugEnabled()) LOG.debug("AmpoolTransactions: get_region_commitedTransactionsBySequenceNumber" 
						+ ", created a new synchronized and sorted TreeMap" 
						+ ", region: " + lv_region_key
						);
	    lv_obj = Collections.synchronizedSortedMap(new TreeMap<Long, AmpoolTransactionState>());
	    all_commitedTransactionsBySequenceNumber.put(lv_region_key, lv_obj);
	}

	return (SortedMap<Long, AmpoolTransactionState>) lv_obj;
    }

    synchronized Set<AmpoolTransactionState>
	get_region_commitPendingTransactions(AmpoolRegionInfo p_region_info) 
    {
	if (LOG.isDebugEnabled()) LOG.debug("Enter: AmpoolTransactions.get_region_commitPendingTransactions()");
	
	String lv_region_key = p_region_info.getEncodedName();
	Object lv_obj = all_commitPendingTransactions.get(lv_region_key);
	if (lv_obj == null) {
	    if (LOG.isDebugEnabled()) LOG.debug("AmpoolTransactions: get_region_commitPendingTransactions" 
						+ ", created a new synchronized HashSet" 
						+ ", region: " + lv_region_key
						);
	    lv_obj = Collections.synchronizedSet(new HashSet<AmpoolTransactionState>());
	    all_commitPendingTransactions.put(lv_region_key, lv_obj);
	}

	return (Set<AmpoolTransactionState>) lv_obj;
    }

    synchronized Map<Long, List<WALEdit>> 
	get_region_indoubtTransactionsById(AmpoolRegionInfo p_region_info) 
    {
	
	if (LOG.isDebugEnabled()) LOG.debug("Enter: AmpoolTransactions.get_region_indoubtTransactionsById()");
	
	String lv_region_key = p_region_info.getEncodedName();
	Object lv_obj = all_indoubtTransactionsById.get(lv_region_key);
	if (lv_obj == null) {
	    if (LOG.isDebugEnabled()) LOG.debug("AmpoolTransactions: get_region_indoubtTransactionsById" 
						+ ", created a new synchronized TreeMap" 
						+ ", region: " + lv_region_key
						);
	    lv_obj = Collections.synchronizedMap(new TreeMap<Long, List<WALEdit>>());
	    all_indoubtTransactionsById.put(lv_region_key, lv_obj);
	}

	return (Map<Long, List<WALEdit>>) lv_obj;
    }

    synchronized List<Long>
	get_region_cleanScannersForTransactions(AmpoolRegionInfo p_region_info) 
    {
	if (LOG.isDebugEnabled()) LOG.debug("Enter: AmpoolTransactions.get_region_cleanScannersForTransactions()");
	
	String lv_region_key = p_region_info.getEncodedName();
	Object lv_obj = all_cleanScannersForTransactions.get(lv_region_key);
	if (lv_obj == null) {
	    if (LOG.isDebugEnabled()) LOG.debug("AmpoolTransactions: get_region_cleanScannersForTransactions" 
						+ ", created a new synchronized LinkedList" 
						+ ", region: " + lv_region_key
						);
	    lv_obj = Collections.synchronizedList(new LinkedList<Long>());
	    all_cleanScannersForTransactions.put(lv_region_key, lv_obj);
	}

	return (List<Long>) lv_obj;
    }

    synchronized Map<Integer, Integer>
	get_region_indoubtTransactionsCountByTmid(AmpoolRegionInfo p_region_info) 
    {
	if (LOG.isDebugEnabled()) LOG.debug("Enter: AmpoolTransactions.get_region_indoubtTransactionsCountByTmid()");
	
	String lv_region_key = p_region_info.getEncodedName();
	Object lv_obj = all_indoubtTransactionsCountByTmid.get(lv_region_key);
	if (lv_obj == null) {
	    if (LOG.isDebugEnabled()) LOG.debug("AmpoolTransactions: get_region_indoubtTransactionsCountByTmid" 
						+ ", created a new synchronized LinkedList" 
						+ ", region: " + lv_region_key
						);
	    lv_obj = new TreeMap<Integer,Integer>();
	    all_indoubtTransactionsCountByTmid.put(lv_region_key, lv_obj);
	}
	
	return (Map<Integer, Integer>) lv_obj;
    }

    public AmpoolTransactions() 
    {
    }

    public void cp_execute(MCoprocessorContext p_cp_context) 
    {
	m_count++;
	m_cp_context = p_cp_context;

	try 
	    {
		MExecutionRequest request = m_cp_context.getRequest();
	
		m_region = new EsgynAmpoolRegion(m_cp_context.getMTableRegion());
	    
		regionInfo = new AmpoolRegionInfo(m_cp_context.getTable(), 
						  m_cp_context.getMTableRegion());
		m_region.setRegionInfo(regionInfo);

		transactionsById = get_region_transactionsById(regionInfo);
		commitedTransactionsBySequenceNumber = get_region_commitedTransactionsBySequenceNumber(regionInfo);
		commitPendingTransactions = get_region_commitPendingTransactions(regionInfo);
		indoubtTransactionsById = get_region_indoubtTransactionsById(regionInfo);
		//TBD		cleanScannersForTransactions = get_region_cleanScannersForTransactions(regionInfo);
		indoubtTransactionsCountByTmid = get_region_indoubtTransactionsCountByTmid(regionInfo);
    
		AmpoolIPC.RequestType lv_arg;
		EsgynMPut lv_put = null;
		AmpoolIPC lv_traf_req = (AmpoolIPC) request.getArguments();
		lv_arg = lv_traf_req.m_request_type;

		if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactions.cp_execute"
						  + ", request_type: " + lv_arg
						  + ", request: " + lv_traf_req.m_request
						  );
	    
		if (lv_arg == AmpoolIPC.RequestType.CheckAndPut) {
		    processCheckAndPut(m_cp_context,
				       lv_traf_req);
		}
		else if (lv_arg == AmpoolIPC.RequestType.GetTransactional) {
		    processGet(m_cp_context,
			       lv_traf_req);
		}
		else if (lv_arg == AmpoolIPC.RequestType.PutTransactional) {
		    processPut(m_cp_context,
			       lv_traf_req);
		}
		else if (lv_arg == AmpoolIPC.RequestType.DeleteTransactional) {
		    processDelete(m_cp_context,
				  lv_traf_req);
		}
		else if (lv_arg == AmpoolIPC.RequestType.GetTransactionCount) {
		    processGetTransactionCount(m_cp_context);

		}
		else if (lv_arg == AmpoolIPC.RequestType.GetTransactionList) {
		    processGetTransactionList(m_cp_context);
		}
		else if (lv_arg == AmpoolIPC.RequestType.PrepareTransaction) {
		    processPrepareTransaction(m_cp_context,
					      lv_traf_req);
		}
		else if (lv_arg == AmpoolIPC.RequestType.CommitTransaction) {
		    processCommitTransaction(m_cp_context,
					     lv_traf_req);
		}
		else if (lv_arg == AmpoolIPC.RequestType.AbortTransaction) {
		    processAbortTransaction(m_cp_context,
					    lv_traf_req);
		}
		else if (lv_arg == AmpoolIPC.RequestType.Counter) {
		    processCounter(m_cp_context);

		}
		else if (lv_arg == AmpoolIPC.RequestType.NumCPCalls) {
		    m_cp_context.getResultSender().lastResult(m_count);
		}
		if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions.cp_execute exit");
	    } catch (Exception e) {
	    LOG.error("AmpoolTransactions.cp_execute, exception: ", e);
	    throw new MCoprocessorException("Error in scanning results");
	}

    }

    public void cpcall(MCoprocessorContext p_cp_context) 
    {
	AmpoolTransactions lv_at = new AmpoolTransactions();
	lv_at.cp_execute(p_cp_context);
    }

    void processGetTransactionCount(MCoprocessorContext p_context)
    {
	if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactions - Enter processGetTransactionCount" );
	
	MExecutionRequest request = p_context.getRequest();
	AmpoolIPC.GetTransactionCountResponse lv_rp = new AmpoolIPC.GetTransactionCountResponse();

	lv_rp.m_status = true;
	lv_rp.m_has_exception = false;
	if (transactionsById != null) {
	    if (LOG.isInfoEnabled()) 
		LOG.info("AmpoolTransactions - processGetTransactionCount()"
			 + ", #transactionsById: " + (transactionsById !=null ? transactionsById.size():"null")
			 );
	    lv_rp.m_num_transactions = transactionsById.size();
	    if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactions - processGetTransactionCount()"
					      + ", num active txns: " + lv_rp.m_num_transactions);
	}
	else {
	    lv_rp.m_num_transactions = 0;
	}
	
	p_context.getResultSender().lastResult(lv_rp);
    }

    void processGetTransactionList(MCoprocessorContext p_context)
    {
	if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactions - Enter processGetTransactionList" );
	
	MExecutionRequest request = p_context.getRequest();
	AmpoolIPC.GetTransactionListResponse lv_rp = new AmpoolIPC.GetTransactionListResponse();
	lv_rp.m_status = true;
	lv_rp.m_has_exception = false;
	lv_rp.m_num_transactions = transactionsById.size();
	if (transactionsById.size() > 0) {
	    lv_rp.m_txid_list = new LinkedList<String>();
	    for (AmpoolTransactionState lv_tx_state: transactionsById.values()) {
		lv_rp.m_txid_list.add(lv_tx_state.toString());
	    }
	}

	p_context.getResultSender().lastResult(lv_rp);
    }

    void processCounter(MCoprocessorContext p_context)
    {
	if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactions - Enter processCounter" );
	
	long rowCount = 0L;
	MExecutionRequest request = p_context.getRequest();
	Scan scan = request.getScanner();

	LOG.info("AmpoolTransactions: scan object: " + scan);
	// since we only need keys
	scan.setFilter(new KeyOnlyFilter());
	Scanner scanner = null;
	try {
	    scanner = p_context.getMTableRegion().getScanner(scan);
	} catch (Exception e) {
	    p_context.getResultSender().sendException(e);
	    return;
	}

	while (scanner.next()!=null){
	    rowCount++;
	}

	p_context.getResultSender().lastResult(rowCount);
    }

    void processCheckAndPut(MCoprocessorContext p_context,
			    AmpoolIPC           p_traf_req)
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Enter processCheckAndPut");
	AmpoolIPC.CheckAndPutRequest lv_rq = (AmpoolIPC.CheckAndPutRequest) p_traf_req.m_request;
	AmpoolIPC.CheckAndPutResponse lv_rp = new AmpoolIPC.CheckAndPutResponse();

	lv_rp.m_status = true;
	lv_rp.m_has_exception = false; 

	EsgynMPut lv_put = lv_rq.m_put;

	if ( !Bytes.equals(lv_put.getRowKey(), lv_rq.m_row_key)) {
	    if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactions - processCheckAndPut, rowkeys do not match." 
					      + ", rowKey in request: " + lv_rq.m_row_key
					      + ", rowKey in EsgynMPut: " + lv_put.getRowKey()
					      );
	    lv_rp.m_status = false;
	}
	else {
	    try {
		if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Calling perform_check_and_put." );
		boolean lv_result = 
		    perform_check_and_put(lv_rq.m_transaction_id,
					  lv_rq.m_row_key,
					  lv_rq.m_column,
					  lv_rq.m_value,
					  lv_rq.m_put
					  );
		if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactions - perform_check_and_put returned: " + lv_result);
		lv_rp.m_status = lv_result;
	    }
	    catch (Exception e) {
		LOG.error("AmpoolTransactions - perform_check_and_put threw an exception: ", e);
		lv_rp.m_status = false;
		lv_rp.m_has_exception = true;
		lv_rp.m_exception = e;
	    }
	}

	p_context.getResultSender().lastResult(lv_rp);
    }

    void processPrepareTransaction(MCoprocessorContext p_context,
				   AmpoolIPC           p_traf_req)
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Enter processPrepareTransaction");

	AmpoolIPC.CommitRequestRequest lv_rq = (AmpoolIPC.CommitRequestRequest) p_traf_req.m_request;
	AmpoolIPC.CommitRequestResponse lv_rp = new AmpoolIPC.CommitRequestResponse();

	lv_rp.m_status = true;
	lv_rp.m_has_exception = false; 

	try {
	    if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Calling commitRequest." );
	    lv_rp.m_status_code = commitRequest(lv_rq.m_transaction_id,
						lv_rq.m_participant_num
						);
	    if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - commitRequest returned: " 
						+ lv_rp.m_status_code
						);
	}
	catch (Exception e) {
	    LOG.error("AmpoolTransactions - commitRequest threw an exception: ", e);
	    lv_rp.m_status = false;
	    lv_rp.m_has_exception = true;
	    lv_rp.m_exception = e;
	}

	p_context.getResultSender().lastResult(lv_rp);
    }

    void processCommitTransaction(MCoprocessorContext p_context,
				  AmpoolIPC           p_traf_req)
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Enter processCommitTransaction");

	AmpoolIPC.CommitRequest lv_rq = (AmpoolIPC.CommitRequest) p_traf_req.m_request;
	AmpoolIPC.CommitResponse lv_rp = new AmpoolIPC.CommitResponse();

	lv_rp.m_status = true;
	lv_rp.m_has_exception = false; 

	try {
	    commit(lv_rq.m_transaction_id,
		   lv_rq.m_participant_num
		   );
	}
	catch (Exception e) {
	    LOG.error("AmpoolTransactions - commit threw an exception: ", e);
	    lv_rp.m_status = false;
	    lv_rp.m_has_exception = true;
	    lv_rp.m_exception = e;
	}

	p_context.getResultSender().lastResult(lv_rp);
    }

    void processAbortTransaction(MCoprocessorContext p_context,
				 AmpoolIPC           p_traf_req)
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Enter processAbortTransaction");

	AmpoolIPC.AbortTransactionRequest lv_rq = (AmpoolIPC.AbortTransactionRequest) p_traf_req.m_request;
	AmpoolIPC.AbortTransactionResponse lv_rp = new AmpoolIPC.AbortTransactionResponse();

	lv_rp.m_status = true;
	lv_rp.m_has_exception = false; 

	try {
	    if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Calling aborttransaction." );
	    abortTransaction(lv_rq.m_transaction_id);
	    if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - abort returned without throwing any Exception.");
	}
	catch (Exception e) {
	    LOG.error("AmpoolTransactions - abort threw an exception: ", e);
	    lv_rp.m_status = false;
	    lv_rp.m_has_exception = true;
	    lv_rp.m_exception = e;
	}

	p_context.getResultSender().lastResult(lv_rp);
    }

    void processGet(MCoprocessorContext p_context,
		    AmpoolIPC           p_traf_req)
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Enter processGet" );

	AmpoolIPC.GetTransactionalRequest lv_rq = (AmpoolIPC.GetTransactionalRequest) p_traf_req.m_request;
	AmpoolIPC.GetTransactionalResponse lv_rp = new AmpoolIPC.GetTransactionalResponse();

	lv_rp.m_status = true;
	lv_rp.m_has_exception = false; 

	try {
	    lv_rp.m_result = this.get(lv_rq.m_transaction_id,
				      lv_rq.m_get);
	} catch (Exception e) {
	    lv_rp.m_status = false;
	    lv_rp.m_exception = e;
	    lv_rp.m_has_exception = true; 
	}
	
	p_context.getResultSender().lastResult(lv_rp);
    }

    void processPut(MCoprocessorContext p_context,
		    AmpoolIPC           p_traf_req)
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Enter processPut" );
	AmpoolIPC.PutTransactionalRequest lv_rq = (AmpoolIPC.PutTransactionalRequest) p_traf_req.m_request;
	AmpoolIPC.PutTransactionalResponse lv_rp = new AmpoolIPC.PutTransactionalResponse();

	lv_rp.m_status = true;
	lv_rp.m_has_exception = false; 

	try {
	    put(lv_rq.m_transaction_id,
		lv_rq.m_put);
	} catch (Exception e) {
	    lv_rp.m_status = false;
	    lv_rp.m_exception = e;
	    lv_rp.m_has_exception = true; 
	}
	
	p_context.getResultSender().lastResult(lv_rp);
    }

    void processDelete(MCoprocessorContext p_context,
		       AmpoolIPC           p_traf_req)
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Enter processDelete" );

	AmpoolIPC.DeleteTransactionalRequest lv_rq = (AmpoolIPC.DeleteTransactionalRequest) p_traf_req.m_request;
	AmpoolIPC.DeleteTransactionalResponse lv_rp = new AmpoolIPC.DeleteTransactionalResponse();

	lv_rp.m_status = true;
	lv_rp.m_has_exception = false; 

	try {
	    //TBD: Delete is not 'serializable' right now so a workaround for now
	    EsgynMDelete lv_delete = new EsgynMDelete(lv_rq.m_delete);
	    delete(lv_rq.m_transaction_id,
		   lv_delete);
	} catch (Exception e) {
	    lv_rp.m_status = false;
	    lv_rp.m_exception = e;
	    lv_rp.m_has_exception = true; 
	}
	
	p_context.getResultSender().lastResult(lv_rp);
    }

    Row get(long      p_transaction_id,
		EsgynMGet p_get) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - Enter get()" 
					    + ", txId: " + p_transaction_id
					    );

	checkBlockNonPhase2(p_transaction_id);
	
	//TBD	Scan lv_mscan = new Scan(p_get.getRowKey());
	//TBD	lv_mscan.addColumns(p_get.getColumnNameList());

	AmpoolTransactionState lv_transaction = this.beginTransIfNotExist(p_transaction_id);
	
	Row lv_result = lv_transaction.get(p_get);
	if (lv_result != null) {
	    if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions - get(), transaction state returned results" 
						+ ", txn ID: " + p_transaction_id
						+ ", rowKey: " + new String(lv_result.getRowId())
						);
	}

	return lv_result;
    }

    void put(long p_transaction_id,
	     EsgynMPut p_put) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions.put" 
					    + ", txId: " + p_transaction_id
					    );
	checkBlockNonPhase2(p_transaction_id);

	AmpoolTransactionState lv_transaction = this.beginTransIfNotExist(p_transaction_id);

	lv_transaction.addWrite(p_put);
    }

    void delete(long p_transaction_id,
		EsgynMDelete p_delete) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions.delete" 
					    + ", txId: " + p_transaction_id
					    );
	checkBlockNonPhase2(p_transaction_id);
	
	AmpoolTransactionState lv_transaction = this.beginTransIfNotExist(p_transaction_id);

	lv_transaction.addDelete(p_delete);
    }

    @Override
	public String getId() {
	return this.getClass().getName();
    }


    public boolean perform_check_and_put(
					 long       p_transaction_id,
					 byte[]     p_row_key,
					 byte[]     p_column,
					 byte[]     p_value,
					 EsgynMPut  p_put
					 )
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions.perform_check_and_put" 
					    + ", txId: " + p_transaction_id
					    );
	boolean lv_result = false;

	try {
	    EsgynMGet get = new EsgynMGet(p_row_key);
	    if (p_column != null) {
		get.addColumn(p_column);
	    }
	    Row rs = this.get(p_transaction_id, get);
	    if (LOG.isTraceEnabled()) LOG.trace("perform_check_and_put: 1"
						+ " result set is empty: " + rs.isEmpty()
						);
	    
	    boolean valueIsNull = ((p_value == null) ||
				   (p_value.length == 0));

	    if (LOG.isTraceEnabled()) LOG.trace("perform_check_and_put: 2" 
						+ " input value is null: " + valueIsNull
						);
	    if (rs.isEmpty() && valueIsNull) {
		if (LOG.isTraceEnabled()) LOG.info("perform_check_and_put: 5, performing the put");
		this.put(p_transaction_id, p_put);
		lv_result = true;
	    } else if (!rs.isEmpty() && valueIsNull) {
		if (LOG.isTraceEnabled()) LOG.trace("TxnCP: checkAndPut" 
						    + ", txId: " + p_transaction_id 
						    + ", rowKey: " + Bytes.toStringBinary(p_row_key) 
						    + ", first check, setting result to false"
						    );
		lv_result = false;
	    }
	    else if (!rs.isEmpty() && !valueIsNull) {
		if (LOG.isInfoEnabled()) LOG.info("perform_check_and_put: 6");
		Cell lv_cell = rs.getCells().get(0);
		if (Bytes.equals((byte[]) lv_cell.getColumnValue(),
				 p_value)) {
		    if (LOG.isInfoEnabled()) LOG.info("perform_check_and_put: 6.1");
		    this.put(p_transaction_id, p_put);
		    lv_result = true;
		}
	    } else {
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: checkAndPut" 
						  + ", txid: " + p_transaction_id 
						  + ", rowKey: " + Bytes.toStringBinary(p_row_key) 
						  + ", second check, setting result to false"
						  );
		lv_result = false;
	    }
	} catch (Exception e) {
	    if (LOG.isWarnEnabled()) LOG.warn("TxnCP: checkAndPut"
					      + " txId: " + p_transaction_id 
					      + ", returning false"
					      + ", Caught internal exception ",
					      e
					      );
	    throw e;
	}

	return lv_result;
    }

    /**begin transaction if not yet
     * @param transactionId
     * @return true: begin; false: not necessary to begin
     * @throws IOException
     */

    private AmpoolTransactionState beginTransIfNotExist(final long p_transaction_id) 
	throws IOException
    {

	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions.beginTransIfNotExist"
					    + ", txId: " + p_transaction_id
					    + ", transactionsById size: " + transactionsById.size()
					    );
	checkBlockNewTrans(p_transaction_id);

	String key = getTransactionalUniqueId(p_transaction_id);
	//TBD - NGG	synchronized (transactionsById) 
	{
	    AmpoolTransactionState state = transactionsById.get(key);
	    if (state == null) {
		if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactions.beginTransIfNotExist"
						    + ", txId: " + p_transaction_id
						    + ", txKey: " + key
						    + ", beginning the transaction internally as state was null"
						    );
		this.beginTransaction(p_transaction_id);
		state =  transactionsById.get(key);
	    }
	    return state;
	}
    }

    /**
     * Obtains a transactional lease id                            
     * @param long transactionId
     * @return String 
     */
    private String getTransactionalUniqueId(final long transactionId) 
    {
	String lstring = m_region.getRegionInfo().getRegionNameAsString() + transactionId;
	//TBD      String lstring = m_region.toString() + transactionId;
	//TBD      String lstring = Long.toString(transactionId);
	if (LOG.isTraceEnabled()) {
	    LOG.trace("TxnCP: getTransactionalUniqueId -- EXIT txId: " 
		      + transactionId + " transactionsById size: "
		      + transactionsById.size() + " name " + lstring);
	}

	return lstring;
    }

    // Internal support methods
    /**
     * Checks if the region is closing and needs to block all activity
     * @param long transactionId
     * @return String 
     * @throws IOException 
     */
    private void checkBlockAll(final long transactionId) throws IOException {
	if (blockAll.get()) {
	    if(LOG.isWarnEnabled()) LOG.warn("AmpoolTransactions.checkBlockAll"
					     + ", txId: " + transactionId
					     + ", No Transactional activity allowed.");
	    //TBD      throw new IOException("closing region, no more transactional activity allowed. Region: " 
	    //TBD + regionInfo.getRegionNameAsString());
	    throw new IOException("closing region, no more transactional activity allowed. Region: " + m_region);
	}
    }

    /**
     * Checks if the region is closing and needs to block non phase 2 activity
     * @param long transactionId
     * @return String 
     * @throws IOException 
     */
    private void checkBlockNonPhase2(final long transactionId) throws IOException {
	if (blockNonPhase2.get()) {
	    if(LOG.isWarnEnabled()) LOG.warn("AmpoolTransactions.checkBlockNonPhase2"
					     + ", txId: " + transactionId
					     + ", No Transactional activity allowed."
					     );
	    throw new IOException("closing region, no more non phase 2 transactional activity allowed."
				  + " Region: " + m_region
				  );
	}
    
	// sometimes we only set the most sever in which case we always need to check the higher up levels
	checkBlockAll(transactionId);
    }

    /**
     * Checks if new transactions are disabled
     * @param long transactionId
     * @return String 
     * @throws IOException 
     */
    private void checkBlockNewTrans(final long transactionId) throws IOException {
	if (blockNewTrans.get()) {
	    if(LOG.isWarnEnabled()) LOG.warn("AmpoolTransactionscheckNewTrans"
					     + ", txId: " + transactionId
					     + ", No more new transactions allowed."
					     );
	    throw new IOException("closing region, no more new transactions allowed." 
				  + " Region: " + m_region
				  );
	}

	// sometimes we only set the most sever in which case we always need to check the higher up levels
	checkBlockNonPhase2(transactionId);
    }

    public void startRegionAfterRecovery() throws IOException {
	boolean isFlush = false;

	try {
	    if (LOG.isInfoEnabled()) LOG.info("TxnCP: Trafodion Recovery:  Flushing cache in startRegionAfterRecovery" 
					      + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
					      );
	    m_region.flushcache();
	    //if (!m_region.flushcache().isFlushSucceeded()) { 
	    //   LOG.info("TxnCP: Trafodion Recovery:  Flushcache returns false !!! " + m_region.getRegionInfo().getRegionNameAsString());
	    //}
	} catch (Exception e) {
	    LOG.error("TxnCP: Trafodion Recovery: Flush failed after replay edits" 
		      + m_region.getRegionInfo().getRegionNameAsString() 
		      + ", Caught exception " + e.toString()
		      , e
		      );
	    return;
	}

	//FileSystem fileSystem = m_region.getFilesystem();
	//Path archiveTHLog = new Path (recoveryTrxPath.getParent(),"archivethlogfile.log");
	//if (fileSystem.exists(archiveTHLog)) fileSystem.delete(archiveTHLog, true);
	//if (fileSystem.exists(recoveryTrxPath))fileSystem.rename(recoveryTrxPath,archiveTHLog);
	if (indoubtTransactionsById != null)
	    if (LOG.isInfoEnabled()) LOG.info("TxnCP: Trafodion Recovery: region " + recoveryTrxPath + " has " + indoubtTransactionsById.size() + " in-doubt transactions and edits are archived.");
	    else
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: Trafodion Recovery: region " + recoveryTrxPath + " has 0 in-doubt transactions and edits are archived.");
	regionState = REGION_STATE_START; 
	if (LOG.isInfoEnabled()) LOG.info("TxnCP: Trafodion Recovery: region " + m_region.getRegionInfo().getEncodedName() + " is STARTED.");
    }

    //TBD  public void constructIndoubtTransactions() {
    //TBD
    //TBD      synchronized (recoveryCheckLock) {
    //TBD            if ((indoubtTransactionsById == null) || (indoubtTransactionsById.size() == 0)) {
    //TBD              if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV endpoint CP: Region " + regionInfo.getRegionNameAsString() + " has no in-doubt transaction, set region START ");
    //TBD              regionState = REGION_STATE_START; // region is started for transactional access
    //TBD              reconstructIndoubts = 1; 
    //TBD              try {
    //TBD              startRegionAfterRecovery();
    //TBD               } catch (IOException exp1) {
    //TBD                    if (LOG.isDebugEnabled()) LOG.info("Trafodion Recovery: flush error during region start");
    //TBD               }
    //TBD              return;
    //TBD            }
    //TBD
    //TBD	    if ((m_isTrafodionMetadata) || 
    //TBD		(LOG.isInfoEnabled())) 
    //TBD		LOG.info("Trafodion Recovery Endpoint Coprocessor: Trafodion Recovery RegionObserver to Endpoint coprocessor "
    //TBD			 + "data exchange test try to access indoubt transaction list with size " 
    //TBD			 + indoubtTransactionsById.size());
    //TBD
    //TBD            if (reconstructIndoubts == 0) {
    //TBD            //Retrieve (tid,Edits) from indoubt Transaction and construct/add into desired transaction data list
    //TBD            for (Entry<Long, List<WALEdit>> entry : indoubtTransactionsById.entrySet()) {
    //TBD                      long txId = 0;
    //TBD                      long transactionId = entry.getKey();
    //TBD		      String key = String.valueOf(transactionId);
    //TBD                      ArrayList<WALEdit> editList = (ArrayList<WALEdit>) entry.getValue();
    //TBD                      //editList = (ArrayList<WALEdit>) indoubtTransactionsById.get(transactionId);
    //TBD		      if ((m_isTrafodionMetadata) || 
    //TBD			  (LOG.isInfoEnabled())) 
    //TBD			  LOG.info("Trafodion endpoint CP: reconstruct transaction in Region " 
    //TBD				   + regionInfo.getRegionNameAsString() + " process in-doubt transaction " + transactionId);
    //TBD		      AmpoolTransactionState state = new AmpoolTransactionState(transactionId, /* 1L my_Region.getLog().getSequenceNumber()*/
    //TBD										nextLogSequenceId.getAndIncrement(), 
    //TBD										nextLogSequenceId, 
    //TBD										regionInfo, 
    //TBD										m_region.getTableDesc(), 
    //TBD										tHLog,
    //TBD										false);
    //TBD
    //TBD		      if ((m_isTrafodionMetadata) || 
    //TBD			  (LOG.isInfoEnabled())) 
    //TBD			  LOG.info("Trafodion endpointCP: reconstruct transaction in Region " 
    //TBD				   + regionInfo.getRegionNameAsString() + " create transaction state for " + transactionId);
    //TBD
    //TBD                      state.setFullEditInCommit(true);
    //TBD		      state.setStartSequenceNumber(nextSequenceId.get());
    //TBD    		      transactionsById.put(getTransactionalUniqueId(transactionId), state);
    //TBD
    //TBD                      // Re-establish write ordering (put and get) for in-doubt transactional
    //TBD                     int num  = editList.size();
    //TBD                     if (LOG.isInfoEnabled()) LOG.info("TrxRegion endpoint CP: reconstruct transaction " + transactionId + ", region " + regionInfo.getRegionNameAsString() +
    //TBD                               " with number of edit list kvs size " + num);
    //TBD                    for (int i = 0; i < num; i++){
    //TBD                          WALEdit b = editList.get(i);
    //TBD                          if (LOG.isInfoEnabled()) LOG.info("TrxRegion endpoint CP: reconstruction transaction " + transactionId + ", region " + regionInfo.getRegionNameAsString() +
    //TBD                               " with " + b.size() + " kv in WALEdit " + i);
    //TBD                          for (Cell kv : b.getCells()) {
    //TBD                             Put put;
    //TBD                             Delete del;
    //TBD                             synchronized (editReplay) {
    //TBD                             if (LOG.isInfoEnabled()) LOG.info("TrxRegion endpoint CP:reconstruction transaction " + transactionId + ", region " + regionInfo.getRegionNameAsString() +
    //TBD                               " re-establish write ordering Op Code " + kv.getTypeByte());
    //TBD                             if (kv.getTypeByte() == KeyValue.Type.Put.getCode()) {
    //TBD                               put = new Put(CellUtil.cloneRow(kv)); // kv.getRow()
    //TBD                                put.add(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv), kv.getTimestamp(), CellUtil.cloneValue(kv));
    //TBD                                state.addWrite(put);
    //TBD                             }
    //TBD                             else if (CellUtil.isDelete(kv))  {
    //TBD                               del = new Delete(CellUtil.cloneRow(kv));
    //TBD                                if (CellUtil.isDeleteFamily(kv)) {
    //TBD                                   del.deleteFamily(CellUtil.cloneFamily(kv));
    //TBD                                } else if (CellUtil.isDeleteType(kv)) {
    //TBD                                   del.deleteColumn(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv));
    //TBD                                }
    //TBD                                 state.addDelete(del);
    //TBD
    //TBD                             } // handle put/delete op code
    //TBD                             } // sync editReplay
    //TBD                          } // for all kv in edit b
    //TBD                    } // for all edit b in ediList 
    //TBD                      state.setReinstated();
    //TBD		      state.setStatus(TransactionState.Status.COMMIT_PENDING);
    //TBD		      commitPendingTransactions.add(state);
    //TBD		      state.setSequenceNumber(nextSequenceId.getAndIncrement());
    //TBD		      commitedTransactionsBySequenceNumber.put(state.getSequenceNumber(), state);
    //TBD		      if ((m_isTrafodionMetadata) || 
    //TBD			  (LOG.isInfoEnabled())) 
    //TBD			  LOG.info("TrxRegion endpoint CP: reconstruct transaction " 
    //TBD				   + transactionId + ", region " + regionInfo.getRegionNameAsString() +
    //TBD				   " complete in prepared state");
    //TBD
    //TBD                     // Rewrite Hlogger for prepared edit (this method should be invoked in postOpen Observer ??
    //TBD                    try {
    //TBD                       //txid = this.tHLog.appendNoSync(this.regionInfo, this.regionInfo.getTable(),
    //TBD                       //state.getEdit(), new ArrayList<UUID>(), TrxEnvironmentEdgeManager.currentTimeMillis(), this.m_region.getTableDesc(),
    //TBD                       //nextLogSequenceId, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
    //TBD                       
    //TBD                       final WALKey wk = new WALKey(this.regionInfo.getEncodedNameAsBytes(), 
    //TBD						    this.regionInfo.getTable(),
    //TBD						    TrxEnvironmentEdgeManager.currentTime());;
    //TBD		       txid = this.tHLog.append(this.m_region.getTableDesc(),
    //TBD						this.regionInfo, 
    //TBD						wk , 
    //TBD						state.getEdit(),
    //TBD						this.m_region.getSequenceId(), 
    //TBD						false, 
    //TBD						null);
    //TBD		       
    //TBD		       if ((m_isTrafodionMetadata) || 
    //TBD			   (LOG.isInfoEnabled())) 
    //TBD			   LOG.info("TxnCP: commitRequest COMMIT_OK -- EXIT txId: " 
    //TBD				    + transactionId 
    //TBD				    + " HLog seq " 
    //TBD				    + txid
    //TBD				    );
    //TBD                       this.tHLog.sync(txid);
    //TBD                    }
    //TBD                    catch (IOException exp) {
    //TBD                       LOG.warn("TrxRegion endpoint CP: reconstruct transaction - Caught IOException in Hlogger appendNoSync -- EXIT txId: " + transactionId + " HLog seq " + txid);
    //TBD                       //throw exp;
    //TBD                    }   
    //TBD                    if (LOG.isInfoEnabled()) LOG.info("TrxRegion endpoint CP: reconstruct transaction: rewrite to HLOG CR edit for transaction " + transactionId);
    //TBD		    int clusterid = (int) TransactionState.getClusterId(transactionId);
    //TBD		    int tmid = (int) TransactionState.getNodeId(transactionId);
    //TBD                    if (LOG.isInfoEnabled()) LOG.info("TrxRegion endpoint CP " + regionInfo.getRegionNameAsString() + " reconstruct transaction " + transactionId + " for Cluster " + clusterid + " TM " + tmid);
    //TBD             } // for all txns in indoubt transcation list
    //TBD	    } // not reconstruct indoubtes yet
    //TBD             reconstructIndoubts = 1;
    //TBD             if (this.configuredConflictReinstate) {
    //TBD	         regionState = REGION_STATE_START; // set region state START , so new transaction can start with conflict re-established
    //TBD             }
    //TBD        } // synchronized
    //TBD  }

    public void beginTransaction(final long transactionId)
	throws IOException 
    {

	if (LOG.isTraceEnabled()) LOG.trace("TxnCP: beginTransaction -- ENTRY" 
					    + ", txId: " + transactionId
					    );

	checkBlockNonPhase2(transactionId);

	// TBD until integration with recovery 
	//TBD    if (reconstructIndoubts == 0) {
	//TBD       if (LOG.isInfoEnabled()) LOG.info("TxnCP: RECOV beginTransaction -- ENTRY txId: " + transactionId);
	//TBD       constructIndoubtTransactions();
	//TBD    }

	if (regionState != REGION_STATE_START) {
	    //print out all the in-doubt transaction at this moment
	    if ((indoubtTransactionsById == null) || (indoubtTransactionsById.size() == 0))
		regionState = REGION_STATE_START;
	    else {
		LOG.warn("TRAF RCOV coprocessor: RECOVERY WARN beginTransaction while the region is still in recovering state " 
			 +  regionState + " indoubt tx size " 
			 + indoubtTransactionsById.size()
			 );
		for (Entry<Long, List<WALEdit>> entry : indoubtTransactionsById.entrySet()) {
		    long tid = entry.getKey();
		    if (LOG.isInfoEnabled()) LOG.info("Trafodion Recovery" 
						      + ", region: " + regionInfo.getEncodedName() 
						      + " still has in-doubt transaction" 
						      + ", txId: " + tid 
						      + " when new transaction arrives"
						      );
		}
		throw new IOException("NewTransactionStartedBeforeRecoveryCompleted");
	    }
	}

	AmpoolTransactionState state;
	//TBD - NGG synchronized (transactionsById) 
	{
	    if (transactionsById.get(getTransactionalUniqueId(transactionId)) != null) {
		AmpoolTransactionState alias = getTransactionState(transactionId);

		LOG.error("TxnCP: beginTransaction - Ignoring - Existing transaction" 
			  + ", txId: " + transactionId 
			  + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
			  );
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: beginTransaction -- EXIT txId: " + transactionId);

		return;
	    }

	    if (LOG.isTraceEnabled()) LOG.trace("TxnCP: beginTransaction -- creating new AmpoolTransactionState" 
						+ ",  txId: " + transactionId
						);

	    state = new AmpoolTransactionState(transactionId,
					       nextLogSequenceId.getAndIncrement(),
					       nextLogSequenceId,
					       m_region.getRegionInfo(),
					       m_region.getTableDesc(), 
					       tHLog, 
					       configuredEarlyLogging);

	    state.setFullEditInCommit(this.fullEditInCommit);
	    state.setStartSequenceNumber(nextSequenceId.get());
	}

	List<AmpoolTransactionState> commitPendingCopy = 
	    new ArrayList<AmpoolTransactionState>(commitPendingTransactions);

	for (AmpoolTransactionState commitPending : commitPendingCopy) {
            state.addTransactionToCheck(commitPending);
	}

	String key = getTransactionalUniqueId(transactionId);
	if (LOG.isTraceEnabled()) LOG.trace("TxnCP: beginTransaction -- new AmpoolTransactionState" 
					    + ", txId: " + transactionId 
					    + ", key: " + key
					    );
	//TBD - NGG synchronized (transactionsById) 
	{
	    transactionsById.put(key, state);
	}

	try {
	    if (this.transactionLeases == null) {
		this.transactionLeases = new Leases(LEASE_CHECK_FREQUENCY);
	    }
	    transactionLeases.createLease(key, transactionLeaseTimeout, new TransactionLeaseListener(transactionId));
	} catch (LeaseStillHeldException e) {
	    LOG.error("TxnCP: beginTransaction - Lease still held for [" + transactionId + "] in region ["
		      + m_region.getRegionInfo().getRegionNameAsString() + "]");

	    throw new RuntimeException(e);
	}

	if (LOG.isInfoEnabled()) LOG.info("TxnCP: beginTransaction - Added to list -- EXIT" 
					  + ", txId:" + transactionId 
					  + ", region: " + m_region.getRegionInfo().getRegionNameAsString() 
					  + ", transactionsById size: " + transactionsById.size()
					  );
    }

    /**
     * Gets the transaction state                   
     * @param long transactionId
     * @return AmpoolTransactionState
     * @throws UnknownTransactionException
     */
    protected AmpoolTransactionState getTransactionState(final long transactionId)
	throws UnknownTransactionException {
	AmpoolTransactionState state = null;
	boolean throwUTE = false;

	String key = getTransactionalUniqueId(transactionId);
	state = transactionsById.get(key);

	if (state == null) 
	    {
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: getTransactionState Unknown txId: " + transactionId + ", throwing UnknownTransactionException");
		throwUTE = true;
	    }
	else {
	    if (LOG.isTraceEnabled()) LOG.trace("TxnCP: getTransactionState Found txId: " + transactionId );

	    try {
		transactionLeases.renewLease(key);
	    } catch (LeaseException e) {
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: getTransactionState renewLease failed will try to createLease"
						  + ", txId: " + transactionId
						  );
		try {
		    transactionLeases.createLease(key, transactionLeaseTimeout, new TransactionLeaseListener(transactionId));
		} catch (LeaseStillHeldException lshe) {
		    if (LOG.isInfoEnabled()) LOG.info("TxnCP: getTransactionState renewLeasefollowed by createLease failed throwing original LeaseException for txId: " + transactionId);
		    throw new RuntimeException(e);
		}
	    }
	}

	if (throwUTE)
	    throw new UnknownTransactionException();

	return state;
    }

    /**
     * Simple helper class that just keeps track of whether or not its stopped.
     */
    private static class StoppableImplementation implements Stoppable {
	private volatile boolean stop = false;

	@Override
	    public void stop(String why) {
	    this.stop = true;
	}

	@Override
	    public boolean isStopped() {
	    return this.stop;
	}
    }

    /**
     * TransactionLeaseListener
     */
    private class TransactionLeaseListener implements LeaseListener {

	//private final long transactionName;
	private final String transactionName;

	TransactionLeaseListener(final long n) {
	    this.transactionName = getTransactionalUniqueId(n);
	}

	public void leaseExpired() {
	    long transactionId = 0L;
	    if (LOG.isInfoEnabled()) LOG.info("TxnCP: leaseExpired Transaction [" + this.transactionName
					      + "] expired in region ["
					      + m_region.getRegionInfo().getRegionNameAsString() + "]");
	    AmpoolTransactionState s = null;
	    synchronized (transactionsById) {
		s = (AmpoolTransactionState) transactionsById.remove(transactionName);
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: leaseExpired Removing Transaction: " + this.transactionName + " from list");
	    }
	    if (s == null) {
		LOG.warn("leaseExpired Unknown transaction expired " + this.transactionName);
		return;
	    }

	    transactionId = s.getTransactionId(); 

	    switch (s.getStatus()) {
	    case PENDING:
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: leaseExpired transaction " + transactionId + " was PENDING, calling retireTransaction");
		s.setStatus(TransactionState.Status.ABORTED);  
		//TBD       retireTransaction(s, true);
		break;
	    case COMMIT_PENDING:
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: leaseExpired  Transaction " + transactionId
						  + " expired in COMMIT_PENDING state");

		String key = getTransactionalUniqueId(transactionId);
		try {
		    if (s.getCommitPendingWaits() > MAX_COMMIT_PENDING_WAITS) {
			if (LOG.isInfoEnabled()) LOG.info("TxnCP: leaseExpired  Checking transaction status in transaction log");
			//TBD            resolveTransactionFromLog(s);
			break;
		    }
		    if (LOG.isInfoEnabled()) LOG.info("TxnCP: leaseExpired  renewing lease and hoping for commit");
		    s.incrementCommitPendingWaits();
		    synchronized (transactionsById) {
			transactionsById.put(key, s);
			if (LOG.isInfoEnabled()) LOG.info("TxnCP: leaseExpired  Adding transaction: " + transactionId + " to list");
		    }
		    try {
			transactionLeases.createLease(key, transactionLeaseTimeout, this);
		    } catch (LeaseStillHeldException e) {
			transactionLeases.renewLease(key);
		    }
		} catch (IOException e) {
		    throw new RuntimeException(e);
		}

		break;

	    default:
		LOG.warn("TxnCP: leaseExpired  Unexpected status on expired lease");
	    }
	}
    }

    /**
     * Commits the transaction
     * @param long TransactionId
     * @throws IOException
     */
    public void commit(final long transactionId, final int participantNum) throws IOException {
	commit(transactionId, participantNum, false /* IgnoreUnknownTransactionException */);
    }

    /**
     * Commits the transaction                        
     * @param long TransactionId
     * @param boolean ignoreUnknownTransactionException
     * @throws IOException 
     */
    public void commit(final long transactionId, 
		       final int participantNum, 
		       final boolean ignoreUnknownTransactionException
		       ) 
	throws IOException 
    {
	if (LOG.isTraceEnabled()) LOG.trace("TxnCP: commit(txId) -- ENTRY"
					    + ", txId: " + transactionId 
					    + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
					    + ", #transactionsById: " + transactionsById.size() 
					    + ", #commitedTransactionsBySequenceNumber: " + commitedTransactionsBySequenceNumber.size() 
					    + ", #commitPendingTransactions: " + commitPendingTransactions.size()
					    + ", participantNum: " + participantNum
					    + ", ignoreUnknownTransaction: " + ignoreUnknownTransactionException
					    );
	TransactionState.CommitProgress commitStatus = TransactionState.CommitProgress.NONE;
	AmpoolTransactionState state;

	checkBlockAll(transactionId);

	try {
	    state = getTransactionState(transactionId);
	} 
	catch (UnknownTransactionException e) {
	    if (ignoreUnknownTransactionException == true) {
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: ignoring UnknownTransactionException in commit"
						  + ", txId: " + transactionId 
						  + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
						  + ", participantNum: " + participantNum
						  + ", ignoreUnknownTransaction: " + ignoreUnknownTransactionException
						  );
		return;
	    }
	    LOG.fatal("TxnCP: Asked to commit unknown transaction"
		      + ", txId: " + transactionId 
		      + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
		      + ", participantNum: " + participantNum
		      + ", ignoreUnknownTransaction: " + ignoreUnknownTransactionException
		      );

	    throw new IOException("UnknownTransactionException"
				  + ", txId: " + transactionId 
				  + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
				  + ", participantNum: " + participantNum
				  + ", ignoreUnknownTransaction: " + ignoreUnknownTransactionException
				  );
	}

	if (!state.getStatus().equals(TransactionState.Status.COMMIT_PENDING)) {
	    LOG.fatal("TxnCP: commit - Asked to commit a non pending transaction"
		      + ", state: " + state.getStatus()
		      + ", txId: " + transactionId 
		      + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
		      + ", participantNum: " + participantNum
		      + ", ignoreUnknownTransaction: " + ignoreUnknownTransactionException
		      );

	    throw new IOException("Asked to commit a non-pending transaction, transid: " + transactionId +
				  " state: " + state.getStatus());
	}

	// manage concurrent duplicate commit requests through TS.xaOperation object

	synchronized(state.getXaOperationObject()) {
	    commitStatus = state.getCommitProgress();
	    // already committed, this is likely unnecessary due to Status check above
	    if (commitStatus == TransactionState.CommitProgress.COMMITED) { 
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit - duplicate commit for committed transaction ");
	    }
	    else if (commitStatus == TransactionState.CommitProgress.COMMITTING) {
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit - duplicate commit during committing transaction ");
		try {
		    Thread.sleep(1000);          ///1000 milliseconds is one second.
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
	    }
	    else if (commitStatus == TransactionState.CommitProgress.NONE) {
		state.setCommitProgress(TransactionState.CommitProgress.COMMITTING);
		if (LOG.isTraceEnabled()) LOG.trace("TxnCP: commit status changed."
						    + ", from: " + commitStatus
						    + ", to: " + state.getCommitProgress()
						    );
		commit(state,
		       participantNum,
		       ignoreUnknownTransactionException);

		if (LOG.isTraceEnabled()) LOG.trace("TxnCP: commit(txId) -- EXIT txId: " + transactionId);
	    } 
	}

    }

    public void deleteRecoveryzNode(int node, String encodedName) throws IOException {

	synchronized(zkRecoveryCheckLock) {
	    // default zNodePath
	    String zNodeKey = lv_hostName + "," + lv_port + "," + encodedName;

	    StringBuilder sb = new StringBuilder();
	    sb.append("TM");
	    sb.append(node);
	    String str = sb.toString();
	    String zNodePathTM = zNodePath + str;
	    String zNodePathTMKey = zNodePathTM + "/" + zNodeKey;
	    if (LOG.isTraceEnabled()) LOG.trace("Trafodion Recovery Region Observer CP: ZKW Delete region recovery znode" + node + " zNode Path " + zNodePathTMKey);
	    // delete zookeeper recovery zNode, call ZK ...
	    try {
		ZKUtil.deleteNodeFailSilent(zkw1, zNodePathTMKey);
	    } catch (KeeperException e) {
		throw new IOException("Trafodion Recovery Region Observer CP: ZKW Unable to delete recovery zNode to TM " + node, e);
	    }
	}
    } // end of deleteRecoveryzNode


    /**
     * Commits the transaction
     * @param AmpoolTransactionState state
     * @throws IOException
     */
    private void commit(final AmpoolTransactionState state,
			final int participantNum, 
			final boolean ignoreUnknownTransactionException
			) 
	throws IOException 
    {
	long txid = 0;
	WALEdit b = null;
	int num = 0;
	Tag commitTag;
	ArrayList<WALEdit> editList;
	long transactionId = state.getTransactionId();

	if (LOG.isTraceEnabled()) LOG.trace("TxnCP:commit" 
					    + ", txId: " + transactionId 
					    + ", region: " + m_region.getRegionInfo().getRegionNameAsString() 
					    + ", transactionsById: " + transactionsById.size() 
					    + ", commitedTransactionsBySequenceNumber: " + commitedTransactionsBySequenceNumber.size() 
					    + ", commitPendingTransactions: " + commitPendingTransactions.size()
					    );

	if (state.isReinstated() && !this.configuredConflictReinstate) {
	    if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit" 
					      + " Trafodion Recovery: commit reinstated indoubt transaction " + transactionId 
					      + " in region " + m_region.getRegionInfo().getRegionNameAsString()
					      );
	    //TBD - NGG synchronized (indoubtTransactionsById) 
	    {  
		editList = (ArrayList<WALEdit>) indoubtTransactionsById.get(transactionId);
	    }
	    num  = editList.size();
	    if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit -" 
					      + " txId " + transactionId 
					      + ", region " + regionInfo.getRegionNameAsString() 
					      + ", Redrive commit with number of edit kvs list size " + num
					      );
	    for ( int i = 0; i < num; i++){
		b = editList.get(i);
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit -"
						  + ", txId" + transactionId 
						  + ", Writing " + b.size() + " updates for reinstated transaction"
						  );
		for (org.apache.hadoop.hbase.Cell kv : b.getCells()) {
		    synchronized (editReplay) {
			EsgynMPut put;
			EsgynMDelete del;
			if (LOG.isInfoEnabled()) LOG.info("TxnCP:commit -"
							  + ", txId " + transactionId 
							  + ", Trafodion Recovery: region " + m_region.getRegionInfo().getRegionNameAsString() 
							  + ", Replay commit for transaction with Op " + kv.getTypeByte()
							  );
			if (kv.getTypeByte() == KeyValue.Type.Put.getCode()) {
			    //TBD                put = new Put(CellUtil.cloneRow(kv), kv.getTimestamp()); // kv.getRow()
			    put = new EsgynMPut(CellUtil.cloneRow(kv)); // kv.getRow()
			    put.setTimeStamp(kv.getTimestamp());
			    //TBD - Decide the contents of WALEdit first
			    //TBD put.add(CellUtil.cloneFamily(kv),
			    //TBD                        CellUtil.cloneQualifier(kv),
			    //TBD   kv.getTimestamp(),
			    //TBD   CellUtil.cloneValue(kv));
			    //state.addWrite(put); // no need to add since add has been done in constructInDoubtTransactions
			    try {
				m_region.put(put);
			    }
			    catch (Exception e) {
				LOG.warn("TxnCP: commit -" 
					 + ", txId " + transactionId 
					 + ", Trafodion Recovery: Executing put caught an exception " + e.toString()
					 );
				throw new IOException(e.toString());
			    }
			} 
			else if (CellUtil.isDelete(kv))  {
			    //TBD                del = new Delete(CellUtil.cloneRow(kv), kv.getTimestamp());
			    del = new EsgynMDelete(CellUtil.cloneRow(kv));
			    del.setTimestamp(kv.getTimestamp());
			    //TBD                if (CellUtil.isDeleteFamily(kv)) {
			    //TBD                   del.deleteFamily(CellUtil.cloneFamily(kv), kv.getTimestamp());
			    //TBD                } else if (CellUtil.isDeleteType(kv)) {
			    //TBD                   del.deleteColumn(CellUtil.cloneFamily(kv),
			    //TBD                		   CellUtil.cloneQualifier(kv), kv.getTimestamp());
			    //TBD	     }
			    //state.addDelete(del);  // no need to add since add has been done in constructInDoubtTransactions
			    try {
				m_region.delete(del);
			    }
			    catch (Exception e) {
				LOG.warn("TxnCP: commit -" 
					 + " txId " + transactionId 
					 + ", Trafodion Recovery: Executing delete caught an exception " + e.toString()
					 );
				throw new IOException(e.toString());
			    }
			}
		    } // synchronized reply edits
		} // for WALEdit
	    } // for ediList
	}  // reinstated transactions
	else { // either non-reinstated transaction, or reinstate transaction with conflict reinstate TRUE (write from TS write ordering)
	    // Perform write operations timestamped to right now
	    // maybe we can turn off WAL here for HLOG since THLOG has contained required edits in phase 1

	    ListIterator<AmpoolTransactionState.WriteAction> writeOrderIter = null;
	    int size = state.writeSize();
	    int index = 0;
	    for (writeOrderIter = state.getWriteOrderingIter();
		 writeOrderIter.hasNext();) {
		AmpoolTransactionState.WriteAction action =(AmpoolTransactionState.WriteAction) writeOrderIter.next();
		// Process Put
		EsgynMPut put = action.getPut();

		if (null != put) {
		    if (this.skipWal){
			put.setDurability(Durability.SKIP_WAL); 
		    }
		    else { 
			if (this.asyncWal != 0) {
			    put.setDurability(Durability.ASYNC_WAL); 
			    if ((index == 0) && ((this.asyncWal == 3) || (this.asyncWal == 4))) {
				put.setDurability(Durability.SYNC_WAL); 
			    }
			    if ((index == (size - 1)) && ((this.asyncWal == 2)  || (this.asyncWal == 4))) {
				put.setDurability(Durability.SYNC_WAL); 
			    }
			}
		    }
		    if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit, Executing put"
						      + ", txId: " + transactionId 
						      + ", put: " + put
						      + ", index: " + index 
						      + ", durability: " + put.getDurability().toString() 
						      + " directly to m_region");
		    try {
			m_region.put(put);
		    }
		    catch (Exception e) {
			LOG.warn("TxnCP: commit -" 
				 + ", txId: " + transactionId 
				 + ", Executing put caught an exception " + e.toString()
				 , e
				 );
			throw new IOException(e.toString());
		    }
		}

		// Process Delete
		EsgynMDelete delete = action.getDelete();

		if (null != delete){
		    if (this.skipWal){
			delete.setDurability(Durability.SKIP_WAL); 
		    }
		    else { 
			if (this.asyncWal != 0) {
			    delete.setDurability(Durability.ASYNC_WAL); 
			    if ((index == 0) && ((this.asyncWal == 3) || (this.asyncWal == 4))) {
				delete.setDurability(Durability.SYNC_WAL);
			    }
			    if ((index == (size - 1)) && ((this.asyncWal == 2)  || (this.asyncWal == 4))) {
				delete.setDurability(Durability.SYNC_WAL); 
			    }
			}
		    }
		    if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit, Executing delete"
						      + ", txId: " + transactionId 
						      + ", delete: " + delete
						      + ", index: " + index 
						      + ", durability: " + delete.getDurability().toString() 
						      + " directly to m_region");
		    try {
			m_region.delete(delete);
		    }
		    catch (Exception e) {
			LOG.warn("TxnCP: commit  -" 
				 + " txId: " + transactionId 
				 + ", Executing delete caught an exception " + e.toString()
				 );
			throw new IOException(e.toString());
		    }
		}
		index++;
	    }
	} // normal transactions

	// Now write a commit edit to HLOG

	//TBD    List<Tag> tagList = new ArrayList<Tag>();
	//TBD    if (state.hasWrite() || state.isReinstated()) {
	//TBD         if (!state.getFullEditInCommit()) {
	//TBD            commitTag = state.formTransactionalContextTag(TS_COMMIT);
	//TBD            tagList.add(commitTag);
	//TBD            WALEdit e1 = state.getEdit();
	//TBD            WALEdit e = new WALEdit();
	//TBD            if (e1.isEmpty() || e1.getCells().size() <= 0) {
	//TBD               if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV endpoint CP: commit - txId "
	//TBD                        + transactionId
	//TBD                        + ", Encountered empty TS WAL Edit list during commit, HLog txid "
	//TBD                        + txid);
	//TBD            }
	//TBD            else {
	//TBD                 Cell c = e1.getCells().get(0);
	//TBD                 KeyValue kv = new KeyValue(c.getRowArray(), c.getRowOffset(), (int)c.getRowLength(),
	//TBD					c.getFamilyArray(), c.getFamilyOffset(), (int)c.getFamilyLength(),
	//TBD					c.getQualifierArray(), c.getQualifierOffset(), (int) c.getQualifierLength(),
	//TBD					c.getTimestamp(), Type.codeToType(c.getTypeByte()), c.getValueArray(), c.getValueOffset(),
	//TBD					c.getValueLength(), tagList);
	//TBD      
	//TBD                 e.add(kv);
	//TBD                 try {
	//TBD                	 final WALKey wk = new WALKey(this.regionInfo.getEncodedNameAsBytes(), 
	//TBD						      this.regionInfo.getTable(), 
	//TBD						      TrxEnvironmentEdgeManager.currentTime());
	//TBD			 AtomicLong lv_seqid = this.m_region.getSequenceId();
	//TBD                	 txid = this.tHLog.append(this.m_region.getTableDesc(),
	//TBD						  this.regionInfo, 
	//TBD						  wk , 
	//TBD						  e,
	//TBD						  lv_seqid,
	//TBD						  false, 
	//TBD						  null);
	//TBD                	
	//TBD                    if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit -" 
	//TBD							+ " txId " + transactionId 
	//TBD							+ ", Write commit HLOG seq " + txid
	//TBD							);
	//TBD                 }
	//TBD                 catch (IOException exp1) {
	//TBD                    if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV endpoint CP: commit -" 
	//TBD							+ " txId " + transactionId 
	//TBD							+ ", Writing to HLOG : Threw an exception " + exp1.toString()
	//TBD							);
	//TBD                    throw exp1;
	//TBD                 }
	//TBD            } // e1 is not empty
	//TBD         } // not full edit write in commit record during phase 2
	//TBD        else { //do this for  rollover case
	//TBD           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV endpoint CP:commit -- HLOG rollover txId: " + transactionId);
	//TBD           commitTag = state.formTransactionalContextTag(TS_CONTROL_POINT_COMMIT);
	//TBD           tagList.add(commitTag);
	//TBD           WALEdit e1 = state.getEdit();
	//TBD           WALEdit e = new WALEdit();
	//TBD
	//TBD           for (Cell c : e1.getCells()) {
	//TBD              KeyValue kv = new KeyValue(c.getRowArray(), c.getRowOffset(), (int)c.getRowLength(),
	//TBD					c.getFamilyArray(), c.getFamilyOffset(), (int)c.getFamilyLength(),
	//TBD					c.getQualifierArray(), c.getQualifierOffset(), (int) c.getQualifierLength(),
	//TBD					c.getTimestamp(), Type.codeToType(c.getTypeByte()), c.getValueArray(), c.getValueOffset(),
	//TBD					c.getValueLength(), 
	//TBD					 tagList);
	//TBD              e.add(kv);
	//TBD            }
	//TBD            try {
	//TBD
	//TBD                final WALKey wk = new WALKey(this.regionInfo.getEncodedNameAsBytes(), 
	//TBD					     this.regionInfo.getTable(), 
	//TBD					     TrxEnvironmentEdgeManager.currentTime());;
	//TBD           	 	txid = this.tHLog.append(this.m_region.getTableDesc(),
	//TBD						 this.regionInfo, 
	//TBD						 wk , 
	//TBD						 e,
	//TBD						 this.m_region.getSequenceId(), 
	//TBD						 false, 
	//TBD						 null);
	//TBD           	 	this.tHLog.sync(txid);
	//TBD                if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit -" 
	//TBD						    + " txId " + transactionId 
	//TBD						    + ", Y11 write commit HLOG seq " + txid
	//TBD						    );
	//TBD            }
	//TBD            catch (IOException exp1) {
	//TBD               if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV endpoint CP: commit -" 
	//TBD						   + " txId " + transactionId 
	//TBD						   + ", Writing to HLOG : Threw an exception " + exp1.toString()
	//TBD						   );
	//TBD               throw exp1;
	//TBD             }
	//TBD            if (LOG.isInfoEnabled()) LOG.info("TxnCP:commit -- EXIT" 
	//TBD						+ " txId: " + transactionId 
	//TBD						+ " HLog seq " + txid
	//TBD						);
	//TBD           // if (this.fullEditInCommit) this.fullEditInCommit = false;
	//TBD        } // else -- full edit write in commit record during phase 2
	//TBD    } // write or reinstated

	state.setStatus(TransactionState.Status.COMMITED);
	if (state.hasWrite() || state.isReinstated()) {
	    synchronized (commitPendingTransactions) {
		if (!commitPendingTransactions.remove(state)) {
		    LOG.fatal("TxnCP: commit -" 
			      + " txId: " + transactionId 
			      + ", Commiting a non-query transaction that is not in commitPendingTransactions"
			      );
		    // synchronized statements are cleared for a throw
		    throw new IOException("commit failure");
		}
	    }
	}

	if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit(tstate) -- EXIT" 
					  + ", transactionState: " + state.toString()
					  + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
					  + ", #transactionsById: " + transactionsById.size() 
					  + ", #commitedTransactionsBySequenceNumber: " + commitedTransactionsBySequenceNumber.size() 
					  + ", #commitPendingTransactions: " + commitPendingTransactions.size()
					  + ", participantNum: " + participantNum
					  + ", ignoreUnknownTransaction: " + ignoreUnknownTransactionException
					  );

	if (state.isReinstated()) {
	    synchronized(indoubtTransactionsById) {
		indoubtTransactionsById.remove(state.getTransactionId());
		int clusterid = (int) TransactionState.getClusterId(transactionId);
		int tmid = (int) TransactionState.getNodeId(transactionId);
		if (  (clusterid != pSTRConfig.getTrafClusterIdInt()) && 
                       ((clusterid != 1) && (pSTRConfig.getConfiguredPeerCount() == 0))    ) tmid = -2; // for any peer
		int count = 0;
		if (indoubtTransactionsCountByTmid.containsKey(tmid)) {
		    count =  (int) indoubtTransactionsCountByTmid.get(tmid) - 1;
		    if (count > 0) indoubtTransactionsCountByTmid.put(tmid, count);
		}
		if (count == 0) {
		    indoubtTransactionsCountByTmid.remove(tmid);
		    String lv_encoded = m_region.getRegionInfo().getEncodedName();
		    try {
			if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit  -" 
							  + " txId " + transactionId 
							  + ", Trafodion Recovery: delete in commit recovery zNode TM " + tmid 
							  + " region encoded name " + lv_encoded + " for 0 in-doubt transaction"
							  );
			deleteRecoveryzNode(tmid, lv_encoded);
		    } catch (IOException e) {
			LOG.error("TxnCP: commit -" 
				  + " txId " + transactionId 
				  + ", Trafodion Recovery: delete recovery zNode failed. Caught exception " + e.toString()
				  );
		    }
		}

		if ((indoubtTransactionsById == null) || (indoubtTransactionsById.size() == 0)) {
		    if (indoubtTransactionsById == null) 
			if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit -" 
							  + " txId " + transactionId 
							  + ", Trafodion Recovery: start region in commit with indoubtTransactionsById null"
							  );
			else
			    if (LOG.isInfoEnabled()) LOG.info("TxnCP: commit -" 
							      + " txId " + transactionId 
							      + ", Trafodion Recovery: start region in commit with indoubtTransactionsById size " 
							      + indoubtTransactionsById.size()
							      );
		    startRegionAfterRecovery();
		}
	    }
	}

	state.setCommitProgress(TransactionState.CommitProgress.COMMITED);
	retireTransaction(state, false);
    }

    /**
     * @param transactionId
     * @return TransactionRegionInterface commit code
     * @throws IOException
     */
    public int commitRequest(final long transactionId, final int participantNum) throws IOException, UnknownTransactionException {
	return commitRequest(transactionId, participantNum, true);
    }

    public int commitRequest(final long transactionId, final int participantNum, boolean flushHLOG) 
	throws IOException,
	       UnknownTransactionException 
    {
	long txid = 0;
	if (LOG.isTraceEnabled()) LOG.trace("TxnCP: commitRequest -- ENTRY" 
					    + ", txId: " + transactionId 
					    + ", participantNum: " + participantNum 
					    + ", regionName " + m_region.getRegionInfo().getRegionNameAsString()
					    );

	checkBlockNonPhase2(transactionId);

	AmpoolTransactionState state;

	int lv_totalCommits = 0;
	int lv_timeIndex = 0;
	if (LOG.isInfoEnabled()) {
	    synchronized (totalCommits){
		lv_totalCommits = totalCommits.incrementAndGet();
		lv_timeIndex = (timeIndex.getAndIncrement() % 50 );
	    }
	}

	boolean returnPending = false;
	long commitCheckEndTime = 0;
	long hasConflictStartTime = 0;
	long hasConflictEndTime = 0;
	long putBySequenceStartTime = 0;
	long putBySequenceEndTime = 0;
	long writeToLogEndTime = 0;
	long commitCheckStartTime = System.nanoTime();

	try {
	    state = getTransactionState(transactionId);
	} catch (UnknownTransactionException e) {
	    if (LOG.isInfoEnabled()) LOG.info("TxnCP: commitRequest Unknown transaction"
					      + ", txId: " + transactionId 
					      + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
					      + ", participantNum: " + participantNum + " ignoring"
					      );
	    state = null;
	}

	// may change to indicate a NOTFOUND case  then depends on the TM ts state, if reinstated tx, ignore the exception
	if (state == null) {
	    String errMsg = "TxnCP: commitRequest Unknown transaction"
		+ ", txId: " + transactionId 
		+ ", region: " + m_region.getRegionInfo().getRegionNameAsString()
		+ ", participantNum: " + participantNum
		;

	    if (LOG.isInfoEnabled()) LOG.info(errMsg);
	    throw new UnknownTransactionException(errMsg);
	}

	if (LOG.isInfoEnabled()) 
	    hasConflictStartTime = System.nanoTime();

	synchronized (commitCheckLock) {
	    if (hasConflict(state)) {
		if (LOG.isInfoEnabled()) {
		    hasConflictEndTime = System.nanoTime();
		    hasConflictTimes[lv_timeIndex] = hasConflictEndTime - hasConflictStartTime;
		    totalConflictTime += hasConflictTimes[lv_timeIndex];
		}
		state.setStatus(TransactionState.Status.ABORTED);
		retireTransaction(state, true);
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: commitRequest encountered conflict"
						  + ", txId: " + transactionId 
						  + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
						  + ", participantNum: " + participantNum
						  + ", returning COMMIT_CONFLICT"
						  );
		return COMMIT_CONFLICT;
	    }

	    if (LOG.isInfoEnabled()) 
		hasConflictEndTime = System.nanoTime();

	    // No conflicts, we can commit.
	    if (LOG.isTraceEnabled()) LOG.trace("TxnCP: No conflicts"
						+ ", txId: " + transactionId 
						+ ", region: " + m_region.getRegionInfo().getRegionNameAsString()
						+ ", participantNum: " + participantNum
						+ ", votes to commit"
						);

	    // If there are writes we must keep record of the transaction
	    putBySequenceStartTime = System.nanoTime();
	    if (state.hasWrite()) {
		if (LOG.isInfoEnabled()) {
		    // Only increment this counter if we are logging the statistics
		    putBySequenceOperations.getAndIncrement();
		}

		// Order is important
		state.setStatus(TransactionState.Status.COMMIT_PENDING);
		state.setCPEpoch(controlPointEpoch.get());
		commitPendingTransactions.add(state);
		state.setSequenceNumber(nextSequenceId.getAndIncrement());
		commitedTransactionsBySequenceNumber.put(state.getSequenceNumber(), state);

		if (LOG.isInfoEnabled()) LOG.info("TxnCP"
						  + ", txId: " + transactionId 
						  + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
						  + ", participantNum: " + participantNum
						  + ". Adding to commitedTransactionsBySequenceNumber for sequence number: " 
						  + state.getSequenceNumber()
						  );
	    }
	    commitCheckEndTime = putBySequenceEndTime = System.nanoTime();
	} // exit sync block of commitCheckLock
                
	if (state.hasWrite()) {
	    if (LOG.isTraceEnabled()) LOG.trace("write commitRequest edit to HLOG 1");
	    //call HLog by passing tagged WALEdits and associated fields
	    try {

		// Once we append edit into HLOG during DML operation, there is no need to do any HLOG write in phase 1.
		// Likely all the edits have been synced, so just do a sync(state.getLargestFlushTxId()) --> most likely, it is an no-op
		// This is to leverage the time between last DML and phase 1, so there is no need to do any logging (and waited) in phase 1.
		// And there is no need to write "prepared" record since we try to optimize normal running mode (99.99% commit)
		// We can append "commit" or "abort" into HLOG in phase 2 in a no-sync mode, but it can also be eliminated if necessary.
		// All the transaction reinstated during recovery will be treated as "in-doubt" and need to use TLOG to resolve. 
		// So TLOG must be kept for a while (likely a few CP), besides, HRegion chore thread can perform ~ CP action by writing
		//  the smallest sequenceId (not logSeqid), so we don't need to deal with commite case (but we did write abort edit since
		//  the number of abort is < 0.1% -- we can set this as a configurable property).

		if (LOG.isTraceEnabled()) LOG.trace("write commitRequest edit to HLOG 2");
		if (!state.getEarlyLogging()) {
		    //txid = this.tHLog.appendNoSync(this.regionInfo, this.regionInfo.getTable(),
		    //state.getEdit(), new ArrayList<UUID>(), TrxEnvironmentEdgeManager.currentTimeMillis(), this.m_region.getTableDesc(),
		    //nextLogSequenceId, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
                  
		    final WALKey wk = new WALKey(this.regionInfo.getEncodedNameAsBytes(), 
						 this.regionInfo.getTable(),
						 TrxEnvironmentEdgeManager.currentTime());
		  
		    if (LOG.isTraceEnabled()) LOG.trace("write commitRequest edit to HLOG 4");
		    AtomicLong lv_seqid = this.m_region.getSequenceId();
		    /* TBD		txid = this.tHLog.append(this.m_region.getTableDesc(),
		       this.regionInfo, 
		       wk,
		       state.getEdit(),
		       lv_seqid,
		       false,
		       null);
		    */
		    if ((m_isTrafodionMetadata) || 
			(LOG.isInfoEnabled())) LOG.info("TxnCP: PH1 commitRequest COMMIT_OK -- EXIT "
							+ m_regionName
							+ " isTrafodionMD: " + m_isTrafodionMetadata
							+ " txId: " + transactionId
							+ " seqId: " + lv_seqid
							+ " HLog seq: " + txid 
							+ " flushHLOG: " + flushHLOG
							);
		    //TBD		  if (flushHLOG) this.tHLog.sync(txid);
		    if (LOG.isTraceEnabled()) LOG.trace("write commitRequest edit to HLOG 6");
		}
		else {
		    if (LOG.isInfoEnabled()) LOG.info("write commitRequest edit to HLOG 7");
		    if (flushHLOG) this.tHLog.sync(state.getFlushTxId());
		    if (LOG.isTraceEnabled()) LOG.trace("write commitRequest edit to HLOG 8" );
		}
		if (LOG.isInfoEnabled()) {
		    writeToLogEndTime = System.nanoTime();
		    writeToLogTimes[lv_timeIndex] = writeToLogEndTime - commitCheckEndTime;
		    writeToLogOperations.getAndIncrement();
		}
	    } 
	    catch (IOException exp) {
		LOG.error("TxnCP: commitRequest - Caught IOException in HLOG appendNoSync -- EXIT"
			  + ", txId: " + transactionId 
			  + ", HLog seq: " + txid
			  , exp);
		throw exp;
	    }      
	    catch (Exception all_e) {
		LOG.error("TxnCP: commitRequest - Caught Exception in HLOG appendNoSync -- EXIT"
			  + ", txId: " + transactionId 
			  + ", HLog seq: " + txid
			  , all_e);
		throw all_e;
	    }
	  
	    if (LOG.isTraceEnabled()) LOG.trace("TxnCP: commitRequest COMMIT_OK -- EXIT txId: " + transactionId);
	    returnPending = true;
	}
	// No write pending
	else {
	    if (LOG.isInfoEnabled())
		writeToLogTimes[lv_timeIndex] = 0;
	}

	if (LOG.isInfoEnabled()) {
	    commitCheckTimes[lv_timeIndex] = commitCheckEndTime - commitCheckStartTime;
	    hasConflictTimes[lv_timeIndex] = hasConflictEndTime - hasConflictStartTime;
	    putBySequenceTimes[lv_timeIndex] = putBySequenceEndTime - putBySequenceStartTime;
	    totalCommitCheckTime += commitCheckTimes[lv_timeIndex];
	    totalConflictTime += hasConflictTimes[lv_timeIndex];
	    totalPutTime += putBySequenceTimes[lv_timeIndex];
	    totalWriteToLogTime += writeToLogTimes[lv_timeIndex];
	    if (commitCheckTimes[lv_timeIndex] > maxCommitCheckTime) {
		maxCommitCheckTime = commitCheckTimes[lv_timeIndex];
	    }
	    if (commitCheckTimes[lv_timeIndex] < minCommitCheckTime) {
		minCommitCheckTime = commitCheckTimes[lv_timeIndex];
	    }
	    if (hasConflictTimes[lv_timeIndex] > maxConflictTime) {
		maxConflictTime = hasConflictTimes[lv_timeIndex];
	    }
	    if (hasConflictTimes[lv_timeIndex] < minConflictTime) {
		minConflictTime = hasConflictTimes[lv_timeIndex];
	    }
	    if (putBySequenceTimes[lv_timeIndex] > maxPutTime) {
		maxPutTime = putBySequenceTimes[lv_timeIndex];
	    }
	    if (putBySequenceTimes[lv_timeIndex] < minPutTime) {
		minPutTime = putBySequenceTimes[lv_timeIndex];
	    }
	    if (writeToLogTimes[lv_timeIndex] > maxWriteToLogTime) {
		maxWriteToLogTime = writeToLogTimes[lv_timeIndex];
	    }
	    if (writeToLogTimes[lv_timeIndex] < minWriteToLogTime) {
		minWriteToLogTime = writeToLogTimes[lv_timeIndex];
	    }

	    if (lv_timeIndex == 49) {
		timeIndex.set(1);  // Start over so we don't exceed the array size
	    }

	    if (lv_totalCommits == 9999) {
		avgCommitCheckTime = (double) (totalCommitCheckTime/lv_totalCommits);
		avgConflictTime = (double) (totalConflictTime/lv_totalCommits);
		avgPutTime = (double) (totalPutTime/lv_totalCommits);
		avgWriteToLogTime = (double) ((double)totalWriteToLogTime/(double)lv_totalCommits);
		if (LOG.isInfoEnabled()) LOG.info("commitRequest Report\n" +
						  "  Region: " + m_region.getRegionInfo().getRegionNameAsString() + "\n" +
						  "                        Total commits: "
						  + lv_totalCommits + "\n" +
						  "                        commitCheckLock time:\n" +
						  "                                     Min:  "
						  + minCommitCheckTime / 1000 + " microseconds\n" +
						  "                                     Max:  "
						  + maxCommitCheckTime / 1000 + " microseconds\n" +
						  "                                     Avg:  "
						  + avgCommitCheckTime / 1000 + " microseconds\n" +
						  "                        hasConflict time:\n" +
						  "                                     Min:  "
						  + minConflictTime / 1000 + " microseconds\n" +
						  "                                     Max:  "
						  + maxConflictTime / 1000 + " microseconds\n" +
						  "                                     Avg:  "
						  + avgConflictTime / 1000 + " microseconds\n" +
						  "                        putBySequence time:\n" +
						  "                                     Min:  "
						  + minPutTime / 1000 + " microseconds\n" +
						  "                                     Max:  "
						  + maxPutTime / 1000 + " microseconds\n" +
						  "                                     Avg:  "
						  + avgPutTime / 1000 + " microseconds\n" +
						  "                                     Ops:  "
						  + putBySequenceOperations.get() + "\n" +
						  "                        writeToLog time:\n" +
						  "                                     Min:  "
						  + minWriteToLogTime / 1000 + " microseconds\n" +
						  "                                     Max:  "
						  + maxWriteToLogTime / 1000 + " microseconds\n" +
						  "                                     Avg:  "
						  + avgWriteToLogTime / 1000 + " microseconds\n" +
						  "                                     Ops:  "
						  + writeToLogOperations.get() + "\n\n");
		totalCommits.set(0);
		writeToLogOperations.set(0);
		putBySequenceOperations.set(0);
		totalCommitCheckTime    =    0;
		totalConflictTime =    0;
		totalPutTime      =    0;
		totalWriteToLogTime     =    0;
		minCommitCheckTime      =    1000000000;
		maxCommitCheckTime      =    0;
		avgCommitCheckTime      =    0;
		minConflictTime   =    1000000000;
		maxConflictTime   =    0;
		avgConflictTime   =    0;
		minPutTime        =    1000000000;
		maxPutTime        =    0;
		avgPutTime        =    0;
		minWriteToLogTime =    1000000000;
		maxWriteToLogTime =    0;
		avgWriteToLogTime =    0;
	    }
	} // end of LOG.Info

	if (returnPending) {
	    if (LOG.isTraceEnabled()) LOG.trace("write commitRequest edit to HLOG - commit_ok");
	    return COMMIT_OK;
	}

	// Otherwise we were read-only and commitable, so we can forget it.
	state.setStatus(TransactionState.Status.COMMITED);
	if(state.getSplitRetry())
	    return COMMIT_RESEND;
	retireTransaction(state, true);
	if (LOG.isInfoEnabled()) LOG.info("TxnCP: commitRequest READ ONLY for participant --EXIT"
					  + ", txId: " + transactionId 
					  + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
					  + ", participantNum: " + participantNum
					  );
	return COMMIT_OK_READ_ONLY;
    }

    /**
     * Determines if the transaction has any conflicts
     * @param AmpoolTransactionState state
     * @return boolean
     */
    private boolean hasConflict(final AmpoolTransactionState state) {
	// Check transactions that were committed while we were running
      
	synchronized (commitedTransactionsBySequenceNumber) {
	    for (long i = state.getStartSequenceNumber(); i < nextSequenceId.get(); i++)
		{
		    AmpoolTransactionState other = commitedTransactionsBySequenceNumber.get(i);
		    if (other == null) {
			continue;
		    }

		    if (LOG.isDebugEnabled()) LOG.debug("hasConflict"
							+ ", state.getStartSequenceNumber: " + i 
							+ ", nextSequenceId.get(): " + nextSequenceId.get() 
							+ ", state object: " + state.toString() 
							+ ", calling addTransactionToCheck"
							);

		    state.addTransactionToCheck(other);
		}
	}

	return state.hasConflict();
    }

    /**
     * Abort the transaction.
     * 
     * @param transactionId
     * @throws IOException
     * @throws UnknownTransactionException
     */

    public void abortTransaction(final long transactionId) 
	throws IOException, UnknownTransactionException 
    {

	long txid = 0;

	if (LOG.isInfoEnabled()) LOG.info("TxnCP: abort"
					  + ", txId: " + transactionId 
					  + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
					  );

	checkBlockNonPhase2(transactionId);

	AmpoolTransactionState state;
	try {
	    state = getTransactionState(transactionId);
	} catch (UnknownTransactionException e) {
	    IOException ioe = new IOException("UnknownTransactionException");
	    if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactions.abortTransaction - Unknown transaction"
					      + ", txId: " + transactionId
					      + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
					      + ioe.toString());
	    throw new IOException("UnknownTransactionException");
	}

	synchronized(state.getXaOperationObject()) {
	    if (state.getStatus().equals(TransactionState.Status.ABORTED)) { // already aborted, duplicate abort requested
		if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactions.abortTransaction - duplicate abort transaction"
						  + ", txId: " + transactionId
						  + ", region: " + m_region.getRegionInfo().getRegionNameAsString()
						  );
		return;
	    }
	    state.setStatus(TransactionState.Status.ABORTED);
	}

	if (state.hasWrite()) {
	    // TODO log
	    //  this.transactionLog.writeAbortToLog(m_region.getRegionInfo(),
	    //                                      state.getTransactionId(),
	    //                                    m_region.getTableDesc());
	    if (LOG.isInfoEnabled()) LOG.info("TxnCP: abortTransaction - abort write to HLOG");
	    Tag abortTag = state.formTransactionalContextTag(TS_ABORT);
	    List<Tag> tagList = new ArrayList<Tag>();
	    tagList.add(abortTag);

	    WALEdit e1 = state.getEdit();
	    WALEdit e = new WALEdit();

	    if (e1.getCells().size() > 0) {
		// get 1st Cell to associated with the abort record as a workaround through HLOG async append
		org.apache.hadoop.hbase.Cell c = e1.getCells().get(0);
		//TBD         KeyValue kv = new KeyValue(c.getRowArray(), c.getRowOffset(), (int)c.getRowLength(),
		//TBD         c.getFamilyArray(), c.getFamilyOffset(), (int)c.getFamilyLength(),
		//TBD         c.getQualifierArray(), c.getQualifierOffset(), (int) c.getQualifierLength(),
		//TBD         c.getTimestamp(), Type.codeToType(c.getTypeByte()), c.getValueArray(), c.getValueOffset(),
		//TBD         c.getValueLength(), tagList);
      
		//TBD         e.add(kv);
		try {
		    //txid = this.tHLog.appendNoSync(this.regionInfo, this.regionInfo.getTable(),
		    //     e, new ArrayList<UUID>(), TrxEnvironmentEdgeManager.currentTimeMillis(), this.m_region.getTableDesc(),
		    //     nextLogSequenceId, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
             
		    final WALKey wk = new WALKey(this.regionInfo.getEncodedNameAsBytes(),
						 this.regionInfo.getTable(),
						 TrxEnvironmentEdgeManager.currentTime());;
		    //TBD       	 	 txid = this.tHLog.append(this.m_region.getTableDesc(),
		    //TBD					  this.regionInfo, 
		    //TBD					  wk , 
		    //TBD					  e,
		    //TBD					  this.m_region.getSequenceId(), 
		    //TBD					  false,
		    //TBD					  null);
       	 	
		    if (LOG.isInfoEnabled()) LOG.info("TxnCP: Y99 write abort HLOG" 
						      + ", txId: " + transactionId 
						      + ", HLog seq: " + txid
						      );
		}
		//TBD         catch (IOException exp1) {
		catch (Exception exp1) {
		    if (LOG.isInfoEnabled()) LOG.info("TxnCP: abortTransaction - abort writing to HLOG : Caught an exception " 
						      + exp1.toString()
						      );
		    throw exp1;
		}
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: abortTransaction -- EXIT"
						  + ", txId: " + transactionId 
						  + ", HLog seq: " + txid
						  );

	    }

	}

	synchronized (commitPendingTransactions) {
	    commitPendingTransactions.remove(state);
	}

	if (state.isReinstated()) {
	    synchronized(indoubtTransactionsById) {
		if (LOG.isInfoEnabled()) LOG.info("TxnCP: Trafodion Recovery: abort reinstated indoubt transaction" 
						  + ", txId: " + transactionId
						  );
		indoubtTransactionsById.remove(state.getTransactionId());
		int clusterid = (int) TransactionState.getClusterId(transactionId);
		int tmid = (int) TransactionState.getNodeId(transactionId);
                if (  (clusterid != pSTRConfig.getTrafClusterIdInt()) && 
                       ((clusterid != 1) && (pSTRConfig.getConfiguredPeerCount() == 0))    ) tmid = -2; // for any peer
		int count = 0;

		// indoubtTransactionsCountByTmid protected by 
		// indoubtTransactionsById synchronization
		if (indoubtTransactionsCountByTmid.containsKey(tmid)) {
		    count =  (int) indoubtTransactionsCountByTmid.get(tmid) - 1;
		    if (count > 0) indoubtTransactionsCountByTmid.put(tmid, count);
		}

		// if all reinstated txns are resolved from a TM, remove it and delete associated zNode
		if (count == 0) {
		    indoubtTransactionsCountByTmid.remove(tmid);
		    String lv_encoded = m_region.getRegionInfo().getEncodedName();
		    try {
			if (LOG.isInfoEnabled()) LOG.info("TxnCP: Trafodion Recovery: delete in abort recovery zNode"
							  + ", TM " + tmid 
							  + ", region encoded name: " + lv_encoded 
							  + " for 0 in-doubt transaction"
							  );
			deleteRecoveryzNode(tmid, lv_encoded);
		    } catch (IOException e) {
			LOG.error("TxnCP: Trafodion Recovery: delete recovery zNode failed"
				  , e
				  );
		    }
		}

		if ((indoubtTransactionsById == null) || 
		    (indoubtTransactionsById.size() == 0)) {
		    // change region state to STARTED, and archive the split-thlog

		    if (LOG.isInfoEnabled()) LOG.info("TxnCP: Trafodion Recovery: start region in abort"
						      + ", #indoubtTransactionsById: "
						      + (indoubtTransactionsById == null ? "0" : indoubtTransactionsById.size())
						      );
		    startRegionAfterRecovery();
		}
	    }
	}

	retireTransaction(state, true);

	if (LOG.isInfoEnabled()) LOG.info("TxnCP: abortTransaction, looking for abort" 
					  + ", txId: " + transactionId 
					  + ", #transactionsById " + transactionsById.size() 
					  + ", #commitedTransactionsBySequenceNumber " + commitedTransactionsBySequenceNumber.size() 
					  + ", #commitPendingTransactions" + commitPendingTransactions.size()
					  );

    }

    /**
     * Retires the transaction                        
     * @param AmpoolTransactionState state
     */
    private void retireTransaction(final AmpoolTransactionState state, final boolean clear) {

	String key = getTransactionalUniqueId(state.getTransactionId());
	long transId = state.getTransactionId();

	if (LOG.isTraceEnabled()) LOG.trace("TxnCP: retireTransaction"
					    + ", txId: " + transId
					    );              
	try {
	    transactionLeases.cancelLease(key);
	} catch (LeaseException le) {
	    if (LOG.isInfoEnabled()) LOG.info("TxnCP: retireTransaction, txId: " 
					      + transId + ", LeaseException");
	    // Ignore
	} catch (Exception e) {
	    if (LOG.isInfoEnabled()) LOG.info("TxnCP: retireTransaction, txId: " 
					      + transId + ", General Lease exception" + e.getMessage() + " " + stackTraceToString(e));
	    // Ignore
	}

	// Clear out transaction state
      
	if (clear == true) {
	    state.clearState();
	    if (LOG.isTraceEnabled()) LOG.trace("TxnCP: retireTransaction clearState"
						+ " for: " + key 
						+ " from all its AmpoolTransactionState lists"
						);
	}
	else {
	    state.clearTransactionsToCheck();	
	    if (LOG.isTraceEnabled()) LOG.trace("TxnCP: retireTransaction clearTransactionsToCheck for: " + key);
	}

	/*
	synchronized (cleanScannersForTransactions) {
	    cleanScannersForTransactions.add(transId);
	}
	*/

	if (LOG.isTraceEnabled()) LOG.trace("TxnCP: retireTransaction calling remove entry" 
					    + " for: " + key 
					    + " , from transactionById map "
					    );
	transactionsById.remove(key);
    
	if (LOG.isInfoEnabled()) LOG.info("TxnCP:retireTransaction " + key 
					  + ", looking for retire txId: " + transId 
					  + ", transactionsById " + transactionsById.size() 
					  + ", commitedTransactionsBySequenceNumber " + commitedTransactionsBySequenceNumber.size() 
					  + ", commitPendingTransactions " + commitPendingTransactions.size()
					  );
    
    }

    /**
     * Formats the throwable stacktrace to a string
     * @param Throwable e
     * @return String 
     */
    public String stackTraceToString(Throwable e) {
	StringBuilder sb = new StringBuilder();
	for (StackTraceElement element : e.getStackTrace()) {
	    sb.append(element.toString());
	    sb.append("\n");
	}
	return sb.toString();
    }


}
