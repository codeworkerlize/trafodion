package org.apache.hadoop.hbase.coprocessor.transactional.server;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.cache.ConnectionCache;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.cache.VectorCache;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.TransactionCleaner;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.DeadLockDetect;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.LockWait;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.LockWaitInfoOutputWorker;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.DistributedDeadLockDetectThread;
import org.apache.hadoop.hbase.coprocessor.transactional.TrxRegionEndpoint;
import org.apache.hadoop.hbase.coprocessor.transactional.TrxRegionObserver;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.*;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.IPUtils;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.*;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.pit.MutationCapture2;
import org.apache.hadoop.hbase.coprocessor.transactional.EndpointCostStats;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.OrderedProperties;

import javax.net.ssl.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.MemoryMXBean;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.esgyn.common.LicenseHelper;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockInfoReqMessage;
import org.apache.hadoop.hbase.pit.MutationCapture2;
import org.apache.hadoop.hbase.pit.MC2TransactionsChore;
public class RSServer {
    private static Logger LOG = Logger.getLogger(RSServer.class);
    //Global object cache in current regionServer
    private volatile AtomicInteger currentLockCount = new AtomicInteger(0);
    private VectorCache<Lock> lockVectorCache = null;

    public synchronized Lock getLockFromVectorCache(RowKey rowKey, String regionName) {
        Lock lock = lockVectorCache.getElement();
        if (lock != null) {
            lock.reInit(rowKey, regionName);
        } else {
            if (rowKey != null) {
                lock = new Lock(rowKey, regionName);
            } else {
                lock = new Lock(regionName);
            }
        }
        currentLockCount.incrementAndGet();
        return lock;
    }

    public synchronized void returnLockToVectorCache(Lock lock) {
        lockVectorCache.add(lock);
        currentLockCount.decrementAndGet();
    }

    private volatile AtomicInteger currentTransactionCount = new AtomicInteger(0);
    private VectorCache<Transaction> transactionVectorCache = null;

    public Transaction getTransactionFromVectorCache(Long txId) {
        Transaction transaction = transactionVectorCache.getElement();
        if (transaction != null) {
            transaction.reInit(txId);
        } else {
            transaction = new Transaction(txId);
        }
        currentTransactionCount.incrementAndGet();
        return transaction;
    }
    public void returnTransactionToVectorCache(Transaction trans) {
        transactionVectorCache.add(trans);
        currentTransactionCount.decrementAndGet();
    }

    private VectorCache<LockHolder> lockHolderVectorCache = null;

    static {
        try {
            String envEnableRowLock = System.getenv("ENABLE_ROW_LEVEL_LOCK");
            if (envEnableRowLock != null) {
                envEnableRowLock = envEnableRowLock.trim();
                LockConstants.ENABLE_ROW_LEVEL_LOCK = (envEnableRowLock.equals("1") || envEnableRowLock.equals("true")) ? true : false;
            }
        } catch (Exception e) {
            LOG.error("The value of ENABLE_ROW_LEVEL_LOCK is invalid");
        }

        //check license
        if(!LicenseHelper.isModuleOpen(LicenseHelper.Modules.ROW_LOCK)) {
            LockConstants.ENABLE_ROW_LEVEL_LOCK = false;
            LockConstants.HAS_VALID_LICENSE = false;
        } else {
            LockConstants.HAS_VALID_LICENSE = true;
        }

        try {
            String envLockSkipMetaTable = System.getenv("LOCK_SKIP_META_TABLE");
            if (envLockSkipMetaTable != null) {
                envLockSkipMetaTable = envLockSkipMetaTable.trim();
                LockConstants.LOCK_SKIP_META_TABLE = (envLockSkipMetaTable.equals("1") || envLockSkipMetaTable.equals("true")) ? true : false;
            }
        } catch (Exception e) {
            LOG.error("The value of LOCK_SKIP_META_TABLE is invalid");
        }
        try {
            String envLockCacheSize = System.getenv("LOCK_CACHE_SIZE");
            if (envLockCacheSize != null) {
                LockConstants.LOCK_CACHE_SIZE = Integer.parseInt(envLockCacheSize.trim());
                LockConstants.LOCK_CACHE_SIZE = LockConstants.LOCK_CACHE_SIZE <= 0
                        ? LockConstants.DEFALUT_LOCK_CACHE_SIZE
                        : LockConstants.LOCK_CACHE_SIZE;
            }
        } catch (Exception e) {
            LOG.error("The value of LOCK_CACHE_SIZE is invalid");
        }
        try {
            String lockEscalationThreshold = System.getenv("LOCK_ESCALATION_THRESHOLD");
            if (lockEscalationThreshold != null) {
                LockConstants.LOCK_ESCALATION_THRESHOLD = Integer.parseInt(lockEscalationThreshold);
                // LOCK_ESCALATION_THRESHOLD must be odd number
                if (LockConstants.LOCK_ESCALATION_THRESHOLD % 2 == 0) {
                    LockConstants.LOCK_ESCALATION_THRESHOLD = LockConstants.LOCK_ESCALATION_THRESHOLD -1;
                }
            }
        } catch(Exception e) {
            LOG.error("The value of LOCK_ESCALATION_THRESHOLD is invalid");
        }
        try {
            String tryLockTimes = System.getenv("LOCK_TRYLOCK_TIMES");
            if (tryLockTimes != null) {
                LockConstants.LOCK_TRYLOCK_TIMES = Integer.parseInt(tryLockTimes);
            }
        } catch(Exception e) {
            LOG.error("The value of LOCK_TRYLOCK_TIMES is invalid");
        }
        try {
            String restServerUri = System.getenv("REST_SERVER_URI");
            if (restServerUri != null && restServerUri.indexOf(":") > 0) {
                LockConstants.REST_SERVER_URI = restServerUri;
            }
        } catch(Exception e) {
            LOG.error("The value of REST_SERVER_URI is invalid");
        }
        try {
            String xdcMasterSlave= System.getenv("XDC_MASTER_SLAVE");
            if (xdcMasterSlave != null && xdcMasterSlave.indexOf(";") > 0) {
                LockConstants.XDC_MASTER_SLAVE = xdcMasterSlave;
            }
        } catch(Exception e) {
            LOG.error("The value of XDC_MASTER_SLAVE is invalid");
        }
        try {
            String txCleanInterval = System.getenv("TRANSACTION_CLEAN_INTERVAL");
            if (txCleanInterval != null) {
                LockConstants.TRANSACTION_CLEAN_INTERVAL = Long.parseLong(txCleanInterval);
            }
        } catch(Exception e) {
            LOG.error("The value of TRANSACTION_CLEAN_INTERVAL is invalid");
        }
        try {
            String lockEnableDeadlockDetect = System.getenv("LOCK_ENABLE_DEADLOCK_DETECT");
            if (lockEnableDeadlockDetect != null) {
                lockEnableDeadlockDetect = lockEnableDeadlockDetect.trim().toLowerCase();
                LockConstants.LOCK_ENABLE_DEADLOCK_DETECT = ((lockEnableDeadlockDetect.equals("true") || lockEnableDeadlockDetect.equals("1"))? true : false);
            }
        } catch(Exception e) {
            LOG.error("The value of LOCK_ENABLE_DEADLOCK_DETECT is invalid");
        }
        try {
            String rsListenerPort = System.getenv("RS_LISTENER_PORT");
            if (rsListenerPort != null) {
                RSConstants.RS_LISTENER_PORT = Integer.parseInt(rsListenerPort);
            }
        } catch(Exception e) {
            LOG.error("The value of RS_LISTENER_PORT is invalid");
        }
        try {
            String distributedInterval = System.getenv("DISTRIBUTED_DEADLOCK_DETECT_INTERVAL");
            if (distributedInterval != null) {
                LockConstants.DISTRIBUTED_DEADLOCK_DETECT_INTERVAL = Integer.parseInt(distributedInterval);
            }
        } catch(Exception e) {
            LOG.error("The value of DISTRIBUTED_DEADLOCK_DETECT_INTERVAL is invalid");
        }
        try {
            String restConnFailedTimes = System.getenv("REST_CONN_FAILED_TIMES");
            if (restConnFailedTimes != null) {
                LockConstants.REST_CONN_FAILED_TIMES = Integer.parseInt(restConnFailedTimes);
            }
        } catch(Exception e) {
            LOG.error("The value of REST_CONN_FAILED_TIMES is invalid");
        }
        try {
            String restConnectTimeout = System.getenv("REST_CONNECT_TIMEOUT");
            if (restConnectTimeout != null) {
                LockConstants.REST_CONNECT_TIMEOUT = Integer.parseInt(restConnectTimeout);
            }
        } catch(Exception e) {
            LOG.error("The value of REST_CONNECT_TIMEOUT is invalid");
        }
        try {
            String restReadTimeout = System.getenv("REST_READ_TIMEOUT");
            if (restReadTimeout != null) {
                LockConstants.REST_READ_TIMEOUT = Integer.parseInt(restReadTimeout);
            }
        } catch(Exception e) {
            LOG.error("The value of REST_READ_TIMEOUT is invalid");
        }
        try {
            String restConnectDetectInterval = System.getenv("REST_CONNECT_DETECT_INTERVAL");
            if (restConnectDetectInterval != null) {
                LockConstants.REST_CONNECT_DETECT_INTERVAL = Long.parseLong(restConnectDetectInterval);
            }
        } catch(Exception e) {
            LOG.error("The value of REST_CONNECT_DETECT_INTERVAL is invalid");
        }
        try {
            String envLockLogLevel = System.getenv("LOCK_LOG_LEVEL");
            if (envLockLogLevel != null && !envLockLogLevel.equals("")) {
                envLockLogLevel = envLockLogLevel.trim().toLowerCase();
                LockLogLevel.parse(envLockLogLevel); 
            } else {
                LockLogLevel.init();
            }
        } catch(Exception e) {
            LOG.error("The value of LOCK_LOG_LEVEL is invalid");
        }

        try {
            String envLockStatistics = System.getenv("ENABLE_LOCK_STATISTICS");
            if (envLockStatistics != null) {
                LockConstants.ENABLE_LOCK_STATISTICS = (Integer.parseInt(envLockStatistics.trim()) == 1 ? true : false);
            }
        } catch(Exception e) {
            LOG.error("The value of ENABLE_LOCK_STATISTICS is invalid");
        }

        try {
            String envLockTimeOut = System.getenv("LOCK_TIME_OUT");
            if (envLockTimeOut != null)
                LockConstants.LOCK_WAIT_TIMEOUT = Integer.parseInt(envLockTimeOut.trim());
            TrxRegionEndpoint.checkAndSetLockTimeOut(LockConstants.LOCK_WAIT_TIMEOUT);
        } catch (Exception e) {
        }
        try {
            String envLockStatistics = System.getenv("LOCK_WAIT_INFO_WAITTIME_THRESHOLD");
            if (envLockStatistics != null) {
                int num = Integer.parseInt(envLockStatistics.trim());
                LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD = ((num < 100)
                        && (num != 0)) ? 100 : num;
            }
        } catch (Exception e) {
            LOG.error("The value of LOCK_WAIT_INFO_WAITTIME_THRESHOLD is invalid");
        }
        try {
            String envRecordTxLockNumThreshold  = System.getenv("RECORD_TRANSACTION_LOCKNUM_THRESHOLD");
            if (envRecordTxLockNumThreshold != null) {
                LockConstants.RECORD_TRANSACTION_LOCKNUM_THRESHOLD = Integer.parseInt(envRecordTxLockNumThreshold.trim());
            }
        } catch (Exception e) {
            LOG.error("The value of RECORD_TRANSACTION_LOCKNUM_THRESHOLD is invalid");
        }
        try {
            String envEnableSavepoint  = System.getenv("ENABLE_SAVEPOINT");
            if (envEnableSavepoint != null) {
                envEnableSavepoint = envEnableSavepoint.trim().toLowerCase();
                LockConstants.ENABLE_SAVEPOINT = (envEnableSavepoint.equals("1") || envEnableSavepoint.equals("true"));
            }
        } catch (Exception e) {
            LOG.error("The value of ENABLE_SAVEPOINT is invalid");
        }
        try {
            String envDisplayImplicitSavepoint  = System.getenv("DISPLAY_IMPLICIT_SAVEPOINT");
            if (envDisplayImplicitSavepoint != null) {
                envDisplayImplicitSavepoint = envDisplayImplicitSavepoint.trim().toLowerCase();
                LockConstants.DISPLAY_IMPLICIT_SAVEPOINT = (envDisplayImplicitSavepoint.equals("1") || envDisplayImplicitSavepoint.equals("true"));
            }
        } catch (Exception e) {
            LOG.error("The value of DISPLAY_IMPLICIT_SAVEPOINT is invalid");
        }
        try {
            String envDisplaySavepointID  = System.getenv("DISPLAY_SAVEPOINT_ID");
            if (envDisplaySavepointID != null) {
                envDisplaySavepointID = envDisplaySavepointID.trim().toLowerCase();
                LockConstants.DISPLAY_SAVEPOINT_ID = (envDisplaySavepointID.equals("1") || envDisplaySavepointID.equals("true"));
            }
        } catch (Exception e) {
            LOG.error("The value of DISPLAY_SAVEPOINT_ID is invalid");
        }
        try {
            String envNumOfLogOneTime  = System.getenv("NUMBER_OF_OUTPUT_LOGS_PER_TIME");
            if (envNumOfLogOneTime != null) {
                int tmp = Integer.parseInt(envNumOfLogOneTime.trim());
                LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME = tmp > 0 ? tmp : LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
            }
        } catch (Exception e) {
            LOG.error("The value of NUMBER_OF_OUTPUT_LOGS_PER_TIME is invalid");
        }
       try {
            String envEnableLockForSelect = System.getenv("ENABLE_LOCK_FOR_SELECT");
            if (envEnableLockForSelect != null) {
                LockConstants.ENABLE_LOCK_FOR_SELECT = (envEnableLockForSelect.equals("1") || envEnableLockForSelect.equals("true")) ? true : false;
                TrxRegionEndpoint.switchSelectLockMode();
            }
        } catch (Exception e) {
            LOG.error("value of ENABLE_LOCK_FOR_SELECT is invalid");
        }
        try {
            String envEnableLockForFullScan = System.getenv("ENABLE_TABLELOCK_FOR_FULL_SCAN");
            if (envEnableLockForFullScan != null) {
                LockConstants.ENABLE_TABLELOCK_FOR_FULL_SCAN = (envEnableLockForFullScan.equals("1") || envEnableLockForFullScan.equals("true")) ? true : false;
            }
        } catch (Exception e) {
            LOG.error("value of ENABLE_TABLELOCK_FOR_FULL_SCAN is invalid");
        }

        try {
            String envMaxLockNumber = System.getenv("MAX_LOCK_NUMBER");
            if (envMaxLockNumber != null) {
                LockConstants.MAX_LOCK_NUMBER = Integer.parseInt(envMaxLockNumber.trim());
                LockConstants.MAX_LOCK_NUMBER = (LockConstants.MAX_LOCK_NUMBER < 0)
                        ? 0
                        : LockConstants.MAX_LOCK_NUMBER;
            }
        } catch (Exception e) {
            LOG.error("The value of MAX_LOCK_NUMBER is invalid");
        }

        try {
            String envMaxLockPerTransNumber = System.getenv("MAX_LOCK_NUMBER_PER_TRANS");
            if (envMaxLockPerTransNumber != null) {
                LockConstants.MAX_LOCK_NUMBER_PER_TRANS = Integer.parseInt(envMaxLockPerTransNumber.trim());
                LockConstants.MAX_LOCK_NUMBER_PER_TRANS = (LockConstants.MAX_LOCK_NUMBER_PER_TRANS < 0)
                        ? 0
                        : LockConstants.MAX_LOCK_NUMBER_PER_TRANS;
            }
        } catch (Exception e) {
            LOG.error("The value of MAX_LOCK_NUMBER_PER_TRANS is invalid");
        }

        try {
            String envTraceLockApplied = System.getenv("TRACE_LOCK_APPLIED");
            if (envTraceLockApplied != null) {
                envTraceLockApplied = envTraceLockApplied.trim().toLowerCase();
                LockConstants.TRACE_LOCK_APPLIED = (envTraceLockApplied.equals("1")
                        || envTraceLockApplied.equals("true"));
            }
        } catch (Exception e) {
            LOG.error("The value of TRACE_LOCK_APPLIED is invalid");
        }
        try {
            String envEnableLongWaitLockPrinting = System.getenv("ENABLE_LONG_WAIT_LOCK_PRINTING");
            if (envEnableLongWaitLockPrinting != null) {
                envEnableLongWaitLockPrinting = envEnableLongWaitLockPrinting.trim().toLowerCase();
                LockConstants.ENABLE_LONG_WAIT_LOCK_PRINTING = (envEnableLongWaitLockPrinting.equals("1")
                        || envEnableLongWaitLockPrinting.equals("true"));
            }
        } catch (Exception e) {
            LOG.error("The value of ENABLE_LONG_WAIT_LOCK_PRINTING is invalid");
        }
        try {
            String envFetchLockInfoTimeoutString = System.getenv("LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT");
            if (envFetchLockInfoTimeoutString != null) {
                int num = Integer.parseInt(envFetchLockInfoTimeoutString);
                LockConstants.LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT = (num > LockConstants.LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT)
                        ? num
                        : LockConstants.LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT;
            }
        } catch(Exception e) {
            LOG.error("The value of LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT is invalid");
        }

        try {
            String envEnableTraceWaitLockInfo = System.getenv("ENABLE_TRACE_WAIT_LOCK_INFO");
            if (envEnableTraceWaitLockInfo != null) {
                envEnableTraceWaitLockInfo = envEnableTraceWaitLockInfo.trim().toLowerCase();
                LockConstants.ENABLE_TRACE_WAIT_LOCK_INFO = (envEnableTraceWaitLockInfo.equals("1")
                        || envEnableTraceWaitLockInfo.equals("true"));
            }
        } catch (Exception e) {
            LOG.error("The value of ENABLE_TRACE_WAIT_LOCK_INFO is invalid");
        }

        if (LockConstants.MAX_LOCK_NUMBER != 0
                && LockConstants.MAX_LOCK_NUMBER_PER_TRANS > LockConstants.MAX_LOCK_NUMBER) {
            LOG.error("The value of MAX_LOCK_NUMBER_PER_TRANS is bigger than MAX_LOCK_NUMBER");
            LockConstants.MAX_LOCK_NUMBER_PER_TRANS = LockConstants.MAX_LOCK_NUMBER;
        }

        try {
            String envStr = System.getenv("REGION_MEMORY_HIGHLOAD_THRESHOLD");
            if (envStr != null) {
                int num = Integer.parseInt(envStr.trim());
                if (num >= 10 && num <= 100)
                    RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD = num;
                else
                    LOG.warn("The value of REGION_MEMORY_HIGHLOAD_THRESHOLD must in [10-100]");
            }
        } catch(Exception e) {
            LOG.error("The value of REGION_MEMORY_HIGHLOAD_THRESHOLD is invalid");
        }

        try {
            String envStr = System.getenv("REGION_MEMORY_WARNING_THRESHOLD");
            if (envStr != null) {
                int num = Integer.parseInt(envStr.trim());
                if (num >= 0 && num <= RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD)
                    RSConstants.REGION_MEMORY_WARNING_THRESHOLD = num;
                else
                    LOG.warn("The value of REGION_MEMORY_WARNING_THRESHOLD must in [70-REGION_MEMORY_HIGHLOAD_THRESHOLD]");
            }
        } catch(Exception e) {
            LOG.error("The value of REGION_MEMORY_WARNING_THRESHOLD is invalid");
        }

        if (RSConstants.REGION_MEMORY_WARNING_THRESHOLD > RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD)
                RSConstants.REGION_MEMORY_WARNING_THRESHOLD = RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD;

        try {
            String envStr = System.getenv("CHECK_REGION_MEMORY_INTERVALS");
            if (envStr != null) {
                int num = Integer.parseInt(envStr.trim());
                if (num >= 1)
                    RSConstants.CHECK_REGION_MEMORY_INTERVALS = num;
                else
                    LOG.warn("The value of CHECK_REGION_MEMORY_INTERVALS must > 0");
            }
        } catch(Exception e) {
            LOG.error("The value of CHECK_REGION_MEMORY_INTERVALS is invalid");
        }

        try {
            String envStr = System.getenv("MEMORY_ALLOC_AMPLIFICATION_FACTOR");
            if (envStr != null) {
                int num = Integer.parseInt(envStr.trim());
                if (num >= 0)
                    RSConstants.MEMORY_ALLOC_AMPLIFICATION_FACTOR = num;
                else
                    LOG.warn("The value of MEMORY_ALLOC_AMPLIFICATION_FACTOR must >= 0");
            }
        } catch(Exception e) {
            LOG.error("The value of MEMORY_ALLOC_AMPLIFICATION_FACTOR is invalid");
        }

        //column width for output log by log4j
        LockConstants.COLUMN_WIDTH_MAP.put(1, 18);
        LockConstants.COLUMN_WIDTH_MAP.put(2, 40);
        LockConstants.COLUMN_WIDTH_MAP.put(3, 30);
        LockConstants.COLUMN_WIDTH_MAP.put(4, 40);
        LockConstants.COLUMN_WIDTH_MAP.put(5, 20);
        LockConstants.COLUMN_WIDTH_MAP.put(6, 19);
        LockConstants.COLUMN_WIDTH_MAP.put(7, 12);
        LockConstants.COLUMN_WIDTH_MAP.put(8, 14);
    }

    private static RSServer rsServer = null;
    @Getter
    private Map<String, LockManager> lockManagers = new ConcurrentHashMap<>();
    @Getter
    @Setter
    private String serverName;
    @Setter
    private ZooKeeperWatcher zkw1;

    private volatile DeadLockDetect deadLockDetect;

    public int conflictMaskTab[] = new int[LockConstants.LOCK_MAX_TABLE];

    private volatile Set<Long> regionTxIDs = new HashSet<>();

    private TransactionCleaner transactionCleaner;

    private DistributedDeadLockDetectThread distributedDeadLockDetectThread;

    private LockWaitInfoOutputWorker lockWaitInfoOutputWorker = null;

    private CopyOnWriteArraySet<String> allRegionServers = new CopyOnWriteArraySet<>();

    private CopyOnWriteArraySet<String> allRegionServerIPs = new CopyOnWriteArraySet<>();

    private ReentrantLock regionServerLock = new ReentrantLock();

    private List<List<String>> restUriListList = new ArrayList<>();

    private List<AbstractMap.SimpleEntry<Integer, Boolean> > xdcListPair = new ArrayList<AbstractMap.SimpleEntry<Integer, Boolean> >();

    /*
        AtomicInteger in lockHolderMap
        > 0 : The number of lockHolers applied by transaction
        = 0 : Transaction has no lockHolder
        < 0 : The number of lockHolers applied by transaction,
              but LMServer had no enough lockHoler.
              using a negative counter for reducing code of state transmission
    */
    public final Map<Long, AtomicInteger> lockHolderMap = new ConcurrentHashMap<>();

    public final ReentrantReadWriteLock holderRWLock = new ReentrantReadWriteLock();

    private final AtomicInteger checkMemoryCounter = new AtomicInteger(0);

    private static MemoryMXBean memoryBean = null;

    @Getter
    private ConnectionCache connectionCache = new ConnectionCache();

    private Stoppable stopper;

    private ReentrantLock sessionLock = new ReentrantLock();

    private List<RSSession> idleSessions = new ArrayList<>();

    private List<RSSession> workingSessions = new ArrayList<>();

    private int messageRetryTimes = 3;

    private AtomicInteger overusedLockHolderCount = new AtomicInteger(0);

    private AtomicInteger maxUsedLockHolderCount = new AtomicInteger(0);

    private Set<LockHolder> appliedSet = null;

    private String hbaseLogpath = null;

    private static boolean isJUnit = false;

    private ReentrantLock zkTransactionCleanLock = new ReentrantLock();

    private RSServer(Stoppable stopper, ZooKeeperWatcher zkw, String hbaseLogpath, String localhostName) {
        this.zkw1 = zkw;
        this.stopper = stopper;
        this.hbaseLogpath = hbaseLogpath;
        this.serverName = localhostName;
        if (this.serverName == null || "".equals(this.serverName)) {
            this.serverName = IPUtils.getHostName();
        }
        try {
            new RegionServerNodesListener(this.zkw1);
        } catch (Exception e) {
            LOG.error("failed to register region server node Listener!", e);
        }
        try {
            RSListener rsListener = new RSListener(this, RSConstants.RS_LISTENER_PORT, stopper);
            rsListener.setName("Lock Manager Server");
            rsListener.setDaemon(true);
            rsListener.start();
            enableRowLevelLockFeature(); 
        } catch (IOException e) {
            LOG.error("failed to start Lock Manager Listener! " + RSConstants.RS_LISTENER_PORT + " port occupancys\n You can Release port " + RSConstants.RS_LISTENER_PORT +
                " or change env RS_LISTENER_PORT in hbase to another port.\n", e);
            System.exit(1);
        } catch (Exception e) {
            LOG.error("failed to start Lock Manager Listener!", e);
            System.exit(1);
        }
    }

    public void enableRowLevelLockFeature() {
        if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            LOG.warn("could not enable row level lock for ENABLE_ROW_LEVEL_LOCK is false, please chech ENABLE_ROW_LEVEL_LOCK in ms.env and hbase-env.sh");
            return;
        }
        globalConflictTableInit();

        memoryBean = ManagementFactory.getMemoryMXBean();
        checkMemoryCounter.set(RSConstants.CHECK_REGION_MEMORY_INTERVALS);

        lockVectorCache = new VectorCache<>(LockConstants.LOCK_CACHE_SIZE);
        transactionVectorCache = new VectorCache<>(LockConstants.TRANS_CACHE_SIZE);//not LOCK_CACHE_SIZE
        lockHolderVectorCache = new VectorCache<>(LockConstants.MAX_LOCK_NUMBER);

        if (LockConstants.LOCK_CACHE_SIZE > 0) {
            for (int i = 0; i < LockConstants.LOCK_CACHE_SIZE; i++) {
                lockVectorCache.add(new Lock());
            }
        }
        if (LockConstants.TRANS_CACHE_SIZE > 0) {
            for (int i = 0; i < LockConstants.TRANS_CACHE_SIZE; i++) {
                transactionVectorCache.add(new Transaction());
            }
        }
        if (LockConstants.MAX_LOCK_NUMBER > 0) {
            for (int i = 0; i < LockConstants.MAX_LOCK_NUMBER; i++) {
                lockHolderVectorCache.add(new LockHolder());
            }
        } else {
            if (LockConstants.LOCK_CACHE_SIZE > 0) {
                lockHolderVectorCache.changeCacheSize(LockConstants.DEFALUT_LOCK_HOLDER_SIZE);
                for (int i = 0; i < LockConstants.DEFALUT_LOCK_HOLDER_SIZE; i++) {
                    lockHolderVectorCache.add(new LockHolder());
                }
            }
        }
        if (LockConstants.TRACE_LOCK_APPLIED) {
            appliedSet = Collections.synchronizedSet(new HashSet<LockHolder>(LockConstants.MAX_PRELOAD_APPLIED_LOG));
        }

        if (LockConstants.REST_SERVER_URI != null && !"".equals(LockConstants.REST_SERVER_URI.trim()) && transactionCleaner == null) {
            transactionCleaner = new TransactionCleaner(this, stopper);
            transactionCleaner.setDaemon(true);
            transactionCleaner.start();
        }
        deadLockDetect = new DeadLockDetect(this);
        if (LockConstants.LOCK_ENABLE_DEADLOCK_DETECT && distributedDeadLockDetectThread == null) {
            distributedDeadLockDetectThread = new DistributedDeadLockDetectThread(this, stopper);
            distributedDeadLockDetectThread.setDaemon(true);
            distributedDeadLockDetectThread.start();
        }
        if (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD > 0 && !isJUnit) {
            if (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD >= LockConstants.LOCK_WAIT_TIMEOUT) {
                LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD = (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD
                        / LockConstants.LOCK_WAIT_TIMEOUT - 1) * LockConstants.LOCK_WAIT_TIMEOUT;
                if (LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD == 0) {
                    LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD = LockConstants.LOCK_WAIT_TIMEOUT;
                }
            } else {
                //LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD = LockConstants.LOCK_WAIT_TIMEOUT;
                LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD = LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD;
            }
            lockWaitInfoOutputWorker = new LockWaitInfoOutputWorker(this, stopper, hbaseLogpath);
            lockWaitInfoOutputWorker.run();
        }
    }

    private void globalConflictTableInit() {
        for (int dwCount = 0; dwCount < LockConstants.LOCK_MAX_TABLE; dwCount++) {
            conflictMaskTab[dwCount] = 0;//conflict none
            for (int dwPos = 0; dwPos < LockConstants.LOCK_MAX_MODES; dwPos++) {
                if ((dwCount & (1 << dwPos)) != 0) {
                    conflictMaskTab[dwCount] |= LockConstants.defaultLockConflictsMatrix[dwPos];
                }
            }
        }
    }

    public static synchronized RSServer getInstance() {
        return rsServer;
    }

    //for Junit
    public static synchronized RSServer getInstance(Stoppable stopper, ZooKeeperWatcher zkw) {
        if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            return null;
        }
        isJUnit = true;
        return getInstance(stopper, zkw, null, null);
    }

    public static synchronized RSServer getInstance(Stoppable stopper, ZooKeeperWatcher zkw, String hbaseLogpath, String localhostName) {
        if (rsServer == null) {
            rsServer = new RSServer(stopper, zkw, hbaseLogpath, localhostName);
        }
        return rsServer;
    }

    public void addLockManager(String regionName, LockManager lockManager) {
        lockManagers.put(regionName, lockManager);
    }

    public void removeLockManager(String regionName) {
        LockManager removedLockManager = lockManagers.remove(regionName);
        if (removedLockManager != null) {
            removedLockManager.releaseTableLock();
        }
    }

    /**
     * process remote request messages
     * @param lmMessage   message
     * @param lmConn      connection
     */
    public void processRSMessage(RSMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter process message " + lmMessage + " from " + lmConn);
        }
        switch (lmMessage.getMsgType()) {
            case RSMessage.MSG_TYP_DEADLOCK_DETECT:
                deadLockDetect.processDeadLockDetect((LMDeadLockDetectMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_DEADLOCK_VICTIM:
                deadLockDetect.processDeadLockVictim((LMVictimMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_INFO:
                processLockInfo((LMLockInfoReqMessage) lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE:
                processSingleNodeLockInfo((LMLockInfoReqMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_CLEAN:
                processLockClean((LMLockInfoReqMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_CLEAN_SINGLE_NODE:
                processSingleNodeLockClean((LMLockInfoReqMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_STATISTICS_INFO:
                processLockStatistics((LMLockStatisticsReqMessage) lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_STATISTICS_INFO_SINGLE_NODE:
                processSingleNodeLockStatistics((LMLockStatisticsReqMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_GET_REGION_INFO:
                processGetVailRegionList((LMMRegionInfoMsg) lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_CLEAN_XDC:
                processLockCleanXdc((LMLockCleanXdcMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_CLEAN_XDC_SINGLE_NODE:
                processLockCleanXdcSingleNode((LMLockCleanXdcMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_RS_PARAMETER:
                processRSParameter((RSParameterReqMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_RS_PARAMETER_SINGLE_NODE:
                processRSParameterSingleNode((RSParameterReqMessage)lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_GET_RS_PARAMETER:
                processGetRSParameter(lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_GET_RS_PARAMETER_SINGLE_NODE:
                processGetRSParameterOnSingleNode(lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_CLEAN_LOCK_COUNTER_SINGLE_NODE:
                processLockCountDownSingleNode(lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_CLEAN_LOCK_COUNTER:
                processCountDownLockAppliedCounter(lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_MSG:
                processGetAppliedList((LMAppliedLockReqMessage) lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_SINGLE_NODE_MSG:
                processGetLocalAppliedList((LMAppliedLockReqMessage) lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_MEM_INFO:
                processLockMemoryUsage(lmMessage, lmConn);
                break;
            case RSMessage.MSG_TYP_LOCK_MEM_SINGLE_NODE_INFO:
                processSingleNodeLockMemoryUsage(lmMessage, lmConn);
                break;
            default:
                LOG.error("Unexpected kind of message: " + lmMessage.getMsgType());
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit process message " + lmMessage + " from " + lmConn);
        }
    }

    private LMLockMemInfoMessage processSingleNodeLockMemoryUsage(RSMessage lmMessage, RSConnection lmConn)
            throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processSingleNodeLockMemoryUsage " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        LMLockMemInfoMessage reqMessage = (LMLockMemInfoMessage) RSMessage
                .getMessage(RSMessage.MSG_TYP_LOCK_MEM_INFO_RESPONSE);
        reqMessage.setRegionName(serverName);
        //header
        //measure by RamUsageEstimator
        long summayMemSize = 128;
        //cache
        {
            summayMemSize += lockVectorCache.sizeof();
            summayMemSize += transactionVectorCache.sizeof();
            summayMemSize += lockHolderVectorCache.sizeof();
        }
        //LockManager
        summayMemSize += LockSizeof.Size_ConcurrentHashMap;
        for (Map.Entry<String, LockManager> entry : lockManagers.entrySet()) {
            //key is reference
            summayMemSize += (LockSizeof.Size_Reference + entry.getValue().sizeof() + LockSizeof.Size_Pair);
        }
        //regionTxIDs
        summayMemSize += LockSizeof.Size_HashSet;
        synchronized (regionTxIDs) {
            //HashSet = HashMap
            summayMemSize += (2 * LockSizeof.Size_Long + LockSizeof.Size_Pair) * regionTxIDs.size();
        }
        //connectionCache
        summayMemSize += connectionCache.sizeof();
        //idleSessions
        summayMemSize += LockSizeof.Size_ArrayList;
        //workingSessions
        summayMemSize += LockSizeof.Size_ArrayList;
        // in case of unknown exception
        try {
            sessionLock.lock();
            for (RSSession session : idleSessions) {
                summayMemSize += (session.sizeof() + LockSizeof.Size_Reference);
            }
            for (RSSession session : workingSessions) {
                summayMemSize += (session.sizeof() + LockSizeof.Size_Reference);
            }
        } catch (Exception e) {
            LOG.error("failed in Sizeof", e);
        } finally {
            sessionLock.unlock();
        }
        //log
        {
            //appliedSet
            if (appliedSet != null) {
                synchronized (lockHolderVectorCache) {//lockHolderVectorCache is lock
                    summayMemSize += LockSizeof.Size_ArrayList;
                    summayMemSize += LockSizeof.Size_Reference * 2 * appliedSet.size();
                }
            }
        }
        //currentLockCount + currentTransactionCount
        summayMemSize += 2 * LockSizeof.Size_AtomicInteger;

        //holderRWLock
        summayMemSize += LockSizeof.Size_Object;

        //overusedLockHolderCount
        summayMemSize += LockSizeof.Size_AtomicInteger;
        //maxUsedLockHolderCount
        summayMemSize += LockSizeof.Size_AtomicInteger;

        //checkMemoryCounter
        summayMemSize += LockSizeof.Size_Integer;
        //memoryBean
        summayMemSize += LockSizeof.Size_Object;

        reqMessage.setLockMemSize(LockUtils.alignLockObjectSize(summayMemSize));
        reqMessage.setUsingLockNum(currentLockCount.get());
        reqMessage.setUsingTransactionNum(currentTransactionCount.get());
        int sumCount = 0;
        holderRWLock.writeLock().lock();
        if (overusedLockHolderCount.intValue() > 0) {
            //overused
            sumCount = lockHolderVectorCache.size() + overusedLockHolderCount.intValue();
        } else {
            sumCount = lockHolderVectorCache.size() - lockHolderVectorCache.cachedSize();
        }
        holderRWLock.writeLock().unlock();

        reqMessage.setMemMax(memoryBean.getHeapMemoryUsage().getMax());
        reqMessage.setMemUsed(memoryBean.getHeapMemoryUsage().getUsed());
        reqMessage.setUsingLockHolderNum(sumCount);
        if (lmConn != null) {
            try {
                lmConn.send(reqMessage);
            } catch (Exception e) {
                LOG.error("processSingleNodeLockMemoryUsage failed to send message to " + lmConn);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processSingleNodeLockMemoryUsage " + reqMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        return reqMessage;
    }

    public void processLockMemoryUsage(RSMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processLockMemoryUsage " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        LMLockMemInfoMessage message = processSingleNodeLockMemoryUsage(lmMessage, null);
        Set<String> regionServers = listAllRegionServersWithoutLocal();
        if (regionServers != null && regionServers.size() > 0) {
            lmMessage.setMsgType(RSMessage.MSG_TYP_LOCK_MEM_SINGLE_NODE_INFO);
            for (String regionServer : regionServers) {
                RSMessage retMsg = sendAndReceiveLMMessage(regionServer, lmMessage);
                if (retMsg != null && retMsg.getMsgType() == RSMessage.MSG_TYP_LOCK_MEM_INFO_RESPONSE) {
                    message.merge((LMLockMemInfoMessage) retMsg);
                } else {
                    message.setErrorFlag(true);
                    String errorMsg = message.getErrorMsg();
                    errorMsg += "unexpected lock response message: " + retMsg + " from " + regionServer + "\n";
                    message.setErrorMsg(errorMsg);
                    LOG.error("unexpected lock response message: " + retMsg + " from " + regionServer);
                }
            }
        }
        if (lmConn != null) {
            try {
                lmConn.send(message);
            } catch (Exception e) {
                LOG.error("processLockMemoryUsage failed to send message to " + lmConn, e);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processLockMemoryUsage " + message + " " + (lmConn == null ? "local" : lmConn));
        }
    }

    private void processLockCountDownSingleNode(RSMessage lmMessage, RSConnection lmConn) {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processLockCountDownSingleNode " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
         holderRWLock.writeLock().lock();
         {
            overusedLockHolderCount.set(0);
            maxUsedLockHolderCount.set(0);
            if (appliedSet != null) {
                synchronized (appliedSet) {
                    appliedSet.clear();
                }
            }
            int extendedSize = lockHolderVectorCache.size() - lockHolderVectorCache.cachedSize();
            while (extendedSize > 0) {
                lockHolderVectorCache.add(new LockHolder());
                extendedSize--;
            }
            lockHolderMap.clear();
        }
        holderRWLock.writeLock().unlock();
        if (lmConn != null) {
            try {
                lmMessage.setMsgType(RSMessage.MSG_TYP_LOCK_CLEAN_LOCK_COUNTER_RESPONSE);
                lmConn.send(lmMessage);
            } catch (Exception e) {
                LOG.error("processLockCountDownSingleNode failed to send message to " + lmConn);
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processLockCountDownSingleNode " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
    }

    private String processRSParameterSingleNode(RSParameterReqMessage lmMessage, RSConnection lmConn) {
        StringBuffer errMsg = new StringBuffer();
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processLockParameterSingleNode " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        Properties properties = lmMessage.getParameters();
        if (properties == null) {
            return "";
        }
        try {
            String envRecordScanRowThreshold = properties.getProperty("RECORD_SCAN_ROW_THRESHOLD");
            if (envRecordScanRowThreshold != null && !envRecordScanRowThreshold.equals("")) {
                RSConstants.RECORD_SCAN_ROW_THRESHOLD = Integer.parseInt(envRecordScanRowThreshold);
            }
        } catch(Exception e) {
            errMsg.append("The value of RECORD_SCAN_ROW_THRESHOLD is invalid\n");
        }
        try {
            String envMaxBlockCheckRetryTimes = properties.getProperty("MAX_BLOCK_CHECK_RETRY_TIMES");
            if (envMaxBlockCheckRetryTimes != null && !envMaxBlockCheckRetryTimes.equals("")) {
                if (!lmMessage.isForced()) {
                    errMsg.append("The option of MAX_BLOCK_CHECK_RETRY_TIMES is not user modifiable\n");
                } else {
                    RSConstants.MAX_BLOCK_CHECK_RETRY_TIMES = Integer.parseInt(envMaxBlockCheckRetryTimes);
                }
            }
        } catch(Exception e) {
            errMsg.append("The value of MAX_BLOCK_CHECK_RETRY_TIMES is invalid\n");
        }
        if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            if (errMsg.length() != 0) {
                //remove last of \n
                errMsg.deleteCharAt(errMsg.length() - 1);
                String str = errMsg.toString();
                LOG.error(str);
                return str;
            }
            return "";
        }
        try {
            String envLockTimeOut = properties.getProperty("LOCK_TIME_OUT");
            if (envLockTimeOut != null) {
                LockConstants.LOCK_WAIT_TIMEOUT = Integer.parseInt(envLockTimeOut.trim());
                TrxRegionEndpoint.checkAndSetLockTimeOut(LockConstants.LOCK_WAIT_TIMEOUT);
            }
        } catch (Exception e) {
            errMsg.append("value of LOCK_TIME_OUT is invalid\n");
        }
        try {
            String envEnableLockForSelect = properties.getProperty("ENABLE_LOCK_FOR_SELECT");
            if (envEnableLockForSelect != null) {
                LockConstants.ENABLE_LOCK_FOR_SELECT = (envEnableLockForSelect.equals("1") || envEnableLockForSelect.equals("true")) ? true : false;
                TrxRegionEndpoint.switchSelectLockMode();
            }
        } catch (Exception e) {
            errMsg.append("value of ENABLE_LOCK_FOR_SELECT is invalid\n");
        }
        try {
            String envLockLogLevel = properties.getProperty("LOCK_LOG_LEVEL");
            if (envLockLogLevel != null && !envLockLogLevel.equals("")) {
                envLockLogLevel = envLockLogLevel.trim().toLowerCase();
                LockLogLevel.parse(envLockLogLevel);
            } else {
                LockLogLevel.init();
            }
        } catch(Exception e) {
            errMsg.append("The value of LOCK_LOG_LEVEL is invalid\n");
        }
        try {
            String envLockSkipMetaTable = properties.getProperty("LOCK_SKIP_META_TABLE");
            if (envLockSkipMetaTable != null) {
                envLockSkipMetaTable = envLockSkipMetaTable.trim();
                LockConstants.LOCK_SKIP_META_TABLE = (envLockSkipMetaTable.equals("1") || envLockSkipMetaTable.equals("true")) ? true : false;
            }
        } catch (Exception e) {
            errMsg.append("The value of LOCK_SKIP_META_TABLE is invalid\n");
        }
        try {
            int tmp = LockConstants.LOCK_CACHE_SIZE;
            String envLockCacheSize = properties.getProperty("LOCK_CACHE_SIZE");
            if (envLockCacheSize != null) {
                tmp = Integer.parseInt(envLockCacheSize.trim());
                tmp = (tmp <= 0) ? LockConstants.LOCK_CACHE_SIZE : tmp;
            }
            if (tmp != LockConstants.LOCK_CACHE_SIZE) {
                int extendedSize = tmp - LockConstants.LOCK_CACHE_SIZE;
                synchronized (lockVectorCache) {
                    lockVectorCache.changeCacheSize(tmp);
                    for (int i = 0; i < extendedSize; i++) {
                        lockVectorCache.add(new Lock());
                    }
                }
                LockConstants.LOCK_CACHE_SIZE = tmp;
            }
        } catch (Exception e) {
            errMsg.append("The value of LOCK_CACHE_SIZE is invalid\n");
        }
        try {
            String lockEscalationThreshold = properties.getProperty("LOCK_ESCALATION_THRESHOLD");
            if (lockEscalationThreshold != null) {
                LockConstants.LOCK_ESCALATION_THRESHOLD = Integer.parseInt(lockEscalationThreshold);
                // LOCK_ESCALATION_THRESHOLD must be odd number
                if (LockConstants.LOCK_ESCALATION_THRESHOLD % 2 == 0) {
                    LockConstants.LOCK_ESCALATION_THRESHOLD = LockConstants.LOCK_ESCALATION_THRESHOLD - 1;
                }
            }
        } catch(Exception e) {
            errMsg.append("The value of LOCK_ESCALATION_THRESHOLD is invalid\n");
        }
        try {
            String tryLockTimes = properties.getProperty("LOCK_TRYLOCK_TIMES");
            if (tryLockTimes != null) {
                if (!lmMessage.isForced()) {
                    errMsg.append("The option of LOCK_TRYLOCK_TIMES is not user modifiable\n");
                } else {
                    LockConstants.LOCK_TRYLOCK_TIMES = Integer.parseInt(tryLockTimes);
                }
            }
        } catch(Exception e) {
            errMsg.append("The value of LOCK_TRYLOCK_TIMES is invalid\n");
        }
        try {
            String restServerUri = properties.getProperty("REST_SERVER_URI");
            if (restServerUri != null && restServerUri.indexOf(":") > 0) {
                LockConstants.REST_SERVER_URI = restServerUri;
                parseRestServerURI();
                if (transactionCleaner == null) {
                    transactionCleaner = new TransactionCleaner(this, stopper);
                    transactionCleaner.setDaemon(true);
                    transactionCleaner.start();
                }
            }
        } catch(Exception e) {
            errMsg.append("The value of REST_SERVER_URI is invalid\n");
        }
        try {
            String xdcMasterSlave= properties.getProperty("XDC_MASTER_SLAVE");
            if (xdcMasterSlave != null && xdcMasterSlave.indexOf(";") > 0) {
                LockConstants.XDC_MASTER_SLAVE = xdcMasterSlave;
                parseXDCMasterSlave();
            }
        } catch(Exception e) {
            errMsg.append("The value of XDC_MASTER_SLAVE is invalid\n");
        }
        try {
            String txCleanInterval = properties.getProperty("TRANSACTION_CLEAN_INTERVAL");
            if (txCleanInterval != null) {
                LockConstants.TRANSACTION_CLEAN_INTERVAL = Long.parseLong(txCleanInterval);
            }
        } catch(Exception e) {
            errMsg.append("The value of TRANSACTION_CLEAN_INTERVAL is invalid\n");
        }
        try {
            String distributedInterval = properties.getProperty("DISTRIBUTED_DEADLOCK_DETECT_INTERVAL");
            if (distributedInterval != null) {
                LockConstants.DISTRIBUTED_DEADLOCK_DETECT_INTERVAL = Integer.parseInt(distributedInterval);
            }
        } catch(Exception e) {
            errMsg.append("The value of DISTRIBUTED_DEADLOCK_DETECT_INTERVAL is invalid\n");
        }
        try {
            String restConnFailedTimes = properties.getProperty("REST_CONN_FAILED_TIMES");
            if (restConnFailedTimes != null) {
                LockConstants.REST_CONN_FAILED_TIMES = Integer.parseInt(restConnFailedTimes);
            }
        } catch(Exception e) {
            errMsg.append("The value of REST_CONN_FAILED_TIMES is invalid\n");
        }
        try {
            String restConnectTimeout = properties.getProperty("REST_CONNECT_TIMEOUT");
            if (restConnectTimeout != null) {
                LockConstants.REST_CONNECT_TIMEOUT = Integer.parseInt(restConnectTimeout);
            }
        } catch(Exception e) {
            errMsg.append("The value of REST_CONNECT_TIMEOUT is invalid\n");
        }
        try {
            String restReadTimeout = properties.getProperty("REST_READ_TIMEOUT");
            if (restReadTimeout != null) {
                LockConstants.REST_READ_TIMEOUT = Integer.parseInt(restReadTimeout);
            }
        } catch(Exception e) {
            errMsg.append("The value of REST_READ_TIMEOUT is invalid\n");
        }
        try {
            String restConnectDetectInterval = properties.getProperty("REST_CONNECT_DETECT_INTERVAL");
            if (restConnectDetectInterval != null) {
                LockConstants.REST_CONNECT_DETECT_INTERVAL = Long.parseLong(restConnectDetectInterval);
            }
        } catch(Exception e) {
            errMsg.append("The value of REST_CONNECT_DETECT_INTERVAL is invalid\n");
        }
        try {
            String envLockStatistics = properties.getProperty("ENABLE_LOCK_STATISTICS");
            if (envLockStatistics != null) {
                LockConstants.ENABLE_LOCK_STATISTICS = (Integer.parseInt(envLockStatistics.trim()) == 1 ? true : false);
            }
        } catch(Exception e) {
            errMsg.append("The value of ENABLE_LOCK_STATISTICS is invalid\n");
        }
        try {
            String envLockStatistics = properties.getProperty("LOCK_WAIT_INFO_WAITTIME_THRESHOLD");
            if (envLockStatistics != null) {
                int num = Integer.parseInt(envLockStatistics.trim());
                //the min values is LOCK_WAIT_TIMEOUT for debug
                if (num != LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD) {
                    LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD = ((num < 100)
                        && (num != 0)) ? 100 : num;
                    if (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD >= LockConstants.LOCK_WAIT_TIMEOUT) {
                        LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD = (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD
                                / LockConstants.LOCK_WAIT_TIMEOUT - 1) * LockConstants.LOCK_WAIT_TIMEOUT;
                        if (LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD == 0) {
                            LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD = LockConstants.LOCK_WAIT_TIMEOUT;
                        }
                    } else {
                        //LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD = LockConstants.LOCK_WAIT_TIMEOUT;
                        LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD = LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD;
                    }
                    if (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD != 0 && !isJUnit
                            && lockWaitInfoOutputWorker == null) {
                        lockWaitInfoOutputWorker = new LockWaitInfoOutputWorker(this, stopper, hbaseLogpath);
                        lockWaitInfoOutputWorker.run();
                    } else if (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD == 0
                            && lockWaitInfoOutputWorker != null) {
                        lockWaitInfoOutputWorker.stop();
                        lockWaitInfoOutputWorker = null;
                    } else {
                        lockWaitInfoOutputWorker.updateFetchLockInfoTimeout();
                    }
                }
            }
        } catch (Exception e) {
            errMsg.append("The value of LOCK_WAIT_INFO_WAITTIME_THRESHOLD is invalid\n");
        }
        try {
            String envRecordTxLockNumThreshold  = properties.getProperty("RECORD_TRANSACTION_LOCKNUM_THRESHOLD");
            if (envRecordTxLockNumThreshold != null) {
                LockConstants.RECORD_TRANSACTION_LOCKNUM_THRESHOLD = Integer.parseInt(envRecordTxLockNumThreshold.trim());
            }
        } catch (Exception e) {
            errMsg.append("The value of RECORD_TRANSACTION_LOCKNUM_THRESHOLD is invalid\n");
        }
        try {
            String envEnableSavepoint  = properties.getProperty("ENABLE_SAVEPOINT");
            if (envEnableSavepoint != null) {
                if (!lmMessage.isForced()) {
                    errMsg.append("The option of ENABLE_SAVEPOINT is not user modifiable\n");
                } else {
                    envEnableSavepoint = envEnableSavepoint.trim().toLowerCase();
                    LockConstants.ENABLE_SAVEPOINT = (envEnableSavepoint.equals("1") || envEnableSavepoint.equals("true"));
                }
            }
        } catch (Exception e) {
            errMsg.append("The value of ENABLE_SAVEPOINT is invalid\n");
        }
        try {
            String envNumOfLogOneTime = properties.getProperty("NUMBER_OF_OUTPUT_LOGS_PER_TIME");
            if (envNumOfLogOneTime != null) {
                int tmp = Integer.parseInt(envNumOfLogOneTime.trim());
                LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME = tmp > 0 ? tmp : LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
            }
        } catch (Exception e) {
            errMsg.append("The value of NUMBER_OF_OUTPUT_LOGS_PER_TIME is invalid\n");
        }
        try {
            String envEnableLockForFullScan = properties.getProperty("ENABLE_TABLELOCK_FOR_FULL_SCAN");
            if (envEnableLockForFullScan != null) {
                LockConstants.ENABLE_TABLELOCK_FOR_FULL_SCAN = (envEnableLockForFullScan.equals("1") || envEnableLockForFullScan.equals("true")) ? true : false;
            }
        } catch (Exception e) {
            errMsg.append("value of ENABLE_TABLELOCK_FOR_FULL_SCAN is invalid\n");
        }

        holderRWLock.writeLock().lock();
        {

            try {
                String envMaxLockNumber = properties.getProperty("MAX_LOCK_NUMBER");
                if (envMaxLockNumber != null) {
                    LockConstants.MAX_LOCK_NUMBER = Integer.parseInt(envMaxLockNumber.trim());
                    LockConstants.MAX_LOCK_NUMBER = (LockConstants.MAX_LOCK_NUMBER < 0)
                            ? 0
                            : LockConstants.MAX_LOCK_NUMBER;
                }
            } catch (Exception e) {
                errMsg.append("The value of MAX_LOCK_NUMBER is invalid\n");
            }

            try {
                String envMaxLockPerTransNumber = properties.getProperty("MAX_LOCK_NUMBER_PER_TRANS");
                if (envMaxLockPerTransNumber != null) {
                    LockConstants.MAX_LOCK_NUMBER_PER_TRANS = Integer.parseInt(envMaxLockPerTransNumber.trim());
                    LockConstants.MAX_LOCK_NUMBER_PER_TRANS = (LockConstants.MAX_LOCK_NUMBER_PER_TRANS < 0)
                            ? 0
                            : LockConstants.MAX_LOCK_NUMBER_PER_TRANS;
                }
            } catch (Exception e) {
                LOG.error("The value of MAX_LOCK_NUMBER_PER_TRANS is invalid");
            }

            if (LockConstants.MAX_LOCK_NUMBER != 0
                    && LockConstants.MAX_LOCK_NUMBER_PER_TRANS > LockConstants.MAX_LOCK_NUMBER) {
                LOG.error("The value of MAX_LOCK_NUMBER_PER_TRANS is bigger than MAX_LOCK_NUMBER");
                LockConstants.MAX_LOCK_NUMBER_PER_TRANS = LockConstants.MAX_LOCK_NUMBER;
            }

            try {
                String envEnableLongWaitLockPrinting = properties.getProperty("ENABLE_LONG_WAIT_LOCK_PRINTING");
                if (envEnableLongWaitLockPrinting != null) {
                    envEnableLongWaitLockPrinting = envEnableLongWaitLockPrinting.trim().toLowerCase();
                    LockConstants.ENABLE_LONG_WAIT_LOCK_PRINTING = (envEnableLongWaitLockPrinting.equals("1")
                            || envEnableLongWaitLockPrinting.equals("true"));
                }
            } catch (Exception e) {
                errMsg.append("The value of ENABLE_LONG_WAIT_LOCK_PRINTING is invalid\n");
            }

            if (LockConstants.MAX_LOCK_NUMBER != lockHolderVectorCache.size()) {
                int currentCacheSize = lockHolderVectorCache.size();
                int extendedSize = 0;
                if (LockConstants.MAX_LOCK_NUMBER != 0) {
                    extendedSize = LockConstants.MAX_LOCK_NUMBER - currentCacheSize;
                } else {
                    extendedSize = LockConstants.DEFALUT_LOCK_HOLDER_SIZE - currentCacheSize;
                }
                int usedSize = currentCacheSize - lockHolderVectorCache.cachedSize();
                boolean isOverused = true;
                if (extendedSize < 0) {
                    //shrink
                    if (usedSize == currentCacheSize) {
                        //overused
                        overusedLockHolderCount.addAndGet(-1 * extendedSize);
                    } else if (lockHolderVectorCache.cachedSize() < (-1 * extendedSize)) {
                        //will be overused
                        overusedLockHolderCount.set(-1 * (extendedSize + lockHolderVectorCache.cachedSize()));
                    } else {
                        //no overused
                        isOverused = false;
                    }
                } else {
                    //extend
                    if (usedSize == currentCacheSize) {
                        extendedSize -= overusedLockHolderCount.intValue();
                        if (extendedSize < 0) {
                            //still overused
                            overusedLockHolderCount.set(-1 * extendedSize);
                        } else {
                            //will be no overused
                            overusedLockHolderCount.set(0);
                            isOverused = false;
                        }
                    } else {
                        isOverused = false;
                    }
                }
                if (isOverused) {
                    lockHolderVectorCache.clear();
                }
                int newSize = LockConstants.MAX_LOCK_NUMBER == 0 ? LockConstants.DEFALUT_LOCK_HOLDER_SIZE : LockConstants.MAX_LOCK_NUMBER;
                lockHolderVectorCache.changeCacheSize(newSize);
                if (extendedSize < 0) {
                    //shrink
                    while (usedSize > 0) {
                        //drop
                        LockHolder tmp = lockHolderVectorCache.getElement();
                        tmp = null;
                        usedSize --;
                    }
                }
                while (extendedSize > 0) {
                    //fill new instance
                    lockHolderVectorCache.add(new LockHolder());
                    extendedSize--;
                }
            }

            try {
                String envTraceLockApplied = properties.getProperty("TRACE_LOCK_APPLIED");
                if (envTraceLockApplied != null) {
                    envTraceLockApplied = envTraceLockApplied.trim();
                    LockConstants.TRACE_LOCK_APPLIED = (envTraceLockApplied.equals("1")
                            || envTraceLockApplied.equals("true"));
                }
            } catch (Exception e) {
                errMsg.append("The value of TRACE_LOCK_APPLIED is invalid\n");
            }

            if (LockConstants.TRACE_LOCK_APPLIED && appliedSet == null) {
                appliedSet = Collections
                        .synchronizedSet(new HashSet<LockHolder>(LockConstants.MAX_PRELOAD_APPLIED_LOG));
            } else if (!LockConstants.TRACE_LOCK_APPLIED && appliedSet != null) {
                synchronized (appliedSet) {
                    appliedSet.clear();
                    appliedSet = null;
                }
            }

            try {
                String envStr = properties.getProperty("REGION_MEMORY_HIGHLOAD_THRESHOLD");
                if (envStr != null) {
                    int num = Integer.parseInt(envStr.trim());
                    if (num >= 10 && num <= 100)
                        RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD = num;
                    else
                        LOG.error("The value of REGION_MEMORY_HIGHLOAD_THRESHOLD must in [10-100]");
                }
            } catch(Exception e) {
                LOG.error("The value of REGION_MEMORY_HIGHLOAD_THRESHOLD is invalid");
            }

            try {
                String envStr = properties.getProperty("REGION_MEMORY_WARNING_THRESHOLD");
                if (envStr != null) {
                    int num = Integer.parseInt(envStr.trim());
                    if (num >= 0 && num <= RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD)
                        RSConstants.REGION_MEMORY_WARNING_THRESHOLD = num;
                    else
                        LOG.error("The value of REGION_MEMORY_WARNING_THRESHOLD must in [70-REGION_MEMORY_HIGHLOAD_THRESHOLD]");
                }
            } catch(Exception e) {
                LOG.error("The value of REGION_MEMORY_WARNING_THRESHOLD is invalid");
            }

            if (RSConstants.REGION_MEMORY_WARNING_THRESHOLD > RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD)
                RSConstants.REGION_MEMORY_WARNING_THRESHOLD = RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD;

            try {
                String envStr = properties.getProperty("CHECK_REGION_MEMORY_INTERVALS");
                if (envStr != null) {
                    int num = Integer.parseInt(envStr.trim());
                    if (num >= 1) {
                        RSConstants.CHECK_REGION_MEMORY_INTERVALS = num;
                        if (checkMemoryCounter.intValue() > num)
                            checkMemoryCounter.set(num);
                    } else
                        LOG.error("The value of CHECK_REGION_MEMORY_INTERVALS must > 0");
                }
            } catch(Exception e) {
                LOG.error("The value of CHECK_REGION_MEMORY_INTERVALS is invalid");
            }
        }
        holderRWLock.writeLock().unlock();

        try {
            String envStr = properties.getProperty("MEMORY_ALLOC_AMPLIFICATION_FACTOR");
            if (envStr != null) {
                int num = Integer.parseInt(envStr.trim());
                if (num >= 0) {
                    RSConstants.MEMORY_ALLOC_AMPLIFICATION_FACTOR = num;
                } else
                    LOG.error("The value of MEMORY_ALLOC_AMPLIFICATION_FACTOR must >= 0");
            }
        } catch(Exception e) {
            LOG.error("The value of MEMORY_ALLOC_AMPLIFICATION_FACTOR is invalid");
        }

        try {
            String envClientLockRetryCount = properties.getProperty("LOCK_CLIENT_RETRIES_TIMES");
            if (envClientLockRetryCount != null) {
                if (!lmMessage.isForced()) {
                    errMsg.append("The option of LOCK_CLIENT_RETRIES_TIMES is not user modifiable\n");
                } else {
                    int num = Integer.parseInt(envClientLockRetryCount.trim());
                    LockConstants.LOCK_CLIENT_RETRIES_TIMES = (num > 0) ? num : LockConstants.LOCK_CLIENT_RETRIES_TIMES;
                }
            }
        } catch (Exception e) {
            errMsg.append("The value of LOCK_CLIENT_RETRIES_TIMES is invalid\n");
        }

        try {
            String envFetchLockInfoTimeoutString = properties.getProperty("LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT");
            if (envFetchLockInfoTimeoutString != null) {
                int num = Integer.parseInt(envFetchLockInfoTimeoutString);
                LockConstants.LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT = (num > LockConstants.LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT)
                        ? num
                        : LockConstants.LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT;
                if (lockWaitInfoOutputWorker != null) {
                    lockWaitInfoOutputWorker.updateFetchLockInfoTimeout();
                }
            }
        } catch(Exception e) {
            errMsg.append("The value of LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT is invalid\n");
        }

        try {
            String envEnableTraceWaitLockInfo = properties.getProperty("ENABLE_TRACE_WAIT_LOCK_INFO");
            if (envEnableTraceWaitLockInfo != null) {
                envEnableTraceWaitLockInfo = envEnableTraceWaitLockInfo.trim().toLowerCase();
                LockConstants.ENABLE_TRACE_WAIT_LOCK_INFO = (envEnableTraceWaitLockInfo.equals("1")
                        || envEnableTraceWaitLockInfo.equals("true"));
            }
        } catch (Exception e) {
            errMsg.append("The value of ENABLE_TRACE_WAIT_LOCK_INFO is invalid\n");
        }

        String envString = null;
        try {
            envString = properties.getProperty("ENABLE_ONLINE_BALANCE");
            if (envString != null) {
                envString = envString.trim();
                RSConstants.ONLINE_BALANCE_ENABLED = (envString.equals("1") || envString.equals("true"));
            }
        } catch (Exception e) {
            LOG.error("The value of ENABLE_ONLINE_BALANCE is invalid");
        }
        try {
            envString = properties.getProperty("PRINT_TRANSACTION_LOG");
            if (envString != null) {
                RSConstants.PRINT_TRANSACTION_LOG = Integer.parseInt(envString.trim());
            }
        } catch (Exception e) {
            LOG.error("value of PRINT_TRANSACTION_LOG is invalid");
        }
        try {
            envString = properties.getProperty("DISABLE_NEWOBJECT_FOR_ENDPOINT");
            if (envString != null) {
                int num = Integer.parseInt(envString.trim());
                RSConstants.DISABLE_NEWOBJECT_FOR_ENDPOINT = (num > 0) ? 1 : 0;
            }
        } catch (Exception e) {
            LOG.error("value of DISABLE_NEWOBJECT_FOR_ENDPOINT is invalid");
        }
        try {
            envString = properties.getProperty("DISABLE_NEWOBJECT_FOR_MC");
            if (envString != null) {
                int num = Integer.parseInt(envString.trim());
                RSConstants.DISABLE_NEWOBJECT_FOR_MC = (num > 0) ? 1 : 0;
            }
        } catch (Exception e) {
            LOG.error("value of DISABLE_NEWOBJECT_FOR_MC is invalid");
        }
        try {
            envString = properties.getProperty("RECORD_TIME_COST_COPRO");
            if (envString != null) {
                RSConstants.RECORD_TIME_COST_COPRO = Integer.parseInt(envString.trim());
            }
        } catch (Exception e) {
            LOG.error("value of RECORD_TIME_COST_COPRO is invalid");
        }
        try {
            envString = properties.getProperty("RECORD_SCAN_ROW_THRESHOLD");
            if (envString != null) {
                RSConstants.RECORD_SCAN_ROW_THRESHOLD = Integer.parseInt(envString.trim());
            }
        } catch (Exception e) {
            LOG.error("value of RECORD_SCAN_ROW_THRESHOLD is invalid");
        }
        try {
            envString = properties.getProperty("MAX_BLOCK_CHECK_RETRY_TIMES");
            if (envString != null) {
                RSConstants.MAX_BLOCK_CHECK_RETRY_TIMES = Integer.parseInt(envString.trim());
            }
        } catch (Exception e) {
            LOG.error("value of MAX_BLOCK_CHECK_RETRY_TIMES is invalid");
        }
        try {
            envString = properties.getProperty("RECORD_TIME_COST_COPRO_ALL");
            if (envString != null) {
                RSConstants.RECORD_TIME_COST_COPRO_ALL = Integer.parseInt(envString.trim());
            }
        } catch (Exception e) {
            LOG.error("value of RECORD_TIME_COST_COPRO_ALL is invalid");
        }

        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processLockParameterSingleNode " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        if (errMsg.length() != 0) {
            //remove last of \n
            errMsg.deleteCharAt(errMsg.length() - 1);
            String str = errMsg.toString();
            LOG.error(str);
            return str;
        }
        return "";
    }

    private void processRSParameter(RSParameterReqMessage lmMessage, RSConnection lmConn) throws Exception {
        Set<String> regionServers = listAllRegionServersWithoutLocal();
        if (regionServers != null && regionServers.size() > 0) {
            lmMessage.setMsgType(RSMessage.MSG_TYP_RS_PARAMETER_SINGLE_NODE);
            for (String regionServer : regionServers) {
                sendLMMessage(regionServer, lmMessage);
            }
        }
        //the format of parameters is same on all servers, so one wrong, all wrong.
        String errMsg = processRSParameterSingleNode(lmMessage, lmConn);
        if (lmConn == null) {
            return;
        }
        RSMessage retMsg = null;
        if (errMsg.isEmpty()) {
            retMsg = RSMessage.OK;
        } else {
            retMsg = RSMessage.NO;
            retMsg.setErrorFlag(true);
            retMsg.setErrorMsg(errMsg);
        }
        try {
            lmConn.send(retMsg);
        } catch (Exception e) {
            LOG.error("processLockParameter failed to send " + retMsg.toString() + " for set lock parameter to " + lmConn);
            throw e;
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processLockParameter " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
    }

    private void processGetVailRegionList(LMMRegionInfoMsg lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processGetVailRegionList " + lmMessage + " " + lmConn);
        }
        lmMessage.setRegionList(new ArrayList<String>(listAllRegionServerIPs()));
        try {
            lmConn.send(lmMessage);
        } catch (Exception e) {
            LOG.error("processGetVailRegionList failed to send message to " + lmConn);
            throw e;
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processGetVailRegionList " + lmMessage + " " + lmConn);
        }
    }

    private void processLockCleanXdcSingleNode(LMLockCleanXdcMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel)
            LOG.info("enter processLockCleanXdcSingleNode" + lmMessage + " " + lmConn);
        if (lmConn != null) {
            try {
                if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
                    lmConn.send(RSMessage.NO);
                    return;
                }
                int clusterId = lmMessage.getClusterId();
                XDC_STATUS xdcStatus = lmMessage.getXdcStatus();
                boolean setFlag = false;
                LOG.info("processLockCleanXdcSingleNode clusterId: " + clusterId  + " XDC_STATUS: " + xdcStatus);
                if (xdcStatus == XDC_STATUS.XDC_PEER_DOWN) {
                    regionServerLock.lock();
                    for (AbstractMap.SimpleEntry<Integer, Boolean> pairItem : xdcListPair) {
                        if (pairItem.getKey() == clusterId) {
                            pairItem.setValue(false);
                            setFlag = true;
                            break;
                        }
                    }
                    regionServerLock.unlock();
                    if (setFlag) {
                        lmConn.send(RSMessage.OK);
                        LOG.info("processLockCleanXdcSingleNode cluserid: " + clusterId + " down succeeded");
                    } else {
                        lmConn.send(RSMessage.NO);
                        LOG.info("processLockCleanXdcSingleNode cluserid: " + clusterId + " down failed");
                    }
                } else if (xdcStatus == XDC_STATUS.XDC_PEER_UP) {
                    regionServerLock.lock();
                    for (AbstractMap.SimpleEntry<Integer, Boolean> pairItem : xdcListPair) {
                        if (pairItem.getKey() == clusterId) {
                            pairItem.setValue(true);
                            setFlag = true;
                            break;
                        }
                    }
                    regionServerLock.unlock();
                    if (setFlag) {
                        lmConn.send(RSMessage.OK);
                        LOG.info("processLockCleanXdcSingleNode cluserid: " + clusterId + " up succeeded");
                    } else {
                        lmConn.send(RSMessage.NO);
                        LOG.info("processLockCleanXdcSingleNode cluserid: " + clusterId + " up failed");
                    }
                } else {
                    LOG.error("processLockCleanXdcSingleNode failed because of invaild cluserid: " + clusterId + " or invaild XDC_STATUS: " + xdcStatus);
                    lmConn.send(RSMessage.NO);
                }
            } catch (Exception e) {
                LOG.error("processLockCleanXdcSingleNode failed to send message to " + lmConn , e);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel)
            LOG.info("exit processLockCleanXdc" + lmMessage + " " + lmConn);
    }

    private void processLockCleanXdc(LMLockCleanXdcMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel)
            LOG.info("enter processLockCleanXdc" + lmMessage + " " + lmConn);

        if (lmConn != null) {
            if ((LockConstants.ENABLE_ROW_LEVEL_LOCK && this.restUriListList.size() != 0 && xdcListPair.size() != 0) == false) {
                lmConn.send(RSMessage.LOCK_CLEAN_XDC_NO_EXIST);
                return;
            }
            Set<String> regionServers = listAllRegionServersWithoutLocal();
            if (regionServers != null && regionServers.size() > 0) {
                lmMessage.setMsgType(RSMessage.MSG_TYP_LOCK_CLEAN_XDC_SINGLE_NODE);
                for (String regionServer : regionServers) {
                    RSMessage retMsg = sendAndReceiveLMMessage(regionServer, lmMessage);
                    if (retMsg != null && retMsg.getMsgType() != RSMessage.MSG_TYP_OK) {
                        try {
                            lmConn.send(RSMessage.NO);
                        } catch (Exception e) {
                            LOG.error("processLockCleanXdc failed to send message to " + lmConn , e);
                            throw e;
                        }
                        return;
                    }
                }
            }
            try {
                int clusterId = lmMessage.getClusterId();
                XDC_STATUS xdcStatus = lmMessage.getXdcStatus();
                boolean setFlag = false;
                LOG.info("processLockCleanXdc clusterId: " + clusterId  + " XDC_STATUS: " + xdcStatus);
                if (xdcStatus == XDC_STATUS.XDC_PEER_DOWN) {
                    regionServerLock.lock();
                    for (AbstractMap.SimpleEntry<Integer, Boolean> pairItem: xdcListPair) {
                        if (pairItem.getKey() == clusterId) {
                            pairItem.setValue(false);
                            setFlag = true;
                            break;
                        }
                    }
                    regionServerLock.unlock();
                    if (setFlag) {
                        lmConn.send(RSMessage.OK);
                        LOG.info("processLockCleanXdc cluserid: " + clusterId + " down succeeded");
                    } else {
                        lmConn.send(RSMessage.NO);
                        LOG.info("processLockCleanXdc cluserid: " + clusterId + " down failed");
                    }
                } else if (xdcStatus == XDC_STATUS.XDC_PEER_UP) {
                    regionServerLock.lock();
                    for (AbstractMap.SimpleEntry<Integer, Boolean> pairItem : xdcListPair) {
                        if (pairItem.getKey() == clusterId) {
                            pairItem.setValue(true);
                            setFlag = true;
                            break;
                        }
                    }
                    regionServerLock.unlock();
                    if (setFlag) {
                        lmConn.send(RSMessage.OK);
                        LOG.info("processLockCleanXdc cluserid: " + clusterId + " up succeeded");
                    } else {
                        lmConn.send(RSMessage.NO);
                        LOG.info("processLockCleanXdc cluserid: " + clusterId + " up failed");
                    }
                } else {
                    LOG.error("processLockCleanXdc failed because of invaild cluserid: " + clusterId + " or invaild XDC_STATUS: " + xdcStatus);
                    lmConn.send(RSMessage.NO);
                }
            } catch (Exception e) {
                LOG.error("processLockCleanXdc failed to send message to " + lmConn, e);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel)
            LOG.info("exit processLockCleanXdc" + lmMessage + " " + lmConn);
    }

    private void processCountDownLockAppliedCounter(RSMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processCountDownLockAppliedCounter " + lmMessage + " "
                    + (lmConn == null ? "local" : lmConn));
        }
        processLockCountDownSingleNode(lmMessage, null);
        Set<String> regionServers = listAllRegionServersWithoutLocal();
        if (regionServers != null && regionServers.size() > 0) {
            for (String regionServer : regionServers) {
                lmMessage.setMsgType(RSMessage.MSG_TYP_LOCK_CLEAN_LOCK_COUNTER_SINGLE_NODE);
                RSMessage retMsg = sendAndReceiveLMMessage(regionServer, lmMessage);
                if (retMsg != null && retMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_RESPONSE) {
                    LOG.error("unexpected lock response message: " + retMsg + " from " + regionServer);
                }
            }
        }
        if (lmConn != null) {
            try {
                lmConn.send(RSMessage.OK);
            } catch (Exception e) {
                LOG.error("processCountDownLockAppliedCounter failed to send message to " + lmConn, e);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info(
                    "exit processCountDownLockAppliedCounter " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
    }

    public LMAppliedLockMessage processGetAppliedList(LMAppliedLockReqMessage lmMessage, RSConnection lmConn)
            throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processGetAppliedList " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        LMAppliedLockMessage message = processGetLocalAppliedList(lmMessage, null);
        Set<String> regionServers = listAllRegionServersWithoutLocal();
        if (regionServers != null && regionServers.size() > 0) {
            lmMessage.setMsgType(RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_SINGLE_NODE_MSG);
            for (String regionServer : regionServers) {
                RSMessage retMsg = sendAndReceiveLMMessage(regionServer, lmMessage);
                if (retMsg != null && retMsg.getMsgType() == RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_RESPONSE) {
                    message.merge((LMAppliedLockMessage) retMsg);
                } else {
                    message.setErrorFlag(true);
                    String errorMsg = message.getErrorMsg();
                    errorMsg += "unexpected lock response message: " + retMsg + " from " + regionServer + "\n";
                    message.setErrorMsg(errorMsg);
                    LOG.error("unexpected lock response message: " + retMsg + " from " + regionServer);
                }
            }
        }
        if (lmConn != null) {
            try {
                lmConn.send(message);
            } catch (Exception e) {
                LOG.error("processGetAppliedList failed to send message to " + lmConn, e);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processGetAppliedList " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        return message;
    }

    private LMAppliedLockMessage processGetLocalAppliedList(LMAppliedLockReqMessage lmMessage, RSConnection lmConn)
            throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processGetLocalAppliedList " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        LMAppliedLockMessage message = (LMAppliedLockMessage) RSMessage
                .getMessage(RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_RESPONSE);
        LMAppliedLockInfoInPerServer lockInfoInLocalServer = new LMAppliedLockInfoInPerServer();
        lockInfoInLocalServer.lockHolderLimitNum = LockConstants.MAX_LOCK_NUMBER;
        lockInfoInLocalServer.lockHolderPerTransLimitNum = LockConstants.MAX_LOCK_NUMBER_PER_TRANS;
        Map<Long, Integer> transMap = new HashMap<>();
        List<LockHolderInfo> holdersList = new ArrayList<>();
        holderRWLock.writeLock().lock();
        {
            lockInfoInLocalServer.maxLockNum = maxUsedLockHolderCount.intValue();
            if (overusedLockHolderCount.intValue() != 0) {
                //overused
                lockInfoInLocalServer.sumLockNum = lockHolderVectorCache.size() + overusedLockHolderCount.intValue();
            } else {
                lockInfoInLocalServer.sumLockNum = lockHolderVectorCache.size() - lockHolderVectorCache.cachedSize();
            }
            //gets lock holder counter for all transactions in current region server
            synchronized (this.lockHolderMap) {
                for (Map.Entry<Long, AtomicInteger> entry : this.lockHolderMap.entrySet()) {
                    int holderNum = entry.getValue().intValue();
                    if (holderNum == 0) {
                        continue;
                    }
                    Long txId = entry.getKey();
                    if (transMap.containsKey(txId)) {
                        int tmp = transMap.get(txId);
                        if (tmp < 0 || holderNum < 0) {
                            //in rollback
                            tmp = 0 - Math.abs(tmp) + Math.abs(holderNum);
                        } else
                            tmp += holderNum;
                        transMap.replace(txId, tmp);
                    } else {
                        transMap.put(txId, holderNum);
                    }
                }
            }
            //if TRACE_LOCK_APPLIED == true
            if (lmMessage.getDebug() == 1 && LockConstants.TRACE_LOCK_APPLIED && appliedSet != null) {
                synchronized (appliedSet) {
                    for (LockHolder lockHolder : appliedSet) {
                        if (lockHolder == null) {
                            continue;
                        }
                        RowKey rowKey = null;
                        Lock lock = lockHolder.getLock();
                        if (lock != null) {
                            rowKey = lock.getRowKey();
                        }
                        LockHolderInfo info = new LockHolderInfo();
                        info.setRegionName((lock == null) ? "" : lock.getRegionName());
                        info.setRowKey((rowKey != null) ? rowKey.toString() : "");
                        info.setHolding(Arrays.copyOf(lockHolder.getHolding(), 7));
                        if (lockHolder.getTransaction() != null) {
                            info.setTxID(lockHolder.getTransaction().getTxID());
                        } else {
                            info.setTxID(-1L);
                        }
                        info.setSvptID(lockHolder.getSvptID());
                        info.setImplicitSavepoint(lockHolder.isImplicitSavepoint());
                        holdersList.add(info);
                    }
                }
            }
        }
        holderRWLock.writeLock().unlock();
        lockInfoInLocalServer.transMap = transMap;
        lockInfoInLocalServer.appliedList = holdersList;
        message.getServerList().put(this.serverName, lockInfoInLocalServer);

        if (lmConn != null) {
            try {
                lmConn.send(message);
            } catch (Exception e) {
                LOG.error(
                        "processGetLocalAppliedList failed to send message to " + (lmConn == null ? "local" : lmConn));
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processGetLocalAppliedList " + message + " " + (lmConn == null ? "local" : lmConn));
        }
        return message;
    }

    private void processSingleNodeLockStatistics(LMLockStatisticsReqMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processSingleNodeLockStatistics " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        LMLockStatisticsMessage message = (LMLockStatisticsMessage) RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_STATISTICS_INFO_RESPONSE);
        if (LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            message.getLockStatistics(lmMessage);
        } else {
            message.setErrorFlag(true);
            if (LockConstants.HAS_VALID_LICENSE)
                message.setErrorMsg("row level lock is disable");
            else
                message.setErrorMsg("The module ROWLOCK is not supported in this database version.");
        }
        if (lmConn != null) {
            try {
                lmConn.send(message);
            } catch (Exception e) {
                LOG.error("processSingleNodeLockStatistics failed to send message to " + lmConn);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processSingleNodeLockStatistics " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
    }

    public LMLockStatisticsMessage processLockStatistics(LMLockStatisticsReqMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processLockStatistics " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        LMLockStatisticsMessage message = (LMLockStatisticsMessage) RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_STATISTICS_INFO_RESPONSE);
        if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            message.setErrorFlag(true);
            if (LockConstants.HAS_VALID_LICENSE)
                message.setErrorMsg("row level lock is disable");
            else
                message.setErrorMsg("The module ROWLOCK is not supported in this database version.");
        } else {
            message.getLockStatistics(lmMessage);
            Set<String> regionServers = listAllRegionServersWithoutLocal();
            if (regionServers != null && regionServers.size() > 0) {
                lmMessage.setMsgType(RSMessage.MSG_TYP_LOCK_STATISTICS_INFO_SINGLE_NODE);
                for (String regionServer : regionServers) {
                    RSMessage retMsg = sendAndReceiveLMMessage(regionServer, lmMessage);
                    if (retMsg != null && retMsg.getMsgType() == RSMessage.MSG_TYP_LOCK_STATISTICS_INFO_RESPONSE) {
                        message.merge((LMLockStatisticsMessage) retMsg, lmMessage.getTopN());
                    } else {
                        message.setErrorFlag(true);
                        String errorMsg = message.getErrorMsg();
                        errorMsg += "unexpected lock response message: " + retMsg + " from " + regionServer + "\n";
                        message.setErrorMsg(errorMsg);
                        LOG.error("unexpected lock response message: " + retMsg + " from " + regionServer);
                    }
                }
            }
        }
        if (lmConn != null) {
            try {
                lmConn.send(message);
            } catch (Exception e) {
                LOG.error("processLockStatistics failed to send message to " + lmConn, e);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processLockStatistics " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        return message;
    }

    /**
     * get lock info in all region server
     * @param lmMessage lock req message
     * @param lmConn    connection
     */
    public LMLockInfoMessage processLockInfo(LMLockInfoReqMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processLockInfo " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        LMLockInfoMessage message = (LMLockInfoMessage) RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_INFO_RESPONSE);
        if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            message.setErrorFlag(true);
            if (LockConstants.HAS_VALID_LICENSE)
                message.setErrorMsg("row level lock is disable");
            else
                message.setErrorMsg("The module ROWLOCK is not supported in this database version.");
        } else {
            int maxLockInfoNum = lmMessage.getMaxRetLockInfoLimit();
            maxLockInfoNum = (maxLockInfoNum == 0) ? LockConstants.LOCK_INFO_RESPONSE_SIZE_LIMIT : maxLockInfoNum;
            message.setLockInfoNumLimit(maxLockInfoNum);
            message.getLock(lmMessage);
            if (message.getLockInfoNumLimit() < 0) {
                message.setErrorFlag(true);
                message.setErrorMsg("The number of client wants lock object is overload, the max limit number is " + maxLockInfoNum);
            }
            Set<String> regionServers = listAllRegionServersWithoutLocal();
            if (regionServers != null && regionServers.size() > 0) {
                lmMessage.setMsgType(RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE);
                for (String regionServer : regionServers) {
                    RSMessage retMsg = sendAndReceiveLMMessage(regionServer, lmMessage);
                    if (retMsg != null && retMsg.getMsgType() == RSMessage.MSG_TYP_LOCK_INFO_RESPONSE) {
                        message.merge((LMLockInfoMessage) retMsg);
                    } else {
                        LOG.error("unexpected lock response message: " + retMsg + " from " + regionServer);
                        if (message.isErrorFlag()) {
                            message.setErrorMsg(message.getErrorMsg() + " unexpected lock response message from " + regionServer);
                        } else {
                            message.setErrorFlag(true);
                            message.setErrorMsg("unexpected lock response message from " + regionServer);
                        }
                    }
                }
            }
            if (lmMessage.getType().equals(LMLockInfoMessage.DEAD_LOCK)) {
                message.setDeadLockMsgMap(getDeadLockInfo(message.getLockWaitMsgMap()));
                message.setLockWaitMsgMap(null);
            }
        }
        if (lmConn != null) {
            try {
                lmConn.send(message);
            } catch (Exception e) {
                LOG.error("processLockInfo failed to send message to " + lmConn, e);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processLockInfo " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        return message;
    }

    public LMLockInfoMessage processSingleNodeLockInfo(LMLockInfoReqMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processSingleNodeLockInfo " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        LMLockInfoMessage message = (LMLockInfoMessage) RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_INFO_RESPONSE);
        if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            message.setErrorFlag(true);
            if (LockConstants.HAS_VALID_LICENSE)
                message.setErrorMsg("row level lock is disable");
            else
                message.setErrorMsg("The module ROWLOCK is not supported in this database version.");
        } else {
            int maxLockInfoNum = lmMessage.getMaxRetLockInfoLimit();
            maxLockInfoNum = (maxLockInfoNum == 0) ? LockConstants.LOCK_INFO_RESPONSE_SIZE_LIMIT : maxLockInfoNum;
            message.setLockInfoNumLimit(maxLockInfoNum);
            message.getLock(lmMessage);
            if (message.getLockInfoNumLimit() < 0) {
                message.setErrorFlag(true);
                message.setErrorMsg("The number of client wants lock object is overload, the max limit number is " + maxLockInfoNum);
            }
        }
        if (lmConn != null) {
            try {
                lmConn.send(message);
            } catch (Exception e) {
                LOG.error("processSingleNodeLockInfo failed to send message to " + lmConn);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processSingleNodeLockInfo " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        return message;
    }

    private void processLockClean(LMLockInfoReqMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processLockClean " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        if (LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            Set<String> regionServers = listAllRegionServersWithoutLocal();
            if (regionServers != null && regionServers.size() > 0) {
                lmMessage.setMsgType(RSMessage.MSG_TYP_LOCK_CLEAN_SINGLE_NODE);
                for (String regionServer : regionServers) {
                    sendLMMessage(regionServer, lmMessage);
                }
            }
            processSingleNodeLockClean(lmMessage, lmConn);
        }
        if (lmConn == null) {
            return;
        }
        try {
            if (LockConstants.ENABLE_ROW_LEVEL_LOCK) {
                lmConn.send(RSMessage.OK);
            } else {
                lmConn.send(RSMessage.NO);
            }
        } catch (Exception e) {
            LOG.error("processLockClean failed to send OK for lock clean to " + lmConn);
            throw e;
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processLockClean " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
    }

    private void processSingleNodeLockClean(LMLockInfoReqMessage lmMessage, RSConnection lmConn) {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processSingleNodeLockClean " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
        if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            return;
        }
        List<Long> txIDs = lmMessage.getTxIDs();
        List<String> tableNames = lmMessage.getTableNames();
        boolean forceClean = lmMessage.isForceClean();
        if (forceClean) {
            LOG.info("processSingleNodeLockClean txIDs:" + txIDs + " tableNames:" + tableNames + " forceClean:" + forceClean + " " + (lmConn == null ? "local" : lmConn));
        }
        List<Long> nonExistTxIDs = null;
        if (lmMessage.isClient()) {
            if (!forceClean) {
                nonExistTxIDs = getLockCheckTransIDs();
            }
            if (txIDs != null && txIDs.size() > 0) {
                if (!forceClean) {
                    txIDs.retainAll(nonExistTxIDs);
                }
                if (tableNames != null && tableNames.size() > 0) {
                    for (Long txID :  txIDs) {
                        for (String tableName : tableNames) {
                            releaseLocks(txID, tableName, forceClean, nonExistTxIDs);
                        }
                    }
                } else {
                    for (Long txID :  txIDs) {
                        releaseLocks(txID, null, forceClean, nonExistTxIDs);
                    }
                }
            } else if (tableNames != null && tableNames.size() > 0) {
                for (String tableName : tableNames) {
                    releaseLocks(-1, tableName, forceClean, nonExistTxIDs);
                }
            } else {
                releaseLocks(-1, null, forceClean, nonExistTxIDs);
            }
        } else {
            releaseLocksAuto(txIDs);
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processSingleNodeLockClean " + lmMessage + " " + (lmConn == null ? "local" : lmConn));
        }
    }

    private void releaseLocks(long txID, String tableName, boolean forceClean, List<Long> nonExistTxIDs) {
        for (Map.Entry<String, LockManager> entry : lockManagers.entrySet()) {
            if (tableName != null && entry.getKey().indexOf(tableName.toUpperCase()+",") < 0) {
                continue;
            }
            if (txID > 0) {
                if (entry.getValue().getCommitPendingTx().contains(txID)) {
                    return;
                }
                entry.getValue().lockReleaseAll(txID);
            } else {
                if (forceClean) {
                    entry.getValue().forceLockReleaseAll();
                } else {
                    Set<Long> txIDs = entry.getValue().getTransactionMap().keySet();
                    txIDs.retainAll(nonExistTxIDs);
                    txIDs.removeAll(entry.getValue().getCommitPendingTx());
                    for (long tmpTxID : txIDs) {
                        entry.getValue().lockReleaseAll(tmpTxID);
                    }
                }
            }
        }
    }

    private void releaseLocksAuto(final List<Long> txIDs) {
        if (LOG.isDebugEnabled())
            LOG.debug("enter releaseLocksAuto " + txIDs);
        try {
            zkTransactionCleanLock.lock();
            if (ZKUtil.checkExists(zkw1, LockConstants.ZK_NODE_LOCK_PATH) == -1) {
                ZKUtil.createWithParents(zkw1, LockConstants.ZK_NODE_LOCK_PATH);
            }
        } catch (Exception e) {
            LOG.error("failed to releaseLocksAuto " + txIDs, e);
        } finally {
            zkTransactionCleanLock.unlock();
        }
        CopyOnWriteArraySet<Long> commitPendingTxs = null;
        List<Long> tmpTxIDs = null;
        String regionName = null;
        ConcurrentHashMap<Integer, List<Long>> txsByTMIDMap = new ConcurrentHashMap<>();
        List<Long> tmTxIDs = null;
        for (Map.Entry<String, LockManager> entry : lockManagers.entrySet()) {
            commitPendingTxs = entry.getValue().getCommitPendingTx();
            if (commitPendingTxs.size() > 0) {
                tmpTxIDs = new ArrayList<>(txIDs);
                tmpTxIDs.removeAll(commitPendingTxs);
            } else {
                tmpTxIDs = new ArrayList<>(txIDs);
            }
            tmpTxIDs.retainAll(entry.getValue().getTransactionMap().keySet());
            regionName = entry.getValue().getRegionName();
            if (tmpTxIDs.size() == 0) {
                continue;
            }
            LOG.info("tmpTxIDs: " + tmpTxIDs);
            txsByTMIDMap.clear();
            int tmID = -1;
            for (long txID : tmpTxIDs) {
                tmID = LockUtils.getNodeId(txID);
                tmTxIDs = txsByTMIDMap.get(tmID);
                if (tmTxIDs == null) {
                    tmTxIDs = new ArrayList<>();
                    txsByTMIDMap.put(tmID, tmTxIDs);
                }
                tmTxIDs.add(txID);
            }
            for (Map.Entry<Integer, List<Long>> entry1 : txsByTMIDMap.entrySet()) {
                createLockCleanZkNode(regionName, entry.getValue().getRegionEncodedName(), entry1.getKey(), entry1.getValue(), entry.getValue().getRegionInfo());
            }
        }
    }

    private void createLockCleanZkNode(String regionName, String regionEncodedName, Integer key, List<Long> txIDs, byte[] regionInfo) {
        if (regionInfo == null) {
            return;
        }
        if (txIDs.size() == 0) {
            return;
        }
        LOG.info("createLockCleanZkNode key: " + key + " txIDs: " + txIDs +  " regionInfo size: " + regionInfo.length + " regionName: " + regionName + " regionEncodedName: " + regionEncodedName);
        try {
            zkTransactionCleanLock.lock();
            Long[] txIDarr = txIDs.toArray(new Long[0]);
            String zNodePathTM = LockConstants.ZK_NODE_LOCK_PATH + "/TM" + key;
            if (ZKUtil.checkExists(zkw1, zNodePathTM) == -1) {
                ZKUtil.createWithParents(zkw1, zNodePathTM);
            }
            String zNodePathTMRegion = zNodePathTM + "/" + regionEncodedName;
            int regionInfoLen = regionInfo.length;
            String txIDStr = Arrays.toString(txIDarr);
            txIDStr = txIDStr.substring(1, txIDStr.length() - 1); 
            byte[] txIDBytes = txIDStr.getBytes();
            int dataLen = regionInfoLen + txIDBytes.length + 4;
            byte[] data = new byte[dataLen];
            data[0] = (byte)((regionInfoLen >> 24) & 0xFF);
            data[1] = (byte)((regionInfoLen >> 16) & 0xFF);
            data[2] = (byte)((regionInfoLen >> 8) & 0xFF);
            data[3] = (byte)((regionInfoLen) & 0xFF);
            System.arraycopy(regionInfo, 0, data, 4, regionInfoLen);
            System.arraycopy(txIDBytes, 0, data, regionInfoLen + 4, txIDBytes.length);
            LOG.info("createSetData zNodePathTMRegion: " + zNodePathTMRegion + " data: " + Arrays.toString(data));
            ZKUtil.createSetData(zkw1, zNodePathTMRegion, data);
        } catch (Exception e) {
            LOG.error("failed to createLockCleanZkNode key: " + key + " txIDs: " + txIDs + " regionName: " + regionName + " regionEncodedName: " + regionEncodedName, e);
        } finally {
            zkTransactionCleanLock.unlock();
        }
    }

    public void sendLMMessage(String destServer, RSMessage message) {
        RSConnection rsConnection = null;
        int currentRetryTimes = 0;
        boolean needRetry = true;
        while (needRetry) {
            try {
                currentRetryTimes++;
                rsConnection = connectionCache.borrowConnection(destServer);
                if (rsConnection != null) {
                    rsConnection.send(message);
                    needRetry = false;
                    // cache the connection again, after the work has been done
                    connectionCache.releseConnection(destServer, rsConnection);
                }
            } catch (Exception e) {
                LOG.error("failed to send message to " + destServer + " " + message, e);
                connectionCache.removeConnection(destServer, rsConnection);
            }
            if (needRetry && currentRetryTimes >= messageRetryTimes) {
                LOG.error("failed to send message to " + destServer + " " + message + " retry times" + currentRetryTimes);
                needRetry = false;
            }
        }
    }

    public RSMessage sendAndReceiveLMMessage(String destServer, RSMessage message) {
        RSMessage retMsg = null;
        RSConnection rsConnection = null;
        int currentRetryTimes = 0;
        boolean needRetry = true;
        while (needRetry) {
            try {
                currentRetryTimes++;
                rsConnection = connectionCache.borrowConnection(destServer);
                if (rsConnection != null) {
                    rsConnection.send(message);
                    retMsg = rsConnection.receive();
                    needRetry = false;
                    connectionCache.releseConnection(destServer, rsConnection);
                }
            } catch (Exception e) {
                connectionCache.removeConnection(destServer, rsConnection);
                LOG.error("failed to send and receive message " + destServer + " " + message, e);
            }
            if (currentRetryTimes >= messageRetryTimes) {
                LOG.error("failed to send and receive message " + destServer + " " + message + " retry times" + currentRetryTimes);
                needRetry = false;
            }
        }

        return retMsg;
    }

    public CopyOnWriteArraySet<String> listAllRegionServerIPs() {
        if (allRegionServers.size() == 0) {
            listAllRegionServers();
        }
        regionServerLock.lock();
        if (allRegionServerIPs.size() > 0) {
            regionServerLock.unlock();
            return allRegionServerIPs;
        }
        for (String regionServer : allRegionServers) {
            try {
                InetAddress inetAddress = InetAddress.getByName(regionServer);
                allRegionServerIPs.add(inetAddress.getHostAddress());
            } catch (Exception e) {
                LOG.error("failed to get ip by hostName: " + regionServer, e);
            }
        }
        regionServerLock.unlock();
        return allRegionServerIPs;
    }

    public CopyOnWriteArraySet<String> listAllRegionServersWithoutLocal() {
        CopyOnWriteArraySet<String> allRSs = listAllRegionServers();
        if (allRSs == null) {
            return allRSs;
        }
        if (serverName == null || "".equals(serverName)) {
            serverName = IPUtils.getHostName();
        }
        if (serverName != null) {
            allRSs = new CopyOnWriteArraySet(allRSs);
            allRSs.remove(serverName);
        }
        return allRSs;
    }

    public CopyOnWriteArraySet<String> listAllRegionServers() {
        return listAllRegionServers(false);
    }

    public CopyOnWriteArraySet<String> listAllRegionServers(boolean refresh) {
        if (zkw1 != null) {
            regionServerLock.lock();
            if (refresh) {
                allRegionServers.clear();
                allRegionServerIPs.clear();
            }
            if (allRegionServers.size() > 0) {
                regionServerLock.unlock();
                return allRegionServers;
            }
            try {
                String znodeParent = zkw1.getConfiguration().get("zookeeper.znode.parent");
                List<String> rsNames = ZKUtil.listChildrenNoWatch(zkw1, znodeParent + "/rs");
                LOG.info("listAllRegionServers: refresh: " + refresh + " znodeParent: " + znodeParent + " reNames: [" + rsNames + "]");

                String[] tmpRsnames = null;
                for (String rsName : rsNames) {
                    tmpRsnames = rsName.split(",");
                    if (tmpRsnames == null || tmpRsnames.length != 3) {
                        continue;
                    }
                    allRegionServers.add(tmpRsnames[0]);
                }
                LOG.info("listAllRegionServers: refresh: " + refresh + " allRegionServers: [" + allRegionServers + "]");
            } catch (Exception e) {
                LOG.error("failed to get all region servers", e);
            }
            regionServerLock.unlock();
        }
        return allRegionServers;
    }

    public Map<Long,TransactionMsg> getLockInfo(List<Long> txIDs, List<String> tableNames, AtomicInteger lockInfoNumLimit, int debug) {
        Map<Long,TransactionMsg> regionServerLocks = new ConcurrentHashMap<>();
        for (LockManager lockManager : lockManagers.values()) {
            getLockInfo(lockManager, regionServerLocks, txIDs, tableNames, lockInfoNumLimit, debug);
            if (lockInfoNumLimit.intValue() < 0) {
                break;
            }
        }
        if (debug == 2) {
            getSessionInfo();
        } else if (debug == 3) {
            getLockStatistics(null, null, 0, debug, txIDs, tableNames);
        }

        return regionServerLocks;
    }

    public void getLockStatistics(ConcurrentHashMap<Long, ConcurrentHashMap<String, Integer>> txLockNumMapMap, ConcurrentHashMap<Long, LMLockStaticisticsData> txLockNumMap, int topN, int debug,
            List<Long> txIDs, List<String> tableNames) {
        ConcurrentHashMap<Long, ConcurrentHashMap<String, Integer>> currentTxLockNumMapMap = null;
        ConcurrentHashMap<Long, LMLockStaticisticsData> currentTxLockNumMap = null;
        if (topN > 0) {
            currentTxLockNumMapMap = new ConcurrentHashMap<>();
            currentTxLockNumMap = new ConcurrentHashMap<>();
        } else {
            currentTxLockNumMapMap = txLockNumMapMap;
            currentTxLockNumMap = txLockNumMap;
        }
        ConcurrentHashMap<String, Integer> regionLockNumMap = null;
        LMLockStaticisticsData lmlockStaticisticsData = null;
        String regionName = null;
        long txID = -1;
        int regionTxLockNum = -1;
        for (Entry<String, LockManager> entry : lockManagers.entrySet()) {
            regionName = entry.getKey();
            //tables are not on current Region
            if (tableNames != null && !tableNames.isEmpty()) {
                if (!containCurrentRegion(regionName, tableNames)) {
                    //next
                    continue;
                }
            }
            Set<Long> selectedTxIDSet = null;
            //trans are not on current Region
            if(!txIDs.isEmpty()){
                Set<Long> txOnCurrentServerSet = entry.getValue().getTransactionMap().keySet();
                selectedTxIDSet = new HashSet<Long>(txIDs);
                selectedTxIDSet.retainAll(txOnCurrentServerSet);
                if(selectedTxIDSet.isEmpty()){
                    //next
                    continue;
                }
            }
            for (Entry<Long, Transaction> entry1 : entry.getValue().getTransactionMap().entrySet()) {
                txID = entry1.getKey();
                if(selectedTxIDSet != null){
                    // selectedTxIDSet is definitely not empty!
                    if(!selectedTxIDSet.contains(txID)){
                        continue;
                    }
                }
                regionLockNumMap = currentTxLockNumMapMap.get(txID);
                lmlockStaticisticsData = currentTxLockNumMap.get(txID);
                if (lmlockStaticisticsData == null) {
                    lmlockStaticisticsData = new LMLockStaticisticsData(0, entry1.getValue().getQueryContext());
                    currentTxLockNumMap.put(txID, lmlockStaticisticsData);
                }
                if (regionLockNumMap == null) {
                    regionLockNumMap = new ConcurrentHashMap<>();
                    currentTxLockNumMapMap.put(txID, regionLockNumMap);
                }
                regionTxLockNum = entry1.getValue().getLockNumber();
                regionLockNumMap.put(regionName, regionTxLockNum);
                lmlockStaticisticsData.setLockNum(lmlockStaticisticsData.getLockNum() + regionTxLockNum);
            }
        }
        if (debug == 3) {
            for (Entry<Long, LMLockStaticisticsData> entry : currentTxLockNumMap.entrySet()) {
                if (entry.getValue().getLockNum() >= LockConstants.RECORD_TRANSACTION_LOCKNUM_THRESHOLD) {
                    LOG.info("txID: " + entry.getKey() + "\n    " + currentTxLockNumMapMap.get(entry.getKey()));
                }
            }
        }
        if (topN > 0) {
            LinkedHashMap<Long, LMLockStaticisticsData> sortedTxLockNumMap = LockUtils.sortMapByValue(currentTxLockNumMap, topN);
            for (Map.Entry<Long, LMLockStaticisticsData> entry : sortedTxLockNumMap.entrySet()) {
                txLockNumMap.put(entry.getKey(), entry.getValue());
                txLockNumMapMap.put(entry.getKey(), currentTxLockNumMapMap.get(entry.getKey()));
            }
        }
    }

    private void getSessionInfo() {
        StringBuffer message = new StringBuffer(1000);
        message.append("\nidelSession: ").append(idleSessions.size()).append("\n");
        int i = 1;
        for (RSSession session : idleSessions) {
            message.append("  session:").append(i).append("\n    ").append(session).append("\n");
            i++;
        }
        message.append("\nworkingSessions: ").append(workingSessions.size()).append("\n");
        i = 1;
        for (RSSession session : workingSessions) {
            message.append("  session:").append(i).append("    \n").append(session).append("\n");
            i++;
        }
        message.append("\n").append(connectionCache.toString());
        LOG.info(message);
    }

    public Map<Long, List<LMLockWaitMsg>> getLockWaitInfo(List<Long> txIDs, List<Long> holderTxIDs, List<String> tableNames) {
        Map<Long, List<LMLockWaitMsg>> lockWaitInfo = new ConcurrentHashMap<>();
        for (LockManager lockManager : lockManagers.values()) {
            getLockWaitInfo(lockManager, lockWaitInfo, txIDs, holderTxIDs, tableNames);
        }
        return lockWaitInfo;
    }

    public List<Long> getLockCheckTransIDs() {
        if (LockConstants.REST_SERVER_URI == null || LockConstants.REST_SERVER_URI.equals("")) {
            return null;
        }
        List<Long> transIDs = new ArrayList<Long>();
        Set<Long> regionTxIDs = new HashSet<>();
        // deal single region transaction
        synchronized (this.regionTxIDs) {
            regionTxIDs.addAll(this.regionTxIDs);
        }
        Set<Long> currentTxIDs = rsServer.getCurrentAllTxIDs();
        // deal single region transaction
        synchronized (this.regionTxIDs) {
            regionTxIDs.addAll(this.regionTxIDs);
        }
        if (currentTxIDs == null || currentTxIDs.size() == 0) {
            return transIDs;
        }
        List<Long> txIDFromTM = rsServer.getAllTxIDsFromTM();
        try {
            if (LockLogLevel.enableTraceLevel) {
                LOG.info("currentTxIDs:" + currentTxIDs);
                LOG.info("txIDFromTM:" + txIDFromTM);
                LOG.info("regionTxIDs:" + regionTxIDs);
            }
            if (txIDFromTM == null) {
                return transIDs;
            }
            currentTxIDs.removeAll(txIDFromTM);
            currentTxIDs.removeAll(regionTxIDs);

            // have to remove txIDs that have been commited or rollbacked
            Set<Long> currentTxIDs1 = getCurrentAllTxIDs();
            if (LockLogLevel.enableTraceLevel) {
                LOG.info("currentTxIDs1:" + currentTxIDs1);
            }
            currentTxIDs.retainAll(currentTxIDs1);

            // dtmci transid
            for (Long txID: currentTxIDs) {
                if (transExistsInAll(txID) == false) {
                    transIDs.add(txID);
                }
            }
            // have to remove txIDs that have been commited or rollbacked
            Set<Long> currentTxIDs2 = getCurrentAllTxIDs();
            if (LockLogLevel.enableTraceLevel) {
                LOG.info("currentTxIDs2:" + currentTxIDs2);
            }
            transIDs.retainAll(currentTxIDs2);
        } catch (Exception e) {
            LOG.error("failed to getLockCheckTransIDs", e);
        }

        return transIDs;
    }

    public Map<Long, LMLockWaitMsg> getDeadLockInfo(Map<Long, List<LMLockWaitMsg>> lockWaitInfo) {
        Map<Long, LMLockWaitMsg> deadLockInfo = new ConcurrentHashMap<>();
        Map<Long, LMLockWaitMsg> tmpLockInfo = null;
        for (Map.Entry<Long, List<LMLockWaitMsg>> entry : lockWaitInfo.entrySet()) {
            if (deadLockInfo.get(entry.getKey()) != null) {
                continue;
            }
            for (LMLockWaitMsg lmLockWaitMsg : entry.getValue()) {
                tmpLockInfo = new ConcurrentHashMap<>();
                tmpLockInfo.put(entry.getKey(), lmLockWaitMsg);
                if (deadLockCheck(entry.getKey(), lmLockWaitMsg.getHolderTxIDs(), tmpLockInfo, lockWaitInfo) != -1) {
                    deadLockInfo.putAll(tmpLockInfo);
                }
            }
        }
        return deadLockInfo;
    }

    private long deadLockCheck(long originTxID, Set<Long> detectTxIDs, Map<Long, LMLockWaitMsg> deadLockInfo, Map<Long, List<LMLockWaitMsg>> lockWaitInfo) {
        for (Long detectTxID : detectTxIDs) {
            List<LMLockWaitMsg> lockWaits = lockWaitInfo.get(detectTxID);
            if (lockWaits == null || lockWaits.size() == 0) {
                continue;
            }
            for (LMLockWaitMsg lockWait : lockWaits) {
                if (lockWait == null) {
                    continue;
                }
                deadLockInfo.put(detectTxID, lockWait);
                if (lockWait.getHolderTxIDs() != null && lockWait.getHolderTxIDs().contains(originTxID)) {
                    return originTxID;
                }
                long retVal = deadLockCheck(originTxID, lockWait.getHolderTxIDs(), deadLockInfo, lockWaitInfo);
                if (retVal == originTxID) {
                    return retVal;
                }
            }
        }
        return -1;
    }

    private void getLockInfo(LockManager lockManager, Map<Long, TransactionMsg> regionServerLocks, List<Long> txIDs, List<String> tableNames, AtomicInteger lockInfoNumLimit, int debug) {
        String regionName = lockManager.getRegionName();
        boolean contain = containCurrentRegion(regionName, tableNames);
        if (!contain) {
            return;
        }
        if (debug == 1) {
            lockManager.printAllLocks();
        }

        ConcurrentHashMap<Long, Transaction> transactionMap = lockManager.getTransactionMap();
        if (transactionMap.size() == 0) {
            return;
        }
        TransactionMsg transactionMsg = null;
        if (txIDs == null || txIDs.size() == 0) {
            for (Map.Entry<Long, Transaction> entry : transactionMap.entrySet()) {
                transactionMsg = regionServerLocks.get(entry.getKey());
                if (transactionMsg == null) {
                    transactionMsg = new TransactionMsg();
                    transactionMsg.setQueryContext(entry.getValue().getQueryContext());
                    regionServerLocks.put(entry.getKey(), transactionMsg);
                }
                getTransactionMsg(regionName, transactionMsg, entry.getValue(), lockInfoNumLimit);
                if (lockInfoNumLimit.intValue() < 0) {
                    break;
                }
            }
        } else {
            ArrayList<Long> tmpTxIDList = new ArrayList<>(txIDs);
            tmpTxIDList.retainAll(transactionMap.keySet());

            for (Long txID : tmpTxIDList) {
                transactionMsg = regionServerLocks.get(txID);
                if (transactionMsg == null) {
                    transactionMsg = new TransactionMsg();
                    Transaction transaction = transactionMap.get(txID);
                    if (transaction != null) {
                        transactionMsg.setQueryContext(transaction.getQueryContext());   
                    }
                    regionServerLocks.put(txID, transactionMsg);
                }
                getTransactionMsg(regionName, transactionMsg, transactionMap.get(txID), lockInfoNumLimit);
                if (lockInfoNumLimit.intValue() < 0) {
                    break;
                }
            }
        }
    }

    private void getTransactionMsg(String regionName, TransactionMsg transactionMsg, Transaction transaction, AtomicInteger lockInfoNumLimit) {
        LockHolder rowHolder = transaction.getHead();
        Map<String, LMLockMsg> rowLockMap = transactionMsg.getRowLocks().get(regionName);
        if (transaction.getTableHolder() != null) {
            LMLockMsg tmp = getLockMsg(regionName, transaction.getTableHolder(), lockInfoNumLimit);
            if (tmp != null) {
                transactionMsg.getTableLocks().put(regionName, tmp);
            }
        }
        if (lockInfoNumLimit.intValue() < 0) {
            return;
        }
        if (rowHolder != null) {
            if (rowLockMap == null) {
                rowLockMap = new ConcurrentHashMap<>();
                transactionMsg.getRowLocks().put(regionName, rowLockMap);
            }
            while (rowHolder != null && rowHolder.getLock() != null) {
                LMLockMsg tmp = getLockMsg(regionName, rowHolder, lockInfoNumLimit);
                if (tmp != null) {
                    rowLockMap.put(tmp.getRowID(), tmp);
                }
                if (lockInfoNumLimit.intValue() < 0) {
                    return;
                }
                rowHolder = rowHolder.getTransNext();
            }
        }
    }

    private LMLockMsg getLockMsg(String regionName, LockHolder lockHolder, AtomicInteger lockInfoNumLimit) {
        Lock lock = lockHolder.getLock();
        if(lock == null) {
            return null;
        }

        // < not <=
        if (lockInfoNumLimit.decrementAndGet() < 0) {
            return null;
        }

        LMLockMsg lmLockMsg = new LMLockMsg();
        int idx = regionName.indexOf(",");
        lmLockMsg.setTableName(regionName.substring(0, idx));
        idx = regionName.lastIndexOf(",");
        lmLockMsg.setRegionName(regionName.substring(idx + 1));
        if (lock.getRowKey() != null) {
            lmLockMsg.setRowID(lock.getRowKey().toString());
        }
        for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            lmLockMsg.getHolding()[i] += lockHolder.getHolding()[i];
        }
        lmLockMsg.setMaskHold(lockHolder.getMaskHold());

        if (lockHolder.getChild() != null) {
            Stack<LockHolder> stack = new Stack<>();
            stack.push(lockHolder.getChild());
            LockHolder tmpHolder = null;
            long parentSvptID = -1;
            while (stack.size() > 0) {
                tmpHolder = stack.pop();
                if (tmpHolder.isImplicitSavepoint()) {
                    if (tmpHolder.getParentSvptID() > 0) {
                        lmLockMsg.getSubImplicitSvptHoldings().put(tmpHolder.getSvptID(), tmpHolder.getHolding().clone());
                    } else {
                        lmLockMsg.getImplicitSvptHoldings().put(tmpHolder.getSvptID(), tmpHolder.getHolding().clone());
                    }
                } else {
                    lmLockMsg.getSvptHoldings().put(tmpHolder.getSvptID(), tmpHolder.getHolding().clone());
                }
                parentSvptID = tmpHolder.getParentSvptID();
                if (parentSvptID > 0) {
                    lmLockMsg.getSvptRelation().put(tmpHolder.getSvptID(), parentSvptID);
                }
                lmLockMsg.setMaskHold(lmLockMsg.getMaskHold() | tmpHolder.getMaskHold());
                if (tmpHolder.getNextNeighbor() != null) {
                    stack.push(tmpHolder.getNextNeighbor());
                }
                if (tmpHolder.getChild() != null) {
                    stack.push(tmpHolder.getChild());
                }
            }
        }

        return lmLockMsg;
    }

    private LMLockMsg getLockMsg(String regionName, LockWait lockWait) {
        Lock lock = lockWait.getLock();
        if (lock == null) {
            return null;
        }
        LMLockMsg lmLockMsg = new LMLockMsg();
        int idx = regionName.indexOf(",");
        lmLockMsg.setTableName(regionName.substring(0, idx));
        idx = regionName.lastIndexOf(",");
        lmLockMsg.setRegionName(regionName.substring(idx + 1));
        
        if (lock.getRowKey() != null) {
            lmLockMsg.setRowID(lock.getRowKey().toString());
        }
        for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            lmLockMsg.getHolding()[i] += lockWait.getHolding()[i];
        }
        lmLockMsg.setMaskHold(lockWait.getMaskHold());
        lmLockMsg.setDurableTime(System.currentTimeMillis() - lockWait.getCreateTimestamp());

        return lmLockMsg;
    }

    private void getLockWaitInfo(LockManager lockManager, Map<Long, List<LMLockWaitMsg>> lockWaitInfo, List<Long> txIDs, List<Long> holderTxIDs, List<String> tableNames) {
        String regionName = lockManager.getRegionName();
        boolean contain = containCurrentRegion(regionName, tableNames);
        if (!contain) {
            return;
        }
        ConcurrentHashMap<Long, LockWait> waitGraph = lockManager.getWaitGraph();
        synchronized (waitGraph) {
            if (waitGraph.size() == 0) {
                return;
            }
            LockWait lockWait = null;
            Set<Long> conflictTxIDs = null;
            LMLockWaitMsg lmLockWaitMsg = null;
            List<LMLockWaitMsg> lmLockWaitMsgs = null;
            LMLockMsg toLockMsg = null;
            for (Map.Entry<Long, LockWait> entry : waitGraph.entrySet()) {
                lmLockWaitMsgs = lockWaitInfo.get(entry.getKey());
                if (lmLockWaitMsgs == null) {
                    lmLockWaitMsgs = new ArrayList<>();
                    lockWaitInfo.put(entry.getKey(), lmLockWaitMsgs);
                }
                lockWait = entry.getValue();
                synchronized (lockWait) {
                    /*if (lockWait.getLock().getRowKey() != null){
                        conflictTxIDs = getRowConflictTransaction(entry.getKey(), lockWait);
                    } else {
                        conflictTxIDs = getTableConflictTransaction(entry.getKey(), lockWait, lockManager);
                    }*/
                    if (txIDs != null && txIDs.size() > 0 && !txIDs.contains(entry.getKey())) {
                        continue;
                    }
                    /*if (holderTxIDs != null && holderTxIDs.size() > 0 && conflictTxIDs.size() > 0) {
                        List<Long> tmpList = new ArrayList<>(holderTxIDs);
                        tmpList.retainAll(conflictTxIDs);
                        if (tmpList.size() == 0) {
                    continue;
                        }
                    }*/
                    if (lockWait.getLock() == null || lockWait.getHolderTxIDs().size() == 0) {
                        continue;
                    }
                    lmLockWaitMsg = new LMLockWaitMsg();
                    lmLockWaitMsg.setTxID(entry.getKey());
                    toLockMsg = getLockMsg(regionName, lockWait);
                    lmLockWaitMsg.setToLock(toLockMsg);
                    lmLockWaitMsg.setHolderTxIDs(new CopyOnWriteArraySet<Long>(lockWait.getHolderTxIDs()));
                    lmLockWaitMsg.setSvptID(lockWait.getSvptID());
                    lmLockWaitMsg.setParentSvptID(lockWait.getParentSvptID());
                    lmLockWaitMsg.setImplicitSavepoint(lockWait.isImplicitSavepoint());
                    Transaction ts = lockWait.getTransaction();
                    lmLockWaitMsg.setQueryContext(ts == null ? "" : ts.getQueryContext());
                    lmLockWaitMsgs.add(lmLockWaitMsg);
                }
            }
        }
    }

    private boolean containCurrentRegion(String regionName, List<String> tableNames) {
        boolean contain = false;
        if (tableNames != null && tableNames.size() > 0) {
            for (String tableName : tableNames) {
                if (regionName.indexOf(tableName.toUpperCase()) > -1) {
                    contain = true;
                    break;
                }
            }
        } else {
            contain = true;
        }
        return contain;
    }

    /*
     * return null : error, such as connection error or
     * REST_SERVER_URI format error
     *
     * return not null : normal
     */
    public List<Long> getAllTxIDsFromTM() {
        if (LockConstants.REST_SERVER_URI == null || LockConstants.REST_SERVER_URI.equals("")) {
            return null;
        }
        // REST_SERVER union
        StringBuffer restUrl = new StringBuffer();
        List<Long> txIDs = null;
        Set<Long> txIDsSet = null;
        List<String> errorUriList = new ArrayList<>();
        int i = 0;
        for (List<String> restUriList : restUriListList) {
            if (xdcListPair.size() != 0) {
                try {
                    regionServerLock.lock();
                    AbstractMap.SimpleEntry<Integer, Boolean> pairItem = xdcListPair.get(i);
                    regionServerLock.unlock();
                    i++;
                    if (pairItem.getValue() == false) {
                        continue;
                    }
                } catch (IndexOutOfBoundsException e) {
                    LOG.error("XDC_MASTER_SLAVE configure error!" + LockConstants.XDC_MASTER_SLAVE);
                    return null;
                }
            }
            // deal a cluster is error
            boolean allErrorFlag = true;
            errorUriList.clear();
            for (String restUri : restUriList) {
                restUrl.delete(0, restUrl.length());
                restUrl.append(restUri).append(LockConstants.REST_TRANSACTION_PATH);
                txIDs = getAllTxIDsFromTM(restUrl.toString());
                if (txIDs != null) {
                    // deal a cluster is error
                    allErrorFlag = false;
                    if (txIDsSet == null)
                        txIDsSet = new HashSet<>();
                    Set<Long> tmpIDsSet = new HashSet<>(txIDs);
                    txIDsSet.addAll(tmpIDsSet);
                    break;
                } else {
                    errorUriList.add(restUri);
                }
            }
            // deal a cluster is error
            if (allErrorFlag) {
                return null;
            }
            if (errorUriList.size() > 0) {
                restUriList.removeAll(errorUriList);
                restUriList.addAll(errorUriList);
            }
        }
        if (txIDsSet == null) {
            return null;
        } else {
            return new ArrayList<Long>(txIDsSet);
        }
    }

    public boolean transExistsInAll(Long txID) {
        if (restUriListList.size() == 0) {
            return true;
        }
        StringBuffer restUrl = new StringBuffer();
        for (List<String> restUriList : restUriListList) {
            for (String restUri : restUriList) {
                restUrl.delete(0, restUrl.length());
                restUrl.append(restUri).append(LockConstants.REST_TRANSACTION_STATUS).append("?transid=").append(Long.toString(txID));
                if (transExists(restUrl.toString()) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean transExists(String restUrl) {
        HttpURLConnection conn = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("transExists: \"" + restUrl + "\"");
        }

        try {
            conn = openHttpConnection(restUrl, true);

            if (conn.getResponseCode() != 200 && conn.getResponseCode() != 201) {
                String message = getErrorMessage(conn);
                LOG.error("failed to transExists from REST" + restUrl, new Exception(message));
                return true;
            }

            String output = getMessage(conn);
            if (!output.equals("")) {
                if (output.equals("-1")) {
                    return true;
                }
                if (output.equals("0")) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            LOG.error("failed to get all transactions from tm", e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        return true;
    }

    private String getMessage(HttpURLConnection conn) throws IOException {
        StringBuffer outputBuffer = new StringBuffer();
        InputStream is = conn.getInputStream();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset()));

            String output = null;
            while ((output = br.readLine()) != null) {
                outputBuffer.append(output);
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {}
            }
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e) {}
            }
        }
        return outputBuffer.toString().trim();
    }

    private String getErrorMessage(HttpURLConnection conn) throws IOException {
        String message = conn.getResponseMessage();
        StringBuffer outputBuffer = new StringBuffer();
        InputStream es = conn.getErrorStream();
        if (es != null) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new InputStreamReader(es, Charset.defaultCharset()));

                String output = "";
                while ((output = br.readLine()) != null) {
                    outputBuffer.append(output);
                    outputBuffer.append(System.getProperty("line.separator"));
                }
                message = outputBuffer.toString();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (Exception e){}
                }
                if (es != null) {
                    try {
                        es.close();
                    } catch (Exception e){}
                }
            }
        }

        return message;
    }

    private List<Long> getAllTxIDsFromTM(String restUrl) {
        HttpURLConnection conn = null;
        List<Long> allTxIDs = new ArrayList<>();
        long start = System.currentTimeMillis();
        try {
            conn = openHttpConnection(restUrl, true);

            StringBuffer outputBuffer = new StringBuffer();
            if (conn.getResponseCode() != 200 && conn.getResponseCode() != 201) {
                String message = getErrorMessage(conn);

                LOG.error("failed to getTxIDS from REST", new Exception(message));
                return null;
            }

            String output = getMessage(conn);
            if (!output.equals("")) {
                if (output.equals("-1")) {
                    return null;
                }

                String[] txIDs = output.split(",");
                for (String txID : txIDs) {
                    if (txID.trim().equals("")) {
                        continue;
                    }
                    allTxIDs.add(Long.valueOf(txID));
                }
            }
            return allTxIDs;
        } catch (Exception e) {
            LOG.error("failed to get all transactions from tm " + restUrl, e);
        } finally {
            // LOG.warn("get transactions from tm " + restUrl + " elaps: " + (System.currentTimeMillis() - start));
            if (conn != null) {
                conn.disconnect();
            }
        }

        return null;
    }

    public void addRegionTx(Long txID) {
        synchronized (regionTxIDs) {
            regionTxIDs.add(txID);
        }
    }

    public void removeRegionTx(Long txID) {
        synchronized (regionTxIDs) {
            regionTxIDs.remove(txID);
        }
    }

    public Set<Long> getCurrentAllTxIDs() {
        Set<Long> txIDs = new HashSet<>();
        for (LockManager lockManager : lockManagers.values()) {
            txIDs.addAll(lockManager.getTransactionMap().keySet());
        }
        return txIDs;
    }

    public boolean checkAndCleanTransaction(boolean isCluster) {
        Set<Long> regionTxIDs = new HashSet<>();
        // deal single region transaction
        synchronized (this.regionTxIDs) {
            regionTxIDs.addAll(this.regionTxIDs);
        }
        Set<Long> currentTxIDs = rsServer.getCurrentAllTxIDs();
        // deal single region transaction
        synchronized (this.regionTxIDs) {
            regionTxIDs.addAll(this.regionTxIDs);
        }
        if (currentTxIDs == null && currentTxIDs.size() == 0) {
            return true;
        }
        List<Long> txIDFromTM = rsServer.getAllTxIDsFromTM();

        try {
            if (LockLogLevel.enableTraceLevel) {
                LOG.info("currentTxIDs:" + currentTxIDs);
                LOG.info("txIDFromTM:" + txIDFromTM);
                LOG.info("regionTxIDs:" + regionTxIDs);
            }
            if (txIDFromTM != null) { 
                currentTxIDs.removeAll(txIDFromTM);
            } else if (isCluster) {
                LOG.warn("txIDFromTM is null, isCluster: " + isCluster);
                if (hasActiveRest()) {
                    return true;
                }
                return false;
            }
            currentTxIDs.removeAll(regionTxIDs);

            // have to remove txIDs that have been commited or rollbacked
            Set<Long> currentTxIDs1 = getCurrentAllTxIDs();
            if (LockLogLevel.enableTraceLevel) {
                LOG.info("currentTxIDs1:" + currentTxIDs1);
            }
            currentTxIDs.retainAll(currentTxIDs1);

            if (currentTxIDs.size() == 0) {
                return true;
            }
            // dtmci transid
            List<Long> checkTxIDs = new ArrayList<Long>();
            for (Long txID: currentTxIDs) {
                if (transExistsInAll(txID) == false) {
                    checkTxIDs.add(txID);
                }
            }
            if (checkTxIDs.size() > 0) {
                List<Long> commitPendingTxs = getAllCommitPendingTxs();
                if (LOG.isDebugEnabled())
                    LOG.debug("unLockAll with CommitPendingTx: " + checkTxIDs + "\ncommit pending tx: " + commitPendingTxs);
                checkTxIDs.removeAll(commitPendingTxs);
                if (checkTxIDs.size() > 0) {
                    LOG.warn("unLockAll: " + checkTxIDs);
                    unLockAll(checkTxIDs);
                    if (LOG.isDebugEnabled())
                        LOG.debug("unLockAll end: " + checkTxIDs);
                }
            }
            if (txIDFromTM == null && !isCluster) {
                LOG.warn("txIDFromTM is null, isCluster: " + isCluster);
                return false;
            }
        } catch (Exception e) {
            LOG.error("failed to checkAndCleanTransaction", e);
        }
        return true;
    }

    public void unLockAll(List<Long> txIDs) {
        if (txIDs == null || txIDs.size() == 0) {
            return;
        }
        try {
            LMLockInfoReqMessage cleanLockReqMsg = new LMLockInfoReqMessage(RSMessage.MSG_TYP_LOCK_CLEAN);
            cleanLockReqMsg.setTxIDs(txIDs);
            cleanLockReqMsg.setClient(false);
            processRSMessage(cleanLockReqMsg, null);
        } catch (Exception e) {
            LOG.error("failed to unLock UnExists transactions:" + txIDs, e);
        }
    }

    public List<Long> getAllCommitPendingTxs() {
        List<Long> commitPendingTxs = new ArrayList<>();
        try {
        for (LockManager lockManager : lockManagers.values()) {
            commitPendingTxs.addAll(lockManager.getCommitPendingTx());
        }
        } catch (Exception e) {
            LOG.error("failed to getAllCommitPendingTxs", e);
        }
        return commitPendingTxs;
    }

    private TrustManager[] get_trust_mgr() {
        TrustManager[] certs = new TrustManager[] { new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
            }
        } };
        return certs;
    }

    public long localDeadLockDetect(String regionName, Long txID) {
        return deadLockDetect.localDeadLockDetect(regionName, txID);
    }

    public boolean distributedDeadLockDetect(String regionName, long txID) {
        if (listAllRegionServers().size() <= 1) {
            return true;
        }
        LockWait lockWait = rsServer.getLockManagers().get(regionName).getLockWait(txID);
        if (lockWait == null) {
            return true;
        }
        try {
            return deadLockDetect.distributedDeadLockDetect(lockWait, serverName, lockWait.getTxID(), new ArrayList<Long>());
        } catch (Exception e) {
            LOG.error("failed to start distributed dead lock detect", e);
        }
        return true;
    }

    public boolean isTMStarted() {
        //change true to false
        //no need to wait all of rest servers are active
        return checkRestStatus(false);
    }

    private boolean checkRestStatus(boolean allActive) {
        int i = 0;
        boolean allXdcRestStatus = false;
        for (List<String> restUriList : restUriListList) {
            if (xdcListPair.size() != 0) {
                try {
                    regionServerLock.lock();
                    AbstractMap.SimpleEntry<Integer, Boolean> pairItem = xdcListPair.get(i);
                    regionServerLock.unlock();
                    i++;
                    if (pairItem.getValue() == false) {
                        continue;
                    }
                } catch (IndexOutOfBoundsException e) {
                    LOG.error("XDC_MASTER_SLAVE configure error!" + LockConstants.XDC_MASTER_SLAVE);
                    return false;
                }
            }
            allXdcRestStatus = checkRestStatusOneCluster(restUriList, allActive);
            if (!allXdcRestStatus) {
                break;
            }
        }
        return allXdcRestStatus;
    }

    private boolean checkRestStatusOneCluster(List<String> restUriList, boolean allActive) {
        // deal a cluster
        boolean XdcRestStatus = false;
        StringBuffer restUrl = new StringBuffer();
        for (String restUri : restUriList) {
            restUrl.delete(0, restUrl.length());
            restUrl.append(restUri).append(LockConstants.REST_TRANSACTION_PATH);
            if (testConnect(restUrl.toString())) {
                if (!allActive) {
                    // Find one alive server
                    XdcRestStatus = true;
                    break;
                }
            } else if (allActive) {
                if (LockLogLevel.enableTraceLevel) {
                    LOG.warn("REST SERVER or TM ERROR:" + restUrl.toString());
                }
                return false;
            }
        }
        return allActive ? true : XdcRestStatus;
    }

    private boolean hasActiveRest() {
        return checkRestStatus(false);
    }

    private boolean testConnect(String restUrl) {
        HttpURLConnection conn = null;
        try {
            conn = openHttpConnection(restUrl, false);
            conn.connect();
        } catch (Exception e) {
            LOG.error("failed to connect " + restUrl, e);
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        return true;
    }

    private HttpURLConnection openHttpConnection(String restUrl, boolean setProperty) throws Exception {
        SSLContext ssl_ctx = SSLContext.getInstance("TLS");
        TrustManager[] trust_mgr = get_trust_mgr();
        ssl_ctx.init(null, // key manager
                trust_mgr, // trust manager
                new SecureRandom()); // random number generator
        HttpsURLConnection.setDefaultSSLSocketFactory(ssl_ctx.getSocketFactory());

        URL url = new URL(restUrl.toString());
        HttpURLConnection conn = null;
        if (restUrl.indexOf("https") >= 0) {
            conn = (HttpsURLConnection) url.openConnection();
            ((HttpsURLConnection) conn).setHostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String arg0, SSLSession arg1) {
                    return true;
                }
            });
        } else {
            conn = (HttpURLConnection) url.openConnection();
        }
        if (setProperty) {
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json, application/text, text/plain");
            conn.setConnectTimeout(LockConstants.REST_CONNECT_TIMEOUT);
            conn.setReadTimeout(LockConstants.REST_READ_TIMEOUT);
        }
        conn.setRequestProperty("connection", "close");

        return conn;
    }

    public void parseRestServerURI() {
        if (LockConstants.REST_SERVER_URI == null || LockConstants.REST_SERVER_URI.equals("")) {
            return;
        }
        regionServerLock.lock();
        if (restUriListList.size() > 0) {
            regionServerLock.unlock();
            return;
        }
        try {
            String[] restUris = LockConstants.REST_SERVER_URI.split(";");
            for (String restUri: restUris) {
                List<String> restUriList = new ArrayList<>();
                String[] LocationStrings = restUri.split("://");
                String procotolString = LocationStrings[0].trim();
                String[] strIPPortStrings = LocationStrings[1].split(":");
                String portString = strIPPortStrings[1].trim();
                String[] ipStrings = strIPPortStrings[0].split(",");
                for (String ipString : ipStrings) {
                    restUriList.add(procotolString + "://" + ipString.trim() + ":" + portString);
                }
                restUriListList.add(restUriList);
            }
        } catch (Exception e) {
            LOG.error("failed to get RestServer URI", e);
        }
        regionServerLock.unlock();
    }

    public void parseXDCMasterSlave() {
        if (LockConstants.XDC_MASTER_SLAVE == null || LockConstants.XDC_MASTER_SLAVE.equals("")) {
            return;
        }
        regionServerLock.lock();
        if (xdcListPair.size() > 0) {
            regionServerLock.unlock();
            return;
        }
        if (LockLogLevel.enableTraceLevel)
            LOG.info("XDC_MASTER_SLAVE: " + LockConstants.XDC_MASTER_SLAVE);
        try {
            String[] xdcMSArray = LockConstants.XDC_MASTER_SLAVE.split(";");
            for (String xdcMS: xdcMSArray) {
                AbstractMap.SimpleEntry<Integer, Boolean> xdcPair = new AbstractMap.SimpleEntry<Integer, Boolean>(Integer.valueOf(xdcMS), true);
                xdcListPair.add(xdcPair);
            }
        } catch (Exception e) {
            LOG.error("failed to get XDC_MASTER_SLAVE", e);
        } finally {
            regionServerLock.unlock();
        }
    }

    public RSSession getSession(String remoteAddress) throws IOException {
        sessionLock.lock();
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("get session for " + remoteAddress);
        }
        RSSession session = null;
        try {
            if (idleSessions.size() > 0) {
                if (LockLogLevel.enableTraceLevel) {
                    LOG.info("get session from idleSessions for " + remoteAddress);
                }
                session = idleSessions.remove(0);
            } else {
                session = new RSSession(this, stopper);
                session.start();
                if (LockLogLevel.enableTraceLevel) {
                    LOG.info("get session by new RSSession for " + remoteAddress);
                }
            }
            session.setName(session.getId() + "," + remoteAddress);
            workingSessions.add(session);
        } finally {
            sessionLock.unlock();
        }

        return session;
    }

    public void releaseSession(RSSession session) {
        sessionLock.lock();
        try {
            session.stopWork();
            workingSessions.remove(session);
            idleSessions.add(session);
        } finally {
            sessionLock.unlock();
        }

    }

    public void close() {
        connectionCache.close();
    }

    public LockHolder applyLock(Long txID, boolean canOverUse) {
        LockHolder retHolder = null;
        int lockHolerNum_ = 0;
        holderRWLock.readLock().lock();
        try {
            AtomicInteger lockHolerNum = lockHolderMap.get(txID);
            if (lockHolerNum == null) {
                //double check
                synchronized (lockHolderMap) {
                    lockHolerNum = lockHolderMap.get(txID);
                }
            }
            if (lockHolerNum == null) {
                lockHolerNum = new AtomicInteger(0);
                lockHolderMap.put(txID, lockHolerNum);
            } else {
                lockHolerNum_ = lockHolerNum.intValue();
                if (LockConstants.MAX_LOCK_NUMBER_PER_TRANS != 0 && !canOverUse
                        && lockHolerNum_ >= LockConstants.MAX_LOCK_NUMBER_PER_TRANS) {
                    LOG.warn("Not enough lock resources for txId " + txID + " Holders " + lockHolerNum_);
                    lockHolerNum.set(0 - lockHolerNum_);//do rollback
                    return null;
                } else if (lockHolerNum_ < 0) {
                    //in rollback
                    LOG.warn("txId " + txID + " is rollbacking Holders " + lockHolerNum_);
                    return null;
                }
            }

            if (checkMemoryCounter.decrementAndGet() <= 0) {
                    //check regionServer memory
                    checkMemoryCounter.set(RSConstants.CHECK_REGION_MEMORY_INTERVALS);
                    if (!canOverUse && !checkForPreventDownRegion(txID, lockHolerNum)) {
                        //set negative later
                        return null;
                    }
            }

            boolean isOverLoad = false;
            retHolder = lockHolderVectorCache.getElement();
            if (retHolder == null) {
                if (LockConstants.MAX_LOCK_NUMBER != 0) {
                    if (canOverUse) {
                        //log only
                        LOG.warn("Not enough lock resources");
                    } else {
                        //do rollback
                        lockHolerNum.set(0 - lockHolerNum_);
                        return null;
                    }
                }
                isOverLoad = true;
                retHolder = new LockHolder();
            }
            if (appliedSet != null) {
                synchronized (appliedSet) {
                    appliedSet.add(retHolder);
                }
            }
            lockHolerNum.incrementAndGet();
            int currentLockNum = 0;
            if (isOverLoad) {
                synchronized (overusedLockHolderCount) {
                    //exhausted
                    // +1 and + cache size
                    currentLockNum =
                        overusedLockHolderCount.incrementAndGet() + lockHolderVectorCache.size();
                }
            } else {
                    currentLockNum = lockHolderVectorCache.size() - lockHolderVectorCache.cachedSize();
            }
            synchronized (maxUsedLockHolderCount) {
                if (currentLockNum > maxUsedLockHolderCount.intValue()) {
                    maxUsedLockHolderCount.set(currentLockNum);
                }
            }
        } finally {
            holderRWLock.readLock().unlock();
        }
        return retHolder;
    }

    public void returnLock(Long txID, LockHolder holder) {
        holderRWLock.readLock().lock();
        try {
            holder.reInit();
            AtomicInteger lockHolerNum = lockHolderMap.get(txID);
            if (lockHolerNum == null) {
                //double check
                synchronized (lockHolderMap) {
                    lockHolerNum = lockHolderMap.get(txID);
                }
            }
            if (lockHolerNum == null)
                return;
            if (lockHolerNum.intValue() > 0)
                lockHolerNum.decrementAndGet();
            else
                lockHolerNum.incrementAndGet();//in rollback
            if (appliedSet != null) {
                synchronized (appliedSet) {
                    appliedSet.remove(holder);
                }
            }

            synchronized (overusedLockHolderCount) {
                if (overusedLockHolderCount.intValue() > 0) {
                    overusedLockHolderCount.decrementAndGet();
                    //no return to cache
                    return;
                }
            }
            lockHolderVectorCache.add(holder);
        } finally {
            holderRWLock.readLock().unlock();
        }
    }

    public void checkAndRecordLockWait(LockWait lockWait) {
        checkAndRecordLockWait(lockWait, true);
    }

    public void checkAndRecordLockWait(LockWait lockWait, boolean waitIsOver) {
        if (this.lockWaitInfoOutputWorker == null || lockWait == null
                || LockUtils.isIntentionLock(lockWait.getMaskHold()) || !isOverTimeThreshold(lockWait)) {
            return;
        }
        try {
            if (waitIsOver) {
                boolean removeOnly = (lockWait.getDurableTime() < LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD);
                if (!removeOnly) {
                    //avoid rowID is reinited when the lockWait is printting
                    Lock tmp = (Lock) lockWait.getLock().clone();
                    lockWait.setLock(tmp);
                }
                this.lockWaitInfoOutputWorker.terminatedLockWaitQueue.add(lockWait);
            } else {
                //clone for LockWaitCollector to avoid concurrent access holderTxIDs of LockWait in waitGraph in LockManager
                LockWait copyLockWait = (LockWait) lockWait.clone();
                copyLockWait.setHolderTxIDs(new CopyOnWriteArraySet<Long>());
                copyLockWait.getHolderTxIDs().addAll(lockWait.getHolderTxIDs());
                copyLockWait.setDurableTime(lockWait.getDurableTime());
                this.lockWaitInfoOutputWorker.timeoutLockWaitQueue.add(copyLockWait);
            }
        } catch (CloneNotSupportedException ex) {
            LOG.warn("checkAndRecordLockWait: ", ex);
        }
    }

    private boolean isOverTimeThreshold(LockWait lockWait) {
        long durableTime = System.currentTimeMillis() - lockWait.getCreateTimestamp();
        //record 1 LOCK_WAIT_TIMEOUT early for get trans from all region
        if (durableTime >= LockConstants.LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD) {
            lockWait.setDurableTime(durableTime);
            return true;
        } else if (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD <= LockConstants.LOCK_WAIT_TIMEOUT) {
            //The lockwait may end in one LOCK_WAIT_TIMEOUT, recording all of lockwait
            //No guarantee of accuracy
            lockWait.setDurableTime(durableTime);
            return true;
        }
        return false;
    }

    private void processGetRSParameter(RSMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processGetLockParameter " + lmMessage + " " + lmConn);
        }
        StringBuffer sb = new StringBuffer();
        RSParameterMessage message = (RSParameterMessage) RSMessage.getMessage(RSMessage.MSG_TYP_GET_RS_PARAMETER_RESPONSE);
        lmMessage.setMsgType(RSMessage.MSG_TYP_GET_RS_PARAMETER_SINGLE_NODE);
        RSParameterMessage local = processGetRSParameterOnSingleNode(lmMessage, null);
        if (local.isErrorFlag()) {
            sb.append(this.serverName + ": " + local.getErrorMsg());
        }
        message.merge(local);
        Set<String> regionServers = listAllRegionServersWithoutLocal();
        if (regionServers != null && regionServers.size() > 0) {
            for (String regionServer : regionServers) {
                RSParameterMessage remote = (RSParameterMessage) sendAndReceiveLMMessage(regionServer,lmMessage);
                if (remote == null || remote.getMsgType() != RSMessage.MSG_TYP_GET_RS_PARAMETER_RESPONSE) {
                    String msg = "unexpected lock response message: " + remote + " from " + regionServer;
                    LOG.error(msg);
                    if (sb.length() > 0)
                        sb.append("\n");
                    sb.append(msg);
                    continue;
                }
                if (remote.isErrorFlag()) {
                    if (sb.length() > 0)
                        sb.append("\n");
                    sb.append(regionServer + ": " + remote.getErrorMsg());
                }
                message.merge(remote);
            }
        }
        if(sb.length() > 0){
            message.setErrorFlag(true);
            message.setErrorMsg(sb.toString());
        }
        try {
            lmConn.send(message);
        } catch (Exception e) {
            LOG.error("processGetLockParameter failed to send message to " + lmConn);
            throw e;
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processGetLockParameter " + message + " " + lmConn);
        }
    }

    private RSParameterMessage processGetRSParameterOnSingleNode(RSMessage lmMessage, RSConnection lmConn) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processGetLockParameterOnSingleNode " + lmMessage + " "
                    + (lmConn == null ? "local" : lmConn));
        }
        String type = ((LMLockInfoReqMessage)lmMessage).getType();
        //Properties properties = new Properties();
        OrderedProperties properties = new OrderedProperties();

        if (type.equals(RSMessage.GET_PARAMETER) || type.equals(RSMessage.LOCK_STATUS)) {
            properties.setProperty("ENABLE_ROW_LEVEL_LOCK", String.valueOf(LockConstants.ENABLE_ROW_LEVEL_LOCK));
        }
        if (LockConstants.ENABLE_ROW_LEVEL_LOCK && (type.equals(RSMessage.GET_PARAMETER) || type.equals(RSMessage.GET_LOCK_PARAMETER))) {
            properties.setProperty("LOCK_TIME_OUT", String.valueOf(LockConstants.LOCK_WAIT_TIMEOUT));
            properties.setProperty("LOCK_ENABLE_DEADLOCK_DETECT", String.valueOf(LockConstants.LOCK_ENABLE_DEADLOCK_DETECT));
            properties.setProperty("LOCK_SKIP_META_TABLE", String.valueOf(LockConstants.LOCK_SKIP_META_TABLE));
            properties.setProperty("LOCK_CACHE_SIZE", String.valueOf(LockConstants.LOCK_CACHE_SIZE));
            properties.setProperty("LOCK_ESCALATION_THRESHOLD", String.valueOf(LockConstants.LOCK_ESCALATION_THRESHOLD));
            properties.setProperty("LOCK_TRYLOCK_TIMES", String.valueOf(LockConstants.LOCK_TRYLOCK_TIMES));
            properties.setProperty("REST_SERVER_URI", LockConstants.REST_SERVER_URI != null ? LockConstants.REST_SERVER_URI : "");
            properties.setProperty("XDC_MASTER_SLAVE", LockConstants.XDC_MASTER_SLAVE != null ? LockConstants.XDC_MASTER_SLAVE : "");
            properties.setProperty("TRANSACTION_CLEAN_INTERVAL", String.valueOf(LockConstants.TRANSACTION_CLEAN_INTERVAL));
            properties.setProperty("DISTRIBUTED_DEADLOCK_DETECT_INTERVAL",String.valueOf(LockConstants.DISTRIBUTED_DEADLOCK_DETECT_INTERVAL));
            properties.setProperty("REST_CONN_FAILED_TIMES", String.valueOf(LockConstants.REST_CONN_FAILED_TIMES));
            properties.setProperty("REST_CONNECT_TIMEOUT", String.valueOf(LockConstants.REST_CONNECT_TIMEOUT));
            properties.setProperty("REST_READ_TIMEOUT", String.valueOf(LockConstants.REST_READ_TIMEOUT));
            properties.setProperty("REST_CONNECT_DETECT_INTERVAL", String.valueOf(LockConstants.REST_CONNECT_DETECT_INTERVAL));
            properties.setProperty("LOCK_LOG_LEVEL", String.valueOf(LockConstants.LOCK_LOG_LEVEL));
            properties.setProperty("ENABLE_LOCK_STATISTICS", String.valueOf(LockConstants.ENABLE_LOCK_STATISTICS));
            properties.setProperty("LOCK_WAIT_INFO_WAITTIME_THRESHOLD",String.valueOf(LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD));
            properties.setProperty("RECORD_TRANSACTION_LOCKNUM_THRESHOLD",String.valueOf(LockConstants.RECORD_TRANSACTION_LOCKNUM_THRESHOLD));
            properties.setProperty("ENABLE_SAVEPOINT", String.valueOf(LockConstants.ENABLE_SAVEPOINT));
            properties.setProperty("TRANSACTION_CLEAN_INTERVAL", String.valueOf(LockConstants.TRANSACTION_CLEAN_INTERVAL));
            properties.setProperty("NUMBER_OF_OUTPUT_LOGS_PER_TIME", String.valueOf(LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME));
            properties.setProperty("ENABLE_LOCK_FOR_SELECT", String.valueOf(LockConstants.ENABLE_LOCK_FOR_SELECT));
            properties.setProperty("ENABLE_TABLELOCK_FOR_FULL_SCAN", String.valueOf(LockConstants.ENABLE_TABLELOCK_FOR_FULL_SCAN));
            properties.setProperty("MAX_LOCK_NUMBER", String.valueOf(LockConstants.MAX_LOCK_NUMBER));
            properties.setProperty("TRACE_LOCK_APPLIED", String.valueOf(LockConstants.TRACE_LOCK_APPLIED));
            properties.setProperty("MAX_LOCK_NUMBER_PER_TRANS", String.valueOf(LockConstants.MAX_LOCK_NUMBER_PER_TRANS));
            properties.setProperty("ENABLE_LONG_WAIT_LOCK_PRINTING", String.valueOf(LockConstants.ENABLE_LONG_WAIT_LOCK_PRINTING));
            properties.setProperty("LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT", String.valueOf(LockConstants.LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT));
            properties.setProperty("ENABLE_TRACE_WAIT_LOCK_INFO", String.valueOf(LockConstants.ENABLE_TRACE_WAIT_LOCK_INFO));
            properties.setProperty("LOCK_CLIENT_RETRIES_TIMES", String.valueOf(LockConstants.LOCK_CLIENT_RETRIES_TIMES));
        }

        if (type.equals(RSMessage.GET_PARAMETER) || type.equals(RSMessage.GET_RS_PARAMETER)) {
            properties.setProperty("RECORD_SCAN_ROW_THRESHOLD",
                String.valueOf(RSConstants.RECORD_SCAN_ROW_THRESHOLD));
            properties.setProperty("MAX_BLOCK_CHECK_RETRY_TIMES",
                String.valueOf(RSConstants.MAX_BLOCK_CHECK_RETRY_TIMES));
            properties.setProperty("ENABLE_ONLINE_BALANCE",
                String.valueOf(RSConstants.ONLINE_BALANCE_ENABLED));
            properties.setProperty("CHECK_REGION_MEMORY_INTERVALS",
                String.valueOf(RSConstants.CHECK_REGION_MEMORY_INTERVALS));
            properties.setProperty("REGION_MEMORY_WARNING_THRESHOLD",
                String.valueOf(RSConstants.REGION_MEMORY_WARNING_THRESHOLD));
            properties.setProperty("REGION_MEMORY_HIGHLOAD_THRESHOLD",
                String.valueOf(RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD));
            properties.setProperty("MEMORY_ALLOC_AMPLIFICATION_FACTOR",
                String.valueOf(RSConstants.MEMORY_ALLOC_AMPLIFICATION_FACTOR));
            properties.setProperty("Transactional CDC ChoreService ", MutationCapture2.getMc2TransactionsChoreThreadInfo()+",MutationCapture2.mcMap: "+ MutationCapture2.mcMap.size()); 
            List<String> tableNames= ((LMLockInfoReqMessage)lmMessage).getTableNames();
            if(tableNames !=null && tableNames.size()>0){
                for (Map.Entry<String, MutationCapture2> mc2 :  MutationCapture2.mcMap.entrySet()) {
                    String key = mc2.getKey();
                    for(String table:tableNames){
                        if(key.startsWith(table)){
                            properties.setProperty(key, mc2.getValue().getMc2TransactionsChoreThread().getName());
                            LOG.info(" Get Table info "+mc2.getValue().getMc2TransactionsChoreThread().getName());
                        }
                    }
                }
            }
            properties.setProperty("PRINT_TRANSACTION_LOG",
                String.valueOf(RSConstants.PRINT_TRANSACTION_LOG));
            properties.setProperty("DISABLE_NEWOBJECT_FOR_ENDPOINT",
                String.valueOf(RSConstants.DISABLE_NEWOBJECT_FOR_ENDPOINT));
            properties.setProperty("DISABLE_NEWOBJECT_FOR_MC",
                String.valueOf(RSConstants.DISABLE_NEWOBJECT_FOR_MC));
            properties.setProperty("RECORD_TIME_COST_COPRO",
                String.valueOf(RSConstants.RECORD_TIME_COST_COPRO));
            properties.setProperty("RECORD_SCAN_ROW_THRESHOLD",
                String.valueOf(RSConstants.RECORD_SCAN_ROW_THRESHOLD));
            properties.setProperty("RECORD_TIME_COST_COPRO_ALL",
                String.valueOf(RSConstants.RECORD_TIME_COST_COPRO_ALL));
        }

        RSParameterMessage message = (RSParameterMessage) RSMessage.getMessage(RSMessage.MSG_TYP_GET_RS_PARAMETER_RESPONSE);
        message.addNewProperties(serverName, properties);
        //check license
        if (!LockConstants.HAS_VALID_LICENSE) {
            message.setErrorFlag(true);
            message.setErrorMsg("The module ROWLOCK is not supported in this database version.");
        }
        if (lmConn != null) {
            try {
                lmConn.send(message);
            } catch (Exception e) {
                LOG.error("processGetLockParameterOnSingleNode failed to send message to " + lmConn);
                throw e;
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processGetLockParameterOnSingleNode " + message + " " + (lmConn == null ? "local" : lmConn));
        }
        return message;
    }

    public List<Long> checkNotExistsLockWaits(Map<String, Set<Long>> suspectLockWaitMap) {
        List<Long> notExistsLockWaits = new ArrayList<>();
        if (!suspectLockWaitMap.isEmpty()) {
            Set<Long> tmpSet = new HashSet<>();
            for (Map.Entry<String, Set<Long>> kv : suspectLockWaitMap.entrySet()) {
                String regionName = kv.getKey();
                Set<Long> suspectLockWaits = kv.getValue();
                tmpSet.clear();
                //no block
                LockManager lockManager = lockManagers.get(regionName);
                if (lockManager != null) {
                    ConcurrentHashMap<Long, Transaction> transactionMap = lockManager.getTransactionMap();
                    if (transactionMap != null && !transactionMap.isEmpty()) {
                        tmpSet.addAll(suspectLockWaits);
                        suspectLockWaits.containsAll(transactionMap.keySet());
                        tmpSet.removeAll(suspectLockWaits);
                        notExistsLockWaits.addAll(tmpSet);
                    } else {
                        notExistsLockWaits.addAll(suspectLockWaits);
                    }
                }
            }
        }
        return notExistsLockWaits;
    }

    //self return switch
    private boolean checkForPreventDownRegion(Long txId, AtomicInteger counter) {
        if (RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD == 100)
            return true;//no check
        long memUsed = memoryBean.getHeapMemoryUsage().getUsed();
        long memMax = memoryBean.getHeapMemoryUsage().getMax();
        if (memMax <= 0) {
            LOG.error("preventDownRegion can't get HeapMemoryUsage.");
            return false;//error
        }
        int memoryPercentage = (int) (memUsed * 1.0 / memMax * 100);
        if (memoryPercentage > RSConstants.REGION_MEMORY_HIGHLOAD_THRESHOLD) {
            if (isTopHolderTransaction(counter)) {
                LOG.error("no enough memory in local regionServer, memory usage " + String.valueOf(memoryPercentage)
                        + "% kill tx: " + txId + " has " + counter.intValue() + " lockHolders.");
                return false;
            }
        }
        return true;
    }

    private boolean isTopHolderTransaction(AtomicInteger counter) {
        if (counter.intValue() < 0) //in rollback
            return true;
        for (Map.Entry<Long, AtomicInteger> entry : lockHolderMap.entrySet()) {
            if (counter.intValue() < entry.getValue().intValue())
                return false;
        }
        return true;
    }
}
