package org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock;

import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSConnection;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockLogLevel;
import org.apache.hadoop.hbase.coprocessor.transactional.message.RSMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.TransactionMsg;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockInfoReqMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockMsg;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockInfoMessage;

//log4j
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.Date;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class LockWaitInfoOutputWorker {
    private RSServer rsServer;
    private Stoppable stopper;
    private Logger LOG = Logger.getRootLogger();
    private Logger extLOG = Logger.getLogger(LockWaitInfoOutputWorker.class);
    private Level OM_LEVEL = new CustomLog(Level.FATAL_INT + 100, "OM_LEVEL", 6);
    private String localHostname;
    private LMLockInfoReqMessage sentMsg = new LMLockInfoReqMessage(RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE);
    private LMLockInfoMessage mergedMsg = (LMLockInfoMessage) RSMessage
            .getMessage(RSMessage.MSG_TYP_LOCK_INFO_RESPONSE);

    //cache
    private Map<Long, LockWait> waitLockMap = new ConcurrentHashMap<>();//for lockwait which has got lock
    private Map<Long, TransactionMsg> lockwaitTransCache = new ConcurrentHashMap<>();//cache for current blocked transaction,no need update every time
    private Map<Long, LockInfoCache> relatedTransCache = new ConcurrentHashMap<>();//update every time

    //for batch search
    private volatile LMLockInfoMessage buffer[] = null;//no List no lock
    private volatile CyclicBarrier wakeBarrier;
    private Map<String, GetInfoTaskThread> taskThreads = new HashMap<>();//threads for getting lock info = do local merge
    private Map<String, AtomicInteger> targetTableMap = new HashMap<>();//tablename && counter protected by synchronized

    //for log output
    private int count = 0;
    private StringBuffer sb = new StringBuffer();//output buffer
    private Set<Long> fetchList = new HashSet<>();
    private Set<Long> printedTransSet = new HashSet<>();//has been printed
    private Map<Long, TransactionMsg> printableTransMap = new HashMap<>();//ready to printing
    private Set<String> printableTableSet = new HashSet<>();//Printable table range
    private Set<String> printableRegionSet = new HashSet<>();//Printable region range

    //timeout
    private static int BATCH_SEARCH_TIMEOUT = LockConstants.LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT;
    private static long MAX_CACHE_TTL = LockConstants.LOCK_WAIT_TIMEOUT + 1000;
    private static final int QUEUE_TIMEOUT_WAIT = 10000;//wake up to clean cache when blockingQueue is idle
    private static int WAIT_BATCH_SEARCH_TIMEOUT = BATCH_SEARCH_TIMEOUT + 1000;
    private static final int QUEUE_NO_WAIT = 100;
    private static final int THREAD_SLEEP_TIME = 30;
    private static int COMBO_BATCH_TX_SEARCH_SIZE = 5;
    private static int DO_PRINTING_PRE_TRANS_SIZE = 10;

    private ThreadLocal<SimpleDateFormat> timeFormatter = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("HH:mm:ss.S");
        }
    };

    private LockWaitCollector lockWaitCollector = new LockWaitCollector();
    private LockWaitInfoOutputer lockWaitInfoOutputer = new LockWaitInfoOutputer();
    private LockInfoCollector lockInfoCollector = new LockInfoCollector();
    private ReentrantLock lockInfoCollectorLock = new ReentrantLock(true);

    public LinkedBlockingQueue<LockWait> timeoutLockWaitQueue = new LinkedBlockingQueue<>();
    public LinkedBlockingQueue<LockWait> terminatedLockWaitQueue = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Set<Long>> needToCollectTxIdsQueue = new LinkedBlockingQueue<>();

    //LockInfoCache
    class LockInfoCache {
        //timestamp
        public long timestamp;
        //lock info
        public TransactionMsg lockInfo;
        //related table List
        public Set<String> relatedTableList;
        //related region List
        public Set<String> containRegionList;

        // 1  is fresh
        // 0  is unfresh but not timeout
        // -1 is timeout
        public int isFresh() {
            long aliveTime = System.currentTimeMillis() - this.timestamp;
            long exp = MAX_CACHE_TTL - aliveTime;
            if ( exp <= 0) return -1;
            else if (exp >= (LockConstants.LOCK_WAIT_TIMEOUT + 100)) return 1;
            else return 0;
        }
        private boolean isTableContains(String tableName) {
            for (String regionName : this.lockInfo.getTableLocks().keySet()) {
                if (regionName.indexOf(tableName.toUpperCase()) != -1) {
                    return true;
                }
            }
            return false;
        }

        private boolean isRegionContains(String regionName) {
            return this.lockInfo.getRowLocks().containsKey(regionName);
        }

        public boolean isCompletedCache() {
            boolean ret = false;
            for (String regionName : relatedTableList) {
                ret = isTableContains(regionName);
                if (!ret) {
                    break;
                }
            }
            if (ret) {
                for (String regionName : containRegionList) {
                    ret = isRegionContains(regionName);
                    if (!ret) {
                        break;
                    }
                }
            }
            return ret;
        }

        public boolean isCompletedCache(String tableName, String regionName) {
            return isTableContains(tableName) && isRegionContains(regionName);
        }

        public LockInfoCache(TransactionMsg lockInfo, long createTimestamp) {
            this.timestamp = createTimestamp;
            this.lockInfo = lockInfo;
            this.relatedTableList = new HashSet<>();
            this.containRegionList = new HashSet<>();
        }
    }

    class CustomLog extends Level {
        private static final long serialVersionUID = 1L;

        public CustomLog(int level, String levelStr, int syslogEquivalent) {
            super(level, levelStr, syslogEquivalent);
        }
    }

    //task thread
    class GetInfoTaskThread extends Thread {
        public String hostName;
        public volatile int arrayIndex = 0;
        private boolean isLocal;

        public GetInfoTaskThread(String hostName) {
            this.hostName = hostName;
            this.isLocal = localHostname.equalsIgnoreCase(hostName);
            this.setDaemon(true);
        }

        public void wakeup() throws InterruptedException {
            //spin lock
            int i = 100;
            while (this.getState() != Thread.State.WAITING && i > 0) {
                Thread.sleep(THREAD_SLEEP_TIME);
                i--;
            }
            synchronized (this) {
                this.notify();
            }
        }

        @Override
        public void run() {
            while (true) {
                //wait for work
                try {
                    synchronized (this) {
                        this.wait();
                    }
                } catch (InterruptedException e) {
                    //wake up for death
                    break;
                }
                RSConnection conn = null;
                try {
                    if (isLocal) {
                        buffer[arrayIndex] = rsServer.processSingleNodeLockInfo(sentMsg, null);
                    } else {
                        conn = rsServer.getConnectionCache().borrowConnection(hostName);
                        conn.send(sentMsg);
                        buffer[arrayIndex] = (LMLockInfoMessage) conn.receive_timeout(BATCH_SEARCH_TIMEOUT);
                    }
                } catch (Exception e) {
                    LOG.warn("LockWaitInfoOutputer: " + e.getMessage(), e);
                    //came from RSConnection
                    if (isLocal != true) {
                        rsServer.getConnectionCache().removeConnection(hostName, conn);
                        conn = null;
                    }
                    buffer[arrayIndex] = null;
                } finally {
                    if (conn != null) {
                        rsServer.getConnectionCache().releseConnection(hostName, conn);
                    }
                }
                try {
                    //let class outputLockInfo captures TimeoutException,no here
                    //wakeBarrier.await(30, TimeUnit.SECONDS);
                    wakeBarrier.await();
                } catch (Exception e) {
                    if (e instanceof BrokenBarrierException) {
                        //class outputLockInfo resets CyclicBarrier
                    } else {
                        LOG.warn("LockWaitInfoOutputer: " + e.getMessage(), e);
                    }
                }
            }
        }
    }

    //waitLock collector
    class LockWaitCollector extends Thread {
        private volatile int sleepTime = QUEUE_TIMEOUT_WAIT;
        private volatile Set<Long> needFetchTrans = new HashSet<>();

        @Override
        public void run() {
            long currentTimestamp = 0;
            LockWait lockWait = null;
            Map<Long, LockWait> needPrintTrans = new HashMap<>();
            while (true) {
                try {
                    lockWait = timeoutLockWaitQueue.poll(sleepTime, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    //wake up for death
                    break;
                }
                currentTimestamp = System.currentTimeMillis();
                //queue is idle
                if (lockWait == null) {
                    if (sleepTime == QUEUE_NO_WAIT) {
                        //batch search
                        fetchLockInfoFromRegionServer(needFetchTrans, true);
                        if (LockConstants.ENABLE_LONG_WAIT_LOCK_PRINTING && !needPrintTrans.isEmpty()) {
                            outputLockInfo(needPrintTrans, currentTimestamp);
                        }
                        needFetchTrans.clear();
                        sleepTime = QUEUE_TIMEOUT_WAIT;
                    }
                    continue;
                }
                //recording lockWait until the queue has been empty
                sleepTime = QUEUE_NO_WAIT;
                if (lockWait.getWaitStatus() != LockWaitStatus.WAITING) {
                    continue;
                }
                Long txId = lockWait.getTxID();
                synchronized (waitLockMap) {
                    if (LockConstants.ENABLE_TRACE_WAIT_LOCK_INFO && !waitLockMap.containsKey(txId)) {
                        if (LockLogLevel.enableLockInfoOutputerLevel) {
                            LOG.info("[TRACE] wants cache for lockwait " + txId + "." + "\n");
                        }
                        //update cache
                        needFetchTrans.add(txId);
                    }
                    waitLockMap.put(txId, lockWait);
                }
                //record copy of lockwait for GOT_LOCK
                //If empty, then empty
                if (lockWait.getHolderTxIDs().isEmpty()) {
                    /*
                    LockWait copyLockWait = waitLockMap.remove(txId);
                    if (copyLockWait != null && !copyLockWait.getHolderTxIDs().isEmpty()) {
                        lockWait.getHolderTxIDs().addAll(copyLockWait.getHolderTxIDs());
                    }
                    */
                    if (LockLogLevel.enableLockInfoOutputerLevel) {
                        LOG.info("[TRACE] The HolderTxIDs of lockwait txID: " + lockWait.getTxID() + " is empty.\n");
                    }
                }
                //aim to target table
                String tableName = lockWait.getTableName();
                {
                    synchronized (targetTableMap) {
                        AtomicInteger counter = targetTableMap.get(tableName);
                        if (counter == null) {
                            targetTableMap.put(tableName, new AtomicInteger(1));
                        } else {
                            counter.incrementAndGet();
                        }
                    }
                }

                if (LockConstants.ENABLE_TRACE_WAIT_LOCK_INFO) {
                    synchronized (waitLockMap) {
                        synchronized (relatedTransCache) {
                            //make or update snapshot for HolderTxID
                            for (Long holderTxID : lockWait.getHolderTxIDs()) {
                                if (!needFetchTrans.contains(holderTxID) && !waitLockMap.containsKey(holderTxID)) {
                                    LockInfoCache cache = relatedTransCache.get(holderTxID);
                                    if (cache != null) {
                                        int flg = cache.isFresh();
                                        if (flg != 1) {
                                            if (LockLogLevel.enableLockInfoOutputerLevel) {
                                                LOG.info("[TRACE] cache txid " + holderTxID + " is not fresh." + "\n");
                                            }
                                            if (flg == 0) {
                                                if (LockLogLevel.enableLockInfoOutputerLevel) {
                                                    LOG.info("[TRACE] reuse txid " + holderTxID + " once when this transaction does not exist." + "\n");
                                                }
                                            } else {
                                                relatedTransCache.remove(holderTxID);
                                            }
                                        } else {
                                            if (LockLogLevel.enableLockInfoOutputerLevel) {
                                                LOG.info("[TRACE] wants cache for txid " + holderTxID + "." + "\n");
                                            }
                                            cache.relatedTableList.add(tableName);
                                            cache.containRegionList.add(lockWait.getRegionName());
                                        }
                                    }
                                    needFetchTrans.add(holderTxID);
                                }
                            }
                        }
                    }
                }

                //regular print log
                if (LockConstants.ENABLE_LONG_WAIT_LOCK_PRINTING) {
                    if (lockWait.getDurableTime() >= LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD) {
                        needPrintTrans.put(lockWait.getTxID(), lockWait);
                        if (needPrintTrans.size() >= DO_PRINTING_PRE_TRANS_SIZE) {
                            fetchLockInfoFromRegionServer(needFetchTrans, true);
                            needFetchTrans.clear();
                            outputLockInfo(needPrintTrans, currentTimestamp);
                        }
                    }
                }

                //put all lock-wait trans into lockwaitTransCache
                if (needFetchTrans != null && needFetchTrans.size() >= COMBO_BATCH_TX_SEARCH_SIZE) {
                    boolean isSync = (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD <= LockConstants.LOCK_WAIT_TIMEOUT);
                    fetchLockInfoFromRegionServer(needFetchTrans, isSync);
                    needFetchTrans.clear();
                }
                if (LockLogLevel.enableLockInfoOutputerLevel) {
                    LOG.info("[TRACE] Using " + (System.currentTimeMillis() - currentTimestamp) + " ms to record lockwait txid " + lockWait.getTxID() + ".\n");
                    LOG.info("[TRACE] The size of timeoutLockWaitQueue " + timeoutLockWaitQueue.size() + ".\n");
                    LOG.info("[TRACE] The size of needFetchTrans " + (needFetchTrans != null ? needFetchTrans.size() : 0) + ".\n");
                }
            }
        }
    }

    //waitLock outputer
    class LockWaitInfoOutputer extends Thread {

        @Override
        public void run() {
            LockWait lockWait = null;
            Map<Long, LockWait> needPrintTrans = new HashMap<>();
            while (true) {
                try {
                    lockWait = terminatedLockWaitQueue.poll(QUEUE_TIMEOUT_WAIT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    //wake up for death
                    break;
                }
                long currentTimestamp = System.currentTimeMillis();
                if (lockWait == null) {
                    int relatedTransCacheSize = 0;
                    int lockwaitTransCacheSize = 0;
                    int waitLockMapSize = 0;
                    //idle,wake up to clean cache
                    synchronized (relatedTransCache) {
                        for (Iterator<Map.Entry<Long, LockInfoCache>> itor = relatedTransCache.entrySet()
                                .iterator(); itor.hasNext();) {
                            Map.Entry<Long, LockInfoCache> entry = itor.next();
                            LockInfoCache cache = entry.getValue();
                            // >= not >
                            if (currentTimestamp - cache.timestamp >= MAX_CACHE_TTL) {
                                if (LockLogLevel.enableLockInfoOutputerLevel) {
                                    LOG.info("[TRACE] drop unfresh cache txid " + entry.getKey() + "\n");
                                }
                                itor.remove();
                            }
                        }
                        relatedTransCacheSize = relatedTransCache.size();
                    }
                    //clean not exists entries
                    synchronized (waitLockMap) {
                        //clean not exists lockwait
                        cleanNotExistsLockWaits();
                        waitLockMapSize = waitLockMap.size();
                        synchronized (lockwaitTransCache) {
                            for (Iterator<Map.Entry<Long, TransactionMsg>> itor = lockwaitTransCache.entrySet()
                                    .iterator(); itor.hasNext();) {
                                Map.Entry<Long, TransactionMsg> pair = itor.next();
                                if (!waitLockMap.containsKey(pair.getKey())) {
                                    if (LockLogLevel.enableLockInfoOutputerLevel) {
                                        LOG.info("[TRACE] drop not exists lockwait cache txid " + pair.getKey() + "\n");
                                    }
                                    itor.remove();
                                }
                            }
                            lockwaitTransCacheSize = lockwaitTransCache.size();
                        }
                    }
                    if (LockLogLevel.enableLockInfoOutputerLevel) {
                        LOG.info("[TRACE] The sizeof lockwaitTransCache " + lockwaitTransCacheSize + "\n");
                        LOG.info("[TRACE] The sizeof relatedTransCache " + relatedTransCacheSize + "\n");
                        LOG.info("[TRACE] The sizeof waitLockMap " + waitLockMapSize + "\n");
                    }
                    continue;
                }

                Long txId = lockWait.getTxID();
                LockWaitStatus status = lockWait.getWaitStatus();
                LockWait copyLockWait = null;
                synchronized (waitLockMap) {
                    copyLockWait = waitLockMap.remove(txId);//pop
                }
                //filter
                {
                    if (lockWait.getDurableTime() < LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD) {
                        removeTableOnTargetTableMap(lockWait.getTableName());
                        synchronized(lockwaitTransCache) {
                            lockwaitTransCache.remove(txId);
                        }
                        continue;
                    }
                    if (status == LockWaitStatus.WAITING) {
                        //just remove
                        removeTableOnTargetTableMap(lockWait.getTableName());
                        synchronized(lockwaitTransCache) {
                            lockwaitTransCache.remove(txId);
                        }
                        continue;
                    } else if (status == LockWaitStatus.OK) {
                        //use copy of lockwait
                        if (copyLockWait != null) {
                            copyLockWait.setDurableTime(lockWait.getDurableTime());
                            copyLockWait.setWaitStatus(LockWaitStatus.OK);
                            lockWait = copyLockWait;
                        } else {
                            if (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD <= LockConstants.LOCK_WAIT_TIMEOUT) {
                                //ignore because of conflict duplicate call checkAndRecordLockWait
                                synchronized (lockwaitTransCache) {
                                    lockwaitTransCache.remove(txId);
                                }
                                continue;
                            }
                            LOG.warn("LockWaitInfoOutputer: Can't find copy of LockWait " + txId + " for GOT_LOCK");
                        }
                    } else {
                        //don't use LockWait copyLockWait
                    }
                }
                //output
                needPrintTrans.put(lockWait.getTxID(), lockWait);
                //too lag to printing log
                //if (needPrintTrans.size() >= DO_PRINTING_PRE_TRANS_SIZE) {
                //     outputLockInfo(needPrintTrans, currentTimestamp, relatedTrans);
                //}
                outputLockInfo(needPrintTrans, currentTimestamp);
                //lockwaitTransCache.remove(txId);//not needed
                removeTableOnTargetTableMap(lockWait.getTableName());
            }
        }
    }

    //lockInfo collector
    class LockInfoCollector extends Thread {
        private Set<Long> txIds = null;
        private List<Long> fetchList = new ArrayList<>();
        private StringBuffer timeoutTaskThreadNameStr = new StringBuffer();
        private Map<Long, TransactionMsg> putIntoLockwaitTransCache = new HashMap<>();
        private Map<Long, LockInfoCache> putIntoRelatedTransCache = new HashMap<>();
        private volatile GetInfoTaskThread idxTaskThreads[] = null;//for print timeout taskThread
        private List<String> targetTableList = new ArrayList<>();//link to sentMsg.tableNames
        private Set<String> servers = new HashSet<>();
        private volatile int sleepTime = QUEUE_TIMEOUT_WAIT;
        private Object alarm = new Object();
        public void waitForFinish() {
            try {
                int i = WAIT_BATCH_SEARCH_TIMEOUT / 3;
                synchronized (alarm) {
                    while (i > 0) {
                        if (this.getState() == State.TIMED_WAITING && needToCollectTxIdsQueue.isEmpty()) {
                            break;
                        }
                        alarm.wait(3);
                        i--;
                    }
                }
            } catch (InterruptedException e) {
                return;
            }
        }

        @Override
        public void run() {
            servers.add(localHostname);
            while (true) {
                putIntoLockwaitTransCache.clear();
                putIntoRelatedTransCache.clear();
                try {
                    txIds = needToCollectTxIdsQueue.poll(sleepTime, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    //wake up for death
                    break;
                }
                long currentTimestamp = 0;
                if (txIds != null) {
                    synchronized (waitLockMap) {
                        synchronized (relatedTransCache) {
                            for (Iterator<Long> itor = txIds.iterator(); itor.hasNext();) {
                                Long txId = itor.next();
                                if (!waitLockMap.containsKey(txId)) {
                                    LockInfoCache cache = relatedTransCache.get(txId);
                                    if (cache != null && cache.isFresh() == 1 && cache.isCompletedCache()) {
                                        if (LockLogLevel.enableLockInfoOutputerLevel) {
                                            LOG.info("[TRACE] cache is fresh " + txId + ".\n");
                                        }
                                        //drop
                                    } else {
                                        fetchList.add(txId);
                                    }
                                } else {
                                    fetchList.add(txId);
                                }
                            }
                        }
                    }
                    sleepTime = QUEUE_NO_WAIT;
                    if (fetchList.size() < COMBO_BATCH_TX_SEARCH_SIZE) {
                        continue;
                    }
                } else {
                    sleepTime = QUEUE_TIMEOUT_WAIT;
                    if (fetchList.isEmpty()) {
                        synchronized (alarm) {
                            alarm.notify();
                        }
                        continue;
                    }
                }
                targetTableList.clear();
                synchronized(targetTableMap) {
                    for (Map.Entry<String, AtomicInteger> kv : targetTableMap.entrySet()) {
                        if (kv.getValue().intValue() != 0) {
                            targetTableList.add(kv.getKey());
                        }
                    }
                }
                sentMsg.setTableNames(targetTableList);
                long sTime = 0;
                if (LockLogLevel.enableLockInfoOutputerLevel) {
                    sTime = System.currentTimeMillis();
                    LOG.info("[TRACE] fetchList " + fetchList + " target table " + sentMsg.getTableNames() + "\n");
                }
                getLockInfoFromAllRegion(fetchList);
                if (LockLogLevel.enableLockInfoOutputerLevel) {
                    LOG.info("[TRACE] got " + mergedMsg.getTransactionMsgMap().keySet() + " cost " + (System.currentTimeMillis() - sTime) +" ms\n");
                }
                currentTimestamp = System.currentTimeMillis();
                synchronized (waitLockMap) {
                    for (Map.Entry<Long, TransactionMsg> entry : mergedMsg.getTransactionMsgMap().entrySet()) {
                        if (waitLockMap.containsKey(entry.getKey())) {
                            putIntoLockwaitTransCache.put(entry.getKey(), entry.getValue());
                        } else {
                            LockInfoCache cache = new LockInfoCache(entry.getValue(), currentTimestamp);
                            for (String regionName : entry.getValue().getTableLocks().keySet()) {
                                cache.relatedTableList.add(regionName.substring(0, regionName.indexOf(',')));
                            }
                            cache.containRegionList.addAll(entry.getValue().getRowLocks().keySet());
                            putIntoRelatedTransCache.put(entry.getKey(), cache);
                        }
                    }
                }
                if (!putIntoLockwaitTransCache.isEmpty()) {
                    if (LockLogLevel.enableLockInfoOutputerLevel) {
                        LOG.info("[TRACE] put " + putIntoLockwaitTransCache.keySet() + " into lockwaitTransCache\n");
                    }
                    synchronized (lockwaitTransCache) {
                        lockwaitTransCache.putAll(putIntoLockwaitTransCache);
                    }
                }
                if (!putIntoRelatedTransCache.isEmpty()) {
                    if (LockLogLevel.enableLockInfoOutputerLevel) {
                        LOG.info("[TRACE] put " + putIntoRelatedTransCache.keySet() + " into relatedTransCache\n");
                    }
                    synchronized (relatedTransCache) {
                        relatedTransCache.putAll(putIntoRelatedTransCache);
                    }
                    synchronized (lockwaitTransCache) {
                        for (Long txId : putIntoRelatedTransCache.keySet()) {
                            lockwaitTransCache.remove(txId);
                        }
                    }
                }
                fetchList.clear();
            }
        }

        private void getLockInfoFromAllRegion(List<Long> fetchList) {
            mergedMsg.getTransactionMsgMap().clear();
            if (!fetchList.isEmpty()) {
                sentMsg.setTxIDs(fetchList);
                if (wakeBarrier == null || buffer == null || idxTaskThreads == null
                        || wakeBarrier.getParties() != servers.size() + 1) {
                    wakeBarrier = new CyclicBarrier(servers.size() + 1);// +1 self
                    buffer = new LMLockInfoMessage[servers.size()];
                    idxTaskThreads = new GetInfoTaskThread[servers.size()];
                }
                //local merge
                int idx = 0;
                Arrays.fill(buffer, null);
                Arrays.fill(idxTaskThreads, null);
                for (String serverName : servers) {
                    GetInfoTaskThread task = taskThreads.get(serverName);
                    if (task == null) {
                        task = CreateGetLockInfoThreads(serverName);
                        taskThreads.put(serverName, task);
                    }
                    task.arrayIndex = idx;
                    idxTaskThreads[idx] = task;
                    try {
                        task.wakeup();
                    } catch (InterruptedException e) {
                        //wakeup for death
                        wakeBarrier.reset();
                        return;
                    }
                    idx++;
                }
                try {
                    wakeBarrier.await(WAIT_BATCH_SEARCH_TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    wakeBarrier.reset();
                    String errorMsg = "";
                    if (ex instanceof TimeoutException) {
                        timeoutTaskThreadNameStr.delete(0, timeoutTaskThreadNameStr.length());
                        //who is timeout
                        for (int i = 0; i < buffer.length; i++) {
                            if (buffer[i] == null) {
                                timeoutTaskThreadNameStr.append(idxTaskThreads[i].hostName);
                                timeoutTaskThreadNameStr.append(",");
                            }
                        }
                        int strLen = timeoutTaskThreadNameStr.length();
                        if (strLen > 0) {
                            //remove last of ','
                            timeoutTaskThreadNameStr.deleteCharAt(strLen - 1);
                        }
                        errorMsg = "get lock information of transaction from " + timeoutTaskThreadNameStr.toString()
                                + " are timeout. ";
                        errorMsg += "Fetch list: " + fetchList;
                    }
                    if (errorMsg.isEmpty()) {
                        //other exception?
                        errorMsg = ex.getMessage();
                    }
                    LOG.warn("LockWaitInfoOutputer: " + errorMsg, ex);
                }
                for (int i = 0; i < buffer.length; i++) {
                    if (buffer[i] != null) {
                        mergedMsg.merge(buffer[i]);
                    }
                }
                //fetchList.clear();
            }
        }
    }

    private synchronized void outputLockInfo(Map<Long, LockWait> lockWaits, long doOutputTimestamp) {

        if (!LockConstants.ENABLE_TRACE_WAIT_LOCK_INFO) {
            //printing lockwait only
            //LockWait info
            sb.delete(0, sb.length());
            sb.append("\n"); //for log4j
            sb.append("waited lock:\n");
            StringUtil.generateHeader(sb, LockConstants.LOCK_WAIT_HEADER_FOR_LOG);
            for (LockWait lockWait : lockWaits.values()) {
                StringUtil.generateRowForWaitLock(sb, timeFormatter.get(), lockWait);
            }
            if (sb.length() > 1) {
                String msg = sb.toString();
                LOG.info(msg);
                extLOG.log(OM_LEVEL, msg);
            }
            lockWaits.clear();
            return;
        }

        fetchList.clear();
        printedTransSet.clear();
        printableTransMap.clear();
        printableTableSet.clear();
        printableRegionSet.clear();

        //First: Find all not in cache transactions as soon as possible
        synchronized (waitLockMap) {
            synchronized (relatedTransCache) {
                synchronized (lockwaitTransCache) {
                    for (LockWait lockWait : lockWaits.values()) {
                        //self
                        Long selfTxId = lockWait.getTxID();
                        TransactionMsg self = lockwaitTransCache.get(selfTxId);
                        if (self != null) {
                            printableTransMap.put(selfTxId, self);
                        }
                        printedTransSet.add(selfTxId);
                        if (lockWait.getWaitStatus() != LockWaitStatus.WAITING) {
                            fetchList.add(selfTxId);//update TransactionMsg for itself,and put it into the cache
                        }
                        //related lock
                        for (Long txId : lockWait.getHolderTxIDs()) {
                            if (printedTransSet.contains(txId)) {
                                continue;
                            }
                            printedTransSet.add(txId);
                            TransactionMsg lockInfo = null;
                            if (waitLockMap.containsKey(txId)) {
                                //never expire
                                lockInfo = lockwaitTransCache.get(txId);
                            } else {
                                LockInfoCache cachedLockInfo = relatedTransCache.get(txId);
                                if (cachedLockInfo != null) {
                                    if (doOutputTimestamp - cachedLockInfo.timestamp > MAX_CACHE_TTL) {
                                        //cache is expiration
                                        if (LockLogLevel.enableLockInfoOutputerLevel) {
                                            LOG.info("[TRACE] cache txid " + txId + " is expiration." + "\n");
                                        }
                                        if (lockWait.getWaitStatus() == LockWaitStatus.OK) {
                                            if (cachedLockInfo.isCompletedCache(lockWait.getTableName(),
                                                    lockWait.getRegionName())) {
                                                if (LockLogLevel.enableLockInfoOutputerLevel) {
                                                    LOG.info("[TRACE] reuse cache txid " + txId + " for snapshot for got_lock." + "\n");
                                                }
                                                lockInfo = cachedLockInfo.lockInfo;
                                            }
                                        }
                                        if (lockInfo == null) {
                                            relatedTransCache.remove(txId);
                                        }
                                    } else {
                                        if (cachedLockInfo.isCompletedCache(lockWait.getTableName(),
                                                lockWait.getRegionName())) {
                                            lockInfo = cachedLockInfo.lockInfo;
                                        } else {
                                            if (LockLogLevel.enableLockInfoOutputerLevel) {
                                                LOG.info("[TRACE] cache txid " + txId + " is not completed cache." + "\n");
                                            }
                                            relatedTransCache.remove(txId);
                                            lockInfo = null;
                                        }
                                    }
                                }
                            }
                            if (lockInfo == null) {
                                if (lockWait.getWaitStatus() != LockWaitStatus.OK) {
                                    fetchList.add(txId);
                                }
                            } else {
                                printableTransMap.put(txId, lockInfo);
                            }
                        }
                    }
                }
            }
        }
        //Second: Fetch from server
        {
            if (!fetchList.isEmpty()) {
                fetchLockInfoFromRegionServer(fetchList, true);
                synchronized (lockwaitTransCache) {
                    synchronized (relatedTransCache) {
                        for (Long txId : fetchList) {
                            if (printedTransSet.contains(txId)) {
                                LockInfoCache cachedLockInfo = relatedTransCache.get(txId);
                                if (cachedLockInfo != null) {
                                    printableTransMap.putIfAbsent(txId, cachedLockInfo.lockInfo);
                                } else {
                                    TransactionMsg lockInfo = lockwaitTransCache.get(txId);
                                    if (lockInfo != null) {
                                        printableTransMap.putIfAbsent(txId, lockInfo);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        //Third: Print log
        {
            //LockWait info
            sb.delete(0, sb.length());
            sb.append("\n"); //for log4j
            sb.append("waited lock:\n");
            StringUtil.generateHeader(sb, LockConstants.LOCK_WAIT_HEADER_FOR_LOG);
            for (LockWait lockWait : lockWaits.values()) {
                StringUtil.generateRowForWaitLock(sb, timeFormatter.get(), lockWait);
                printableTableSet.add(lockWait.getTableName());
                String regionFullName = lockWait.getRegionName();
                printableRegionSet
                        .add(regionFullName.substring(regionFullName.lastIndexOf(',') + 1, regionFullName.length()));
            }
            //to hbase log
            LOG.info(sb.toString());
            //related lock info
            sb.append("related lock:\n");
            StringUtil.generateHeader(sb, LockConstants.LOCK_HEADER);
            count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
            for (Map.Entry<Long, TransactionMsg> entry : printableTransMap.entrySet()) {
                generateRowForLock(entry.getKey(), entry.getValue());
            }
            if (sb.length() > 1) {
                extLOG.log(OM_LEVEL, sb.toString());
            }
        }
        //Final: Clean
        {
            lockWaits.clear();
        }
    }

    //copy from LMLockInfoMessage.java::getAllLock
    private void generateRowForLock(Long txId, TransactionMsg trans) {
        if (trans.getTableLocks() != null) {
            for (LMLockMsg lockMsg : trans.getTableLocks().values()) {
                if (!printableTableSet.contains(lockMsg.getTableName())) {
                    continue;
                }
                StringUtil.generateRow(sb, txId, lockMsg);
                count--;
                if (count == 0) {
                    extLOG.log(OM_LEVEL, sb.toString());
                    sb.delete(0, sb.length());
                    sb.append("\n");
                    count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
                }
            }
        }
        if (trans.getRowLocks() != null) {
            for (Map<String, LMLockMsg> rowLocks : trans.getRowLocks().values()) {
                for (LMLockMsg rowLock : rowLocks.values()) {
                    if (!printableRegionSet.contains(rowLock.getRegionName())) {
                        continue;
                    }
                    StringUtil.generateRow(sb, txId, rowLock);
                    count--;
                    if (count == 0) {
                        extLOG.log(OM_LEVEL, sb.toString());
                        sb.delete(0, sb.length());
                        sb.append("\n");
                        count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
                    }
                }
            }
        }
    }

    private GetInfoTaskThread CreateGetLockInfoThreads(String serverName) {
        GetInfoTaskThread task = new GetInfoTaskThread(serverName);
        task.setName("get_lock_info_from_" + serverName);
        task.setPriority(getThreadPriorityForInnerThread());
        task.start();
        //wait for thread has slept
        int i = 100;
        try {
            while (task.getState() != Thread.State.WAITING && i > 0) {
                Thread.sleep(THREAD_SLEEP_TIME);
                i--;
            }
        } catch (Exception e) {
            //no wait
        }
        return task;
    }

    public LockWaitInfoOutputWorker(RSServer rsServer, Stoppable stopper, String hbaseLogpath) {
        this.localHostname = rsServer.getServerName();
        this.rsServer = rsServer;
        this.stopper = stopper;
        this.sentMsg.setDebug(0);
        this.sentMsg.setType(RSMessage.LOCK_INFO);
        this.mergedMsg.setTransactionMsgMap(new ConcurrentHashMap<Long, TransactionMsg>());
        //threadPool
        //change to fatch lock info from local region server to reduce resource
        this.taskThreads.putIfAbsent(this.localHostname, CreateGetLockInfoThreads(this.localHostname));
        //log to extra logFile
        {
            //change additivity from true to false to reduce number of logs in hbase's log
            extLOG.setAdditivity(false);
            PatternLayout layout = new PatternLayout();
            //yyyy-MM-dd HH:mm:ss.SSS, thread-id, INFO, classname.funcationname:line ,message
            //%d{ISO8601}                 2020-10-25 14:00:24,389
            //%d{yyyy-MM-dd HH:mm:ss.SSS} 2020-10-25 14:00:24.389
            layout.setConversionPattern("%d{yyyy-MM-dd HH:mm:ss.SSS}, %t, INFO, %l, %m");
            RollingFileAppender appender = new RollingFileAppender();
            appender.setName("OM_LEVEL");
            appender.setThreshold(OM_LEVEL);
            appender.setImmediateFlush(true);
            appender.setBufferedIO(false);
            appender.setMaxBackupIndex(10);
            appender.setMaxFileSize("200MB");
            if (hbaseLogpath == null || hbaseLogpath.isEmpty()) {
                //local hadoop?
                hbaseLogpath = System.getenv("HBASE_LOG_DIR");
                if (hbaseLogpath == null || hbaseLogpath.isEmpty()) {
                    hbaseLogpath = "/var/log/hbase";
                }
            }
            //trafodion.lockwait-hostname.log
            appender.setFile(
                    hbaseLogpath + "/" + LockConstants.LOCKWAIT_LOGFILENAME + "-" + this.localHostname + ".log");
            appender.setEncoding("UTF-8");
            appender.setLayout(layout);
            appender.activateOptions();
            extLOG.addAppender(appender);
        }
        if (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD <= LockConstants.LOCK_WAIT_TIMEOUT) {
            COMBO_BATCH_TX_SEARCH_SIZE = 1;
            DO_PRINTING_PRE_TRANS_SIZE = 1;
            MAX_CACHE_TTL = new Double(LockConstants.LOCK_WAIT_TIMEOUT * 0.5F).longValue();
        }
    }

    private void removeTableOnTargetTableMap(String tableName) {
        synchronized (targetTableMap) {
            AtomicInteger counter = targetTableMap.get(tableName);
            if (counter != null && counter.intValue() > 0) {
                counter.decrementAndGet();
            }
        }
    }

    private int getThreadPriorityForInnerThread() {
        int threadPriority = Thread.currentThread().getPriority();
        if (threadPriority != Thread.MAX_PRIORITY) {
            threadPriority += 2;
            threadPriority = (threadPriority > Thread.MAX_PRIORITY) ? Thread.MAX_PRIORITY : threadPriority;
        }
        return threadPriority;
    }

    public void updateFetchLockInfoTimeout() {
        BATCH_SEARCH_TIMEOUT = LockConstants.LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT;
        MAX_CACHE_TTL = LockConstants.LOCK_WAIT_TIMEOUT + 1000;
        WAIT_BATCH_SEARCH_TIMEOUT = BATCH_SEARCH_TIMEOUT + 1000;
        COMBO_BATCH_TX_SEARCH_SIZE = 5;
        DO_PRINTING_PRE_TRANS_SIZE = 10;
        if (LockConstants.LOCK_WAIT_INFO_WAITTIME_THRESHOLD <= LockConstants.LOCK_WAIT_TIMEOUT) {
            COMBO_BATCH_TX_SEARCH_SIZE = 1;
            DO_PRINTING_PRE_TRANS_SIZE = 1;
            MAX_CACHE_TTL = new Double(LockConstants.LOCK_WAIT_TIMEOUT * 0.5F).longValue();
        }
    }

    public boolean fetchLockInfoFromRegionServer(Set<Long> txIds, boolean waitForFinish) {
        if (!LockConstants.ENABLE_TRACE_WAIT_LOCK_INFO) {
            return true;
        }
        if (txIds == null || txIds.isEmpty()) {
            return true;
        }
        if (LockLogLevel.enableLockInfoOutputerLevel) {
            LOG.info("[TRACE] fetch " + txIds + " from regionSever." + "\n");
        }
        lockInfoCollectorLock.lock();
        if (waitForFinish) {
            lockInfoCollector.waitForFinish();
        }
        needToCollectTxIdsQueue.add(txIds);
        if (waitForFinish) {
            lockInfoCollector.waitForFinish();
        }
        lockInfoCollectorLock.unlock();
        return true;
    }

    public void run() {
        lockWaitCollector.setName("lockWaitCollector");
        lockWaitInfoOutputer.setName("lockWaitInfoOutputer");
        lockInfoCollector.setName("lockInfoCollector");
        lockInfoCollector.setPriority(getThreadPriorityForInnerThread());
        lockWaitCollector.setDaemon(true);
        lockWaitInfoOutputer.setDaemon(true);
        lockInfoCollector.setDaemon(true);
        lockWaitCollector.start();
        lockWaitInfoOutputer.start();
        lockInfoCollector.start();
    }

    public void stop() {
        lockWaitCollector.interrupt();
        lockWaitInfoOutputer.interrupt();
        lockInfoCollector.interrupt();
        for (GetInfoTaskThread thx : taskThreads.values()) {
            thx.interrupt();
        }
    }

    private void cleanNotExistsLockWaits() {
        //group by regionName
        Map<String, Set<Long>> suspectLockWaitMap = new HashMap<>();
        for (Map.Entry<Long, LockWait> kv : waitLockMap.entrySet()) {
            Long txId = kv.getKey();
            LockWait lockWait = kv.getValue();
            String regionName = lockWait.getRegionName();
            Set<Long> txIds = suspectLockWaitMap.get(regionName);
            if (txIds == null) {
                suspectLockWaitMap.put(regionName, new HashSet<Long>());
                txIds = suspectLockWaitMap.get(regionName);
            }
            txIds.add(txId);
        }
        List<Long> notExistsLockWaits = this.rsServer.checkNotExistsLockWaits(suspectLockWaitMap);
        for (Long txId : notExistsLockWaits) {
            waitLockMap.remove(txId);
            if (LockLogLevel.enableLockInfoOutputerLevel) {
                LOG.info("[TRACE] remove not exists LockWait " + txId + " from waitLockMap." + "\n");
            }
        }
    }
}
