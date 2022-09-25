package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

import org.apache.log4j.Logger;
import org.json.JSONObject;

public class LMLockInfoMessage extends RSMessage {
    private static final long serialVersionUID = 1L;

    @Getter
    @Setter
    private Map<Long, TransactionMsg> transactionMsgMap;// key is transID
    @Getter
    @Setter
    private Map<Long, List<LMLockWaitMsg>> lockWaitMsgMap;
    @Getter
    @Setter
    private Map<Long, LMLockWaitMsg> deadLockMsgMap;
    @Getter
    @Setter
    private List<Long> transIDsList;  // record lock check transaction id
    @Getter
    @Setter
    private int lockInfoNumLimit;
    @Getter
    private List<Long> commitPendingIDs;

    public LMLockInfoMessage() {
        super(MSG_TYP_LOCK_INFO_RESPONSE);
    }

    public void getLock(LMLockInfoReqMessage lmLockInfoReqMessage) {
        if (lmLockInfoReqMessage.getType() == null) {
            return;
        }
        RSServer rsServer = RSServer.getInstance();
        if (lmLockInfoReqMessage.getType().equals(LOCK_INFO) || lmLockInfoReqMessage.getType().equals(LOCK_AND_WAIT)) {
            //use AtomicInteger instead of int
            AtomicInteger num = new AtomicInteger(lockInfoNumLimit);
            transactionMsgMap = rsServer.getLockInfo(lmLockInfoReqMessage.getTxIDs(), lmLockInfoReqMessage.getTableNames(), num, lmLockInfoReqMessage.getDebug());
            lockInfoNumLimit = num.intValue();
        }
        if (lmLockInfoReqMessage.getType().equals(LOCK_WAIT) || lmLockInfoReqMessage.getType().equals(LOCK_AND_WAIT) ||
            lmLockInfoReqMessage.getType().equals(DEAD_LOCK)){
            lockWaitMsgMap = rsServer.getLockWaitInfo(lmLockInfoReqMessage.getTxIDs(), lmLockInfoReqMessage.getHolderTxIDs(), lmLockInfoReqMessage.getTableNames());
        }
        if (lmLockInfoReqMessage.getType().equals(LOCK_CHECK)) {
            transIDsList = rsServer.getLockCheckTransIDs();
            this.commitPendingIDs = rsServer.getAllCommitPendingTxs();
        }
    }

    @Override
    public void merge(RSMessage msg) {
        LMLockInfoMessage singleNodeMessage = (LMLockInfoMessage)msg;
        TransactionMsg currentTransactionMsg = null;
        TransactionMsg tmpTransactionMsg = null;
        if (transactionMsgMap != null) {
            if (singleNodeMessage.getTransactionMsgMap() != null) {
                for (Map.Entry<Long, TransactionMsg> entry : singleNodeMessage.getTransactionMsgMap().entrySet()) {
                    currentTransactionMsg = transactionMsgMap.get(entry.getKey());
                    tmpTransactionMsg = entry.getValue();
                    if (currentTransactionMsg == null) {
                        transactionMsgMap.put(entry.getKey(), tmpTransactionMsg);
                    } else {
                        currentTransactionMsg.merge(tmpTransactionMsg);
                    }
                }
            }
        } else {
            if (singleNodeMessage.getTransactionMsgMap() != null) {
                transactionMsgMap = singleNodeMessage.getTransactionMsgMap();
            }
        }
        List<LMLockWaitMsg> currentLockWaitMsg = null;
        List<LMLockWaitMsg> tmpLockWaitMsg = null;
        if (lockWaitMsgMap != null) {
            if (singleNodeMessage.getLockWaitMsgMap() != null) {
                for (Map.Entry<Long, List<LMLockWaitMsg>> entry : singleNodeMessage.getLockWaitMsgMap().entrySet()) {
                    currentLockWaitMsg = lockWaitMsgMap.get(entry.getKey());
                    tmpLockWaitMsg = entry.getValue();
                    if (currentLockWaitMsg == null) {
                        lockWaitMsgMap.put(entry.getKey(), tmpLockWaitMsg);
                    } else {
                        currentLockWaitMsg.addAll(tmpLockWaitMsg);
                    }
                }
            }
        } else {
            if (singleNodeMessage.getLockWaitMsgMap() != null) {
                lockWaitMsgMap = singleNodeMessage.getLockWaitMsgMap();
            }
        }

        if (transIDsList != null) {
            if (singleNodeMessage.getTransIDsList() != null) {
                singleNodeMessage.getTransIDsList().removeAll(transIDsList);
                transIDsList.addAll(singleNodeMessage.getTransIDsList());
            }
        } else {
            if (singleNodeMessage.getTransIDsList() != null) {
                transIDsList = singleNodeMessage.getTransIDsList();
            }
        }
        if (commitPendingIDs != null) {
            if (singleNodeMessage.getCommitPendingIDs() != null) {
                singleNodeMessage.getCommitPendingIDs().removeAll(commitPendingIDs);
                commitPendingIDs.addAll(singleNodeMessage.getCommitPendingIDs());
            }
        } else {
            if (singleNodeMessage.getCommitPendingIDs() != null) {
                commitPendingIDs = singleNodeMessage.getCommitPendingIDs();
            }
        }
    }

    public String getAllLockJson() {
        if (transactionMsgMap == null) {
            return "{}";
        } else if (transactionMsgMap.isEmpty()) {
            return "{}";
        }

        //skip transactions without lock.
        Iterator<Map.Entry<Long, TransactionMsg>> itor = transactionMsgMap.entrySet().iterator();
        while (itor.hasNext()) {
            boolean isEmptyTransaction = true;
            Map.Entry<Long, TransactionMsg> entry = itor.next();
            TransactionMsg trans = entry.getValue();
            Map<String, Map<String, LMLockMsg>> rowLocks = trans.getRowLocks();
            Map<String, LMLockMsg> tableLocks = trans.getTableLocks();
            if ((rowLocks == null || rowLocks.isEmpty()) && (tableLocks == null || tableLocks.isEmpty())) {
                isEmptyTransaction = true;
            } else {
                //rowLock
                for (Map<String, LMLockMsg> rowLocks_ : rowLocks.values()) {
                    if (!rowLocks_.isEmpty()) {
                        isEmptyTransaction = false;
                        break;
                    }
                }
                if (isEmptyTransaction) {
                    //tableLock
                    for (LMLockMsg lock_ : tableLocks.values()) {
                        if (lock_.getMaskHold() != 0) {
                            isEmptyTransaction = false;
                            break;
                        }
                    }
                }
            }
            if (isEmptyTransaction) {
                itor.remove();
            }
        }

        if (LockConstants.REMOVE_SPACE_FROM_ROWID) {
            TransactionMsg transactionMsg = null;
            for (Map.Entry<Long, TransactionMsg> entry : transactionMsgMap.entrySet()) {
                transactionMsg = entry.getValue();
                if (transactionMsg.getRowLocks() != null) {
                    LMLockMsg rowLock = null;
                    Map<String, LMLockMsg> newLockMap = new HashMap<String, LMLockMsg>();
                    for (Map<String, LMLockMsg> rowLocks : transactionMsg.getRowLocks().values()) {
                        newLockMap.clear();
                        for (Map.Entry<String, LMLockMsg> rowEntry : rowLocks.entrySet()) {
                            String rowID = rowEntry.getKey();
                            String newID = null;
                            rowLock = rowEntry.getValue();
                            if (rowID == null) {
                                continue;
                            }
                            newID = StringUtil.replaceBland(rowID);
                            rowLock.setRowID(newID);
                            rowLocks.remove(rowID);
                            newLockMap.put(newID, rowLock);
                        }
                        rowLocks.putAll(newLockMap);
                    }
                }
            }
        }
        JSONObject json = new JSONObject(transactionMsgMap);
        return json.toString();
    }

    public String getLockWaitJson() {
        if (lockWaitMsgMap == null) {
            return "{}";
        } else if (lockWaitMsgMap.isEmpty()) {
            return "{}";
        }
        //skip transactions without lock.
        Iterator<Map.Entry<Long, List<LMLockWaitMsg>>> itor = lockWaitMsgMap.entrySet().iterator();
        while (itor.hasNext()) {
            Map.Entry<Long, List<LMLockWaitMsg>> entry = itor.next();
            if (entry.getValue().isEmpty()) {
                itor.remove();
            }
        }
        if (LockConstants.REMOVE_SPACE_FROM_ROWID || LockConstants.SHOW_NON_WAITING) {
            Map<Long, List<LMLockWaitMsg>> nonWaitingMap = new ConcurrentHashMap<>();
            List<LMLockWaitMsg> lockWaitMsgs = null;
            LMLockMsg lmLockMsg = null;
            for (Map.Entry<Long, List<LMLockWaitMsg>> entry : lockWaitMsgMap.entrySet()) {
                lockWaitMsgs = entry.getValue();
                for (LMLockWaitMsg lockWaitMsg : lockWaitMsgs) {
                    lmLockMsg = lockWaitMsg.getToLock();
                    if (LockConstants.REMOVE_SPACE_FROM_ROWID) {
                        String rowID = lmLockMsg.getRowID();
                        if (rowID != null) {
                            rowID = StringUtil.replaceBland(rowID);
                            lmLockMsg.setRowID(rowID);
                        }
                    }
                    if (LockConstants.SHOW_NON_WAITING) {
                        checkNonWaiting(nonWaitingMap, lockWaitMsg);
                    }
                }
            }
            if (nonWaitingMap.size() > 0) {
                lockWaitMsgMap.putAll(nonWaitingMap);
            }
        }

        JSONObject json = new JSONObject(lockWaitMsgMap);
        return json.toString();
    }

    private void checkNonWaiting(Map<Long, List<LMLockWaitMsg>> nonWaitingMap, LMLockWaitMsg lockWaitMsg) {
        List<LMLockWaitMsg> nonWaitingList = null;
        CopyOnWriteArraySet<Long> holderTxIDs = null;
        LMLockWaitMsg nonWaitingMsg = null;
        for (Long holderTxID : lockWaitMsg.getHolderTxIDs()) {
            if (!lockWaitMsgMap.containsKey(holderTxID)) {
                nonWaitingList = nonWaitingMap.get(holderTxID);
                if (nonWaitingList == null) {
                    nonWaitingList = new ArrayList<>();
                    nonWaitingMap.put(holderTxID, nonWaitingList);
                } else {
                    continue;
                }
                nonWaitingMsg = new LMLockWaitMsg();
                nonWaitingMsg.setTxID(holderTxID);
                {
                    LMLockMsg waitLockMsg = lockWaitMsg.getToLock();
                    LMLockMsg lockmsg = new LMLockMsg();
                    lockmsg.setTableName(waitLockMsg.getTableName());
                    lockmsg.setRegionName(waitLockMsg.getRegionName());
                    lockmsg.setRowID(waitLockMsg.getRowID());
                    nonWaitingMsg.setToLock(lockmsg);
                }
                holderTxIDs = new CopyOnWriteArraySet<>();
                holderTxIDs.add(-1L);
                nonWaitingMsg.setHolderTxIDs(holderTxIDs);
                nonWaitingList.add(nonWaitingMsg);
            }
        }
    }

    public void getLockWait() {
        getLockWait(null);
    }

    public void getLockWait(Logger LOG) {
        StringBuffer result = new StringBuffer();
        if (LOG != null) {
            result.append("\nlocak wait:\n");
        }
        StringUtil.generateHeader(result, LockConstants.LOCK_WAIT_HEADER);
        if (LOG != null) {
            LOG.info(result.toString());
        } else {
            System.out.print(result.toString());
        }
        result.delete(0, result.length());
        if (lockWaitMsgMap == null || lockWaitMsgMap.size() == 0) {
            //just a header
            return;
        }
        int count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
        for (List<LMLockWaitMsg> lmLockWaitMsgs : lockWaitMsgMap.values()) {
            for (LMLockWaitMsg lmLockWaitMsg : lmLockWaitMsgs) {
                StringUtil.generateRow(result, lmLockWaitMsg);
                count--;
                if (count == 0) {
                    outPutLogs(result, LOG);
                    count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
                }
                if (LockConstants.SHOW_NON_WAITING) {
                    count = StringUtil.generateNonWaitRow(result, lockWaitMsgMap, lmLockWaitMsg, count);
                }
                if (count == 0) {
                    outPutLogs(result, LOG);
                    count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
                }
            }
        }
        if (count != 0 && count != LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME) {
            if (LOG != null) {
                LOG.info(result.toString());
            } else {
                System.out.println(result.toString());
            }
        }
    }

    private void outPutLogs(StringBuffer result, Logger LOG) {
        if (LOG != null) {
            LOG.info(result.toString());
        } else {
            System.out.println(result.toString());
        }
        result.delete(0, result.length());
        if (LOG != null) {
            result.append("\n");
        }
    }

    public void printLockInfo(Logger LOG) {
        getLockWait(LOG);
        getAllLock(LOG);
    }

    public void printDeadLock(StringBuffer result, Logger LOG) {
        StringUtil.generateHeader(result, LockConstants.LOCK_WAIT_HEADER);
        if (deadLockMsgMap == null || deadLockMsgMap.size() == 0) {
            LOG.info(result.toString());
            return;
        }
        int count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
        Set<Long> list = new HashSet<>();
        for (LMLockWaitMsg lmDeadLockMsg : deadLockMsgMap.values()) {
            StringUtil.generateRow(result, lmDeadLockMsg);
            count--;
            if (count == 0) {
                outPutLogs(result, LOG);
                count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
            }
            long txid = lmDeadLockMsg.getTxID();
            if (!list.contains(txid)) {
                list.add(txid);
            }
        }
        if (count != 0 && count != LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME) {
            LOG.info(result.toString());
        }
        if (!list.isEmpty()) {
            LockUtils.getListOfLocksInfo(new ArrayList<>(list), LOG);
        }
    }

    public void getAllLock() {
        getAllLock(null);
    }

    public void getAllLock(Logger LOG) {
        StringBuffer result = new StringBuffer();
        if (LOG != null) {
            result.append("\nlock details:\n");
        }
        StringUtil.generateHeader(result, LockConstants.LOCK_HEADER);
        if (LOG != null) {
            LOG.info(result.toString());
        } else {
            System.out.println(result.toString());
        }
        result.delete(0, result.length());
        if (transactionMsgMap == null || transactionMsgMap.size() == 0) {
            //just a header
            return;
        }
        long txID = -1;
        TransactionMsg transactionMsg = null;
        int count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
        for (Map.Entry<Long, TransactionMsg> entry : transactionMsgMap.entrySet()) {
            txID = entry.getKey();
            transactionMsg = entry.getValue();
            if (transactionMsg.getTableLocks() != null) {
                for (LMLockMsg lockMsg : transactionMsg.getTableLocks().values()) {
                    StringUtil.generateRow(result, txID, lockMsg);
                    count--;
                    if (count == 0) {
                        if (LOG != null) {
                            LOG.info(result.toString());
                        } else {
                            System.out.println(result.toString());
                        }
                        result.delete(0, result.length());
                        if (LOG != null) {
                            result.append("\n");
                        }
                        count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
                    }
                }
            }
            if (transactionMsg.getRowLocks() != null) {
                for (Map<String, LMLockMsg> rowLocks : transactionMsg.getRowLocks().values()) {
                    for (LMLockMsg rowLock : rowLocks.values()) {
                        StringUtil.generateRow(result, txID, rowLock);
                        count--;
                        if (count == 0) {
                            if (LOG != null) {
                                LOG.info(result.toString());
                            } else {
                                System.out.println(result.toString());
                            }
                            result.delete(0, result.length());
                            if (LOG != null) {
                                result.append("\n");
                            }
                            count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
                        }
                    }
                }
            }
        }
        if (count != 0 && count != LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME) {
            if (LOG != null) {
                LOG.info(result.toString());
            } else {
                System.out.println(result.toString());
            }
        }
    }

    public void getCheckTransID() {
        System.out.println("txID: ");
        System.out.println(transIDsList);
        if (commitPendingIDs != null && commitPendingIDs.size() > 0) {
            System.out.println("commit pending tx: ");
            System.out.println(commitPendingIDs);
        }
    }
}

