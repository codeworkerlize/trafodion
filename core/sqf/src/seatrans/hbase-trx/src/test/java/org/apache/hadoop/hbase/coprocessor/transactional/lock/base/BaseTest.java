package org.apache.hadoop.hbase.coprocessor.transactional.lock.base;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.*;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.*;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.JVM)
public class BaseTest {
    private static int CHECK_TABLE_LOCK = 1;
    private static int CHECK_ROW_LOCK = 2;
    private static RSServer rsServer = null;

    protected LockManager lockManager;
    protected LockObject lockObject;
    protected LockObject lockObject1;

    protected void init() {
        LockConstants.ENABLE_ROW_LEVEL_LOCK = true;
        LockConstants.TRACE_LOCK_APPLIED = true;
        if (rsServer == null) {
            rsServer = RSServer.getInstance();
        }
        if (rsServer == null) {
            rsServer = RSServer.getInstance(null, null);
            rsServer.enableRowLevelLockFeature();
        }
    }

    protected boolean flushSvptToTx(LockManager lockManager, LockObject lockObject) {
        return lockManager.flushSvptToTx(lockObject.getTxID(), lockObject.getSvptID());
    }

    protected boolean acquireRowLock(LockManager lockManager, LockObject lockObject, int timeout) {
        try {
            RetCode retCode = lockManager.lockAcquire(lockObject.getTxID(), lockObject.getSvptID(), lockObject.getParentSvptID(), lockObject.getRowID(), lockObject.getLockMode(), timeout, lockObject.isImplicitSavepoint(), null);
            return ((retCode == RetCode.OK) || (retCode == RetCode.OK_LOCKED));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    protected void releaseRowLock(LockManager lockManager, LockObject lockObject) {
        boolean result = false;
        try {
            result = lockManager.lockRelease(lockObject.getTxID(), lockObject.getSvptID(), lockObject.getRowID(), lockObject.getLockMode());
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(result);
    }

    protected void releaseTableLock(LockManager lockManager, LockObject lockObject) {
        boolean result = false;
        try {
            result = lockManager.lockRelease(lockObject.getTxID(), lockObject.getSvptID(), lockObject.getLockMode());
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(result);
    }

    protected boolean acquireTableLock(LockManager lockManager, LockObject lockObject, int timeout) {
        try {
            RetCode retCode = lockManager.lockAcquire(lockObject.getTxID(), lockObject.getSvptID(), lockObject.getParentSvptID(), lockObject.getLockMode(), timeout, lockObject.isImplicitSavepoint(), null);
            return ((retCode == RetCode.OK) || (retCode == RetCode.OK_LOCKED));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    protected void releaseAll(LockManager lockManager, LockObject lockObject) {
        boolean res = lockManager.lockReleaseAll(lockObject.getTxID());
        assertTrue(res);
    }

    protected void commitSavepoint(LockManager lockManager, LockObject lockObject) {
        if (lockObject.getSvptID() > 0) {
            boolean res = lockManager.flushSvptToTx(lockObject.getTxID(), lockObject.getSvptID());
            assertTrue(res);
        } else {
            // unexpected
            assertTrue(false);
        }
    }

    protected void releaseSavepoint(LockManager lockManager, LockObject lockObject) {
        boolean res = lockManager.lockReleaseAll(lockObject.getTxID(), lockObject.getSvptID());
        assertTrue(res);
    }

    protected boolean checkTxLockNum(LockObject lockObject, int expectedLockNum) {
        LMLockStatisticsMessage responseMsg = getLockStatisticsMsg(lockObject);
        if (responseMsg == null && expectedLockNum == 0) {
            return true;
        }
        ConcurrentHashMap<Long, LMLockStaticisticsData> txLockNumMap = responseMsg.getTxLockNumMap();

        if (txLockNumMap == null || txLockNumMap.size() == 0) {
            if (expectedLockNum == 0) {
                return true;
            } else {
                return false;
            }
        }
        LMLockStaticisticsData lmLSD = txLockNumMap.get(lockObject.getTxID());
        if (lmLSD == null) {
            return false;
        }
        if (lmLSD.getLockNum() == expectedLockNum) {
            return true;
        }
        return false;
    }

    protected boolean checkTableLockModes(LockObject lockObject, int expectedLockMode) {
        return checkTableLockModes(lockObject.getTxID(), lockObject.getSvptID(), expectedLockMode);
    }

    protected boolean checkRowLockModes(LockObject lockObject, int expectedLockMode) {
        if (expectedLockMode == LockMode.LOCK_NO) {
            return true;
        }
        int intentionLockMode = getIntentionLockMode(expectedLockMode);
        if (!checkTableLockModes(lockObject, intentionLockMode)) {
            return false;
        }
        return checkRowLockModes(lockObject.getTxID(), lockObject.getSvptID(), expectedLockMode);
    }

    protected boolean checkTableLockModes(Long txID, Long svptID, int expectedLockMode) {
        return checkLockModes(txID, svptID, expectedLockMode, CHECK_TABLE_LOCK);
    }

    protected boolean checkRowLockModes(Long txID, Long svptID, int expectedLockMode) {
        return checkLockModes(txID, svptID, expectedLockMode, CHECK_ROW_LOCK);
    }

    protected boolean checkLockWait(Long txID, int expectedLockMode) {
        List<Long> txIDs = new ArrayList<>();
        txIDs.add(txID);
        LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(RSMessage.MSG_TYP_LOCK_INFO);
        lmLockInfoReqMessage.setType(LMLockInfoMessage.LOCK_WAIT);
        lmLockInfoReqMessage.setTxIDs(txIDs);
        try {
            LMLockInfoMessage lmLockInfoMessage = rsServer.processLockInfo(lmLockInfoReqMessage, null);
            if (lmLockInfoMessage == null) {
                return false;
            }
            Map<Long, List<LMLockWaitMsg>> lockWaitMsgMap = lmLockInfoMessage.getLockWaitMsgMap();
            if (lockWaitMsgMap == null || lockWaitMsgMap.size() == 0) {
                return false;
            }
            for (List<LMLockWaitMsg> lmLockWaitMsgs : lockWaitMsgMap.values()) {
                for (LMLockWaitMsg lmLockWaitMsg : lmLockWaitMsgs) {
                    if (containLockMode(lmLockWaitMsg, expectedLockMode)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }

    protected boolean checkLockApply(int expectedApplyNum) {
        LMAppliedLockReqMessage lmAppliedLockReqMessage = (LMAppliedLockReqMessage) RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_MSG);
        lmAppliedLockReqMessage.setDebug(0);
        try {
            LMAppliedLockMessage lmAppliedLockMessage = rsServer.processGetAppliedList(lmAppliedLockReqMessage, null);
            Map<String, LMAppliedLockInfoInPerServer> serverList = lmAppliedLockMessage.getServerList();
            if (serverList == null || serverList.size() == 0) {
                if (expectedApplyNum == 0) {
                    return true;
                } else {
                    return false;
                }
            }
            int realApplyNum = 0;
            for (LMAppliedLockInfoInPerServer lmAppliedLockInfoInPerServer : serverList.values()) {
                if (lmAppliedLockInfoInPerServer.appliedList != null && lmAppliedLockInfoInPerServer.sumLockNum > expectedApplyNum) {
                    return false;
                }
                realApplyNum += lmAppliedLockInfoInPerServer.sumLockNum;
            }
            boolean ret = (realApplyNum == expectedApplyNum);
            if (ret) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    protected int getIntentionLockMode(int dwLockModes) {
        int tableLockModes = LockMode.LOCK_NO;
        if ((dwLockModes & LockMode.LOCK_X) == LockMode.LOCK_X || (dwLockModes & LockMode.LOCK_U) == LockMode.LOCK_U) {
            tableLockModes = LockMode.LOCK_IX;
        } else if ((dwLockModes & LockMode.LOCK_S) == LockMode.LOCK_S) {
            tableLockModes = LockMode.LOCK_IS;
        }
        return tableLockModes;
    }

    private boolean checkLockModes(Long txID, Long svptID, int expectedLockMode, int checkLockType) {
        if (expectedLockMode == LockMode.LOCK_NO) {
            return true;
        }
        List<Long> txIDs = new ArrayList<>();
        txIDs.add(txID);
        LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(RSMessage.MSG_TYP_LOCK_INFO);
        lmLockInfoReqMessage.setType(LMLockInfoMessage.LOCK_INFO);
        lmLockInfoReqMessage.setTxIDs(txIDs);
        try {
            LMLockInfoMessage lmLockInfoMessage = rsServer.processLockInfo(lmLockInfoReqMessage, null);
            if (lmLockInfoMessage == null) {
                return false;
            }
            Map<Long, TransactionMsg> transactionMsgMap = lmLockInfoMessage.getTransactionMsgMap();
            if (transactionMsgMap == null || transactionMsgMap.size() == 0) {
                return false;
            }
            TransactionMsg transactionMsg = transactionMsgMap.get(txID);
            if (transactionMsg == null) {
                return false;
            }
            if (checkLockType == CHECK_TABLE_LOCK) {
                for (LMLockMsg lockMsg : transactionMsg.getTableLocks().values()) {
                    if (containLockMode(lockMsg, svptID, expectedLockMode)) {
                        return true;
                    }
                }
            } else {
                for (Map<String, LMLockMsg> rowLocks : transactionMsg.getRowLocks().values()) {
                    for (LMLockMsg rowLock : rowLocks.values()) {
                        if (containLockMode(rowLock, svptID, expectedLockMode)) {
                            return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }

    private boolean containLockMode(LMLockMsg lockMsg, long svptID, int expectedLockMode) {
        if (lockMsg == null) {
            return false;
        }
        int idx = LockMode.getIndex(expectedLockMode);
        long[] holding = lockMsg.getHolding();
        if (holding[idx] > 0) {
            return true;
        }
        if (svptID > 0) {
            holding = lockMsg.getSvptHoldings().get(svptID);
            if (holding != null) {
                if (holding[idx] > 0) {
                    return true;
                }
            }
            Long parentSvptID = lockMsg.getSvptRelation().get(svptID);
            while (parentSvptID != null) {
            	holding = lockMsg.getSvptHoldings().get(parentSvptID);
                if (holding != null) {
                    if (holding[idx] > 0) {
                        return true;
                    }
                }
                parentSvptID = lockMsg.getSvptRelation().get(parentSvptID);
            }
            holding = lockMsg.getImplicitSvptHoldings().get(svptID);
            if (holding != null) {
                if (holding[idx] > 0) {
                    return true;
                }
            }
            holding = lockMsg.getSubImplicitSvptHoldings().get(svptID);
            if (holding != null) {
                if (holding[idx] > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean containLockMode(LMLockWaitMsg lockWaitMsg, int expectedLockMode) {
        if (lockWaitMsg == null) {
            return false;
        }
        int idx = LockMode.getIndex(expectedLockMode);
        LMLockMsg lmLockMsg = lockWaitMsg.getToLock();
        if (lmLockMsg == null) {
            return false;
        }
        long[] holding = lmLockMsg.getHolding();
        if (holding[idx] > 0) {
            return true;
        }
        return false;
    }

    protected String getLockModeName(int lockMode) {
        switch (lockMode) {
            case LockMode.LOCK_IS:
                return "IS";
            case LockMode.LOCK_S:
                return "S";
            case LockMode.LOCK_IX:
                return "IX";
            case LockMode.LOCK_U:
                return "U";
            case LockMode.LOCK_X:
                return "X";
            case LockMode.LOCK_RS:
                return "RS";
            case LockMode.LOCK_RX:
                return "RX";
        }
        return "unexpected lockMode";
    }

    private LMLockStatisticsMessage getLockStatisticsMsg(LockObject lockObject) {
        LMLockStatisticsReqMessage lmLockStatisticsReqMessage = (LMLockStatisticsReqMessage) RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_STATISTICS_INFO);
        lmLockStatisticsReqMessage.setDebug(0);
        lmLockStatisticsReqMessage.setTopN(0);
        lmLockStatisticsReqMessage.setTableNames(null);
        List<Long> txIDs = new ArrayList<>();
        txIDs.add(lockObject.getTxID());
        lmLockStatisticsReqMessage.setTxIDs(txIDs);
        LMLockStatisticsMessage responseMsg = null;
        try {
            responseMsg = rsServer.processLockStatistics(lmLockStatisticsReqMessage, null);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        if (responseMsg == null) {
            return null;
        }
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_STATISTICS_INFO_RESPONSE) {
            return null;
        }

        if (responseMsg.isErrorFlag()) {
            return null;
        }

        return responseMsg;
    }
}
