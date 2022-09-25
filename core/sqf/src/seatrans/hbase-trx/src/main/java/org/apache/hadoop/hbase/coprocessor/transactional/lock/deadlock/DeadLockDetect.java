package org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.Transaction;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockLogLevel;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMDeadLockDetectMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.message.RSMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMVictimMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockInfoReqMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockInfoMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSConnection;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

public class DeadLockDetect {
    private static Logger LOG = Logger.getLogger(DeadLockDetect.class);

    private RSServer rsServer;

    public DeadLockDetect(RSServer rsServer) {
        this.rsServer = rsServer;
    }

    public long localDeadLockDetect(String regionName, Long txID) {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter localDeadLockDetect " + txID + "," + regionName);
        }
        LockWait lockWait = rsServer.getLockManagers().get(regionName).getLockWait(txID);
        if (lockWait == null) {
            return -1;
        }
        List<Integer> scannedLockWaits = new ArrayList<>();
        Stack<LockWait> stackWait = new Stack<>();
        stackWait.push(lockWait);

        do {
            if (lockWait.getHolderTxIDs().contains(txID)) {
                try {
                    StringBuffer waitBuf = new StringBuffer(3000);
                    waitBuf.append(" local originTxID: ").append(txID).append(" ").append(stackWait.size()).append("\n");
                    printWaitInfo(waitBuf, stackWait);
                } catch (Exception e) {
                    LOG.error("failed to print local deadlocks: ", e);
                }
                try {
                    Set<Long> txIDSet = new HashSet<>();
                    for (LockWait tmpLockWait : stackWait) {
                        txIDSet.add(tmpLockWait.getTxID());
                    }
                    LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE);
                    lmLockInfoReqMessage.setType(LMLockInfoMessage.LOCK_AND_WAIT);
                    lmLockInfoReqMessage.setTxIDs(new ArrayList(txIDSet));
                    LMLockInfoMessage lmLockInfoMessage = rsServer.processSingleNodeLockInfo(lmLockInfoReqMessage, null);
                    lmLockInfoMessage.printLockInfo(LOG);
                } catch (Exception e) {
                    LOG.error("failed to print distributed deadlocks: ", e);
                }

                long vicTimTxID = getDeadLockVictim(stackWait);
                LOG.info("localDeadLockDetect victim: " + vicTimTxID);
                return vicTimTxID;
            }
            scannedLockWaits.add(lockWait.hashCode());
            lockWait = findHolderLockWait(lockWait, scannedLockWaits);

            if (lockWait != null) {
                stackWait.push(lockWait);
            } else {
                while (!stackWait.isEmpty() && (lockWait = stackWait.pop()) != null) {
                    if (!lockWait.getHolderTxIDs().contains(txID) && (lockWait = findHolderLockWait(lockWait, scannedLockWaits)) != null) {
                        stackWait.push(lockWait);
                        break;
                    }
                }
            }
        } while (lockWait != null);

        return -1;
    }

    private LockWait findHolderLockWait(LockWait lockWait, List scannedLockWaits) {
        for (Long detectTxID : lockWait.getHolderTxIDs()) {
            LockWait tmpLockWait = getLockWaitByTxID(detectTxID);
            if (tmpLockWait != null && !scannedLockWaits.contains(tmpLockWait.hashCode())) {
                return tmpLockWait;
            }
        }
        return null;
    }

    public boolean distributedDeadLockDetect(LockWait lockWait, String originalRegionServer, Long originalWaitingTxID, List<Long> detectPath) {
        if (lockWait == null) {
            return true;
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter distributedDeadLockDetect " + originalRegionServer + "," + lockWait.getTxID());
        }
        //prepareWaitRelations();
        Set<String> trxRegionServers = rsServer.listAllRegionServersWithoutLocal();
        if (trxRegionServers.size() == 0) {
            return true;
        }

        try {
            boolean isOriginatingDetect = (detectPath.size() == 0);
            LMDeadLockDetectMessage lmDeadLockDetectMessage = (LMDeadLockDetectMessage) RSMessage.getMessage(RSMessage.MSG_TYP_DEADLOCK_DETECT);
            lmDeadLockDetectMessage.setOriginalWaitingTxID(originalWaitingTxID);
            lmDeadLockDetectMessage.setWatingTxID(lockWait.getTxID());
            lmDeadLockDetectMessage.setHolderTxIDs(lockWait.getHolderTxIDs());
            detectPath.add(lockWait.getTxID());
            lmDeadLockDetectMessage.getDetectPath().addAll(detectPath);
            lmDeadLockDetectMessage.setOriginalRegionServer(originalRegionServer);
            lmDeadLockDetectMessage.setSenderRegionServer(rsServer.getServerName());
            lmDeadLockDetectMessage.setVictimTxRegionServers(rsServer.listAllRegionServers());

            for (String trxRegionServer : trxRegionServers) {
                lmDeadLockDetectMessage.setReceiverRegionServer(trxRegionServer);
                rsServer.sendLMMessage(trxRegionServer, lmDeadLockDetectMessage);
                if (LockLogLevel.enableTraceLevel) {
                    LOG.info("sendAndReceive DetectMessage success: " +  originalRegionServer + "," + lockWait.getTxID() + "," + trxRegionServer);
                }
            }
            if (!isOriginatingDetect) {
                rsServer.processRSMessage(lmDeadLockDetectMessage, null);
            }
        } catch (Exception e) {
            LOG.error("failed to check distributed DeadLock Cycle" + originalRegionServer + "," + lockWait.getTxID(), e);
            if (LockLogLevel.enableTraceLevel) {
                LOG.info("exit distributedDeadLockDetect false " + originalRegionServer + "," + lockWait.getTxID());
            }
            return false;
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit distributedDeadLockDetect " + originalRegionServer + "," + lockWait.getTxID());
        }
        return true;
    }

    public void processDeadLockDetect(LMDeadLockDetectMessage lmMessage, RSConnection lmConn) {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter processDeadLockDetect, deadLockMessage: " + lmMessage + " " + lmConn);
        }
        if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            return;
        }
        //LOG.info("processDeadLockDetect, deadLockMessage: " + lmMessage.getHolderTxID() + ", originalWaitingTxID:" + lmMessage.getOriginalWaitingTxID());
        List<LockWait> lockWaits = getLockWaitByTxIDs(lmMessage.getHolderTxIDs());
        if (lockWaits.size() != 0) {
            // valid detect
            if (lmMessage.getOriginalRegionServer().equals(rsServer.getServerName()) && lmMessage.getHolderTxIDs().contains(lmMessage.getOriginalWaitingTxID())) {
                // deadlock detect message back to original region server, there is a dead lock cycle
                //LOG.info("processDeadLockDetect holderTxID: " + lmMessage.getHolderTxID() + ", originalWaitingTxID:" + lmMessage.getOriginalWaitingTxID());

                try {
                    LMVictimMessage lmVictimMessage = (LMVictimMessage) RSMessage.getMessage(RSMessage.MSG_TYP_DEADLOCK_VICTIM);
                    lmVictimMessage.setVictimTxID(Collections.max(lmMessage.getDetectPath()));
                    lmVictimMessage.getDetectPath().addAll(lmMessage.getDetectPath());
                    for (String victimTxRegionServer : lmMessage.getVictimTxRegionServers()) {
                        if (victimTxRegionServer.equals(rsServer.getServerName())) {
                            processDeadLockVictim(lmVictimMessage, null);
                        } else {
                            if (LockLogLevel.enableTraceLevel) {
                                LOG.info("send victim message, victimTxID: " + lmVictimMessage.getVictimTxID());
                            }
                            rsServer.sendLMMessage(victimTxRegionServer, lmVictimMessage);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("failed to check distributed DeadLock Cycle", e);
                }
            } else {
                // find the wait relationship in current Region Server and forward deadlock detect message
                for (LockWait lockWait : lockWaits) {
                    distributedDeadLockDetect(lockWait, lmMessage.getOriginalRegionServer(), lmMessage.getOriginalWaitingTxID(), lmMessage.getDetectPath());
                }
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit processDeadLockDetect, deadLockMessage: " + lmMessage + " " + lmConn);
        }
    }

    public void processDeadLockVictim(LMVictimMessage lmMessage, RSConnection lmConn) {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("enter receive victim message, " + (lmConn == null ? "local" : lmConn) + ", victimTxID: " + lmMessage.getVictimTxID() + ", thread: " + Thread.currentThread().getName());
        }
        if (!LockConstants.ENABLE_ROW_LEVEL_LOCK) {
            return;
        }
        LockManager lockManager = getLockManagerByTxID(lmMessage.getVictimTxID());
        LockWait lockWait = null;
        if (lockManager != null) {
            lockWait = lockManager.getLockWait(lmMessage.getVictimTxID());
        }

        if (lockWait != null) {
            synchronized (lockWait.getLock()) {
                if (lockWait.getWaitStatus() == LockWaitStatus.CANCEL_FOR_DEADLOCK) {
                    return;
                } else {
                    lockWait.setWaitStatus(LockWaitStatus.CANCEL_FOR_DEADLOCK);
                }
            }
            try {
                LOG.info("distributed processDeadLockVictim, get distributed deadlocks" + ", thread: " + Thread.currentThread().getName());
                LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(RSMessage.MSG_TYP_LOCK_INFO);
                lmLockInfoReqMessage.setType(LMLockInfoMessage.DEAD_LOCK);
                lmLockInfoReqMessage.setTxIDs(lmMessage.getDetectPath());
                LMLockInfoMessage lmLockInfoMessage = rsServer.processLockInfo(lmLockInfoReqMessage, null);
                StringBuffer deadLockMsg = new StringBuffer("\ndistributed deadlocks: \n");
                lmLockInfoMessage.printDeadLock(deadLockMsg, LOG);
            } catch (Exception e) {
                LOG.error("failed to print distributed deadlocks: ", e);
            }

            if (LockLogLevel.enableTraceLevel) {
                LOG.info("distributed processDeadLockVictim, victimNode: " + lockWait.getTxID() + "," + lockWait.getLock().hashCode() + " notify" + ", thread: " + Thread.currentThread().getName());
            }
            synchronized (lockWait.getLock()) {
                lockWait.setWaitStatus(LockWaitStatus.CANCEL_FOR_DEADLOCK);
                try {
                    lockManager.logNotify(-1, lockWait.getLock());
                    lockWait.getLock().notifyAll();
                } catch (Exception e) {
                    LOG.error("failed to notify", e);
                }
            }
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("exit receive victim message, " + (lmConn == null ? "local" : lmConn) + ", victimTxID: " + lmMessage.getVictimTxID() + ", thread: " + Thread.currentThread().getName());
        }
    }

    private LockWait getLockWaitByTxID(long txID) {
        LockWait lockWait = null;
        for (LockManager lockManager : rsServer.getLockManagers().values()) {
            lockWait = lockManager.getLockWait(txID);
            if (lockWait != null) {
                return lockWait;
            }
        }
        return null;
    }

    private List<LockWait> getLockWaitByTxIDs(CopyOnWriteArraySet<Long> txIDs) {
        List<LockWait> lockWaits = new ArrayList<>();
        LockWait lockWait = null;
        for (Long txID : txIDs) {
            lockWait = getLockWaitByTxID(txID);
            if (lockWait != null) {
                lockWaits.add(lockWait);
            }
        }
        return lockWaits;
    }

    private LockManager getLockManagerByTxID(long txID) {
        LockWait lockWait = null;
        for (LockManager lockManager : rsServer.getLockManagers().values()) {
            lockWait = lockManager.getWaitGraph().get(txID);
            if (lockWait != null) {
                return lockManager;
            }
        }
        return null;
    }

    private long getDeadLockVictim(Stack<LockWait> stackWait) {
        LockWait victimWait = null;
        Transaction waitTransaction = null;
        int currentRowHolderNum = 0;
        long victimTxID = -1;
        Collections.sort(stackWait);
        int waitRowHolderNum = 0;
        for (int i = 0; i < stackWait.size(); i++) {
            victimWait = stackWait.get(i);
            if (victimWait == null) {
                continue;
            }
            waitTransaction = victimWait.getTransaction();
            waitRowHolderNum = waitTransaction.getLockNumber();
            if (currentRowHolderNum == 0 || currentRowHolderNum > waitRowHolderNum ||
                    (currentRowHolderNum == waitRowHolderNum && victimTxID < waitTransaction.getTxID())) {
                currentRowHolderNum = waitRowHolderNum;
                victimTxID = waitTransaction.getTxID();
            }
        }
        return victimTxID;
    }

    private void printWaitInfo(StringBuffer waitBuf, Stack<LockWait> allLockWaits) {
        for (LockWait lockWait : allLockWaits) {
            generateRow(waitBuf, lockWait);
        }
        LOG.info("\nlocal deadlocks: \n" + waitBuf.toString());
    }

    private void generateRow(StringBuffer result, LockWait lockWait) {
        String rowKey = null;
        if (lockWait.getLock() != null && lockWait.getLock().getRowKey() != null) {
            rowKey = lockWait.getLock().getRowKey().toString();
        } else {
            rowKey = "";
        }

        result.append("txID:").append(lockWait.getTxID());
        result.append(" ,regionName: ").append(lockWait.getRegionName());
        result.append(" ,rowKey:").append(rowKey);
        result.append(" ,holding:").append(StringUtil.getLockMode(lockWait.getHolding()));
        result.append(" ,holderTxIDs:").append((lockWait.getHolderTxIDs() == null ? "" : lockWait.getHolderTxIDs().toString()));
        Transaction trx = lockWait.getTransaction();
        String queryContext = (trx == null ? null : trx.getQueryContext());
        result.append(" ,QueryContext:").append((queryContext == null ? "" : queryContext));
        result.append("\n");
        /*List<StringBuffer> rows = new ArrayList<>();
        StringUtil.formatOutput(rows, 1, String.valueOf(lockWait.getTxID()));
        StringUtil.formatOutputWide(rows, 2, lockWait.getRegionName());
        String rowKey = null;
        if (lockWait.getLock() != null && lockWait.getLock().getRowKey() != null) {
            rowKey = lockWait.getLock().getRowKey().toString();
        } else {
            rowKey = "";
        }
        StringUtil.formatOutput(rows, 3, rowKey); 
        StringUtil.formatOutput(rows, 4, StringUtil.getLockMode(lockWait.getHolding()));
        StringUtil.formatOutput(rows, 5, (lockWait.getHolderTxIDs() == null ? "" : lockWait.getHolderTxIDs().toString()));
        for (StringBuffer row : rows) {
            result.append(row).append("\n");
        }*/
    }
}
