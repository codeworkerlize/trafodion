package org.apache.hadoop.hbase.coprocessor.transactional.lock;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.log4j.Logger;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Lock implements LockSizeof, Cloneable {

    private static Logger LOG = Logger.getLogger(Lock.class);
    
    // rowkey equals null is table lock
    public Lock() {
    }
    // row lock
    public Lock(RowKey rowKey, String regionName) {
        this.rowKey = rowKey;
        this.regionName = regionName;
    }

    // rowkey equals null is table lock
    public Lock(String regionName) {
        this.rowKey = null;
        this.regionName = regionName;
    }

    @Getter
    @Setter
    private int referenceNum = 0;

    @Getter
    @Setter
    private String regionName;

    @Getter
    @Setter
    private RowKey rowKey;

    // The number of times each lock type of the lock was successfully locked
    @Setter
    @Getter
    private long[] granted = {0, 0, 0, 0, 0, 0, 0};

    // The lock mode mask of which locks have been added to the lock
    @Setter
    @Getter
    private int maskGrant;

    @Setter
    @Getter
    private LockHolder head;

    // row lock reInit
    public void reInit(RowKey rowKey, String regionName) {
        this.rowKey = rowKey;
        this.regionName = regionName;
        this.referenceNum = 0;
        for (int i = 0; i < granted.length; i++) {
            this.granted[i] = 0;
        }
        this.maskGrant = 0;
        this.head = null;
    }

    public synchronized void increaseReferenceNum() {
        this.referenceNum += 1;
    }

    public synchronized void decreaseReferenceNum() {
        this.referenceNum -= 1;
    }


    public void removeHold(LockHolder lockHolder, Transaction transaction, String oregionName) {
        boolean hasIllegalNum = false;
        for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            granted[i] -= lockHolder.getHolding()[i];
            if (!hasIllegalNum && granted[i] < 0) {
                hasIllegalNum = true;
                Exception e = new Exception("removeHold unexpected grant");
                LOG.error("removeHold unexpected idx: " + i + ", txID: " + transaction.getTxID() + "," + transaction.hashCode() + ", regionName: " + oregionName + ",lock: " + this.toString() + ",regionName: " + regionName + ",thread:" + Thread.currentThread().getName() + ", lockHolder:" + lockHolder, e);
            }
        }
        maskGrant = LockUtils.getMask(granted);
    }

    public String toString() {
        StringBuffer message = new StringBuffer(1000);
        message.append("        maskGrant: ").append(maskGrant).append("\n");
        message.append("        granted: ").append(Arrays.toString(granted)).append("\n");
        message.append("        referenceNum: ").append(this.referenceNum).append("\n");
        if (rowKey != null) {
            message.append("        rowKey: ").append(this.rowKey).append("\n");
        }
        LockHolder curHolder = head;
        while (curHolder != null) {
            message.append(curHolder.toString());
            curHolder = curHolder.getLockNext();
        }
        return message.toString();
    }

    public void validateLock(String regionName, ConcurrentHashMap<Long, Transaction> transactionMap, String info) {
        try {
        LockHolder curHolder = head;
        long[] validateGranted = {0, 0, 0, 0, 0, 0, 0};
        if (rowKey == null) { // tableLock
            Set<Long> keySet = transactionMap.keySet();
            Iterator<Long> iterator = keySet.iterator();
            while(iterator.hasNext()) {
                Long key = iterator.next();
                Transaction value = transactionMap.get(key);
                for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
                    if (value.getTableHolder() != null) {
                        validateGranted[i] += value.getTableHolder().getHolding()[i];
                    }
                }
            }
        }
        else {  // rowLock
            while (curHolder != null) {
                for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
                    validateGranted[i] += curHolder.getHolding()[i];
                }
                curHolder = curHolder.getLockNext();
            }
        }
        boolean equals = true;
        for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            if (validateGranted[i] != granted[i]) {
                equals = false;
                LOG.error("lockDebug granted[] not equals" + " granted[" + i + "] = " + granted[i] + " validateGranted[" + i + "] = " + validateGranted[i]);
            }
        }
        if (LockUtils.getMask(validateGranted) != maskGrant) {
            equals = false;
            LOG.error("lockDebug maskGrant not equals maskGrant: " + maskGrant + " validatemaskGrant: " + LockUtils.getMask(validateGranted));
        }
        if (!equals) {
            curHolder = head;
            StringBuffer log = new StringBuffer("lockDebug info: regionName : " + regionName + " rowkey : " + (rowKey != null ? rowKey : "null") + " " + info);
            if (rowKey == null) {
                Set<Long> keySet = transactionMap.keySet();
                Iterator<Long> iterator = keySet.iterator();
                while(iterator.hasNext()) {
                    Long key = iterator.next();
                    Transaction value = transactionMap.get(key);
                    log.append(" txid: " + curHolder.getTransaction().getTxID());
                    for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
                        log.append(" ," + value.getTableHolder().getHolding()[i]);
                    }
                }
            }
            else {
                while (curHolder != null) {
                    log.append(" txid: " + curHolder.getTransaction().getTxID());
                    for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
                        log.append(" ," + curHolder.getHolding()[i]);
                    }
                    curHolder = curHolder.getLockNext();
                }
            }
            LOG.error(log);
        }
        } catch (Exception e) {
           // LOG.error("failed to validateAllLocks", e);
        }

    }

    @Override
    public long sizeof() {
        long size = Size_Object;
        //header
        // 6 attributes
        size += (6 * Size_Reference + Size_Object);
        // 2 int
        size += 8;
        //granted
        size += (Size_Array + 8 * granted.length);
        if (rowKey != null) {
            size += rowKey.sizeof();
        }
        return LockUtils.alignLockObjectSize(size);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
