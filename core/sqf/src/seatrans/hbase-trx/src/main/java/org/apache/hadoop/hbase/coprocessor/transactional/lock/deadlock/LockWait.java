package org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.Lock;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RowKey;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.Transaction;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;

import java.util.concurrent.CopyOnWriteArraySet;

public class LockWait implements Comparable<LockWait>, LockSizeof, Cloneable {
    public LockWait(Lock lock, Transaction transaction, String regionName, long svptID, long parentSvptID, boolean implicitSavepoint) {
        this.lock = lock;
        this.transaction = transaction;
        this.txID = transaction.getTxID();
        this.regionName = regionName;
        this.tableName = regionName.substring(0,regionName.indexOf(','));
        this.rowKey = lock.getRowKey();
        this.svptID = svptID;
        this.parentSvptID = parentSvptID;
        this.implicitSavepoint = implicitSavepoint;
    }
    @Getter
    private Long txID = -1L;
    @Getter
    private long svptID = -1;
    @Getter
    private long parentSvptID = -1;
    @Getter
    private boolean implicitSavepoint = false;
    @Getter
    private String regionName;
    @Getter
    private String tableName;
    @Getter
    private RowKey rowKey;
    @Getter
    @Setter
    private long[] holding = {0, 0, 0, 0, 0, 0, 0};
    @Setter
    @Getter
    private int maskHold;
    @Getter
    @Setter
    private CopyOnWriteArraySet<Long> holderTxIDs = new CopyOnWriteArraySet<>();
    @Getter
    @Setter
    private Lock lock;
    @Getter
    private Transaction transaction;
    @Getter
    @Setter
    private LockWaitStatus waitStatus = LockWaitStatus.WAITING;

    @Getter
    @Setter
    private int clientRetryCount = LockConstants.LOCK_CLIENT_RETRIES_TIMES;

    public long getCreateTimestamp() {
        synchronized (this) {
            return createTimestamp;
        }
    }

    public void setCreateTimestamp(long val) {
        synchronized (this) {
            createTimestamp = val;
        }
    }
    private long createTimestamp;

    @Getter
    @Setter
    private long durableTime;

    public void updateClientRetryCount(){
        this.clientRetryCount --;
    }

    @Override
    public int compareTo(LockWait o) {
        if (this.txID == o.getTxID()) {
            return 0;
        } else if (this.txID > o.getTxID()) {
            return 1;
        }
        return -1;
    }

    @Override
    public long sizeof() {
        long size = Size_Object;
        //10 attributes
        size += 10 * Size_Reference;
        //1 Set
        size += Size_CopyOnWriteArraySet;
        //1 Long
        size += Size_Long;
        //1 long svptID
        size += 8;
        //1 boolean implicitSavepoint
        size += 8;
        //2 int + 1 enum
        size += 12;
        //holding
        size += Size_Array + 8 * holding.length;
        //holderTxIDs
        synchronized (this) {
            size += (Size_Long + Size_Reference) * holderTxIDs.size();
        }
        //tableName
        size += Size_String + Size_Array + tableName.length() * Size_Char;
        return LockUtils.alignLockObjectSize(size);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        StringBuffer message = new StringBuffer(100);
        message.append("txID: ").append(txID);
        message.append(",svptID: ").append(svptID);
        message.append(",parentSvptID: ").append(parentSvptID);
        message.append(",implicitSavepoint: ").append(implicitSavepoint);
        message.append(",lockMode: ").append(maskHold);
        message.append(",holderTxID: ").append(holderTxIDs);
        message.append(",waitStatus: ").append(waitStatus.name());
        message.append(",regionName: ").append(regionName);
        if (rowKey != null) {
            message.append(",rowKey: ").append(rowKey);
        }

        return message.toString();
    }
}
