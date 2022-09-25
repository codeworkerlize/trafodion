package org.apache.hadoop.hbase.coprocessor.transactional.lock.base;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RowKey;

import java.util.List;


public class LockObject {
    public static final int LOCK_ACQUIRE = 0;
    public static final int LOCK_RELEASE = 1;
    public static final int LOCK_ACQUIRE_AND_RELEASE = 2;
    @Getter
    @Setter
    private long txID;
    @Getter
    @Setter
    private long svptID = -1;
    @Getter
    @Setter
    private long parentSvptID = -1;
    @Getter
    @Setter
    private String table;
    @Getter
    @Setter
    private RowKey rowID;
    @Getter
    @Setter
    private int lockMode;

    /**
     * true: 申请锁
     * false: 释放锁
     */
    @Setter
    @Getter
    private boolean isAcquire = true;
    /**
     * 0 acquire
     * 1 release
     * 2 acquire & release
     */
    @Getter
    @Setter
    private int type = LOCK_ACQUIRE;

    @Getter
    @Setter
    private int timeout = 3000;

    private transient int state = 2;
    @Getter
    @Setter
    private int lockClientRetryTimes = 20;
    @Getter
    @Setter
    private boolean implicitSavepoint = false;
    @Getter
    @Setter
    private List<Long> list;

    public synchronized void addState() {
        this.state += 1;
    }

    public synchronized int getState() {
        return this.state;
    }

    public synchronized void setState(int state) {
        this.state = state;
    }

    public LockObject() {}

    public LockObject(long txID, String table, String rowID, int lockMode) {
        this.txID = txID;
        this.table = table;
        if (rowID != null) {
            try {
                this.rowID = new RowKey(rowID);
            } catch (Exception e) {
                this.rowID = new RowKey(rowID.getBytes());
            }
        }
        this.lockMode = lockMode;
    }

    public LockObject(long txID, long svptID, String table, String rowID, int lockMode) {
        this.txID = txID;
        this.svptID = svptID;
        this.table = table;
        if (rowID != null) {
            try {
                this.rowID = new RowKey(rowID);
            } catch (Exception e) {
                this.rowID = new RowKey(rowID.getBytes());
            }
        }
        this.lockMode = lockMode;
    }

    public String toString() {
        return "txID: " + this.txID + (svptID > 0 ?  (", svptID: " + svptID) : "") + (parentSvptID > 0 ?  (", parentSvptID: " + parentSvptID) : "") + ", table: " + this.table + (this.rowID == null ? "" : (", rowID: " + this.rowID)) + ", lockMode: " + this.lockMode;
    }

    public LockObject clone() {
        LockObject lockObject = new LockObject();
        lockObject.setTxID(this.txID);
        lockObject.setSvptID(this.svptID);
        lockObject.setTable(this.table);
        lockObject.setRowID(this.rowID);
        lockObject.setLockMode(this.lockMode);
        lockObject.setTimeout(this.timeout);
        lockObject.setAcquire(this.isAcquire);
        lockObject.setType(this.type);

        return lockObject;
    }
}