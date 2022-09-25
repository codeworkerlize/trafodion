package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionMsg implements Serializable {
    private static final long serialVersionUID = 1L;
    // row locks
    @Getter
    @Setter
    Map<String, Map<String, LMLockMsg>> rowLocks = new ConcurrentHashMap<>();
    // table locks
    @Getter
    @Setter
    Map<String, LMLockMsg> tableLocks = new ConcurrentHashMap<>();
    
    @Getter
    @Setter
    private String queryContext = null; 

    public String toString() {
        return "tableLocks: " + tableLocks +
                "\n        rowLocks: " + rowLocks + "\nQueryContext: " + (queryContext==null?"":queryContext);
    }

    public void merge(TransactionMsg transactionMsg) {
        Map<String, LMLockMsg> currentRowLocks = null;
        Map<String, LMLockMsg> tempRowLocks = null;

        if (rowLocks != null) {
            if (transactionMsg.getRowLocks() != null) {
                for (Map.Entry<String, Map<String, LMLockMsg>> entry : transactionMsg.getRowLocks().entrySet()) {
                    currentRowLocks = rowLocks.get(entry.getKey());
                    tempRowLocks = entry.getValue();
                    if (currentRowLocks == null) {
                        rowLocks.put(entry.getKey(), tempRowLocks);
                    } else {
                        mergeLocks(currentRowLocks, tempRowLocks);
                    }
                }
            }
        } else {
            if (transactionMsg.getRowLocks() != null) {
                rowLocks = transactionMsg.getRowLocks();
            }
        }
        if (tableLocks != null) {
            if (transactionMsg.getTableLocks() != null) {
                mergeLocks(tableLocks, transactionMsg.getTableLocks());
            }
        } else {
            if (transactionMsg.getTableLocks() != null) {
                tableLocks = transactionMsg.getTableLocks();
            }
        }
    }

    private void mergeLocks(Map<String, LMLockMsg> localLocks, Map<String, LMLockMsg> remoteLocks) {
        LMLockMsg currentLock = null;
        LMLockMsg tempLock = null;

        for (Map.Entry<String, LMLockMsg> entry : remoteLocks.entrySet()) {
            currentLock = localLocks.get(entry.getKey());
            tempLock = entry.getValue();
            if (currentLock == null) {
                localLocks.put(entry.getKey(), tempLock);
            } else {
                currentLock.merge(tempLock);
            }
        }
    }

}