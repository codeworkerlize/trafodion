package org.apache.hadoop.hbase.coprocessor.transactional.lock;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.log4j.Logger;

public class Transaction implements LockSizeof {

    private static Logger LOG = Logger.getLogger(Transaction.class);
    // transaction ID
    @Setter
    @Getter
    private long txID;

    private int referenceNum = 0;

    @Getter
    @Setter
    private boolean removedFlag = false;

    /**
     * all row lock holders in current transaction
     * key: rowKey
     * value: lockHold
     */
    @Getter
    private ConcurrentHashMap<RowKey, LockHolder> rowHolderMap = new ConcurrentHashMap();

    @Getter
    private ConcurrentHashMap<Long, ConcurrentHashMap<RowKey, LockHolder>> subTxRowHolderHeaderMap = new ConcurrentHashMap();

    @Getter
    private ConcurrentHashMap<Long, LockHolder> subTxTableHolderHeaderMap = new ConcurrentHashMap();

    @Getter
    @Setter
    private LockHolder tableHolder;

    @Getter
    @Setter
    private LockHolder head;

    private final static HashMap<Integer, Integer> ROW_LOCK_NUM_PER_MODE = new HashMap<>();
    @Getter
    private long svptID = -1;
    @Getter
    private long implicitSavepointID = -1;

    @Getter
    @Setter
    private String queryContext = null;

    public Transaction () {
    }

    public Transaction(Long txID) {
        this.txID = txID;
    }

    public void reInit(Long txID) {
        this.txID = txID;
        this.referenceNum = 0;
        this.removedFlag = false;
        synchronized (this.rowHolderMap) {
            this.rowHolderMap.clear();
        }
        synchronized (subTxRowHolderHeaderMap) {
            this.subTxRowHolderHeaderMap.clear();
        }
        synchronized (subTxTableHolderHeaderMap) {
            this.subTxTableHolderHeaderMap.clear();
        }
        if (this.tableHolder != null) {
            this.tableHolder.reInit();
        }
        this.tableHolder = null;
        this.head = null;
        this.svptID = -1;
        this.implicitSavepointID = -1;
        this.queryContext = null;
    }
  
    public synchronized void increaseReferenceNum() {
        this.referenceNum += 1;
    }

    public synchronized void decreaseReferenceNum(String regionName) {
        this.referenceNum -= 1;
        if (this.referenceNum == 0) {
            try {
                this.notifyAll();
            } catch (Exception e) {
                LOG.error("failed to notifyall transaction", e);
            }
        }
        if (this.referenceNum < 0) {
            LOG.info("decreaseReferenceNum txID: " + txID + " regionName： " + regionName + " referenceNum: " + referenceNum, new Exception("decreaseReferenceNum"));
        }
    }

    public synchronized int getReferenceNum() {
        return this.referenceNum;
    }

    public int getReferenceNumUnSafe() {
        return this.referenceNum;
    }

    public synchronized void setReferenceNum(int referenceNum) {
        this.referenceNum = referenceNum;
    }

    public int getEscaLockMode(long svptID) {
        resetRowLockNum();
        if (svptID > 0) {
            getSavePointEscaLockNum(svptID);
        } else {
            LockHolder lockHolder = null;
            for (Map.Entry<RowKey, LockHolder> entry : rowHolderMap.entrySet()) {
                lockHolder = entry.getValue();
                if (lockHolder.getHolding()[LockMode.getIndex(LockMode.LOCK_S)] > 0) {
                    ROW_LOCK_NUM_PER_MODE.put(LockMode.LOCK_S, ROW_LOCK_NUM_PER_MODE.get(LockMode.LOCK_S) + 1);
                }
                if (lockHolder.getHolding()[LockMode.getIndex(LockMode.LOCK_U)] > 0) {
                    ROW_LOCK_NUM_PER_MODE.put(LockMode.LOCK_U, ROW_LOCK_NUM_PER_MODE.get(LockMode.LOCK_U) + 1);
                }
                if (lockHolder.getHolding()[LockMode.getIndex(LockMode.LOCK_X)] > 0) {
                    ROW_LOCK_NUM_PER_MODE.put(LockMode.LOCK_X, ROW_LOCK_NUM_PER_MODE.get(LockMode.LOCK_X) + 1);
                }
            }
        }

        int escaLockMode = -1;
        int lockSNum = ROW_LOCK_NUM_PER_MODE.get(LockMode.LOCK_S);
        int lockUNum = ROW_LOCK_NUM_PER_MODE.get(LockMode.LOCK_U);
        int lockXNum = ROW_LOCK_NUM_PER_MODE.get(LockMode.LOCK_X);
        if (lockSNum >= lockUNum && lockSNum >= lockXNum) {
            escaLockMode = LockMode.LOCK_S;
        } else if (lockUNum >= lockSNum && lockUNum >= lockXNum) {
            escaLockMode = LockMode.LOCK_U;
        } else {
            escaLockMode = LockMode.LOCK_X;
        }

        return escaLockMode;
    }

  public void getSavePointEscaLockNum(long svptID) {
    LockHolder lockHolder = null;

    lockHolder = subTxTableHolderHeaderMap.get(svptID);
    if (lockHolder != null) {
      Stack<LockHolder> stack = new Stack<>();
      stack.push(lockHolder);
      LockHolder tmpHolder = null;
      ConcurrentHashMap<RowKey, LockHolder> svptRowHolders = null;
      while (stack.size() > 0) {
        tmpHolder = stack.pop();
        svptRowHolders = subTxRowHolderHeaderMap.get(tmpHolder.getSvptID());
        if (svptRowHolders != null) {
          for (Map.Entry<RowKey, LockHolder> entry : svptRowHolders.entrySet()) {
            lockHolder = entry.getValue();
            if (lockHolder.getHolding()[LockMode.getIndex(LockMode.LOCK_S)] > 0) {
              ROW_LOCK_NUM_PER_MODE.put(LockMode.LOCK_S, ROW_LOCK_NUM_PER_MODE.get(LockMode.LOCK_S) + 1);
            }
            if (lockHolder.getHolding()[LockMode.getIndex(LockMode.LOCK_U)] > 0) {
              ROW_LOCK_NUM_PER_MODE.put(LockMode.LOCK_U, ROW_LOCK_NUM_PER_MODE.get(LockMode.LOCK_U) + 1);
            }
            if (lockHolder.getHolding()[LockMode.getIndex(LockMode.LOCK_X)] > 0) {
              ROW_LOCK_NUM_PER_MODE.put(LockMode.LOCK_X, ROW_LOCK_NUM_PER_MODE.get(LockMode.LOCK_X) + 1);
            }
          }
        }
        if (tmpHolder.getNextNeighbor() != null) {
          stack.push(tmpHolder.getNextNeighbor());
        }
        if (tmpHolder.getChild() != null) {
          stack.push(tmpHolder.getChild());
        }
      }
    }
  }

    private void resetRowLockNum() {
        ROW_LOCK_NUM_PER_MODE.put(LockMode.LOCK_S, 0);
        ROW_LOCK_NUM_PER_MODE.put(LockMode.LOCK_U, 0);
        ROW_LOCK_NUM_PER_MODE.put(LockMode.LOCK_X, 0);
    }

    public int getLockNumber() {
        int sum = ((tableHolder != null) ? tableHolder.getLockNum() : 0);
        for (Map.Entry<RowKey, LockHolder> entry : rowHolderMap.entrySet()) {
            sum += entry.getValue().getLockNum();
        }
        for (Map.Entry<Long, LockHolder> entry : subTxTableHolderHeaderMap.entrySet()) {
            sum += entry.getValue().getLockNum();
        }
        for (ConcurrentHashMap<RowKey, LockHolder> rowLocks : subTxRowHolderHeaderMap.values()) {
            for (Map.Entry<RowKey, LockHolder> entry : rowLocks.entrySet()) {
                sum += entry.getValue().getLockNum();
            }
        }
        return sum;
    }

    public LockHolder getSvptTableHolder(long svptID) {
        synchronized (subTxTableHolderHeaderMap) {
            return subTxTableHolderHeaderMap.get(svptID);
        }
    }

    public void addSvptTableHolder(long svptID, LockHolder lockHolder) {
        synchronized (subTxTableHolderHeaderMap) {
            subTxTableHolderHeaderMap.put(svptID, lockHolder);
        }
    }

    public void removeSvptTableHolder(long svptID) {
        synchronized (subTxTableHolderHeaderMap) {
            subTxTableHolderHeaderMap.remove(svptID);
        }
    }

    public LockHolder getSvptRowHolder(long svptID, RowKey rowKey) {
        ConcurrentHashMap<RowKey, LockHolder> subRowLockMap = null;
        synchronized (subTxRowHolderHeaderMap) {
            subRowLockMap = subTxRowHolderHeaderMap.get(svptID);
            if (subRowLockMap == null) {
                return null;
            }
            return subRowLockMap.get(rowKey);
        }
    }

    public void addSvptRowHolder(long svptID, RowKey rowKey, LockHolder lockHolder) {
        ConcurrentHashMap<RowKey, LockHolder> subRowLockMap = null;
        synchronized (subTxRowHolderHeaderMap) {
            subRowLockMap = subTxRowHolderHeaderMap.get(svptID);
            if (subRowLockMap == null) {
                subRowLockMap = new ConcurrentHashMap<>();
                subTxRowHolderHeaderMap.put(svptID, subRowLockMap);
            }
            subRowLockMap.put(rowKey, lockHolder);
        }
    }

    public void removeSvptRowHolder(long svptID, RowKey rowKey) {
        ConcurrentHashMap<RowKey, LockHolder> subRowLockMap = null;
        synchronized (subTxRowHolderHeaderMap) {
            subRowLockMap = subTxRowHolderHeaderMap.get(svptID);
            if (subRowLockMap == null) {
                return;
            }
            subRowLockMap.remove(rowKey);
            if (subRowLockMap.size() == 0) {
                subTxRowHolderHeaderMap.remove(svptID);
            }
        }
    }

    public int getRowHolderNumber(long svptID) {
        if (svptID > 0) {
            int rowHolderNumber = 0;
            LockHolder lockHolder = subTxTableHolderHeaderMap.get(svptID);
            if (lockHolder != null) {
                Stack<LockHolder> stack = new Stack<>();
                stack.push(lockHolder);
                LockHolder tmpHolder = null;
                ConcurrentHashMap<RowKey, LockHolder> svptRowHolders = null;
                while (stack.size() > 0) {
                    tmpHolder = stack.pop();
                    svptRowHolders = subTxRowHolderHeaderMap.get(tmpHolder.getSvptID());
                    if (svptRowHolders != null) {
                        rowHolderNumber += svptRowHolders.size();
                    }
                    if (tmpHolder.getNextNeighbor() != null) {
                        stack.push(tmpHolder.getNextNeighbor());
                    }
                    if (tmpHolder.getChild() != null) {
                        stack.push(tmpHolder.getChild());
                    }
                }
            }
            return rowHolderNumber;
        } else {
            return rowHolderMap.size();
        }
    }

    public void tryRemoveSubTxRowHolderHeaderMap(long svptID) {
        synchronized (subTxRowHolderHeaderMap) {
            ConcurrentHashMap<RowKey, LockHolder> subTxRowHolderMap = subTxRowHolderHeaderMap.get(svptID);
            if (subTxRowHolderMap == null || subTxRowHolderMap.size() == 0) {
                subTxRowHolderHeaderMap.remove(svptID);
            }
        }
    }

    @Override
    public long sizeof() {
        long size = 0L;
        //11 attributes
        size += 11 * Size_Reference;
        //4 Map
        size += 3 * Size_ConcurrentHashMap + Size_HashMap;
        // 2 long
        size += 16;
        // 2 int + boolean
        size += 8;
        //rowHolderMap
        synchronized (rowHolderMap) {
            if (!rowHolderMap.isEmpty()) {
                Iterator<Map.Entry<RowKey, LockHolder>> itor = rowHolderMap.entrySet().iterator();
                if (itor.hasNext()) {
                    Map.Entry<RowKey, LockHolder> entry = itor.next();
                    size += (entry.getKey().sizeof() + entry.getValue().sizeof() + Size_Pair) * rowHolderMap.size();
                }
            }
        }
        //subTxRowHolderHeaderMap
        synchronized (subTxRowHolderHeaderMap) {
            if (!subTxRowHolderHeaderMap.isEmpty()) {
                for (Map.Entry<Long, ConcurrentHashMap<RowKey, LockHolder>> entry : subTxRowHolderHeaderMap
                        .entrySet()) {
                    size += Size_Long;//key
                    ConcurrentHashMap<RowKey, LockHolder> holders = entry.getValue();
                    size += Size_ConcurrentHashMap;
                    if (!holders.isEmpty()) {
                        Iterator<Map.Entry<RowKey, LockHolder>> itor = holders.entrySet().iterator();
                        if (itor.hasNext()) {
                            Map.Entry<RowKey, LockHolder> subEntry = itor.next();
                            size += (subEntry.getKey().sizeof() + subEntry.getValue().sizeof() + Size_Pair)
                                    * holders.size();
                        }
                    }
                    size += Size_Pair;
                }
            }
        }
        //subTxTableHolderHeaderMap
        synchronized (subTxTableHolderHeaderMap) {
            if (!subTxTableHolderHeaderMap.isEmpty()) {
                Iterator<Map.Entry<Long, LockHolder>> itor = subTxTableHolderHeaderMap.entrySet().iterator();
                if (itor.hasNext()) {
                    Map.Entry<Long, LockHolder> entry = itor.next();
                    size += (Size_Long + entry.getValue().sizeof() + Size_Pair) * subTxTableHolderHeaderMap.size();
                }
            }
        }
        //tableHolder
        if (tableHolder != null) {
            size += tableHolder.sizeof();
        }
        //ROW_LOCK_NUM_PER_MODE
        synchronized (ROW_LOCK_NUM_PER_MODE) {
            if (!ROW_LOCK_NUM_PER_MODE.isEmpty()) {
                size += (2 * Size_Long + Size_Pair) * ROW_LOCK_NUM_PER_MODE.size();
            }
        }
        return LockUtils.alignLockObjectSize(size);
    }

    public void setSvptID(long svptID) {
        if (svptID > 0 && svptID > this.svptID) {
            this.svptID = svptID;
        }
    }

    public void resetSvptID() {
        this.svptID = -1;
        LockHolder lockHolder = null;
        for (Map.Entry<Long, LockHolder> entry: subTxTableHolderHeaderMap.entrySet()) {
            lockHolder = entry.getValue();
            if (!lockHolder.isImplicitSavepoint() && lockHolder.getSvptID() > this.svptID) {
                this.svptID = lockHolder.getSvptID();
            }
        }
    }

    public void setImplicitSavepointID(long implicitSavepointID) {
        if (implicitSavepointID > 0 && implicitSavepointID > this.implicitSavepointID) {
            this.implicitSavepointID = implicitSavepointID;
        }
    }

    public void resetImplicitSavepointID() {
        this.implicitSavepointID = -1;
    }

    public long getSubRowSvptID(long svptID) {
        for (Long tmpSvptID : this.subTxRowHolderHeaderMap.keySet()) {
            if (tmpSvptID >= svptID) {
                return tmpSvptID;
            }
        }
        return -1;
    }

    public long getSubTableSvptID(long svptID) {
        for (Long tmpSvptID : this.subTxTableHolderHeaderMap.keySet()) {
            if (tmpSvptID >= svptID) {
                return tmpSvptID;
            }
        }
        return -1;
    }
}
