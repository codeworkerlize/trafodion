package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class LMAppliedLockInfoInPerServer implements Serializable {
    private static final long serialVersionUID = 1L;
    public Map<Long,Integer> transMap;
    public List<LockHolderInfo> appliedList;
    public long maxLockNum;
    public long sumLockNum;
    public long lockHolderPerTransLimitNum;
    public int lockHolderLimitNum;

    public LMAppliedLockInfoInPerServer() {}
}
