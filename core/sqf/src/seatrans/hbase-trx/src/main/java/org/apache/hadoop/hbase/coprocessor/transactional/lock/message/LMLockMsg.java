package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LMLockMsg implements Serializable {
    private static final long serialVersionUID = 1L;
    @Setter
    @Getter
    private String objID;
    @Setter
    @Getter
    private String rowID;
    @Getter
    @Setter
    private String tableName;
    @Getter
    @Setter
    private String regionName;
    @Setter
    @Getter
    private long[] holding = {0, 0, 0, 0, 0, 0, 0};
    @Getter
    @Setter
    private Map<Long, long[]> svptHoldings = new ConcurrentHashMap<>();
    @Getter
    @Setter
    private Map<Long, long[]> implicitSvptHoldings = new ConcurrentHashMap<>();
    @Getter
    @Setter
    private Map<Long, long[]> subImplicitSvptHoldings = new ConcurrentHashMap<>();
    @Getter
    @Setter
    private Map<Long, Long> svptRelation = new ConcurrentHashMap<>();
    @Setter
    @Getter
    private int maskHold;

    @Setter
    @Getter
    private long durableTime;

    @Setter
    private long svptID = -1;
    @Setter
    private long parentSvptID = -1;
    @Setter
    private boolean implicitSavepoint = false;

    //copy from LockHolder.java
    private int getLockNum(long[] holds) {
        int lockNum = 0;
        for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            lockNum += (holds[i] > 0 ? 1 : 0);
        }

        return lockNum;
    }

    public int getLockNum() {
        int lockNum = 0;
        lockNum += this.getLockNum(holding);
        for (long[] holds : svptHoldings.values()) {
            lockNum += this.getLockNum(holds);
        }
        if (LockConstants.DISPLAY_IMPLICIT_SAVEPOINT) {
            for (long[] holds : implicitSvptHoldings.values()) {
                lockNum += this.getLockNum(holds);
            }
            for (long[] holds : subImplicitSvptHoldings.values()) {
                lockNum += this.getLockNum(holds);
            }
        }
        return lockNum;
    }

    public String getLockMode() {
        if (!LockConstants.DISPLAY_IMPLICIT_SAVEPOINT) {
            return getLockModeDefault();
        }
        StringBuffer lockMode = new StringBuffer();

        if (svptID > 0) {
            if (implicitSavepoint == false ||
                (parentSvptID > 0 && !LockConstants.DISPLAY_IMPLICIT_SAVEPOINT)) {
                svptHoldings.put(svptID, holding);
            } else {
                implicitSvptHoldings.put(svptID, holding);
            }
        } else {
            for (int i=0; i<holding.length; i++) {
                if (holding[i] > 0) {
                    lockMode.append(getLockMode(i)).append(",");
                }
            }
        }
        
        StringBuffer svptLockMode = new StringBuffer();
        StringBuffer lockModeMsg = new StringBuffer(20);
        StringBuffer implicitSvptLockMode = new StringBuffer();
        for (Map.Entry<Long, long[]> entry : implicitSvptHoldings.entrySet()) {
            getLockMode(lockModeMsg, entry.getValue(), implicitSvptLockMode);
            if (lockModeMsg.length() > 0) {
                if (LockConstants.DISPLAY_SAVEPOINT_ID) {
                    implicitSvptLockMode.append(entry.getKey()).append(":");
                }
                implicitSvptLockMode.append(lockModeMsg);
            }
        }
        for (Map.Entry<Long, long[]> entry : subImplicitSvptHoldings.entrySet()) {
            getLockMode(lockModeMsg, entry.getValue(), implicitSvptLockMode);
            if (lockModeMsg.length() > 0) {
                if (LockConstants.DISPLAY_SAVEPOINT_ID) {
                    implicitSvptLockMode.append(entry.getKey()).append(":");
                }
                implicitSvptLockMode.append(lockModeMsg);
            }
        }
        if (implicitSvptLockMode.length() > 0) {
            lockMode.append("implicit savepoint:").append(implicitSvptLockMode);
        }

        for (Map.Entry<Long, long[]> entry : svptHoldings.entrySet()) {
            getLockMode(lockModeMsg, entry.getValue(), svptLockMode);
            if (lockModeMsg.length() > 0) {
                if (LockConstants.DISPLAY_SAVEPOINT_ID) {
                    svptLockMode.append(entry.getKey()).append(":");
                }
                svptLockMode.append(lockModeMsg);
            }
        }
        if (svptLockMode.length() > 0) {
            lockMode.append("savepoint:").append(svptLockMode);
        }

        if (lockMode.length() > 0) {
            lockMode.deleteCharAt(lockMode.length() - 1);
        }
        return lockMode.toString();
    }

    private String getLockModeDefault() {
        for (Map.Entry<Long, long[]> entry : implicitSvptHoldings.entrySet()) {
            mergeHolding(this.holding, entry.getValue());
        }
        StringBuffer lockMode = new StringBuffer();
        if (svptID > 0 && (!implicitSavepoint || parentSvptID > 0)) {
            svptHoldings.put(svptID, this.holding);
        } else {
            getLockMode(this.holding, lockMode);
        }

        StringBuffer svptLockMode = new StringBuffer();
        if (svptHoldings.size() > 0) {
            if (LockConstants.DISPLAY_SAVEPOINT_ID) {
                Set<Long> svptIDSet = svptHoldings.keySet();
                Long[] svptIDs = new Long[svptIDSet.size()];
                svptIDSet.toArray(svptIDs);
                Arrays.sort(svptIDs);
                long[] currentHolding = null;
                for (int i = 0; i < svptIDs.length - 1; i++) {
                    currentHolding = svptHoldings.get(svptIDs[i]);
                    for (long j = svptIDs[i] + 1; j < svptIDs[i + 1]; j++) {
                        mergeHolding(currentHolding, subImplicitSvptHoldings.get(j));
                    }
                }
                long tmpSvptID = svptIDs[svptIDs.length - 1];
                currentHolding = svptHoldings.get(tmpSvptID);
                for (Map.Entry<Long, long[]> entry : subImplicitSvptHoldings.entrySet()) {
                    if (entry.getKey() > tmpSvptID) {
                        mergeHolding(currentHolding, entry.getValue());
                    }
                }
                for (int i = 0; i < svptIDs.length; i++) {
                    currentHolding = svptHoldings.get(svptIDs[i]);
                    svptLockMode.append(svptIDs[i]).append(":");
                    getLockMode(currentHolding, svptLockMode);
                }
            } else {
                long[] currentHolding = null;
                for (Map.Entry<Long, long[]> entry : svptHoldings.entrySet()) {
                    if (currentHolding == null) {
                        currentHolding = entry.getValue();
                    } else {
                        mergeHolding(currentHolding, entry.getValue());
                    }
                }
                for (Map.Entry<Long, long[]> entry : subImplicitSvptHoldings.entrySet()) {
                    if (currentHolding == null) {
                        currentHolding = entry.getValue();
                    } else {
                        mergeHolding(currentHolding, entry.getValue());
                    }
                }
                getLockMode(currentHolding, svptLockMode);
            }
        }
        if (svptLockMode.length() > 0) {
            lockMode.append("savepoint:").append(svptLockMode);
        }

        if (lockMode.length() > 0) {
            lockMode.deleteCharAt(lockMode.length() - 1);
        }
        return lockMode.toString();
    }

    private void getLockMode(long[] currentHolding, StringBuffer lockModeBuf) {
        if (currentHolding == null) {
            return;
        }
        for (int i=0; i<currentHolding.length; i++) {
            if (currentHolding[i] > 0) {
                lockModeBuf.append(getLockMode(i)).append(",");
            }
        }
    }

    private void getLockMode(StringBuffer lockModeMsg, long[] lockHolding, StringBuffer existLockMode) {
        lockModeMsg.delete(0, lockModeMsg.length());
        String currentLockMode = null;
        int idx = -1;
        for (int i=0; i<lockHolding.length; i++) {
            if (lockHolding[i] > 0) {
                currentLockMode = getLockMode(i);
                if (!LockConstants.DISPLAY_SAVEPOINT_ID) {
                    idx = existLockMode.indexOf("," + currentLockMode + ",");
                    if (idx > 0) {
                        continue;
                    } else {
                        idx = existLockMode.indexOf(currentLockMode);
                        if (idx >= 0 && idx != 1) {
                            continue;
                        }
                    }
                }
                lockModeMsg.append(currentLockMode).append(",");
            }
        }
    }

    public void merge(LMLockMsg remoteLock) {
        int dwLockModes = remoteLock.getMaskHold();
        for (int dwCount = 0; dwCount < LockConstants.LOCK_MAX_MODES; dwCount++) {
            this.holding[dwCount] += remoteLock.getHolding()[dwCount];
        }
        long[] localHolding = null;
        for (Map.Entry<Long, long[]> entry : remoteLock.getImplicitSvptHoldings().entrySet()) {
            localHolding = this.implicitSvptHoldings.get(entry.getKey());
            if (localHolding == null) {
                this.implicitSvptHoldings.put(entry.getKey(), entry.getValue());
            } else {
                mergeHolding(localHolding, entry.getValue());
            }
        }
        for (Map.Entry<Long, long[]> entry : remoteLock.getSubImplicitSvptHoldings().entrySet()) {
            localHolding = this.subImplicitSvptHoldings.get(entry.getKey());
            if (localHolding == null) {
                this.subImplicitSvptHoldings.put(entry.getKey(), entry.getValue());
            } else {
                mergeHolding(localHolding, entry.getValue());
            }
        }
        for (Map.Entry<Long, long[]> entry : remoteLock.getSvptHoldings().entrySet()) {
            localHolding = this.svptHoldings.get(entry.getKey());
            if (localHolding == null) {
                this.svptHoldings.put(entry.getKey(), entry.getValue());
            } else {
                mergeHolding(localHolding, entry.getValue());
            }
        }
        this.maskHold |= dwLockModes;
    }

    private void mergeHolding(long[] localHolding, long[] remoteHolding) {
        if (remoteHolding == null) {
            return;
        }
        for (int dwCount = 0; dwCount < LockConstants.LOCK_MAX_MODES; dwCount++) {
            localHolding[dwCount] += remoteHolding[dwCount];
        }
    }

    private String getLockMode(int lockMode) {
        switch (lockMode) {
            case 0:
                return "IS";
            case 1:
                return "S";
            case 2:
                return "IX";
            case 3:
                return "U";
            case 4:
                return "X";
            case 5:
                return "RS";
            case 6:
                return "RX";
        }
        return null;
    }
}
