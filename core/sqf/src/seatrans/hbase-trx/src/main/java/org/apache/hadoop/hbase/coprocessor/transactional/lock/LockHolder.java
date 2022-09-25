package org.apache.hadoop.hbase.coprocessor.transactional.lock;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Stack;

public class LockHolder implements LockSizeof {
    private static Logger LOG = Logger.getLogger(Lock.class);
    public LockHolder() {
    }

    public LockHolder(Lock lock, Transaction transaction) {
        this.lock = lock;
        this.transaction = transaction;
    }
    // lock object that held by current holder
    @Setter
    @Getter
    private Lock lock;

    // The number of times the current holder adds each lock modes to the lock object
    @Setter
    @Getter
    private long[] holding = {0, 0, 0, 0, 0, 0, 0};
    
    // The lock mode mask of the holder
    @Setter
    @Getter
    private int maskHold;

    @Getter
    @Setter
    private LockHolder transPrev;

    @Getter
    @Setter
    private LockHolder transNext;

    @Getter
    @Setter
    private LockHolder lockPrev;

    @Getter
    @Setter
    private LockHolder lockNext;

    @Getter
    @Setter
    private LockHolder parent;

    @Getter
    @Setter
    private LockHolder child;

    @Getter
    @Setter
    private LockHolder preNeighbor;

    @Getter
    @Setter
    private LockHolder nextNeighbor;

    @Getter
    @Setter
    private long svptID = -1;

    @Getter
    @Setter
    private long parentSvptID = -1;

    @Getter
    @Setter
    private boolean implicitSavepoint = false;

    @Getter
    @Setter
    private Transaction transaction;

    public void reInit() {
        this.lock = null;
        for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            this.holding[i] = 0;
        }
        this.maskHold = 0;
        this.transaction = null;
        this.transPrev = null;
        this.transNext = null;
        this.lockPrev = null;
        this.lockNext = null;
        this.parent = null;
        this.child = null;
        this.preNeighbor = null;
        this.nextNeighbor = null;
        this.svptID = -1;
        this.parentSvptID = -1;
        this.transaction = null;
        this.implicitSavepoint = false;
    }

    public void reInit(Lock lock, Transaction transaction) {
        this.lock = lock;
        for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            this.holding[i] = 0;
        }
        this.maskHold = 0;
        this.transPrev = null;
        this.transNext = null;
        this.lockPrev = null;
        this.lockNext = null;
        this.parent = null;
        this.child = null;
        this.preNeighbor = null;
        this.nextNeighbor = null;
        this.svptID = -1;
        this.parentSvptID = -1;
        this.transaction = transaction;
        this.implicitSavepoint = false;
    }

    public int getLockNum() {
        int lockNum = 0;
        for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            lockNum += (holding[i] > 0 ? 1 : 0);
        }

        return lockNum;
    }

    public Boolean canHold(int lockMode){
        long[] OtherLockHolding = {0, 0, 0, 0, 0, 0, 0};
        for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            OtherLockHolding[i] = lock.getGranted()[i] - holding[i];
        }
        if (child != null) {
            Stack<LockHolder> stack = new Stack<>();
            stack.push(child);
            LockHolder tmpHolder = null;
            while (stack.size() > 0) {
                tmpHolder = stack.pop();
                for (int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
                    OtherLockHolding[i] = OtherLockHolding[i] - tmpHolder.getHolding()[i];
                }
                if (tmpHolder.getNextNeighbor() != null) {
                    stack.push(tmpHolder.getNextNeighbor());
                }
                if (tmpHolder.getChild() != null) {
                    stack.push(tmpHolder.getChild());
                }
            }
        }

        int OtherLockMaskHold = LockUtils.getMask(OtherLockHolding);
        switch (lockMode) {
            case LockMode.LOCK_IS:
                if (LockUtils.existsIS(maskHold) || LockUtils.existsS(maskHold) || LockUtils.existsX(maskHold)) {
                    return true;
                }
                else if (!LockUtils.existsX(OtherLockMaskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_S :
                if (LockUtils.existsS(maskHold) || LockUtils.existsU(maskHold) || LockUtils.existsX(maskHold)) {
                    return true;
                }
                else if (!LockUtils.existsIX(OtherLockMaskHold) && !LockUtils.existsX(OtherLockMaskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_IX :
                if (LockUtils.existsIX(maskHold) || LockUtils.existsX(maskHold)) {
                    return true;
                }
                else if (!LockUtils.existsS(OtherLockMaskHold) && !LockUtils.existsX(OtherLockMaskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_U :
                if(LockUtils.existsU(maskHold) || LockUtils.existsX(maskHold)) {
                    return true;
                }
                else if (!LockUtils.existsU(OtherLockMaskHold) && !LockUtils.existsX(OtherLockMaskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_X:
                if (LockUtils.existsX(maskHold)) {
                    return true;
                }
                else if (!LockUtils.existsIS(OtherLockMaskHold) && !LockUtils.existsIX(OtherLockMaskHold) && !LockUtils.existsS(OtherLockMaskHold) && !LockUtils.existsU(OtherLockMaskHold) && !LockUtils.existsX(OtherLockMaskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_RS:
                if (LockUtils.existsRS(maskHold)) {
                    return true;
                } else if (!LockUtils.existsRX(OtherLockMaskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_RX:
                if (LockUtils.existsRX(maskHold)) {
                    return true;
                } else if (!LockUtils.existsRS(OtherLockMaskHold) && !LockUtils.existsRX(OtherLockMaskHold)) {
                    return true;
                }
                break;
        }
        return false;
    }

    public Boolean lockContain(int lockMode){
        switch (lockMode) {
            case LockMode.LOCK_IS:
                if (LockUtils.existsIS(maskHold) || LockUtils.existsS(maskHold) || LockUtils.existsX(maskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_S :
                if (LockUtils.existsS(maskHold) || LockUtils.existsU(maskHold) || LockUtils.existsX(maskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_IX :
                if (LockUtils.existsIX(maskHold) || LockUtils.existsX(maskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_U :
                if(LockUtils.existsU(maskHold) || LockUtils.existsX(maskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_X:
                if (LockUtils.existsX(maskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_RS:
                if (LockUtils.existsRS(maskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_RX:
                if (LockUtils.existsRX(maskHold)) {
                    return true;
                }
                break;
        }
        return false;
    }

    public boolean tableLockContain(int lockMode) {
        switch (lockMode) {
            case LockMode.LOCK_S :
                if (LockUtils.existsS(maskHold) || LockUtils.existsX(maskHold)) {
                    return true;
                }
                break;
            case LockMode.LOCK_U :
            case LockMode.LOCK_X :
                if (LockUtils.existsX(maskHold)) {
                    return true;
                }
                break;
        }
        return false;
    }

    public void increaseHolding(int dwLockMode) {
        // holder
        int modeIdx = LockMode.getIndex(dwLockMode);
        holding[modeIdx] += 1;
        if (holding[modeIdx] == 1) {
            setMaskHold(getMaskHold() | dwLockMode);
        }
        // lock
        lock.getGranted()[modeIdx] += 1;
        if (lock.getGranted()[modeIdx] == 1) {
            lock.setMaskGrant(lock.getMaskGrant() | dwLockMode);
        }
    }

    public void decreaseHolding(int dwLockMode) {
        // holder
        int modeIdx = LockMode.getIndex(dwLockMode);
        holding[modeIdx] -= 1;
        if (lock.getGranted()[modeIdx] < 0) {
            Exception e = new Exception("decreaseHolding unexpected holding");
            LOG.error("decreaseHolding unexpected holding " + holding[modeIdx]  + ",regionName: " + lock.getRegionName() + ",thread:" + Thread.currentThread().getName() + ",lockHolder:" + this, e);
        }
        if (holding[modeIdx] == 0) {
            setMaskHold(getMaskHold() & LockUtils.BITS_OFF[modeIdx]);
        }
        // lock
        lock.getGranted()[modeIdx] -= 1;
        if (lock.getGranted()[modeIdx] < 0) {
            Exception e = new Exception("decreaseHolding unexpected grant");
            LOG.error("decreaseHolding unexpected grant " + lock.getGranted()[modeIdx]  + ",regionName: " + lock.getRegionName() + ",thread:" + Thread.currentThread().getName() + ",lockHolder:" + this, e);
        }
        if (lock.getGranted()[modeIdx] == 0) {
            lock.setMaskGrant(lock.getMaskGrant()  & LockUtils.BITS_OFF[modeIdx]);
        }
    }

    public void addChild(LockHolder child) {
        if (this.child == null) {
            this.child = child;
            child.parent = this;
        } else {
            child.nextNeighbor = this.child;
            this.child.preNeighbor = child;
            this.child = child;
            child.parent = this;
        }

    }

    public LockHolder getLastChild() {
        if (child != null) {
            LockHolder childLockHolder = child;
            while (childLockHolder.getChild() != null) {
                childLockHolder = childLockHolder.getChild();
            }
            return childLockHolder;
        }
        return null;
    }

    public String toString() {
        StringBuffer msg = new StringBuffer(100);
        msg.append("        hashCode: ").append(hashCode()).append("\n");
        if (svptID > 0) {
            msg.append("        savepoint: ").append(svptID).append(" parentSavepointID:").append(parentSvptID).append(" implicitSavepoint:").append(implicitSavepoint).append(" parent is null:").append(parent == null).append("\n");
        }
        msg.append("        transaction: ").append((transaction == null ? "null" :transaction.getTxID() + "," + transaction.hashCode())).append("\n");
        msg.append("        maskHold: ").append(maskHold).append("\n");
        msg.append("        holding: ").append(Arrays.toString(holding)).append("\n");
        if (parent == null && child != null) {
            Stack<LockHolder> stack = new Stack<>();
            stack.push(child);
            LockHolder tmpHolder = null;
            while (stack.size() > 0) {
                tmpHolder = stack.pop();
                msg.append(tmpHolder);
                if (tmpHolder.getNextNeighbor() != null) {
                    stack.push(tmpHolder.getNextNeighbor());
                    msg.append("        next neighbor hashCode: ").append(tmpHolder.getNextNeighbor().hashCode()).append("\n");
                }
                if (tmpHolder.getChild() != null) {
                    stack.push(tmpHolder.getChild());
                    msg.append("        next neighbor hashCode: ").append(tmpHolder.getChild().hashCode()).append("\n");
                }
            }
        }

        return msg.toString();
    }

    public void commitTo(LockHolder parentHolder) {
        for (int i = 0; i < holding.length; i++) {
            parentHolder.getHolding()[i] += holding[i];
            holding[i] = 0;
        }
        parentHolder.setMaskHold(parentHolder.getMaskHold() | maskHold);
        maskHold = 0;
    }

    @Override
    public long sizeof() {
        long size = Size_Object;
        //reference: lock,orthogonal list,transaction
        size += (10 * Size_Reference);
        //long: svptID,parentSvptID,holding
        size += (Size_Array + 8 * holding.length) + 16;
        //int: implicitSavepoint,maskHold
        size += 8;

        return LockUtils.alignLockObjectSize(size);
    }
}
