package org.apache.hadoop.hbase.coprocessor.transactional.lock.queue;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RetCode;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockUtil;

import java.util.concurrent.Callable;

public class LockAcquireTask implements Callable<Boolean> {
    private LockObject lockObject;
    private LockManager lockManager;

    public LockAcquireTask(LockManager lockManager, LockObject lockObject) {
        this.lockManager = lockManager;
        this.lockObject= lockObject.clone();
    }

    @Override
    public Boolean call() throws Exception {
        boolean canLoop = true;
        RetCode lockRetCode = RetCode.OK;
        boolean retCode = false;
        int retryTimes = lockObject.getLockClientRetryTimes();
        Thread.currentThread().setName("TX:" + lockObject.getTxID() + "-" + lockObject.getTable() + "-" + lockObject.getLockMode());
        switch (lockObject.getType()) {
            case LockObject.LOCK_ACQUIRE:
                while (canLoop && retryTimes > 0) {
                    lockRetCode = LockUtil.acquireLockWithRetCode(lockManager, lockObject);
                    retCode = (lockRetCode == RetCode.OK || lockRetCode == RetCode.OK_LOCKED);
                    if (retCode) {
                        canLoop = false;
                    } else {
                        canLoop = (lockRetCode != RetCode.CANCEL_FOR_DEADLOCK);
                    }
                    retryTimes--;
                }
                break;
            case LockObject.LOCK_RELEASE:
                retCode = LockUtil.lockReleaseAll(lockManager, lockObject);
                break;
            case LockObject.LOCK_ACQUIRE_AND_RELEASE:
                while (canLoop && retryTimes > 0) {
                    lockRetCode = LockUtil.acquireLockWithRetCode(lockManager, lockObject);
                    retCode = (lockRetCode == RetCode.OK || lockRetCode == RetCode.OK_LOCKED);
                    if (retCode) {
                        canLoop = false;
                    } else {
                        canLoop = (lockRetCode != RetCode.CANCEL_FOR_DEADLOCK);
                    }
                    retryTimes--;
                }
                if (!retCode) {
                    LockUtil.lockReleaseAll(lockManager, lockObject);
                }
                if (lockObject.getTxID() == 1) {
                    Thread.sleep(20000);
                } else {
                    Thread.sleep(1000);
                }
                LockUtil.lockReleaseAll(lockManager, lockObject);
                System.out.println("transaction finished: " + lockObject.getTxID());
                break;
        }
        return retCode;
    }
}