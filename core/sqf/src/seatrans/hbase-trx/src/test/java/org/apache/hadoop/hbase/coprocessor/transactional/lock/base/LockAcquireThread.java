package org.apache.hadoop.hbase.coprocessor.transactional.lock.base;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;

public class LockAcquireThread extends Thread {
    LockObject lockObject;
    LockManager lockManager;

    public LockAcquireThread(LockManager lockManager, LockObject lockObject) {
        super("LockAcquireThread " + lockObject.getTxID() + "-" + lockObject.getSvptID() );
        this.lockManager = lockManager;
        this.lockObject= lockObject.clone();
    }

    public void run() {
        switch (lockObject.getType()) {
            case 0:
                LockUtil.acquireLock(lockManager, lockObject);
                break;
            case 1:
                LockUtil.lockReleaseAll(lockManager, lockObject);
                break;
            case 2:
                if (!LockUtil.acquireLock(lockManager, lockObject)) {
                    LockUtil.lockReleaseAll(lockManager, lockObject);
                }
                break;
        }
    }


}