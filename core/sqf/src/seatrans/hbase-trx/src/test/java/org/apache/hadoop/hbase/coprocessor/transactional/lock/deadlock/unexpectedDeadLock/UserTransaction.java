package org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.unexpectedDeadLock;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RetCode;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

public class UserTransaction extends BaseTest implements Runnable {
    private LockObject acquire;
    private LockManager lockManager;
    private CountDownLatch countDownLatch;

    public UserTransaction(LockManager lockManager, long txID, CountDownLatch countDownLatch) {
        super.init();
        this.lockManager = lockManager;
        this.countDownLatch = countDownLatch;
        this.acquire = new LockObject(txID, "seabase.trafodion.test1,aaa", "001", LockMode.LOCK_S);
    }

    @Override
    public void run() {
        int i = 0;
        try {
            while (i < 10000) {
                try {
                    acquire.setLockMode(LockMode.LOCK_S);
                    assertTrue(acquireLock(lockManager, acquire));
                    lockRelease(lockManager, acquire);
                    acquire.setLockMode(LockMode.LOCK_U);
                    assertTrue(acquireLock(lockManager, acquire));
                    acquire.setLockMode(LockMode.LOCK_X);
                    assertTrue(acquireLock(lockManager, acquire));
                } catch (Exception e) {
                    break;
                } finally {
                    lockReleaseAll(lockManager, acquire);
                    checkTxLockNum(acquire, 0);
                }
                i++;
                System.out.println(Thread.currentThread().getName() + " " + i);
            }
        } finally {
            countDownLatch.countDown();
        }
    }

    protected boolean acquireLock(LockManager lm, LockObject lockObject) {
        boolean result = false;
        try {
            RetCode retCode;
            if (lockObject.getRowID() == null) {
                retCode = lm.lockAcquire(lockObject.getTxID(), lockObject.getLockMode(), lockObject.getTimeout(), null);
            } else {
                retCode = lm.lockAcquire(lockObject.getTxID(), lockObject.getRowID(), lockObject.getLockMode(), lockObject.getTimeout(), null);
            }
            result = (retCode == RetCode.OK || retCode == RetCode.OK_LOCKED);
            if (lockObject.getRowID() == null) {
                if (result) {
                    System.out.println(Thread.currentThread().getName() + " " + lockObject + " 申请表锁成功");
                } else {
                    System.out.println(Thread.currentThread().getName() + " " + lockObject + " 申请表锁失败");
                }
            } else {
                if (result) {
                    System.out.println(Thread.currentThread().getName() + " " + lockObject + " 申请行锁成功");
                } else {
                    System.out.println(Thread.currentThread().getName() + " " + lockObject + " 申请行锁失败");
                }
            }
        } catch (Exception e) {
            System.out.println(Thread.currentThread().getName() + " " + lockObject + " 申请锁失败");
        }
        return result;
    }

    private void lockRelease(LockManager lm, LockObject lockObject) {
        if (lockObject.getRowID() != null) {
            lm.lockRelease(lockObject.getTxID(), lockObject.getRowID(), lockObject.getLockMode());
        } else {
            lm.lockRelease(lockObject.getTxID(), lockObject.getLockMode());
        }
        System.out.println(Thread.currentThread().getName() + " " + lockObject + " 释放锁成功");
    }

    protected void lockReleaseAll(LockManager lm, LockObject lockObject) {
        boolean result = lm.lockReleaseAll(lockObject.getTxID());
        if (result) {
            System.out.println(Thread.currentThread().getName() + " 事务： " + lockObject.getTxID() + " 中的锁全部释放成功");
        } else {
            System.out.println(Thread.currentThread().getName() + " 事务： " + lockObject.getTxID() + " 中的锁全部释放失败");
        }
    }
}
