package org.apache.hadoop.hbase.coprocessor.transactional.lock.base;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RetCode;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LockUtil {
    private static DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static boolean acquireLock(LockManager lm, LockObject lockObject) {
        RetCode retCode = acquireLockWithRetCode(lm, lockObject);
        boolean result = ((retCode == RetCode.OK) || (retCode == RetCode.OK_LOCKED) || (retCode == RetCode.OK_WITHRETRY));
        return result;
    }

    public static RetCode acquireLockWithRetCode(LockManager lm, LockObject lockObject) {
        LocalDateTime now = null;
        now = LocalDateTime.now();
        if (lockObject.getRowID() == null) {
            System.out.println(format.format(now) + " " + Thread.currentThread().getName() + " " + lockObject + " 申请表锁 开始");
        } else {
            System.out.println(format.format(now) + " " + Thread.currentThread().getName() + " " + lockObject + " 申请行锁 开始");
        }
        RetCode retCode = RetCode.OK;
        boolean result = false;
        try {
            if (lockObject.getRowID() == null) {
                retCode = lm.lockAcquire(lockObject.getTxID(), lockObject.getSvptID(), lockObject.getParentSvptID(), lockObject.getLockMode(), lockObject.getTimeout(), null);
            } else {
                retCode = lm.lockAcquire(lockObject.getTxID(), lockObject.getSvptID(), lockObject.getParentSvptID(), lockObject.getRowID(), lockObject.getLockMode(), lockObject.getTimeout(), null);
            }
            result = ((retCode == RetCode.OK) || (retCode == RetCode.OK_LOCKED) || (retCode == RetCode.OK_WITHRETRY));
            now = LocalDateTime.now();
            boolean isDeadLock = (retCode == RetCode.CANCEL_FOR_DEADLOCK);
            if (lockObject.getRowID() == null) {
                if (result) {
                    System.out.println(format.format(now) + " " + Thread.currentThread().getName() + " " + lockObject + " 申请表锁 成功");
                } else {
                    System.out.println(format.format(now) + " " +Thread.currentThread().getName() + " " + lockObject + (isDeadLock ? " 死锁" : "申请表锁 失败"));
                }
            } else {
                if (result) {
                    System.out.println(format.format(now) + " " +Thread.currentThread().getName() + " " + lockObject + " 申请行锁 成功");
                } else {
                    System.out.println(format.format(now) + " " +Thread.currentThread().getName() + " " + lockObject + (isDeadLock ? " 死锁" : "申请表锁 失败"));
                }
            }
        } catch (Exception e) {
            if (now == null) {
                now = LocalDateTime.now();
            }
            System.out.println(format.format(now) + " " + Thread.currentThread().getName() + " " + lockObject + " 申请表锁失败");
        }

        return retCode;
    }

    public static boolean lockReleaseAll(LockManager lm, LockObject lockObject) {
        boolean result = lm.lockReleaseAll(lockObject.getTxID());
        LocalDateTime now = LocalDateTime.now();
        if (result) {
            System.out.println(format.format(now) + " " + Thread.currentThread().getName() + " 事务： " + lockObject.getTxID() + " 中的锁全部释放成功");
        } else {
            System.out.println(format.format(now) + " " + Thread.currentThread().getName() + " 事务： " + lockObject.getTxID() + " 中的锁全部释放失败");
        }
        return result;
    }

    public static boolean lockReleaseSavepoint(LockManager lm, LockObject lockObject) {
        boolean result = lm.lockReleaseAll(lockObject.getTxID(), lockObject.getSvptID());
        LocalDateTime now = LocalDateTime.now();
        if (result) {
            System.out.println(format.format(now) + " " + Thread.currentThread().getName() + " 事务: " + lockObject.getTxID() + " 保存点: " + lockObject.getSvptID() + " 中的锁全部释放成功");
        } else {
            System.out.println(format.format(now) + " " + Thread.currentThread().getName() + " 事务: " + lockObject.getTxID() + " 保存点: " + lockObject.getSvptID() + " 中的锁全部释放失败");
        }
        return result;
    }

    public static boolean lockWithRetry(LockManager lockManager, LockObject lockObject) {
        boolean canLoop = true;
        int retryTimes = LockConstants.LOCK_CLIENT_RETRIES_TIMES;
        RetCode lockRetCode = null;
        boolean retCode = false;
        while (canLoop && retryTimes > 0) {
            lockRetCode = LockUtil.acquireLockWithRetCode(lockManager, lockObject);
            retCode = (lockRetCode == RetCode.OK || lockRetCode == RetCode.OK_LOCKED || lockRetCode == RetCode.OK_WITHRETRY);
            if (retCode) {
                canLoop = false;
            } else {
                canLoop = (lockRetCode != RetCode.CANCEL_FOR_DEADLOCK);
            }
            retryTimes--;
        }
        return retCode;
    }

}
