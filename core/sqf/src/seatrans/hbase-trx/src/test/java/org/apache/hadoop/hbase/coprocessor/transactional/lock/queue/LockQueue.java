package org.apache.hadoop.hbase.coprocessor.transactional.lock.queue;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LockQueue extends BaseTest {
    private LockManager lockManager;
    private LockObject lockObject;
    private ExecutorService threadPool = null;

    @Before
    public void init() {
        super.init();
        if (threadPool != null) {
            threadPool.shutdown();
        }
        threadPool = Executors.newFixedThreadPool(5);
        if (lockManager == null) {
            lockManager = new LockManager("seabase.trafodion.test,aaa", false);
        }
        if (lockObject == null) {
            lockObject = new LockObject(1, "seabase.trafodion.test,aaa", "00001", LockMode.LOCK_IS);
            lockObject.setType(2);
        }
    }

    @Test
    public void testLockU1() {
        lockObject.setLockMode(LockMode.LOCK_U);
        testCase();
    }

    private void testCase() {
        Future<Boolean> future1 = threadPool.submit(new LockAcquireTask(lockManager, lockObject));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        lockObject.setTxID(2);
        Future<Boolean> future2 = threadPool.submit(new LockAcquireTask(lockManager, lockObject));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        lockObject.setTxID(3);
        Future<Boolean> future3 = threadPool.submit(new LockAcquireTask(lockManager, lockObject));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        lockObject.setTxID(4);
        Future<Boolean> future4 = threadPool.submit(new LockAcquireTask(lockManager, lockObject));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        lockObject.setTxID(5);
        Future<Boolean> future5 = threadPool.submit(new LockAcquireTask(lockManager, lockObject));
        try {
            future1.get();
            future2.get();
            future3.get();
            future4.get();
            future5.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
