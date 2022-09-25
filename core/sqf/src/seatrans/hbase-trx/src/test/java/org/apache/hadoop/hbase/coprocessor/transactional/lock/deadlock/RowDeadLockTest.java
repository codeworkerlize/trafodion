package org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.*;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.JVM)
public class RowDeadLockTest extends BaseTest {
    private LockManager lockManager1;
    private ExecutorService threadPool = null;

    @Before
    public void init() {
        super.init();
        if (threadPool != null) {
            threadPool.shutdown();
        }
        threadPool = Executors.newFixedThreadPool(2);
        if (lockManager == null) {
            lockManager = new LockManager("seabase.trafodion.test,aaa", false);
        }
        if (lockManager1 == null) {
            lockManager1 = new LockManager("seabase.trafodion.test1,aaa", false);
        }
        if (lockObject == null) {
            lockObject = new LockObject(1, "seabase.trafodion.test,aaa", "0001", LockMode.LOCK_S);
            lockObject.setType(LockObject.LOCK_ACQUIRE_AND_RELEASE);
        }
        if (lockObject1 == null) {
            lockObject1 = new LockObject(2, "seabase.trafodion.test1,aaa", "0001", LockMode.LOCK_S);
            lockObject1.setType(LockObject.LOCK_ACQUIRE_AND_RELEASE);
        }
    }

    @Test
    public void testRowDeadLockSX() {
        testRowDeadLockCase(LockMode.LOCK_S, LockMode.LOCK_X);
        testRowDeadLockCase(LockMode.LOCK_X, LockMode.LOCK_S);
    }

    @Test
    public void testRowDeadLockUX() {
        testRowDeadLockCase(LockMode.LOCK_U, LockMode.LOCK_X);
        testRowDeadLockCase(LockMode.LOCK_X, LockMode.LOCK_U);
    }

    @Test
    public void testRowDeadLockUU() {
        testRowDeadLockCase(LockMode.LOCK_U, LockMode.LOCK_U);
    }


    @Test
    public void testRowDeadLockXX() {
        testRowDeadLockCase(LockMode.LOCK_X, LockMode.LOCK_X);
    }

    @Test
    public void testSavepointRowDeadLockSX() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        testRowDeadLockCase(LockMode.LOCK_S, LockMode.LOCK_X);
        testRowDeadLockCase(LockMode.LOCK_X, LockMode.LOCK_S);
    }

    @Test
    public void testSavepointRowDeadLockUX() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        testRowDeadLockCase(LockMode.LOCK_U, LockMode.LOCK_X);
        testRowDeadLockCase(LockMode.LOCK_X, LockMode.LOCK_U);
    }

    @Test
    public void testSavepointRowDeadLockUU() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        testRowDeadLockCase(LockMode.LOCK_U, LockMode.LOCK_U);
    }

    @Test
    public void testSavepointRowDeadLockXX() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        testRowDeadLockCase(LockMode.LOCK_X, LockMode.LOCK_X);
    }

    @Test
    public void testNestedSavepointRowDeadLockSX() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testRowDeadLockCase(LockMode.LOCK_S, LockMode.LOCK_X);
        testRowDeadLockCase(LockMode.LOCK_X, LockMode.LOCK_S);
    }

    @Test
    public void testNestedSavepointRowDeadLockUX() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testRowDeadLockCase(LockMode.LOCK_U, LockMode.LOCK_X);
        testRowDeadLockCase(LockMode.LOCK_X, LockMode.LOCK_U);
    }

    @Test
    public void testNestedSavepointRowDeadLockUU() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testRowDeadLockCase(LockMode.LOCK_U, LockMode.LOCK_U);;
    }

    @Test
    public void testNestedSavepointRowDeadLockXX() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testRowDeadLockCase(LockMode.LOCK_X, LockMode.LOCK_X);
    }

    private void testRowDeadLockCase(int lockMode, int lockMode1) {
        String message = "test table dead lock " + getLockModeName(lockMode) + " " + getLockModeName(lockMode1);
        System.out.println("------------------- " + message + " start ---------------------");

        lockObject.setTxID(1);
        lockObject.setLockMode(lockMode);
        lockObject1.setTxID(2);
        lockObject1.setLockMode(lockMode1);
        assertTrue(LockUtil.acquireLock(lockManager, lockObject));
        assertTrue(LockUtil.acquireLock(lockManager1, lockObject1));

        lockObject.setTxID(2);
        lockObject.setLockMode(lockMode1);
        lockObject1.setTxID(1);
        lockObject1.setLockMode(lockMode);

        Future<Boolean> future = threadPool.submit(new LockAcquireTask(lockManager1, lockObject1));
        Future<Boolean> future1 = threadPool.submit(new LockAcquireTask(lockManager, lockObject));

        try {
            assertFalse(future1.get());
            lockObject1.setTxID(2);
            releaseAll(lockManager1, lockObject1);
            assertTrue(checkTxLockNum(lockObject1, 0));
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }

        try {
            assertTrue(future.get());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }

        lockObject.setTxID(1);
        releaseAll(lockManager, lockObject);
        assertTrue(lockManager.validateAllLocks(message));

        lockObject.setTxID(1);
        releaseAll(lockManager1, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));
        assertTrue(checkLockApply(0));
        assertTrue(lockManager1.validateAllLocks(message));

        System.out.println("-------------------" + message + " end ---------------------");
    }
}