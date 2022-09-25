package org.apache.hadoop.hbase.coprocessor.transactional.lock.savepoint;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockAcquireTask;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 测试两个session下锁冲突
 */
public class TestSavepointWithThread extends BaseTest {
    private LockManager lockManager;
    private LockObject lockObject;
    private LockObject lockObject1;
    private ExecutorService threadPool = null;

    @Before
    public void init() {
        LockConstants.ENABLE_ROW_LEVEL_LOCK = true;
        if (threadPool != null) {
            threadPool.shutdown();
        }
        threadPool = Executors.newFixedThreadPool(2);
        RSServer rsServer = RSServer.getInstance(null, null);
        rsServer.enableRowLevelLockFeature();
        if (lockManager == null) {
            lockManager = new LockManager("seabase.trafodion.test,aaa", false);
        }
        if (lockObject == null) {
            lockObject = new LockObject(1, "seabase.trafodion.test,aaa", "00001", LockMode.LOCK_IS);
            lockObject.setType(2);
        }
        if (lockObject1 == null) {
            lockObject1 = new LockObject(2, "seabase.trafodion.test,aaa", "00001", LockMode.LOCK_IS);
            lockObject1.setType(2);
        }
    }

    private void testCase(String message) {
        System.out.println("-------------------" + message + " start ---------------------");
        Future<Boolean> future = threadPool.submit(new LockAcquireTask(lockManager, lockObject));
        try {
            assertTrue(future.get());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        future = threadPool.submit(new LockAcquireTask(lockManager, lockObject1));
        try {
            System.out.println("sleep 2s");
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LockUtil.lockReleaseAll(lockManager, lockObject);
        try {
            assertTrue(future.get());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        LockUtil.lockReleaseAll(lockManager, lockObject1);
        assertTrue(lockManager.validateAllLocks(message));
        System.out.println("-------------------" + message + " end ---------------------");
    }

    @Test
    public void testMantis16371() {
        // 模拟事务下的显示savepoint
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(LockMode.LOCK_U);
        Future<Boolean> future = threadPool.submit(new LockAcquireTask(lockManager, lockObject));
        try {
            assertTrue(future.get());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }

        // 模拟事务下的显示savepoint
        lockObject1.setSvptID(1);
        lockObject1.setParentSvptID(-1);
        lockObject1.setImplicitSavepoint(false);
        lockObject1.setLockMode(LockMode.LOCK_U);
        future = threadPool.submit(new LockAcquireTask(lockManager, lockObject1));
        try {
            System.out.println("sleep 2s");
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LockUtil.lockReleaseSavepoint(lockManager, lockObject);
        try {
            assertTrue(future.get());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        LockUtil.lockReleaseAll(lockManager, lockObject1);

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(LockMode.LOCK_U);
        future = threadPool.submit(new LockAcquireTask(lockManager, lockObject));
        try {
            assertTrue(future.get());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }

        // 模拟事务下的显示savepoint
        lockObject1.setTxID(3);
        lockObject1.setSvptID(1);
        lockObject1.setParentSvptID(-1);
        lockObject1.setImplicitSavepoint(false);
        lockObject1.setLockMode(LockMode.LOCK_U);
        future = threadPool.submit(new LockAcquireTask(lockManager, lockObject1));
        try {
            assertFalse(future.get());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

}
