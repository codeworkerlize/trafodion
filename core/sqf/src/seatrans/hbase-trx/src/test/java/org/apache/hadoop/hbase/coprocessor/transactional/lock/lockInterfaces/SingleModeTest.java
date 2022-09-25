package org.apache.hadoop.hbase.coprocessor.transactional.lock.lockInterfaces;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.JVM)
public class SingleModeTest extends BaseTest {
    private LockManager lockManager;
    private LockObject lockObject;

    @Before
    public void init() {
        super.init();
        if (lockManager == null) {
            lockManager = new LockManager("seabase.trafodion.test,aaa", false);
        }
        if (lockObject == null) {
            lockObject = new LockObject(1, "seabase.trafodion.test,aaa", "00001", LockMode.LOCK_IS);
        }
    }

    @Test
    public void testTableLock() {
        lockObject.setLockMode(LockMode.LOCK_IS);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IS
        testTableLockSingleMode(LockMode.LOCK_IS);
        // S
        testTableLockSingleMode(LockMode.LOCK_S);
        // IX
        testTableLockSingleMode(LockMode.LOCK_IX);
        // U
        testTableLockSingleMode(LockMode.LOCK_U);
        // X
        testTableLockSingleMode(LockMode.LOCK_X);
        // TABLE LOCK END....................................................
    }

    @Test
    public void testRowLock() {
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IS
        testRowLockSingleMode(LockMode.LOCK_IS);
        // S
        testRowLockSingleMode(LockMode.LOCK_S);
        // IX
        testRowLockSingleMode(LockMode.LOCK_IX);
        // U
        testRowLockSingleMode(LockMode.LOCK_U);
        // X
        testRowLockSingleMode(LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testSavepointTableLock() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        testTableLock();
    }

    @Test
    public void testSavepointRowLock() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        testRowLock();
    }

    @Test
    public void testNestedSavepointTableLock() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testTableLock();
    }

    @Test
    public void testNestedSavepointRowLock() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testRowLock();
    }

    private void testTableLockSingleMode(int lockMode) {
        lockObject.setLockMode(lockMode);
        assertTrue(acquireTableLock(lockManager, lockObject, 1));
        assertTrue(checkTableLockModes(lockObject, lockMode));
        releaseTableLock(lockManager, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));
        assertTrue(checkLockApply(0));
        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
        assertTrue(checkLockApply(0));
        assertTrue(lockManager.validateAllLocks("test table lock single lock mode " + getLockModeName(lockMode)));
    }

    private void testRowLockSingleMode(int lockMode) {
        lockObject.setLockMode(lockMode);
        assertTrue(acquireRowLock(lockManager, lockObject, 1));
        assertTrue(checkRowLockModes(lockObject, lockMode));
        releaseRowLock(lockManager, lockObject);
        int intentionLockMode = getIntentionLockMode(lockMode);
        lockObject.setLockMode(intentionLockMode);
        releaseTableLock(lockManager, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));
        assertTrue(checkLockApply(0));
        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
        assertTrue(checkLockApply(0));
        assertTrue(lockManager.validateAllLocks("test table lock single lock mode " + getLockModeName(lockMode)));
    }
}
