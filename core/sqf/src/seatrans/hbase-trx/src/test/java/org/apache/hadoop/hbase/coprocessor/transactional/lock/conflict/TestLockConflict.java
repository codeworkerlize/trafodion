package org.apache.hadoop.hbase.coprocessor.transactional.lock.conflict;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 测试释放锁接口lockRelease
 */
@FixMethodOrder(MethodSorters.JVM)
public class TestLockConflict extends BaseTest {

    @Before
    public void init() {
        super.init();
        if (lockManager == null) {
            lockManager = new LockManager("seabase.trafodion.test,aaa", false);
        }
        if (lockObject == null) {
            lockObject = new LockObject(1, "seabase.trafodion.test,aaa", "00001", LockMode.LOCK_IS);
        }
        if (lockObject1 == null) {
            lockObject1 = new LockObject(2, "seabase.trafodion.test,aaa", "00001", LockMode.LOCK_IS);
        }
    }

    @Test
    public void testLockIS() {
        lockObject.setLockMode(LockMode.LOCK_IS);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IS IS
        testTableLockConflict(LockMode.LOCK_IS, LockMode.LOCK_IS);
        // IS S
        testTableLockConflict(LockMode.LOCK_IS, LockMode.LOCK_S);
        // IS IX
        testTableLockConflict(LockMode.LOCK_IS, LockMode.LOCK_IX);
        // IS U
        testTableLockConflict(LockMode.LOCK_IS, LockMode.LOCK_U);
        // IS X
        testTableLockConflict(LockMode.LOCK_IS, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IS IS
        testRowLockConflict(LockMode.LOCK_IS, LockMode.LOCK_IS);
        // IS S
        testRowLockConflict(LockMode.LOCK_IS, LockMode.LOCK_S);
        // IS IX
        testRowLockConflict(LockMode.LOCK_IS, LockMode.LOCK_IX);
        // IS U
        testRowLockConflict(LockMode.LOCK_IS, LockMode.LOCK_U);
        // IS X
        testRowLockConflict(LockMode.LOCK_IS, LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockS() {
        lockObject.setLockMode(LockMode.LOCK_S);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // S IS
        testTableLockConflict(LockMode.LOCK_S, LockMode.LOCK_IS);
        // S S
        testTableLockConflict(LockMode.LOCK_S, LockMode.LOCK_S);
        // S IX
        testTableLockConflict(LockMode.LOCK_S, LockMode.LOCK_IX);
        // S U
        testTableLockConflict(LockMode.LOCK_S, LockMode.LOCK_U);
        // S X
        testTableLockConflict(LockMode.LOCK_S, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // S IS
        testRowLockConflict(LockMode.LOCK_S, LockMode.LOCK_IS);
        // S S
        testRowLockConflict(LockMode.LOCK_S, LockMode.LOCK_S);
        // S IX
        testRowLockConflict(LockMode.LOCK_S, LockMode.LOCK_IX);
        // S U
        testRowLockConflict(LockMode.LOCK_S, LockMode.LOCK_U);
        // S X
        testRowLockConflict(LockMode.LOCK_S, LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockIX() {
        lockObject.setLockMode(LockMode.LOCK_IX);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IX IS
        testTableLockConflict(LockMode.LOCK_IX, LockMode.LOCK_IS);
        // IX S
        testTableLockConflict(LockMode.LOCK_IX, LockMode.LOCK_S);
        // IX IX
        testTableLockConflict(LockMode.LOCK_IX, LockMode.LOCK_IX);
        // IX U not support
        //testTableLock(LockMode.LOCK_IX, LockMode.LOCK_U);
        // IX X
        testTableLockConflict(LockMode.LOCK_IX, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IX IS
        testRowLockConflict(LockMode.LOCK_IX, LockMode.LOCK_IS);
        // IX S
        testRowLockConflict(LockMode.LOCK_IX, LockMode.LOCK_S);
        // IX IX
        testRowLockConflict(LockMode.LOCK_IX, LockMode.LOCK_IX);
        // IX U not support
        // testRowLock(LockMode.LOCK_IX, LockMode.LOCK_U);
        // IX X
        testRowLockConflict(LockMode.LOCK_IX, LockMode.LOCK_X);
       // ROW LOCK END ............................................
    }

    @Test
    public void testLockU() {
        lockObject.setLockMode(LockMode.LOCK_U);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // U IS
        testTableLockConflict(LockMode.LOCK_U, LockMode.LOCK_IS);
        // U S
        testTableLockConflict(LockMode.LOCK_U, LockMode.LOCK_S);
        // U IX not support
        // testTableLock(LockMode.LOCK_U, LockMode.LOCK_IX);
        // U U
        testTableLockConflict(LockMode.LOCK_U, LockMode.LOCK_U);
        // U X
        testTableLockConflict(LockMode.LOCK_U, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // U IS
        testRowLockConflict(LockMode.LOCK_U, LockMode.LOCK_IS);
        // U S
        testRowLockConflict(LockMode.LOCK_U, LockMode.LOCK_S);
        // U IX not support
        // testRowLock(LockMode.LOCK_U, LockMode.LOCK_IX);
        // U U
        testRowLockConflict(LockMode.LOCK_U, LockMode.LOCK_U);
        // U X
        testRowLockConflict(LockMode.LOCK_U, LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockX() {
        lockObject.setLockMode(LockMode.LOCK_X);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // X IS
        testTableLockConflict(LockMode.LOCK_X, LockMode.LOCK_IS);
        // X S
        testTableLockConflict(LockMode.LOCK_X, LockMode.LOCK_S);
        // X IX
        testTableLockConflict(LockMode.LOCK_X, LockMode.LOCK_IX);
        // X U
        testTableLockConflict(LockMode.LOCK_X, LockMode.LOCK_U);
        // X X
        testTableLockConflict(LockMode.LOCK_X, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // X IS
        testRowLockConflict(LockMode.LOCK_X, LockMode.LOCK_IS);
        // X S
        testRowLockConflict(LockMode.LOCK_X, LockMode.LOCK_S);
        // X IX
        testRowLockConflict(LockMode.LOCK_X, LockMode.LOCK_IX);
        // X U
        testRowLockConflict(LockMode.LOCK_X, LockMode.LOCK_U);
        // X X
        testRowLockConflict(LockMode.LOCK_X, LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testSavepointLockIS() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject1.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_IS);
        testLockIS();
    }

    @Test
    public void testSavepointLockS() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject1.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_S);
        testLockS();
    }

    @Test
    public void testSavepointLockIX() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject1.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_IX);
        testLockIX();
    }

    @Test
    public void testSavepointLockU() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject1.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_U);
        testLockU();
    }

    @Test
    public void testSavepointLockX() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject1.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_X);
        testLockX();
    }

    @Test
    public void testNestedSavepointLockIS() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_IS);
        testLockIS();
    }

    @Test
    public void testNestedSavepointLockS() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        testLockS();
    }

    @Test
    public void testNestedSavepointLockIX() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        testLockIX();
    }

    @Test
    public void testNestedSavepointLockU() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        testLockU();
    }

    @Test
    public void testNestedSavepointLockX() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        testLockX();
    }

    private void testTableLockConflict(int lockMode, int lockMode1) {
        lockObject.setLockMode(lockMode);
        lockObject1.setLockMode(lockMode1);
        assertTrue(acquireTableLock(lockManager, lockObject, 1));
        assertTrue(checkTableLockModes(lockObject, lockMode));

        int idx = LockMode.getIndex(lockMode);
        if ((LockConstants.defaultLockConflictsMatrix[idx] & lockMode1) == lockMode1) {
            assertFalse(acquireTableLock(lockManager, lockObject1, 1));
        } else {
            assertTrue(acquireTableLock(lockManager, lockObject1, 1));
            assertTrue(checkTableLockModes(lockObject1, lockMode1));
        }
        releaseTableLock(lockManager, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));
        releaseTableLock(lockManager, lockObject1);
        assertTrue(checkTxLockNum(lockObject1, 0));
        releaseAll(lockManager, lockObject);
        releaseAll(lockManager, lockObject1);
        assertTrue(checkLockApply(0));
        assertTrue(lockManager.validateAllLocks("test table lock conflict " + getLockModeName(lockMode) + " " + getLockModeName(lockMode1)));
    }

    private void testRowLockConflict(int lockMode, int lockMode1) {
        lockObject.setLockMode(lockMode);
        lockObject1.setLockMode(lockMode1);
        assertTrue(acquireRowLock(lockManager, lockObject, 1));
        assertTrue(checkRowLockModes(lockObject, lockMode));

        int idx = LockMode.getIndex(lockMode);
        if ((LockConstants.defaultLockConflictsMatrix[idx] & lockMode1) == lockMode1) {
            assertFalse(acquireRowLock(lockManager, lockObject1, 1));
        } else {
            assertTrue(acquireRowLock(lockManager, lockObject1, 1));
            assertTrue(checkRowLockModes(lockObject1, lockMode1));
        }

        releaseRowLock(lockManager, lockObject);
        int intentionLockMode = getIntentionLockMode(lockMode);
        lockObject.setLockMode(intentionLockMode);
        releaseTableLock(lockManager, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));

        releaseRowLock(lockManager, lockObject1);
        intentionLockMode = getIntentionLockMode(lockMode1);
        lockObject1.setLockMode(intentionLockMode);
        releaseTableLock(lockManager, lockObject1);
        assertTrue(checkTxLockNum(lockObject1, 0));

        releaseAll(lockManager, lockObject);
        releaseAll(lockManager, lockObject1);
        assertTrue(checkLockApply(0));
        assertTrue(lockManager.validateAllLocks("test row lock conflict " + getLockModeName(lockMode) + " " + getLockModeName(lockMode1)));
    }
}
