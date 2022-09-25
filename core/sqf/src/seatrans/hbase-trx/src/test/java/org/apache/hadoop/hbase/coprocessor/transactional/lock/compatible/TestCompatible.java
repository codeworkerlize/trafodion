package org.apache.hadoop.hbase.coprocessor.transactional.lock.compatible;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.JVM)
public class TestCompatible extends BaseTest {
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
    public void testLockIS() {
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IS IS
        testTableLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_IS);
        // IS S
        testTableLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_S);
        // IS IX
        testTableLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_IX);
        // IS U
        testTableLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_U);
        // IS X
        testTableLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IS IS
        testRowLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_IS);
        // IS S
        testRowLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_S);
        // IS IX
        testRowLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_IX);
        // IS U
        testRowLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_U);
        // IS X
        testRowLockCompatible(LockMode.LOCK_IS, LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockS() {
        lockObject.setLockMode(LockMode.LOCK_S);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // S IS
        testTableLockCompatible(LockMode.LOCK_S, LockMode.LOCK_IS);
        // S S
        testTableLockCompatible(LockMode.LOCK_S, LockMode.LOCK_S);
        // S IX
        testTableLockCompatible(LockMode.LOCK_S, LockMode.LOCK_IX);
        // S U
        testTableLockCompatible(LockMode.LOCK_S, LockMode.LOCK_U);
        // S X
        testTableLockCompatible(LockMode.LOCK_S, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // S IS
        testRowLockCompatible(LockMode.LOCK_S, LockMode.LOCK_IS);
        // S S
        testRowLockCompatible(LockMode.LOCK_S, LockMode.LOCK_S);
        // S IX
        testRowLockCompatible(LockMode.LOCK_S, LockMode.LOCK_IX);
        // S U
        testRowLockCompatible(LockMode.LOCK_S, LockMode.LOCK_U);
        // S X
        testRowLockCompatible(LockMode.LOCK_S, LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockIX() {
        lockObject.setLockMode(LockMode.LOCK_IX);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IX IS not supported
//        testTableLockCompatible(LockMode.LOCK_IX, LockMode.LOCK_IS);
        // IX S
        testTableLockCompatible(LockMode.LOCK_IX, LockMode.LOCK_S);
        // IX IX
        testTableLockCompatible(LockMode.LOCK_IX, LockMode.LOCK_IX);
        // IX U not support
        //testTableLock(LockMode.LOCK_IX, LockMode.LOCK_U);
        // IX X
        testTableLockCompatible(LockMode.LOCK_IX, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IX IS not supported
//        testRowLockCompatible(LockMode.LOCK_IX, LockMode.LOCK_IS);
        // IX S
        testRowLockCompatible(LockMode.LOCK_IX, LockMode.LOCK_S);
        // IX IX
        testRowLockCompatible(LockMode.LOCK_IX, LockMode.LOCK_IX);
        // IX U not support
        // testRowLock(LockMode.LOCK_IX, LockMode.LOCK_U);
        // IX X
        testRowLockCompatible(LockMode.LOCK_IX, LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockU() {
        lockObject.setLockMode(LockMode.LOCK_U);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // U IS is not supported
//        testTableLockCompatible(LockMode.LOCK_U, LockMode.LOCK_IS);
        // U S
        testTableLockCompatible(LockMode.LOCK_U, LockMode.LOCK_S);
        // U IX not support
        // testTableLock(LockMode.LOCK_U, LockMode.LOCK_IX);
        // U U
        testTableLockCompatible(LockMode.LOCK_U, LockMode.LOCK_U);
        // U X
        testTableLockCompatible(LockMode.LOCK_U, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // U IS is not supported
//        testRowLockCompatible(LockMode.LOCK_U, LockMode.LOCK_IS);
        // U S
        testRowLockCompatible(LockMode.LOCK_U, LockMode.LOCK_S);
        // U IX not support
        // testRowLock(LockMode.LOCK_U, LockMode.LOCK_IX);
        // U U
        testRowLockCompatible(LockMode.LOCK_U, LockMode.LOCK_U);
        // U X
        testRowLockCompatible(LockMode.LOCK_U, LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockX() {
        lockObject.setLockMode(LockMode.LOCK_X);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // X IS
        testTableLockCompatible(LockMode.LOCK_X, LockMode.LOCK_IS);
        // X S
        testTableLockCompatible(LockMode.LOCK_X, LockMode.LOCK_S);
        // X IX
        testTableLockCompatible(LockMode.LOCK_X, LockMode.LOCK_IX);
        // X U
        testTableLockCompatible(LockMode.LOCK_X, LockMode.LOCK_U);
        // X X
        testTableLockCompatible(LockMode.LOCK_X, LockMode.LOCK_X);
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // X IS
        testRowLockCompatible(LockMode.LOCK_X, LockMode.LOCK_IS);
        // X S
        testRowLockCompatible(LockMode.LOCK_X, LockMode.LOCK_S);
        // X IX
        testRowLockCompatible(LockMode.LOCK_X, LockMode.LOCK_IX);
        // X U
        testRowLockCompatible(LockMode.LOCK_X, LockMode.LOCK_U);
        // X X
        testRowLockCompatible(LockMode.LOCK_X, LockMode.LOCK_X);
        // ROW LOCK END ............................................
    }

    @Test
    public void testSavepointLockIS() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_IS);
        testLockIS();
    }

    @Test
    public void testSavepointLockS() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_S);
        testLockS();
    }

    @Test
    public void testSavepointLockIX() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_IX);
        testLockIX();
    }

    @Test
    public void testSavepointLockU() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_U);
        testLockU();
    }

    @Test
    public void testSavepointLockX() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setLockMode(LockMode.LOCK_X);
        testLockX();
    }

    @Test
    public void testNestedSavepointLockIS() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_IS);
        testLockIS();
    }

    @Test
    public void testNestedSavepointLockS() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testLockS();
    }

    @Test
    public void testNestedSavepointLockIX() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testLockIX();
    }

    @Test
    public void testNestedSavepointLockU() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testLockU();
    }

    @Test
    public void testNestedSavepointLockX() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testLockX();
    }
    
    private void testTableLockCompatible(int lockMode, int lockMode1) {
        lockObject.setLockMode(lockMode);
        assertTrue(acquireTableLock(lockManager, lockObject, 1));
        assertTrue(checkTableLockModes(lockObject, lockMode));

        lockObject.setLockMode(lockMode1);
        assertTrue(acquireTableLock(lockManager, lockObject, 1));
        int idx = LockMode.getIndex(lockMode);
        if ((lockMode != lockMode1) && ((LockConstants.defaultLockContainMatrix[idx] & lockMode1) == lockMode1)) {
            assertFalse(checkTableLockModes(lockObject, lockMode1));
        } else {
            assertTrue(checkTableLockModes(lockObject, lockMode1));
            if (lockMode != lockMode1) {
                releaseTableLock(lockManager, lockObject);
            }
        }
        lockObject.setLockMode(lockMode);
        releaseTableLock(lockManager, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));
        assertTrue(checkLockApply(0));

        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
        assertTrue(checkLockApply(0));
        assertTrue(lockManager.validateAllLocks("test table lock compatible " + getLockModeName(lockMode) + " " + getLockModeName(lockMode1)));
    }

    private void testRowLockCompatible(int lockMode, int lockMode1) {
        lockObject.setLockMode(lockMode);
        assertTrue(acquireRowLock(lockManager, lockObject, 1));
        assertTrue(checkRowLockModes(lockObject, lockMode));

        lockObject.setLockMode(lockMode1);
        assertTrue(acquireRowLock(lockManager, lockObject, 1));
        int idx = LockMode.getIndex(lockMode);
        if ((lockMode != lockMode1) && ((LockConstants.defaultLockContainMatrix[idx] & lockMode1) == lockMode1)) {
            assertFalse(checkRowLockModes(lockObject, lockMode1));
        } else {
            assertTrue(checkRowLockModes(lockObject, lockMode1));
            if (lockMode != lockMode1) {
                releaseRowLock(lockManager, lockObject);
            }
        }
        int intentionLockMode = getIntentionLockMode(lockMode1);
        lockObject.setLockMode(intentionLockMode);
        releaseTableLock(lockManager, lockObject);

        lockObject.setLockMode(lockMode);
        releaseRowLock(lockManager, lockObject);
        intentionLockMode = getIntentionLockMode(lockMode);
        lockObject.setLockMode(intentionLockMode);
        releaseTableLock(lockManager, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));
        assertTrue(checkLockApply(0));

        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
        assertTrue(checkLockApply(0));
        assertTrue(lockManager.validateAllLocks("test table lock compatible " + getLockModeName(lockMode) + " " + getLockModeName(lockMode1)));
    }
}
