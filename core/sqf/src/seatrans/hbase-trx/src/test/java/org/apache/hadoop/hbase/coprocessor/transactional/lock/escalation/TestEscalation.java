package org.apache.hadoop.hbase.coprocessor.transactional.lock.escalation;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RowKey;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.JVM)
public class TestEscalation extends BaseTest {
    @Before
    public void init() {
        super.init();
        if (lockManager == null) {
            lockManager = new LockManager("seabase.trafodion.test,aaa", false);
        }
        if (lockObject == null) {
            lockObject = new LockObject(1, "seabase.trafodion.test,aaa", "00001", LockMode.LOCK_IS);
        }
        LockConstants.LOCK_ESCALATION_THRESHOLD = 99;
    }

    @Test
    public void testLockSEscalation() {
        testLockEscalation(LockMode.LOCK_S);
    }

    @Test
    public void testLockUEscalation() {
        testLockEscalation(LockMode.LOCK_U);
    }

    @Test
    public void testLockXEscalation() {
        testLockEscalation(LockMode.LOCK_X);
    }

    @Test
    public void testSavepointLockSEscalation() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        testLockEscalation(LockMode.LOCK_S);
    }

    @Test
    public void testSavepointLockUEscalation() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        testLockEscalation(LockMode.LOCK_U);
    }

    @Test
    public void testSavepointLockXEscalation() {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        testLockEscalation(LockMode.LOCK_X);
    }

    @Test
    public void testNestedSavepointLockSEscalation() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testLockEscalation(LockMode.LOCK_S);
    }

    @Test
    public void testNestedSavepointLockUEscalation() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testLockEscalation(LockMode.LOCK_U);
    }

    @Test
    public void testNestedSavepointLockXEscalation() {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        testLockEscalation(LockMode.LOCK_X);
    }

    @Test
    public void testLockSUEscalation() {
        testLockEscalation(LockMode.LOCK_S, LockMode.LOCK_U);
        testLockEscalation(LockMode.LOCK_U, LockMode.LOCK_S);
    }

    @Test
    public void testLockSXEscalation() {
        testLockEscalation(LockMode.LOCK_S, LockMode.LOCK_X);
        testLockEscalation(LockMode.LOCK_X, LockMode.LOCK_S);
    }

    @Test
    public void testLockUXEscalation() {
        testLockEscalation(LockMode.LOCK_U, LockMode.LOCK_X);
        testLockEscalation(LockMode.LOCK_X, LockMode.LOCK_U);
    }

    private void testLockEscalation(int lockMode) {
        lockObject.setLockMode(lockMode);
        int i = 1;
        for (; i <= LockConstants.LOCK_ESCALATION_THRESHOLD; i++) {
            lockObject.setRowID(new RowKey("0000" + i));
            assertTrue(acquireRowLock(lockManager, lockObject, 1));
        }
        assertTrue(checkTxLockNum(lockObject, i));
        assertTrue(checkRowLockModes(lockObject, lockObject.getLockMode()));
        assertFalse(checkTableLockModes(lockObject, lockObject.getLockMode()));

        // after escalation
        lockObject.setRowID(new RowKey("0000" + i));
        assertTrue(acquireRowLock(lockManager, lockObject, 1));
        assertTrue(checkTxLockNum(lockObject, 2));
        if (lockMode == LockMode.LOCK_U) {
            lockObject.setLockMode(LockMode.LOCK_X);
        }
        assertTrue(checkTableLockModes(lockObject, lockObject.getLockMode()));
        assertFalse(checkRowLockModes(lockObject, lockObject.getLockMode()));
        releaseAll(lockManager, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));
        assertTrue(checkLockApply(0));
        assertTrue(lockManager.validateAllLocks("test table lock escalation " + getLockModeName(lockMode)));
    }

    private void testLockEscalation(int lockMode, int lockMode1) {
        int escaLockMode = ((lockMode == LockMode.LOCK_U) ? LockMode.LOCK_X : lockMode);
        boolean isSameIntentionLock = (getIntentionLockMode(lockMode) == getIntentionLockMode(lockMode1));
        lockObject.setLockMode(lockMode);
        int i = 1;
        for (; i <= LockConstants.LOCK_ESCALATION_THRESHOLD/2 + 1; i++) {
            lockObject.setRowID(new RowKey("0000" + i));
            assertTrue(acquireRowLock(lockManager, lockObject, 1));
        }
        assertTrue(checkTxLockNum(lockObject, i));
        assertTrue(checkRowLockModes(lockObject, lockObject.getLockMode()));
        assertFalse(checkTableLockModes(lockObject, lockObject.getLockMode()));

        // another lock mode trigger escalation
        int expectedRowNumAfterEsca = 0;
        lockObject.setLockMode(lockMode1);
        for (; i <= LockConstants.LOCK_ESCALATION_THRESHOLD; i++) {
            lockObject.setRowID(new RowKey("0000" + i));
            assertTrue(acquireRowLock(lockManager, lockObject, 1));
            expectedRowNumAfterEsca += 1;
        }
        if (isSameIntentionLock) {
            assertTrue(checkTxLockNum(lockObject, i));
        } else {
            assertTrue(checkTxLockNum(lockObject, i + 1));
        }
        assertTrue(checkRowLockModes(lockObject, lockObject.getLockMode()));
        assertFalse(checkTableLockModes(lockObject, lockObject.getLockMode()));
        expectedRowNumAfterEsca += 1;

        lockObject.setRowID(new RowKey("0000" + i));
        assertTrue(acquireRowLock(lockManager, lockObject, 1));
        if (escaLockMode == LockMode.LOCK_X) {
            if (isSameIntentionLock) {
                assertTrue(checkTxLockNum(lockObject, 2));
            } else {
                assertTrue(checkTxLockNum(lockObject, 3));
            }
        } else {
            assertTrue(checkTxLockNum(lockObject, expectedRowNumAfterEsca + 3));
        }
        if (lockMode == LockMode.LOCK_U) {
            lockObject.setLockMode(LockMode.LOCK_X);
        } else {
            lockObject.setLockMode(lockMode);
        }
        assertTrue(checkTableLockModes(lockObject, lockObject.getLockMode()));
        assertFalse(checkRowLockModes(lockObject, lockObject.getLockMode()));
        releaseAll(lockManager, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));
        assertTrue(checkLockApply(0));
        assertTrue(lockManager.validateAllLocks("test table lock escalation " + getLockModeName(lockMode) + " " + getLockModeName(lockMode1)));
    }
}
