package org.apache.hadoop.hbase.coprocessor.transactional.lock.conflict;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RowKey;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockAcquireTask;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
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
public class TestLockConflictWithThread extends BaseTest {
    private LockManager lockManager;
    private LockObject lockObject;
    private LockObject lockObject1;
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
        if (lockObject == null) {
            lockObject = new LockObject(1, "seabase.trafodion.test,aaa", "00001", LockMode.LOCK_IS);
            lockObject.setType(2);
        }
        if (lockObject1 == null) {
            lockObject1 = new LockObject(2, "seabase.trafodion.test,aaa", "00001", LockMode.LOCK_IS);
            lockObject1.setType(2);
        }
    }

    private void testCaseForTable(String message) {
        lockObject.setRowID(null);
        lockObject1.setRowID(null);
        testCase(message);
    }

    private void testCaseForRow(String message) {
        lockObject.setRowID(new RowKey("0001"));
        lockObject1.setRowID(new RowKey("0001"));
        testCase(message);
    }

    private void testCase(String message) {
        System.out.println("-------------------" + message + " start ---------------------");
        int lockMode = lockObject.getLockMode();
        int lockMode1 = lockObject1.getLockMode();
        int idx = LockMode.getIndex(lockMode);
        boolean isConflict = false;
        if ((LockConstants.defaultLockConflictsMatrix[idx] & lockMode1) == lockMode1) {
            isConflict = true;
        }
        Future<Boolean> future = threadPool.submit(new LockAcquireTask(lockManager, lockObject));
        try {
            assertTrue(future.get());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        future = threadPool.submit(new LockAcquireTask(lockManager, lockObject1));
        if (isConflict) {
            try {
                System.out.println("sleep 2s");
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            assertTrue(checkLockWait(lockObject1.getTxID(), lockMode1));
        }

        LockUtil.lockReleaseAll(lockManager, lockObject);
        assertTrue(checkTxLockNum(lockObject, 0));
        try {
            assertTrue(future.get());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        LockUtil.lockReleaseAll(lockManager, lockObject1);
        assertTrue(checkTxLockNum(lockObject1, 0));
        assertTrue(checkLockApply(0));
        assertTrue(lockManager.validateAllLocks(message));
        System.out.println("-------------------" + message + " end ---------------------");
    }

    @Test
    public void testLockIS() {
        lockObject.setLockMode(LockMode.LOCK_IS);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IS IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table lock IS IS");
        // IS S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table lock IS S");
        // IS IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table lock IS IX");
        // IS U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table lock IS U");
        // IS X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table lock IS X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IS IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row lock IS IS");
        // IS S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row lock IS S");
        // IS IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row lock IS IX");
        // IS U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row lock IS U");
        // IS X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row lock IS X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockS() {
        lockObject.setLockMode(LockMode.LOCK_S);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // S IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table lock S IS");
        // S S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table lock S S");
        // S IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table lock S IX");
        // S U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table lock S U");
        // S X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table lock S X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // S IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row lock S IS");
        // S S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row lock S S");
        // S IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row lock S IX");
        // S U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row lock S U");
        // S X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row lock S X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockIX() {
        lockObject.setLockMode(LockMode.LOCK_IX);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IX IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table lock IX IS");
        // IX S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table lock IX S");
        // IX IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table lock IX IX");
        // IX U 不支持
/*        lockObject1.setLockMode(LockMode.LOCK_U);
        testCase("test table lock IX U");*/
        // IX X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table lock IX X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IX IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row lock IX IS");
        // IX S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row lock IX S");
        // IX IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row lock IX IX");
        // IX U 不支持
 /*       lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row lock IX U");*/
        // IX X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row lock IX X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockU() {
        lockObject.setLockMode(LockMode.LOCK_U);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // U IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table lock U IS");
        // U S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table lock U S");
        // U IX
        /*lockObject1.setLockMode(LockMode.LOCK_IX);
        testCase("test table lock U IX");*/
        // U U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table lock U U");
        // U X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table lock U X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // U IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row lock U IS");
        // U S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row lock U S");
        // U IX
        /*lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row lock U IX");*/
        // U U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row lock U U");
        // U X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row lock U X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testLockX() {
        lockObject.setLockMode(LockMode.LOCK_X);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // X IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table lock X IS");
        // X S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table lock X S");
        // X IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table lock X IX");
        // X U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table lock X U");
        // X X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table lock X X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // X IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row lock X IS");
        // X S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row lock X S");
        // X IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row lock X IX");
        // X U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row lock X U");
        // X X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row lock X X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testSavepointLockIS() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_IS);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IS IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table savepoint lock IS IS");
        // IS S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table savepoint lock IS S");
        // IS IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table savepoint lock IS IX");
        // IS U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table savepoint lock IS U");
        // IS X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table savepoint lock IS X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IS IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row savepoint lock  IS IS");
        // IS S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row savepoint lock IS S");
        // IS IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row savepoint lock IS IX");
        // IS U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row savepoint lock IS U");
        // IS X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row savepoint lock IS X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testSavepointLockS() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_S);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // S IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table savepoint lock S IS");
        // S S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table savepoint lock S S");
        // S IX
        testCaseForTable("test table savepoint lock S IX");
        // S U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table savepoint lock S U");
        // S X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table savepoint lock S X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // S IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row savepoint lock S IS");
        // S S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row savepoint lock S S");
        // S IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row savepoint lock S IX");
        // S U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row savepoint lock S U");
        // S X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row savepoint lock S X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testSavepointLockIX() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_IX);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IX IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table savepoint lock IX IS");
        // IX S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table savepoint lock IX S");
        // IX IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table savepoint lock IX IX");
        // IX U 不支持
/*        lockObject1.setLockMode(LockMode.LOCK_U);
        testCase("test table savepoint lock IX U");*/
        // IX X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table savepoint lock IX X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IX IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row savepoint lock IX IS");
        // IX S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row savepoint lock IX S");
        // IX IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row savepoint lock IX IX");
        // IX U 不支持
 /*       lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row savepoint lock IX U");*/
        // IX X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row savepoint lock IX X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testSavepointLockU() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_U);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // U IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table savepoint lock U IS");
        // U S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table savepoint lock U S");
        // U IX
        /*lockObject1.setLockMode(LockMode.LOCK_IX);
        testCase("test table savepoint lock U IX");*/
        // U U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table savepoint lock U U");
        // U X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table savepoint lock U X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // U IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row savepoint lock lock U IS");
        // U S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row savepoint lock lock U S");
        // U IX
        /*lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row savepoint lock U IX");*/
        // U U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row savepoint lock lock U U");
        // U X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row savepoint lock lock U X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testSavepointLockX() {
        lockObject.setSvptID(1);
        lockObject1.setSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_X);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // X IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table savepoint lock X IS");
        // X S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table savepoint lock X S");
        // X IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table savepoint lock X IX");
        // X U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table savepoint lock X U");
        // X X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table savepoint lock X X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // X IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row savepoint lock X IS");
        // X S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row savepoint lock X S");
        // X IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row savepoint lock X IX");
        // X U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row savepoint lock X U");
        // X X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row savepoint lock X X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testNestedSavepointLockIS() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_IS);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IS IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table nexted savepoint lock IS IS");
        // IS S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table nexted savepoint lock IS S");
        // IS IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table nexted savepoint lock IS IX");
        // IS U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table nexted savepoint lock IS U");
        // IS X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table nexted savepoint lock IS X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IS IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row nexted savepoint lock  IS IS");
        // IS S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row nexted savepoint lock IS S");
        // IS IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row nexted savepoint lock IS IX");
        // IS U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row nexted savepoint lock IS U");
        // IS X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row nexted savepoint lock IS X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testNestedSavepointLockS() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_S);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // S IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table nexted savepoint lock S IS");
        // S S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table nexted savepoint lock S S");
        // S IX
        testCaseForTable("test table nexted savepoint lock S IX");
        // S U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table nexted savepoint lock S U");
        // S X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table nexted savepoint lock S X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // S IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row nexted savepoint lock S IS");
        // S S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row nexted savepoint lock S S");
        // S IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row nexted savepoint lock S IX");
        // S U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row nexted savepoint lock S U");
        // S X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row nexted savepoint lock S X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testNestedSavepointLockIX() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_IX);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // IX IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table nexted savepoint lock IX IS");
        // IX S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table nexted savepoint lock IX S");
        // IX IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table nexted savepoint lock IX IX");
        // IX U 不支持
/*        lockObject1.setLockMode(LockMode.LOCK_U);
        testCase("test table nexted savepoint lock IX U")*/
        // IX X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table nexted savepoint lock IX X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // IX IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row nexted savepoint lock IX IS");
        // IX S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row nexted savepoint lock IX S");
        // IX IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row nexted savepoint lock IX IX");
        // IX U 不支持
 /*       lockObject1.setLockMode(LockMode.LOCK_U);
       testCaseForRow("test row nexted savepoint lock IX U");*/
        // IX X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row nexted savepoint lock IX X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testNestedSavepointLockU() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_U);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // U IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table nexted savepoint lock U IS");
        // U S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table nexted savepoint lock U S");
        // U IX
        /*lockObject1.setLockMode(LockMode.LOCK_IX);
        testCase("test table nexted savepoint lock U IX");*/
        // U U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table nexted savepoint lock U U");
        // U X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table nexted savepoint lock U X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // U IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row nexted savepoint lock lock U IS");
        // U S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row nexted savepoint lock lock U S");
        // U IX
        /*lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row nexted savepoint lock U IX");*/
        // U U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row nexted savepoint lock lock U U");
        // U X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row nexted savepoint lock lock U X");
        // ROW LOCK END ............................................
    }

    @Test
    public void testNestedSavepointLockX() {
        lockObject.setSvptID(2);
        lockObject1.setSvptID(2);
        lockObject1.setParentSvptID(1);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(LockMode.LOCK_X);
        // TABLE LOCK
        // TABLE LOCK BEGIN...............................................
        // X IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForTable("test table nexted savepoint lock X IS");
        // X S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForTable("test table nexted savepoint lock X S");
        // X IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForTable("test table nexted savepoint lock X IX");
        // X U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForTable("test table nexted savepoint lock X U");
        // X X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForTable("test table nexted savepoint lock X X");
        // TABLE LOCK END....................................................
        // ROW LOCK
        // ROW LOCK BEGIN ............................................
        // X IS
        lockObject1.setLockMode(LockMode.LOCK_IS);
        testCaseForRow("test row nexted savepoint lock X IS");
        // X S
        lockObject1.setLockMode(LockMode.LOCK_S);
        testCaseForRow("test row nexted savepoint lock X S");
        // X IX
        lockObject1.setLockMode(LockMode.LOCK_IX);
        testCaseForRow("test row nexted savepoint lock X IX");
        // X U
        lockObject1.setLockMode(LockMode.LOCK_U);
        testCaseForRow("test row nexted savepoint lock X U");
        // X X
        lockObject1.setLockMode(LockMode.LOCK_X);
        testCaseForRow("test row nexted savepoint lock X X");
        // ROW LOCK END ............................................
    }
}
