package org.apache.hadoop.hbase.coprocessor.transactional.lock.multisavepoint;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RowKey;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultiSavepointTest extends BaseTest {
    private LockManager lockManager;
    private LockObject lockObject;
    private LockObject lockObject1;
    private static int COMMIT = 1;
    private static int ROLLBACK = 2;

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

    /**
     * 2级嵌套显示隐式savepoint
     */
    @Test
    public void Test2LevelSavepoint1() {
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(-1);
        lockObject.setParentSvptID(0);
        lockObject.setLockMode(2);
        acquireTableLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(-1);
        lockObject.setLockMode(2);
        releaseTableLock(lockManager, lockObject);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(-1);
        lockObject.setParentSvptID(0);
        lockObject.setLockMode(1);
        acquireTableLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("D51AE000817D661D"));
        acquireRowLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setLockMode(16);
        lockObject.setRowID(new RowKey("D51AE000817D661D"));
        acquireRowLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(-1);
        lockObject.setParentSvptID(-1);
        lockObject.setLockMode(2);
        acquireTableLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(-1);
        lockObject.setLockMode(2);
        releaseTableLock(lockManager, lockObject);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(-1);
        lockObject.setParentSvptID(-1);
        lockObject.setLockMode(1);
        acquireTableLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(3);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("D51AE000817F4A48"));
        acquireRowLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(3);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(16);
        lockObject.setRowID(new RowKey("D51AE000817F4A48"));
        acquireRowLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(5);
        lockObject.setParentSvptID(4);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("D51AE000817F626F"));
        acquireRowLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(5);
        lockObject.setParentSvptID(4);
        lockObject.setLockMode(16);
        lockObject.setRowID(new RowKey("D51AE000817F626F"));
        acquireRowLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(4);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(2);
        acquireTableLock(lockManager, lockObject, 0);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(4);
        lockObject.setLockMode(2);
        releaseTableLock(lockManager, lockObject);
        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(4);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(1);
        acquireTableLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(-1);
        releaseSavepoint(lockManager, lockObject);

        lockObject.setTable("TRAF_RSRVD_3:TRAFODION.SEABASE.TB,,1611643105867.d374fdc4ee02efe12d0be9933be805a9.");
        lockObject.setTxID(72339069014658593L);
        lockObject.setSvptID(-1);
        lockObject.setParentSvptID(-1);
        releaseAll(lockManager, lockObject);
    }

    /**
     * 2级嵌套显示savepoint
     */
    @Test
    public void Test2LevelSavepoint() {
    	int lockMode = LockMode.LOCK_X;
    	Base2LevelSavepointTestCase(lockMode, true);
    	Base2LevelSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_S;
        Base2LevelSavepointTestCase(lockMode, true);
        Base2LevelSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_U;
        Base2LevelSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_IS;
        Base2LevelSavepointTestCase(lockMode, true);

        lockMode = LockMode.LOCK_IX;
        Base2LevelSavepointTestCase(lockMode, true);
    }

    /**
     * 2级嵌套显示savepoint，显示savepoint无锁
     */
    @Test
    public void Test2LevelNoImplicitSavepoint() {
        int lockMode = LockMode.LOCK_X;
        Base2LevelNoImplicitSavepointTestCase(lockMode, true);
        Base2LevelNoImplicitSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_S;
        Base2LevelNoImplicitSavepointTestCase(lockMode, true);
        Base2LevelNoImplicitSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_U;
        Base2LevelNoImplicitSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_IS;
        Base2LevelNoImplicitSavepointTestCase(lockMode, true);

        lockMode = LockMode.LOCK_IX;
        Base2LevelNoImplicitSavepointTestCase(lockMode, true);
    }
    
    /**
     * 3级嵌套显示savepoint
     */
    @Test
    public void Test3LevelSavepoint() {
        int lockMode = LockMode.LOCK_X;
        Base3LevelSavepointTestCase(lockMode, true);
        Base3LevelSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_S;
        Base3LevelSavepointTestCase(lockMode, true);
        Base3LevelSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_U;
        Base3LevelSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_IS;
        Base3LevelSavepointTestCase(lockMode, true);

        lockMode = LockMode.LOCK_IX;
        Base3LevelSavepointTestCase(lockMode, true);
    }

    public void Test3LevelSavepoint1() {
        int lockMode = LockMode.LOCK_U;
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        lockObject.setRowID(new RowKey("001"));
        acquireRowLock(lockManager, lockObject, 1);
        assertTrue(checkRowLockModes(lockObject, lockMode));

        lockObject.setSvptID(3);
        lockObject.setParentSvptID(2);
        acquireRowLock(lockManager, lockObject, 1);
        assertTrue(checkRowLockModes(lockObject, lockMode));
        System.out.println();
    }

    /**
     * 4级嵌套隐式savepoint
     */
    @Test
    public void Test4LevelSavepoint() {
        int lockMode = LockMode.LOCK_X;
        Base4LevelSavepointTestCase(lockMode, true);
        Base4LevelSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_S;
        Base4LevelSavepointTestCase(lockMode, true);
        Base4LevelSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_U;
        Base4LevelSavepointTestCase(lockMode, false);

        lockMode = LockMode.LOCK_IS;
        Base4LevelSavepointTestCase(lockMode, true);

        lockMode = LockMode.LOCK_IX;
        Base4LevelSavepointTestCase(lockMode, true);
    }

    /**
     * 4级嵌套隐式savepoint commit
     */
    @Test
    public void Test4LevelSavepointCommit() {
        int lockMode = LockMode.LOCK_X;
        Base4LevelSavepointTestCase(lockMode, true, COMMIT);
        Base4LevelSavepointTestCase(lockMode, false, COMMIT);

        lockMode = LockMode.LOCK_S;
        Base4LevelSavepointTestCase(lockMode, true, COMMIT);
        Base4LevelSavepointTestCase(lockMode, false, COMMIT);

        lockMode = LockMode.LOCK_U;
        Base4LevelSavepointTestCase(lockMode, false, COMMIT);

        lockMode = LockMode.LOCK_IS;
        Base4LevelSavepointTestCase(lockMode, true, COMMIT);

        lockMode = LockMode.LOCK_IX;
        Base4LevelSavepointTestCase(lockMode, true, COMMIT);
    }

    /**
     * 4级嵌套隐式savepoint rollback
     */
    @Test
    public void Test4LevelSavepointRollback() {
        int lockMode = LockMode.LOCK_X;
        Base4LevelSavepointTestCase(lockMode, true, ROLLBACK);
        Base4LevelSavepointTestCase(lockMode, false, ROLLBACK);

        lockMode = LockMode.LOCK_S;
        Base4LevelSavepointTestCase(lockMode, true, ROLLBACK);
        Base4LevelSavepointTestCase(lockMode, false, ROLLBACK);

        lockMode = LockMode.LOCK_U;
        Base4LevelSavepointTestCase(lockMode, false, ROLLBACK);

        lockMode = LockMode.LOCK_IS;
        Base4LevelSavepointTestCase(lockMode, true, ROLLBACK);

        lockMode = LockMode.LOCK_IX;
        Base4LevelSavepointTestCase(lockMode, true, ROLLBACK);
    }

    @Test
    public void TestLoop1() {
        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(44);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(44);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(44);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(16);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(80);
        lockObject.setParentSvptID(54);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(80);
        lockObject.setParentSvptID(54);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(80);
        lockObject.setParentSvptID(54);
        lockObject.setLockMode(16);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(-2);
        releaseAll(lockManager, lockObject);
        assertTrue(lockManager.validateAllLocks("test lock loop"));
    }

    @Test
    public void TestLoop() {
        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343732313100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343732313100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343936363200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343936363200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343937343600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343937343600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353231323000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353231323000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353231363100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353231363100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353232363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353232363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353233363900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353233363900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353234363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353234363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235393100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235393100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353238313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353238313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353539343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353539343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353731393400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353731393400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353732353100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353732353100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353734353900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353734353900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353735333300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353735333300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353834373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353834373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363233333500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363233333500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363235313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363235313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363235353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363235353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363236303800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363236303800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363332363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363332363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363336373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363336373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373138313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373138313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373139303600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373139303600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373139373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373139373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373238393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373238393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383130313200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383130313200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383534363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383534363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383535323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383535323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383535373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383535373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383630393400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383630393400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383634333300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383634333300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383634383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383634383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383636313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383636313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383638363200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383638363200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383639343600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383639343600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733333200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733333200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383734383000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383734383000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137393635393800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137393635393800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303832303300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303832303300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303832363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303832363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833323800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833323800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833343400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833343400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313731393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313731393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313732323000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313732323000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313835313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313835313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313835353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313835353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313836303800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313836303800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313931363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313931363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313932343200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313932343200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313934353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313934353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313934363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313934363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313939373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313939373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353436383400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353436383400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632313800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632313800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632333400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632333400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632383300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632383300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353633303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353633303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353830373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353830373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353831313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353831313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353832383900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353832383900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139363730363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139363730363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037323200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037323200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037333000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037333000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037343800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037343800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037393700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037393700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373133393900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373133393900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373338303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373338303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373339373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373339373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373434303100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373434303100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373634363300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373634363300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373635343700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373635343700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373936353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373936353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373936383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373936383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373937393800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373937393800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373938313400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373938313400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383033383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383033383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383034323400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383034323400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383134313400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383134313400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383530363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383530363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383531323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383531323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383532363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383532363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383537333800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383537333800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383538373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383538373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383637383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383637383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393237363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393237363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393238323500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393238323500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335313800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335313800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335333400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335333400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335383300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335383300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393337343000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393337343000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393339363300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393339363300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393634393500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393634393500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393635313100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393635313100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393835323500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393835323500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393839393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393839393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393936373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393936373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303039343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303039343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303137303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303137303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303139333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303139333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303139363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303139363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303230303400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303230303400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303235343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303235343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303236313600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303236313600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303433333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303433333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303433363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303433363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434323200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434323200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434333000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434333000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434343800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434343800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434393700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434393700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303437383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303437383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303438373700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303438373700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303539373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303539373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303631333800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303631333800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303633303200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303633303200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303730383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303730383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731323800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731323800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731343400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731343400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303732363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303732363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303833343000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303833343000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303836323100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303836323100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303837373900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303837373900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303838343500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303838343500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303839333600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303839333600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303931363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303931363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303931383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303931383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313035383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313035383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313039363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313039363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313130303500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313130303500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313635363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313635363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313636323400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313636323400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313830373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313830373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313831303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313831303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313833323300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313833323300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313936313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313936313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313936363900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313936363900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313939313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313939313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313939353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313939353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323638313300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323638313300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323639393500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323639393500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323732383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323732383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230333639303300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230333639303300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363437333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363437333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363437363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363437363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363439313300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363439313300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363530343300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363530343300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363531363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363531363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230373938373900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230373938373900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230373939363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230373939363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230383030303000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230383030303000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393233353100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393233353100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393234323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393234323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393234353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393234353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393535353200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393535353200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393535383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393535383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393536373700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393536373700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393538343200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393538343200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393539363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393539363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393539393000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393539393000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303237343700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303237343700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183528305078962L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303836373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303836373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303837383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303837383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303838323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303838323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303838353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303838353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(44);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(44);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(44);
        lockObject.setParentSvptID(2);
        lockObject.setLockMode(16);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343732313100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343732313100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343936363200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343936363200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343937343600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137343937343600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353231323000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353231323000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353231363100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353231363100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353232363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353232363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353233363900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353233363900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353234363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353234363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235393100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353235393100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353238313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353238313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353539343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353539343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353731393400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353731393400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353732353100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353732353100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353734353900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353734353900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353735333300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353735333300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353834373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137353834373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363233333500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363233333500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363235313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363235313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363235353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363235353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363236303800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363236303800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363331393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363332363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363332363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363336373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137363336373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373138313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373138313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373139303600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373139303600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373139373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373139373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373238393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137373238393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383130313200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383130313200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383534363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383534363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383535323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383535323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383535373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383535373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383630393400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383630393400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383634333300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383634333300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383634383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383634383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383636313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383636313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383638363200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383638363200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383639343600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383639343600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733333200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733333200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383733383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383734383000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137383734383000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303137393635393800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303137393635393800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303832303300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303832303300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303832363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303832363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833323800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833323800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833343400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138303833343400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313731393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313731393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313732323000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313732323000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313835313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313835313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313835353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313835353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313836303800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313836303800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313931363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313931363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313932343200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313932343200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313934353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313934353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313934363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313934363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313939373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303138313939373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353436383400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353436383400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632313800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632313800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632333400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632333400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632383300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353632383300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353633303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353633303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353739393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353830373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353830373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353831313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353831313500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353832383900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139353832383900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139363730363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139363730363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037323200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037323200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037333000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037333000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037343800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037343800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037393700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373037393700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373133393900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373133393900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373338303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373338303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373339373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373339373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373434303100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373434303100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373634363300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373634363300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373635343700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373635343700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373936353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373936353700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373936383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373936383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373937393800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373937393800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373938313400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139373938313400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383033383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383033383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383034323400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383034323400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383134313400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383134313400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383530363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383530363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383531323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383531323600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383532363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383532363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383537333800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383537333800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383538373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383538373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383637383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139383637383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393237363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393237363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393238323500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393238323500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335313800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335313800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335333400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335333400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335383300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393335383300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393337343000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393337343000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393339363300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393339363300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393634393500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393634393500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393635313100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393635313100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393835323500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393835323500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393839393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393839393600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393936373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303139393936373100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303039343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303039343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303137303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303137303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303139333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303139333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303139363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303139363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303230303400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303230303400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303235343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303235343100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303236313600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303236313600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303433333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303433333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303433363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303433363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434323200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434323200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434333000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434333000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434343800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434343800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434393700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303434393700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303437383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303437383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303438373700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303438373700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303539373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303539373300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303631333800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303631333800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303633303200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303633303200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303730383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303730383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731323800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731323800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731343400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303731343400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303732363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303732363800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303833343000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303833343000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303836323100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303836323100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303837373900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303837373900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303838343500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303838343500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303839333600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303839333600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303931363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303931363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303931383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230303931383100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313035383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313035383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313036393200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313039363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313039363500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313130303500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313130303500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313635363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313635363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313636323400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313636323400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313830373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313830373500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313831303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313831303900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313833323300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313833323300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313936313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313936313000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313936363900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313936363900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313939313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313939313700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313939353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230313939353800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323638313300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323638313300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323639393500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323639393500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323732383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230323732383200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230333639303300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230333639303300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363437333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363437333100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363437363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363437363400000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363439313300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363439313300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363530343300000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363530343300000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363531363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230363531363700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230373938373900000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230373938373900000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230373939363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230373939363000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230383030303000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230383030303000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393233353100000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393233353100000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393234323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393234323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393234353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393234353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393535353200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393535353200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393535383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393535383600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393536373700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393536373700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393538343200000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393538343200000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393539363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393539363600000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393539393000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303230393539393000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303237343700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303237343700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303836373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303836373800000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303837383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303837383500000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303838323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303838323700000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);


        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303838353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setRowID(new RowKey("000000093530303131303030303231303838353000000000000000000000000000000000000000000000000039393939"));
        lockObject.setLockMode(2);
        releaseRowLock(lockManager, lockObject);




        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183536894703552L);
        lockObject.setSvptID(46);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(2);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(54);
        lockObject.setParentSvptID(1);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(false);
        acquireRowLock(lockManager, lockObject, 0);



        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(80);
        lockObject.setParentSvptID(54);
        lockObject.setLockMode(2);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(80);
        lockObject.setParentSvptID(54);
        lockObject.setLockMode(8);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(80);
        lockObject.setParentSvptID(54);
        lockObject.setLockMode(16);
        lockObject.setRowID(new RowKey("000000093832303030303030303030333830333437343030303135360000000000000000000000000000000039393939"));
        lockObject.setImplicitSavepoint(true);
        acquireRowLock(lockManager, lockObject, 0);

        lockObject.setTable("TRAFODION.LTTSV7.KDPA_ZHXINX,\\x00\\x00\\x00\\x09\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,1612770220902.ce1a7f60c5802e4ce41494e081f6a599.");
        lockObject.setTxID(73183511124989816L);
        lockObject.setSvptID(-2);
        releaseAll(lockManager, lockObject);
        assertTrue(lockManager.validateAllLocks("test lock loop"));
    }

    private void Base2LevelSavepointTestCase(int lockMode, boolean isTable) {
    	lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
        	acquireTableLock(lockManager, lockObject, 1);
        	assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
        	lockObject.setRowID(new RowKey("001"));
        	acquireRowLock(lockManager, lockObject, 1);
        	assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
        	acquireTableLock(lockManager, lockObject, 1);
        	assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
        	lockObject.setRowID(new RowKey("002"));
        	acquireRowLock(lockManager, lockObject, 1);
        	assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 释放锁
        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
        assertTrue(lockManager.validateAllLocks("test table lock single lock mode " + getLockModeName(lockMode)));
    }

    private void Base2LevelNoImplicitSavepointTestCase(int lockMode, boolean isTable) {
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("001"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(4);
        lockObject.setParentSvptID(3);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("002"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        lockObject.setSvptID(3);
        commitSavepoint(lockManager, lockObject);
        // 释放锁
        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
        assertTrue(lockManager.validateAllLocks("test table lock single lock mode " + getLockModeName(lockMode)));
    }

    private void Base3LevelSavepointTestCase(int lockMode, boolean isTable) {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("001"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("002"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(3);
        lockObject.setParentSvptID(2);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("003"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 释放锁
        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
        assertTrue(lockManager.validateAllLocks("test table lock single lock mode " + getLockModeName(lockMode)));
    }

    private void Base4LevelSavepointTestCase(int lockMode, boolean isTable) {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("001"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            try {
                assertTrue(checkTableLockModes(lockObject, lockMode));
            } catch (AssertionError e) {
                lockObject.setSvptID(1);
                assertTrue(checkTableLockModes(lockObject, lockMode));
            }
        } else {
            lockObject.setRowID(new RowKey("002"));
            acquireRowLock(lockManager, lockObject, 1);
            try {
                assertTrue(checkRowLockModes(lockObject, lockMode));
            } catch (AssertionError e) {
                lockObject.setSvptID(1);
                assertTrue(checkRowLockModes(lockObject, lockMode));
            }
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(3);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("003"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(4);
        lockObject.setParentSvptID(3);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("004"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(5);
        lockObject.setParentSvptID(4);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            try {
                assertTrue(checkTableLockModes(lockObject, lockMode));
            } catch (AssertionError e) {
                lockObject.setSvptID(4);
                assertTrue(checkTableLockModes(lockObject, lockMode));
            }
        } else {
            lockObject.setRowID(new RowKey("005"));
            acquireRowLock(lockManager, lockObject, 1);
            try {
                assertTrue(checkRowLockModes(lockObject, lockMode));
            } catch (AssertionError e) {
                lockObject.setSvptID(4);
                assertTrue(checkRowLockModes(lockObject, lockMode));
            }
        }

        // 释放锁
        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
        assertTrue(lockManager.validateAllLocks("test table lock single lock mode " + getLockModeName(lockMode)));
    }

    private void Base4LevelSavepointTestCase(int lockMode, boolean isTable, int commitOrRollback) {
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("001"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            try {
                assertTrue(checkTableLockModes(lockObject, lockMode));
            } catch (AssertionError e) {
                lockObject.setSvptID(1);
                assertTrue(checkTableLockModes(lockObject, lockMode));
            }
        } else {
            lockObject.setRowID(new RowKey("002"));
            acquireRowLock(lockManager, lockObject, 1);
            try {
                assertTrue(checkRowLockModes(lockObject, lockMode));
            } catch (AssertionError e) {
                lockObject.setSvptID(1);
                assertTrue(checkRowLockModes(lockObject, lockMode));
            }
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(3);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("003"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(4);
        lockObject.setParentSvptID(3);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            lockObject.setRowID(new RowKey("004"));
            acquireRowLock(lockManager, lockObject, 1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(5);
        lockObject.setParentSvptID(4);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(lockMode);
        if (isTable) {
            acquireTableLock(lockManager, lockObject, 1);
            try {
                assertTrue(checkTableLockModes(lockObject, lockMode));
            } catch (AssertionError e) {
                lockObject.setSvptID(4);
                assertTrue(checkTableLockModes(lockObject, lockMode));
            }
        } else {
            lockObject.setRowID(new RowKey("005"));
            acquireRowLock(lockManager, lockObject, 1);
            try {
                assertTrue(checkRowLockModes(lockObject, lockMode));
            } catch (AssertionError e) {
                lockObject.setSvptID(4);
                assertTrue(checkRowLockModes(lockObject, lockMode));
            }
        }

        lockObject.setSvptID(3);
        if (commitOrRollback == COMMIT) {
            commitSavepoint(lockManager, lockObject);
        } else {
            releaseSavepoint(lockManager, lockObject);
        }
        if (isTable) {
            assertFalse(checkTableLockModes(lockObject, lockMode));
            lockObject.setSvptID(1);
            assertTrue(checkTableLockModes(lockObject, lockMode));
        } else {
            assertFalse(checkRowLockModes(lockObject, lockMode));
            lockObject.setSvptID(1);
            assertTrue(checkRowLockModes(lockObject, lockMode));
        }

        // 释放锁
        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
        assertTrue(lockManager.validateAllLocks("test table lock single lock mode " + getLockModeName(lockMode)));
    }
}
