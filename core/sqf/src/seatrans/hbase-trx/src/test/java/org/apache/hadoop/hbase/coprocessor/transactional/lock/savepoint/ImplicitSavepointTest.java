package org.apache.hadoop.hbase.coprocessor.transactional.lock.savepoint;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.BaseTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockObject;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RowKey;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.base.LockUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ImplicitSavepointTest extends BaseTest {
    private LockManager lockManager;
    private LockObject lockObject;
    private LockObject lockObject1;

    @Before
    public void init() {
        LockConstants.ENABLE_ROW_LEVEL_LOCK = true;
        RSServer rsServer = RSServer.getInstance(null, null);
        rsServer.enableRowLevelLockFeature();
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
    public void TestRollbackSavepoint() {
        // 模拟事务下的显示savepoint
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(LockMode.LOCK_X);
        acquireTableLock(lockManager, lockObject, 1);

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(LockMode.LOCK_X);
        acquireRowLock(lockManager, lockObject, 1);

        // 释放显示savepoint
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        releaseSavepoint(lockManager, lockObject);

        // 模拟显示savepoint下的隐式savepoint
        lockObject.setSvptID(3);
        lockObject.setParentSvptID(-1); // 显示savepoint ID
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(LockMode.LOCK_X);
        acquireRowLock(lockManager, lockObject, 1);
    }

    @Test
    public void testCommit1LevelSavepoint() {
        // 模拟事务下的隐式savepoint
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(LockMode.LOCK_X);
        acquireRowLock(lockManager, lockObject, 1);
        lockManager.flushSvptToTx(lockObject.getTxID(), lockObject.getSvptID());
    }

    @Test
    public void testCommit2LevelSavepoint() {
        // 模拟事务下的隐式savepoint
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(LockMode.LOCK_X);
        acquireRowLock(lockManager, lockObject, 1);

        // 模拟显示savepoint下的隐式savepoint
        lockObject.setSvptID(3);
        lockObject.setParentSvptID(2); // 显示savepoint ID
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(LockMode.LOCK_X);
        lockObject.setRowID(new RowKey("0002".getBytes()));
        acquireRowLock(lockManager, lockObject, 1);

        lockObject.setSvptID(2);
        lockManager.flushSvptToTx(lockObject.getTxID(), lockObject.getSvptID());
    }

    @Test
    public void test2LevelUpdateSavepoint() {
        // 模拟事务下的隐式savepoint
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(LockMode.LOCK_U);
        acquireRowLock(lockManager, lockObject, 1);

        lockObject.setLockMode(LockMode.LOCK_X);
        acquireRowLock(lockManager, lockObject, 1);

        LockUtil.lockReleaseSavepoint(lockManager, lockObject);
        LockUtil.lockReleaseAll(lockManager, lockObject);

        lockObject.setSvptID(4);
        lockObject.setParentSvptID(3);
        lockObject.setLockMode(LockMode.LOCK_U);
        acquireRowLock(lockManager, lockObject, 1);

        lockObject.setLockMode(LockMode.LOCK_X);
        acquireRowLock(lockManager, lockObject, 1);

    }

    @Test
    public void TestRollback3LevelSavepoint() {
        // 模拟事务下的显示savepoint
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setRowID(new RowKey("001"));
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(LockMode.LOCK_X);
        acquireRowLock(lockManager, lockObject, 1);

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(2);
        lockObject.setParentSvptID(1);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(LockMode.LOCK_X);
        lockObject.setRowID(new RowKey("002"));
        acquireRowLock(lockManager, lockObject, 1);

        // 模拟事务下的显示savepoint
        lockObject.setSvptID(3);
        lockObject.setParentSvptID(2);
        lockObject.setImplicitSavepoint(false);
        lockObject.setLockMode(LockMode.LOCK_X);
        lockObject.setRowID(new RowKey("003"));
        acquireRowLock(lockManager, lockObject, 1);

        // 释放显示savepoint
        lockObject.setSvptID(3);
        releaseSavepoint(lockManager, lockObject);

        // 释放显示savepoint
        lockObject.setSvptID(2);
        releaseSavepoint(lockManager, lockObject);

        // 释放显示savepoint
        lockObject.setSvptID(1);
        releaseSavepoint(lockManager, lockObject);

        // 释放所有的lock
        lockObject.setSvptID(-1);
        releaseAll(lockManager, lockObject);
    }

    @Test
    public void testFlushLock() {
        // 模拟事务下的隐式savepoint
        lockObject.setSvptID(1);
        lockObject.setParentSvptID(-1);
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(LockMode.LOCK_X);
        acquireRowLock(lockManager, lockObject, 1);

        // 模拟显示savepoint下的隐式savepoint
        lockObject.setSvptID(3);
        lockObject.setParentSvptID(2); // 显示savepoint ID
        lockObject.setImplicitSavepoint(true);
        lockObject.setLockMode(LockMode.LOCK_X);
        lockObject.setRowID(new RowKey("0002".getBytes()));
        acquireRowLock(lockManager, lockObject, 1);

        lockObject.setSvptID(2);
        releaseAll(lockManager, lockObject);

        try {
            HFileContext context = new HFileContextBuilder().withIncludesTags(false).build();
            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get(config);
            Path path = new Path("/lockmanager");
            lockManager.flushToFS(lockManager.getRegionName(), context, fs, config, path, true);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        System.out.println();
    }
}
