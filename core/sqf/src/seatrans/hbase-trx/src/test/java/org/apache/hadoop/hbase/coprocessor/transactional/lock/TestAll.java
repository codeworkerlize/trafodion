package org.apache.hadoop.hbase.coprocessor.transactional.lock;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.compatible.TestCompatible;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.compatible.TestCompatible1;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.conflict.TestLockConflict;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.conflict.TestLockConflict1;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.conflict.TestLockConflictWithThread;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.RowDeadLockTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.TableDeadLockTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.unexpectedDeadLock.TestUnExpectedDeadlock;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.escalation.TestEscalation;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.lockInterfaces.SingleModeTest;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.lockInterfaces.SingleModeTest1;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.mantis.TestForMantis;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@FixMethodOrder(MethodSorters.JVM)
@RunWith(Suite.class)
@SuiteClasses({
        SingleModeTest.class,
        SingleModeTest1.class,
        TestCompatible.class,
        TestCompatible1.class,
        TestLockConflict.class,
        TestLockConflict1.class,
        TestLockConflictWithThread.class,
        TestForMantis.class,
        TestEscalation.class,
        TableDeadLockTest.class,
        RowDeadLockTest.class,
        TestUnExpectedDeadlock.class
})
public class TestAll {
    @Before
    public void init() {
        LockConstants.ENABLE_ROW_LEVEL_LOCK = true;
        RSServer rsServer = RSServer.getInstance(null, null);
        rsServer.enableRowLevelLockFeature();
    }
}
