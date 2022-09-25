package org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.unexpectedDeadLock;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.*;

public class TestUnExpectedDeadlock {
    @Test
    public void testUnExpectedDeadLockException() {
        int threadNum = 10;
        LockConstants.ENABLE_ROW_LEVEL_LOCK = true;
        RSServer rsServer = RSServer.getInstance(null, null);
        rsServer.enableRowLevelLockFeature();

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        ExecutorService fixedThreadPool = newFixedThreadPool(threadNum);
        RSServer.getInstance(null, null);
        LockManager lockManager = new LockManager("seabase.trafodion.test1,aaa", false);
        for (int i=0; i < threadNum; i++) {
            fixedThreadPool.execute(new UserTransaction(lockManager, i+1, countDownLatch));
        }
        while (countDownLatch.getCount() != 0) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        fixedThreadPool.shutdownNow();
        System.out.println("finished " + fixedThreadPool.isShutdown());
    }
}
