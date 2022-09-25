package org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock;

import org.apache.hadoop.hbase.Stoppable;

import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockManager;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import org.apache.log4j.Logger;

public class DistributedDeadLockDetectThread extends Thread {
    private static Logger LOG = Logger.getLogger(DistributedDeadLockDetectThread.class);

    private RSServer rsServer;
    private Stoppable stopper;

    public DistributedDeadLockDetectThread(RSServer rsServer, Stoppable stopper) {
        super("DistributedDeadLockDetectThread");
        this.rsServer = rsServer;
        this.stopper = stopper;
        LOG.info("DistributedDeadLockDetectThread started");
    }

    public void run() {
        while (true) {
            String regionName = null;
            LockManager lockManager = null;
            try {
                for (Map.Entry<String, LockManager> entry : rsServer.getLockManagers().entrySet()) {
                    regionName = entry.getKey();
                    lockManager = entry.getValue();
                    if (lockManager.getWaitGraph().size() == 0) {
                        continue;
                    }
                    LOG.info("distributed dead lock detect regionName: " + regionName + " " + getLockWaitInfo(lockManager.getWaitGraph()));
                    for (long txID : lockManager.getWaitGraph().keySet()) {
                        rsServer.distributedDeadLockDetect(regionName, txID);
                    }
                }
            } catch (Exception e) {
                LOG.error("failed to start distributed dead lock detect", e);
            }
            try {
                Thread.sleep(LockConstants.DISTRIBUTED_DEADLOCK_DETECT_INTERVAL);
            } catch (InterruptedException e) {
            }
        }
    }

    private String getLockWaitInfo(ConcurrentHashMap<Long, LockWait> waitGraph) {
        if (waitGraph.size() == 0) {
            return "";
        }
        StringBuffer message = new StringBuffer(1000);
        LockWait lockWait = null;
        for (Map.Entry<Long, LockWait> entry : waitGraph.entrySet()) {
            lockWait = entry.getValue();
            if (lockWait == null) {
                continue;
            }
            message.append(entry.getKey()).append("->").append(lockWait.getHolderTxIDs());
        }
        return message.toString();
    }
}
