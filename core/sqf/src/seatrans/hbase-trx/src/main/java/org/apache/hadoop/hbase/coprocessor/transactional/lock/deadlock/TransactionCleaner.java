package org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.log4j.Logger;

public class TransactionCleaner extends Thread  {
    private static Logger LOG = Logger.getLogger(TransactionCleaner.class);

    private RSServer rsServer;
    private Stoppable stopper;

    public TransactionCleaner(RSServer rsServer, Stoppable stopper) {
        super("TransactionCleaner");
        this.rsServer = rsServer;
        this.stopper = stopper;
    }

    public void run() {
        boolean isTMstarted = false;
        int failedTimes = 0;
        int regionNum = 0;
        boolean isCluster = false;
        try {
            regionNum = rsServer.listAllRegionServers().size();
            isCluster = (regionNum > 1);
            rsServer.parseRestServerURI();
            rsServer.parseXDCMasterSlave();
        } catch (Exception e) {
            LOG.warn("TransactionCleaner initialize encountered an error", e);
        }
        while (true) {
            try {
                if (regionNum <= 0) {
                    LOG.warn("TransactionCleaner: regionservers don't exist, regionNum: " + regionNum);
                    regionNum = rsServer.listAllRegionServers().size();
                    isCluster = (regionNum > 1);
                    try {
                        Thread.sleep(LockConstants.REST_CONNECT_DETECT_INTERVAL);
                    } catch(Exception e) {
                    }
                    continue;
                }
                if (!isTMstarted) {
                    isTMstarted = rsServer.isTMStarted();
                    if (isTMstarted) {
                        LOG.info("TransactionCleaner: TM started!");
                    }
                }
                if (!isTMstarted) {
                    failedTimes++;
                    LOG.info("TransactionCleaner is waiting for TM start!");
                } else {
                    failedTimes = 0;
                    isTMstarted = rsServer.checkAndCleanTransaction(isCluster);
                }
                if (!isTMstarted && failedTimes >= LockConstants.REST_CONN_FAILED_TIMES) {
                    Thread.sleep(LockConstants.REST_CONNECT_DETECT_INTERVAL);
                } else {
                    Thread.sleep(LockConstants.TRANSACTION_CLEAN_INTERVAL);
                }
            } catch (Exception e) {
                LOG.warn("TransactionCleaner loop encountered an error: ", e);
            }
        }
    }
}

