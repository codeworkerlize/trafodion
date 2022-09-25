package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMDeadLockDetectMessage extends RSMessage {
    private static final long serialVersionUID = 1L;
    @Getter
    @Setter
    private String originalRegionServer;
    @Getter
    @Setter
    private Long originalWaitingTxID;
    // which transaction is waiting for holderTxID transaction
    @Getter
    @Setter
    private Long watingTxID;
    // transaction that watingTxID is waiting for
    @Getter
    @Setter
    private CopyOnWriteArraySet<Long> holderTxIDs;
    // minimum transactionID in dead lock cycle
    @Getter
    @Setter
    private List<Long> detectPath = new ArrayList<>();
    @Getter
    @Setter
    private CopyOnWriteArraySet<String> victimTxRegionServers;

    public LMDeadLockDetectMessage() {
        super(MSG_TYP_DEADLOCK_DETECT);
    }

    public String toString() {
        return "originalRegionServer: " + originalRegionServer + ", originalWaitingTxID:" + originalWaitingTxID + ", watingTxID: " + watingTxID + ", holderTxIDs: " + holderTxIDs + ", detectPath: " + detectPath + ", victimTxRegionServers: " + victimTxRegionServers;
    }
}
