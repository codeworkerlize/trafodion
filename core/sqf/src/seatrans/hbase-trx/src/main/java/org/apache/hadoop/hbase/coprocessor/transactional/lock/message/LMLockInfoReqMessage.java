package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMLockInfoReqMessage extends RSMessage {
    private static final long serialVersionUID = 1L;
    @Getter
    @Setter
    private List<String> tableNames;
    @Getter
    @Setter
    private List<Long> txIDs;
    @Getter
    @Setter
    private List<Long> holderTxIDs;
    @Getter
    @Setter
    private String type;
    @Getter
    @Setter
    private int debug;
    @Getter
    @Setter
    private int maxRetLockInfoLimit;
    @Getter
    @Setter
    private boolean isClient = true;
    @Getter
    @Setter
    private boolean forceClean = false;

    public LMLockInfoReqMessage(byte msgType) {
        super(msgType);
        isClient = true;
        maxRetLockInfoLimit = 0;
    }
}
