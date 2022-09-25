package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMAppliedLockReqMessage extends RSMessage {
    private static final long serialVersionUID = 1L;

    @Getter
    @Setter
    private int debug;

    public LMAppliedLockReqMessage() {
        super(MSG_TYP_LOCK_APPLIED_LOCK_MSG);
    }
}
