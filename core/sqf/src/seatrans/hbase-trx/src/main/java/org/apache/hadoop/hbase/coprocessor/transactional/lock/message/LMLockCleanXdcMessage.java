package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.XDC_STATUS;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMLockCleanXdcMessage extends RSMessage {
    private static final long serialVersionUID = 1L;
    @Getter
    @Setter
    private int clusterId;
    @Getter
    @Setter
    private XDC_STATUS xdcStatus;

    public LMLockCleanXdcMessage() {
        super(MSG_TYP_LOCK_CLEAN_XDC);
    }
}
