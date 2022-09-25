package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import java.util.List;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMLockStatisticsReqMessage extends RSMessage {
    private static final long serialVersionUID = 1L;
    @Getter
    @Setter
    private int topN = 0;
    @Getter
    @Setter
    private int debug;

    @Getter
    @Setter
    private List<Long> txIDs;

    @Getter
    @Setter
    private List<String> tableNames;

    public LMLockStatisticsReqMessage() {
        super(MSG_TYP_LOCK_STATISTICS_INFO);
        tableNames = null;
        txIDs = null;
    }
}
