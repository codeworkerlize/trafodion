package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Set;

public class LMLockWaitMsg implements Serializable {
    private static final long serialVersionUID = 1L;
    @Setter
    @Getter
    private long txID;
    @Setter
    @Getter
    private long svptID = -1;
    @Setter
    @Getter
    private long parentSvptID = -1;
    @Setter
    @Getter
    private boolean implicitSavepoint = false;
    @Setter
    @Getter
    private LMLockMsg toLock;
    @Setter
    @Getter
    private Set<Long> holderTxIDs;
    @Setter
    @Getter
    private String queryContext;
}
