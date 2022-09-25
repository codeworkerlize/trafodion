package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

public class LMLockStaticisticsData implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    @Getter
    @Setter
    private Integer lockNum;
    @Getter
    @Setter
    private String queryContext;
    public LMLockStaticisticsData(int lockNum, String queryContext) {
        this.lockNum = lockNum;
        this.queryContext = queryContext;
    }
    
    public LMLockStaticisticsData(LMLockStaticisticsData lmLSD) {
        this.lockNum = lmLSD.lockNum;
        this.queryContext = lmLSD.queryContext;
    }
}
