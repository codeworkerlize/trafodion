package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMMRegionInfoMsg extends RSMessage {
    private static final long serialVersionUID = 1L;

    @Getter
    @Setter
    List<String> regionList;

    public LMMRegionInfoMsg() {
        super(MSG_TYP_LOCK_GET_REGION_INFO);
    }
}