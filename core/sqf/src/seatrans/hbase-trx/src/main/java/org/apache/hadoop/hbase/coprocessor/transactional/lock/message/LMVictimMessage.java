package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMVictimMessage extends RSMessage {
    private static final long serialVersionUID = 1L;
    @Getter
    @Setter
    private long victimTxID;
    @Getter
    @Setter
    private List<Long> detectPath = new ArrayList<>();

    public LMVictimMessage() {
        super(MSG_TYP_DEADLOCK_VICTIM);
    }
}
