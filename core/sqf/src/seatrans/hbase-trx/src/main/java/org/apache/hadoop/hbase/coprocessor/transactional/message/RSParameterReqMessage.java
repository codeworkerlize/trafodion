package org.apache.hadoop.hbase.coprocessor.transactional.message;

import lombok.Getter;
import lombok.Setter;

import java.util.Properties;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class RSParameterReqMessage extends RSMessage {
    private static final long serialVersionUID = 1L;
    @Getter
    @Setter
    private Properties parameters;

    @Getter
    @Setter
    private boolean forced;

    public RSParameterReqMessage(byte msgType) {
        super(msgType);
        this.forced = false;
    }
}
