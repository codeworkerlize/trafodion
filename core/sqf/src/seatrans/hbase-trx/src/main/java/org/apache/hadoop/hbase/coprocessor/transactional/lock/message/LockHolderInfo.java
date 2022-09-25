package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

public class LockHolderInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    @Setter
    @Getter
    private Long txID;
    @Setter
    @Getter
    private Long svptID;
    @Getter
    @Setter
    private boolean implicitSavepoint = false;
    @Setter
    @Getter
    private String regionName;
    @Setter
    @Getter
    private String rowKey;
    @Setter
    @Getter
    private long[] holding = { 0, 0, 0, 0, 0, 0, 0 };

    public String getLockMode() {
        if (holding == null) {
            return "";
        }
        StringBuffer lockMode = new StringBuffer();
        if (svptID > 0) {
            if (implicitSavepoint) {
                lockMode.append("implicit ");
            }
            lockMode.append("savepoint: ");
        }
        boolean flag = false;
        for (int i=0; i< holding.length; i++) {
            if (holding[i] > 0) {
                if (!flag) {
                    flag = true;
                } else {
                    lockMode.append(",");
                }
                switch (i) {
                    case 0:
                        lockMode.append("IS");
                        break;
                    case 1:
                        lockMode.append("S");
                        break;
                    case 2:
                        lockMode.append("IX");
                        break;
                    case 3:
                        lockMode.append("U");
                        break;
                    case 4:
                        lockMode.append("X");
                        break;
                    case 5:
                        lockMode.append("RS");
                        break;
                    case 6:
                        lockMode.append("RX");
                        break;
                }
            }
        }
        return lockMode.toString();
    }
}
