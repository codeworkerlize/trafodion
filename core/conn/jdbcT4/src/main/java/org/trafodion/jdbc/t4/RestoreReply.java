package org.trafodion.jdbc.t4;

import java.sql.SQLException;

public class RestoreReply {

    private boolean rsFlag = true;
    
    RestoreReply(LogicalByteArray buf, InterfaceConnection ic, int reconnNum) throws SQLException {
        boolean loclen = false;
        buf.setLocation(Header.sizeOf());
        
        int loc = buf.getLocation();
        int len = buf.getLength();
        if (loc >= len) {
            loclen = true;
        }
        if (!loclen) {
            int restore = buf.extractInt();
            if (restore > reconnNum) {
                reconnNum = restore;
                ic.setReconn(false);
                rsFlag = false;
            }
        }
    }

    public boolean isRsFlag() {
        return rsFlag;
    }
    
}
