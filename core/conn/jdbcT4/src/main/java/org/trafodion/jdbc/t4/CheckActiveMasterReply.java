package org.trafodion.jdbc.t4;

import java.sql.SQLException;
import java.util.Vector;

public class CheckActiveMasterReply {

    String allMasterIp;
    int operationId;
    Vector<String> masterIpVector;
    private int isEmptyEqualsNull;

    CheckActiveMasterReply(LogicalByteArray buf, InterfaceConnection ic)
            throws Exception {

        buf.setLocation(Header.sizeOf());
        operationId = buf.extractInt();
        if(ic.getDcsVersion() >= 5 && ic.getDcsVersion() != 6){
            isEmptyEqualsNull = buf.extractInt();
        }
        masterIpVector = new Vector<String>();
        if (operationId == TRANSPORT.AS_API_ACTIVE_MASTER) {
            //hostname1:ip1,hostname12:ip2,hostname13:ip3.....
            allMasterIp = ic
                    .decodeBytes(buf.extractString(), InterfaceUtilities.SQLCHARSETCODE_UTF8);
            if (allMasterIp != null || allMasterIp.length() > 0) {
                String[] ipArry = allMasterIp.split(",");
                for (int i = 0; i < ipArry.length; i++) {
                    masterIpVector.add(ipArry[i].substring(ipArry[i].indexOf(":") + 1));
                }
            }
        } else {
            throw new SQLException(TRANSPORT.ACTIVE_MASTER_REPLY_ERROR);
        }
    }

    public String getIpString() {
        return allMasterIp;
    }

    public Vector<String> getMasterIpVector() {
        return masterIpVector;
    }

    public int getIsEmptyEqualsNull() {
        return isEmptyEqualsNull;
    }
}
