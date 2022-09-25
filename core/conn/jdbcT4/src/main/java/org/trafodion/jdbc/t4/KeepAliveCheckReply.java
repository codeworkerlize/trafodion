package org.trafodion.jdbc.t4;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.SQLException;

public class KeepAliveCheckReply {

    String keepAliveRes = null;
    int operationId;
    int size = 0;

    KeepAliveCheckReply(LogicalByteArray buf, InterfaceConnection ic)
        throws SQLException, CharacterCodingException,
        UnsupportedCharsetException {
        buf.setLocation(Header.sizeOf());
        operationId = buf.extractInt();
        if (operationId == TRANSPORT.AS_API_DCSMXOKEEPALIVE) {
            size = buf.extractInt();
            if (size > 0) {
                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < size; i++){
                    int tmpId = buf.extractInt();
                    sb.append(tmpId).append(",");
                }
                keepAliveRes = sb.toString();
            }
        }

    }

    public String getKeepalive() {
        return keepAliveRes;
    }

}
