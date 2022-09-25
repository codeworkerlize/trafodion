package org.trafodion.jdbc.t4;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;

public class CheckActiveMasterMessage {
    // ----------------------------------------------------------
    static LogicalByteArray marshal(InterfaceConnection ic)
            throws UnsupportedCharsetException, CharacterCodingException {

        int wlength = Header.sizeOf();
        LogicalByteArray buf;

        InetAddress ip;
        String clientHostName;

        try {
            ip = InetAddress.getLocalHost();
            clientHostName = ip.getHostName();

        } catch (UnknownHostException e) {
            clientHostName = "";
        }
        //client HostName
        byte[] clientHostNameBytes = ic.encodeString(clientHostName, InterfaceUtilities.SQLCHARSETCODE_ISO88591);

        wlength += TRANSPORT.size_bytes(clientHostNameBytes);

        buf = new LogicalByteArray(wlength, Header.sizeOf(), ic.getByteSwap());

        buf.insertString(clientHostNameBytes);

        return buf;
    }

}
