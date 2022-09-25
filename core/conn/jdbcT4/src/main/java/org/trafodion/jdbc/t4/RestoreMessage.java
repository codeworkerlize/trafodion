package org.trafodion.jdbc.t4;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.UnsupportedCharsetException;

public class RestoreMessage {

    static LogicalByteArray marshal(int dialogueId, String restoreFlag, InterfaceConnection ic)
            throws UnsupportedCharsetException, CharacterCodingException {
        int wlength = Header.sizeOf();
        LogicalByteArray buf;

        byte[] restoreBytes = ic.encodeString(restoreFlag, InterfaceUtilities.SQLCHARSETCODE_UTF8);

        wlength += TRANSPORT.size_int; // dialogueId
        wlength += TRANSPORT.size_bytes(restoreBytes);

        buf = new LogicalByteArray(wlength, Header.sizeOf(), ic.getByteSwap());

        buf.insertInt(dialogueId);
        buf.insertString(restoreBytes);

        return buf;
    }
    
}
