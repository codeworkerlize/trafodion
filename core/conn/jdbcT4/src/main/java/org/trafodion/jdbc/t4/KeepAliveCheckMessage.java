package org.trafodion.jdbc.t4;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.UnsupportedCharsetException;

public class KeepAliveCheckMessage {

    static LogicalByteArray marshal(String dialogueIds, String remoteprocess,
        InterfaceConnection ic) throws UnsupportedCharsetException, CharacterCodingException {
        int wlength = Header.sizeOf();
        LogicalByteArray buf = null;

        int tmpIndex = remoteprocess.indexOf("$");
        remoteprocess = remoteprocess.substring(tmpIndex + 1);

        byte[] dialogueIdsBytes = ic.encodeString(dialogueIds, 1);
        byte[] remoteprocessBytes = ic.encodeString(remoteprocess, 1);

        wlength += TRANSPORT.size_bytes(dialogueIdsBytes); // srvrObjReference
        wlength += TRANSPORT.size_bytes(remoteprocessBytes); // srvrObjReference

        buf = new LogicalByteArray(wlength, Header.sizeOf(), ic.getByteSwap());

        buf.insertString(dialogueIdsBytes);
        buf.insertString(remoteprocessBytes);

        return buf;
    }

}
