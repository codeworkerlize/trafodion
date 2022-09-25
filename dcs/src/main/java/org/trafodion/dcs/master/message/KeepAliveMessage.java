package org.trafodion.dcs.master.message;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.master.listener.Util;

public class KeepAliveMessage {

    private static final Logger LOG = LoggerFactory.getLogger(KeepAliveMessage.class);

    private final String dialogueIds;
    private final String remoteProcess;

    private KeepAliveMessage(String dialogueIds, String remoteProcess) {
        super();
        this.dialogueIds = dialogueIds;
        this.remoteProcess = remoteProcess;
    }

    public String getDialogueId() {
        return dialogueIds;
    }


    public String getRemoteProcess() {
        return remoteProcess;
    }


    /**
     * extract From {@link java.nio.ByteBuffer} for NIO.
     *
     * @param buf
     */
    public static KeepAliveMessage extractDcsMxoMessageFromByteBuffer(ByteBuffer buf)
        throws IOException {
        String dialogueIds = null;
        String remoteProcess = null;
        try {
            dialogueIds = Util.extractString(buf);
            remoteProcess = Util.extractString(buf);
        }catch(BufferUnderflowException e1){
            LOG.warn("BufferUnderflowException enter with : {}", e1.getMessage());
            dialogueIds = "";
            remoteProcess = "";
            throw new IOException(e1);
        }

        return new KeepAliveMessage(dialogueIds, remoteProcess);
    }

}
