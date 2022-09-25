package org.trafodion.dcs.master.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.master.listener.Util;

public class CheckActiveMasterMessage {

    private static final Logger LOG = LoggerFactory.getLogger(CheckActiveMasterMessage.class);

    private final String clientHostName;

    private CheckActiveMasterMessage(String clientHostName) {
        this.clientHostName = clientHostName;
    }

    public static CheckActiveMasterMessage extractDcsMxoMessageFromByteBuffer(ByteBuffer buf)
            throws IOException {
        String clientHostName = null;

        clientHostName = Util.extractString(buf);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Check Active Master, clientHostName:<{}>...", clientHostName);
        }
        return new CheckActiveMasterMessage(clientHostName);
    }

    public String getClientHostName() {
        return clientHostName;
    }
}
