package org.trafodion.dcs.master.listener;

import java.io.IOException;
import java.net.SocketAddress;
import org.trafodion.dcs.master.message.CancelMessage;
import org.trafodion.dcs.master.message.CheckActiveMasterMessage;
import org.trafodion.dcs.master.message.ConnectMessage;
import org.trafodion.dcs.master.message.KeepAliveMessage;
import org.trafodion.dcs.master.message.Header;
import org.trafodion.dcs.master.message.ReplyMessage;

public interface Data {
    Header getHeader();

    ConnectMessage getConnectMessage();

    CancelMessage getCancelMessage();

    KeepAliveMessage getKeepAliveMessage();

    SocketAddress getClientSocketAddress();

    CheckActiveMasterMessage getCheckActiveMasterMessage();

    void setRequestReply(int requestReply);

    void setReplyMessage(ReplyMessage replyMessage) throws IOException;

}
