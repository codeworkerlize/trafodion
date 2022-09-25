package org.trafodion.dcs.master.listener.nio;

import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.master.listener.ConfigReader;
import org.trafodion.dcs.master.listener.RequestGetObjectRef;

public class GetConnectTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(GetConnectTask.class);
    private ConfigReader configReader;
    private DataEvent dataEvent;

    public GetConnectTask(ConfigReader configReader, DataEvent dataEvent) {
        this.configReader = configReader;
        this.dataEvent = dataEvent;
    }

    @Override
    public void run() {
        try {
            RequestGetObjectRef requestGetObjectRef = new RequestGetObjectRef(configReader);
            SelectionKey key = dataEvent.key;
            SocketChannel client = (SocketChannel) key.channel();
            Socket s = client.socket();
            ClientData clientData = (ClientData) key.attachment();
            ListenerService server = dataEvent.server;
            dataEvent.key = null;
            dataEvent.server = null;
            requestGetObjectRef.processRequest(clientData);
            // Return to sender
            int requestReply = clientData.getRequestReply();
            key.attach(clientData);
            server.send(new PendingRequest(key, requestReply));
        } catch (Exception e) {
            LOG.error("FATAL to get connection", e);
            System.exit(-1);
        }
    }
}
