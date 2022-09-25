package org.trafodion.dcs.master.listener.nio;


import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.ConfigReader;
import org.trafodion.dcs.master.listener.RequestCancelQuery;
import org.trafodion.dcs.util.GetJavaProperty;

public class ListenerCancel {
    private static  final Logger LOG = LoggerFactory.getLogger(ListenerCancel.class);
    private final ThreadPoolExecutor threadPool;
    private ConfigReader configReader;

    ListenerCancel(ConfigReader configReader){
        GetJavaProperty.setProperty("hbaseclient.log4j.properties", GetJavaProperty.getProperty(
                Constants.DCS_CONF_DIR) + "/log4j.properties");
        this.configReader = configReader;
        this.threadPool  = (ThreadPoolExecutor)Executors.newFixedThreadPool(3);
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                threadPool.shutdownNow();
            }
        } );
        this.threadPool .setKeepAliveTime(1, TimeUnit.SECONDS);
        this.threadPool .allowCoreThreadTimeOut(true);
        GetJavaProperty.setProperty(Constants.DCS_ROOT_LOGGER, GetJavaProperty.getProperty(Constants.DCS_ROOT_LOGGER));
        GetJavaProperty.setProperty(Constants.DCS_LOG_DIR, GetJavaProperty.getProperty(Constants.DCS_LOG_DIR));
        GetJavaProperty.setProperty(Constants.DCS_LOG_FILE, GetJavaProperty.getProperty(Constants.DCS_LOG_FILE));
    }

    public void processData(ListenerService server, SelectionKey key) {
        threadPool.execute(new ListenerCancelRunnable((configReader),server, key));
    }

    class ListenerCancelRunnable implements Runnable{
        private RequestCancelQuery requestCancelQuery;
        private DataEvent dataEvent;

        ListenerCancelRunnable(ConfigReader configReader,ListenerService server, SelectionKey key){
            requestCancelQuery = new RequestCancelQuery(configReader);
            dataEvent = new DataEvent(server, key);
        }
        @Override
        public void run() {
            try {
                SelectionKey key = dataEvent.key;
                SocketChannel client = (SocketChannel) key.channel();
                Socket s = client.socket();
                ClientData clientData = (ClientData) key.attachment();
                ListenerService server = dataEvent.server;
                dataEvent.key = null;
                dataEvent.server = null;
                // process cancel Request
                requestCancelQuery.processRequest(clientData);
                // Return to sender
                int requestReply = clientData.getRequestReply();
                key.attach(clientData);
                server.send(new PendingRequest(key, requestReply));
            } catch (Exception e){
                LOG.error("Unexpected Exception", e);
                System.exit(-1);
            }
        }
    }
}
