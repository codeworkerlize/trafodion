/**
 * @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

 * @@@ END COPYRIGHT @@@
 */
package org.trafodion.dcs.master.listener.nio;

import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.ConfigReader;
import org.trafodion.dcs.master.listener.RequestCheckActiveMaster;
import org.trafodion.dcs.util.GetJavaProperty;

public class ListenerChecker extends Thread {
    private static  final Logger LOG = LoggerFactory.getLogger(ListenerChecker.class);
    private List<DataEvent> queue = new LinkedList<DataEvent>();

    private RequestCheckActiveMaster requestCheckActiveMaster;

    ListenerChecker(ConfigReader configReader) {
        requestCheckActiveMaster = new RequestCheckActiveMaster(configReader);

        GetJavaProperty.setProperty("hbaseclient.log4j.properties", GetJavaProperty.getProperty(Constants.DCS_CONF_DIR) + "/log4j.properties");
        GetJavaProperty.setProperty(Constants.DCS_ROOT_LOGGER, GetJavaProperty.getProperty(Constants.DCS_ROOT_LOGGER));
        GetJavaProperty.setProperty(Constants.DCS_LOG_DIR, GetJavaProperty.getProperty(Constants.DCS_LOG_DIR));
        GetJavaProperty.setProperty(Constants.DCS_LOG_FILE, GetJavaProperty.getProperty(Constants.DCS_LOG_FILE));
    }

    public void processData(ListenerService server, SelectionKey key) {
        synchronized(queue) {
            queue.add(new DataEvent(server, key));
            queue.notify();
        }
    }

    public void run() {
        DataEvent dataEvent;

        while(true) {
            try {
                // Wait for data to become available
                synchronized(queue) {
                    while(queue.isEmpty()) {
                        try {
                            queue.wait();
                        } catch (InterruptedException e) {
                            LOG.warn(e.getMessage(), e);
                        }
                    }
                    dataEvent = queue.remove(0);
                }
                SelectionKey key = dataEvent.key;
                SocketChannel client = (SocketChannel) key.channel();
                Socket s = client.socket();
                ClientData clientData = (ClientData) key.attachment();
                ListenerService server = dataEvent.server;
                dataEvent.key = null;
                dataEvent.server = null;
                requestCheckActiveMaster.processRequest(clientData);

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

