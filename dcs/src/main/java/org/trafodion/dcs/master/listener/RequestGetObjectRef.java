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
package org.trafodion.dcs.master.listener;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.nio.BufferUnderflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.master.message.ConnectMessage;
import org.trafodion.dcs.master.message.Header;
import org.trafodion.dcs.util.LicenseHolder;

public class RequestGetObjectRef {
    private static final Logger LOG = LoggerFactory.getLogger(RequestGetObjectRef.class);

    private ConnectReply connectReplay = null;

    private int enableBase64 = 1;

    public RequestGetObjectRef(ConfigReader configReader){
        enableBase64 = configReader.getEncryptBase64Enable();
        connectReplay = new ConnectReply(configReader);
    }

    public void processRequest(Data data) {
        boolean cancelConnection = false;
        boolean replyException = false;
        try {
            replyException = buildConnectReply(data);
        } catch (UnsupportedEncodingException ue){
            LOG.error("UnsupportedEncodingException in RequestGetObjectRef: <{}> : <{}>.", data.getClientSocketAddress(), ue.getMessage(), ue);
            cancelConnection = true;
        } catch (IOException io){
            LOG.error("IOException in RequestGetObjectRef: <{}> : <{}>.", data.getClientSocketAddress(), io.getMessage(), io);
            cancelConnection = true;
        } catch (BufferUnderflowException e){
            LOG.error("BufferUnderflowException in RequestGetObjectRef: <{}> : <{}>.", data.getClientSocketAddress(), e.getMessage(), e);
            cancelConnection = true;
        } catch (Exception ex){
            LOG.error("Exception in RequestGetObjectRef: <{}> : <{}>.", data.getClientSocketAddress(), ex.getMessage(), ex);
            cancelConnection = true;
        }
        // Return to sender
        if (cancelConnection) {
            data.setRequestReply(ListenerConstants.REQUST_CLOSE);
        } else if (replyException) {
            data.setRequestReply(ListenerConstants.REQUST_WRITE_EXCEPTION);
        } else {
            data.setRequestReply(ListenerConstants.REQUST_WRITE);
        }
    }

    boolean buildConnectReply(Data data) throws IOException{

        Header hdr = data.getHeader();
        ConnectMessage connectMessage = data.getConnectMessage();
        SocketAddress clientSocketAddress = data.getClientSocketAddress();

        ConnectionContext conectContex = new ConnectionContext(connectMessage);

        //Check the whitelist function
        if (connectReplay.checkWhitelist(conectContex)) {
            data.setReplyMessage(connectReplay.getReplyMessage());
            return true;
        }

        conectContex.setMultiTenancy(LicenseHolder.isMultiTenancyEnabled());

        int replyExceptionNr = 0;
        for (int i = 0; i < 3; i++) {
            replyExceptionNr = connectReplay.buildConnectReply(conectContex, clientSocketAddress);
            //replyExceptionNr=0 means no exception;
            //replyExceptionNr>0 means has available servers and need to retry;
            //replyExceptionNr<0 means other exceptions and not necessary to retry.
            if (replyExceptionNr <= 0) {
                break;
            }
            LOG.warn("Retry[{}] to buildConnectReply with replyExceptionNr being {}", i,
                    replyExceptionNr);
        }

        data.setReplyMessage(connectReplay.getReplyMessage());
        if (enableBase64 > 0 && replyExceptionNr == 0) {
            connectReplay.getReplyMessage().enableBase64();
        }

        return replyExceptionNr != 0;

    }
}
