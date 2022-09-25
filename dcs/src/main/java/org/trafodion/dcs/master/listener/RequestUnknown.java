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
import org.trafodion.dcs.master.message.ReplyExceptionMessage;
import org.trafodion.dcs.master.message.ReplyMessage;

public class RequestUnknown {
    private static final Logger LOG = LoggerFactory.getLogger(RequestUnknown.class);

    public RequestUnknown(ConfigReader listener) {}

    public void processRequest(Data clientData) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Request Unknown.");
        }
        boolean cancelConnection = false;
        SocketAddress s = clientData.getClientSocketAddress();

        ReplyExceptionMessage exception =new ReplyExceptionMessage(ListenerConstants.DcsMasterParamError_exn, 0,
                        "Api is not implemented [" + clientData.getHeader().getOperationId() + "]");
        ReplyMessage reply = new ReplyMessage(exception);
        LOG.warn("<{}> : <{}>", s, exception.toString());
        try {
            clientData.setReplyMessage(reply);
        } catch (UnsupportedEncodingException ue){
            LOG.error("UnsupportedEncodingException in RequestUnknown: <{}> : <{}>.", s, ue.getMessage(), ue);
            cancelConnection = true;
        } catch (BufferUnderflowException e){
            LOG.error("BufferUnderflowException in RequestUnknown: <{}> : <{}>.", s, e.getMessage(), e);
            cancelConnection = true;
        } catch (IOException e) {
            LOG.error("IOException in RequestUnknown: <{}> : <{}>.", s, e.getMessage(), e);
            cancelConnection = true;
        }

        if (cancelConnection) {
            clientData.setRequestReply(ListenerConstants.REQUST_CLOSE);
        } else {
            clientData.setRequestReply(ListenerConstants.REQUST_WRITE_EXCEPTION);
        }
    }
}
