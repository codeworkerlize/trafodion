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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.master.listener.Data;
import org.trafodion.dcs.master.listener.ListenerConstants;
import org.trafodion.dcs.master.message.CancelMessage;
import org.trafodion.dcs.master.message.CheckActiveMasterMessage;
import org.trafodion.dcs.master.message.ConnectMessage;
import org.trafodion.dcs.master.message.KeepAliveMessage;
import org.trafodion.dcs.master.message.Header;
import org.trafodion.dcs.master.message.ReplyMessage;

public class ClientData implements Data {
    private static final Logger LOG = LoggerFactory.getLogger(ClientData.class);

    private ByteBuffer headerBuffer = null;
    private ByteBuffer bodyBuffer = null;
    private ByteBuffer[] buf = null;
    private long totalRead;
    private long totalWrite;
    private int bufferState = ListenerConstants.BUFFER_INIT;

    private Header header = null;
    private ConnectMessage connectMessage = null;
    private CancelMessage cancelMessage = null;
    private KeepAliveMessage keepAliveMessage = null;
    private CheckActiveMasterMessage checkActiveMasterMessage = null;

    private SocketAddress clientSocketAddress = null;
    private int requestReply = 0;

    ClientData(SocketAddress clientSocketAddress) {
        headerBuffer = ByteBuffer.allocate(ListenerConstants.HEADER_SIZE);
        bodyBuffer = ByteBuffer.allocate(ListenerConstants.BODY_SIZE);
        buf = new ByteBuffer[] {headerBuffer, bodyBuffer};

        totalRead = 0;
        totalWrite = 0;

        this.clientSocketAddress = clientSocketAddress;
    }

    void process() throws IOException {
        if (bufferState == ListenerConstants.BUFFER_INIT) {
            ByteBuffer hdr = buf[0];
            hdr.flip();
            header = Header.extractFromByteBuffer(hdr);
            if (header.getSignature() == ListenerConstants.LITTLE_ENDIAN_SIGNATURE) {
                switchEndian();
                hdr = buf[0];
                header = Header.extractFromByteBuffer(hdr);
            }
            if (header.getSignature() != ListenerConstants.BIG_ENDIAN_SIGNATURE)
                throw new IOException("Wrong signature in read Header : " + header.getSignature());
            bufferState = ListenerConstants.HEADER_PROCESSED;
        }
        if (bufferState == ListenerConstants.HEADER_PROCESSED) {
            ByteBuffer body = buf[1];
            body.flip();
            switch (header.getOperationId()) {
                case ListenerConstants.DCS_MASTER_GETSRVRAVAILABLE:
                    connectMessage = ConnectMessage.extractFromByteBuffer(body);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("receive connect message : {}.", connectMessage.toString());
                    }
                    break;
                case ListenerConstants.DCS_MASTER_CANCELQUERY:
                    cancelMessage = CancelMessage.extractCancelMessageFromByteBuffer(body);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("receive cancel message : {}.", cancelMessage.toString());
                    }
                    break;
                case ListenerConstants.DCS_MASTER_KEEPALIVECHECK:
                    keepAliveMessage = KeepAliveMessage.extractDcsMxoMessageFromByteBuffer(body);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("receive DcsMxoKeepAliveCheck message : {}.",
                                keepAliveMessage.toString());
                    }
                    break;
                case ListenerConstants.DCS_MASTER_CHECKACTIVEMASTER:
                    checkActiveMasterMessage = CheckActiveMasterMessage.extractDcsMxoMessageFromByteBuffer(body);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("receive CheckActiveMaster message : {}.",
                                checkActiveMasterMessage.toString());
                    }
                    break;
                default:
                    break;
            }
            bufferState = ListenerConstants.BODY_PROCESSED;
        }
    }

    void clear() {
        headerBuffer.clear();
        headerBuffer = null;
        bodyBuffer.clear();
        bodyBuffer = null;
        buf[0] = null;
        buf[1] = null;
        buf = null;
        header = null;
        connectMessage = null;
        cancelMessage = null;
        totalRead = 0;
        totalWrite = 0;
        bufferState = ListenerConstants.BUFFER_INIT;
        clientSocketAddress = null;
        requestReply = 0;
    }

    void addTotalRead(long readLen) {
        this.totalRead += readLen;
    }

    long getTotalRead() {
        return totalRead;
    }

    long getTotalWrite() {
        return totalWrite;
    }

    void addTotalWrite(long totalWrite) {
        this.totalWrite += totalWrite;
    }

    public int getBufferState() {
        return bufferState;
    }

    ByteBuffer[] getByteBufferArray() {
        return buf;
    }

    void switchEndian() {
        ByteOrder buffOrder = headerBuffer.order();
        if (buffOrder == ByteOrder.BIG_ENDIAN)
            buffOrder = ByteOrder.LITTLE_ENDIAN;
        else if (buffOrder == ByteOrder.LITTLE_ENDIAN)
            buffOrder = ByteOrder.BIG_ENDIAN;
        headerBuffer.order(buffOrder);
        bodyBuffer.order(buffOrder);
        headerBuffer.position(0);
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public ConnectMessage getConnectMessage() {
        return connectMessage;
    }

    @Override
    public SocketAddress getClientSocketAddress() {
        return clientSocketAddress;
    }

    @Override
    public void setRequestReply(int requestReply) {
        this.requestReply=requestReply;
    }

    public int getRequestReply() {
        return requestReply;
    }

    @Override
    public void setReplyMessage(ReplyMessage replyMessage) throws IOException {
        headerBuffer.clear();
        bodyBuffer.clear();

        switch (header.getOperationId()) {
            case ListenerConstants.DCS_MASTER_GETSRVRAVAILABLE:
                switch (header.getVersion()) {
                    case ListenerConstants.CLIENT_HEADER_VERSION_BE: //from jdbc
                        header.setSwap(ListenerConstants.YES);
                        headerBuffer.order(ByteOrder.BIG_ENDIAN);
                        bodyBuffer.order(ByteOrder.LITTLE_ENDIAN);
                        header.setVersion(ListenerConstants.SERVER_HEADER_VERSION_LE);
                        break;
                    case ListenerConstants.CLIENT_HEADER_VERSION_LE: //from odbc
                        header.setSwap(ListenerConstants.NO);
                        headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
                        bodyBuffer.order(ByteOrder.LITTLE_ENDIAN);
                        header.setVersion(ListenerConstants.SERVER_HEADER_VERSION_LE);
                        break;
                    default:
                        throw new IOException(clientSocketAddress + ": " + "Wrong Header Version");
                }
                boolean ODBC = connectMessage.getClientComponentId()
                        != ListenerConstants.JDBC_DRVR_COMPONENT;
                replyMessage.insertIntoByteBufferCompatible(bodyBuffer, ODBC);
                break;
            case ListenerConstants.DCS_MASTER_CANCELQUERY:
                replyMessage.insertIntoByteBuffer(bodyBuffer);
                break;
            case ListenerConstants.DCS_MASTER_KEEPALIVECHECK:
                replyMessage.insertIntoByteBuffer(bodyBuffer,
                        ListenerConstants.DCS_MASTER_KEEPALIVECHECK);
                break;
            case ListenerConstants.DCS_MASTER_CHECKACTIVEMASTER:
                replyMessage.insertIntoByteBuffer(bodyBuffer,
                        ListenerConstants.DCS_MASTER_CHECKACTIVEMASTER,ListenerConstants.DCS_MASTER_CHECKACTIVEMASTER);
                break;
            default:
                if (LOG.isInfoEnabled()) {
                    LOG.info("Unknown OperationId...");
                }
        }
        bodyBuffer.flip();
        header.setTotalLength(bodyBuffer.limit());
        header.insertIntoByteBuffer(headerBuffer);
        headerBuffer.flip();
    }

    @Override
    public CancelMessage getCancelMessage() {
        return cancelMessage;
    }

    @Override
    public KeepAliveMessage getKeepAliveMessage() {
        return keepAliveMessage;
    }

    @Override
    public CheckActiveMasterMessage getCheckActiveMasterMessage() {
        return checkActiveMasterMessage;
    }
}
