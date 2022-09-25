package org.apache.hadoop.hbase.coprocessor.transactional.server;

import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockLogLevel;
import org.apache.hadoop.hbase.coprocessor.transactional.message.RSMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class RSSession extends Thread implements LockSizeof {
    private  static final Logger LOG = Logger.getLogger(RSSession.class);

    private RSServer rsServer;
    private Selector readSelector;
    private boolean inWorking = false;
    private Stoppable stopper;
    private RSConnection rsConnection;

    public RSSession(RSServer rsServer, Stoppable stopper) throws IOException {
        this.rsServer = rsServer;
        this.stopper = stopper;
        this.readSelector = Selector.open();
    }

    @Override
    public void run() {
        while (true/*!stopper.isStopped()*/) {
            try {
                if (!inWorking) {
                    synchronized(this) {
                        this.wait(1000);
                    }
                    continue;
                }

                if (readSelector.select(1000) < 1) {
                    continue;
                }
                Iterator<SelectionKey> it = readSelector.selectedKeys().iterator();
                while(it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if(key.isValid()) {
                        if(key.isReadable()) {
                            doRead(key);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private void doRead(SelectionKey selectionKey) {
        rsConnection = (RSConnection) selectionKey.attachment();
        if(rsConnection == null) {
            return;
        }
        if (LockLogLevel.enableTraceLevel) {
            LOG.info("session:" + this + " " + rsConnection + " startWork");
        }
        try {
            RSMessage lmMessage = rsConnection.receive();
            if (lmMessage.getMsgType() == lmMessage.MSG_TYP_CLOSE) {
                if (LockLogLevel.enableTraceLevel) {
                    LOG.info("session:" + this + " " + rsConnection + " stopWork and enter idleSession cache");
                }
                rsConnection.close();
                rsConnection = null;
                rsServer.releaseSession(this);
                return;
            }
            rsServer.processRSMessage(lmMessage, rsConnection);
        } catch (Exception e) {
            LOG.error("failed to read message: " + rsConnection, e);
            if (rsConnection != null) {
                rsConnection.close();
                rsConnection = null;
            }
            rsServer.releaseSession(this);
            LOG.error("session:" + this + " " + rsConnection + " get exception, stopWork and enter idleSession cache");
        }
    }

    public void registerChannel(SocketChannel socketChannel) throws Exception {
        if (LockLogLevel.enableTraceLevel) {
            LOG.info(socketChannel.socket().getRemoteSocketAddress().toString() + "(" + socketChannel + ") register channel to session " + this);
        }
        SelectionKey readKey = socketChannel.register(readSelector, SelectionKey.OP_READ);
        readKey.attach(new RSConnection(socketChannel));
    }

    public void startWork() {
        this.inWorking = true;
        try {
            this.notifyAll();
        } catch (Exception e) {
        }
        try {
            this.readSelector.wakeup();
        } catch (Exception e) {
        }
    }

    public void stopWork() {
        this.inWorking = false;
    }

    public String toString() {
        StringBuffer message = new StringBuffer(100);
        message.append(this.getName()).append("\n");
        if (rsConnection != null) {
            message.append("remote:").append(rsConnection.getCh().socket().getRemoteSocketAddress().toString()).append("\n");
            message.append("local:").append(rsConnection.getCh().socket().getLocalAddress().toString());
        }
        return message.toString();
    }

    @Override
    public long sizeof() {
        long size = Size_Object;
        //5 attributes
        size += 5 * Size_Reference;
        //rsConnection
        if (rsConnection != null) {
            size += rsConnection.sizeof();
        }
        return LockUtils.alignLockObjectSize(size);
    }
}
