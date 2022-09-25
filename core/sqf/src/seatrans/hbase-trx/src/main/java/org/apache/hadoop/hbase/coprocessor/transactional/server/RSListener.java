package org.apache.hadoop.hbase.coprocessor.transactional.server;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockLogLevel;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

public class RSListener extends Thread {
    private static Logger LOG = Logger.getLogger(RSListener.class);

    private int port;
    private ServerSocketChannel channel = null;
    private Selector selector = null;
    private RSServer rsServer;
    private Stoppable stopper;

    public RSListener(RSServer rsServer, int port, Stoppable stopper) throws IOException {
        this.rsServer = rsServer;
        this.port = port;
        this.stopper = stopper;
        open();
    }

    public void open() throws IOException {
        selector = Selector.open();
        // create a server socket channel and bind to port
        channel = ServerSocketChannel.open();
        channel.configureBlocking(false);
        InetSocketAddress isa = new InetSocketAddress(port);
        channel.socket().bind(isa);

        // register interest in Connection Attempts by clients
        channel.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void run() {
        if (rsServer == null) {
            return;
        }
        while (true) {
            try {
                if (selector.select(1000) > 0) {
                    Set<SelectionKey> readyKeys = selector.selectedKeys();
                    Iterator<SelectionKey> readyIter = readyKeys.iterator();
                    while (readyIter.hasNext()) {
                        // get the key
                        SelectionKey key = (SelectionKey) readyIter.next();

                        // remove the current key
                        readyIter.remove();

                        if (key.isAcceptable()) {
                            doAccept(key);
                        }
                    } // end while readyIter
                }
            } catch (ClosedChannelException e) {
                LOG.error("failed to close channel", e);
            } catch (Exception e) {
                LOG.error("failed to connect for client", e);
            }
        }
        /*reserve for stopper
        rsServer.close();
        close();
        */
    }

    private void doAccept(SelectionKey key) {
        ServerSocketChannel serverSocketChannel = null;
        try {
            serverSocketChannel = (ServerSocketChannel) key.channel();
            SocketChannel socketChannel;
            while ((socketChannel = serverSocketChannel.accept()) != null) {
                String remoteAddress = socketChannel.socket().getRemoteSocketAddress().toString();
                try {
                    socketChannel.configureBlocking(false);
                    socketChannel.socket().setTcpNoDelay(LockConstants.LM_TCPNODELAY);
                    socketChannel.socket().setKeepAlive(LockConstants.LM_TCPKEEPLIVE);
                    //socketChannel.socket().setReuseAddress(true);
                    socketChannel.socket().setSoTimeout(1000);
                    if (socketChannel.isConnectionPending()) {
                        socketChannel.finishConnect();
                    }
                } catch (IOException e) {
                    LOG.error("failed to finish connect for " + remoteAddress, e);
                    try {
                        socketChannel.close();
                    } catch (Exception e1){}
                    throw e;
                }

                try {
                    if (LockLogLevel.enableTraceLevel) {
                        LOG.info("success connected from " + remoteAddress);
                    }
                    RSSession session = rsServer.getSession(remoteAddress);
                    session.registerChannel(socketChannel);
                    session.startWork();
                } catch (Exception e) {
                    LOG.error("failed to start session for " + remoteAddress, e);
                }
            }
        } catch (Exception e) {
            LOG.error("failed to accept and close the server socket channel", e);
            try {
                serverSocketChannel.close();
            } catch (IOException ioe) {
                LOG.error("failed to close server socket chanel", ioe);
            }
        }
    }

    private void close() {
        try {
            this.channel.close();
        } catch (IOException e) {
            LOG.error("failed to close listener channel", e);
        }
    }
}
