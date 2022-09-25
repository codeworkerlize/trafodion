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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.master.Metrics;
import org.trafodion.dcs.master.listener.ConfigReader;
import org.trafodion.dcs.master.listener.ListenerConstants;
import org.trafodion.dcs.master.listener.Util;
import org.trafodion.dcs.zookeeper.ZkClient;

public class ListenerService implements Runnable {
    private static  final Logger LOG = LoggerFactory.getLogger(ListenerService.class);
    private int selectorTimeout;
    private int requestTimeout;
    private ServerSocketChannel server=null;
    private Selector selector=null;
    private int port;
    public Metrics metrics;
    private ListenerWorker worker=null;
    private ListenerConnecter connecter = null;
    private ListenerChecker checker = null;
    private List<PendingRequest> pendingChanges = new LinkedList<PendingRequest>();	//list of PendingRequests instances
    private HashMap<SelectionKey, Long> timeouts = new HashMap<SelectionKey, Long>(); // hash map of timeouts

    private ConfigReader configReader;
    private ListenerCancel cancel;

    public ListenerService(ZkClient zkc, int port, int requestTimeout, int selectorTimeout, Metrics metrics, Configuration conf) {
        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Do init for ListenerService. port <{}>, requestTimeout <{}>, selectorTimeout <{}>",
                    port, requestTimeout, selectorTimeout);
        }
        this.port = port;
        this.requestTimeout = requestTimeout;
        this.selectorTimeout = selectorTimeout;
        this.metrics = metrics;
        this.configReader = new ConfigReader(conf, zkc);
        try {
            if (metrics != null) {
                metrics.initListenerMetrics(System.nanoTime());
            }
            worker = new ListenerWorker(configReader);
            new Thread(worker).start();
            if (LOG.isInfoEnabled()) {
                LOG.info("Start ListenerWorker thread.");
            }

            connecter = new ListenerConnecter(configReader);
            new Thread(connecter).start();
            if (LOG.isInfoEnabled()) {
                LOG.info("Start ListenerConnecter thread.");
            }

            checker = new ListenerChecker(configReader);
            new Thread(checker).start();
            if (LOG.isInfoEnabled()) {
                LOG.info("Start ListenerChecker thread.");
            }

            new Thread(this).start();
            if (LOG.isInfoEnabled()) {
                LOG.info("Start ListenerService thread.");
            }

            cancel = new ListenerCancel(configReader);
            if (LOG.isInfoEnabled()) {
                LOG.info("init ListenerCancel thread.");
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    public ConfigReader getConfigReader() {
        return configReader;
    }

    public void send(PendingRequest preq) {
        synchronized (this.pendingChanges) {
            this.pendingChanges.add(preq);
        }
        this.selector.wakeup();
    }
    
    private int setSelectorTimeout(){
        if (!timeouts.isEmpty()){
            return selectorTimeout;
        }
        else {
            return 0;
        }
    }

    private static void gc() {
         Object obj = new Object();
         java.lang.ref.WeakReference ref = new java.lang.ref.WeakReference<Object>(obj);
         obj = null;
         while(ref.get() != null) {
             System.gc();
         }
    }

    public void run () {
        try {
            selector = SelectorProvider.provider().openSelector();
            if (LOG.isDebugEnabled()) {
                LOG.debug("ServerSocketChannel.open()");
            }
            server = ServerSocketChannel.open();
            server.configureBlocking(false);
            InetSocketAddress isa = new InetSocketAddress(port); //use any ip address for this port
            server.socket().bind(isa);
            SelectionKey serverkey = server.register(selector, SelectionKey.OP_ACCEPT );
            int keysAdded = 0;
            PendingRequest preq = null;

            while(true){
                synchronized (this.pendingChanges) {
                    Iterator<PendingRequest> changes = this.pendingChanges.iterator();
                    while (changes.hasNext()) {
                        preq = changes.next();
                        SelectionKey key = preq.key;
                        int request = preq.request;
                        switch(request){
                        case ListenerConstants.REQUST_WRITE_EXCEPTION:
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("request write exception.");
                            }
                            if(metrics != null) {
                                metrics.listenerNoAvailableServers();
                            }
                        case ListenerConstants.REQUST_WRITE:
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("request write.");
                            }
                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                            break;
                        case ListenerConstants.REQUST_CLOSE:
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("request close.");
                            }
                            try {
                                SocketChannel client = (SocketChannel)key.channel();
                                client.close();
                                ClientData clientData = (ClientData) key.attachment();
                                clientData.clear();;
                                key.cancel();
                                if(metrics != null) metrics.listenerRequestRejected();
                            } catch (IOException e) {}
                            break;
                        }
                        preq.key = null;
                    }
                    this.pendingChanges.clear();
                }

                while ((keysAdded = selector.select(setSelectorTimeout())) > 0) {
                    Set<SelectionKey> keys = selector.selectedKeys();
                    Iterator<SelectionKey> i = keys.iterator();

                    while (i.hasNext()) {
                        SelectionKey key = i.next();
                        i.remove();
                        if (!key.isValid()) continue;

                        if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Ready to process ACCEPT");
                            }
                            processAccept(key);
                        } else if ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Ready to process READ");
                            }
                            processRead(key);
                        } else if ((key.readyOps() & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Ready to process WRITE");
                            }
                            processWrite(key);
                        }
                    }
                }
                if (!timeouts.isEmpty()){
                    long currentTime = System.currentTimeMillis();
                    Iterator<SelectionKey> i = timeouts.keySet().iterator();
                    while(i.hasNext()){
                      SelectionKey key = i.next();
                      if (currentTime - timeouts.get(key) > requestTimeout){
                          long timeout = (currentTime - timeouts.get(key))/1000;
                          SocketChannel client = (SocketChannel) key.channel();
                          Socket s = client.socket();
                          if ((key.interestOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ){
                              if (LOG.isDebugEnabled()) {
                                  LOG.debug("Read from client timeouted <{}> seconds from : <{}>.",
                                          timeout, s.getRemoteSocketAddress());
                              }
                                if(metrics != null) metrics.listenerReadTimeout();
                          }
                          else if ((key.interestOps() & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
                              if (LOG.isDebugEnabled()) {
                                  LOG.debug("Write to client timeouted <{}> seconds from : <{}>.",
                                          timeout, s.getRemoteSocketAddress());
                              }
                                if(metrics != null) metrics.listenerWriteTimeout();
                          }
                          else {
                              if (LOG.isDebugEnabled()) {
                                  LOG.debug("Client timeouted <{}> seconds from : <{}>.", timeout,
                                          s.getRemoteSocketAddress());
                              }
                                if(metrics != null) metrics.listenerRequestRejected();
                          }
                          try {
                              client.close();
                          } catch (IOException ex) {
                              LOG.warn(ex.getMessage(), ex);
                          }
                          ClientData clientData = (ClientData) key.attachment();
                          clientData.clear();
                          key.attach(null);
                          key.cancel();
                          i.remove();
                        }
                    }
                }
                //gc();
            }
        }catch (Throwable e){
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }finally {
            if (server != null) {
                try {
                    server.close();
                    LOG.error("ListenerService and socket on port {} were closed unexpectedly.",
                            port);
                } catch (IOException e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
        }
    }
    private void processAccept(SelectionKey key) {
        try {

            ServerSocketChannel server = (ServerSocketChannel)key.channel();
            SocketChannel client = server.accept();
            Socket s = client.socket();
            // Accept the request
            this.metrics.listenerStartRequest(System.nanoTime());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received an incoming connection from: <{}>.",
                        s.getRemoteSocketAddress());
            }
            client.configureBlocking( false );
            SelectionKey clientkey = client.register( selector, SelectionKey.OP_READ );
            clientkey.attach(new ClientData(s.getRemoteSocketAddress()));
            if (LOG.isDebugEnabled()) {
                LOG.debug("<{}>: Accept processed.", s.getRemoteSocketAddress());
            }
        } catch (IOException ie) {
            LOG.error("Cannot Accept connection: <{}>.", ie.getMessage(), ie);
        }
    }

    private void processRead(SelectionKey key) {

        key.interestOps( key.interestOps() ^ SelectionKey.OP_READ);

        SocketChannel client = (SocketChannel) key.channel();
        Socket s = client.socket();
        long readLength=0;

        ClientData clientData = (ClientData) key.attachment();
        if (!timeouts.isEmpty() && timeouts.containsKey(key)){
            timeouts.remove(key);
        }

        try {
            while ((readLength = client.read(clientData.getByteBufferArray())) > 0) {
                clientData.addTotalRead(readLength);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<{}>: Read readLength <{}> total <{}>.", s.getRemoteSocketAddress(),
                            readLength, clientData.getTotalRead());
                }
            }
            if (readLength == -1 ){
                throw new IOException(s.getRemoteSocketAddress() + ": Connection closed by peer on READ.");
            }

            if (clientData.getTotalRead() < ListenerConstants.HEADER_SIZE){
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
                key.attach(clientData);
                timeouts.put(key, System.currentTimeMillis());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<{}>: Read length less than HEADER size. Added timeout on READ.",
                            s.getRemoteSocketAddress());
                }
                return;
            }

            clientData.process();
            if (clientData.getTotalRead() < (clientData.getHeader().getTotalLength() + ListenerConstants.HEADER_SIZE)){
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
                key.attach(clientData);
                timeouts.put(key, System.currentTimeMillis());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{}: Total length less than in read HEADER. Added timeout on READ.",
                            s.getRemoteSocketAddress());
                }
                return;
            }
            if (clientData.getTotalRead() > (clientData.getHeader().getTotalLength() + ListenerConstants.HEADER_SIZE)){
                throw new IOException("Wrong total length in read Header : total_read " + clientData.getTotalRead() + ", hdr_total_length + hdr_size " + clientData.getHeader().getTotalLength() +  + ListenerConstants.HEADER_SIZE);
            }
            Util.toHexString("Client buf", clientData.getByteBufferArray()[1]);
            clientData.process();

            key.attach(clientData);

            short operationId = clientData.getHeader().getOperationId();
            if (LOG.isInfoEnabled()) {
                LOG.info("clientData Header OperationId : <{}>", operationId);
            }
            switch (operationId){
                case ListenerConstants.DCS_MASTER_GETSRVRAVAILABLE:
                    this.connecter.processData(this, key);
                    break;
                case ListenerConstants.DCS_MASTER_CHECKACTIVEMASTER:
                    this.checker.processData(this, key);
                    break;
                case ListenerConstants.DCS_MASTER_CANCELQUERY:
                    this.cancel.processData(this, key);
                    break;
                default:
                    this.worker.processData(this, key);
                    break;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("<{}>: Read processed.", s.getRemoteSocketAddress());
            }
        } catch (IOException ie){
            if (readLength == -1) {
                LOG.warn("IOException: <{}> : <{}>", s.getRemoteSocketAddress(), ie.getMessage(),
                        ie);
            } else {
                LOG.error("IOException: <{}> : <{}>", s.getRemoteSocketAddress(), ie.getMessage(),
                        ie);
            }
            try {
                client.close();
            } catch (IOException ex){
                LOG.warn(ex.getMessage(), ex);
            }
            clientData = (ClientData) key.attachment();
            clientData.clear();
            key.attach(null);
            key.cancel();
            if(metrics != null) metrics.listenerRequestRejected();
        }
    }

    private void processWrite(SelectionKey key) {
    
        key.interestOps(key.interestOps() ^ SelectionKey.OP_WRITE);

        SocketChannel client = (SocketChannel) key.channel();
        Socket s = client.socket();
        long writeLength=0;

        ClientData clientData = (ClientData) key.attachment();
        if (!timeouts.isEmpty() && timeouts.containsKey(key)){
            timeouts.remove(key);
        }
        try {
            while ((writeLength = client.write(clientData.getByteBufferArray())) > 0) {
                clientData.addTotalWrite(writeLength);
            }
            if (writeLength == -1 ){
                throw new IOException(s.getRemoteSocketAddress() + ": " + "Connection closed by peer on WRITE");
            }

            if (clientData.getByteBufferArray()[0].hasRemaining() || clientData.getByteBufferArray()[1].hasRemaining()){
                key.attach(clientData);
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                timeouts.put(key, System.currentTimeMillis());
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "<{}>: Write length less than total write length. Added timeout on WRITE.",
                            s.getRemoteSocketAddress());
                }
            } else {
                client.close();
                clientData.clear();
                key.attach(null);
                key.cancel();
                if(metrics != null)metrics.listenerEndRequest(System.nanoTime());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{}: Write processed.", s.getRemoteSocketAddress());
                }
            }
        } catch (IOException ie){
            LOG.error("IOException: <{}> : <{}>", s.getRemoteSocketAddress(), ie.getMessage(), ie);
            try {
                client.close();
            } catch (IOException ex) {
                LOG.warn(ex.getMessage(), ex);
            }
            clientData.clear();
            key.attach(null);
            key.cancel();
            if(metrics != null)metrics.listenerRequestRejected();
        }
    }
}
