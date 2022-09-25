package org.apache.hadoop.hbase.coprocessor.transactional.server;

import lombok.Getter;
import org.apache.hadoop.hbase.coprocessor.transactional.message.RSMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockLogLevel;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.Channels;
import java.util.Arrays;

public class RSConnection implements LockSizeof {
    private static Logger LOG = Logger.getLogger(RSConnection.class);
    private static final int TOTAL_RETRY_TIMES = 20;

    private String host;
    private int port;
    @Getter
    private SocketChannel ch;
    private boolean bConnected = false;
    private boolean isClient = false;
    private boolean noLogOutput = false;

    public RSConnection(String host, int port) throws Exception {
        this(host, port, false);
    }

    public RSConnection(String host, int port, boolean noLogoutput) throws Exception {
        this.host = host;
        this.port = port;
        this.noLogOutput = noLogoutput;
        this.ch = SocketChannel.open();
        this.ch.configureBlocking(true);
        this.ch.socket().setSoTimeout(5000);
        checkConnection();
        this.isClient = true;
    }

    public RSConnection(SocketChannel ch) throws Exception {
        this.ch = ch;
        this.bConnected = ch.isConnected();
        if (bConnected) {
            this.host = ch.socket().getInetAddress().getHostName();
            this.port = ch.socket().getPort();
        } else {
            checkConnection();
        }
        this.isClient = false;
    }

    private void checkConnection() throws Exception {
        try{
            if (!bConnected) {
                bConnected = ch.connect(new InetSocketAddress(host, port));
                int waitCount = LockConstants.LM_TRYCONNECT_TIMES;
                while (!bConnected) {
                    if (waitCount >= 0 && waitCount-- == 0) {
                        throw new Exception("Failed to connect IP:" + host + ", PORT:" + port);
                    }
                    bConnected = ch.finishConnect();
                    Thread.sleep(10);
                }
            }
        } catch (Exception e) {
            //We need log for finger out who is dead
            if (!noLogOutput) {
                LOG.error("failed to connect " + host + ":" + port, e);
            }
            throw new Exception("failed to connect " + host + ":" + port);
        }
    }

    public void send(RSMessage message) throws Exception {
        try {
            byte[] msgBytes = RSMessage.getBytes(message);
            byte[] buf = new byte[msgBytes.length + 4];
            buf[0] = (byte)((msgBytes.length >> 24) & 0xFF);
            buf[1] = (byte)((msgBytes.length >> 16) & 0xFF);
            buf[2] = (byte)((msgBytes.length >> 8) & 0xFF);
            buf[3] = (byte)((msgBytes.length) & 0xFF);
            System.arraycopy(msgBytes, 0, buf, 4, msgBytes.length);
            ByteBuffer bb = ByteBuffer.wrap(buf);
            int ret = -1;
            int retryTimes = 0;
            do {
                ret = ch.write(bb);
                if (ret <=0) {
                    retryTimes++;
                    if (LockLogLevel.enableDebugLevel) {
                        byte[] tmpBytes = new byte[10];
                        System.arraycopy(msgBytes, 0, tmpBytes, 0, 10);
                        LOG.info("bytebuffer position: " + bb.position() + ",limit: " + bb.limit() + " msgBytes(head 10 bytes):" + Arrays.toString(tmpBytes) + ",write ret:" + ret + ",thread: " + Thread.currentThread().getName());
                    }
                    try {
                        Thread.sleep(5);
                    } catch (Exception e) {}
                    if (retryTimes > TOTAL_RETRY_TIMES) {
                        throw new Exception("failed to write, it's return " + ret);
                    }
                } else {
                    retryTimes = 0;
                }
            } while (bb.hasRemaining());
        } catch (Exception e) {
            String msg = "failed to send message " + host + ":" + port + " ";
            LOG.error(msg + message, e);
            bConnected = false;
            //throw e;
            if (e instanceof IOException) {
                throw new IOException(msg, e);
            }
            throw new Exception(msg, e);
        }
    }

    public RSMessage receive() throws Exception {
        try {
            int bytesRead = -1;
            ByteBuffer buffer = ByteBuffer.allocate(4);
            while (buffer.position() != 4) {
                bytesRead = ch.read(buffer);
                if (bytesRead == -1) {
                    bConnected = false;
                    throw new IOException("Connection is broken " + host + ":" + port);
                }
            }
            byte[] data = buffer.array();
            int length = (int)((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) |
                    ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
            buffer = ByteBuffer.allocate(length);
            while (buffer.position() != buffer.limit()) {
                bytesRead = ch.read(buffer);
                if (bytesRead == -1) {
                    bConnected = false;
                    throw new IOException("Connection is broken " + host + ":" + port);
                }
                ch.read(buffer);
            }
            return RSMessage.decodeBytes(buffer.array());
        } catch (Exception e) {
            String msg = "failed to receive message from " + host + ":" + port;
            if (!noLogOutput) {
                LOG.error(msg, e);
            }
            if (e instanceof IOException) {
                throw new IOException(msg, e);
            }
            throw new Exception(msg, e);
        }
    }

    //http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4614802
    public RSMessage receive_timeout(int timeout) throws Exception {
        try {
            int bytesRead = -1;
            if (timeout != 0) {
                ch.socket().setSoTimeout(timeout);
            }
            ReadableByteChannel readCh = Channels.newChannel(ch.socket().getInputStream());
            ByteBuffer buffer = ByteBuffer.allocate(4);
            while (buffer.position() != 4) {
                bytesRead = readCh.read(buffer);
                if (bytesRead == -1) {
                    bConnected = false;
                    throw new IOException("Connection is broken " + host + ":" + port);
                }
            }
            byte[] data = buffer.array();
            int length = (int) ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) | ((data[2] & 0xFF) << 8)
                    | (data[3] & 0xFF);
            buffer = ByteBuffer.allocate(length);
            while (buffer.position() != buffer.limit()) {
                bytesRead = readCh.read(buffer);
                if (bytesRead == -1) {
                    bConnected = false;
                    throw new IOException("Connection is broken " + host + ":" + port);
                }
            }
            return RSMessage.decodeBytes(buffer.array());
        } catch (Exception e) {
            String errorMsg = "failed to receive message from " + host + ":" + port;
            if (!noLogOutput) {
                LOG.error(errorMsg, e);
            }
            if (e instanceof IOException) {
                throw new IOException(errorMsg, e);
            }
            throw new Exception(errorMsg, e);
        }
    }

    public void close() {
        if (ch == null) {
            return;
        }
        if (isClient) {
            try {
                this.send(RSMessage.getMessage(RSMessage.MSG_TYP_CLOSE));
            } catch (Exception e) {
            }
        }
        try {
            ch.close();
            ch = null;
        } catch (Exception e) {
            if (!noLogOutput) {
                LOG.error("failed to close connection", e);
            }
        }
    }

    public String toString() {
        if (ch == null) {
            return "";
        }
        StringBuffer message = new StringBuffer(100);
        message.append(" isClient:").append(isClient).append(" remote:").append(ch.socket().getRemoteSocketAddress().toString()).append(" local:").append(ch.socket().getLocalAddress().toString());
        return message.toString();
    }

    @Override
    public long sizeof() {
        long size = Size_Object;
        //6 attributes
        size += 6 * Size_Reference;
        //4 int + boolean
        size += 16;
        //1 String
        //String = char[] + header of String
        size += Size_String + Size_Array + host.length();
        //1 Object
        size += 16;

        return LockUtils.alignLockObjectSize(size);
    }
}
