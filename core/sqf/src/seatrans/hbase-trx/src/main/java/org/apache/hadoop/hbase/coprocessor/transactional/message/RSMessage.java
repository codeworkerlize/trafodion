package org.apache.hadoop.hbase.coprocessor.transactional.message;

import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import net.jpountz.lz4.*;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.*;

public class RSMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Logger LOG = Logger.getLogger(RSMessage.class);

    public static final byte MSG_TYP_OK = 'Y';

    public static final byte MSG_TYP_NO = 'N';

    public static final byte MSG_TYP_DEADLOCK_DETECT = 'D';

    public static final byte MSG_TYP_DEADLOCK_VICTIM = 'V';

    public static final byte MSG_TYP_CLOSE = 'C';

    public static final byte MSG_TYP_LOCK_INFO = 'L';

    public static final byte MSG_TYP_LOCK_INFO_SINGLE_NODE = 'S';

    public static final byte MSG_TYP_LOCK_INFO_RESPONSE = 'l';

    public static final byte MSG_TYP_LOCK_STATISTICS_INFO = 'T';

    public static final byte MSG_TYP_LOCK_STATISTICS_INFO_SINGLE_NODE = 's';

    public static final byte MSG_TYP_LOCK_STATISTICS_INFO_RESPONSE = 't';

    public static final byte MSG_TYP_LOCK_CLEAN = 'R';

    public static final byte MSG_TYP_LOCK_CLEAN_SINGLE_NODE = 'r';

    public static final byte MSG_TYP_LOCK_GET_REGION_INFO = 'I';

    public static final byte MSG_TYP_LOCK_CLEAN_XDC = 'X';

    public static final byte MSG_TYP_LOCK_CLEAN_XDC_SINGLE_NODE = 'x';

    public static final byte MSG_TYP_LOCK_CLEAN_XDC_NO_EXIST = 'n';

    public static final byte MSG_TYP_RS_PARAMETER = 'P';

    public static final byte MSG_TYP_RS_PARAMETER_SINGLE_NODE = 'p';

    public static final byte MSG_TYP_GET_RS_PARAMETER = 'Q';

    public static final byte MSG_TYP_GET_RS_PARAMETER_SINGLE_NODE = 'q';

    public static final byte MSG_TYP_GET_RS_PARAMETER_RESPONSE = 'U';

    public static final byte MSG_TYP_LOCK_CLEAN_LOCK_COUNTER = 'K';

    public static final byte MSG_TYP_LOCK_CLEAN_LOCK_COUNTER_SINGLE_NODE = 'k';

    public static final byte MSG_TYP_LOCK_APPLIED_LOCK_MSG = 'A';

    public static final byte MSG_TYP_LOCK_APPLIED_LOCK_SINGLE_NODE_MSG = 'a';

    public static final byte MSG_TYP_LOCK_APPLIED_LOCK_RESPONSE = 'W';

    public static final byte MSG_TYP_LOCK_CLEAN_LOCK_COUNTER_RESPONSE = 'w';

    public static final byte MSG_TYP_LOCK_MEM_INFO = 'M';

    public static final byte MSG_TYP_LOCK_MEM_SINGLE_NODE_INFO = 'o';

    public static final byte MSG_TYP_LOCK_MEM_INFO_RESPONSE = 'm';

    public static final RSMessage OK = getMessage(MSG_TYP_OK);

    public static final RSMessage NO = getMessage(MSG_TYP_NO);

    public static final RSMessage LOCK_CLEAN_XDC_NO_EXIST = getMessage(MSG_TYP_LOCK_CLEAN_XDC_NO_EXIST);

    public static final String LOCK_INFO = "LOCK_INFO";
    //ALL_LOCK = LOCK_STATISTICS + LOCK_AND_WAIT
    //public static final String ALL_LOCK = "ALL_LOCK";
    public static final String LOCK_WAIT = "LOCK_WAIT";
    public static final String LOCK_AND_WAIT = "LOCK_AND_WAIT";
    public static final String LOCK_CLEAN = "LOCK_CLEAN";
    public static final String DEAD_LOCK = "DEAD_LOCK";
    public static final String LOCK_STATISTICS = "LOCK_STATISTICS";
    public static final String RS_PARAMETER = "RS_PARAMETER";
    public static final String LOCK_PARAMETER = "LOCK_PARAMETER";
    public static final String GET_PARAMETER = "GET_PARAMETER";
    public static final String GET_LOCK_PARAMETER = "GET_LOCK_PARAMETER";
    public static final String GET_RS_PARAMETER = "GET_RS_PARAMETER";
    public static final String LOCK_CHECK = "LOCK_CHECK";
    public static final String LOCK_MEM = "LOCK_MEM";
    public static final String LOCK_STATUS = "LOCK_STATUS";

    @Getter
    @Setter
    protected String senderRegionServer;
    @Getter
    @Setter
    protected String receiverRegionServer;
    @Getter
    @Setter
    protected byte msgType;
    @Getter
    @Setter
    protected String errorMsg;
    @Getter
    @Setter
    protected boolean errorFlag;

    protected boolean needResponse = false;

    public RSMessage(byte msgType) {
        this.msgType = msgType;
        this.errorFlag = false;
        this.errorMsg = "";
    }

    public static final RSMessage getMessage(byte msgType) {
        switch (msgType) {
            case MSG_TYP_DEADLOCK_DETECT:
                return new LMDeadLockDetectMessage();
            case MSG_TYP_DEADLOCK_VICTIM:
                return new LMVictimMessage();
            case MSG_TYP_LOCK_INFO_RESPONSE:
                return new LMLockInfoMessage();
            case MSG_TYP_LOCK_STATISTICS_INFO:
                return new LMLockStatisticsReqMessage();
            case MSG_TYP_LOCK_STATISTICS_INFO_RESPONSE:
                return new LMLockStatisticsMessage();
            case MSG_TYP_LOCK_GET_REGION_INFO:
                return new LMMRegionInfoMsg();
            case MSG_TYP_LOCK_CLEAN_XDC:
                return new LMLockCleanXdcMessage();
            case MSG_TYP_RS_PARAMETER:
            case MSG_TYP_GET_RS_PARAMETER_SINGLE_NODE:
                return new RSParameterReqMessage(msgType);
            case MSG_TYP_GET_RS_PARAMETER_RESPONSE:
                return new RSParameterMessage();
            case MSG_TYP_LOCK_APPLIED_LOCK_RESPONSE:
                return new LMAppliedLockMessage();
            case MSG_TYP_LOCK_APPLIED_LOCK_MSG:
                return new LMAppliedLockReqMessage();
            case MSG_TYP_LOCK_MEM_INFO_RESPONSE:
                return new LMLockMemInfoMessage();
            default:
                return new RSMessage(msgType);
        }
    }

    public static final byte[] getBytes(RSMessage message) throws IOException {
        return getBytes(message, LockConstants.LOCK_MESSAGE_COMPRESS_THRESHOLD);
    }

    private static final byte[] getBytes(RSMessage message, int compressThreshold) throws IOException {
        byte[] aMessage = null;
        // TODO find out good initial size
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            // Indicate not compressed stream
            baos.write(0);
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            try {
                try {
                    oos.writeObject(message);
                } catch (NotSerializableException e) {
                    LOG.error("failed to serialize message", e);
                    throw new IOException("Can not serialize message " + message);
                }
                oos.flush();
                aMessage = baos.toByteArray();
            } finally {
                oos.close();
            }
        } finally {
            baos.close();
        }
        if (aMessage.length < compressThreshold) {
            return aMessage;
        }

        int originalTextLen = aMessage.length - 1;
        byte[] compressed = null;
        int compressedLen = 0;
        try {
            LZ4Compressor compressor = StringUtil.getCompressor();
            int maxCompressedLength = compressor.maxCompressedLength(originalTextLen);
            //1 for Compress flag
            //4 for length flag
            compressed = new byte[maxCompressedLength + 1 + 4];
            compressedLen = compressor.compress(aMessage, 1, originalTextLen, compressed, 1 + 4,
                    maxCompressedLength - 1 - 4);
        } catch (Throwable t) {
            LOG.warn(t.getMessage(), t);
            compressed = null;
            return aMessage;
        }
        aMessage = null;
        //flag for compress
        compressed[0] = 1;
        //length for uncompressed string
        compressed[1] = (byte) ((originalTextLen >> 24) & 0xFF);
        compressed[2] = (byte) ((originalTextLen >> 16) & 0xFF);
        compressed[3] = (byte) ((originalTextLen >> 8) & 0xFF);
        compressed[4] = (byte) ((originalTextLen) & 0xFF);
        return Arrays.copyOf(compressed, compressedLen + 1 + 4);
    }

    public static final RSMessage decodeBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        return decodeBytes(bytes, 0, bytes.length);
    }

    private static final RSMessage decodeBytes(byte[] bytes, int offset, int length) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = null;
        ObjectInputStream ois = null;
        try {
            if (bytes[offset] == 0) {
                bais = new ByteArrayInputStream(bytes, offset + 1, length - 1);
            } else {
                //size of uncompressed string
                int originalTextLen = (int) ((bytes[offset + 1] & 0xFF) << 24) | ((bytes[offset + 2] & 0xFF) << 16)
                        | ((bytes[offset + 3] & 0xFF) << 8) | (bytes[offset + 4] & 0xFF);
                LZ4SafeDecompressor decompressor = StringUtil.getDecompressor();
                byte[] restored = new byte[originalTextLen];
                decompressor.decompress(bytes, offset + 5, length - offset - 5, restored, 0);
                bais = new ByteArrayInputStream(restored);
            }
            ois = new ObjectInputStream(bais);
            return (RSMessage) ois.readObject();
        } catch (Throwable t) {
            LOG.warn(t.getMessage(), t);
        } finally {
            if (ois != null) {
                ois.close();
            }
            if (bais != null) {
                bais.close();
            }
        }
        return null;
    }

    public String toString() {
        switch (msgType) {
            case RSMessage.MSG_TYP_OK:
                return "OK";
            case RSMessage.MSG_TYP_NO:
                return "NO";
            case RSMessage.MSG_TYP_DEADLOCK_DETECT:
                return "DEADLOCK_DETECT";
            case RSMessage.MSG_TYP_DEADLOCK_VICTIM:
                return "DEADLOCK_VICTIM";
            case RSMessage.MSG_TYP_CLOSE:
                return "CLOSE";
            case RSMessage.MSG_TYP_LOCK_INFO:
                return "LOCK_INFO_REQUEST";
            case RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE:
                return "LOCK_INFO_SINGLE_NODE";
            case RSMessage.MSG_TYP_LOCK_INFO_RESPONSE:
                return "LOCK_INFO_RESPONSE";
            case RSMessage.MSG_TYP_LOCK_CLEAN:
                return "LOCK_CLEAN";
            case RSMessage.MSG_TYP_LOCK_CLEAN_SINGLE_NODE:
                return "LOCK_CLEAN_SINGLE_NODE";
            case RSMessage.MSG_TYP_LOCK_GET_REGION_INFO:
                return "MSG_TYP_LOCK_GET_REGION_INFO";
            case RSMessage.MSG_TYP_LOCK_CLEAN_XDC:
                return "MSG_TYP_LOCK_CLEAN_XDC";
            case RSMessage.MSG_TYP_LOCK_CLEAN_XDC_SINGLE_NODE:
                return "MSG_TYP_LOCK_CLEAN_XDC_SINGLE_NODE";
            case RSMessage.MSG_TYP_RS_PARAMETER_SINGLE_NODE:
                return "MSG_TYP_LOCK_PARAMETER_SINGLE_NODE";
            case RSMessage.MSG_TYP_GET_RS_PARAMETER:
                return "MSG_TYP_GET_LOCK_PARAMETER";
            case RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_MSG:
                return "MSG_TYP_LOCK_APPLIED_LOCK_MSG";
            case RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_SINGLE_NODE_MSG:
                return "MSG_TYP_LOCK_APPLIED_LOCK_SINGLE_NODE_MSG";
            case RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_RESPONSE:
                return "MSG_TYP_LOCK_APPLIED_LOCK_RESPONSE";
            case RSMessage.MSG_TYP_LOCK_CLEAN_LOCK_COUNTER_RESPONSE:
                return "MSG_TYP_LOCK_CLEAN_LOCK_COUNTER_RESPONSE";
            case RSMessage.MSG_TYP_LOCK_MEM_INFO:
                return "MSG_TYP_LOCK_MEM_INFO";
            case RSMessage.MSG_TYP_LOCK_MEM_INFO_RESPONSE:
                return "MSG_TYP_LOCK_MEM_INFO_RESPONSE";
            default:
                return "message type: " + msgType;
        }
    }

    public void merge(RSMessage message) {
        return;
    }
}
