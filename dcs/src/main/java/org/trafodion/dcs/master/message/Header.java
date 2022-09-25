package org.trafodion.dcs.master.message;

import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.buffer.ByteBuf;
import org.trafodion.dcs.master.listener.ListenerConstants;

public class Header {
    private static final Logger LOG = LoggerFactory.getLogger(Header.class);
    public static final int HEAD_LEN = 40;
    public static final int BODY_LEN_INDEX = 8;
    //
    // The Java version of the HEADER structure taken from TransportBase.h
    //
    private final short operationId;// 2
    private final short compatibilityVersion;// 2
    // + 2 filler
    private final int dialogueId;
    private int totalLength;
    private final int cmpLength;
    private final byte compressInd;
    private final byte compressType;
    // + 2 filler
    private final int hdrType;
    private final int signature;
    private int version;
    private final byte platform;
    private final byte transport;
    private byte swap;
    // + 1 filler
    private final short error;
    private final short errorDetail;



    public Header(short operationId, short compatibilityVersion, int dialogueId, int totalLength, int cmpLength,
            byte compressInd, byte compressType, int hdrType, int signature, int version,
            byte platform, byte transport, byte swap, short error, short errorDetail) {
        super();
        this.operationId = operationId;
        this.compatibilityVersion = compatibilityVersion;
        this.dialogueId = dialogueId;
        this.totalLength = totalLength;
        this.cmpLength = cmpLength;
        this.compressInd = compressInd;
        this.compressType = compressType;
        this.hdrType = hdrType;
        this.signature = signature;
        this.version = version;
        this.platform = platform;
        this.transport = transport;
        this.swap = swap;
        this.error = error;
        this.errorDetail = errorDetail;
    }

    static int sizeOf() {
        return HEAD_LEN;
    }

    public short getOperationId() {
        return operationId;
    }

    public short getCompatibilityVersion() {
        return compatibilityVersion;
    }

    public int getDialogueId() {
        return dialogueId;
    }

    public int getTotalLength() {
        return totalLength;
    }

    public int getCmpLength() {
        return cmpLength;
    }

    public byte getCompressInd() {
        return compressInd;
    }

    public byte getCompressType() {
        return compressType;
    }

    public int getHdrType() {
        return hdrType;
    }

    public int getSignature() {
        return signature;
    }

    public int getVersion() {
        return version;
    }

    public byte getPlatform() {
        return platform;
    }

    public byte getTransport() {
        return transport;
    }

    public byte getSwap() {
        return swap;
    }

    public short getError() {
        return error;
    }

    public short getErrorDetail() {
        return errorDetail;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("OperationId : " + operationId);
        sb.append(", dialogueId : " + dialogueId);
        sb.append(", totalLength : " + totalLength);
        sb.append(", cmpLength : " + cmpLength);
        sb.append(", compressInd : " + String.valueOf(compressInd));
        sb.append(", compressType : " + String.valueOf(compressType));
        sb.append(", hdrType : " + hdrType);
        sb.append(", signature : " + signature);
        sb.append(", version : " + version);
        sb.append(", platform : " + String.valueOf(platform));
        sb.append(", transport : " + String.valueOf(transport));
        sb.append(", swap : " + String.valueOf(swap));
        sb.append(", error : " + error);
        sb.append(", errorDetail : " + errorDetail);
        return sb.toString();
    }

    /**
     * insert into {@link java.nio.ByteBuffer} for NIO
     * 
     * @param buf
     */
    public void insertIntoByteBuffer(ByteBuffer buf) {
        buf.putShort(operationId);
        buf.putShort(ListenerConstants.DCS_VERSION);
        buf.putInt(dialogueId);
        buf.putInt(totalLength);
        buf.putInt(cmpLength);
        buf.put((byte) compressInd);
        buf.put((byte) compressType);
        buf.put((byte) 0); // + filler
        buf.put((byte) 0); // + filler
        buf.putInt(hdrType);
        buf.putInt(signature);
        buf.putInt(version);
        buf.put((byte) platform);
        buf.put((byte) transport);
        buf.put((byte) swap);
        buf.put((byte) 0); // + filler
        buf.putShort(error);
        buf.putShort(errorDetail);
    }

    /**
     * extract From {@link java.nio.ByteBuffer} for NIO.
     * 
     * @param buf
     */
    public static Header extractFromByteBuffer(ByteBuffer buf) {
        buf.rewind();
        short operationId = buf.getShort();
        short compatibilityVersion = buf.getShort();
        int dialogueId = buf.getInt();
        int totalLength = buf.getInt();
        int cmpLength = buf.getInt();
        byte compressInd = buf.get();
        byte compressType = buf.get();
        buf.get(); // + filler
        buf.get(); // + filler
        int hdrType = buf.getInt();
        int signature = buf.getInt();
        int version = buf.getInt();
        byte platform = buf.get();
        byte transport = buf.get();
        byte swap = buf.get();
        buf.get(); // + filler
        short error = buf.getShort();
        short errorDetail = buf.getShort();
        return new Header(operationId, compatibilityVersion, dialogueId, totalLength, cmpLength, compressInd,
                compressType, hdrType, signature, version, platform, transport, swap, error,
                errorDetail);

    }

    /**
     * insert into {@link io.netty.buffer.ByteBuf} for Netty, BigEndian.
     * 
     * @param buf
     */
    public void insertIntoByteBuf(ByteBuf buf) {
        buf.writeShort(operationId);
        buf.writeByte(0x30); // + filler
        buf.writeByte(0x30); // + filler
        buf.writeInt(dialogueId);
        buf.writeInt(totalLength);// BODY_LEN_INDEX
        buf.writeInt(cmpLength);
        buf.writeByte(compressInd);
        buf.writeByte(compressType);
        buf.writeByte(0); // + filler
        buf.writeByte(0); // + filler
        buf.writeInt(hdrType);
        buf.writeInt(signature);
        buf.writeInt(version);
        buf.writeByte(platform);
        buf.writeByte(transport);
        buf.writeByte(swap);
        buf.writeByte(0); // + filler
        buf.writeShort(error);
        buf.writeShort(errorDetail);
    }

    /**
     * insert into {@link io.netty.buffer.ByteBuf} for Netty, Little Endian.
     * 
     * @param buf
     */
    public void insertIntoByteBufLE(ByteBuf buf) {
        buf.writeShortLE(operationId);
        buf.writeByte(0x30); // + filler
        buf.writeByte(0x30); // + filler
        buf.writeIntLE(dialogueId);
        buf.writeIntLE(totalLength);// BODY_LEN_INDEX
        buf.writeIntLE(cmpLength);
        buf.writeByte(compressInd);
        buf.writeByte(compressType);
        buf.writeByte(0); // + filler
        buf.writeByte(0); // + filler
        buf.writeIntLE(hdrType);
        buf.writeIntLE(signature);
        buf.writeIntLE(version);
        buf.writeByte(platform);
        buf.writeByte(transport);
        buf.writeByte(swap);
        buf.writeByte(0); // + filler
        buf.writeShortLE(error);
        buf.writeShortLE(errorDetail);
    }

    /**
     * extract From {@link io.netty.buffer.ByteBuf} for Netty, Big Endian.
     * 
     * @param buf
     */
    public static Header extractFromByteBuf(ByteBuf buf) {
        short operationId = buf.readShort();
        short compatibilityVersion = buf.readShort();
        int dialogueId = buf.readInt();
        int totalLength = buf.readInt();
        int cmpLength = buf.readInt();
        byte compressInd = buf.readByte();
        byte compressType = buf.readByte();
        buf.readByte(); // + filler
        buf.readByte(); // + filler
        int hdrType = buf.readInt();
        int signature = buf.readInt();
        int version = buf.readInt();
        byte platform = buf.readByte();
        byte transport = buf.readByte();
        byte swap = buf.readByte();
        buf.readByte(); // + filler
        short error = buf.readShort();
        short errorDetail = buf.readShort();

        return new Header(operationId, compatibilityVersion, dialogueId, totalLength, cmpLength, compressInd,
                compressType, hdrType, signature, version, platform, transport, swap, error,
                errorDetail);
    }

    public void setSwap(byte swap) {
        this.swap = swap;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setTotalLength(int totalLength) {
        this.totalLength = totalLength;
    }

}