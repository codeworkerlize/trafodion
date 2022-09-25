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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import io.netty.buffer.ByteBuf;
import org.trafodion.dcs.script.ScriptContext;
import org.trafodion.dcs.script.ScriptManager;
import org.trafodion.dcs.util.GetJavaProperty;

public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    Util() {}

    static void checkLength(int len) throws IOException {
        // if the length is over 10MB, that means some errors happens
        if (len > 10485760) {
            throw new IOException("Trying to allocate a pice of memory over 10MB. Len=" + len);
        }
    }

    public static String extractString(ByteBuffer buf) throws IOException {
        int len = buf.getInt();
        checkLength(len);
        byte[] str = null;
        if (len > 0) {
            str = new byte[len - 1];;
            buf.get(str, 0, len - 1);
            buf.get(); // trailing null
        } else
            str = new byte[0];
        return new String(str, StandardCharsets.UTF_8);
    }

    static byte[] extractByteString(ByteBuffer buf) throws IOException {
        int len = buf.getInt();
        checkLength(len);
        byte[] str = null;
        if (len > 0) {
            str = new byte[len];
            buf.get(str, 0, len);
            buf.get(); // trailing null
        } else
            str = new byte[0];
        return str;
    }

    public static byte[] extractByteArray(ByteBuffer buf) throws IOException {
        int len = buf.getInt();
        checkLength(len);
        if (len > 0) {
            byte[] a = new byte[len];
            buf.get(a, 0, len);
            return a;
        } else
            return new byte[0];
    }

    public static void insertString(String str, ByteBuffer buf)
            throws java.io.UnsupportedEncodingException {
        if (str != null && str.length() > 0) {
            buf.putInt(str.length() + 1);
            try {
                if (str.length() > buf.remaining()) {
                    throw new UnsupportedEncodingException(
                            "The length of the string exceeds the remaining size of the byte array buffer,string length:"
                                    + str.length() + ",buf remaining:" + buf.remaining());
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            buf.put(str.getBytes(StandardCharsets.UTF_8), 0, str.length());
            buf.put((byte) 0);
        } else { // buffer is null or length 0
            buf.putInt(0);
        }
    }

    public static void insertByteString(byte[] array, ByteBuffer buf) {
        if (array != null && array.length > 0) {
            buf.putInt(array.length);
            buf.put(array, 0, array.length);
            buf.put((byte) 0);
        } else { // buffer is null or length 0
            buf.putInt(0);
        }
    }

    static void insertByteArray(byte[] array, ByteBuffer buf) {
        if (array != null && array.length > 0) {
            buf.putInt(array.length);
            buf.put(array, 0, array.length);
        } else
            buf.putInt(0);
    }

    public static String extractString(ByteBuf buf) throws UnsupportedEncodingException {
        byte[] str = extractByte(buf);
        return new String(str, StandardCharsets.UTF_8);
    }

    public static String extractStringLE(ByteBuf buf) throws UnsupportedEncodingException {
        byte[] str = extractByteLE(buf);
        return new String(str, StandardCharsets.UTF_8);
    }

    public static byte[] extractByte(ByteBuf buf) {
        int len = buf.readInt();
        byte[] str = null;
        if (len > 0) {
            str = new byte[len - 1];;
            buf.readBytes(str, 0, len - 1);
            buf.readByte(); // trailing null
        } else
            str = new byte[0];
        return str;
    }

    public static byte[] extractByteLE(ByteBuf buf) {
        int len = buf.readIntLE();
        byte[] str = null;
        if (len > 0) {
            str = new byte[len - 1];;
            buf.readBytes(str, 0, len - 1);
            buf.readByte(); // trailing null
        } else
            str = new byte[0];
        return str;
    }

    public static byte[] extractByteArray(ByteBuf buf) {
        int len = buf.readInt();
        if (len > 0) {
            byte[] a = new byte[len];
            buf.readBytes(a, 0, len);
            return a;
        } else
            return new byte[0];
    }

    public static byte[] extractByteArrayLE(ByteBuf buf) {
        int len = buf.readIntLE();
        if (len > 0) {
            byte[] a = new byte[len];
            buf.readBytes(a, 0, len);
            return a;
        } else
            return new byte[0];
    }

    public static void insertString(String str, ByteBuf buf)
            throws java.io.UnsupportedEncodingException {
        if (str != null && str.length() > 0) {
            buf.writeInt(str.length() + 1);
            buf.writeBytes(str.getBytes(StandardCharsets.UTF_8), 0, str.length());
            buf.writeByte((byte) 0);
        } else { // buffer is null or length 0
            buf.writeInt(0);
        }
    }

    public static void insertStringLE(String str, ByteBuf buf)
            throws java.io.UnsupportedEncodingException {
        if (str != null && str.length() > 0) {
            buf.writeIntLE(str.length() + 1);
            buf.writeBytes(str.getBytes(StandardCharsets.UTF_8), 0, str.length());
            buf.writeByte((byte) 0);
        } else { // buffer is null or length 0
            buf.writeIntLE(0);
        }
    }

    public static void insertByteString(byte[] array, ByteBuf buf) {
        if (array != null && array.length > 0) {
            buf.writeInt(array.length);
            buf.writeBytes(array, 0, array.length);
            buf.writeByte((byte) 0);
        } else { // buffer is null or length 0
            buf.writeInt(0);
        }
    }

    public static void insertByteStringLE(byte[] array, ByteBuf buf) {
        if (array != null && array.length > 0) {
            buf.writeIntLE(array.length);
            buf.writeBytes(array, 0, array.length);
            buf.writeByte((byte) 0);
        } else { // buffer is null or length 0
            buf.writeIntLE(0);
        }
    }

    public static void insertByteArray(byte[] array, ByteBuf buf) {
        if (array != null && array.length > 0) {
            buf.writeInt(array.length);
            buf.writeBytes(array, 0, array.length);
        } else
            buf.writeInt(0);
    }

    public static void insertByteArrayLE(byte[] array, ByteBuf buf) {
        if (array != null && array.length > 0) {
            buf.writeIntLE(array.length);
            buf.writeBytes(array, 0, array.length);
        } else
            buf.writeIntLE(0);
    }

    /**
     * Increase ByteBuffer's capacity.
     * 
     * @param buffer the ByteBuffer want to increase capacity
     * @param size increased size
     * @return increased capacity ByteBuffer
     * @throws IllegalArgumentException if size less than 0 or buffer is null
     */
    public static ByteBuffer increaseCapacity(ByteBuffer buffer, int size)
            throws IllegalArgumentException {
        if (buffer == null)
            throw new IllegalArgumentException("buffer is null");
        if (size < 0)
            throw new IllegalArgumentException("size less than 0");

        ByteOrder buffOrder = buffer.order();
        int capacity = buffer.capacity() + size;
        ByteBuffer result = buffer.isDirect() ? ByteBuffer.allocateDirect(capacity)
                : ByteBuffer.allocate(capacity);
        result.order(buffOrder);
        buffer.flip();
        result.put(buffer);
        return result;
    }

    /**
     * Gather ByteBuffers to one ByteBuffer.
     * 
     * @param buffers ByteBuffers
     * @return the gather ByteBuffer
     */
    public static ByteBuffer gather(ByteBuffer[] buffers) {
        int remaining = 0;
        for (int i = 0; i < buffers.length; i++) {
            if (buffers[i] != null)
                remaining += buffers[i].remaining();
        }
        ByteBuffer result = ByteBuffer.allocate(remaining);
        for (int i = 0; i < buffers.length; i++) {
            if (buffers[i] != null)
                result.put(buffers[i]);
        }
        result.flip();
        return result;
    }

    /**
     * Judge ByteBuffers have remaining bytes.
     * 
     * @param buffers ByteBuffers
     * @return have remaining
     */
    public static boolean hasRemaining(ByteBuffer[] buffers) {
        if (buffers == null)
            return false;
        for (int i = 0; i < buffers.length; i++) {
            if (buffers[i] != null && buffers[i].hasRemaining())
                return true;
        }
        return false;
    }

    /**
     * Returns the index within this buffer of the first occurrence of the specified pattern buffer.
     * 
     * @param buffer the buffer
     * @param pattern the pattern buffer
     * @return the position within the buffer of the first occurrence of the pattern buffer
     */
    public static int indexOf(ByteBuffer buffer, ByteBuffer pattern) {
        int patternPos = pattern.position();
        int patternLen = pattern.remaining();
        int lastIndex = buffer.limit() - patternLen + 1;

        Label: for (int i = buffer.position(); i < lastIndex; i++) {
            for (int j = 0; j < patternLen; j++) {
                if (buffer.get(i + j) != pattern.get(patternPos + j))
                    continue Label;
            }
            return i;
        }
        return -1;
    }

    public static void toHexString(String header, ByteBuffer buf) {
        StringBuilder sb = new StringBuilder();
        int bufPosition = buf.position();
        int bufLimit = buf.limit();

        if (LOG.isDebugEnabled()) {
            LOG.debug("hex->{}: position,limit,capacity {},{},{}", header, buf.position(),
                    buf.limit(), buf.capacity());
        }
        for (int index = buf.position(); index < buf.limit(); index++) {
            String hex = Integer.toHexString(0x0100 + (buf.get(index) & 0x00FF)).substring(1);
            sb.append(hex.length() < 2 ? "0" : "").append(hex).append(" ");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("hex->{}", sb.toString());
        }
        buf.position(bufPosition);
        buf.limit(bufLimit);
    }

    public static void toHexString(String header, ByteBuffer buf, int length) {
        StringBuilder sb = new StringBuilder();
        int bufPosition = buf.position();
        int bufLimit = buf.limit();
        int len = bufPosition + length;
        len = Math.min(len, bufLimit);

        if (LOG.isDebugEnabled()) {
            LOG.debug("hex->{}: position,limit,capacity {},{},{}", header, buf.position(),
                    buf.limit(), buf.capacity());
        }
        for (int index = buf.position(); index < len; index++) {
            String hex = Integer.toHexString(0x0100 + (buf.get(index) & 0x00FF)).substring(1);
            sb.append(hex.length() < 2 ? "0" : "").append(hex).append(" ");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("hex->{}", sb.toString());
        }
        buf.position(bufPosition);
        buf.limit(bufLimit);
    }

    public static String isEmptyEqualsNull(){
        String result = "";
        ScriptContext scriptContext = new ScriptContext();
        scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
        scriptContext.setStripStdOut(true);
        scriptContext.setStripStdErr(true);

        String dcsHome = GetJavaProperty.getDcsHome();
        scriptContext.setCommand("cd "+ dcsHome
                + " ;bin/scripts/isEmptyEqualsNull.sh");
        try {
            ScriptManager.getInstance().runScript(scriptContext);// Blocking call

            if(scriptContext.getExitCode() != 0) {
                String stdOut = scriptContext.getStdOut().toString();
                int start = stdOut.indexOf("ERROR");
                int end = stdOut.indexOf("---");

                String errorText = stdOut.substring(start, end)
                        + "; exitcode:"
                        + scriptContext.getExitCode();

                LOG.warn("showenv failed with error : {}", errorText);
                //default is EMPTY STRING EQUIVALENT NULL : ENABLE
                result = "ENABLE";
            }else{
                result = scriptContext.getStdOut().toString();
                result = result.substring(result.indexOf(":") + 1, result.indexOf("---")).trim();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            //default is EMPTY STRING EQUIVALENT NULL : ENABLE
            result = "ENABLE";
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("isEmptyEqualsNull->{}", result);
        }
        return result;
    }
}
