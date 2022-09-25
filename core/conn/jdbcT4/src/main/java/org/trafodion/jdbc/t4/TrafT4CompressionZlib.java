// @@@ START COPYRIGHT @@@
// //
// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.
// //
// // @@@ END COPYRIGHT @@@
package org.trafodion.jdbc.t4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class TrafT4CompressionZlib implements TrafT4Compression {
    @Override
    public void compress(char compType, LogicalByteArray buffer, int buffer_index, int length) throws IOException {
        int realLen = length - Header.sizeOf();
        byte[] data = new byte[realLen];
        System.arraycopy(buffer.getBuffer(), Header.sizeOf(), data, 0, realLen);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DeflaterOutputStream out = null;
        try {
            out = new DeflaterOutputStream(bout);
            out.write(data);
        } finally {
            if (out != null)
                out.close();
        }
        buffer.resize(Header.sizeOf() + bout.size());
        buffer.setLocation(Header.sizeOf());
        buffer.insertByteArray(bout.toByteArray(), bout.size());
    }

    @Override
    public void uncompress(LogicalByteArray buffer) throws IOException {
        byte[] data = new byte[buffer.getLength() - Header.sizeOf()];
        System.arraycopy(buffer.getBuffer(), Header.sizeOf(), data, 0, data.length);
        InflaterInputStream in = null;
        try {
            in = new InflaterInputStream(new ByteArrayInputStream(data));
            byte[] buff = new byte[102400];
            int len = -1;
            int bufferIndex = Header.sizeOf();
            buffer.setLocation(bufferIndex);
            while ((len = in.read(buff)) >= 0) {
                bufferIndex += len;
                if (bufferIndex > buffer.getTotalAllocated()) {
                    buffer.resize(bufferIndex << 1);
                }
                buffer.insertByteArray(buff, len);
                buffer.setLocation(bufferIndex);
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
