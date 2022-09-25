/**
 * * @@@ START COPYRIGHT @@@
 * *
 * * Licensed to the Apache Software Foundation (ASF) under one
 * * or more contributor license agreements.  See the NOTICE file
 * * distributed with this work for additional information
 * * regarding copyright ownership.  The ASF licenses this file
 * * to you under the Apache License, Version 2.0 (the
 * * "License"); you may not use this file except in compliance
 * * with the License.  You may obtain a copy of the License at
 * *
 * *   http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing,
 * * software distributed under the License is distributed on an
 * * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * * KIND, either express or implied.  See the License for the
 * * specific language governing permissions and limitations
 * * under the License.
 * *
 * * @@@ END COPYRIGHT @@@
 * **/

package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.codec.binary.Hex;

public class ByteArrayKey implements Comparable<ByteArrayKey>{
   byte[] bytes;

   public ByteArrayKey(byte[] bytes) {
      super();
      this.bytes = bytes;
   }

   @Override
   public int compareTo(ByteArrayKey that) {
      return Bytes.compareTo(
      this.bytes, that.bytes);
   }

   @Override
   public int hashCode() {
      return Bytes.hashCode(bytes);
   }

   @Override
   public boolean equals(Object obj) {
      if (obj instanceof ByteArrayKey) {
         return Bytes.equals(this.bytes,((ByteArrayKey)obj).bytes);
      }
      return false;
   }

   public String toHexString() {
     return Hex.encodeHexString(bytes);
   }

   public byte[] getBytesArray() {
     return bytes;
   }
}

