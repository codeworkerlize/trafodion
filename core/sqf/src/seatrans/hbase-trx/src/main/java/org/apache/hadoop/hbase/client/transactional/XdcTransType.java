/**
* @@@ START COPYRIGHT @@@
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
**/


package org.apache.hadoop.hbase.client.transactional;


 // xDC Transaction types
public enum XdcTransType {
   XDC_TYPE_NONE(0),     // xDC not configured
   XDC_TYPE_XDC_UP(1),   // xDC configured and up
   XDC_TYPE_XDC_DOWN(2), // xDC configured and remote is down
   XDC_TYPE_LAST(2),
   XDC_TYPE_BAD(-1);

   private Integer value;

   private XdcTransType(int value) { this.value = value; }
   private XdcTransType(short value) { this.value = new Integer(value); }
   public short getShort() { return value.shortValue(); }
   public int getValue() { return value; }
   public String toString() {
      return super.toString();
   }
}
