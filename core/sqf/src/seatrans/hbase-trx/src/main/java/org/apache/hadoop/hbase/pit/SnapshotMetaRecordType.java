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


package org.apache.hadoop.hbase.pit;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * @author null,changed by hjw
 */
public enum SnapshotMetaRecordType {
    SNAPSHOT_RECORD(0),
    SNAPSHOT_START_RECORD(1),
    SNAPSHOT_INCREMENTAL_RECORD(2),
    SNAPSHOT_LOCK_RECORD(3),
    SNAPSHOT_MUTATION_FLUSH_RECORD(4),
    SNAPSHOT_END_RECORD(5),
    SNAPSHOT_TAG_INDEX_RECORD(6),
    SNAPSHOT_TABLE_INDEX_RECORD(7),
    NAPSHOT_LAST(8);

	static final int version = 100;
    private Integer value;

    private static final Map<Integer, SnapshotMetaRecordType> SNAPSHOT_META_TYPE_MAP =
            new ImmutableMap.Builder<Integer, SnapshotMetaRecordType>()
                    .put(0, SNAPSHOT_RECORD)
                    .put(1, SNAPSHOT_START_RECORD)
                    .put(2, SNAPSHOT_INCREMENTAL_RECORD)
                    .put(3, SNAPSHOT_LOCK_RECORD)
                    .put(4, SNAPSHOT_MUTATION_FLUSH_RECORD)
                    .put(5, SNAPSHOT_END_RECORD)
                    .put(6, SNAPSHOT_TAG_INDEX_RECORD)
                    .put(7, SNAPSHOT_TABLE_INDEX_RECORD)
                    .put(8, NAPSHOT_LAST)
                    .build();
    public static Map<Integer, SnapshotMetaRecordType> getSnapshotMetaTypeMap() {
        return SNAPSHOT_META_TYPE_MAP;
    }
    private SnapshotMetaRecordType(int value) { this.value = value; }
    private SnapshotMetaRecordType(short value) { this.value = new Integer(value); }
    public short getShort() { return value.shortValue(); }
    public int getValue() { return value; }
    public static int getVersion() { return version; }
    public String toString() {
      return super.toString();
    }
}
