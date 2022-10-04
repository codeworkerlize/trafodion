/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
#ifndef EXP_HBASE_DEFS_H
#define EXP_HBASE_DEFS_H

#include "common/BaseTypes.h"
#include "export/NABasicObject.h"

#define HBASE_ACCESS_SUCCESS        0
#define HBASE_ACCESS_PREEMPT        1
#define HBASE_ACCESS_EOD            100
#define HBASE_ACCESS_EOR            101
#define HBASE_ACCESS_NO_ROW         102
#define HBASE_NOT_IMPLEMENTED       103
#define TRIGGER_EXECUTE_EXCEPTION   104
#define TRIGGER_PARAMETER_EXCEPTION 105

typedef struct {
  //  char val[1000];
  char *val;
  Int32 len;
} HbaseStr;

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
// this enum MUST be kept in sync with the Java constants in HBaseClient.java
// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
enum HbaseOptionEnum {
  // name                          number   level where it is applied
  // --------------------------    ------   ------------------------------
  HBASE_NAME = 0,                      // column family
  HBASE_MAX_VERSIONS = 1,              //      "
  HBASE_MIN_VERSIONS = 2,              //      "
  HBASE_TTL = 3,                       //      "
  HBASE_BLOCKCACHE = 4,                //      "
  HBASE_IN_MEMORY = 5,                 //      "
  HBASE_COMPRESSION = 6,               //      "
  HBASE_BLOOMFILTER = 7,               //      "
  HBASE_BLOCKSIZE = 8,                 //      "
  HBASE_DATA_BLOCK_ENCODING = 9,       //      "
  HBASE_CACHE_BLOOMS_ON_WRITE = 10,    //      "
  HBASE_CACHE_DATA_ON_WRITE = 11,      //      "
  HBASE_CACHE_INDEXES_ON_WRITE = 12,   //      "
  HBASE_COMPACT_COMPRESSION = 13,      //      "
  HBASE_PREFIX_LENGTH_KEY = 14,        // KeyPrefixRegionSplitPolicy
  HBASE_EVICT_BLOCKS_ON_CLOSE = 15,    // column family
  HBASE_KEEP_DELETED_CELLS = 16,       //      "
  HBASE_REPLICATION_SCOPE = 17,        //      "
  HBASE_MAX_FILESIZE = 18,             // table
  HBASE_COMPACT = 19,                  //   "
  HBASE_DURABILITY = 20,               //   "
  HBASE_MEMSTORE_FLUSH_SIZE = 21,      //   "
  HBASE_SPLIT_POLICY = 22,             //   "
  HBASE_ENCRYPTION = 23,               //   "
  HBASE_CACHE_DATA_IN_L1 = 24,         // column family
  HBASE_PREFETCH_BLOCKS_ON_OPEN = 25,  //   "
  HBASE_HDFS_STORAGE_POLICY = 26,      //   "
  HBASE_REGION_REPLICATION = 27,
  HBASE_MAX_OPTIONS
};

class HbaseCreateOption : public NABasicObject {
 public:
  HbaseCreateOption(const NAText &key, const NAText &val) {
    key_ = key;
    val_ = val;
  }

  HbaseCreateOption(const char *key, const char *val) {
    key_ = key;
    val_ = val;
  }

  HbaseCreateOption(HbaseCreateOption &hbo) {
    key_ = hbo.key();
    val_ = hbo.val();
  }

  NAText &key() { return key_; }
  NAText &val() { return val_; }
  void setVal(NAText &val) { val_ = val; }

 private:
  NAText key_;
  NAText val_;
};

class HbaseAccessOptions : public NABasicObject {
 public:
  HbaseAccessOptions() : versions_(0), minTS_(-1), maxTS_(-1) {}

  Lng32 getNumVersions() { return versions_; }
  void setNumVersions(Lng32 v) { versions_ = v; }

  NABoolean multiVersions() { return (versions_ != 0); }

  NABoolean isMaxVersions() { return (versions_ == -1); }
  NABoolean isAllVersions() { return (versions_ == -2); }

  void setHbaseMinTS(Int64 minTS) { minTS_ = minTS; }
  void setHbaseMaxTS(Int64 maxTS) { maxTS_ = maxTS; }

  Int64 hbaseMinTS() { return minTS_; }
  Int64 hbaseMaxTS() { return maxTS_; }

  NABoolean versionSpecified() { return (versions_ != 0); }
  NABoolean tsSpecified() { return ((minTS_ != -1) || (maxTS_ != -1)); }

 private:
  // 0, version not specified, return default of 1.
  // -1, return max versions
  // -2, return all versions.
  // N, return N versions.
  Lng32 versions_;
  char filler_[4];

  // min/max values of timestamp range to be returned
  Int64 minTS_;
  Int64 maxTS_;
};

typedef NAList<HbaseStr> HBASE_NAMELIST;

typedef enum {
  HBASE_MIN_ERROR_NUM = 700,
  HBASE_OPER_OK = HBASE_MIN_ERROR_NUM,
  HBASE_CREATE_ERROR,
  HBASE_ALTER_ERROR,
  HBASE_DROP_ERROR,
  HBASE_OPEN_ERROR,
  HBASE_CLOSE_ERROR,
  HBASE_ACCESS_ERROR,
  HBASE_CREATE_ROW_ERROR,
  HBASE_DUP_ROW_ERROR,
  HBASE_ROW_NOTFOUND_ERROR,
  HBASE_CREATE_OPTIONS_ERROR,
  HBASE_COPY_ERROR,
  HBASE_CREATE_HFILE_ERROR,
  HBASE_ADD_TO_HFILE_ERROR,
  HBASE_CLOSE_HFILE_ERROR,
  HBASE_DOBULK_LOAD_ERROR,
  HBASE_CLEANUP_HFILE_ERROR,
  HBASE_INIT_HBLC_ERROR,
  HBASE_INIT_BRC_ERROR,
  HBASE_RETRY_AGAIN,
  HBASE_CREATE_SNAPSHOT_ERROR,
  HBASE_DELETE_SNAPSHOT_ERROR,
  HBASE_VERIFY_SNAPSHOT_ERROR,
  HBASE_RESTORE_SNAPSHOT_ERROR,
  HBASE_BACKUP_NONFATAL_ERROR,
  HBASE_BACKUP_LOCK_TIMEOUT_ERROR,
  HBASE_DELETE_BACKUP_ERROR,
  HBASE_EXPORT_IMPORT_BACKUP_ERROR,
  HBASE_REGISTER_TENANT_ERROR,
  HBASE_ALTER_TENANT_ERROR,
  HBASE_UNREGISTER_TENANT_ERROR,
  HBASE_GET_BACKUP_ERROR,
  HBASE_BACKUP_OPERATION_ERROR,
  HBASE_LOCK_TIME_OUT_ERROR,  // first error code for rowlock
  HBASE_LOCK_ROLLBACK_ERROR,
  HBASE_LOCK_REQUIRED_NOT_INT_TRANSACTION,
  HBASE_DEAD_LOCK_ERROR,
  HBASE_LOCK_REGION_MOVE_ERROR,
  HBASE_LOCK_REGION_SPLIT_ERROR,
  HBASE_CANCEL_OPERATION,
  HBASE_LOCK_NOT_ENOUGH_RESOURCE,  // last error code for rowlock
  HBASE_RPC_TIME_OUT_ERROR,
  HBASE_GENERIC_ERROR,
  HBASE_EXEC_TRIGGER_ERROR,
  HBASE_INVALID_DDL,
  HBASE_SHARED_CACHE_SPACE_EXHUAST,
  HBASE_MAX_ERROR_NUM  // keep this as the last element in enum list.

} HbaseError;

static const char *const hbaseErrorEnumStr[] = {
    "HBASE_ERROR_OK",
    "HBASE_CREATE_ERROR",
    "HBASE_ALTER_ERROR",
    "HBASE_DROP_ERROR",
    "HBASE_OPEN_ERROR",
    "HBASE_CLOSE_ERROR",
    "HBASE_ACCESS_ERROR",
    "HBASE_CREATE_ROW_ERROR",
    "HBASE_DUP_ROW_ERROR",
    "HBASE_ROW_NOTFOUND_ERROR",
    "HBASE_CREATE_OPTIONS_ERROR",
    "HBASE_COPY_ERROR",
    "HBASE_CREATE_HFILE_ERROR",
    "HBASE_ADD_TO_HFILE_ERROR",
    "HBASE_CLOSE_HFILE_ERROR",
    "HBASE_DOBULK_LOAD_ERROR",
    "HBASE_CLEANUP_HFILE_ERROR",
    "HBASE_INIT_HBLC_ERROR",
    "HBASE_INIT_BRC_ERROR",
    "HBASE_RETRY_AGAIN",
    "HBASE_CREATE_SNAPSHOT_ERROR",
    "HBASE_DELETE_SNAPSHOT_ERROR",
    "HBASE_VERIFY_SNAPSHOT_ERROR",
    "HBASE_RESTORE_SNAPSHOT_ERROR",
    "HBASE_BACKUP_NONFATAL_ERROR",
    "HBASE_BACKUP_LOCK_TIMEOUT_ERROR",
    "HBASE_DELETE_BACKUP_ERROR",
    "HBASE_EXPORT_IMPORT_BACKUP_ERROR",
    "HBASE_REGISTER_TENANT_ERROR",
    "HBASE_ALTER_TENANT_ERROR",
    "HBASE_UNREGISTER_TENANT_ERROR",
    "HBASE_GET_BACKUP_ERROR",
    "HBASE_BACKUP_OPERATION_ERROR",
    "HBASE_LOCK_TIME_OUT_ERROR",
    "HBASE_LOCK_ROLLBACK_ERROR",
    "HBASE_LOCK_REQUIRED_NOT_INT_TRANSACTION",
    "HBASE_DEAD_LOCK_ERROR",
    "HBASE_LOCK_REGION_MOVE_ERROR",
    "HBASE_LOCK_REGION_SPLIT_ERROR",
    "HBASE_LOCK_NOT_ENOUGH_RESOURCE",
    "HBASE_RPC_TIME_OUT_ERROR",
    "HBASE_CANCEL_OPERATION",
    "HBASE_GENERIC_ERROR",
    "HBASE_EXEC_TRIGGER_ERROR",
    "Invalid DDL detected at fetch time",
    "HBASE_SHARED_CACHE_SPACE_EXHUAST",
    "HBASE_MAX_ERROR_NUM"  // keep this as the last element in enum list.
};

#endif
