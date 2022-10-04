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
/* -*-C++-*- */

#include "optimizer/OptHints.h"
#include "common/DatetimeType.h"
#include "arkcmp/CmpContext.h"
#include "optimizer/ControlDB.h"

Hint::Hint(const NAString &indexName, NAMemory *h)
    : indexes_(h, 1), selectivity_(-1.0), cardinality_(-1.0), pCqdsInHint_(0) {
  indexes_.insert(indexName);
}

Hint::Hint(double c, double s, NAMemory *h) : indexes_(h, 0), selectivity_(s), cardinality_(c), pCqdsInHint_(0) {}

Hint::Hint(double s, NAMemory *h) : indexes_(h, 0), selectivity_(s), cardinality_(-1.0), pCqdsInHint_(0) {}

Hint::Hint(NAMemory *h) : indexes_(h, 0), selectivity_(-1.0), cardinality_(-1.0), pCqdsInHint_(0) {}

Hint::Hint(const Hint &hint, NAMemory *h)
    : indexes_(hint.indexes_, h), selectivity_(hint.selectivity_), cardinality_(hint.cardinality_), pCqdsInHint_(0) {}

NABoolean Hint::hasIndexHint(const NAString &xName) { return indexes_.contains(xName); }

Hint *Hint::addIndexHint(const NAString &indexName) {
  indexes_.insert(indexName);
  return this;
}

void Hint::setCqdsInHint(NAHashDictionary<NAString, NAString> *p) { pCqdsInHint_ = p; }

long OptHbaseAccessOptions::computeHbaseTS(const char *tsStr) {
  UInt32 fracPrec;

  NAString epochStr("1970-01-01:00:00:00");
  DatetimeValue epochDT(epochStr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec,
                        (CmpCommon::getDefault(USE_OLD_DT_CONSTRUCTOR) == DF_ON));
  long epochJTS = DatetimeType::julianTimestampValue((char *)epochDT.getValue(), epochDT.getValueLen(), fracPrec);

  long jts = 0;
  if (tsStr) {
    DatetimeValue dtVal(tsStr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec,
                        (CmpCommon::getDefault(USE_OLD_DT_CONSTRUCTOR) == DF_ON));
    if (!dtVal.isValid()) {
      return -1;
    }

    jts = DatetimeType::julianTimestampValue((char *)dtVal.getValue(), dtVal.getValueLen(), fracPrec);
    if (jts == 0) {
      return -1;
    }

    if (jts < epochJTS) {
      return -1;
    }

    if (CmpCommon::context()->gmtDiff() != 0) jts += (long)(CmpCommon::context()->gmtDiff()) * 60 * 1000000;
  } else
    jts = epochJTS;

  jts = (jts - epochJTS) / 1000;

  return jts;
}

short OptHbaseAccessOptions::setHbaseTS(const char *minTSstr, const char *maxTSstr) {
  UInt32 fracPrec;
  long minJTS = -1;
  long maxJTS = -1;

  NAString epochStr("1970-01-01:00:00:00");
  DatetimeValue epochDT(epochStr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec,
                        (CmpCommon::getDefault(USE_OLD_DT_CONSTRUCTOR) == DF_ON));
  long epochJTS = DatetimeType::julianTimestampValue((char *)epochDT.getValue(), epochDT.getValueLen(), fracPrec);

  NAString highestStr("9999-12-31:00:00:00");
  DatetimeValue highestDT(highestStr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec,
                          (CmpCommon::getDefault(USE_OLD_DT_CONSTRUCTOR) == DF_ON));
  long highestJTS =
      DatetimeType::julianTimestampValue((char *)highestDT.getValue(), highestDT.getValueLen(), fracPrec);

  minJTS = computeHbaseTS(minTSstr);
  if (minJTS < 0) {
    isValid_ = FALSE;
    return -1;
  }

  maxJTS = computeHbaseTS(maxTSstr);
  if (maxJTS < 0) {
    isValid_ = FALSE;
    return -1;
  }

  setHbaseMinTS(minJTS);
  setHbaseMaxTS(maxJTS);

  return 0;
}

OptHbaseAccessOptions::OptHbaseAccessOptions(int v, NAMemory *h) : HbaseAccessOptions(), isValid_(TRUE) {
  setNumVersions(v);
}

OptHbaseAccessOptions::OptHbaseAccessOptions(const char *hbase_auths) : HbaseAccessOptions(), isValid_(TRUE) {
  hbaseAuths_ = hbase_auths;
}

OptHbaseAccessOptions::OptHbaseAccessOptions(const char *minTSstr, const char *maxTSstr)
    : HbaseAccessOptions(), isValid_(TRUE) {
  if (setHbaseTS(minTSstr, maxTSstr)) return;

  return;
}

const NAString *OptHbaseAccessOptions::getControlTableValue(const QualifiedName &tableName, const char *ct) {
  const NAString *haStrNAS = ActiveControlDB()->getControlTableValue(tableName.getQualifiedNameAsString(), ct);
  if ((haStrNAS && (NOT haStrNAS->isNull())) && (tableName.isSeabase()) && (NOT tableName.isSeabaseMD()) &&
      (NOT tableName.isSeabasePrivMgrMD()) && (NOT tableName.isSeabaseReservedSchema())) {
    return haStrNAS;
  }

  return NULL;
}

short OptHbaseAccessOptions::setVersionsFromDef(QualifiedName &tableName) {
  const NAString *haStrNAS = getControlTableValue(tableName, "HBASE_VERSIONS");
  if (haStrNAS) {
    long n = atoi(haStrNAS->data());
    if ((n == -1) || (n == -2) || (n > 1))
      setNumVersions(n);
    else {
      isValid_ = FALSE;
      return -1;
    }
  }

  return 0;
}

short OptHbaseAccessOptions::setHbaseTsFromDef(QualifiedName &tableName) {
  const NAString *haStrNAS = getControlTableValue(tableName, "HBASE_TIMESTAMP_AS_OF");
  if (haStrNAS) {
    if (setHbaseTS(NULL, haStrNAS->data())) {
      isValid_ = FALSE;
      return -1;
    }
  }

  return 0;
}

short OptHbaseAccessOptions::setHbaseAuthsFromDef(QualifiedName &tableName) {
  const NAString *haStrNAS = getControlTableValue(tableName, "HBASE_AUTHS");
  if (haStrNAS) {
    hbaseAuths_ = *haStrNAS;
  }

  return 0;
}

short OptHbaseAccessOptions::setOptionsFromDefs(QualifiedName &tableName) {
  isValid_ = FALSE;
  if (NOT versionSpecified()) {
    if (setVersionsFromDef(tableName)) return -1;
  }

  if (NOT tsSpecified()) {
    if (setHbaseTsFromDef(tableName)) return -1;
  }

  if (NOT authsSpecified()) {
    if (setHbaseAuthsFromDef(tableName)) return -1;
  }

  isValid_ = TRUE;

  return 0;
}

OptHbaseAccessOptions::OptHbaseAccessOptions() : HbaseAccessOptions(), isValid_(TRUE) {}
