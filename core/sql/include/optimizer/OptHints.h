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
#ifndef OPTHINTS_H
#define OPTHINTS_H
/* -*-C++-*- */

#include "common/CmpCommon.h"
#include "export/NAStringDef.h"
#include "exp/ExpHbaseDefs.h"

// -----------------------------------------------------------------------
// forward declarations
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class Hint;

// -----------------------------------------------------------------------
// Optimizer Hint
// -----------------------------------------------------------------------
class Hint : public NABasicObject {
 public:
  // constructors
  Hint(const NAString &indexName, NAMemory *h = HEAP);
  Hint(double c, double s, NAMemory *h = HEAP);
  Hint(double s, NAMemory *h = HEAP);
  Hint(NAMemory *h = HEAP);

  // copy ctor
  Hint(const Hint &hint, NAMemory *h);

  // virtual destructor
  virtual ~Hint() {
    if (pCqdsInHint_) {
      pCqdsInHint_->clear(TRUE);
      delete pCqdsInHint_;
    }
    pCqdsInHint_ = 0;
  }

  // mutators
  Hint *addIndexHint(const NAString &indexName);

  Hint *setCardinality(double c) {
    cardinality_ = c;
    return this;
  }

  Hint *setSelectivity(double s) {
    selectivity_ = s;
    return this;
  }

  void replaceIndexHint(CollIndex x, const NAString &newIndexName) { indexes_[x] = newIndexName; }

  // accessors
  NABoolean hasIndexHint(const NAString &xName);
  CollIndex indexCnt() const { return indexes_.entries(); }
  const NAString &operator[](CollIndex x) const { return indexes_[x]; }

  double getCardinality() const { return cardinality_; }
  NABoolean hasCardinality() const { return cardinality_ != -1.0; }

  double getSelectivity() const { return selectivity_; }
  NABoolean hasSelectivity() const { return selectivity_ != -1.0; }

  void setCqdsInHint(NAHashDictionary<NAString, NAString> *p);

  NAHashDictionary<NAString, NAString> *cqdsInHint() { return pCqdsInHint_; }

 protected:
  LIST(NAString) indexes_;  // ordered list of index hints
  double selectivity_;      // table's hinted selectivity or -1
  double cardinality_;      // table's hinted cardinality or -1
  NAHashDictionary<NAString, NAString> *pCqdsInHint_;
};  // Hint

class OptHbaseAccessOptions : public HbaseAccessOptions {
 public:
  OptHbaseAccessOptions(int v, NAMemory *h = HEAP);

  OptHbaseAccessOptions(const char *minTSstr, const char *maxTSstr);

  OptHbaseAccessOptions(const char *auths);

  OptHbaseAccessOptions();

  NAString &hbaseAuths() { return hbaseAuths_; }
  const NAString &hbaseAuths() const { return hbaseAuths_; }

  NABoolean authsSpecified() { return (NOT hbaseAuths_.isNull()); }
  NABoolean isValid() { return isValid_; }

  static long computeHbaseTS(const char *tsStr);

  short setOptionsFromDefs(QualifiedName &tableName);

  static const NAString *getControlTableValue(const QualifiedName &tableName, const char *ct);

 private:
  short setVersionsFromDef(QualifiedName &tableName);
  short setHbaseTsFromDef(QualifiedName &tableName);
  short setHbaseAuthsFromDef(QualifiedName &tableName);

  short setHbaseTS(const char *minTSstr, const char *maxTSstr);

  NABoolean isValid_;

  NAString hbaseAuths_;
};

// -----------------------------------------------------------------------

#endif /* OPTHINTS_H */
