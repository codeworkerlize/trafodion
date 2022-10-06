
#ifndef NAPARTITION_H
#define NAPARTITION_H
/* -*-C++-*-
******************************************************************************
*
* File:         A partition
* Description:  Partition class declarations
* Created:      3/4/2021
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include <bitset>
#include <string>
#include "common/ComSmallDefs.h"
#include "common/BaseTypes.h"
#include "optimizer/ObjectNames.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class BoundaryValue;
class NAPartition;
class NAPartitionArray;

// -----------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------
class TrafPartitionV2Desc;
class TrafPartDesc;

typedef std::bitset<256> PartitionMaxvalueBitMap;

class BoundaryValue : public NABasicObject {
 public:
  BoundaryValue() : isMaxVal_(FALSE) {
    lowValue = NULL;
    highValue == NULL;
  }
  NABoolean isMaxValue() { return strcmp(highValue, "MAXVALUE") == 0; }

  NABoolean isLowMaxValue() { return strcmp(lowValue, "MAXVALUE") == 0; }
  char *lowValue;  // lowValue is only used for the first
                   // partition column of Range partition
  char *highValue;
  NABoolean isMaxVal_;
};

class NAPartition : public NABasicObject {
 public:
  NAPartition(NAMemory *h, long parentUid, long partitionUid, char *partName, char *entityName,
              NABoolean isSubPartition, NABoolean hasSubPartition, int partPosition, NABoolean isValid,
              NABoolean isReadonly, NABoolean isInMemory, long defTime, long flags, int subpartitionCnt)
      : heap_(h),
        parentUid_(parentUid),
        partitionUid_(partitionUid),
        partitionName_(partName),
        partitionEntityName_(entityName),
        isSubparition_(isSubPartition),
        hasSubPartition_(hasSubPartition),
        partPosition_(partPosition),
        subpartitionCnt_(subpartitionCnt),
        boundaryValueList_(h),
        isValid_(isValid),
        isReadonly_(isReadonly),
        isInMemory_(isInMemory),
        defTime_(defTime),
        flags_(flags),
        subPartitions_(NULL) {}

  // descontruct
  virtual ~NAPartition(){};
  virtual void deepDelete();

  // invoked at the end of a statement by NATable::resetAfterStatement().
  void resetAfterStatement(){};

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------
  const NAPartitionArray *getSubPartitions() { return subPartitions_; };
  NAPartitionArray **getSubPartitionsPtr() { return &subPartitions_; };

  LIST(BoundaryValue *) & getBoundaryValueList() { return boundaryValueList_; }

  void maxValueBitSet(size_t pos) { maxValMap_.set(pos); }

  NABoolean isMaxValue(size_t pos) { return maxValMap_.test(pos); }

  const char *maxValueBitSetToString() { return maxValMap_.to_string().c_str(); }

  void boundaryValueToString(NAString &lowValuetext, NAString &highValueText);
  // ---------------------------------------------------------------------
  // Display function for debugging
  // ---------------------------------------------------------------------
  void print(FILE *ofd = stdout, const char *indent = DEFAULT_INDENT, const char *title = "NAPartition",
             CollHeap *c = NULL, char *buf = NULL);

  void display();

  const char *getPartitionName() const { return partitionName_; }
  const char *getPartitionEntityName() const { return partitionEntityName_; }
  const int getPartPosition() const { return partPosition_; }
  const int getSubPartitionCount() const { return subpartitionCnt_; }
  long getParentUID() const { return parentUid_; }
  long getPartitionUID() const { return partitionUid_; }

  NABoolean hasSubPartition() { return hasSubPartition_; }

 private:
  NAMemory *heap_;

  long parentUid_;
  long partitionUid_;
  char *partitionName_;
  char *partitionEntityName_;
  NABoolean isSubparition_;
  NABoolean hasSubPartition_;
  int partPosition_;
  int subpartitionCnt_;
  LIST(BoundaryValue *) boundaryValueList_;
  PartitionMaxvalueBitMap maxValMap_;

  // reserved
  NABoolean isValid_;
  NABoolean isReadonly_;
  NABoolean isInMemory_;
  long defTime_;
  long flags_;

  NAPartitionArray *subPartitions_;
};  // class NAPartition

// ***********************************************************************
// An array of Partition pointers
// ***********************************************************************

class NAPartitionArray : public LIST(NAPartition *) {
 public:
  NAPartitionArray(CollHeap *h = CmpCommon::statementHeap()) : LIST(NAPartition *)(h) {}

  virtual ~NAPartitionArray(){};
  virtual void deepDelete();

  virtual void print(FILE *ofd = stdout, const char *indent = DEFAULT_INDENT, const char *title = "NAPartitionArray",
                     CollHeap *c = NULL, char *buf = NULL);

  void display() { print(); }

};  // class NAPartitionArray

#endif /* NAPARTITION_H */
