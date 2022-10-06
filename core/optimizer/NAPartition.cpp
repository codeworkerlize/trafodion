
/* -*-C++-*-
******************************************************************************
*
* File:         NAPartition.C
* Description:  Methods on the Partition class
* Created:      3/4/2021
* Language:     C++
*
*
*
*
******************************************************************************
*/

#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's

#include "NAPartition.h"

#include "common/Platform.h"
#include "executor/ex_error.h"
#include "optimizer/NATable.h"
#include "optimizer/Sqlcomp.h"
#include "parser/SqlParserGlobals.h"
#include "sqlcat/TrafDDLdesc.h"
#include "sqlcomp/CmpSeabaseDDLmd.h"
#include "sqlcomp/parser.h"

void NAPartition::display() { print(); }

void NAPartition::print(FILE *ofd, const char *indent, const char *title, CollHeap *c, char *buf) {
  Space *space = (Space *)c;
  char mybuf[3000];
  const char *head = isSubparition_ ? "Subpartition" : "Partition";

  NAString lowValuetext = "", highValueText = "";
  boundaryValueToString(lowValuetext, highValueText);

  snprintf(mybuf, sizeof(mybuf),
           "%sName : %s, %sBaseTableName : %s, %sPos : %d, "
           "Subpartition Count : %d, Low Boundary Value Text : %s, "
           "High Boundary Value Text : %s, MaxValue Bitmap : %s\n",
           head, partitionName_, head, partitionEntityName_, head, partPosition_, subpartitionCnt_, lowValuetext.data(),
           highValueText.data(), maxValueBitSetToString());

  PRINTIT(ofd, c, space, buf, mybuf);
  if (subPartitions_) subPartitions_->print(ofd, indent, title, c, buf);
}

void NAPartition::boundaryValueToString(NAString &lowStr, NAString &highStr) {
  for (int i = 0; i < boundaryValueList_.entries(); i++) {
    if (i == 0) {
      if (boundaryValueList_[i]->lowValue != NULL) lowStr += boundaryValueList_[i]->lowValue;

      highStr += boundaryValueList_[i]->highValue;
    } else {
      if (boundaryValueList_[i]->lowValue != NULL) {
        lowStr += ",";
        lowStr += boundaryValueList_[i]->lowValue;
      }
      highStr += ",";
      highStr += boundaryValueList_[i]->highValue;
    }
  }
}

void NAPartition::deepDelete() {
  if (partitionName_) NADELETEBASIC(partitionName_, heap_);

  if (partitionEntityName_) NADELETEBASIC(partitionEntityName_, heap_);

  for (int i = 0; i < boundaryValueList_.entries(); i++) {
    if (boundaryValueList_[i]) {
      if (boundaryValueList_[i]->lowValue) NADELETEBASIC(boundaryValueList_[i]->lowValue, heap_);
      if (boundaryValueList_[i]->highValue) NADELETEBASIC(boundaryValueList_[i]->highValue, heap_);
      delete boundaryValueList_[i];
    }
  }

  if (subPartitions_) {
    subPartitions_->deepDelete();
    delete subPartitions_;
  }
}

void NAPartitionArray::print(FILE *ofd, const char *indent, const char *title, CollHeap *c, char *buf) {
  for (int i = 0; i < entries(); i++) {
    at(i)->print(ofd, indent, title, c, buf);
  }
}

void NAPartitionArray::deepDelete() {
  for (int i = 0; i < this->entries(); i++) {
    (*this)[i]->deepDelete();
    delete (*this)[i];
  }
}
