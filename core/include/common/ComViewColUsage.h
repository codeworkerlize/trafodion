

/*
 *****************************************************************************
 *
 * File:         ComViewColUsage.h
 * Description:  Defines the referenced object (table or view) column
 *               relationship to the corresponding view column.
 *
 * Created:      7/15/16
 * Language:     C++
 *
 *****************************************************************************
 */

#ifndef COMVIEWCOLUSAGE_H
#define COMVIEWCOLUSAGE_H

#include "common/ComSmallDefs.h"
#include "common/str.h"

class ComViewColUsage {
  long viewUID_;
  int viewColNumber_;
  long refdUID_;
  int refdColNumber_;
  ComObjectType refdObjectType_;

 public:
  ComViewColUsage()
      : viewUID_(0), viewColNumber_(-1), refdUID_(0), refdColNumber_(-1), refdObjectType_(COM_UNKNOWN_OBJECT) {}

  ComViewColUsage(long viewUID, int viewColNumber, long refdUID, int refdColNumber, ComObjectType refdObjectType)
      : viewUID_(viewUID),
        viewColNumber_(viewColNumber),
        refdUID_(refdUID),
        refdColNumber_(refdColNumber),
        refdObjectType_(refdObjectType) {}

  virtual ~ComViewColUsage(){};

  long getViewUID() { return viewUID_; }
  void setViewUID(long viewUID) { viewUID_ = viewUID; }

  int getViewColNumber() { return viewColNumber_; }
  void setViewColNumber(int viewColNumber) { viewColNumber_ = viewColNumber; }

  long getRefdUID() { return refdUID_; }
  void setRefdUID(long refdUID) { refdUID_ = refdUID; }

  int getRefdColNumber() { return refdColNumber_; }
  void setRefdColNumber(int refdColNumber) { refdColNumber_ = refdColNumber; }

  int getRefdObjectType() { return refdObjectType_; }
  void setRefdObjectType(ComObjectType refdObjectType) { refdObjectType_ = refdObjectType; }

  void packUsage(NAString &viewColUsageStr) {
    // usage contains 2 int64 and 3 int32, 200 chars is big enough to hold
    // the string representation
    char buf[200];
    str_sprintf(buf, "viewUID: %ld viewCol: %d refUID: %ld refCol: %d refType: %d;", viewUID_, viewColNumber_, refdUID_,
                refdColNumber_, refdObjectType_);
    viewColUsageStr = buf;
  }

  void unpackUsage(const char *viewColUsageStr) {
    int theRefdObjectType;
    int retcode = sscanf(viewColUsageStr, "viewUID: %Ld viewCol: %d refUID: %Ld refCol: %d refType: %d%*s",
                           (long long int *)&viewUID_, &viewColNumber_, (long long int *)&refdUID_, &refdColNumber_,
                           &theRefdObjectType);
    assert(retcode == 5);
    refdObjectType_ = (ComObjectType)theRefdObjectType;
  }
};

#endif  // COMVIEWCOLUSAGE_H
