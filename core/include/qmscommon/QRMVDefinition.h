// **********************************************************************

// **********************************************************************

#ifndef _QRMVDEFINITION_H_
#define _QRMVDEFINITION_H_

#include "common/NAString.h"
#include "common/NABoolean.h"

class QRMVDefinition {
 public:
  QRMVDefinition(CollHeap *heap) : redefTimeString_(heap), refreshedTimeString_(heap) {}

  QRMVDefinition(NABoolean hasIgnoreChanges, long redefTime, long refreshedTime, long objectUID, CollHeap *heap)
      : hasIgnoreChanges_(hasIgnoreChanges),
        redefTime_(redefTime),
        refreshedTime_(refreshedTime),
        objectUID_(objectUID),
        redefTimeString_(heap),
        refreshedTimeString_(heap) {
    refreshedTimeString_ = Int64ToNAString(refreshedTime);
    redefTimeString_ = Int64ToNAString(redefTime);
  }

  NABoolean hasIgnoreChanges_;
  NAString redefTimeString_;
  long redefTime_;
  NAString refreshedTimeString_;
  long refreshedTime_;
  long objectUID_;
};

#endif  // _QRMVDEFINITION_H_
