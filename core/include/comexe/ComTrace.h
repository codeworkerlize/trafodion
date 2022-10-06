

#ifndef COM_TRACE_H
#define COM_TRACE_H

#include "export/NAVersionedObject.h"

//
// class ComTracePointInfo
// Description:
//   Used to store a trace point and its related info in ComTdb at codegen time.
//   A flat memory buffer will be allocated for each TP set.
//   This information is used to create an ExDp2TracePoint object at runtime.
class ComTracePointInfo {
 private:
  UInt32 tracePoint_;  // trace point
  UInt32 tpCount_;     // count max if specified
  UInt16 tpActions_;   // max of 4 actions per TracePoint
  UInt16 filler_;

 public:
  enum TPSeparators {
    TP_OP_START_SEP = '(',
    TP_OP_END_SEP = ')',
    TP_TP_ACTION_SEP = ':',
    TP_ACTION_SEP = ',',
    TP_ACTION_COUNT_SEP = '.',
    TP_TP_SEP = '|',
    TP_SPACE = ' '
  };

  //
  // Class Methods
  //

  static Int16 parseTPString(char *traceString, UInt32 tdbOperator, Space *space, ComTracePointInfo **tpInfo,
                             UInt32 &tpCount);

  //
  // Object Methods
  //

  //
  // Ctor
  ComTracePointInfo(UInt32 tracePoint, UInt32 counterMax, UInt16 actions)
      : tracePoint_(tracePoint), tpCount_(counterMax), tpActions_(actions), filler_((UInt16)0) {}

  //
  // Ctor
  ComTracePointInfo(UInt32 tracePoint, UInt16 actions)
      : tracePoint_(tracePoint), tpCount_(0), tpActions_(actions), filler_((UInt16)0) {}

  void init(UInt32 tracePoint, UInt32 counterMax, UInt16 actions) {
    tracePoint_ = tracePoint;
    tpCount_ = counterMax;
    tpActions_ = actions;
    filler_ = (UInt16)0;
  }

  UInt32 getTracePoint() { return tracePoint_; }

  UInt32 getCounterMax() { return tpCount_; }

  UInt16 getActions() { return tpActions_; }
};

#endif  // COM_TRACE_H
