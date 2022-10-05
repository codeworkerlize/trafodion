
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         PortProcessCalls.h
 * Description:  Some common Process related functions that are called by the
 *               SQL engine components
 *
 *               Includes class NAProcessHandle. This is used to encapsulate
 *               process handle.
 *               See comments in class heading for more details.
 *
 * Created:      08/28/08
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#ifndef PORTPROCESSCALLS_H
#define PORTPROCESSCALLS_H

#include "common/Platform.h"
#include "seabed/fs.h"

#define PhandleSize      64
#define PhandleStringLen (MS_MON_MAX_PROCESS_NAME + 1 + 20 + 1)
#define NodeNameLen      9

//
// Class NAProcessHandle:
//
// This class encapsulates the differences between platforms for the
// Process Handle.
//
//  On LINUX, the PHANDLE is 64 bytes
//
// ------------------------------------------------------------------------
class NAProcessHandle {
 public:
  // Constructor
  NAProcessHandle();

  NAProcessHandle(const SB_Phandle_Type *phandle);

  // Destructor
  virtual ~NAProcessHandle(void){};

  // Guardian procedure call wrappers
  // Add new methods to support any other guardian procedure calls
  // in this section

  // Wrapper for PROCESSHANDLE_DECOMPOSE_
  short decompose();

  // Wrapper for PROCESSHANDLE_GETMINE_
  short getmine(SB_Phandle_Type *phandle);

  short getmine();

  // Wrapper for PROCESSHANDLE_NULLIT_
  short nullit(SB_Phandle_Type *phandle);

  // Accessors to access various process handle components
  SB_Phandle_Type *getPhandle() { return &phandle_; }
  int getCpu() { return cpu_; }
  int getPin() { return pin_; }
  long getSeqNum() { return seqNum_; }
  void setPhandle(SB_Phandle_Type *phandle);

  int getNodeNumber() { return nodeNumber_; }
  int getSegment() { return nodeNumber_; }
  char *getNodeName();
  short getNodeNameLen();
  char *getPhandleString();
  short getPhandleStringLen();

 private:
  SB_Phandle_Type phandle_;  // 64 bytes phandle
  int cpu_;
  int pin_;
  long seqNum_;

  int nodeNumber_;
  char nodeName_[NodeNameLen + 1];
  short nodeNameLen_;
  char phandleString_[PhandleStringLen + 1];
  short phandleStringLen_;
};

#endif  // PORTPROCESSCALLS_H
