
#ifndef EX_TCB_PRIVATE_H
#define EX_TCB_PRIVATE_H

/* -*-C++-*-
******************************************************************************
*
* File:         ex_tcb_privte.h
* Description:  Class declaration for ex_tcb_private
*
*
* Created:      5/3/94
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "common/Platform.h"

// external reference
class ex_tcb;

//////////////////////////////////////////////////////////////////////////
//
// Each ex_tcb class can store in an input queue a state associated
// with that input row.
// All such states should be a subclass of this one
//
// Allocate_new acts like a virtual constructor given another object
// of the desired sub-class.
//
//////////////////////////////////////////////////////////////////////////

class ex_tcb_private_state : public ExGod {
  // Error related information. For now, make it a long.
  // Later, make it the SQLDiagnosticStruct (or something similar).
  int errorCode_;

 public:
  ex_tcb_private_state();
  virtual ex_tcb_private_state *allocate_new(const ex_tcb *tcb);
  virtual ~ex_tcb_private_state();

  inline int getErrorCode() { return errorCode_; }
  inline void setErrorCode(int error_code) { errorCode_ = error_code; }
};

#endif
