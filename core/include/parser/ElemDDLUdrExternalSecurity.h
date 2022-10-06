
#ifndef ELEMDDLUDREXTERNALSECURITY_H
#define ELEMDDLUDREXTERNALSECURITY_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrExternalSecurity.h
* Description:  class for UDR External Security (parse node) elements in
*               DDL statements
*
*
* Created:      01/20/2012
* Language:     C++
*
*
******************************************************************************
*/

#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"

class ElemDDLUdrExternalSecurity : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrExternalSecurity(ComRoutineExternalSecurity theExternalSecurity);

  // virtual destructor
  virtual ~ElemDDLUdrExternalSecurity(void);

  // cast
  virtual ElemDDLUdrExternalSecurity *castToElemDDLUdrExternalSecurity(void);

  // accessor
  inline const ComRoutineExternalSecurity getExternalSecurity(void) const { return externalSecurity_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComRoutineExternalSecurity externalSecurity_;

};  // class ElemDDLUdrExternalSecurity

#endif /* ELEMDDLUDREXTERNALSECURITY_H */
