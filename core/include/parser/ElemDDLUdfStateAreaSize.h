/* -*-C++-*- */

#ifndef ELEMDDLUDFSTATEAREASIZE_H
#define ELEMDDLUDFSTATEAREASIZE_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdfStateAreaSize.h
* Description:  class for UDF State Area Size (parse node) elements in
*               DDL statements
*
*
* Created:      7/14/09
* Language:     C++
*
*
*
*
******************************************************************************
*/
#include "parser/ElemDDLNode.h"

class ElemDDLUdfStateAreaSize : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdfStateAreaSize(ComUInt32 sizeInBytes);

  // virtual destructor
  virtual ~ElemDDLUdfStateAreaSize(void);

  // cast
  virtual ElemDDLUdfStateAreaSize *castToElemDDLUdfStateAreaSize(void);

  // accessor
  inline const ComUInt32 getStateAreaSize(void) const { return stateAreaSize_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComUInt32 stateAreaSize_;

};  // class ElemDDLUdfStateAreaSize

#endif /* ELEMDDLUDFSTATEAREASIZE_H */
