
#ifndef ELEMDDLUDRTRANSACTION_H
#define ELEMDDLUDRTRANSACTION_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrTransaction.h
* Description:  class for UDR Transaction Attributes (parse node) elements in
*               DDL statements
*
*
* Created:      10/08/1999
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLNode.h"
#include "common/ComSmallDefs.h"

class ElemDDLUdrTransaction : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrTransaction(ComRoutineTransactionAttributes theTransactionAttributes);

  // virtual destructor
  virtual ~ElemDDLUdrTransaction(void);

  // cast
  virtual ElemDDLUdrTransaction *castToElemDDLUdrTransaction(void);

  // accessor
  inline const ComRoutineTransactionAttributes getTransactionAttributes(void) const { return transactionAttributes_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComRoutineTransactionAttributes transactionAttributes_;

};  // class ElemDDLUdrTransaction

#endif /* ELEMDDLUDRTRANSACTION_H */
