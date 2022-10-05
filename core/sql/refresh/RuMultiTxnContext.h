
#ifndef _RU_MULTI_TXN_CONTEXT_H_
#define _RU_MULTI_TXN_CONTEXT_H_

/* -
 *-C++-*-
 ******************************************************************************
 *
 * File:         RuMultiTxnContext.h
 * Description:  Definition of class CRUMultiTxnContext.
 *
 * Created:      08/17/2000
 * Language:     C++
 *
 *
 ******************************************************************************
 */

//--------------------------------------------------------------------------//
// The purpose of this class is to create a stack abstract to the rows in the
// context log.
//
// Prior to any use of this class the user must call the ReadRowsFromContextUMD()
// function.
//
//--------------------------------------------------------------------------//

#include "RuSQLStaticStatementContainer.h"

class REFRESH_LIB_CLASS CRUMultiTxnContext {
 public:
  //----------------------------------//
  //	Public Members
  //----------------------------------//
 public:
  virtual ~CRUMultiTxnContext() {}

 public:
  BOOL IsEmpty() const { return const_cast<CRUMultiTxnContext *>(this)->stack_.IsEmpty(); }

  int GetNumOfRows() const { return const_cast<CRUMultiTxnContext *>(this)->stack_.GetCount(); }

 public:
  enum { ROW_DOES_NOT_EXIST = -1 };

  // This function return ROW_DOES_NOT_EXIST if no such row exists
  // index == 0 is the top row in the stack
  TInt32 GetRowByIndex(int index);
  TInt32 GetTargetEpochOfFirstRow() { return GetRowByIndex(0); }
  TInt32 GetTargetEpochOfSecondRow() { return GetRowByIndex(1); }

 public:
  // Read the "@EPOCH" column from the context table
  void ReadRowsFromContextLog(CDMPreparedStatement *readStmt);

  //  This function Adds a catchup line to the context
  void Push(TInt32 epoch) { stack_.AddHead(epoch); }

  void Pop() { stack_.RemoveHead(); }

  //----------------------------------//
  //	Private Members
  //----------------------------------//

 private:
  // The rows of the context table are orderd when retrieved by begin_epoch desc
  // so it allows us to look at the rows as a stack of context rows
  CDSList<TInt32> stack_;
};

#endif
