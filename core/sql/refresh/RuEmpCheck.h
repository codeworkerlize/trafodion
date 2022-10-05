
#ifndef _RU_EMP_CHECK_H_
#define _RU_EMP_CHECK_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuEmpCheck.h
* Description:  Definition of class CRUEmpCheck
*
*
* Created:      10/15/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"
#include "dsstring.h"

#include "RuEmpCheckVector.h"

class CRUSQLDynamicStatementContainer;
class CRUTbl;
class CUOFsIpcMessageTranslator;

//--------------------------------------------------------------------------//
//	CRUEmpCheck
//
//	This class implements the delta emptiness check algorithm, which checks
//	whether the table-delta	is empty starting from epoch1, epoch2, ...
//
//	Emptiness check is required by various task	executors:
//	EmpCheckTask, MultiTxn Refresh, LogCleanup.
//
//	The algorithm must take into account that the logging of ranges
//	and single-row records is separated into negative and positive
//	epochs, and apply different queries, respectively.
//
//	The emptiness check's results are recorded in an emptiness check vector,
//	which is constructed outside the class, in accordance with the algorithm's
//	client's requirements. The class initializes a *local* vector based
//	on the epochs in the external one, and populates it by the check's result.
//	Local storage makes the class transportable between processes.
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUEmpCheck {
 public:
  CRUEmpCheck();
  CRUEmpCheck(CRUEmpCheckVector &vec);
  virtual ~CRUEmpCheck();

 public:
  const CRUEmpCheckVector &GetVector() const { return *pVec_; }

 public:
  // If upperBound=0, the WHERE predicate
  // does not limit the epoch from above.
  void ComposeSQL(CRUTbl &tbl, TInt32 upperBound = 0);

  void PrepareSQL();

  void PerformCheck();

 public:
  // IPC pack/unpack
  void LoadData(CUOFsIpcMessageTranslator &translator);
  void StoreData(CUOFsIpcMessageTranslator &translator);

 private:
  //-- Prevent copying --//
  CRUEmpCheck(const CRUEmpCheck &other);
  CRUEmpCheck &operator=(const CRUEmpCheck &other);

 private:
  enum StmtType {

    CHECK_NEG_EPOCHS = 0,          // Range records
    CHECK_POS_EPOCHS_INSERTS = 1,  // Single-row insert records
    CHECK_POS_EPOCHS_DELETES = 2,  // Single row delete and update records

    NUM_STMTS
  };

  void ComposeSelectStmt(CRUTbl &tbl, CDSString &to, StmtType stmtType, TInt32 upperBound);

  void PerformSingleCheck(CRUEmpCheckVector::Elem &elem, CRUTbl::IUDLogContentType ct, StmtType stmtType);

 private:
  CRUEmpCheckVector *pVec_;

  CRUSQLDynamicStatementContainer *pSQLContainer_;

  // Which checks to perform?
  int checkMask_;
};

#endif
