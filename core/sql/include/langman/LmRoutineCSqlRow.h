#ifndef LMROUTINECSQLROW_H
#define LMROUTINECSQLROW_H
/* -*-C++-*-
**********************************************************************
*
* File:         LmRoutineCSqlRow.h
* Description:  LmRoutine for C routines using parameter style SQLROW
* Created:      08/02/2009
* Language:     C++
*

**********************************************************************/

#include "LmRoutineC.h"
#include "sqludr/sqludr.h"

//////////////////////////////////////////////////////////////////////
//
// LmRoutineCSqlRow
//
// The LmRoutineCSqlRow is a concrete class used to maintain state
// for, and the invocation of, a C routine that uses the SQLROW
// parameter style. Its base class representation is returned by the
// LMC as a handle to LM clients.
//
//////////////////////////////////////////////////////////////////////
class SQLLM_LIB_FUNC LmRoutineCSqlRow : public LmRoutineC {
  friend class LmLanguageManagerC;

 public:
  virtual LmResult invokeRoutine(void *inputRow, void *outputRow, ComDiagsArea *da);

 protected:
  LmRoutineCSqlRow(const char *sqlName, const char *externalName, const char *librarySqlName, ComUInt32 numSqlParam,
                   char *routineSig, ComUInt32 maxResultSets, ComRoutineTransactionAttributes transactionAttrs,
                   ComRoutineSQLAccess sqlAccessMode, ComRoutineExternalSecurity externalSecurity, int routineOwnerId,
                   const char *parentQid, ComUInt32 inputRowLen, ComUInt32 outputRowLen, const char *currentUserName,
                   const char *sessionUserName, LmParameter *parameters, LmLanguageManagerC *lm, LmHandle routine,
                   LmContainer *container, ComDiagsArea *diagsArea);

  virtual ~LmRoutineCSqlRow();

};  // class LmRoutineCSqlRow

#endif
