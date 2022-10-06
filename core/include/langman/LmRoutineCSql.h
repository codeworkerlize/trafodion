#ifndef LMROUTINECSQL_H
#define LMROUTINECSQL_H
/* -*-C++-*-
**********************************************************************
*
* File:         LmRoutineCSql.h
* Description:  LmRoutine for C routines using parameter style SQL
* Created:      08/02/2009
* Language:     C++
*

**********************************************************************/

#include "LmRoutineC.h"
#include "LmParameter.h"

//////////////////////////////////////////////////////////////////////
//
// LmRoutineCSql
//
// The LmRoutineCSql is a concrete class used to maintain state for,
// and the invocation of, a C function that uses PARAMSTYLE_SQL
// parameter style. Its base class representation (LmRoutine) is
// returned by the LMC as a handle to LM clients.
//
// This is currently used for command line invocation of C functions
// with signature (int, char**) too.
//
//////////////////////////////////////////////////////////////////////
class SQLLM_LIB_FUNC LmRoutineCSql : public LmRoutineC {
  friend class LmLanguageManagerC;

 public:
  virtual LmResult invokeRoutine(void *inputRow, void *outputRow, ComDiagsArea *da);

 protected:
  LmRoutineCSql(const char *sqlName, const char *externalName, const char *librarySqlName, ComUInt32 numSqlParam,
                char *routineSig, ComUInt32 maxResultSets, ComRoutineTransactionAttributes transactionAttrs,
                ComRoutineSQLAccess sqlAccessMode, ComRoutineExternalSecurity externalSecurity, int routineOwnerId,
                const char *parentQid, ComUInt32 inputRowLen, ComUInt32 outputRowLen, const char *currentUserName,
                const char *sessionUserName, LmParameter *parameters, LmLanguageManagerC *lm, LmHandle routine,
                LmContainer *container, ComDiagsArea *diagsArea);

  virtual ~LmRoutineCSql();

 protected:
  // Suppress warnings about data member data types not being exposed
  // to callers via the DLL interface

  // An array of pointers to individual data buffers. These pointers
  // point to the same buffers pointed to by instances of the cBuf_
  // collection (declared below). The cBuf_ instances are used to
  // create and destroy the buffers. The data_ pointer is for
  // convenience only, it gives us the ability to pass the entire
  // collection of pointers into a stack frame using a single stack
  // argument. Much like the "argv" collection passed into a C main()
  // function.
  char **data_;

  // One buffer containing an array of null indicators
  LmCBuffer ind_;

  // A pointer to an an array of buffer objects, each object manages a
  // single data buffer
  LmCBuffer *cBuf_;

};  // class LmRoutineCSql

#endif
