
/* -*-C++-*-
 ******************************************************************************
 *
 * File:         CmpISPInterface.cpp
 * Description:
 * Created:      3/26/2014 (relocate to this file)
 * Language:     C++
 *
 *
 *
 *
 ******************************************************************************
 */

#include "sqlcomp/CmpISPInterface.h"
#include "arkcmp/CmpISPStd.h"
#include "arkcmp/CmpStoredProc.h"
#include "optimizer/NATable.h"
#include "arkcmp/NATableSt.h"
#include "optimizer/NARoutine.h"

CmpISPInterface::CmpISPInterface() { initCalled_ = FALSE; }

void CmpISPInterface::InitISPFuncs() {
  SP_REGISTER_FUNCPTR regFunc = &(CmpISPFuncs::RegFuncs);

  if (initCalled_) return;

  // todo, error handling.
  // Query cache virtual tables
  QueryCacheStatStoredProcedure::Initialize(regFunc);
  QueryCacheEntriesStoredProcedure::Initialize(regFunc);
  QueryCacheDeleteStoredProcedure::Initialize(regFunc);

  HybridQueryCacheStatStoredProcedure::Initialize(regFunc);
  HybridQueryCacheEntriesStoredProcedure::Initialize(regFunc);

  // NATable cache statistics virtual table
  NATableCacheStatStoredProcedure::Initialize(regFunc);
  NATableCacheEntriesStoredProcedure::Initialize(regFunc);

  // NATable cache statistics delete
  NATableCacheDeleteStoredProcedure::Initialize(regFunc);

  // NARoutine cache statistics virtual table
  NARoutineCacheStatStoredProcedure::Initialize(regFunc);

  // NARoutine cache statistics delete
  NARoutineCacheDeleteStoredProcedure::Initialize(regFunc);

  // insert an empty entry to indicate end of array
  CmpISPFuncs::procFuncsArray_.insert(CmpISPFuncs::ProcFuncsStruct());
  initCalled_ = TRUE;
}

CmpISPInterface::~CmpISPInterface() {}

//
// NOTE: The cmpISPInterface variable is being left as a global
//       because, once it is initialized, it is never changed
//       AND the initial thread in the process should initialize it
//       before any other Compiler Instance threads can be created.
//
CmpISPInterface cmpISPInterface;
