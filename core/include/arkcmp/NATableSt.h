
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NATableSt.h
 * Description:
 *
 *
 * Created:      2/06/2012
 * Language:     C++
 *
 *****************************************************************************
 */

#ifndef NATABLEST_H
#define NATABLEST_H

#include "arkcmp/CmpISPStd.h"
#include "export/NABasicObject.h"
#include "arkcmp/QueryCacheSt.h"
#include "optimizer/NATable.h"

//-----------------------------------------------------------------------
// NATableCacheStoredProcedure is a class that contains functions used by
// the NATableCache virtual table, whose purpose is to serve as an interface
// to the SQL/MX NATable cache statistics. This table is implemented as
// an internal stored procedure.
//-----------------------------------------------------------------------
class NATableCacheStatStoredProcedure {
 public:
  NATableCacheStatStoredProcedure();
  virtual ~NATableCacheStatStoredProcedure();

  // Initialize() is called at the time when the stored procedure is
  // being registered with arkcmp.
  static void Initialize(SP_REGISTER_FUNCPTR regFunc);

  // sp_Compile. For Embedded SQL environment, a stored procedure is
  // compiled only  the first time it is invoked.
  static SP_STATUS sp_Compile(SP_COMPILE_ACTION action, SP_COMPILE_HANDLE *pCompileObj, SP_HANDLE pObj,
                              SP_ERROR_STRUCT *error) {
    return SP_SUCCESS;
  }

  static SP_STATUS sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields,
                                  SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj, SP_ERROR_STRUCT *error);

  // sp_NumOutputFields function is called at compile-time of the stored
  // procedure to inquire about the number of output fields in a row.
  static SP_STATUS sp_NumOutputFields(int *numFields, SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                      SP_ERROR_STRUCT *error);

  // sp_OutputFormat is called at compile-time of the stored procedure to
  // determine  the format (type info) of each field that will become part of the
  // row being  output from the stored procedure.
  static SP_STATUS sp_OutputFormat(SP_FIELDDESC_STRUCT *outputFieldFormat, SP_KEYDESC_STRUCT keyFields[],
                                   int *numKeyFields, SP_HANDLE spCompileObj, SP_HANDLE spObj,
                                   SP_ERROR_STRUCT *error);

  // sp_Process is called at run-time of the stored procedure.
  static SP_STATUS sp_Process(SP_PROCESS_ACTION action, SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc,
                              SP_ROW_DATA outputData, SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                              SP_KEYVALUE_FUNCPTR kFunc, SP_PROCESS_HANDLE *spProcHandle, SP_HANDLE spObj,
                              SP_ERROR_STRUCT *error);
};  // class NATableCacheStatStoredProcedure

// ----------------------------------------------------------------------------
// NATableCacheEntriesStoredProcedure is a class that contains functions used
// by the NATableCacheEntries virtual table, whose purpose is to serve as an
// interface to the NATable cache. This table is implemented as an internal
// stored procedure.
// ----------------------------------------------------------------------------
class NATableCacheEntriesStoredProcedure {
 public:
  NATableCacheEntriesStoredProcedure();
  virtual ~NATableCacheEntriesStoredProcedure();

  // Initialize() is called at the time when the stored procedure is initially
  // being registered with ARKCMP.  The default implementation does
  // nothing and the derived class can implement whatever initialization
  // behavior it wants.
  static void Initialize(SP_REGISTER_FUNCPTR regFunc);

  // sp_Compile. For Embedded SQL environment, a stored procedure is
  // compiled only the first time it is invoked.
  static SP_STATUS sp_Compile(SP_COMPILE_ACTION action, SP_COMPILE_HANDLE *pCompileObj, SP_HANDLE pObj,
                              SP_ERROR_STRUCT *error) {
    return SP_SUCCESS;
  }

  static SP_STATUS sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields,
                                  SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj, SP_ERROR_STRUCT *error);

  // sp_NumOutputFields function is called at compile-time of the stored
  // procedure to inquire about the number of output fields in a row.
  static SP_STATUS sp_NumOutputFields(int *numFields, SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                      SP_ERROR_STRUCT *error);

  // sp_OutputFormat is called at compile-time of the stored procedure to
  // determine the format (type info) of each field that will become part of the
  // row being output from the stored procedure.
  static SP_STATUS sp_OutputFormat(SP_FIELDDESC_STRUCT *outputFieldFormat, SP_KEYDESC_STRUCT keyFields[],
                                   int *numKeyFields, SP_HANDLE spCompileObj, SP_HANDLE spObj,
                                   SP_ERROR_STRUCT *error);

  // sp_Process is called at run-time of the stored procedure.
  static SP_STATUS sp_Process(SP_PROCESS_ACTION action, SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc,
                              SP_ROW_DATA outputData, SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                              SP_KEYVALUE_FUNCPTR kFunc, SP_PROCESS_HANDLE *spProcHandle, SP_HANDLE spObj,
                              SP_ERROR_STRUCT *error);
};

//-----------------------------------------------------------------------
// NATableCacheDeleteStoredProcedure is a class that contains functions used
// to delete the contents of the  NATableCache virtual table. The delete
// function is implemented as an internal stored procedure.
//-----------------------------------------------------------------------
class NATableCacheDeleteStoredProcedure {
 public:
  NATableCacheDeleteStoredProcedure();
  virtual ~NATableCacheDeleteStoredProcedure();

  // Initialize() is called at the time when the stored procedure is
  // being registered with arkcmp.
  static void Initialize(SP_REGISTER_FUNCPTR regFunc);

  // sp_Compile. For Embedded SQL environment, a stored procedure is
  // compiled only  the first time it is invoked.
  static SP_STATUS sp_Compile(SP_COMPILE_ACTION action, SP_COMPILE_HANDLE *pCompileObj, SP_HANDLE pObj,
                              SP_ERROR_STRUCT *error) {
    return SP_SUCCESS;
  }

  // sp_InputFormat is called with action=OPEN before any compile-time
  // functions are called.  It is then again called after all compile-time
  // functions have been called, this time with action=CLOSE.
  static SP_STATUS sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields,
                                  SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj, SP_ERROR_STRUCT *error);

  // sp_NumOutputFields function is called at compile-time of the stored
  // procedure to inquire about the number of output fields in a row.
  static SP_STATUS sp_NumOutputFields(int *numFields, SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                      SP_ERROR_STRUCT *error) {
    *numFields = 0;
    return SP_SUCCESS;
  }

  // sp_OutputFormat is called at compile-time of the stored procedure to
  // determine  the format (type info) of each field that will become part of the
  // row being  output from the stored procedure.
  static SP_STATUS sp_OutputFormat(SP_FIELDDESC_STRUCT *outputFieldFormat, SP_KEYDESC_STRUCT keyFields[],
                                   int *numKeyFields, SP_HANDLE spCompileObj, SP_HANDLE spObj,
                                   SP_ERROR_STRUCT *error) {
    return SP_SUCCESS;
  }

  // sp_Process is called at run-time of the stored procedure.
  static SP_STATUS sp_Process(SP_PROCESS_ACTION action, SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc,
                              SP_ROW_DATA outputData, SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                              SP_KEYVALUE_FUNCPTR kFunc, SP_PROCESS_HANDLE *spProcHandle, SP_HANDLE spObj,
                              SP_ERROR_STRUCT *error);
};  // class NATableCacheDeleteStoredProcedure

class NATableCacheStatsISPIterator : public ISPIterator {
 public:
  NATableCacheStatsISPIterator(SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc, SP_ERROR_STRUCT *error,
                               const NAArray<CmpContextInfo *> &ctxs, CollHeap *h);

  NABoolean getNext(NATableCacheStats &stats);
};

class NATableCacheEntriesISPIterator : public ISPIterator {
 public:
  NATableCacheEntriesISPIterator(SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc, SP_ERROR_STRUCT *error,
                                 const NAArray<CmpContextInfo *> &ctxs, CollHeap *h);

  NABoolean getNext(NATableEntryDetails &details);
  int &counter() { return counter_; }
  int &rowid() { return rowid_; }

 private:
  int counter_;
  int rowid_;
};

struct NARoutineCacheStats {
  int numLookups;
  int numCacheHits;
  int currentCacheSize;
  int highWaterMark;
  int maxCacheSize;
  int numEntries;
};

class NARoutineCacheStatsISPIterator : public ISPIterator {
 public:
  NARoutineCacheStatsISPIterator(SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc, SP_ERROR_STRUCT *error,
                                 const NAArray<CmpContextInfo *> &ctxs, CollHeap *h);

  NABoolean getNext(NARoutineCacheStats &stats);
};
#endif
