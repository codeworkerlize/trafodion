/**********************************************************************

//********************************************************************/
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NATableSt.cpp
 * Description:
 *
 * Created:      2/06/2012
 * Language:     C++
 *
 *****************************************************************************
 */

#include "arkcmp/NATableSt.h"

#include "arkcmp/CmpErrors.h"
#include "cli/Context.h"
#include "cli/Globals.h"
#include "optimizer/NATable.h"
#include "optimizer/SchemaDB.h"
#include "sqlcomp/CmpMain.h"

//-----------------------------------------------------------------------
// NATableCacheStoredProcedure is a class that contains functions used by
// the NATableCache virtual table, whose purpose is to serve as an interface
// to the SQL/MX NATable cache statistics. This table is implemented as
// an internal stored procedure.
//-----------------------------------------------------------------------

SP_STATUS NATableCacheStatStoredProcedure::sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields,
                                                          SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                                          SP_ERROR_STRUCT *error) {
  if (numFields != 2) {
    // accepts 2 input columns
    error->error = arkcmpErrorISPWrongInputNum;
    strcpy(error->optionalString[0], "NATableCache");
    error->optionalInteger[0] = 2;
    return SP_FAIL;
  }

  // column as input parameter for ISP, specifiy cache of metadata or user context
  strcpy(&((inputFieldFormat++)->COLUMN_DEF[0]), "instance char(16)  not null");
  strcpy(&((inputFieldFormat++)->COLUMN_DEF[0]), "location char(16)  not null");
  return SP_SUCCESS;
}

const int NUM_OF_OUTPUT = 7;

SP_STATUS NATableCacheStatStoredProcedure::sp_NumOutputFields(int *numFields, SP_COMPILE_HANDLE spCompileObj,
                                                              SP_HANDLE spObj, SP_ERROR_STRUCT *error) {
  *numFields = NUM_OF_OUTPUT;
  return SP_SUCCESS;
}

SP_STATUS NATableCacheStatStoredProcedure::sp_OutputFormat(SP_FIELDDESC_STRUCT *format, SP_KEYDESC_STRUCT keyFields[],
                                                           int *numKeyFields, SP_HANDLE spCompileObj, SP_HANDLE spObj,
                                                           SP_ERROR_STRUCT *error) {
  strcpy(&((format++)->COLUMN_DEF[0]), "Context          CHAR(8) CHARACTER SET ISO88591");
  strcpy(&((format++)->COLUMN_DEF[0]), "Num_lookups      INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "Num_cache_hits   INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "Num_entries   INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "Current_cache_size   INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "High_watermark   INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "Max_cache_size   INT UNSIGNED");

  return SP_SUCCESS;
}

SP_STATUS NATableCacheStatStoredProcedure::sp_Process(SP_PROCESS_ACTION action, SP_ROW_DATA inputData,
                                                      SP_EXTRACT_FUNCPTR eFunc, SP_ROW_DATA outputData,
                                                      SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                                                      SP_KEYVALUE_FUNCPTR kFunc, SP_PROCESS_HANDLE *spProcHandle,
                                                      SP_HANDLE spObj, SP_ERROR_STRUCT *error) {
  if (action == SP_PROC_OPEN) {
    NATableCacheStatsISPIterator *it = new (GetCliGlobals()->exCollHeap()) NATableCacheStatsISPIterator(
        inputData, eFunc, error, GetCliGlobals()->currContext()->getCmpContextInfo(), GetCliGlobals()->exCollHeap());
    *spProcHandle = it;
    return SP_SUCCESS;
  }

  if (action == SP_PROC_FETCH) {
    NATableCacheStatsISPIterator *it = (NATableCacheStatsISPIterator *)(*spProcHandle);

    if (!it) {
      return SP_FAIL;
    }

    NATableCacheStats stats;
    if (!it->getNext(stats)) return SP_SUCCESS;

    fFunc(0, outputData, sizeof(stats.contextType), &(stats.contextType), 0);
    fFunc(1, outputData, sizeof(int), &(stats.numLookups), 0);
    fFunc(2, outputData, sizeof(int), &(stats.numCacheHits), 0);
    fFunc(3, outputData, sizeof(int), &(stats.numEntries), 0);
    fFunc(4, outputData, sizeof(int), &(stats.currentCacheSize), 0);
    fFunc(5, outputData, sizeof(int), &(stats.highWaterMark), 0);
    fFunc(6, outputData, sizeof(int), &(stats.maxCacheSize), 0);
    return SP_MOREDATA;
  }

  if (action == SP_PROC_CLOSE) {
    if (*spProcHandle) NADELETEBASIC((NATableCacheStatsISPIterator *)(*spProcHandle), GetCliGlobals()->exCollHeap());
    return SP_SUCCESS;
  }

  return SP_SUCCESS;
}

void NATableCacheStatStoredProcedure::Initialize(SP_REGISTER_FUNCPTR regFunc) {
  regFunc("NATABLECACHE", sp_Compile, sp_InputFormat, 0, sp_NumOutputFields, sp_OutputFormat, sp_Process, 0,
          CMPISPVERSION);
}

SP_STATUS NATableCacheEntriesStoredProcedure::sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields,
                                                             SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                                             SP_ERROR_STRUCT *error) {
  if (numFields != 2) {
    // accepts 2 input columns
    error->error = arkcmpErrorISPWrongInputNum;
    strcpy(error->optionalString[0], "NATableCacheEntries");
    error->optionalInteger[0] = 2;
    return SP_FAIL;
  }

  // column as input parameter for ISP, specifiy cache of metadata or user context
  strcpy(&((inputFieldFormat++)->COLUMN_DEF[0]), "instance char(16)  not null");
  strcpy(&((inputFieldFormat++)->COLUMN_DEF[0]), "location char(16)  not null");
  return SP_SUCCESS;
}

SP_STATUS
NATableCacheEntriesStoredProcedure::sp_NumOutputFields(int *numFields, SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                                       SP_ERROR_STRUCT *error) {
  *numFields = 5;
  return SP_SUCCESS;
}

// Specifies the columns of the NATableEntries table and their types
SP_STATUS NATableCacheEntriesStoredProcedure::sp_OutputFormat(SP_FIELDDESC_STRUCT *format,
                                                              SP_KEYDESC_STRUCT * /*keyFields */,
                                                              int * /*numKeyFields */, SP_COMPILE_HANDLE cmpHandle,
                                                              SP_HANDLE /* spHandle */, SP_ERROR_STRUCT * /* error */) {
  strcpy(&((format++)->COLUMN_DEF[0]), "Row_id        		INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "Catalog_name 		VARCHAR(128) character set UTF8");
  strcpy(&((format++)->COLUMN_DEF[0]), "Schema_name 		VARCHAR(128) character set UTF8");
  strcpy(&((format++)->COLUMN_DEF[0]), "Object_name 		VARCHAR(128) character set UTF8");
  strcpy(&((format++)->COLUMN_DEF[0]), "Entry_size    		INT");
  return SP_SUCCESS;
}

// Copies information on the NATable cache by the interface provided in
// NATable.h and processes it
SP_STATUS NATableCacheEntriesStoredProcedure::sp_Process(SP_PROCESS_ACTION action, SP_ROW_DATA inputData,
                                                         SP_EXTRACT_FUNCPTR eFunc, SP_ROW_DATA outputData,
                                                         SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE, SP_KEYVALUE_FUNCPTR,
                                                         SP_PROCESS_HANDLE *spProcHandle, SP_HANDLE /* spHandle */,
                                                         SP_ERROR_STRUCT *error) {
  if (action == SP_PROC_OPEN) {
    NATableCacheEntriesISPIterator *it = new (GetCliGlobals()->exCollHeap()) NATableCacheEntriesISPIterator(
        inputData, eFunc, error, GetCliGlobals()->currContext()->getCmpContextInfo(), GetCliGlobals()->exCollHeap());
    *spProcHandle = it;
    return SP_SUCCESS;
  }

  if (action == SP_PROC_FETCH) {
    NATableCacheEntriesISPIterator *it = (NATableCacheEntriesISPIterator *)(*spProcHandle);

    if (!it) {
      return SP_FAIL;
    }

    // Only display objects where the current user has privs
    NATableEntryDetails details;
    NABoolean found = FALSE;
    while (!found) {
      if (!it->getNext(details)) return SP_SUCCESS;
      if (details.hasPriv == 0) continue;
      found = TRUE;
    }

    fFunc(0, outputData, sizeof(it->rowid()), &(it->rowid()), 0);
    fFunc(1, outputData, (int)strlen(details.catalog), (void *)(details.catalog), 1);
    fFunc(2, outputData, (int)strlen(details.schema), (void *)(details.schema), 1);
    fFunc(3, outputData, (int)strlen(details.object), (void *)(details.object), 1);
    fFunc(4, outputData, (int)sizeof(details.size), (void *)(&details.size), 0);

    return SP_MOREDATA;
  }

  if (action == SP_PROC_CLOSE) {
    if (*spProcHandle) NADELETEBASIC((NATableCacheEntriesISPIterator *)(*spProcHandle), GetCliGlobals()->exCollHeap());
    return SP_SUCCESS;
  }

  return SP_SUCCESS;
}

// Registers the NATableEntries function
void NATableCacheEntriesStoredProcedure::Initialize(SP_REGISTER_FUNCPTR regFunc) {
  regFunc("NATABLECACHEENTRIES", sp_Compile, sp_InputFormat, 0, sp_NumOutputFields, sp_OutputFormat, sp_Process, 0,
          CMPISPVERSION);
}

//-----------------------------------------------------------------------
// NATableCacheDeleteStoredProcedure is a class that contains functions used
// to delete the contents of the  NATableCache virtual table. The delete
// function is implemented as an internal stored procedure.
//-----------------------------------------------------------------------

SP_STATUS NATableCacheDeleteStoredProcedure::sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields,
                                                            SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                                            SP_ERROR_STRUCT *error) {
  if (numFields != 1) {
    // The delete NATable built-in function accepts one input column
    error->error = arkcmpErrorISPWrongInputNum;
    strcpy(error->optionalString[0], "NATableCache");
    error->optionalInteger[0] = 1;
    return SP_FAIL;
  }

  // Describe input columns
  strcpy(&((inputFieldFormat++)->COLUMN_DEF[0]), "cachetype char(10) not null");
  return SP_SUCCESS;
}

SP_STATUS NATableCacheDeleteStoredProcedure::sp_Process(SP_PROCESS_ACTION action, SP_ROW_DATA inputData,
                                                        SP_EXTRACT_FUNCPTR eFunc, SP_ROW_DATA outputData,
                                                        SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                                                        SP_KEYVALUE_FUNCPTR kFunc, SP_PROCESS_HANDLE *spProcHandle,
                                                        SP_HANDLE spObj, SP_ERROR_STRUCT *error) {
  SP_STATUS status = SP_SUCCESS;
  NATableDB *tableDB = NULL;

  switch (action) {
    case SP_PROC_OPEN: {
      break;
    }

    case SP_PROC_FETCH: {
      tableDB = ActiveSchemaDB()->getNATableDB();

      // See if reset only specified
      char buf[10];
      if (eFunc(0, inputData, strlen(buf), buf, FALSE) == SP_ERROR_EXTRACT_DATA) {
        error->error = arkcmpErrorISPFieldDef;
        return SP_FAIL;
      }
      buf[5] = '\0';
      NAString cacheType(buf);
      cacheType.toLower();
      NABoolean resetOnly = (cacheType == "reset") ? TRUE : FALSE;

      if (resetOnly)
        tableDB->reset_priv_entries();
      else {
        // clear out NATableCache
        tableDB->setCachingOFF();
        tableDB->setCachingON();
      }

      break;
    }

    case SP_PROC_CLOSE:
      break;
  }
  return status;
}

void NATableCacheDeleteStoredProcedure::Initialize(SP_REGISTER_FUNCPTR regFunc) {
  regFunc("NATABLECACHEDELETE", sp_Compile, sp_InputFormat, 0, sp_NumOutputFields, sp_OutputFormat, sp_Process, 0,
          CMPISPVERSION);
}

NATableCacheStatsISPIterator::NATableCacheStatsISPIterator(SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc,
                                                           SP_ERROR_STRUCT *error,
                                                           const NAArray<CmpContextInfo *> &ctxs, CollHeap *h)
    : ISPIterator(ctxs, h) {
  initializeISPCaches(inputData, eFunc, error, ctxs, contextName_, currCacheIndex_);
}

NABoolean NATableCacheStatsISPIterator::getNext(NATableCacheStats &stats) {
  // Only for remote tdm_arkcmp with 0 context
  if (currCacheIndex_ == -1) {
    const char *cName = "ARKCMP";

    ActiveSchemaDB()->getNATableDB()->getCacheStats(stats);
    memset(stats.contextType, ' ', sizeof(stats.contextType));
    memcpy(stats.contextType, cName, MINOF(sizeof(stats.contextType), strlen(cName)));
    currCacheIndex_ = -2;
    return TRUE;
  }

  // fetch QueryCaches of all CmpContexts with name equal to contextName_
  if (currCacheIndex_ > -1 && currCacheIndex_ < ctxInfos_.entries()) {
    if (!ctxInfos_[currCacheIndex_]->isSameClass(
            contextName_.data())                                 // current context name is not equal to contextName_
        && contextName_.compareTo("ALL", NAString::exact) != 0)  // and contextName_ is not "ALL"
    {                                                            // go to next context in ctxInfos_
      currCacheIndex_++;
      return getNext(stats);
    }
    ctxInfos_[currCacheIndex_]->getCmpContext()->getSchemaDB()->getNATableDB()->getCacheStats(stats);
    const char *cName = ctxInfos_[currCacheIndex_++]->getName();

    memset(stats.contextType, ' ', sizeof(stats.contextType));
    memcpy(stats.contextType, cName, MINOF(sizeof(stats.contextType), strlen(cName)));
    return TRUE;
  }
  // all entries of all caches are fetched, we are done!
  return FALSE;
}

NATableCacheEntriesISPIterator::NATableCacheEntriesISPIterator(SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc,
                                                               SP_ERROR_STRUCT *error,
                                                               const NAArray<CmpContextInfo *> &ctxs, CollHeap *h)
    : ISPIterator(ctxs, h) {
  initializeISPCaches(inputData, eFunc, error, ctxs, contextName_, currCacheIndex_);
  counter_ = 0;
  rowid_ = 0;
}

NABoolean NATableCacheEntriesISPIterator::getNext(NATableEntryDetails &details) {
  NATableDB *currNATableDB;
  // Only for remote tdm_arkcmp with 0 context
  if (currCacheIndex_ == -1) {
    currNATableDB = ActiveSchemaDB()->getNATableDB();
    if (currNATableDB->empty() || currNATableDB->end() == counter_) return FALSE;
    currNATableDB->getEntryDetails(counter_, details);
    counter_++;
    rowid_++;
    return TRUE;
  }

  // fetch QueryCaches of all CmpContexts with name equal to contextName_
  while (currCacheIndex_ > -1 && currCacheIndex_ < ctxInfos_.entries()) {
    if (!ctxInfos_[currCacheIndex_]->isSameClass(
            contextName_.data())                                 // current context name is not equal to contextName_
        && contextName_.compareTo("ALL", NAString::exact) != 0)  // and contextName_ is not "ALL"
    {                                                            // go to next context in ctxInfos_
      currCacheIndex_++;
      return getNext(details);
    }
    currNATableDB = ctxInfos_[currCacheIndex_]->getCmpContext()->getSchemaDB()->getNATableDB();
    if (currNATableDB->empty() || currNATableDB->end() == counter_) {
      currCacheIndex_++;
      counter_ = 0;
      continue;
    }
    currNATableDB->getEntryDetails(counter_, details);
    counter_++;
    rowid_++;
    return TRUE;
  }
  // all entries of all caches are fetched, we are done!
  return FALSE;
}
