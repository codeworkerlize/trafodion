
#ifndef NAROUTINE_H
#define NAROUTINE_H
/* -*-C++-*-
**************************************************************************
*
* File:         NARoutine.h
* Description:  A User defined Routine
* Created:      4/24/2000
* Modified:     8/30/2009 (extensively)
* Language:     C++
*
*
**************************************************************************
*/
#include "optimizer/CostVector.h"
#include "optimizer/Stats.h"
#include "common/ComSmallDefs.h"
#include "common/NABoolean.h"
#include "optimizer/BindWA.h"
#include "optimizer/ObjectNames.h"
#include "sqlcomp/PrivMgrCommands.h"

class NARoutine;
class NARoutineDB;

// forward reference
class SimpleCostVector;

class NARoutineDBKey : public NABasicObject {
 public:
  NARoutineDBKey() {}

  NARoutineDBKey(const QualifiedName &routine, CollHeap *h = 0) : routine_(routine, h), action_(QualifiedName(h), h) {}

  NARoutineDBKey(const QualifiedName &routine, const QualifiedName &action, CollHeap *h = 0)
      : routine_(routine, h), action_(action, h) {}

  NARoutineDBKey(const NAString &routine, const NAString &action, CollHeap *h = 0)
      : routine_(routine, "", "", h), action_(action, "", "", h) {}

  NARoutineDBKey(const NARoutineDBKey &orig, CollHeap *h = 0) : routine_(orig.routine_, h), action_(orig.action_, h) {}

  NABoolean operator==(const NARoutineDBKey &orig) const {
    return ((routine_ == orig.routine_) && (action_ == orig.action_));
  }

  ExtendedQualName routine_;  // Routine name.
  ExtendedQualName action_;   // Action name (blank for routines).

  int hash() const;
};  // class NARoutineDBKey

int hashKey(const NARoutineDBKey &);

class NARoutine : public NABasicObject {
  friend class NARoutineDB;

 public:
  // -------------------------------------------------------------------
  // Constructor/Destructor functions
  // -------------------------------------------------------------------
  // For now we use the statement heap, but once we figure out
  // How to set up the NARoutineDB and delete the NARoutineDB entry at the
  // end of the statement, use contextHeap()
  NARoutine(const QualifiedName &name, const TrafDesc *TrafDesc, BindWA *bindWA, int &errorOccurred,
            NAMemory *heap = CmpCommon::contextHeap());
  NARoutine(NAMemory *heap);

  NARoutine(const QualifiedName &routineName, NAMemory *heap = CmpCommon::statementHeap());
  // copy constructor
  NARoutine(const NARoutine &old, NAMemory *h = 0);

  // Destructor
  ~NARoutine();

  // Assignment operator
  NARoutine &operator=(const NARoutine &other);

  void setSasFormatWidth(NAString &width);

  // -------------------------------------------------------------------
  // Accessor functions
  // -------------------------------------------------------------------
  inline long getRedefTime() const { return redefTime_; }
  inline const long getLastUsedTime() const { return lastUsedTime_; }
  inline NABoolean &getAccessedInCurStmt() { return accessedInCurrentStatement_; }
  inline const NAColumnArray &getInParams() const { return *inParams_; }
  inline ComSInt32 getInParamCount() const { return inParams_->entries(); }
  inline const NAColumnArray &getOutParams() const { return *outParams_; }
  inline ComSInt32 getOutParamCount() const { return outParams_->entries(); }
  inline const ARRAY(int) & getUecValues() const { return uecValues_; }
  inline const NAColumnArray &getParams() const { return *params_; }
  inline ComSInt32 getParamCount() const { return params_->entries(); }
  inline const int getUdfFanOut() const { return udfFanOut_; }
  inline SimpleCostVector &getInitialRowCostVector() { return initialRowCost_; }
  inline SimpleCostVector &getNormalRowCostVector() { return normalRowCost_; }
  inline ComSInt32 getMaxResults() const { return maxResults_; }
  inline const ComString &getExternalPath() const { return externalPath_; }
  inline const ComString &getFile() const { return externalFile_; }
  inline const ComString &getContainerName() const {
    return (language_ == COM_LANGUAGE_JAVA ? externalName_ : externalFile_);
  }
  inline const ComString &getExternalName() const { return externalName_; }
  inline const char *getMethodName() const {
    return (paramStyle_ != COM_STYLE_JAVA_OBJ ? externalName_.data() : "<init>");
  }
  inline const ComString &getSignature() const { return signature_; }
  inline const ComObjectName &getLibrarySqlName() const { return librarySqlName_; }
  inline const QualifiedName &getSqlName() const { return name_; }
  inline ComSecurityKeySet getSecKeySet() { return secKeySet_; }
  inline const long getRoutineID() const { return objectUID_; }
  inline const int getStateAreaSize() const { return stateAreaSize_; }
  inline const NAString &getDllName() const { return dllName_; }
  inline const NAString &getDllEntryPoint() const { return dllEntryPoint_; }
  inline const NAString &getParallelism() const { return comRoutineParallelism_; }
  inline const NAString &getSasFormatWidth() const { return sasFormatWidth_; }
  inline const long getDataNumEntries() const { return passThruDataNumEntries_; }
  inline const char *getData(int index) const { return passThruData_[index]; }
  inline const long getDataSize(int index) const { return passThruDataSize_[index]; }
  inline const NAString &getSystemName() const { return systemName_; }
  inline const NAString &getDataSource() const { return dataSource_; }
  inline const NAString &getFileSuffix() const { return fileSuffix_; }
  inline int getSize() { return heapSize_; }
  inline const ExtendedQualName *getRoutineName() const { return extRoutineName_; }
  inline const NAString *getActionName() const { return extActionName_; }
  inline const ComObjectName *getIntActionName() const { return intActionName_; }
  inline const NARoutineDBKey *getKey() const { return &hashKey_; }
  inline const COM_VERSION getSchemaVersion() const { return schemaVersionOfRoutine_; }
  inline NAMemory *getHeap() const { return heap_; }

  inline ComRoutineLanguage getLanguage() const { return language_; }
  inline ComRoutineType getRoutineType() const { return UDRType_; }
  inline ComRoutineSQLAccess getSqlAccess() const { return sqlAccess_; }
  inline ComRoutineTransactionAttributes getTxAttrs() const { return transactionAttributes_; }
  inline ComRoutineParamStyle getParamStyle() const { return paramStyle_; }
  inline ComRoutineExternalSecurity getExternalSecurity() const { return externalSecurity_; }
  inline int getActionPosition() const { return actionPosition_; }

  inline PrivMgrUserPrivs *getPrivInfo() const { return privInfo_; }
  inline PrivMgrDescList *getPrivDescs() const { return privDescs_; }
  inline int getObjectOwner() const { return objectOwner_; }
  inline int getSchemaOwner() const { return schemaOwner_; }

  inline void setSecKeySet(ComSecurityKeySet secKeySet) { secKeySet_ = secKeySet; }

  inline long getLibRedefTime() const { return libRedefTime_; }
  inline const NAString &getLibBlobHandle() const { return libBlobHandle_; }
  inline const NAString &getLibSchName() const { return libSchName_; }
  inline int getLibVersion() const { return libVersion_; }
  inline long getLibObjUID() const { return libObjUID_; }
  inline void setudfFanOut(int fanOut) { udfFanOut_ = fanOut; }
  inline void setExternalPath(ComString path) { externalPath_ = path; }
  inline void setFile(ComString file) { externalFile_ = file; }
  inline void setExternalName(ComString fname) { externalName_ = fname; }
  inline void setLibRedefTime(long rtime) { libRedefTime_ = rtime; }
  inline void setLibBlobHandle(NAString lobHandle) { libBlobHandle_ = lobHandle; }
  inline void setLibVersion(int version) { libVersion_ = version; }
  inline void setLibObjUID(long libobjuid) { libObjUID_ = libobjuid; }
  inline void setLibSchName(NAString schName) { libSchName_ = schName; }
  inline void setLibrarySqlName(ComObjectName lib) { librarySqlName_ = lib; }
  inline void setLanguage(ComRoutineLanguage lang) { language_ = lang; }
  inline void setRoutineType(ComRoutineType typ) { UDRType_ = typ; }
  inline void setParamStyle(ComRoutineParamStyle st) { paramStyle_ = st; }
  inline void setLastUsedTime(long time) { lastUsedTime_ = time; }
  inline void setUecForParam(int index, int uec) { uecValues_[index] = uec; }

  inline ComRoutineExecutionMode getExecutionMode() const { return executionMode_; }

  inline NABoolean isSPSQL() const { return (str_cmp_ne(externalFile_, SEABASE_SPSQL_CONTAINER) == 0); }
  inline NABoolean isIsolate() const { return isIsolate_; }
  inline NABoolean isFinalCall() const { return isExtraCall_; }
  inline NABoolean isExtraCall() const { return isExtraCall_; }
  inline NABoolean isJava() const { return (language_ == COM_LANGUAGE_JAVA); }
  inline NABoolean isC() const { return (language_ == COM_LANGUAGE_C); }
  inline NABoolean isSQL() const { return (language_ == COM_LANGUAGE_SQL); }
  inline NABoolean isProcedure() const { return (UDRType_ == COM_PROCEDURE_TYPE); }
  inline NABoolean isScalarUDF() const { return (UDRType_ == COM_SCALAR_UDF_TYPE); }
  inline NABoolean isTableValuedUDF() const { return (UDRType_ == COM_TABLE_UDF_TYPE); }
  inline NABoolean isDeterministic() const { return isDeterministic_; }
  inline NABoolean isCallOnNull() const { return isCallOnNull_; }
  inline NABoolean isUniversal() const { return isUniversal_; }
  inline NABoolean hasOutParams() const { return hasOutParams_; }
  inline NABoolean hasResultSets() const { return (maxResults_ > 0); }

  void setPrivInfo(PrivMgrUserPrivs *privInfo) { privInfo_ = privInfo; }
  void setPrivDescs(PrivMgrDescList *privDescs) { privDescs_ = privDescs; }
  void getPrivileges(TrafDesc *priv_desc, BindWA *bindWA);
  void removePrivInfo();

  // -------------------------------------------------------------------
  // Standard operators
  // -------------------------------------------------------------------
  inline NABoolean operator==(const NARoutine &other) const { return (this == &other); }

 private:
  // Default constructor not written
  NARoutine();

  // -----------------------------------------------------------------------
  // The heap for the dynamic allocation of the NARoutine members.
  // -----------------------------------------------------------------------
  NAMemory *heap_;
  int heapSize_;        // Size of this heap, set in constructor
                        // (Each NARoutine should be on own heap if cached)
  QualifiedName name_;  // SP name
  ExtendedQualName *extRoutineName_;
  NAString *extActionName_;       // Empty if not an action.
  ComObjectName *intActionName_;  // The <UUDF uid>_<action> name from
                                  // metadata. Empty if not an action.
  NARoutineDBKey hashKey_;        // For caching
  long redefTime_;
  long lastUsedTime_;
  NABoolean accessedInCurrentStatement_;

  NAColumnArray *inParams_;   // IN & INOUT params
  NAColumnArray *outParams_;  // OUT & INOUT params
  NAColumnArray *params_;     // params

  ComRoutineLanguage language_;  // Java, C, SQL, C++ ???
  ComRoutineType UDRType_;
  ComRoutineSQLAccess sqlAccess_;                          // NO SQL, CONTAINS SQL ...
  ComRoutineTransactionAttributes transactionAttributes_;  // READONLY ...
  ComSInt32 maxResults_;
  ComString externalPath_;  // URL
  ComString externalFile_;
  ComString externalName_;  // Java method name
  long libRedefTime_;
  NAString libBlobHandle_;
  NAString libSchName_;
  int libVersion_;
  long libObjUID_;
  ComString signature_;
  ComObjectName librarySqlName_;  // ANSI name of JAR/DLL
  ComRoutineParamStyle paramStyle_;
  ComSInt32 paramStyleVersion_;
  NABoolean isDeterministic_;
  NABoolean isCallOnNull_;
  NABoolean isIsolate_;
  ComRoutineExternalSecurity externalSecurity_;
  NABoolean isExtraCall_;
  NABoolean hasOutParams_;

  long objectUID_;
  NABoolean isUniversal_;
  int actionPosition_;
  ComRoutineExecutionMode executionMode_;
  int stateAreaSize_;
  NAString dllName_;
  NAString dllEntryPoint_;
  NAString comRoutineParallelism_;
  NAString sasFormatWidth_;
  char **passThruData_;
  long *passThruDataSize_;
  long passThruDataNumEntries_;
  NAString systemName_;
  NAString dataSource_;
  NAString fileSuffix_;
  SimpleCostVector initialRowCost_;
  SimpleCostVector normalRowCost_;
  int udfFanOut_;
  ARRAY(int) uecValues_;  // Use to store UEC values of outputCols

  COM_VERSION schemaVersionOfRoutine_;
  int objectOwner_;
  int schemaOwner_;

  PrivMgrDescList *privDescs_;
  PrivMgrUserPrivs *privInfo_;
  ComSecurityKeySet secKeySet_;
};

//-----------------------------------------------------------------------
// NARoutineCacheStoredProcedure is a class that contains functions used by
// the NARoutineCache virtual table, whose purpose is to serve as an interface
// to the SQL/MX NATable cache statistics. This table is implemented as
// an internal stored procedure.
//-----------------------------------------------------------------------
class NARoutineCacheStatStoredProcedure {
 public:
  NARoutineCacheStatStoredProcedure();
  virtual ~NARoutineCacheStatStoredProcedure();

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
  static SP_STATUS sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields, SP_COMPILE_HANDLE spCompileObj,
                                  SP_HANDLE spObj, SP_ERROR_STRUCT *error);

  // sp_NumOutputFields function is called at compile-time of the stored
  // procedure to inquire about the number of output fields in a row.
  static SP_STATUS sp_NumOutputFields(int *numFields, SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                      SP_ERROR_STRUCT *error);

  // sp_OutputFormat is called at compile-time of the stored procedure to
  // determine  the format (type info) of each field that will become part of the
  // row being  output from the stored procedure.
  static SP_STATUS sp_OutputFormat(SP_FIELDDESC_STRUCT *outputFieldFormat, SP_KEYDESC_STRUCT keyFields[],
                                   int *numKeyFields, SP_HANDLE spCompileObj, SP_HANDLE spObj, SP_ERROR_STRUCT *error);

  // sp_Process is called at run-time of the stored procedure.
  static SP_STATUS sp_ProcessRoutine(SP_PROCESS_ACTION action, SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc,
                                     SP_ROW_DATA outputData, SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                                     SP_KEYVALUE_FUNCPTR kFunc, SP_PROCESS_HANDLE *spProcHandle, SP_HANDLE spObj,
                                     SP_ERROR_STRUCT *error);
  static SP_STATUS sp_ProcessAction(SP_PROCESS_ACTION action, SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc,
                                    SP_ROW_DATA outputData, SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                                    SP_KEYVALUE_FUNCPTR kFunc, SP_PROCESS_HANDLE *spProcHandle, SP_HANDLE spObj,
                                    SP_ERROR_STRUCT *error);
};  // class NARoutineCacheStatStoredProcedure

//-----------------------------------------------------------------------
// NARoutineCacheDeleteStoredProcedure is a class that contains functions used
// to delete the contents of the  NARoutineCache virtual table. The delete
// function is implemented as an internal stored procedure.
//-----------------------------------------------------------------------
class NARoutineCacheDeleteStoredProcedure {
 public:
  NARoutineCacheDeleteStoredProcedure();
  virtual ~NARoutineCacheDeleteStoredProcedure();

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
  static SP_STATUS sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields, SP_COMPILE_HANDLE spCompileObj,
                                  SP_HANDLE spObj, SP_ERROR_STRUCT *error);

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
                                   int *numKeyFields, SP_HANDLE spCompileObj, SP_HANDLE spObj, SP_ERROR_STRUCT *error) {
    return SP_SUCCESS;
  }

  // sp_Process is called at run-time of the stored procedure.
  static SP_STATUS sp_Process(SP_PROCESS_ACTION action, SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc,
                              SP_ROW_DATA outputData, SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                              SP_KEYVALUE_FUNCPTR kFunc, SP_PROCESS_HANDLE *spProcHandle, SP_HANDLE spObj,
                              SP_ERROR_STRUCT *error);
};  // class NARoutineCacheDeleteStoredProcedure

#endif
