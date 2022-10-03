/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpContext.h
 * Description:  The class declaration for CmpContext class, containing the
 *               global variables for compiler components.
 *               
 *               
 * Created:      9/05/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */


#ifndef CMPCONTEXT__H
#define CMPCONTEXT__H

// CmpContext class is used to track the global information arkcmp needs
// for each connection to executor, this is to lay the ground for future
// possibility of multi-threading of arkcmp. This class contains info
// that allocated for each context, i.e. will not be destroyed for each
// statement

#include <fstream>

#include "export/ComDiags.h"
#include "common/CmpCommon.h"
#include "CmpSqlSession.h"
#include "common/NABoolean.h"
#include "export/NAStringDef.h"
#include "ProcessEnv.h"
#include "cli/sqlcli.h"
#include "common/ComSysUtils.h"        // for TimeVal

#include "common/Collections.h"	// for NAList
#include "common/NAAssert.h"		// required after including a RogueWave file!

#include "comexe/CmpMessage.h"
#include "optimizer/TableDesc.h"
#include "common/SharedPtr.h"

class SchemaDB;
class ControlDB;
class CmpStatement;
class CmpSqlSession;
class POSInfo;
class NAClusterInfo;
class RuleSet;
class OptDebug;
class CmpMemoryMonitor;
class OptimizerSimulator;
class QueryCache;
class HistogramCache;
class CompilerTrackingInfo;
class OptDefaults;
struct MDDescsInfo;
class CmpStatementISP;
class EstLogProp;
class NAWNodeSet;
class NATable;
class CNATestPointArray;

typedef IntrusiveSharedPtr<EstLogProp> EstLogPropSharedPtr;
namespace tmudr {
  class UDRInvocationInfo;
  class UDRPlanInfo;
  class UDR;
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Class DDLObjInfo:
//
// Describes a DDL object which is part of an DDLObjInfoList.
//
// members:
//    objName/objUID/objType -> identifies the object
//    qiScope -> for QI requests that indicate if request is local or global
//    redefTime -> added for future consideration
//    epoch_ -> epoch value of current DDL lock
//    ddlOp -> indicates if release of DDL lock should be skipped.
//     (This is relevant when a DDL request that runs in multiple sub txns 
//      is performed. The DDL locks should be released when the DDL completes, 
//      not after each sub txn)
//
// TDB:  add heap support for NAString
//
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class DDLObjInfo
{
public :

  DDLObjInfo()
  : objUID_ (0),
    qiScope_(REMOVE_UNKNOWN),
    objType_(COM_UNKNOWN_OBJECT),
    redefTime_(0),
    epoch_(0),
    ddlOp_(FALSE),
    svptId_(-1)
  {}

  DDLObjInfo( NAString objName,
              Int64    objUID,
              ComObjectType objType)
  : objName_(objName),
    objUID_(objUID),
    objType_(objType),
    qiScope_(REMOVE_UNKNOWN),
    redefTime_(0),
    epoch_(0),
    ddlOp_(FALSE),
    svptId_(-1)
  {}

  NAString getObjName() { return objName_; }
  Int64 getObjUID() { return objUID_; }
  ComObjectType getObjType() { return objType_; }
  ComQiScope getQIScope() { return qiScope_; }
  Int64 getRedefTime() { return redefTime_; }
  UInt32 getEpoch() { return epoch_; }
  NABoolean isDDLOp() { return ddlOp_; }
  Int64 getSvptId() { return svptId_; }

  void setObjName (NAString objName) { objName_ = objName; }
  void setObjUID (Int64 objUID) { objUID_ = objUID; }
  void setObjType (ComObjectType objType) { objType_ = objType; }
  void setQIScope (ComQiScope qiScope) { qiScope_ = qiScope; }
  void setRedefTime (Int64 redefTime) { redefTime_ = redefTime; }
  void setEpoch (UInt32 epoch) { epoch_ = epoch; }
  void setDDLOp (NABoolean ddlOp) { ddlOp_ = ddlOp; }
  void setSvptId (Int64 svptId) { svptId_ = svptId; }
  NAString      objName_;
  Int64         objUID_;
  ComQiScope    qiScope_;
  ComObjectType objType_;
  Int64         redefTime_;
  UInt32        epoch_;
  NABoolean     ddlOp_;
  Int64         svptId_;
};

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// class DDLObjInfoList
//
// This list is used in 2 ways when a transaction completes:
//    Sends QI requests to notify all master executors of the DDL change
//    Releases DDL locks (objectEpochs)
//
// TBD:  perhaps make this an NAHashDictionary list
//
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class DDLObjInfoList : public LIST(DDLObjInfo)
{
  public:

  // constructor
  DDLObjInfoList( CollHeap *heap = NULL )
   : LIST(DDLObjInfo)(heap),
     scope_(UNKNOWN),
     heap_(heap)
  {}

  // virtual destructor
  virtual ~DDLObjInfoList(){};

  const void setHeap  (CollHeap *heap){ heap_ = heap; }
  CollHeap * getHeap() { return heap_; }

  Int32 findEntry(const NAString &objName)
    {
      for (Lng32 i = 0; i < entries(); i++)
        {
          DDLObjInfo &ddlObj = operator[](i);
          if (ddlObj.getObjName() == objName)
            return i;
        }
      return -1;
    }

  void insertEntry (DDLObjInfo &newDDLObj, NABoolean force = FALSE)
    {
      if (force || findEntry(newDDLObj.getObjName()) < 0)
        insert(newDDLObj);
    } 

  void updateObjUID (const NAString objName, Int64 value)
    {
      Int32 index = findEntry(objName);
      if (index >= 0)
        {
          DDLObjInfo &ddlObj = operator[](index);
          ddlObj.setObjUID(value);
        }
    }

#if 0
   void updateQIScope (const NAString &objName, ComQIScope value);
     {
       Int32 index = findEntry(objName);
       if (index >= 0)
        {
           DDLObjInfo &ddlObj = operator[](index);
           ddlObj.setQIScope(value);
         }
     }
#endif

  // In the future, these first two set methods will take a DDLOperation
  // object which they will place on a stack within this object, to keep
  // track of DDL operation nesting. That assumes this object (DDLObjInfoList)
  // is a singleton, which sadly it isn't today, being a member of CmpContext.
  // When we make it a singleton, then it can capture the state of DDL 
  // operation nesting.

  void beginMultiTransDDLOp(); 

  void beginTransactionalDDLOp() { /* a no-op for now */ };

  void endDDLOp(NABoolean successful,ComDiagsArea * diags);

  NABoolean scopeEnded() { return (scope_ == NOT_IN_DDL_OPERATION) || (scope_ == UNKNOWN); };

  void clearList();

  void print(const char * situation);

private:

  // Defines a scope for a DDL operation

  // The time at which we want to send QI keys, update shared cache
  // and release DDL locks depends on what kind of DDL operation is
  // in progress.

  // For example, DDL locks on transactional DDL can be released at
  // transaction abort time for DDL operations that can run in a 
  // transaction. The reason is that the DTM takes care of all rollback;
  // there is no error recovery logic needed beyond that. On the other
  // hand, DDL operations that run in multiple transactions often have
  // recovery logic that must execute after a transaction abort. We must
  // *not* release DDL locks until those are done.

  // At the moment, we are only using the multi-transactional mode; the
  // transactional mode is there for future refactoring. Over time the
  // intent is to make all DDL operations define their scope.

  enum Scope { UNKNOWN,  // temporary; when all DDL are instrumented this can go away
               NOT_IN_DDL_OPERATION,  // no DDL operation is in progress
               IN_MULTI_TRANS_DDL_OPERATION, // a DDL requiring multiple transactions is in progress
               IN_TRANSACTIONAL_DDL_OPERATION  // a DDL that can run inside a transaction is in progress
             };

  Scope scope_;

  CollHeap *heap_;

}; // class DDLObjInfoList



// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Defines an object (schema, index, or table) that was updated during a DDL
// operation.
//
// objName is the fully qualified name in external format
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class SharedCacheDDLInfo 
{
public :
  
  enum DDLOperation 
    {
       DDL_UNKNOWN = 0,
       DDL_DELETE,
       DDL_INSERT,
       DDL_UPDATE,
       DDL_DISABLE,
       DDL_ENABLE
    };

  enum CachedType {
    SHARED_DESC_CACHE = 0,
    SHARED_DATA_CACHE,
  };

  SharedCacheDDLInfo()
  : objType_(COM_UNKNOWN_OBJECT),
    ddlOperation_(DDL_UNKNOWN),
    cachedType_(SHARED_DESC_CACHE),
    isDisabled_(FALSE)
  {}

  SharedCacheDDLInfo( NAString objName,
                      ComObjectType objType, 
                      DDLOperation ddlOperation )
  : objName_(objName),
    objType_(objType),
    ddlOperation_(ddlOperation),
    cachedType_(SHARED_DESC_CACHE),
    isDisabled_(FALSE)
  {}

  SharedCacheDDLInfo( NAString objName,
                      ComObjectType objType,
                      DDLOperation ddlOperation,
                      CachedType cachedType )
  : objName_(objName),
    objType_(objType),
    ddlOperation_(ddlOperation),
    cachedType_(cachedType),
    isDisabled_(FALSE)
  {}
    
  SharedCacheDDLInfo( SharedCacheDDLInfo &otherInfo )
  {
    setObjName   (otherInfo.getObjName());
    setObjType   (otherInfo.getObjType());
    setDDLOperation (otherInfo.getDDLOperation());
    setCachedType   (otherInfo.getCacheType());
    setDisabled  (otherInfo.isDisabled());
  }

  virtual ~SharedCacheDDLInfo(void)
  {}

  const NAString getObjName()      { return objName_; }
  ComObjectType  getObjType()      { return objType_; }
  DDLOperation   getDDLOperation() { return ddlOperation_; }
  CachedType     getCacheType()    { return cachedType_; }
  NABoolean      isDisabled()      { return isDisabled_; }

  void setObjName(NAString objName)            { objName_ = objName; }
  void setObjType(ComObjectType objType)       { objType_ = objType; }
  void setDDLOperation(DDLOperation operation) { ddlOperation_ = operation; }
  void setCachedType(CachedType cacheType)     { cachedType_ = cacheType; }
  void setDisabled(NABoolean disabled)         { isDisabled_ = disabled; }

  bool isCacheDesc () { return cachedType_ == SHARED_DESC_CACHE; }
  bool isCacheData () { return cachedType_ == SHARED_DATA_CACHE; }

private:
  NAString      objName_;
  ComObjectType objType_;
  DDLOperation  ddlOperation_;
  CachedType    cachedType_;
  NABoolean     isDisabled_;

};

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// List of objects that are part of ddl operations within transactional 
// begin/commit(rollback) request.
// Used to update shared cache for DDL operations when the transaction ends.
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class SharedCacheDDLInfoList : public LIST(SharedCacheDDLInfo)
{
  public:

  // constructor
  SharedCacheDDLInfoList( NAHeap *heap = NULL )
   : LIST(SharedCacheDDLInfo)(heap),
     heap_(heap)
  {}

  // virtual destructor
  virtual ~SharedCacheDDLInfoList(){};

  const void setHeap  (NAHeap *heap){ heap_ = heap; }
  NAHeap * getHeap() { return heap_; }

  void insertEntry (SharedCacheDDLInfo &newDDLObj);

 private:

  NAHeap *heap_;

}; // class SharedCacheDDLInfoList


// Template changes for Yosemite compiler incompatible with others
typedef HASHDICTIONARY(NAString, CollIndex) CursorSelectColumns;

class CmpContext;

#define CMPCONTEXT_CLASS_NAME_LEN 7

class CmpContextInfo
{
public :

  // CmpContxt Class list
  enum CmpContextClassType
    {
      CMPCONTEXT_TYPE_UNKNOWN = -1,
      CMPCONTEXT_TYPE_NONE = 0,     // CmpContext without type
      CMPCONTEXT_TYPE_META,         // for metadata compilation
      CMPCONTEXT_TYPE_USTATS,       // for update stats query compilation
      CMPCONTEXT_TYPE_LAST
    };

  static const char * getCmpContextClassName(Int32 t)
    {
      switch(t) {
        case CMPCONTEXT_TYPE_NONE: return("NONE");
        case CMPCONTEXT_TYPE_META: return("META");
        case CMPCONTEXT_TYPE_USTATS: return("USTATS");
        default: return NULL;
        }
    }

  CmpContextInfo(CmpContext *cntxt, const char *name = 0)
  {
    if (name)
     strncpy(name_, name, CMPCONTEXT_CLASS_NAME_LEN);
    else
     strncpy(name_, "NONE", CMPCONTEXT_CLASS_NAME_LEN);
    cmpContext_ = cntxt;
    useCount_ = 0;
  }
  ~CmpContextInfo()
  {
  }

  void incrUseCount() { useCount_++; }
  void decrUseCount() { useCount_--; }

  // access methods
  Int32 getUseCount() { return useCount_; }
  CmpContext *getCmpContext() { return cmpContext_; }
  bool isSameClass (const char *name)
  {
    return !(strncmp(name_, name, CMPCONTEXT_CLASS_NAME_LEN));
  }
  const char *getName() const { return name_; }

private :
  char name_[CMPCONTEXT_CLASS_NAME_LEN]; // care upto CMPCONTEXT_CLASS_NAME_LEN
  CmpContext *cmpContext_;
  Int32 useCount_;
}; 

class ContextCli;

class CmpContext
{
public :

  enum HeapTypeEnum { CONTEXT, STATEMENT, GENERATOR };

  enum Flags	{ IS_STATIC_SQL		= 0x0,
		  IS_DYNAMIC_SQL	= 0x1,


		  // Internal Stored Procedure
		  IS_ISP		= 0x2,

		  // TRUE if Install/setup.exe is compiling the system modules:
		  // determined either by a hidden env var (CmpContext.cpp ctor)
		  // or by a hidden compiler option
		  // (cmpargs.cpp, "arkcmp -install...")
		  IS_INSTALLING		= 0x4,

		  // If TRUE, then convert static compile errors to warnings,
		  // and the statement will be recompiled at runtime,
		  // and only if that fails will it return an error.
		  // Used if a table or column doesn't exist at compile time
		  // but will be created or altered programmatically at runtime.
		  IS_IGNORE_ERR		= 0x8,

		  // TRUE if Standalone Parser (NSK Services are not available).
		  IS_STANDALONE		= 0x10,

		  // TRUE if this process is a "secondary" arkcmp,
		  // started from another arkcmp.
		  IS_SECONDARY_MXCMP    = 0x20,

                  // TRUE if we do not wish mxcmp to abort/exit on a failed 
                  // generator assert. Currently set to TRUE only for first 
                  // pass through compilation when queryCache is ON.
                  IS_DO_NOT_ABORT       = 0x40,
		  IS_EMBEDDED_ARKCMP  = 0x80,

		  IS_UNINITIALIZED_SEABASE = 0x100,

		  IS_CATALOG_SEABASE = 0x200,

		  // IS_AUTHORIZATION_ENABLED is TRUE if one or more privmgr
		  //   metadata tables exist.
		  // IS_AUTHORIZATION_READY is TRUE if all privmgr metadata
		  //   tables exist 
		  IS_AUTHORIZATION_ENABLED = 0x400,
		  IS_AUTHORIZATION_READY = 0x800,

                  // if this context was created in an mxcmp process
                  IS_MXCMP = 0x1000,

                  // if MD is created in Traf reserved namespace and not in
                  // default namespace
                  RESERVED_NAMESPACE = 0x2000,

                  // Indicate whether the compiler executes in ESP: 
                  // 1: yes, 0: no.
                  EXECUTE_IN_ESP = 0x4000,
                  //do initialize for SchemaDB
                  IN_INIT_SCH_STEP = 0x8000
		};

  CmpContext (UInt32 flags,
	      CollHeap* h = NULL
	      );
  
  // retrieve the diags
  ComDiagsArea* diags() { return diags_; }
  
  // retrieve the envs
  ProcessEnv* envs() { return envs_; }

  // get the cluster info (OSIM or global cluster info)
  NAClusterInfo *getClusterInfo() { return clusterInfo_; }
  // number of SMPs from NAClusterInfo, adjusted via CQDs
  Int32 getNumOfSMPs();

  // set or reset specific OSIM NAClusterInfo for this context
  void setCompilerClusterInfo(NAClusterInfo *nac);

  // init, get, check or set available nodes for tenant or system
  // tenant
  const NAWNodeSet * getAvailableNodes() { return availableNodes_; }
  void initAvailableNodes();
  void validateAvailableNodes();
  void setAvailableNodesForOSIM(NAWNodeSet *osimNodes);

  // set/get compiler mode (static or dynamic)
  CompilationMode GetMode()              { return mode_; }
  void SetMode(CompilationMode mode)     { mode_ = mode; }

  // retrieve the ofstream
  ofstream* outFstream() { return outFstream_; }
  void setOutfstream(ofstream* o) { outFstream_ = o; }

  // Some global flags
  NABoolean isDynamicSQL() const { return flags_ & IS_DYNAMIC_SQL; }
  NABoolean isISP()	 const { return flags_ & IS_ISP; }
  NABoolean isInstalling() const { return flags_ & IS_INSTALLING; }
  NABoolean ignoreErrors() const { return flags_ & IS_IGNORE_ERR; }
  NABoolean isStandalone() const { return flags_ & IS_STANDALONE; }
  NABoolean isMxcmp() const { return flags_ & IS_MXCMP; }
  NABoolean isSecondaryMxcmp() const { return flags_ & IS_SECONDARY_MXCMP; }
  NABoolean isEmbeddedArkcmp() const { return flags_ & IS_EMBEDDED_ARKCMP;}
  NABoolean isUninitializedSeabase() const { return flags_ & IS_UNINITIALIZED_SEABASE;}
  NABoolean isCatalogSeabase() const { return flags_ & IS_CATALOG_SEABASE;}
  NABoolean isAuthorizationEnabled(NABoolean errIfNotReady = TRUE); 
  NABoolean isAuthorizationReady() const { return flags_ & IS_AUTHORIZATION_READY; }
  NABoolean isRuntimeCompile() const { return isRuntimeCompile_; }
  const NABoolean isDoNotAbort() const { return flags_ & IS_DO_NOT_ABORT; }
  Int16 getRecursionLevel() { return recursionLevel_;}
  void incrRecursionLevel() {recursionLevel_++;}
  void decrRecursionLevel() { recursionLevel_--;}
  //set the flag indicating if this a primary or a secondary mxcmp.
  //A secondary mxcmp is one that has been spawned by another mxcmp process.
  //a call to this method is made in ExCmpMessage::actOnReceive
  NABoolean isInStepForInitSchemaDB() const { return flags_ & IN_INIT_SCH_STEP; }
  void setSecondaryMxcmp();

  void setAuthorizationState (Int32 state);

  void setDoNotAbort(NABoolean v)
  {
    (v ? flags_ |= IS_DO_NOT_ABORT : flags_ &= ~IS_DO_NOT_ABORT);
  }

  void setIsEmbeddedArkcmp(NABoolean v)
  {
    (v ? flags_ |= IS_EMBEDDED_ARKCMP : flags_ &= ~IS_EMBEDDED_ARKCMP);
  }

  void setIsUninitializedSeabase(NABoolean v)
  {
    (v ? flags_ |= IS_UNINITIALIZED_SEABASE : flags_ &= ~IS_UNINITIALIZED_SEABASE);
  }

  void setIsCatalogSeabase(NABoolean v)
  {
    (v ? flags_ |= IS_CATALOG_SEABASE : flags_ &= ~IS_CATALOG_SEABASE);
  }

  void setIsAuthorizationEnabled(NABoolean v)
  {
    (v ? flags_ |= IS_AUTHORIZATION_ENABLED : flags_ &= ~IS_AUTHORIZATION_ENABLED);
  }
  
  void setIsAuthorizationReady(NABoolean v)
  {
    (v ? flags_ |= IS_AUTHORIZATION_READY : flags_ &= ~IS_AUTHORIZATION_READY);
  }
  
  void setUseReservedNamespace(NABoolean v)
  {
    (v ? flags_ |= RESERVED_NAMESPACE : flags_ &= ~RESERVED_NAMESPACE);
  }
  NABoolean useReservedNamespace() const 
  {
    return (flags_ & RESERVED_NAMESPACE) != 0;
  }
  NABoolean isExecuteInESP() const { return flags_ & EXECUTE_IN_ESP;}
  void setExecuteInESP(NABoolean v) 
  {
    (v ? flags_ |= EXECUTE_IN_ESP: flags_ &= ~EXECUTE_IN_ESP);
  }

  void setInInitSchemaDBStep(NABoolean v)
  {
    (v ? flags_ |= IN_INIT_SCH_STEP : flags_ &= ~IN_INIT_SCH_STEP);
  }

  UInt32 getStatementNum() const { return statementNum_; }

  // access the NAHeap* for context
  NAHeap* statementHeap();
  NAHeap* heap() { return heap_; }

  // Initialization at beginning of each context (ie. user session)
  NABoolean initContextGlobals();

  // Initialization at the beginning of each statement
  void init();

  // clean up globals at the end of each statement. 
  void cleanup(NABoolean exception=TRUE);

  // optimizer globals

  // SchemaDB and initialization procedure at the beginning of each statement
  SchemaDB* schemaDB_;
  void initSchemaDB();

  // table identifier representing each table, easier to hash on.
  CollIndex getTableIdent ()  { return tableIdent_; }
  void incrementTableIdent () { tableIdent_++; }

  ControlDB* controlDB_;		      
  void initControlDB();
  ControlDB* getControlDB() { return controlDB_; }


  // get the current CmpStatement
  CmpStatement* statement() { return currentStatementPtrCache_; }
  void setStatement(CmpStatement* s);
  void unsetStatement(CmpStatement* s, NABoolean exceptionRaised=FALSE);
  NAList<CmpStatement*>& statements() { return statements_; }

  // switch the currentStatement_ to s, for ISP execution
  void setCurrentStatement(CmpStatement* s);

  // By default, isRuntimeCompile_ is true, but StaticCompiler::processMain
  // will call this method to set it to false.
  void setIsNotRuntimeCompile() { isRuntimeCompile_ = FALSE; }  
  NABoolean showQueryStats() { return showQueryStats_; }
  void setShowQueryStats() { showQueryStats_ = TRUE; }
  void resetShowQueryStats() { showQueryStats_ = FALSE; }

  virtual ~CmpContext();

  enum InternalCompileEnum
  { NOT_INTERNAL_COMPILE = FALSE,	// a user module
    INTERNAL_MDF,			// transient state in StaticCompiler.cpp
    INTERNAL_MODULENAME,		// a known system module
    INTERNAL_MODULEPREFIX		// a reserved (unused) system mod name
  };
  InternalCompileEnum &internalCompile() { return internalCompile_; }

  // for static compilations, need to remember declared cursors and
  // how many columns are to be retrieved in order to verify that the
  // allocated static output descriptor has the equivalent number of
  // host variables to receive the information.
  // PUBLIC FOR NOW
  CursorSelectColumns staticCursors_;
  CollIndex saveRetrievedCols_;

  // get/set storage for SQLMX_REGRESS environment variable
  Int32 getSqlmxRegress() const { return sqlmxRegress_; }
  void setSqlmxRegress(Int32 regressEnvVar) { sqlmxRegress_ = regressEnvVar; }

  CmpSqlSession * sqlSession() { return sqlSession_; }

  POSInfo * posInfo() { return posInfo_; }
  void setPOSInfo(POSInfo * pi) { posInfo_ = pi; }


  // Functions to handle reserved memory in case an out-of-memory situation
  // occurs and a response needs to be sent to MXCI or MXOSRVR.
  void reserveMemory();
  void freeReservedMemory();

  Int32 gmtDiff() { return gmtDiff_; }
  const char *getCompilerId() const { return compilerId_; }

  Lng32 &uninitializedSeabaseErrNum() { return uninitializedSeabaseErrNum_;}
  Lng32 &hbaseErrNum() { return hbaseErrNum_;}
  NAString &hbaseErrStr() { return hbaseErrStr_;}

  void switchContext();
  void switchBackContext();
  void resetContext();

  Int32
  compileDirect(char *data, UInt32 dataLen, CollHeap *outHeap, Int32 charset,
                CmpMessageObj::MessageTypeEnum op, char *&gen_code,
                UInt32 &gen_code_len, UInt32 parserFlags,
                const char *parentQid, Int32 parentQidLen,
                ComDiagsArea *&diagsArea, 
                NABoolean needToDoWork = TRUE);

  // set/reset an env in compiler envs
  void setArkcmpEnvDirect(const char *name, const char *value,
                          NABoolean unset);

  // used by sendAllControlsAndFlags() and restoreAllControlsAndFlags()
  NABoolean isSACDone() { return sacDone_; }
  void setSACDone(NABoolean v) { sacDone_ = v; }

  NABoolean getParserResetIsNeeded() { return parserResetIsNeeded_ ; }
  void      setParserResetIsNeeded( NABoolean resetIsNeeded )
                                  { parserResetIsNeeded_ = resetIsNeeded ; }

  TimeVal getPrev_QI_time() { return prev_QI_invalidation_time_ ; }
  void    setPrev_QI_time( TimeVal newTime )
                            { prev_QI_invalidation_time_ = newTime ; }

  Lng32 getPrev_QI_sec() { return prev_QI_invalidation_time_.tv_sec ; }

  void  setLogmxEventSqlText(const NAWString& x);
  void  resetLogmxEventSqlText();

// MV
/*
  void setQCache(CmpQCache *cache)
  { qCache_ = cache; }
  CmpQCache *getQCache() { return qCache_; }
*/
  QueryCache* getQueryCache() { return qcache_; }

  SchemaDB* getSchemaDB() {return schemaDB_;}

  char* getTMFUDF_DLL_InterfaceHostDataBuffer() 
    { return tmfudf_dll_interface_host_data_; };

  ULng32 getTMFUDF_DLL_InterfaceHostDataBufferLen();

  CompilerTrackingInfo* getCompilerTrackingInfo();

  OptDebug* getOptDbg() { return optDbg_; }

  TransMode& getTransMode() { return transMode_; }

  RuleSet *getRuleSet() { return ruleSet_; }

  // context histogram cache
  HistogramCache* getHistogramCache() { return histogramCache_; }
  // Global pointer to the OptimizerSimulator that encapsulates
  // all of the OSIM related information.
  OptimizerSimulator* & getOptimizerSimulator()  { return optSimulator_; }
  // used by stats caching logic
  Int64 getLastUpdateStatsTime() { return lastUpdateStatsTime_; }
  void setLastUpdateStatsTime(Int64 updateTime) { lastUpdateStatsTime_ = updateTime; }


  // optimizer cached defaults
  OptDefaults* getOptDefaults() { return optDefaults_; }

  MDDescsInfo *getTrafMDDescsInfo() { return trafMDDescsInfo_; }

  void setCIClass(CmpContextInfo::CmpContextClassType x) { ciClass_ = x; }
  CmpContextInfo::CmpContextClassType getCIClass() { return ciClass_; }

  void setCIindex(Int32 x) { ciIndex_ = x; }
  Int32 getCIindex() { return ciIndex_; }

  CollationDBList *getCollationDBList() { return CDBList_; }

  void addInvocationInfo(tmudr::UDRInvocationInfo *ii)
                                { invocationInfos_.insert(ii); }
  void addPlanInfo(tmudr::UDRPlanInfo *pi)
                                      { planInfos_.insert(pi); }
  void addRoutineHandle(Int32 rh)
                                 { routineHandles_.insert(rh); }

  DDLObjInfoList& ddlObjsList() { return ddlObjs_; }
  DDLObjInfoList& ddlObjsInSPList() { return ddlObjsInSP_; }
  SharedCacheDDLInfoList& sharedCacheDDLInfoList() { return sharedCacheDDLInfoList_; }

  void releaseAllDDLObjectLocks();
  void releaseAllDMLObjectLocks();
  void releaseAllObjectLocks()
  {
    releaseAllDMLObjectLocks();
    releaseAllDDLObjectLocks();
  }

  void clearAllCaches();

  void resetAllCaches(NABoolean resetHistogramCache = FALSE);

  static void initGlobalNADefaultsEntries();

  void setLastSqlStmt(char* str, UInt32 len);

  void setCliContext(ContextCli* x) { cliContext_ = x; }
  ContextCli* getCliContext() { return cliContext_; }

  NATable* getNATableFromFirstMetaCI(const ExtendedQualName& name);

  NABoolean isConnectByDual() {return isConnectByDual_; }
  void setConnectByDual() { isConnectByDual_ = TRUE; }
  void resetConnectByDual() { isConnectByDual_ = FALSE; }
 
  CNATestPointArray * getTestPointArray() { return testPointArray_; } // might be NULL

  CNATestPointArray * getOrCreateTestPointArray();  // creates it if presently NULL

  Int32 executeTestPoint(Int32 testPoint);

  int getNeedsRetryWithCachingOff() { return needsRetryWithCachingOff_; }

  void setNeedsRetryWithCachingOff(NABoolean n) { needsRetryWithCachingOff_ = n; }

  int getSeqNumForCacheKey() { return seqNumForCacheKey_; }

  void setSeqNumForCacheKey(int x) { seqNumForCacheKey_ = x; }

  int getFilterForQueryCacheHDFS() { return filterForQueryCacheHDFS_; }



  Int64 getObjUIDForQueryCacheHDFS() { return objUIDForQueryCacheHDFS_; }

  void setObjUIDForQueryCacheHDFS(Int64 x) { objUIDForQueryCacheHDFS_ = x; }

  //TableDesc* getTableDescForCacheKey() { return tableDescForCacheKey_; }

  //void setTableDescForCacheKey(TableDesc* t) { tableDescForCacheKey_ = t; }

  //NABoolean getFilterForQueryCache() { return filterForQueryCache_; }

  //void setFilterForQueryCache(NABoolean n) { filterForQueryCache_ = n; }

// MV
private:
// Adding support for multi threaded requestor (multi transactions) handling
// in the arkcmp
// This is needed because arkcmp is also serves as a server for the utilities
// store procedures

  void  OpenTMFFile();
  void  CloseTMFFile();
  short GetTMFFileNumber() const { return tmpFileNumber_; }
  void swithcContext();
  CmpStatementISP* getISPStatement(Int64 id);
// MV

private:

  CmpContext(const CmpContext &);
  CmpContext& operator=(const CmpContext &);

  // arkcmp internal members.

  ComDiagsArea *diags_;
  ProcessEnv *envs_;
  UInt32 flags_;
  NAHeap* heap_;
  ofstream* outFstream_;

  int seqNumForCacheKey_;
  // initialize to 0, 1 for should write to HDFS, -1 for should not write to HDFS
  int filterForQueryCacheHDFS_;
  Int64 objUIDForQueryCacheHDFS_;
  NABoolean needsRetryWithCachingOff_;

  //this var indicates if the status that the mxcmp
  //is a secondary process (i.e. has been spawned by
  //another mxcmp process) or a primary process (i.e.
  //has not been spawned by another mxcmp) has been
  //set
  NABoolean mxcmpPrimarySecondaryStatusSet_;
  // CmpStatements, this is a stack of CmpStatement, 
  // for there might be nested statements in the future.
  // const static long maxNoOfCmpStatements_ = 256; should use this instead of
  // define, when the compiler supports const static.

  NAList<CmpStatement*> statements_;
  Lng32 currentStatement_;

  // For performance reason, cached a pointer to the current statement.
  CmpStatement *currentStatementPtrCache_;

  // This enum, if not FALSE, indicates that this static compile
  // is being done on an 
  // - 'internal' mdf file (weak condition based on mdf file name only), or
  // - 'internal' module (strong condition based on known, trusted module name
  //   in one of Tandem's internal mdf's).
  // Examples of internal mdf files are those that catman uses, or rfork, etc.
  // See methods isInternalMdf and isInternalModName in arkcmp/StaticCompiler.C.
  InternalCompileEnum internalCompile_;

  // Node and Disk autonomy must distinguish run-time compiles.
  NABoolean isRuntimeCompile_;

//MV

  short tmpFileNumber_;
//MV

  // Short to store environment variable SQLMX_REGRESS
  Int32 sqlmxRegress_;

  // SQL session. Created when CmpContext is constructed.
  // Values are added or removed from it based on user session
  // maintaned in master executor(cli).
  // These values (session id, volatile schema name, ets..) are either
  // sent by master executor or internally added (or removed). 
  CmpSqlSession* sqlSession_;		      

  // The reservedMemory pointer points to memory in the context heap
  // that is freed when some out-of-memory situations occur. Without
  // freeing memory in the context heap, it may be impossible to
  // report an error. This can cause MXCMP to get stuck in a loop
  // trying to report the error.
  void *reservedMemory_;

  // system POS info. Current set and used by bulk replication.
  POSInfo * posInfo_;


  NABoolean showQueryStats_;

  // difference between gmt and local time in minutes. Set once when arkcmp
  // is started.
  Int32 gmtDiff_;

  // process start time, node num, pin, segment num on Seaquest.
  char compilerId_[COMPILER_ID_LEN];
  Int16 recursionLevel_;
  Lng32 uninitializedSeabaseErrNum_;
  // underlying hbase error and detail info, if returned.
  // valid when uninitializedSeabaseErrNum_ is set.
  Lng32 hbaseErrNum_; 
  NAString hbaseErrStr_;
  // NAClusterInfo, this either points to the CLI globals or
  // is a special object built from OSIM information
  NAClusterInfo *clusterInfo_;
  NABoolean iOwnClusterInfo_;
  // Available nodes for this context, from a tenant or for Adaptive
  // Segmentation, or just all nodes of the cluster. availableNodes_
  // and availableNodesFromOSIM_ are owned by this context,
  // availableNodesFromCli_ is not.
  // available nodes for use, with affinity computed, if needed
  NAWNodeSet *availableNodes_;
  // this is used to check for tenant changes in the CLI
  const NAWNodeSet *availableNodesFromCLI_;
  // this is used for simulating tenants in OSIM
  const NAWNodeSet *availableNodesFromOSIM_;
  // this is used to check for changes in the AS_AFFINITY_VALUE CQD
  int previousAffinity_;
  RuleSet *ruleSet_;
  OptDebug *optDbg_;
  CmpMemoryMonitor *cmpMemMonitor_;
  OptimizerSimulator *optSimulator_;
  HistogramCache *histogramCache_;

  QueryCache* qcache_;

  // table identifier representing each table, easier to hash on.
  CollIndex tableIdent_;

  char* tmfudf_dll_interface_host_data_;

  // compiler mode (static or dynamic)
  CompilationMode mode_;
  // compiler tracking information 
  CompilerTrackingInfo *compilerTrackingInfo_;

  NABoolean parserResetIsNeeded_ ; // Used by sqlcomp/parser.cpp
  TimeVal prev_QI_invalidation_time_ ; // Used by sqlcomp/CmpMain.cpp

  NAWString* sqlTextBuf_ ; //Used by logmxevent_sq.cpp

  TransMode transMode_; 

  Int64 lastUpdateStatsTime_; // used by stats caching logic

  // query defaults using during a statement compilation
  OptDefaults* optDefaults_;

  MDDescsInfo * trafMDDescsInfo_;

  CmpContextInfo::CmpContextClassType ciClass_;
  Int32 ciIndex_;

  CollationDBList *CDBList_;

  // objects allocated from the system heap, to be deleted
  // after each statement has finished compiling
  LIST(tmudr::UDRInvocationInfo *) invocationInfos_;
  LIST(tmudr::UDRPlanInfo *)       planInfos_;
  LIST(Int32)                      routineHandles_;

  // if CmpSeabaseDDL::sendAllControlsAndFlags() has sent controls.
  NABoolean sacDone_; //

  // Used to keep track of objects that were part of ddl operations within
  // a transactional begin/commit(rollback) session.
  // Used at commit time for NATable cache invalidation.
  DDLObjInfoList ddlObjs_;

  // Used to keep track of objects that were part of ddl operations within
  // a SAVEPOINT begin/commit(rollback) session.
  // Used at commit time for NATable cache invalidation.
  DDLObjInfoList ddlObjsInSP_;

  // Used to keep track of objects that were disable during DDL operations.
  // At commit or rollback time, these objects need to be either enabled or delete.
  SharedCacheDDLInfoList sharedCacheDDLInfoList_;

 // a count of how many statements have been compiled
  UInt32 statementNum_;



  // instrumentation for mantis 9407
  char lastSqlStmt_[200];

  NABoolean isConnectByDual_;

  ContextCli* cliContext_;

  CNATestPointArray * testPointArray_;

public:
  static NABoolean useReservedNameSpace_;
  static Int32 authorizationState_;
}; // end of CmpContext 

static inline CmpContext::InternalCompileEnum &InternalCompile() 
{ return cmpCurrentContext->internalCompile(); }

#endif
