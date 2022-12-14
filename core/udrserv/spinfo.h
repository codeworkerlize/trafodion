
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         spinfo.h
 * Description:  The SPInfo and SPList Classes
 *               Used to store context information for stored procedures
 *               between request messages for an SP.  SPList is an iterator
 *               class.
 *
 * Created:      01/01/2001
 * Language:     C++
 *
 *****************************************************************************
 */

#ifndef SPINFO_H
#define SPINFO_H

#include "executor/UdrExeIpc.h"
#include "cli/sqlcli.h"
#include "comexe/QueueIndex.h"
#include "common/Collections.h"  // needed for NAList
#include "common/ComSmallDefs.h"
#include "common/NABoolean.h"
#include "common/Platform.h"
#include "sqludr/sqludr.h"
#include "udrdefs.h"
#include "udrextrn.h"

//
// Forward declarations
struct UdrParameterInfo;
class UdrServerDataStream;
class UdrServerControlStream;
class UdrServerReplyStream;
class UdrGlobals;
class UdrResultSet;
class LmRoutine;
class LmParameter;
class LmTableInfo;
class UdrImOkReply;
class ComDiagsArea;
class ControlInfo;
struct UdrTableInputInfo;

//
// -----------------------------------------------------------------------
// SPInfo
//
// SPInfo will likely be a subclass of a future UDRInfo class
//
// -----------------------------------------------------------------------
class SPInfo : public NABasicObject {
 public:
  // ---------------------------------------------------------------------
  // Constructor/Destructor.
  // ---------------------------------------------------------------------
  SPInfo(UdrGlobals *udrGlobals, NAHeap *heapPtr, const UdrHandle &udrHandle, char *pSqlName, char *pExternalName,
         char *pRoutineSig, char *pContainerName, char *pExternalPathName, char *pLibrarySqlName, ComUInt32 pNumParams,
         ComUInt32 pNumInParams, ComUInt32 pNumOutParams, ComUInt32 pMaxRSets,
         ComRoutineTransactionAttributes transactionAttrs, ComRoutineSQLAccess psqlAccessMode,
         ComRoutineLanguage pLanguage, ComRoutineParamStyle paramStyle, NABoolean pIsolate, NABoolean pCallOnNull,
         NABoolean pExtraCall, NABoolean pDeterministic, ComRoutineExternalSecurity pExternalSecurity,
         int pRoutineOwnerId, ComUInt32 requestBufferSize, ComUInt32 replyBufferSize, ComUInt32 requestRowSize,
         ComUInt32 replyRowSize, ComDiagsArea &d, char *parentQid, char *clientInfo);

  //
  // This constructor is used to create an uninitialized instance that
  // will not be used for UDR invocations. Its only purpose is to
  // store information about out-of-sequence UNLOAD requests.
  //
  SPInfo(UdrGlobals *udrGlobals, NAHeap *heapPtr, const UdrHandle &udrHandle);

  ~SPInfo();

  NABoolean setUdrContext(ComDiagsArea &d) const;

  //
  // Each instance has a state_ field that should be verified before
  // operating on the instance.
  //
  // Note: The UNLOADING state is used when an UNLOAD message arrives
  // out-of-sequence. To handle this we create a dummy SPInfo instance
  // in the UNLOADING state and delete that instance when the LOAD
  // arrives later.
  //
  enum SPInfoState {
    INITIAL = 1,
    LOADED,
    LOAD_FAILED,
    INVOKED,
    INVOKE_FAILED,
    INVOKED_EMITROWS,        // TMUDF Specific, set when emitting rows to tcb
    INVOKED_GETROWS,         // TMUDF specific, set when waiting for rows from tcb
    INVOKED_GETROWS_FAILED,  // TMUDF specific.
    UNLOADING
  };

  SPInfoState getSPInfoState() const { return spInfoState_; }
  void setSPInfoState(SPInfoState s) { spInfoState_ = s; }
  const char *getSPInfoStateString() const;

  // ---------------------------------------------------------------------
  // Accessors
  // ---------------------------------------------------------------------

  UdrGlobals *getUdrGlobals() const { return udrGlobals_; }
  const UdrHandle &getUdrHandle() const { return udrHandle_; }
  LmRoutine *getLMHandle() const { return lmHandle_; }
  NABoolean isLoaded() const { return spInfoState_ == LOADED; }
  NABoolean isLoadFailed() const { return spInfoState_ == LOAD_FAILED; }
  NABoolean isInvoked() const { return spInfoState_ == INVOKED; }
  NABoolean isInvokeFailed() const { return spInfoState_ == INVOKE_FAILED; }
  ComRoutineTransactionAttributes getTransactionAttrs() const { return transactionAttrs_; }
  ComRoutineSQLAccess getSQLAccessMode() const { return sqlAccessMode_; }
  ComUInt32 getNumParameters() const { return numParameters_; }
  ComUInt32 getNumTables() const { return numTableInfo_; }
  queue_index getParentIndex() { return parentIndex_; }

  const char *getSqlName() const { return sqlName_; }
  const char *getExternalName() const { return externalName_; }
  const char *getRoutineSig() const { return routineSig_; }
  const char *getContainerName() const { return containerName_; }
  const char *getExternalPathName() const { return externalPathName_; }
  const char *getLibrarySqlName() const { return librarySqlName_; }
  const char *getParentQid() const { return parentQid_; }
  const char *getClientInfo() const { return clientInfo_; }
  long getNumCalls() const { return numCalls_; }
  long getLastCallTs() const { return lastCallTs_; }

  ComUInt32 getMaxNumResultSets() const { return maxNumResultSets_; }
  ComUInt32 getNumResultSets() const { return numResultSets_; }

  ComUInt32 getNumInParameters() const { return numInParams_; }
  ComUInt32 getNumOutParameters() const { return numOutParams_; }

  LmParameter &getLmParameter(ComUInt32 i);
  LmParameter *getLmParameters() { return lmParameters_; }
  LmParameter *getReturnValue() { return returnValue_; }

  LmTableInfo *getLmTables() { return tableInfo_; }
  SqlBuffer *getReqSqlBuffer(ComSInt32 tableIndex);
  SqlBuffer *getEmitSqlBuffer(ComSInt32 tableIndex);
  ComUInt32 getInputRowLength(ComSInt32 tableIndex);
  NABoolean isLastReqSqlBuffer(ComSInt32 tableIndex);

  ComRoutineLanguage getLanguage() const { return language_; }
  ComRoutineParamStyle getParamStyle() const { return paramStyle_; }
  NABoolean getIsolate() const { return isolate_; }
  NABoolean getCallOnNull() const { return callOnNull_; }
  NABoolean getExtraCall() const { return extraCall_; }
  NABoolean getDeterministic() const { return deterministic_; }
  ComRoutineExternalSecurity getExternalSecurity() const { return externalSecurity_; }
  int getRoutineOwnerId() const { return routineOwnerId_; }

  UdrServerReplyStream *getTxStream() const { return txStream_; }
  UdrServerDataStream *getDataStream() const { return dataStream_; }
  ComUInt32 getRequestBufferSize() const { return requestBufferSize_; }
  ComUInt32 getReplyBufferSize() const { return replyBufferSize_; }

  ComUInt32 getRequestRowSize() const { return requestRowSize_; }
  ComUInt32 getReplyRowSize() const { return replyRowSize_; }

  // Access to result set object with RS handle
  UdrResultSet *getUdrResultSetByHandle(RSHandle handle);

  // Access to result set object with index
  UdrResultSet *getUdrResultSetByIndex(ComUInt32 index) {
    UDR_ASSERT(index < rsList_.entries(), "An invalid index was passed to SPInfo::getUdrResultSetByIndex()");

    return rsList_[index];
  }

  // ---------------------------------------------------------------------
  // Mutators
  // ---------------------------------------------------------------------
  inline void setUdrHandle(UdrHandle t) { udrHandle_ = t; }
  inline void setLMHandle(LmRoutine *t) { lmHandle_ = t; }
  inline void setTransactionAttrs(ComRoutineTransactionAttributes t) { transactionAttrs_ = t; }
  inline void setSQLAccessMode(ComRoutineSQLAccess t) { sqlAccessMode_ = t; }
  inline void setNumParameters(ComUInt32 t) { numParameters_ = t; }

  void setNumTableInfo(ComUInt32 t);
  void setTableInputInfo(const UdrTableInputInfo info[]);
  void setReqSqlBufferCopy(SqlBuffer *sqlBufCopy, ComSInt32 tableIndex);
  void setParentIndex(queue_index idx) { parentIndex_ = idx; }
  void deleteReqSqlBuffer(ComSInt32 tableIndex);
  void setLastReqSqlBuffer(ComSInt32 tableIndex);
  void setEmitSqlBuffer(SqlBuffer *buf, ComSInt32 tableIndex);
  void reset(void);

  void resetLastCallTs();

  inline void setNumCalls(long t) { numCalls_ = t; }
  inline void setLastCallTs(long t) { lastCallTs_ = t; }
  inline void setMaxNumResultSets(ComUInt32 t) { maxNumResultSets_ = t; }
  inline void setNumResultSets(ComUInt32 t) { numResultSets_ = t; }

  void setInParam(ComUInt32 i, const UdrParameterInfo &info);
  void setOutParam(ComUInt32 i, const UdrParameterInfo &info);

  inline void setLanguage(ComRoutineLanguage t) { language_ = t; }
  inline void setParamStyle(ComRoutineParamStyle t) { paramStyle_ = t; }
  inline void setIsolate(NABoolean t) { isolate_ = t; }
  inline void setCallOnNull(NABoolean t) { callOnNull_ = t; }
  inline void setExtraCall(NABoolean t) { extraCall_ = t; }
  inline void setDeterministic(NABoolean t) { deterministic_ = t; }
  inline void setExternalSecurity(ComRoutineExternalSecurity t) { externalSecurity_ = t; }
  inline void setRoutineOwnerId(int t) { routineOwnerId_ = t; }

  NABoolean activateTransaction();
  void replyToEnterTxMsg(NABoolean doneWithRS = FALSE);

  void prepareToReply(UdrServerReplyStream &msgStream);
  void prepareToReply(UdrServerDataStream &msgStream);

  // ---------------------------------------------------------------------
  // General Methods.
  // ---------------------------------------------------------------------

  // Detail display of SPInfo data structures...
  void displaySPInfo(int indent);
  // ID only display of SPInfo data structures...
  void displaySPInfoId(int indent);

  // support methods
  long createUniqueIdentifier();

  int releaseSP(NABoolean reportErrors, ComDiagsArea &d);

  // Result Set related methods

  // setupUdrResultSets() initializes UdrResultSet objects.
  NABoolean setupUdrResultSets(ComDiagsArea &d);

  // loadUdrResultSet() sets some of UdrResultSet fields that
  // get values from master executor
  void loadUdrResultSet(ComUInt32 index, RSHandle handle, ComUInt32 numRSCols, ComUInt32 rowSize, ComUInt32 bufferSize,
                        UdrParameterInfo *columnDesc, ComDiagsArea &d);

  // prepareForReinvoke() resets UdrResultSet objects for
  // a new UDR invocation.
  void prepareForReinvoke(ComDiagsArea *diags);

  void work();
  void workTM();

  void setCurrentRequest(UdrDataBuffer *request) { currentRequest_ = request; }
  UdrDataBuffer *getCurrentRequest() const { return currentRequest_; }

 private:
  RequestRowProcessingStatus processOneRequestRow(SqlBuffer *reqSqlBuf, SqlBuffer *replySqlBuf, int &numRowsProcessed);

  NABoolean moveRSInfoIntoStream();
  void moveDiagsIntoStream(ComDiagsArea *diags, ControlInfo *replyControlInfo);

  void reportInvokeInParameters();

  SPInfo();

  SQLSTMT_ID *executeSqlStmt(const char *sql_str, ComDiagsArea &d);

  void quiesceExecutor();

  void initLmParameter(const UdrParameterInfo &pInfo, NABoolean isInput);

  void assignStringMember(char *&memberBuff, const char *const src);

  // Data members follow...
  char eyeCatcher_[4];
  UdrGlobals *udrGlobals_;
  UdrHandle udrHandle_;
  LmRoutine *lmHandle_;
  SPInfoState spInfoState_;
  ComRoutineTransactionAttributes transactionAttrs_;
  ComRoutineSQLAccess sqlAccessMode_;

  ComUInt32 numParameters_;  // current number of parameters
  ComUInt32 numInParams_;    // Num IN/INOUT params
  ComUInt32 numOutParams_;   // Num OUT/INOUT params

  char *sqlName_;           // ANSI name
  char *externalName_;      // Java method or DLL function name
  char *routineSig_;        // Java Routine Signature of SPJ
  char *containerName_;     // Container name of SPJ (class file)
  char *externalPathName_;  // Directory name of SPJ class file
  char *librarySqlName_;    // ANSI name of library (JAR/DLL)

  // LRU attributes of this SP to allow memory recovery
  long numCalls_;    // number of invoke calls
  long lastCallTs_;  // time of last call

  ComRoutineLanguage language_;
  ComRoutineParamStyle paramStyle_;

  NABoolean isolate_;
  NABoolean callOnNull_;
  NABoolean extraCall_;
  NABoolean deterministic_;

  // Definer Rights related fields
  ComRoutineExternalSecurity externalSecurity_;
  int routineOwnerId_;

  ComUInt32 maxNumResultSets_;
  ComUInt32 numResultSets_;

  // Space for LM version of parameters & return value.
  // returnValue_ is not used in the product. It will be useful
  // when we support functions.
  LmParameter *lmParameters_;
  LmParameter *returnValue_;

  ComUInt32 requestBufferSize_;
  ComUInt32 replyBufferSize_;
  ComUInt32 requestRowSize_;
  ComUInt32 replyRowSize_;

  UdrServerDataStream *dataStream_;

  // Result Set information
  NAList<UdrResultSet *> rsList_;  // List of UdrResultSet objects

  NAHeap *udrHeapPtr_;

  // TM descriptors and related variables
  ComUInt32 numTableInfo_;
  LmTableInfo *tableInfo_;
  SqlBuffer *sqlBufferScalar_;
  SqlBuffer *sqlBufferTVF_;
  queue_index parentIndex_;

  // IPC stream to save Enter TX message for replying later
  UdrServerReplyStream *txStream_;
  UdrDataBuffer *currentRequest_;

  ComDiagsArea *rowDiags_;

  char *parentQid_;  // Query Id of the CALL Statement
  char *clientInfo_;

};  // SPInfo

// -----------------------------------------------------------------------
// SPList
// -----------------------------------------------------------------------
class SPList {
 public:
  SPList(UdrGlobals *udrGlobals);
  ~SPList(void){};

  void displaySPList(int indent);
  void displaySPListId(int indent);

  ComUInt32 entries() { return (ComUInt32)spInfoElement_.entries(); }
  SPInfo *getSpInfo(ComUInt32 index) { return spInfoElement_.at(index); }

  SPInfo *spFind(const UdrHandle &spId);

  void releaseOldestSPJ(ComDiagsArea &d);

  void addToSPList(SPInfo *spinfo);
  void removeFromSPList(SPInfo *spinfo);

 private:
  char eyeCatcher_[4];

  UdrGlobals *udrGlobals_;
  NAList<SPInfo *> spInfoElement_;
};

#endif
