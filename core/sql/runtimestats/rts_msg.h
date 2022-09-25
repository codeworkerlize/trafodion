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
//
// File:         rtsSSCPIpc.h
// Description:  Message objects shared by the SQL/MX exectuor and 
//               runtime stats
// Created:      04/27/2006
**********************************************************************/
#ifndef _RTS_MSG_H_
#define _RTS_MSG_H_

#include "Ipc.h"
#include "ComSmallDefs.h"
#include "ComCextdecs.h"
#include "Int64.h"
#include "ExpLOBenums.h"
#include <stdio.h>

#include "sqlcli.h"
#include "ComRtUtils.h"
//
// Forward class references
//
//
// Version numbers
//
#ifndef USE_SB_NEW_RI
#define RTS_STATS_MSG_BUF_SIZE 32000  // same as IpcDefGuaMaxMsgIOSize in Ipc.h
#endif

const Int32 CurrSsmpRequestMessageVersion = 100;
const Int32 CurrSsmpReplyMessageVersion = 100;
const Int32 CurrSscpRequestMessageVersion = 100;
const Int32 CurrSscpReplyMessageVersion = 100;
const Int32 CurrRmsReplyMessageVersion = 100;

const Int32 currRtsQueryIdVersionNumber = 102;
const Int32 currRtsStatsReqVersionNumber = 101 ;
const Int32 currRtsStatsReplyVersionNumber = 101 ;
const Int32 currRtsCpuStatsReqVersionNumber = 100 ;
const Int32 currRtsExplainFragVersionNumber = 100;
const Int32 currRtsExplainReqVersionNumber = 100;
const Int32 currRtsExplainReplyVersionNumber = 100;

const Int32 CurrQueryStartedReqVersionNumber = 100;
const Int32 CurrQueryStartedReplyVersionNumber = 101;
const Int32 CurrQueryFinishedReqVersionNumber = 100;
const Int32 CurrCancelQueryReqVersionNumber = 101;
const Int32 CurrControlQueryReplyVersionNumber = 100;
const Int32 CurrKillServersReqVersionNumber = 100;
const Int32 CurrKillServersReplyVersionNumber = 100;
const Int32 CurrSuspendQueryReqVersionNumber = 100;
const Int32 CurrSuspendQueryReplyVersionNumber = 100;
const Int32 CurrActivateQueryReqVersionNumber = 100;
const Int32 CurrActivateQueryReplyVersionNumber = 100;
const Int32 CurrSecurityInvalidKeyVersionNumber = 100;
const Int32 CurrSnapshotLockVersionNumber = 100;
const Int32 CurrSnapshotUnLockVersionNumber = 100;

const Int32 CurrLobLockVersionNumber=100;

const Int32 CurrObjectEpochChangeReqVersionNumber = 100;
const Int32 CurrObjectEpochChangeReplyVersionNumber = 100;

const Int32 CurrObjectEpochStatsRequestVersionNumber = 100 ;
const Int32 CurrObjectEpochStatsReplyVersionNumber = 100 ;

const Int32 CurrObjectLockRequestVersionNumber = 100 ;
const Int32 CurrObjectLockReplyVersionNumber = 100 ;

const Int32 CurrObjectLockStatsRequestVersionNumber = 100 ;
const Int32 CurrObjectLockStatsReplyVersionNumber = 100 ;

const Int32 CurrQueryInvalidationStatsRequestVersionNumber = 100;
const Int32 CurrQueryInvalidationStatsReplyVersionNumber = 100;
//
// An enumeration of all IPC objects for RTS Servers.
// Includes both message objects and stream objects.
//
enum RtsMessageObjType
{
  RTS_IPC_FIRST = IPC_MSG_RTS_FIRST,    // 9000

  //
  // Message types
  //

  IPC_MSG_SSMP_REQUEST,                  // 9001
  IPC_MSG_SSMP_REPLY,                   // 9002
  IPC_MSG_SSCP_REQUEST,                 // 9003
  IPC_MSG_SSCP_REPLY,                   // 9004
  RTS_MSG_STATS_REQ,                    // 9005
  RTS_MSG_STATS_REPLY,                  // 9006
  RTS_MSG_CPU_STATS_REQ,                // 9007
  RTS_MSG_EXPLAIN_REQ,                  // 9008
  RTS_MSG_EXPLAIN_REPLY,                // 9009
  CANCEL_QUERY_STARTED_REQ,             // 9010
  CANCEL_QUERY_STARTED_REPLY,           // 9011
  CANCEL_QUERY_FINISHED_REQ,            // 9012
  IPC_MSG_RMS_REPLY,                    // 9013 
  QUERY_SUSP_ACTIV_REQ,                 // 9014
  CANCEL_QUERY_REQ,                     // 9015
  CONTROL_QUERY_REPLY,                  // 9016
  SUSPEND_QUERY_REQ,                    // 9017
  ACTIVATE_QUERY_REQ,                   // 9018
  CANCEL_QUERY_KILL_SERVERS_REQ,        // 9019
  CANCEL_QUERY_KILL_SERVERS_REPLY,      // 9020 
  SECURITY_INVALID_KEY_REQ,             // 9021
  SNAPSHOT_LOCK_REQ,                    // 9022
  SNAPSHOT_UNLOCK_REQ,                  // 9023

  LOB_LOCK_REQ,                         // 9024
  OBJECT_EPOCH_CHANGE_REQ,              // 9025
  OBJECT_EPOCH_CHANGE_REPLY,            // 9026
  OBJECT_EPOCH_STATS_REQ,               // 9027
  OBJECT_EPOCH_STATS_REPLY,             // 9028
  OBJECT_LOCK_REQ,                      // 9029
  OBJECT_LOCK_REPLY,                    // 9030
  OBJECT_LOCK_STATS_REQ,                // 9031
  OBJECT_LOCK_STATS_REPLY,              // 9032
  QUERY_INVALIDATION_STATS_REQ,         // 9033
  QUERY_INVALIDATION_STATS_REPLY,         // 9034

  // Object Types
  RTS_QUERY_ID = IPC_MSG_RTS_FIRST + 500, // 9500
  RTS_EXPLAIN_FRAG, 			  // 9501
  
  RTS_DIAGNOSTICS_AREA = IPC_SQL_DIAG_AREA,
  
};

typedef Int64 RtsHandle;
#define INVALID_RTS_HANDLE 0
#define RtsHandleIsValid(x) ( (x) != (INVALID_RTS_HANDLE) )

//----------------------------------------------------------------------
// RTS message base class
//
// Currently the only functionality provided by this class is memory
// management on NAMemory heaps. The code for this class is a copy of
// the code from the ExEspMsgObj class which is responsible for heap
// management of ESP message objects.
//----------------------------------------------------------------------
class RtsMessageObj : public IpcMessageObj
{
  typedef IpcMessageObj super;

public:
  //
  // Constructor for allocation on an NAMemory heap
  //
  RtsMessageObj(RtsMessageObjType objType,
                IpcMessageObjVersion objVersion,
                NAMemory *heap);

  //
  // Constructor for copyless receive
  //
  RtsMessageObj(IpcBufferedMsgStream *msgStream)
    : IpcMessageObj(msgStream), heap_(NULL)
  {
  }

  //
  // The delete operator
  // The heap management in this operator is the real reason for the
  // existence of this class
  //
  void operator delete(void *p);

  //
  // Accessor/Mutator methods
  //
  inline NAMemory *getHeap() const { return heap_; }
  inline const RtsHandle &getHandle() const { return handle_; }
  inline void setHandle(const RtsHandle &h) { handle_ = h; }

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);

  void unpackObj(IpcMessageObjType objType,
                 IpcMessageObjVersion objVersion,
                 NABoolean sameEndianness,
                 IpcMessageObjSize objSize,
                 IpcConstMessageBufferPtr buffer);

  //
  // We need our own decrRefCount() method so that the correct
  // operator delete gets called once an object is no longer needed
  //
  virtual IpcMessageRefCount decrRefCount();

  // In DecrRefCount, we call deleteMe() to free any space that the derived 
  // objects of this class have allocated for fields within them. See 
  // class RtsQueryId for example. If any of this class's deriving classes
  // allocate any space for fields within them, they'll need to free it 
  // within their deleteMe() method.
  virtual void deleteMe()
  {} 

protected:
  //
  // Helper functions to manage the NAMemory heap
  //
  char *allocateMemory(ComUInt32 nBytes);
  void deallocateMemory(char *s);
  char *allocateString(const char *s);
  void deallocateString(char *&s);

private:

  RtsHandle handle_;

  //
  // We store a pointer to the heap on which this object is allocated.
  // A NULL pointer indicates that the object is allocated directly
  // inside a message buffer with the copyless IPC protocol used by
  // buffered streams.
  //
  NAMemory *heap_;

  //
  // Do not implement default constructors or an assignment operator
  //
  RtsMessageObj();
  RtsMessageObj(const RtsMessageObj &);
  RtsMessageObj &operator=(const RtsMessageObj &);

}; // class RtsMessageObj

//----------------------------------------------------------------------
// RTS Stats Request
//
// This object currently contains no extra fields. The inherited 
// RTS handle from superclass RtsMessageObj will be used in the SSCP/SSMP
// route the data buffer to the appropriate message stream.
//----------------------------------------------------------------------
class RtsStatsReq : public RtsMessageObj
{
public:
  //
  // Constructor for allocation on a heap
  //
  RtsStatsReq(const RtsHandle &h, NAMemory *heap, NABoolean wmsProcess = FALSE)
    : RtsMessageObj(RTS_MSG_STATS_REQ, currRtsStatsReqVersionNumber, heap)
  {
    setHandle(h);
    wmsProcess_ = wmsProcess;
  }

  //
  // Constructor for copyless send
  //
  RtsStatsReq(const RtsHandle &h, NABoolean wmsProcess = FALSE)
    : RtsMessageObj(RTS_MSG_STATS_REQ, currRtsStatsReqVersionNumber, NULL)
  {
    setHandle(h);
    wmsProcess_ = wmsProcess;
  }

  //
  // Constructor for copyless receive
  //
  RtsStatsReq(IpcBufferedMsgStream *msgStream)
    : RtsMessageObj(msgStream)
  {}

  virtual ~RtsStatsReq()
  {}

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);

  void unpackObj(IpcMessageObjType objType,
                 IpcMessageObjVersion objVersion,
                 NABoolean sameEndianness,
                 IpcMessageObjSize objSize,
                 IpcConstMessageBufferPtr buffer);
  NABoolean getWmsProcess() { return wmsProcess_; }
private:


  //
  // Do not implement default constructors or an assignment operator
  //
  RtsStatsReq();
  RtsStatsReq(const RtsStatsReq &);
  RtsStatsReq &operator=(const RtsStatsReq &);

  NABoolean wmsProcess_;

}; // class RtsStatsReq

//----------------------------------------------------------------------
// RTS Stats Reply
//
// The inherited RTS handle from superclass RtsMessageObj will be used 
// in the SSCP/SSMP
// route the data buffer to the appropriate message stream.
//----------------------------------------------------------------------
class RtsStatsReply : public RtsMessageObj
{
public:
  //
  // Constructor for allocation on a heap
  //
  RtsStatsReply(NAMemory *heap)
    : RtsMessageObj(RTS_MSG_STATS_REPLY, currRtsStatsReplyVersionNumber, heap)
  {
    numSscpErrors_ = 0;
    numSqlProcs_ = 0;
    numCpus_ = 0;
  }
  
  RtsStatsReply(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(RTS_MSG_STATS_REPLY, currRtsStatsReplyVersionNumber, heap)
  {
    setHandle(h);
    numSscpErrors_ = 0;
    numSqlProcs_ = 0;
    numCpus_ = 0;
  }

  //
  // Constructor for copyless send
  //
  RtsStatsReply(const RtsHandle &h)
    : RtsMessageObj(RTS_MSG_STATS_REPLY, currRtsStatsReplyVersionNumber, NULL)
  {
    setHandle(h);
    numSscpErrors_ = 0;
    numSqlProcs_ = 0;
    numCpus_ = 0;
 }

  //
  // Constructor for copyless receive
  //
  RtsStatsReply(IpcBufferedMsgStream *msgStream)
    : RtsMessageObj(msgStream)
  {}

  virtual ~RtsStatsReply()
  {}

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);
  void setNumSscpErrors(short errors)
  { numSscpErrors_ = errors; }
  short &getNumSscpErrors() { return numSscpErrors_; }
  inline void incNumSqlProcs(short i=1) { numSqlProcs_ += i; }
  inline void incNumCpus(short i=1) { numCpus_ += i; }
  inline short getNumSqlProcs() { return numSqlProcs_; }
  inline short getNumCpus() { return numCpus_; }
  
private:
  short numSscpErrors_;
  short numSqlProcs_;
  short numCpus_;

  //
  // Do not implement default constructors or an assignment operator
  //
  RtsStatsReply();
  RtsStatsReply(const RtsStatsReply &);
  RtsStatsReply &operator=(const RtsStatsReply &);

}; // class RtsStatsReply

class RtsQueryId : public RtsMessageObj
{
public:
  //
  // Constructor for allocation on a heap
  //
  RtsQueryId(NAMemory *heap)
    : RtsMessageObj(RTS_QUERY_ID, currRtsQueryIdVersionNumber, heap)
  {
    queryId_ = NULL;
    queryIdLen_ = 0;
    statsMergeType_ = SQLCLI_SAME_STATS;
    cpu_ = -1;
    pid_ = -1;
    timeStamp_ = -1;
    queryNumber_ = -1;
    nodeName_[0] ='\0';
    reqType_ = SQLCLI_STATS_REQ_NONE;
    activeQueryNum_ = ANY_QUERY_;
    detailLevel_ = 0;
    subReqType_  = -1;
  }

  RtsQueryId(NAMemory *heap, char *queryId, Lng32 queryIdLen, 
      UInt16 statsMergeType = SQLCLI_SAME_STATS, short activeQueryNum = 1, 
      short reqType = SQLCLI_STATS_REQ_QID, short detailLevel = 0)
    : RtsMessageObj(RTS_QUERY_ID, currRtsQueryIdVersionNumber, heap),
    statsMergeType_(statsMergeType),
    queryIdLen_(queryIdLen),
    detailLevel_(detailLevel),
    subReqType_(-1)
  {
    queryId_ = new (heap) char[queryIdLen_+1];
    str_cpy_all(queryId_, queryId, queryIdLen_);
    queryId_[queryIdLen_] = '\0';
    reqType_ = reqType;
    cpu_ = -1;
    pid_ = -1;
    timeStamp_ = -1;
    queryNumber_ = -1;
    nodeName_[0] ='\0';
    activeQueryNum_ = activeQueryNum;
  }   

  RtsQueryId(NAMemory *heap, char *nodeName, short cpu, 
        UInt16 statsMergeType = SQLCLI_SAME_STATS, short activeQueryNum = 1);
  RtsQueryId(NAMemory *heap, char *nodeName, short cpu, pid_t pid, 
        UInt16 statsMergeType = SQLCLI_SAME_STATS, short activeQueryNum = 1,
        short reqType = SQLCLI_STATS_REQ_PID);
  RtsQueryId(NAMemory *heap, char *nodeName, short cpu, pid_t pid, 
        Int64 timeStamp, Lng32 queryNumber,
        UInt16 statsMergeType = SQLCLI_SAME_STATS, short activeQueryNum = 1,
        short reqType = SQLCLI_STATS_REQ_QID_INTERNAL);
  char *getQid() { return queryId_; }
  UInt16 getStatsMergeType() { return statsMergeType_; }
  short getStatsReqType() { return reqType_; }
  short getSubReqType() { return subReqType_; }
  void setSubReqType(short subReqType) { subReqType_ = subReqType; }
  short getCpu() { return cpu_; }
  pid_t getPid() { return pid_; }
  Int64 getTimeStamp() { return timeStamp_; }
  Lng32 getQueryNumber() { return queryNumber_; }
  short getActiveQueryNum() { return activeQueryNum_; }
  short getDetailLevel() { return detailLevel_; }
   
  //
  // Constructor for copyless send
  //
  RtsQueryId(const RtsHandle &h)
    : RtsMessageObj(RTS_QUERY_ID, currRtsQueryIdVersionNumber, NULL)
  {
    
  }

  //
  // Constructor for copyless receive
  //
  RtsQueryId(IpcBufferedMsgStream *msgStream)
    : RtsMessageObj(msgStream)
  {}

  virtual ~RtsQueryId()
  {}
  
  void deleteMe();

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);
  char *getQueryId() { return queryId_; }
  Lng32 getQueryIdLen() { return queryIdLen_; }
  enum activeQueryDef
  {
    ALL_ACTIVE_QUERIES_ = 0,
    ANY_QUERY_ = -1
    // Any number is the active query num
  };

private:

  //
  // Do not implement default constructors or an assignment operator
  //
  RtsQueryId(); 
  RtsQueryId(const RtsQueryId &);
  RtsQueryId &operator=(const RtsQueryId &);

  char *queryId_;
  Lng32 queryIdLen_;
  char nodeName_[MAX_SEGMENT_NAME_LEN+1];
  short cpu_;
  pid_t pid_;
  Int64 timeStamp_;
  Lng32 queryNumber_;
  short reqType_;
  UInt16 statsMergeType_;
  short activeQueryNum_; 
  short detailLevel_;
  short subReqType_;
                         
}; // class RtsQueryId

class RtsCpuStatsReq : public RtsMessageObj
{
public:
  //
  // Constructor 
  //
  RtsCpuStatsReq(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(RTS_MSG_CPU_STATS_REQ, currRtsCpuStatsReqVersionNumber, heap)
  {
    setHandle(h);
  }

  RtsCpuStatsReq(const RtsHandle &h, NAMemory *heap, char *nodeName, short cpu, short noOfQueries = -1,
    short reqType = SQLCLI_STATS_REQ_CPU_OFFENDER);
 
  virtual ~RtsCpuStatsReq()
  {}
  
  void deleteMe() {}

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);
  short getNoOfQueries() { return noOfQueries_; }
  short getReqType() { return reqType_; }
  void setSubReqType(short subReqType) { subReqType_ = subReqType; }
  short getSubReqType() { return subReqType_; }
  void setFilter(Lng32 filter) { filter_ = filter; }
  Lng32 getFilter() { return filter_; } 
  short getCpu() { return cpu_; }
  enum noOfQueriesDef
  {
    SE_PERTABLE_STATS = -4,
    SE_ROOT_STATS = -3,
    INIT_RMS_STATS_ = -2,
    INIT_CPU_STATS_HISTORY_ = -1, //  means init the history - Don't send the stats out
    ALL_ACTIVE_QUERIES_ = 0     // 0 - Send back all queries that have consumed cpu time since last invocation
  };
private:
  char  nodeName_[MAX_SEGMENT_NAME_LEN+1];
  short cpu_;
  short noOfQueries_; 
  short reqType_;
  short subReqType_;
  Lng32 filter_;
};

class RtsExplainFrag : public RtsMessageObj
{
public:
  //
  // Constructor 
  //
  RtsExplainFrag()
    : RtsMessageObj(RTS_EXPLAIN_FRAG, currRtsExplainFragVersionNumber, NULL)
  {
    explainFrag_ = NULL;
    explainFragLen_ = 0;
    topNodeOffset_ = 0;
  }

  RtsExplainFrag(NAMemory *heap)
    : RtsMessageObj(RTS_EXPLAIN_FRAG, currRtsExplainFragVersionNumber, heap)
  {
    explainFrag_ = NULL;
    explainFragLen_ = 0;
    topNodeOffset_ = 0;
  }

  RtsExplainFrag(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(RTS_EXPLAIN_FRAG, currRtsExplainFragVersionNumber, heap)
  {
    setHandle(h);
    explainFrag_ = NULL;
    explainFragLen_ = 0;
    topNodeOffset_ = 0;
  }

  RtsExplainFrag(NAMemory *heap, RtsExplainFrag *other);
 
  virtual ~RtsExplainFrag()
  {
    if (explainFrag_ != NULL)
      NADELETEBASIC((char *)explainFrag_, getHeap());
  }
  void fixup(RtsExplainFrag *other)
  {
    char *addrOfStatsVFTPtr, *myStatsVFTPtr;

    // Update the Virtual Function pointer of RtsExplainFrag
    addrOfStatsVFTPtr = (char *)(this);
    myStatsVFTPtr = (char *)(other);
    *((Long *)addrOfStatsVFTPtr) = *((Long *)myStatsVFTPtr);

  }

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);
  void setExplainFrag(void *explainFrag, Lng32 len, Lng32 topNodeOffset);
  inline Lng32 getExplainFragLen() { return explainFragLen_; }
  inline void *getExplainFrag() { return explainFrag_; }
  inline Lng32 getTopNodeOffset() { return topNodeOffset_; }
  
private:
  void *explainFrag_;
  Lng32 explainFragLen_;
  Lng32  topNodeOffset_;
};

class RtsExplainReq : public RtsMessageObj
{
public:
  //
  // Constructor 
  //
  RtsExplainReq(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(RTS_MSG_EXPLAIN_REQ, currRtsExplainReqVersionNumber, heap)
  {
    setHandle(h);
    qid_ = NULL;
    qidLen_ = 0;
  }

  RtsExplainReq(const RtsHandle &h, NAMemory *heap, char *qid, Lng32 qidLen);
  char *getQid() { return qid_; }
  Lng32 getQidLen() { return qidLen_; }
     
  ~RtsExplainReq();

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);
private:
  char  *qid_;
  Lng32  qidLen_;
};

class RtsExplainReply : public RtsMessageObj
{
public:
  //
  // Constructor 
  //
  RtsExplainReply(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(RTS_MSG_EXPLAIN_REPLY, currRtsExplainReplyVersionNumber, heap)
  {
    setHandle(h);
  }    
};

// This message is sent from the subject query at any of the following 
// events:
// 1) The query is started.
// 2) The query is reregistering after the MXSSMP crashed and was restarted. 
// This latter event is to allow fault tolerance and is not yet implemented.
// 
// The MXSSMP, acting as a "control broker", holds this message, deferring
// the reply until any of two events happens: 
// 1) a CancelQueryRequest message is sent from the CANCEL statement.
// 2) a QueryFinished message is sent from the subject query.  
//
// An RtsQueryId follows this.
class QueryStarted : public RtsMessageObj
{
public:

  QueryStarted(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(CANCEL_QUERY_STARTED_REQ, 
                    CurrQueryStartedReqVersionNumber, heap)
    , startTime_(0)
    , executionCount_(0)
    , qsFlags_(0)
  {
    setHandle(h);
    XPROCESSHANDLE_NULLIT_(&master_.phandle_);
  }

  QueryStarted(const RtsHandle &h, NAMemory *heap, 
               Int64 startTime, Int32 executionCount)
    : RtsMessageObj(CANCEL_QUERY_STARTED_REQ, 
                    CurrQueryStartedReqVersionNumber, heap)
    , startTime_(startTime)
    , executionCount_(executionCount)
    , qsFlags_(0)
  {
    setHandle(h);
    XPROCESSHANDLE_GETMINE_(&master_.phandle_);
  }
   
  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  Int64 getStartTime () const { return startTime_; }

  GuaProcessHandle getMasterPhandle() const { return master_; }

  Int32 getExecutionCount() const {return executionCount_; }

private:

  // Compare to Cancel message's timestamp to disallow cancel 
  // before target query has started.
  Int64 startTime_;

  // If query has no servers, or second interval lapses, then kill 
  // master if the CANCEL. 
  GuaProcessHandle master_;

  // When escalating to a kill, check current 
  // ExFragRootOperStats::executionCount_ against the 
  // count when the query started.
  Int32 executionCount_;

  Int32 qsFlags_;
};

// This reply from the MXSSMP, acting as a "control broker", is interpreted by
// the master, depending on the nextAction_ member.  If a CANCEL statement has 
// requested the control broker to cancel the query, then the nextAction_ will
// be CANCEL. If the master has finished its work, it has sent the control 
// broker a QueryFinished message, and the nextAction_ will be COMPLETE
// (even tho the master knows it is finished).  
//
class QueryStartedReply : public RtsMessageObj
{
public:
  QueryStartedReply(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(CANCEL_QUERY_STARTED_REPLY, 
                    CurrQueryStartedReplyVersionNumber, heap)
    , nextAction_(INVALID)
    , cancelLogging_(false)
  {
    setHandle(h);
  }

  QueryStartedReply(const RtsHandle &h, NAMemory *heap, bool cancelLogging)
    : RtsMessageObj(CANCEL_QUERY_STARTED_REPLY,
                    CurrQueryStartedReplyVersionNumber, heap)
    , nextAction_(INVALID)
    , cancelLogging_(cancelLogging)
  {
    setHandle(h);
  }

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);
  
  bool isNextActionComplete() const { return nextAction_ == COMPLETE; }
  bool isNextActionCancel()   const { return nextAction_ == CANCEL; }
  bool cancelLogging()        const { return cancelLogging_; }

  void nextActionIsComplete()         { nextAction_ = COMPLETE; }
  void nextActionIsCancel()           { nextAction_ = CANCEL; }

private:
  enum NextActionType {
    INVALID,
    COMPLETE,
    CANCEL
  };

  Int16 nextAction_;
  bool cancelLogging_;

};


// This message is sent from the master executor to indicate that the
// query is finished, so that MXSSMP can cleanup. An RtsQueryId follows this.
// Its reply is a QueryFinishedReply.
class QueryFinished : public RtsMessageObj
{
public:

  QueryFinished(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(CANCEL_QUERY_FINISHED_REQ, 
                    CurrQueryFinishedReqVersionNumber, heap)
  {
    setHandle(h);
  }

  // No additional info.
};

// A generic reply from MXSSMP to the EXE, or from MXSSCP to MXSSMP,
// with no payload, with the purpose of allowing generic IpcMessageStream
// completion.
// Used by 
// 1. control broker MXSSMP to reply to QueryFinished
// 2. MXSSMP to reply to SecInvalidKeyRequest from EXE
// 3. MXSSCP to reply to SecInvalidKeyRequest from MXSSMP
class RmsGenericReply : public RtsMessageObj
{
public:
  RmsGenericReply(NAMemory *heap)
    : RtsMessageObj(IPC_MSG_RMS_REPLY, 
                    CurrRmsReplyMessageVersion, heap)
  {
  }

  // No additional info.
};

// This message is sent from the CANCEL statement to the cancel broker, MXSSMP.
// An RtsQueryId follows this.
class CancelQueryRequest : public RtsMessageObj
{
public:
  CancelQueryRequest(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(CANCEL_QUERY_REQ, CurrCancelQueryReqVersionNumber, heap)
    , cancelStartTime_(0)
    , firstEscalationInterval_(0)
    , secondEscalationInterval_(0)
    , cancelEscalationSaveabend_(FALSE)
    , comment_(NULL)
    , commentLen_(0)
    , cancelLogging_(false)
    , cancelByPid_(false)
    , cancelPid_(-1)
    , minimumAge_(0)
  {
    setHandle(h);
  }

  CancelQueryRequest(const RtsHandle &h, NAMemory *heap, 
               Int64 startTime, 
               Int32 firstEscalationInterval, Int32 secondEscalationInterval,
               NABoolean cancelEscalationSaveabend, 
               char *comment, Int32 commentLength, bool cancelLogging,
               bool cancelByPid, Int32 cancelPid, Int32 minimumAge) 
    : RtsMessageObj(CANCEL_QUERY_REQ, CurrCancelQueryReqVersionNumber, heap)
    , cancelStartTime_(startTime)
    , firstEscalationInterval_(firstEscalationInterval)
    , secondEscalationInterval_(secondEscalationInterval)
    , cancelEscalationSaveabend_(cancelEscalationSaveabend)
    , cancelLogging_(cancelLogging)
    , cancelByPid_(cancelByPid)
    , cancelPid_(cancelPid)
    , minimumAge_(minimumAge)
  {
    setHandle(h);
    commentLen_ = (comment ? str_len(comment) : 0);
    comment_ = new(heap) char[commentLength + 1];
    str_cpy_all(comment_, comment, commentLength);
    comment_[commentLength] = '\0';
  }

  virtual ~CancelQueryRequest()
  {
    if (comment_)
      NADELETEBASIC(comment_, getHeap());
  }

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  Int64 getCancelStartTime () const { return cancelStartTime_; }

  Int32 getFirstEscalationInterval() const 
    { return firstEscalationInterval_; }

  Int32 getSecondEscalationInterval() const  
    { return secondEscalationInterval_; }

  NABoolean getCancelEscalationSaveabend() const  
    { return cancelEscalationSaveabend_; }

  char *getComment() const 
    { return comment_; }

  Int32 getCommentLen() const 
    { return commentLen_; }

  bool getCancelLogging() const
    { return cancelLogging_; }

  bool getCancelByPid() const
  { return cancelByPid_; }

  Int32 getCancelPid() const
  { return cancelPid_; }

  Int32 getMinimumAge() const
  { return minimumAge_; }

private:

  // Compare to target query's timestamp to disallow cancel 
  // before target query has started.
  Int64 cancelStartTime_;

  // CANCEL_ESCALATION_INTERVAL1 - in seconds
  Int32 firstEscalationInterval_;

  // CANCEL_ESCALATION_INTERVAL2 - in seconds
  Int32 secondEscalationInterval_;

  // CANCEL_ESCALATION_SAVEABEND 
  NABoolean cancelEscalationSaveabend_;

  Int32 commentLen_;
  char *comment_;

  bool cancelLogging_;

  bool cancelByPid_;
  Int32 cancelPid_;
  Int32 minimumAge_;  // in seconds
};

// This is the reply message, sent from the control broker, MXSSMP,
// to reply to any of three kinds of requests: a CancelQueryRequest,
// a SuspendQueryRequest, or an ActivateQueryRequest.  The reply is
// received by a CANCEL, SUSPEND, or ACTIVATE statement respectively.
// This reply indicates whether the request can be forwared to
// the target query.

class ControlQueryReply : public RtsMessageObj
{
public:
  ControlQueryReply(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(CONTROL_QUERY_REPLY, CurrControlQueryReplyVersionNumber, 
                    heap)
    , didAttemptControl_(false)
  {
    setHandle(h);
  }

  ControlQueryReply(const RtsHandle &h, NAMemory *heap, bool didAttemptControl)
    : RtsMessageObj(CONTROL_QUERY_REPLY, CurrControlQueryReplyVersionNumber, 
                    heap)
    , didAttemptControl_(didAttemptControl)
  {
    setHandle(h);
  }
   
  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  bool didAttemptControl() const { return didAttemptControl_; }

private:

  bool didAttemptControl_;
};


// This message is sent from the SUSPEND statement to the control broker.
// Its reply is a SuspendQueryReply.  An RtsQueryId follows this.
class SuspendQueryRequest : public RtsMessageObj
{
public:
  SuspendQueryRequest(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(SUSPEND_QUERY_REQ, CurrSuspendQueryReqVersionNumber, heap)
    , forced_(false)
    , suspendLogging_(false)
  {
    setHandle(h);
  }

  SuspendQueryRequest(const RtsHandle &h, NAMemory *heap, 
               bool forced, bool suspendLogging) 
    : RtsMessageObj(SUSPEND_QUERY_REQ, CurrSuspendQueryReqVersionNumber, heap)
    , forced_(forced)
    , suspendLogging_(suspendLogging)
  {
    setHandle(h);
  }
   
  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  bool getIsForced() const { return forced_; }
  bool getSuspendLogging() const {return suspendLogging_; }

private:

  // Should suspend even if holding locks or pinning audit?
  bool forced_;

  bool suspendLogging_;

};

// This message is sent from the ACTIVATE statement to the control broker.
// Its reply is an ActivateQueryReply.  An RtsQueryId follows this.
class ActivateQueryRequest : public RtsMessageObj
{
public:
  ActivateQueryRequest(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(ACTIVATE_QUERY_REQ, CurrActivateQueryReqVersionNumber, heap)
    , suspendLogging_(false)
  {
    setHandle(h);
  }

  ActivateQueryRequest(const RtsHandle &h, NAMemory *heap, bool suspendLogging)
    : RtsMessageObj(ACTIVATE_QUERY_REQ, CurrActivateQueryReqVersionNumber, heap)
    , suspendLogging_(suspendLogging)
  {
    setHandle(h);
  }

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
                 IpcMessageObjVersion objVersion,
                 NABoolean sameEndianness,
                 IpcMessageObjSize objSize,
                 IpcConstMessageBufferPtr buffer);

  bool getSuspendLogging() const {return suspendLogging_; }

private:
  bool suspendLogging_;
};

// This message is sent from MXSSMP to MXSSCP when the Cancel must 
// be escalated to stop the ESPs.
// An RtsQueryId follows this.
class CancelQueryKillServersRequest : public RtsMessageObj
{
public:
  CancelQueryKillServersRequest(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(CANCEL_QUERY_KILL_SERVERS_REQ, 
                    CurrKillServersReqVersionNumber, heap)
    , executionCount_(-1)
    , makeSaveabend_(false)
    , cancelLogging_(false)
  {
    setHandle(h);
    XPROCESSHANDLE_NULLIT_(&master_.phandle_);
  }

  CancelQueryKillServersRequest(const RtsHandle &h, NAMemory *heap,
            Int32 executionCount, GuaProcessHandle *master, bool makeSaveabend,
            bool cancelLogging)
    : RtsMessageObj(CANCEL_QUERY_KILL_SERVERS_REQ, 
                    CurrKillServersReqVersionNumber, heap)
    , executionCount_(executionCount)
    , master_(*master)
    , makeSaveabend_(makeSaveabend)
    , cancelLogging_(cancelLogging)
  {
    setHandle(h);
  }

  virtual ~CancelQueryKillServersRequest()
  {}

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  Int32 getExecutionCount() const {return executionCount_; }
  GuaProcessHandle getMasterPhandle() const { return master_; }
  bool getMakeSaveabend() const { return makeSaveabend_; }
  bool getCancelLogging() const { return cancelLogging_; }

private:
  // When escalating to a kill, check current 
  // ExFragRootOperStats::executionCount_ against the 
  // count when the query started.
  Int32 executionCount_;
  GuaProcessHandle master_;
  bool makeSaveabend_;
  bool cancelLogging_;
};

// This is the reply message for a CancelQueryKillServersRequest. 
// Also used to reply to SuspendActivateServersRequest.
class CancelQueryKillServersReply : public RtsMessageObj
{
public:
  CancelQueryKillServersReply(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(CANCEL_QUERY_KILL_SERVERS_REPLY, 
                    CurrKillServersReplyVersionNumber, heap)
  {
    setHandle(h);
  }

  virtual ~CancelQueryKillServersReply()
  {}

private:
  // No additional data.
};

// This message is sent from MXSSMP to MXSSCP to Suspend or 
// Activate SQL Exe (master and ESPs). 
// An RtsQueryId follows this.
class SuspendActivateServersRequest : public RtsMessageObj
{
public:
  SuspendActivateServersRequest(const RtsHandle &h, NAMemory *heap)
    : RtsMessageObj(SUSPEND_QUERY_REQ, 
                    CurrSuspendQueryReqVersionNumber, heap)
    , isRequestToSuspend_(true)
    , suspendLogging_(false)
  {
    setHandle(h);
  }

  SuspendActivateServersRequest(const RtsHandle &h, NAMemory *heap,
                           bool requestIsToSuspend, bool suspendLogging)
    : RtsMessageObj(SUSPEND_QUERY_REQ, 
                    CurrSuspendQueryReqVersionNumber, heap)
    , isRequestToSuspend_(requestIsToSuspend)
    , suspendLogging_(suspendLogging)
  {
    setHandle(h);
  }

  virtual ~SuspendActivateServersRequest()
  {}

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  bool isRequestToSuspend() const {return isRequestToSuspend_; }
  bool getSuspendLogging() const { return suspendLogging_; }

private:
  bool isRequestToSuspend_;
  bool suspendLogging_;

};

// This message is sent from the CLI's ContextCli::SetSecInvalidKeys
// to MXSSMP.  It is also sent from MXSSMP to MXSSCP.
class SecInvalidKeyRequest: public RtsMessageObj
{
public:
  SecInvalidKeyRequest(NAMemory *heap)
    : RtsMessageObj(SECURITY_INVALID_KEY_REQ, 
                    CurrSecurityInvalidKeyVersionNumber, heap)
    , numSiks_(0)
    , sikPtr_(NULL)
  {
  }

  SecInvalidKeyRequest(NAMemory *heap,
                       Int32 numSiks, SQL_QIKEY *sikPtr);

  virtual ~SecInvalidKeyRequest();

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  Int32 getNumSiks() const {return numSiks_; }
  SQL_QIKEY *getSik() const { return sikPtr_; }

private:
  Int32 numSiks_;
  SQL_QIKEY *sikPtr_;

};

//This message is sent from the CLI's ContextCli::SnapshotLockRequest
//to MXSSMP.  It is also sent from MXSSMP to MXSSCP.
class SnapshotLockRequest: public RtsMessageObj
{
public:
  SnapshotLockRequest(NAMemory *heap)
   : RtsMessageObj(SNAPSHOT_LOCK_REQ, 
           CurrSnapshotLockVersionNumber, heap)
  {
  }

  virtual ~SnapshotLockRequest(){};
  
  IpcMessageObjSize packedLength() { return baseClassPackedLength();}
  
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer)
    { return packBaseClassIntoMessage(buffer);}
  
  void unpackObj(IpcMessageObjType objType,
    IpcMessageObjVersion objVersion,
    NABoolean sameEndianness,
    IpcMessageObjSize objSize,
    IpcConstMessageBufferPtr buffer)
  { unpackBaseClass(buffer);}
  
private:
  
};

//This message is sent from the CLI's ContextCli::SnapshotLockRequest
//to MXSSMP.  It is also sent from MXSSMP to MXSSCP.
class SnapshotUnLockRequest: public RtsMessageObj
{
public:
  SnapshotUnLockRequest(NAMemory *heap)
   : RtsMessageObj(SNAPSHOT_UNLOCK_REQ, 
       CurrSnapshotUnLockVersionNumber, heap)
  {
  }
 
  virtual ~SnapshotUnLockRequest(){};
  
  IpcMessageObjSize packedLength() { return baseClassPackedLength();}
  
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer)
    { return packBaseClassIntoMessage(buffer);}
  
  void unpackObj(IpcMessageObjType objType,
    IpcMessageObjVersion objVersion,
    NABoolean sameEndianness,
    IpcMessageObjSize objSize,
    IpcConstMessageBufferPtr buffer)
    { unpackBaseClass(buffer);}

private:

};

// This message is sent from the CLI's ContextCli::setLobLock
// to MXSSMP.  It is also sent from MXSSMP to MXSSCP.
class LobLockRequest: public RtsMessageObj
{
public:
  LobLockRequest(NAMemory *heap)
    : RtsMessageObj(LOB_LOCK_REQ, 
                    CurrLobLockVersionNumber, heap)
  {
    memset(lobLockId_,0, sizeof(lobLockId_));
  }

  LobLockRequest(NAMemory *heap,
                  char *lobId );

  virtual ~LobLockRequest();

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  char *getLobLockId()  {return lobLockId_; }

private:
  char lobLockId_[LOB_LOCK_ID_SIZE+1];//allow for the lock as well as a '+' or '-'
};


// This message is sent from the CLI's ContextCli::setObjectEpochEntry method
// to MXSSMP.  It is also sent from MXSSMP to MXSSCP.

class ObjectEpochChangeRequest: public RtsMessageObj
{
public:

  enum Operation { NO_OP,
                   CREATE_OBJECT_EPOCH_ENTRY,
                   START_OR_CONTINUE_DDL_OPERATION,
                   COMPLETE_DDL_OPERATION,
                   ABORT_DDL_OPERATION,
                   FORCE };  

  enum Flags { NO_DDL_IN_PROGRESS = 0,
               DDL_IN_PROGRESS = 0x00000001,
               READS_DISALLOWED = 0x00000002 };

  ObjectEpochChangeRequest(NAMemory *heap)
    : RtsMessageObj(OBJECT_EPOCH_CHANGE_REQ, 
                    CurrObjectEpochChangeReqVersionNumber, heap),
    operation_(NO_OP),
    objectNameLength_(0),
    objectName_(NULL),
    redefTime_(0), 
    key_(0),
    expectedEpoch_(0),
    expectedFlags_(0),
    newEpoch_(0),
    newFlags_(0) 
  {
    // nothing else to do
  }

  ObjectEpochChangeRequest(NAMemory *heap,
    Operation operation,
    Int32 objectNameLength,
    const char * objectName,
    Int64 redefTime, 
    UInt64 key,
    UInt32 expectedEpoch,
    UInt32 expectedFlags,
    UInt32 newEpoch,
    UInt32 newFlags);

  virtual ~ObjectEpochChangeRequest();

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  Int32 operation() { return operation_; };
  Int32 objectNameLength() { return objectNameLength_; };
  const char * objectName() { return objectName_; };
  UInt64 redefTime() { return redefTime_; };
  UInt64 key() { return key_; };
  UInt32 expectedEpoch() { return expectedEpoch_; };
  UInt32 expectedFlags() { return expectedFlags_; };
  UInt32 newEpoch() { return newEpoch_; } ;
  UInt32 newFlags() { return newFlags_; } ;

private:

  Int32 operation_;  // takes value from enum Operation
  Int32 objectNameLength_;
  char * objectName_;
  Int64 redefTime_;  // redefinition timestamp of object (0 if not available)
  UInt64 key_;       // key for Object Epoch Entry
  UInt32 expectedEpoch_;  // expected epoch for START, COMPLETE operations
  UInt32 expectedFlags_;  // expected flags for START, COMPLETE operations
  UInt32 newEpoch_;
  UInt32 newFlags_;
};


// This is the reply message, sent from MXSSCP to MXSSMP, and from
// MXSSMP to the client in response to an ObjectEpochChangeRequest.

class ObjectEpochChangeReply : public RtsMessageObj
{
public:

  enum Result { SUCCESS,
                UNEXPECTED_VALUES_FOUND, // expectedEpoch_ or expectedFlags_ did not match
                CACHE_FULL,  // cache is full (e.g. we got an out-of-memory error)
                INTERNAL_ERROR   // an error occurred when accessing object epoch cache
              };

  ObjectEpochChangeReply(NAMemory *heap)
    : RtsMessageObj(OBJECT_EPOCH_CHANGE_REPLY, CurrObjectEpochChangeReplyVersionNumber, 
                    heap),
    result_(SUCCESS),
    maxExpectedEpochFound_(0),
    maxExpectedFlagsFound_(0) 
  {
    // nothing else to do
  }

  ObjectEpochChangeReply(Result result, 
                         UInt32 maxExpectedEpochFound, 
                         UInt32 maxExpectedFlagsFound,
                         NAMemory *heap)
    : RtsMessageObj(OBJECT_EPOCH_CHANGE_REPLY, CurrObjectEpochChangeReplyVersionNumber, 
                    heap),
    result_(result),
    maxExpectedEpochFound_(maxExpectedEpochFound),
    maxExpectedFlagsFound_(maxExpectedFlagsFound) 
  {
    // nothing else to do
  }

  virtual ~ObjectEpochChangeReply()
  { };
   
  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  Result result() { return (Result)result_; };
  UInt32 maxExpectedEpochFound() { return maxExpectedEpochFound_; };
  UInt32 maxExpectedFlagsFound() { return maxExpectedFlagsFound_; };

private:

  Int32 result_;  // values taken from enum Result, Int32 for pack/unpack
  UInt32 maxExpectedEpochFound_;  // set if UNEXPECTED_VALUES_FOUND is returned
  UInt32 maxExpectedFlagsFound_;  // or of unexpected flags if UNEXPECTED_VALUES_FOUND is returned
};

class ObjectEpochStatsRequest : public RtsMessageObj
{
public:
  ObjectEpochStatsRequest(NAMemory *heap)
    :RtsMessageObj(OBJECT_EPOCH_STATS_REQ,
                   CurrObjectEpochStatsRequestVersionNumber,
                   heap)
  {}

  ObjectEpochStatsRequest(NAMemory *heap,
                          const char *objectName,
                          Lng32 objectNameLen,
                          short cpu,
                          bool locked)
    :RtsMessageObj(OBJECT_EPOCH_STATS_REQ,
                   CurrObjectEpochStatsRequestVersionNumber,
                   heap)
    ,objectName_(NULL)
    ,objectNameLen_(objectNameLen)
    ,cpu_(cpu)
    ,locked_(locked)
  {
    // copy in objectName
    if (objectName != NULL) {
      objectName_ = new(heap) char[objectNameLen+1];
      strncpy(objectName_,objectName,objectNameLen);
      objectName_[objectNameLen] = '\0';
    }
  }

  virtual ~ObjectEpochStatsRequest() {}

  void deleteMe() {}

  IpcMessageObjSize packedLength()
  {
    IpcMessageObjSize result = baseClassPackedLength();
    result += objectNameLen_;
    result += sizeof(objectNameLen_);
    result += sizeof(cpu_);
    result += sizeof(locked_);
    return result;
  }
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer)
  {
    IpcMessageObjSize result = packBaseClassIntoMessage(buffer);
    result += packIntoBuffer(buffer, objectNameLen_);
    str_cpy_all(buffer, objectName_, objectNameLen_);
    buffer += objectNameLen_;
    result += objectNameLen_;
    result += packIntoBuffer(buffer, cpu_);
    result += packIntoBuffer(buffer, locked_);
    return result;
  }
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer)
  {
    unpackBaseClass(buffer);
    unpackBuffer(buffer, objectNameLen_);
    if (objectNameLen_ == 0) {
      objectName_ = NULL;
    } else {
      objectName_ = new (getHeap()) char[objectNameLen_+1];
      str_cpy_all(objectName_, buffer, objectNameLen_);
      objectName_[objectNameLen_] = '\0';
    }
    buffer += objectNameLen_;
    unpackBuffer(buffer, cpu_);
    unpackBuffer(buffer, locked_);
  }

  const char *getObjectName() const { return objectName_; }
  Lng32 getObjectNameLen() const { return objectNameLen_; }
  short getCpu() const { return cpu_; }
  bool getLocked() const { return locked_; }

private:
  char *objectName_;
  Lng32 objectNameLen_;
  short cpu_;
  bool locked_;
};

class ObjectEpochStatsReply : public RtsMessageObj
{
public:
  ObjectEpochStatsReply(NAMemory *heap)
    :RtsMessageObj(OBJECT_EPOCH_STATS_REPLY,
                   CurrObjectEpochStatsReplyVersionNumber,
                   heap)
  {}
  virtual ~ObjectEpochStatsReply() {}

  void deleteMe() {}

  IpcMessageObjSize packedLength()
  {
    IpcMessageObjSize result = baseClassPackedLength();
    return result;
  }
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer)
  {
    IpcMessageObjSize result = packBaseClassIntoMessage(buffer);
    return result;
  }
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer)
  {
    unpackBaseClass(buffer);
  }
};

class ObjectLockRequest : public RtsMessageObj
{
public:
  // NOTE: Please keep this in sync with opTypeLits array
  enum OpType { LOCK,         // Acquire a DDL lock
                UNLOCK,       // Release a DDL lock
                UNLOCKALL,    // Release all DDL locks of a holder
                CLEANUP       // Cleanup all DML/DDL locks of a holder
  };
  static const char* opTypeLit(OpType opType)
  {
    const char *opTypeLits[] =
    { "LOCK",
      "UNLOCK",
      "UNLOCKALL",
      "CLEANUP",
    };
    return opTypeLits[opType];
  }

  ObjectLockRequest(NAMemory *heap)
    :RtsMessageObj(OBJECT_LOCK_REQ,
                   CurrObjectLockRequestVersionNumber,
                   heap)
  {}
  ObjectLockRequest(NAMemory *heap,
                    const char *objectName,
                    Lng32 objectNameLen,
                    ComObjectType objectType,
                    OpType opType,
                    Int32 lockNid,
                    Int32 lockPid,
                    UInt32 maxRetries,
                    UInt32 delay);

  virtual ~ObjectLockRequest()
  {
    if (objectName_ != NULL) {
      NAMemory *h = getHeap();
      if (h) {
        h->deallocateMemory(objectName_);
      }
    }
    objectName_ = NULL;
  }

  IpcMessageObjSize packedLength();
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer);

  const char *getObjectName() const { return objectName_; }
  Lng32 getObjectNameLen() const { return objectNameLen_; }
  ComObjectType getObjectType() const { return objectType_; }
  OpType getOpType() const { return opType_; }
  const char* getOpTypeLit() const { return opTypeLit(opType_); }
  Int32 lockNid() const { return lockNid_; }
  Int32 lockPid() const { return lockPid_; }
  UInt32 maxRetries() const { return maxRetries_; }
  UInt32 delay() const { return delay_; }

private:
  char *objectName_;
  Lng32 objectNameLen_;
  ComObjectType objectType_;
  OpType opType_;
  Int32 lockNid_;
  Int32 lockPid_;
  UInt32 maxRetries_;
  UInt32 delay_;
};

class ObjectLockReply : public RtsMessageObj
{
public:
  enum LockState
  { LOCK_UNKNOWN  = -1,
    LOCK_OK,
    LOCK_CONFLICT_DML,
    LOCK_CONFLICT_DDL,
    LOCK_OUT_OF_ENTRY,
  };
  static const char* lockStateLit(LockState state)
  {
    const char *lockStates[] =
    { "LOCK_UNKNOWN",
      "LOCK_OK",
      "LOCK_CONFLICT_DML",
      "LOCK_CONFLICT_DDL",
      "LOCK_OUT_OF_ENTRY",
    };
    return lockStates[state+1];
  }

  ObjectLockReply(NAMemory *heap)
    :RtsMessageObj(OBJECT_LOCK_REPLY,
                   CurrObjectLockReplyVersionNumber,
                   heap)
    ,lockState_(LOCK_UNKNOWN)
    ,conflictNid_(-1)
    ,conflictPid_(-1)
  {}
  ObjectLockReply(NAMemory *heap,
                  LockState lockState,
                  Int32 nid = -1,
                  Int32 pid = -1)
    :RtsMessageObj(OBJECT_LOCK_REPLY,
                   CurrObjectLockReplyVersionNumber,
                   heap)
    ,lockState_(lockState)
    ,conflictNid_(nid)
    ,conflictPid_(pid)
  {}

  IpcMessageObjSize packedLength()
  {
    IpcMessageObjSize result = baseClassPackedLength();
    result += sizeof(lockState_);
    result += sizeof(conflictNid_);
    result += sizeof(conflictPid_);
    return result;
  }
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer)
  {
    IpcMessageObjSize result = packBaseClassIntoMessage(buffer);
    result += packIntoBuffer(buffer, lockState_);
    result += packIntoBuffer(buffer, conflictNid_);
    result += packIntoBuffer(buffer, conflictPid_);
    return result;
  }
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer)
  {
    unpackBaseClass(buffer);
    unpackBuffer(buffer, lockState_);
    unpackBuffer(buffer, conflictNid_);
    unpackBuffer(buffer, conflictPid_);
  }

  void setLockState(LockState state) { lockState_ = state; }
  LockState getLockState() const { return lockState_; }
  const char* getLockStateLit() const { return lockStateLit(lockState_); }
  void setConflictNid(Int32 nid) { conflictNid_ = nid; }
  Int32 getConflictNid() const { return conflictNid_; }
  void setConflictPid(Int32 pid) { conflictPid_ = pid; }
  Int32 getConflictPid() const { return conflictPid_; }

private:
  LockState lockState_;
  Int32 conflictNid_;
  Int32 conflictPid_;
};

class ObjectLockStatsRequest : public RtsMessageObj
{
public:
  ObjectLockStatsRequest(NAMemory *heap)
    :RtsMessageObj(OBJECT_LOCK_STATS_REQ,
                   CurrObjectLockStatsRequestVersionNumber,
                   heap)
  {}

  ObjectLockStatsRequest(NAMemory *heap,
                         const char *objectName,
                         Lng32 objectNameLen,
                         ComObjectType objectType,
                         short cpu)
    :RtsMessageObj(OBJECT_LOCK_STATS_REQ,
                   CurrObjectLockStatsRequestVersionNumber,
                   heap)
    ,objectName_(NULL)
    ,objectNameLen_(objectNameLen)
    ,objectType_(objectType)
    ,cpu_(cpu)
  {
    // copy in objectName
    if (objectName != NULL) {
      objectName_ = new(heap) char[objectNameLen+1];
      strncpy(objectName_,objectName,objectNameLen);
      objectName_[objectNameLen] = '\0';
    }
  }

  virtual ~ObjectLockStatsRequest()
  {
    if (objectName_ != NULL) {
      NAMemory *h = getHeap();
      if (h) {
        h->deallocateMemory(objectName_);
      }
    }
    objectName_ = NULL;
  }

  void deleteMe() {}

  IpcMessageObjSize packedLength()
  {
    IpcMessageObjSize result = baseClassPackedLength();
    result += objectNameLen_;
    result += sizeof(objectNameLen_);
    result += sizeof(objectType_);
    result += sizeof(cpu_);
    return result;
  }
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer)
  {
    IpcMessageObjSize result = packBaseClassIntoMessage(buffer);
    result += packIntoBuffer(buffer, objectNameLen_);
    str_cpy_all(buffer, objectName_, objectNameLen_);
    buffer += objectNameLen_;
    result += objectNameLen_;
    result += packIntoBuffer(buffer, objectType_);
    result += packIntoBuffer(buffer, cpu_);
    return result;
  }
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer)
  {
    unpackBaseClass(buffer);
    unpackBuffer(buffer, objectNameLen_);
    if (objectNameLen_ == 0) {
      objectName_ = NULL;
    } else {
      objectName_ = new (getHeap()) char[objectNameLen_+1];
      str_cpy_all(objectName_, buffer, objectNameLen_);
      objectName_[objectNameLen_] = '\0';
    }
    buffer += objectNameLen_;
    unpackBuffer(buffer, objectType_);
    unpackBuffer(buffer, cpu_);
  }

  const char *getObjectName() const { return objectName_; }
  Lng32 getObjectNameLen() const { return objectNameLen_; }
  ComObjectType getObjectType() const { return objectType_; }
  short getCpu() const { return cpu_; }

private:
  char *objectName_;
  Lng32 objectNameLen_;
  ComObjectType objectType_;
  short cpu_;
};

class ObjectLockStatsReply : public RtsMessageObj
{
public:
  ObjectLockStatsReply(NAMemory *heap)
    :RtsMessageObj(OBJECT_LOCK_STATS_REPLY,
                   CurrObjectLockStatsReplyVersionNumber,
                   heap)
  {}
  virtual ~ObjectLockStatsReply() {}

  void deleteMe() {}

  IpcMessageObjSize packedLength()
  {
    IpcMessageObjSize result = baseClassPackedLength();
    return result;
  }
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer)
  {
    IpcMessageObjSize result = packBaseClassIntoMessage(buffer);
    return result;
  }
  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer)
  {
    unpackBaseClass(buffer);
  }
};

class QueryInvalidStatsRequest : public RtsMessageObj
{
public:
  QueryInvalidStatsRequest(NAMemory *heap)
    :RtsMessageObj(QUERY_INVALIDATION_STATS_REQ,
                   CurrQueryInvalidationStatsRequestVersionNumber,
                   heap)
  {}

  virtual ~QueryInvalidStatsRequest()
  {
  }
  void deleteMe() {}

  IpcMessageObjSize packedLength()
  {
    return baseClassPackedLength();
  }

  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer)
  {
    return packBaseClassIntoMessage(buffer);
  }

  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer)
  {
    unpackBaseClass(buffer);
  }
};

class QueryInvalidStatsReply : public RtsMessageObj
{
public:
  QueryInvalidStatsReply(NAMemory *heap)
    :RtsMessageObj(QUERY_INVALIDATION_STATS_REPLY,
                   CurrQueryInvalidationStatsReplyVersionNumber,
                   heap)
  {}
  virtual ~QueryInvalidStatsReply() {}

  void deleteMe() {}

  IpcMessageObjSize packedLength()
  {
    return baseClassPackedLength();
  }
  
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer)
  {
    return packBaseClassIntoMessage(buffer);
  }

  void unpackObj(IpcMessageObjType objType,
		 IpcMessageObjVersion objVersion,
		 NABoolean sameEndianness,
		 IpcMessageObjSize objSize,
		 IpcConstMessageBufferPtr buffer)
  {
    unpackBaseClass(buffer);
  }
};

#endif // _RTS_EXE_IPC_H_

