

#ifndef CMPMESSAGE_H
#define CMPMESSAGE_H

#include "cli/SQLCLIdev.h"
#include "common/ComDefs.h"
#include "common/Int64.h"
#include "common/Ipc.h"
#include "common/Platform.h"
#include "common/str.h"
#include "export/NABasicObject.h"

#define EXECMPIPCVERSION 100

typedef int CmpMsgBufLenType;

// this file contains

class CmpMessageObj;
class CmpMessageExit;
class CmpMessageLast;
class CmpMessageSQLText;
class CmpMessageReplyCode;
class CmpMessageCompileStmt;

// forward declaration

class ex_expr;
class sql_buffer;
class FragmentDir;

// -----------------------------------------------------------------------
// The base class for all the executor/compiler messages
// -----------------------------------------------------------------------
class CmpMessageObj : public IpcMessageObj {
 public:
  enum MessageTypeEnum {
    NULL_REQUEST = IPC_MSG_SQLCOMP_FIRST,
    EXIT_CONNECTION,
    LAST_MESSAGE,
    ENVS_SETUP,
    SQLTEXT_COMPILE,
    SQLTEXT_STATIC_COMPILE,
    DESCRIBE,
    PROCESSDDL,
    DDL = PROCESSDDL,  // synonyms
    UPDATE_HIST_STAT,
    XN_OPER,  // begin/commit/rollback
    SET_TRANS,
    DDL_NATABLE_INVALIDATE,
    DDL_COMMIT_SHARED_CACHE,
    DDL_ROLLBACK_SHARED_CACHE,
    INTERNALSP_REQUEST,
    INTERNALSP_GETNEXT,
    ENVS_REFRESH,
    SQLTEXT_RECOMPILE,  // tells mxcmp to decache & recompile query
    SQLTEXT_STATIC_RECOMPILE,
    END_SESSION,
    DATABASE_USER,
    DDL_WITH_STATUS,
    REPLY_CODE = IPC_MSG_SQLCOMP_FIRST + 700,
    REPLY_ISP,
    CONNECTION_TYPE = IPC_MSG_SQLCOMP_FIRST + 900,
    EXE_CMP_MESSAGE = IPC_MSG_SQLCOMP_LAST
  };

  typedef long ID;

  CmpMessageObj(IpcMessageObjType t = NULL_REQUEST, CollHeap *h = 0);

  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }

  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }

  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  // decrRefCount() needs to be redefined to call the destructor and
  // deallocate space. For the classes derived from this, if there is
  // special thing to be done in destructor, needs to write its own
  // destroyMe() method
  virtual void destroyMe() {}
  virtual IpcMessageRefCount decrRefCount();

  CollHeap *getHeap() { return h_; }
  ID id() const { return id_; }

  virtual int getFlags() const { return 0L; }
  virtual void setFlags(int) {}

  friend ostream &operator<<(ostream &, const CmpMessageObj &);

  virtual ~CmpMessageObj() {}

  // Virtual method that sets the version field of the message to the
  // version specified by the caller. If the structure of the message
  // has changed in an incompatible way from one version to the next this
  // method will also do the translation of the new structure to the old
  // before sending it to the downrev compiler.

  virtual void migrateToVersion(short version) { setVersion(EXECMPIPCVERSION); }

 protected:
  // -----------------------------------------------------------------------
  // Some helper routines for CmpMessageObj family
  // -----------------------------------------------------------------------

  IpcMessageObjSize packIntoBuffer(char *&buffer, char *strPtr);
  IpcMessageObjSize packIntoBuffer(char *&buffer, void *strPtr, int sz);

  void unpackBuffer(const char *&buffer, void *&strPtr, CollHeap *h = 0) {
    char *temp = 0;
    unpackBuffer(buffer, temp, h);
    strPtr = temp;
  }

  // unpack a char* buffer
  void unpackBuffer(const char *&buffer, char *&strPtr, CollHeap *h = 0);

  // unpack a char* buffer, but strPtr is a preallocated space
  // with size maxSize. maxSize has to be big enough to hold the
  // whole buffer.
  void unpackBuffer(const char *&buffer, char *strPtr, int maxSize, int &sizeMoved);

  void advanceSize(IpcMessageObjSize &size, const void *const buffPtr, int sz = 0) {
    const int lenSize = sizeof(CmpMsgBufLenType);
    size += lenSize;
    if (buffPtr != NULL) size += ((sz) ? sz : (str_len((char *)buffPtr) + 1));
  }

  ID id_;

  void advanceSize(IpcMessageObjSize &size, long i64) { size += sizeof(i64); }

  IpcMessageObjSize packIntoBuffer(char *&buffer, long i64) {
    IpcMessageObjSize sz = sizeof(long);
    str_cpy_all((char *)buffer, (char *)&i64, sz);
    buffer += sz;
    return sz;
  }

  void unpackBuffer(const char *&buffer, long &i64) {
    IpcMessageObjSize sz = sizeof(long);
    str_cpy_all((char *)&i64, (char *)buffer, sz);
    buffer += sz;
  }

 private:
  CmpMessageObj &operator=(const CmpMessageObj &);
  CmpMessageObj(const CmpMessageObj &);

  CollHeap *h_;

};  // end of CmpMessageObj

// -----------------------------------------------------------------------
// Basic request objects
// -----------------------------------------------------------------------

class CmpMessageRequestBasic : public CmpMessageObj {
 public:
  CmpMessageRequestBasic(MessageTypeEnum t = NULL_REQUEST, CollHeap *h = 0);
  virtual ~CmpMessageRequestBasic();

  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }

  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }

  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

 private:
  CmpMessageRequestBasic(const CmpMessageRequestBasic &);
  const CmpMessageRequestBasic &operator=(const CmpMessageRequestBasic &);

};  // end of CmpMessageRequestBasic

class CmpMessageReplyBasic : public CmpMessageObj {
 public:
  CmpMessageReplyBasic(MessageTypeEnum t = NULL_REQUEST, ID request = 0, CollHeap *h = 0);
  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }

  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }

  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  ID &request() { return request_; }

 protected:
  ID request_;
};  // end of CmpMessageReplyBasic

// -----------------------------------------------------------------------
// Basic class for requests from executor
// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// Information that is sent to recompile a dynamic or static statement.
// This class is sent as part of the data() field.
// -----------------------------------------------------------------------
class CmpCompileInfo {
 public:
  CmpCompileInfo(char *sourceStr, int sourceStrLen, int sourceStrCharSet, char *schemaName, int schemaNameLen,
                 int inputArrayMaxsize, short rowsetAtomicity);
  CmpCompileInfo();

  void init();

  short getClassSize() { return (short)sizeof(CmpCompileInfo); }
  int getVarLength();
  int getLength();
  void packVars(char *buffer, CmpCompileInfo *ci, int &nextOffset);
  void pack(char *buffer);
  void unpack(char *base);

  void getUnpackedFields(char *&sqltext, char *&schemaName);

  const int getInputArrayMaxsize() { return inputArrayMaxsize_; }

  const short getRowsetAtomicity();

  NABoolean odbcProcess() { return (flags_ & ODBC_PROCESS) != 0; }
  void setOdbcProcess(NABoolean v) { (v ? flags_ |= ODBC_PROCESS : flags_ &= ~ODBC_PROCESS); }

  NABoolean isSystemModuleStmt(void) { return (flags_ & SYSTEM_MODULE_STMT) != 0; }
  void setSystemModuleStmt(NABoolean v) { (v ? flags_ |= SYSTEM_MODULE_STMT : flags_ &= ~SYSTEM_MODULE_STMT); }

  NABoolean isAnsiHoldable() { return (flags_ & ANSI_HOLDABLE_CURSOR) != 0; }
  void setAnsiHoldable(NABoolean v) { (v ? flags_ |= ANSI_HOLDABLE_CURSOR : flags_ &= ~ANSI_HOLDABLE_CURSOR); }

  NABoolean isPubsubHoldable() { return (flags_ & PUBSUB_HOLDABLE_CURSOR) != 0; }
  void setPubsubHoldable(NABoolean v) { (v ? flags_ |= PUBSUB_HOLDABLE_CURSOR : flags_ &= ~PUBSUB_HOLDABLE_CURSOR); }

  const short getHoldableAttr();
  NABoolean noTextCache() { return (flags_ & NO_TEXT_CACHE) != 0; }
  void setNoTextCache(NABoolean v) { (v ? flags_ |= NO_TEXT_CACHE : flags_ &= ~NO_TEXT_CACHE); }

  NABoolean aqrPrepare() { return (flags_ & AQR_PREPARE) != 0; }
  void setAqrPrepare(NABoolean v) { (v ? flags_ |= AQR_PREPARE : flags_ &= ~AQR_PREPARE); }

  NABoolean standaloneQuery() { return (flags_ & STANDALONE_QUERY) != 0; }
  void setStandaloneQuery(NABoolean v) { (v ? flags_ |= STANDALONE_QUERY : flags_ &= ~STANDALONE_QUERY); }

  NABoolean sentryPrivRecheck() { return (flags_ & SENTRY_PRIV_RECHECK) != 0; }
  void setSentryPrivRecheck(NABoolean v) { (v ? flags_ |= SENTRY_PRIV_RECHECK : flags_ &= ~SENTRY_PRIV_RECHECK); }

  int getSqlTextLen() const { return sqlTextLen_; }
  void setSqlTextLen(int len) { sqlTextLen_ = len; }

  int getSqlTextCharSet() const { return sqlTextCharSet_; }
  void setSqlTextCharSet(int cs) { sqlTextCharSet_ = cs; }

  NABoolean recompUseNATableCache() { return (flags_ & RECOMP_USE_NATABLE_CACHE) != 0; }
  void setRecompUseNATableCache(NABoolean v) {
    (v ? flags_ |= RECOMP_USE_NATABLE_CACHE : flags_ &= ~RECOMP_USE_NATABLE_CACHE);
  }

  NABoolean isHbaseDDL() { return (flags_ & HBASE_DDL) != 0; }
  void setHbaseDDL(NABoolean v) { (v ? flags_ |= HBASE_DDL : flags_ &= ~HBASE_DDL); }

  NABoolean isGenLoadQueryCache() { return (flags_ & GEN_LOAD_QUERYCACHE) != 0; }
  void setGenLoadQueryCache(NABoolean v) { (v ? flags_ |= GEN_LOAD_QUERYCACHE : flags_ &= ~GEN_LOAD_QUERYCACHE); }

  NABoolean doNotCachePlan() { return (flags_ & DO_NOT_CACHE_PLAN) != 0; }
  void setDoNotCachePlan(NABoolean v) { (v ? flags_ |= DO_NOT_CACHE_PLAN : flags_ &= ~DO_NOT_CACHE_PLAN); }

  enum { FILLERSIZE = 60 };

 protected:
  enum Flags {
    // if nametype was specified as NSK at static compilation time.
    // Used to send this info to mxcmp at auto recompilation time.
    NAMETYPE_NSK = 0x0001,

    // if odbc_process was ON as NSK at static compilation time.
    // Used to send this info to mxcmp at auto recompilation time.
    ODBC_PROCESS = 0x0002,
    SYSTEM_MODULE_STMT = 0x0008,
    // the next two flags go together.
    // The first flag denotes whether rowset atomicity has been specified.
    // The second flag is read on only if the first one is turned ON.
    // The second flag denotes whether the statement is ATOMIC or NOT ATOMIC
    ROWSET_ATOMICITY_SPECIFIED = 0x0010,
    ROWSET_NOT_ATOMIC = 0x0020,

    // do not use text cache during compile.
    NO_TEXT_CACHE = 0x0040,

    // internal prepare during AQR
    AQR_PREPARE = 0x0080,

    // not an user explicitly prepared query
    STANDALONE_QUERY = 0x0100,
    ANSI_HOLDABLE_CURSOR = 0x0200,
    PUBSUB_HOLDABLE_CURSOR = 0x0400,

    // this is a request to perform a SQL ddl operation that will be converted
    // to an hbase object
    HBASE_DDL = 0x0800,

    // do not cache the resulting query plan
    DO_NOT_CACHE_PLAN = 0x1000,

    // this is a request to recheck Apache Sentry privileges
    SENTRY_PRIV_RECHECK = 0x2000,

    // this is for generate/load querycache
    GEN_LOAD_QUERYCACHE = 0x4000,

    // to indicate remove cached NATable or not
    RECOMP_USE_NATABLE_CACHE = 0x8000
  };

  char *sqltext_;          // 00-07
  int sqlTextLen_;         // 08-11
  int unused_;             // 12-15
  char *schemaName_;       // 16-23
  int schemaNameLen_;      // 24-27
  int unused2_;            // 28-31
  int inputArrayMaxsize_;  // 32-35
  int flags_;              // 36-39

  int sqlTextCharSet_;            // 40-43
  char fillerBytes_[FILLERSIZE];  // 44-103
};

class CmpMessageRequest : public CmpMessageRequestBasic {
 public:
  CmpMessageRequest(MessageTypeEnum e, char *data = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0,
                    int cs = SQLCHARSETCODE_UTF8, const char *parentQid = NULL, int parentQidLen = 0);

  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }
  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }
  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  void copyToString(char *&dest, CmpMsgBufLenType &sz, char *s, CmpMsgBufLenType sz1);

  char *data() const { return data_; }
  int charSet() const { return charSet_; }

  virtual int getFlags() const { return flags_; }
  virtual void setFlags(int f) { flags_ = f; }

  virtual void destroyMe();
  virtual ~CmpMessageRequest() { destroyMe(); }

  CmpMsgBufLenType getSize() const { return sz_; };

  inline const char *getParentQid() const { return parentQid_; }
  inline int getParentQidLen() const { return parentQidLen_; }
  CmpCompileInfo *getCmpCompileInfo() const { return (CmpCompileInfo *)data(); };

 private:
  CmpMessageRequest &operator=(const CmpMessageRequest &);
  CmpMessageRequest(const CmpMessageRequest &);

  char *data_;
  CmpMsgBufLenType sz_;
  int flags_;
  int charSet_;  // sender's character set
  int parentQidLen_;
  const char *parentQid_;
  NABoolean allocated_;
};  // end of CmpMessageRequest

// -----------------------------------------------------------------------
// Basic class for reply to executor
// -----------------------------------------------------------------------

class CmpMessageReply : public CmpMessageReplyBasic {
 public:
  // The memeber data_ of this class can be set through data() member function
  // with any pointer. After data_ is set, this class owns that pointer.
  // This memory is supposed to be allocated from outh CollHeap*,
  // NADELETEBASIC will be called to delete the memory.

  CmpMessageReply(MessageTypeEnum e, ID request = 0, CollHeap *h = 0, char *preAllocatedData = 0,
                  int preAllocatedDataSize = 0, CollHeap *outh = 0);

  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }
  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }
  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  CollHeap *outHeap() { return outh_; }

  int getSize() const { return sz_; }
  char *getData() const { return data_; }
  char *takeData() {
    dataAllocated_ = FALSE;
    char *temp = data_;
    data_ = 0;
    return temp;
  }
  char *data() const { return data_; }
  char *&data() {
    dataAllocated_ = TRUE;
    return data_;
  }
  CmpMsgBufLenType &size() { return sz_; }

  virtual void destroyMe();
  virtual ~CmpMessageReply();

 private:
  CmpMessageReply &operator=(const CmpMessageReply &);
  CmpMessageReply(const CmpMessageReply &);

  CmpMsgBufLenType sz_;
  char *data_;
  // flag to identiy whethere data_ is allocated internally or taken from
  // preAllocatedData_.
  NABoolean dataAllocated_;

  // Executor wants to allocate the storage for the reply buffer.
  // preAllocatedData_ is the pre allocated buffer to hold the reply data
  // in unpackObj. preAllocatedDataSize_ is the size allocated.
  char *preAllocatedData_;
  CmpMsgBufLenType preAllocatedSize_;

  // This CollHeap* is used to allocate data_.
  CollHeap *outh_;
};  // end of CmpMessageReply

// -----------------------------------------------------------------------
// connection control messages
// -----------------------------------------------------------------------

class CmpMessageExit : public CmpMessageObj {
 public:
  CmpMessageExit(CollHeap *h = 0) : CmpMessageObj(EXIT_CONNECTION, h) {}
  ~CmpMessageExit() {}

 private:
  CmpMessageExit &operator=(const CmpMessageExit &);
  CmpMessageExit(const CmpMessageExit &);

};  // end of CmpMessageExit

class CmpMessageLast : public CmpMessageObj {
 public:
  CmpMessageLast(CollHeap *h = 0) : CmpMessageObj(LAST_MESSAGE, h) {}
  ~CmpMessageLast() {}

 private:
  CmpMessageLast &operator=(const CmpMessageLast &);
  CmpMessageLast(const CmpMessageLast &);
};  // end of CmpMessageLast

// -----------------------------------------------------------------------
// Requests from executor
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Run time environment setup messages
// -----------------------------------------------------------------------

class CmpMessageEnvs : public CmpMessageObj {
 public:
  // An EXGLOBALS request will be sent to arkcmp at the beginning of each request to make
  // sure arkcmp has same info as executor. It contains:
  // . environemnt variables
  // . current working directory
  // . transId
  // . .etc ....
  // In the future, for performance reason, it can be changed so that this request only
  // be sent when
  // . info changed in executor, needs to refresh arkcmp. this needs a trigger in the info
  //   change code.
  // . arkcmp dies, a new arkcmp is spawned from executor.
  enum EnvsOperatorEnum {
    NONE,
    EXGLOBALS,
    UNSETENV /* not used currently */
  };

  // constructors
  CmpMessageEnvs(EnvsOperatorEnum, char **envvars, NABoolean emptyOne, CollHeap *h);
  CmpMessageEnvs(EnvsOperatorEnum, char *, CollHeap *h = 0);

  // Ipc related routines
  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }
  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }
  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  int nEnvs() const { return nEnvs_; }
  char **envs() const { return envs_; }
  char *cwd() const { return cwd_; }
  NABoolean activeTrans() const { return activeTrans_ == 1; }
  long transId() const { return transId_; }
  EnvsOperatorEnum getOperator() const { return operator_; }

  virtual void destroyMe();
  virtual ~CmpMessageEnvs();

 private:
  CmpMessageEnvs(const CmpMessageEnvs &);
  CmpMessageEnvs &operator=(const CmpMessageEnvs &);

  EnvsOperatorEnum operator_;

  // Information sent to arkcmp from executor, these info need to be in synch with
  // executor.
  // envs_ is a null terminated array of string which is also null terminated
  int nEnvs_;
  char **envs_;

  // current working directory.
  char *cwd_;

  // for EXGLOBALS operator. It will pass the globals executor wants to share with
  // compiler (arkcmp).
  int activeTrans_;  // 0 if not. 1 if yes, transId_ then contains valid info
  long transId_;

};  // end of CmpMessageEnvs

// -----------------------------------------------------------------------
// messages to perform cleanup, userid change, etc.
// Issued from master exe when cleanup session command is issued,
// or userid changes, or something else need to be cleaned up.
// Currently only used for cleanupEsps.
// -----------------------------------------------------------------------

class CmpMessageEndSession : public CmpMessageRequest {
 public:
  CmpMessageEndSession(char *input_data, CmpMsgBufLenType size, CollHeap *h = 0)
      : CmpMessageRequest(END_SESSION, NULL, 0, h), flags_(0) {
    if (input_data) {
      int flags = *(int *)input_data;
      setCleanupEsps((flags & CLEANUP_ESPS) != 0);
      setResetAttrs((flags & RESET_ATTRS) != 0);
    }
  }
  virtual ~CmpMessageEndSession() {}

  enum { CLEANUP_ESPS = 0x0001, RESET_ATTRS = 0x0002, CLEAR_CACHE = 0x0004 };

  // Ipc related routines
  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }
  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }
  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  void setCleanupEsps(NABoolean v) { (v ? flags_ |= CLEANUP_ESPS : flags_ &= ~CLEANUP_ESPS); }
  NABoolean cleanupEsps() { return (flags_ & CLEANUP_ESPS) != 0; };

  void setResetAttrs(NABoolean v) { (v ? flags_ |= RESET_ATTRS : flags_ &= ~RESET_ATTRS); }
  NABoolean resetAttrs() { return (flags_ & RESET_ATTRS) != 0; };

  void setClearCache(NABoolean v) { (v ? flags_ |= CLEAR_CACHE : flags_ &= ~CLEAR_CACHE); }
  NABoolean clearCache() { return (flags_ & CLEAR_CACHE) != 0; };

 private:
  CmpMessageEndSession &operator=(const CmpMessageEndSession &);
  CmpMessageEndSession(const CmpMessageEndSession &);

  UInt32 flags_;
};  // end of CmpMessageEndSession

// -----------------------------------------------------------------------
// The SQL text message for SQL compilation
// -----------------------------------------------------------------------

class CmpMessageSQLText : public CmpMessageRequest {
 public:
  CmpMessageSQLText(char *sqltext = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0, int charset = SQLCHARSETCODE_UTF8,
                    MessageTypeEnum op = SQLTEXT_COMPILE)
      : CmpMessageRequest(op, sqltext, size, h, charset){};

  virtual ~CmpMessageSQLText(){};

 private:
  CmpMessageSQLText &operator=(const CmpMessageSQLText &);
  CmpMessageSQLText(const CmpMessageSQLText &);
};  // end of CmpMessageSQLText

// -----------------------------------------------------------------------
// The SQL text message for recompilation of a static statement.
// -----------------------------------------------------------------------

class CmpMessageCompileStmt : public CmpMessageRequest {
 public:
  CmpMessageCompileStmt(char *sqltext = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0, int cs = SQLCHARSETCODE_UTF8)
      : CmpMessageRequest(SQLTEXT_STATIC_COMPILE, sqltext, size, h, cs){};

  CmpMessageCompileStmt(char *sqltext, CmpMsgBufLenType size, MessageTypeEnum op, CollHeap *h = 0,
                        int cs = SQLCHARSETCODE_UTF8)
      : CmpMessageRequest(op, sqltext, size, h, cs){};

  virtual ~CmpMessageCompileStmt(){};

 private:
  CmpMessageCompileStmt &operator=(const CmpMessageCompileStmt &);
  CmpMessageCompileStmt(const CmpMessageCompileStmt &);
};  // end of CmpMessageCompileStmt

// -----------------------------------------------------------------------
// The DDL commands
// -----------------------------------------------------------------------

class CmpMessageDDL : public CmpMessageRequest {
 public:
  CmpMessageDDL(char *stmt = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0, int charset = SQLCHARSETCODE_UTF8,
                const char *parentQid = NULL, int parentQidLen = 0)
      : CmpMessageRequest(DDL, stmt, size, h, charset, parentQid, parentQidLen){};

  virtual ~CmpMessageDDL(){};

 private:
  CmpMessageDDL &operator=(const CmpMessageDDL &);
  CmpMessageDDL(const CmpMessageDDL &);
};  // end of CmpMessageDDL

// -----------------------------------------------------------------------
// Info about DDL operations that return status.
// -----------------------------------------------------------------------
class CmpDDLwithStatusInfo : public CmpCompileInfo {
 public:
  //  enum { MAX_MSG_LEN_ = 500; };

  enum {
    DONE_ = 0x0001,
    COMPUTE_ST_ = 0x0002,
    COMPUTE_ET_ = 0x0004,
    RETURN_ET_ = 0x0008,
    START_ = 0x0010,
    END_ = 0x0020,
    XN_STARTED_ = 0x0040,
    MD_UPGRADE_ = 0x0080,
    GET_MD_VERSION_ = 0x0100,
    GET_SW_VERSION_ = 0x0200,
    MD_CLEANUP_ = 0x0400,
    CHECK_ONLY_ = 0x0800,
    RETURN_DETAILS_ = 0x1000,
    INIT_TRAF_ = 0x2000,
    MINIMAL_IT_ = 0x4000,
    DDL_XNS_ = 0x8000
  };

 public:
  CmpDDLwithStatusInfo();

  CmpDDLwithStatusInfo(char *sourceStr, int sourceStrLen, int sourceStrCharSet, char *schemaName, int schemaNameLen);

  short getClassSize() { return (short)sizeof(CmpDDLwithStatusInfo); }
  int getLength();
  void pack(char *buffer);
  void unpack(char *base);

  void copyStatusInfo(CmpDDLwithStatusInfo *from);

  void setStartStep(NABoolean v) { (v ? statusFlags_ |= START_ : statusFlags_ &= ~START_); };
  NABoolean startStep() { return (statusFlags_ & START_) != 0; };

  void setEndStep(NABoolean v) { (v ? statusFlags_ |= END_ : statusFlags_ &= ~END_); };
  NABoolean endStep() { return (statusFlags_ & END_) != 0; };

  void setDone(NABoolean v) { (v ? statusFlags_ |= DONE_ : statusFlags_ &= ~DONE_); };
  NABoolean done() { return (statusFlags_ & DONE_) != 0; };

  void setReturnET(NABoolean v) { (v ? statusFlags_ |= RETURN_ET_ : statusFlags_ &= ~RETURN_ET_); };
  NABoolean returnET() { return (statusFlags_ & RETURN_ET_) != 0; };

  void setComputeST(NABoolean v) { (v ? statusFlags_ |= COMPUTE_ST_ : statusFlags_ &= ~COMPUTE_ST_); };
  NABoolean computeST() { return (statusFlags_ & COMPUTE_ST_) != 0; };

  void setComputeET(NABoolean v) { (v ? statusFlags_ |= COMPUTE_ET_ : statusFlags_ &= ~COMPUTE_ET_); };
  NABoolean computeET() { return (statusFlags_ & COMPUTE_ET_) != 0; };

  void setXnStarted(NABoolean v) { (v ? statusFlags_ |= XN_STARTED_ : statusFlags_ &= ~XN_STARTED_); };
  NABoolean xnStarted() { return (statusFlags_ & XN_STARTED_) != 0; };

  void setMDupgrade(NABoolean v) { (v ? statusFlags_ |= MD_UPGRADE_ : statusFlags_ &= ~MD_UPGRADE_); }
  NABoolean getMDupgrade() { return (statusFlags_ & MD_UPGRADE_) != 0; }

  void setGetMDVersion(NABoolean v) { (v ? statusFlags_ |= GET_MD_VERSION_ : statusFlags_ &= ~GET_MD_VERSION_); }
  NABoolean getMDVersion() { return (statusFlags_ & GET_MD_VERSION_) != 0; }

  void setGetSWVersion(NABoolean v) { (v ? statusFlags_ |= GET_SW_VERSION_ : statusFlags_ &= ~GET_SW_VERSION_); }
  NABoolean getSWVersion() { return (statusFlags_ & GET_SW_VERSION_) != 0; }

  void setMDcleanup(NABoolean v) { (v ? statusFlags_ |= MD_CLEANUP_ : statusFlags_ &= ~MD_CLEANUP_); }
  NABoolean getMDcleanup() { return (statusFlags_ & MD_CLEANUP_) != 0; }

  void setCheckOnly(NABoolean v) { (v ? statusFlags_ |= CHECK_ONLY_ : statusFlags_ &= ~CHECK_ONLY_); }
  NABoolean getCheckOnly() { return (statusFlags_ & CHECK_ONLY_) != 0; }

  void setReturnDetails(NABoolean v) { (v ? statusFlags_ |= RETURN_DETAILS_ : statusFlags_ &= ~RETURN_DETAILS_); }
  NABoolean getReturnDetails() { return (statusFlags_ & RETURN_DETAILS_) != 0; }

  void setInitTraf(NABoolean v) { (v ? statusFlags_ |= INIT_TRAF_ : statusFlags_ &= ~INIT_TRAF_); }
  NABoolean getInitTraf() { return (statusFlags_ & INIT_TRAF_) != 0; }

  void setMinimalInitTraf(NABoolean v) { (v ? statusFlags_ |= MINIMAL_IT_ : statusFlags_ &= ~MINIMAL_IT_); }
  NABoolean getMinimalInitTraf() { return (statusFlags_ & MINIMAL_IT_) != 0; }

  void setDDLXns(NABoolean v) { (v ? statusFlags_ |= DDL_XNS_ : statusFlags_ &= ~DDL_XNS_); }
  NABoolean getDDLXns() { return (statusFlags_ & DDL_XNS_) != 0; }

  void setMsg(const char *msg) {
    msgLen_ = strlen(msg);
    strcpy(msg_, msg);
  }

  char *msg() { return msg_; }
  int msgLen() { return msgLen_; }

  int &step() { return step_; }
  int &subStep() { return substep_; }

  void setStep(int step) { step_ = step; }
  void setSubstep(int substep) { substep_ = substep; }

  int myPin() { return myPin_; }
  void setMyPin(int myPin) { myPin_ = myPin; }

  int blackBoxLen() { return blackBoxLen_; }
  char *blackBox() { return blackBox_; }
  void setBlackBoxLen(int v) { blackBoxLen_ = v; }
  void setBlackBox(char *v) { blackBox_ = v; }

 private:
  int step_;
  int substep_;
  UInt32 statusFlags_;
  int msgLen_;
  char msg_[512];
  int myPin_;

  // black box data that can be sent and received. Contents are interpreted
  // based on the query that is being processed.
  int blackBoxLen_;
  char *blackBox_;
};

class CmpMessageDDLwithStatus : public CmpMessageRequest {
 public:
  CmpMessageDDLwithStatus(char *stmt, CmpMsgBufLenType size, CollHeap *h = 0, int charset = SQLCHARSETCODE_UTF8,
                          const char *parentQid = NULL, int parentQidLen = 0)
      : CmpMessageRequest(DDL_WITH_STATUS, stmt, size, h, charset, parentQid, parentQidLen){};

  virtual ~CmpMessageDDLwithStatus(){};

  CmpDDLwithStatusInfo *getCmpDDLwithStatusInfo() const { return (CmpDDLwithStatusInfo *)data(); };

 private:
  CmpMessageDDLwithStatus &operator=(const CmpMessageDDLwithStatus &);
  CmpMessageDDLwithStatus(const CmpMessageDDLwithStatus &);

};  // end of CmpMessageDDLwithStatus

// -----------------------------------------------------------------------
// The Describe command
// -----------------------------------------------------------------------

class CmpMessageDescribe : public CmpMessageRequest {
 public:
  CmpMessageDescribe(char *stmt = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0, int charset = SQLCHARSETCODE_UTF8)
      : CmpMessageRequest(DESCRIBE, stmt, size, h, charset){};

  virtual ~CmpMessageDescribe(){};

 private:
  CmpMessageDescribe &operator=(const CmpMessageDescribe &);
  CmpMessageDescribe(const CmpMessageDescribe &);
};  // end of CmpMessageDescribe

// -----------------------------------------------------------------------
// The UPDATE_HIST_STAT
// -----------------------------------------------------------------------

class CmpMessageUpdateHist : public CmpMessageRequest {
 public:
  CmpMessageUpdateHist(char *stmt = 0, CmpMsgBufLenType size = 0, CollHeap *h = 0, int charset = SQLCHARSETCODE_UTF8)
      : CmpMessageRequest(UPDATE_HIST_STAT, stmt, size, h, charset) {}
  virtual ~CmpMessageUpdateHist() {}
};  // end of CmpMessageUpdateHist

class CmpMessageMFStmt : public CmpMessageObj {};  // end of CmpMessageMFStmt

typedef struct CmpMessageXnOperData {
  int type;
  long svptId;
} CmpMessageXnOperData;

// -----------------------------------------------------------------------
// XnOper: msg from master exe to cmp indicating if begin/commit/rollback
//         was issued.
// -----------------------------------------------------------------------
class CmpMessageXnOper : public CmpMessageRequest {
 public:
  CmpMessageXnOper(char *stmt = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0)
      : CmpMessageRequest(XN_OPER, stmt, size, h){};

  virtual ~CmpMessageXnOper(){};

 private:
  CmpMessageXnOper &operator=(const CmpMessageXnOper &);
  CmpMessageXnOper(const CmpMessageXnOper &);
};  // end of CmpMessageXnOper

// -----------------------------------------------------------------------
// The SET TRANSACTION command
// -----------------------------------------------------------------------

class CmpMessageSetTrans : public CmpMessageRequest {
 public:
  CmpMessageSetTrans(char *stmt = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0)
      : CmpMessageRequest(SET_TRANS, stmt, size, h){};

  virtual ~CmpMessageSetTrans(){};

 private:
  CmpMessageSetTrans &operator=(const CmpMessageSetTrans &);
  CmpMessageSetTrans(const CmpMessageSetTrans &);
};  // end of CmpMessageSetTrans

// -----------------------------------------------------------------------
// The DDL NATABLE INVALIDATION command
// This command is send from executor to arkcmp at the end of a
// transaction. On receiving it, arkcmp invalidates NATable for ddl objects
// that were part of that transaction. They are then reloaded when that
// object is accessed.
// -----------------------------------------------------------------------
class CmpMessageDDLNATableInvalidate : public CmpMessageRequest {
 public:
  CmpMessageDDLNATableInvalidate(char *stmt = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0)
      : CmpMessageRequest(DDL_NATABLE_INVALIDATE, stmt, size, h){};

  virtual ~CmpMessageDDLNATableInvalidate(){};

 private:
  CmpMessageDDLNATableInvalidate &operator=(const CmpMessageDDLNATableInvalidate &);
  CmpMessageDDLNATableInvalidate(const CmpMessageDDLNATableInvalidate &);
};  // end of CmpMessageDDLNATableInvalidate

// -------------------------------------------------------------------------
// The DDL ROLLBACK SHARED CACHE command
// This command is sent from executor to compiler at rollback transaction.
// On receiving it, shared cache changes are rolled back.
// -------------------------------------------------------------------------

class CmpMessageDDLRollbackSharedCache : public CmpMessageRequest {
 public:
  CmpMessageDDLRollbackSharedCache(char *stmt = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0)
      : CmpMessageRequest(DDL_ROLLBACK_SHARED_CACHE, stmt, size, h){};

  virtual ~CmpMessageDDLRollbackSharedCache(){};

 private:
  CmpMessageDDLRollbackSharedCache &operator=(const CmpMessageDDLRollbackSharedCache &);
  CmpMessageDDLRollbackSharedCache(const CmpMessageDDLRollbackSharedCache &);
};  // end of CmpMessageDDLRollbackSharedCache

// -------------------------------------------------------------------------
// The DDL COMMIT SHARED CACHE command
// This command is sent from executor to compiler at commit transaction.
// On receiving it, shared cache changes are applied.
// -------------------------------------------------------------------------

class CmpMessageDDLCommitSharedCache : public CmpMessageRequest {
 public:
  CmpMessageDDLCommitSharedCache(char *stmt = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0)
      : CmpMessageRequest(DDL_COMMIT_SHARED_CACHE, stmt, size, h){};

  virtual ~CmpMessageDDLCommitSharedCache(){};

 private:
  CmpMessageDDLCommitSharedCache &operator=(const CmpMessageDDLCommitSharedCache &);
  CmpMessageDDLCommitSharedCache(const CmpMessageDDLCommitSharedCache &);
};  // end of CmpMessageDDLCommitSharedCache

// -----------------------------------------------------------------------
// Database user ID
// -----------------------------------------------------------------------
class CmpMessageDatabaseUser : public CmpMessageRequest {
 public:
  CmpMessageDatabaseUser(char *stmt = NULL, CmpMsgBufLenType size = 0, CollHeap *h = 0)
      : CmpMessageRequest(DATABASE_USER, stmt, size, h) {}

  virtual ~CmpMessageDatabaseUser() {}

 private:
  CmpMessageDatabaseUser &operator=(const CmpMessageDatabaseUser &);
  CmpMessageDatabaseUser(const CmpMessageDatabaseUser &);
};  // end of CmpMessageDatabaseUser

// -----------------------------------------------------------------------
// The reply code message from arkcmp to executor,
// containing the code generated by the generator.
// -----------------------------------------------------------------------

class CmpMessageReplyCode : public CmpMessageReply {
 public:
  CmpMessageReplyCode(CollHeap *h = 0, ID request = 0, char *preAllocatedData = 0, int preAllocatedSize = 0,
                      CollHeap *outh_ = 0, FragmentDir *fragmentDir = NULL)
      : CmpMessageReply(REPLY_CODE, request, h, preAllocatedData, preAllocatedSize, outh_), fragmentDir_(fragmentDir) {}

  virtual ~CmpMessageReplyCode();

  virtual void destroyMe();

  FragmentDir *getFragmentDir() { return fragmentDir_; }
  void setFragmentDir(FragmentDir *fd) { fragmentDir_ = fd; }

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }

  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }

  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);

  IpcMessageObjSize copyFragsToBuffer(IpcMessageBufferPtr &buffer);

 private:
  CmpMessageReplyCode &operator=(const CmpMessageReplyCode &);
  CmpMessageReplyCode(const CmpMessageReplyCode &);

  FragmentDir *fragmentDir_;
};  // end of CmpMessageReplyCode

// -----------------------------------------------------------------------
// Class declarations for the CmpMessageObjects for internal stored
// procedure execution.
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// CmpMessageISPRequest is the execution of SP request sent from executor.
// -----------------------------------------------------------------------

class CmpMessageISPRequest : public CmpMessageRequest {
 public:
  // When executor instantiate this object.
  // The pointers will be copied into this object, not the contents
  // because of the performance reason.
  // So the caller should not free the memory before the object freed
  // When compiler instantiate this object.
  // The object will be unpacked from the IPC buffers, i.e. the member
  // will be allocated and contents will be copied over. Then this class
  // owns the pointers and delete them at the desructor time.
  //
  CmpMessageISPRequest(char *procName = 0, void *inputExpr = 0, int inputExprSize = 0, void *outputExpr = 0,
                       int outputExprSize = 0, void *keyExpr = 0, int keyExprSize = 0, void *inputData = 0,
                       int inputDataSize = 0, int ouputRowSize = 0, int outputTotalSize = 0, CollHeap *h = 0,
                       const char *parentQid = NULL, int parentQidLen = 0);

  virtual ~CmpMessageISPRequest();

  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }
  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }
  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  virtual void destroyMe();

  // methods to access private members

  const char *procName() const { return procName_; }
  const int inputExprSize() const { return inputExprSize_; }
  const void *inputExpr() const { return inputExpr_; }
  const int outputExprSize() const { return outputExprSize_; }
  const void *outputExpr() const { return outputExpr_; }
  const int keyExprSize() const { return keyExprSize_; }
  const void *keyExpr() const { return keyExpr_; }
  const int inputDataSize() const { return inputDataSize_; }
  const void *inputData() const { return inputData_; }
  const int keyDataSize() const { return keyDataSize_; }
  const void *keyData() const { return keyData_; }
  const int outputRowSize() const { return outputRowSize_; }
  const int outputTotalSize() const { return outputTotalSize_; }

 private:
  CmpMessageISPRequest(const CmpMessageISPRequest &);
  const CmpMessageISPRequest &operator=(const CmpMessageISPRequest &);

  // members, information provided by executor
  char *procName_;  // ISP name, null terminated

  // The expressions are packed as (length, data) by executor before
  // construct this object. executor will provide methods to access
  // the values.
  int inputExprSize_;   // size of the inputExpr
  void *inputExpr_;     // expression for input data
  int outputExprSize_;  // size of the outputExpr
  void *outputExpr_;    // expression for output data
  int keyExprSize_;     // size of the key data
  void *keyExpr_;       // expression for key expression

  // The data is packed by executor before construct this object.
  // executor will provide methods to access the values.

  int inputDataSize_;  // size of input data
  void *inputData_;    // input data
  int keyDataSize_;    // size of the key data
  void *keyData_;      // key data

  int outputRowSize_;    // the size of each row of output data
  int outputTotalSize_;  // buffer size for output data, once the
  // output exceeds the limit, it should be sent back in multiple
  // replies.

  // internal flag to identify whether the void* for data are allocated
  // internally in this class.
  // in executor side, where the constructor is called with parameters
  // the allocated_ flag will be FALSE, and the caller owns the pointers.
  // in compiler, where the constructor is called with 0 parameters, and
  // data is unpacked from IPC buffer, allocated_flag will be set to TRUE,
  // and this class owns ths pointers, should delete them in destructor.
  NABoolean allocated_;

};  // end of CmpMessageISPRequest

// -----------------------------------------------------------------------
// CmpMessageISPGetNext is the request to arkcmp from executor to get
// more data. This is used to fetch more data when the previous
// CmpMessageISPReply returns with status as more data to be fetched in
// arkcmp site.
// -----------------------------------------------------------------------

class CmpMessageISPGetNext : public CmpMessageRequest {
 public:
  CmpMessageISPGetNext(int bufSize = 0, ID ispRequest = 0, int serialNo = 0, CollHeap *h = 0,
                       const char *parentQid = NULL, int parentQidLen = 0);
  virtual ~CmpMessageISPGetNext();

  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }
  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }
  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  int bufSize() const { return bufSize_; }
  ID ispRequest() const { return ispRequest_; }

 private:
  CmpMessageISPGetNext(const CmpMessageISPGetNext &);
  const CmpMessageISPGetNext &operator=(const CmpMessageISPGetNext &);

  // members
  int bufSize_;
  ID ispRequest_;
  int serialNo_;
};

// -----------------------------------------------------------------------
// CmpMessageISPReply is the reply of the execution of SP request sent
// from compiler.
// -----------------------------------------------------------------------

class CmpMessageReplyISP : public CmpMessageReply {
 public:
  CmpMessageReplyISP(CollHeap *h = 0, ID request = 0, char *preAllocatedData = 0, int preAllocatedSize = 0,
                     CollHeap *outh_ = 0)
      : CmpMessageReply(REPLY_ISP, request, h, preAllocatedData, preAllocatedSize, outh_) {
    areMore_ = 0;
  }

  virtual ~CmpMessageReplyISP() {}

  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }
  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }
  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  void setAreMore(NABoolean b) { areMore_ = b; }
  NABoolean areMore() { return areMore_; }

 private:
  CmpMessageReplyISP &operator=(const CmpMessageReplyISP &);
  CmpMessageReplyISP(const CmpMessageReplyISP &);

  // 0 if there is more data to be fetched, 1 if there is no more.
  int areMore_;
};  // end of CmpMessageReplyISP

// -----------------------------------------------------------------------
// CmpMessageConnectionType is the message sent from executor to arkcmp
// to specify the property of connection, i.e. for DML/DDL or ISP execution.
// This message is sent at the beginning of each connection, so arkcmp can
// skip the functions only needed for DML/DDL. It saves resource for arkcmp
// spawned in ISP execution.
// -----------------------------------------------------------------------

class CmpMessageConnectionType : public CmpMessageRequestBasic {
 public:
  enum ConnectionTypeEnum { DMLDDL, ISP };
  CmpMessageConnectionType(ConnectionTypeEnum e = DMLDDL, CollHeap *h = 0);

  ConnectionTypeEnum connectionType() { return type_; }

  IpcMessageObjSize mypackedLength();
  IpcMessageObjSize packMyself(IpcMessageBufferPtr &buffer);
  void unpackMyself(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                    IpcMessageObjSize objSize, IpcConstMessageBufferPtr &buffer);

  virtual IpcMessageObjSize packedLength() { return mypackedLength(); }
  virtual IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer) { return packMyself(buffer); }
  virtual void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                         IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer) {
    unpackMyself(objType, objVersion, sameEndianness, objSize, buffer);
  }

  virtual ~CmpMessageConnectionType() {}

 private:
  ConnectionTypeEnum type_;
  CmpMessageConnectionType &operator=(const CmpMessageConnectionType &);
  CmpMessageConnectionType(const CmpMessageConnectionType &);

};  // end of CmpMessageConnectionType
#endif
