/**********************************************************************

 **********************************************************************/
#ifndef EX_EXE_UTIL_H
#define EX_EXE_UTIL_H

/* -*-C++-*-
*****************************************************************************
*
* File:         ExExeUtil.h
* Description:
*
*
* Created:      9/12/2005
* Language:     C++
*
*
*
*
*****************************************************************************
*/

// forward
class ex_expr;
class Queue;
class ExTransaction;
class ContextCli;
class IpcServer;
class IpcServerClass;
class ExStatisticsArea;
class CmpDDLwithStatusInfo;
class ExSqlComp;

class ExProcessStats;

class ExpHbaseInterface;
class ExpExtStorageInterface;

// class FILE_STREAM;
#include "SequenceFileReader.h"
#include "comexe/ComTdbExeUtil.h"
#include "comexe/ComTdbRoot.h"
#include "common/ComAnsiNamePart.h"
#include "common/ComRtUtils.h"
#include "executor/ExExeUtilCli.h"
#include "exp/ExpHbaseDefs.h"
#include "exp/ExpLOBstats.h"

#define TO_FMT3u(u)                MINOF(((u) + 500) / 1000, 999)
#define MAX_ACCUMULATED_STATS_DESC 2
#define MAX_PERTABLE_STATS_DESC    256
#define MAX_PROGRESS_STATS_DESC    256
#define MAX_OPERATOR_STATS_DESC    512
#define MAX_RMS_STATS_DESC         512
#define BUFFER_SIZE                4000
#define MAX_AUTHIDTYPE_CHAR        11
#define MAX_USERINFO_CHAR          257
// max number of attempts to create ORSERV upon failure
#define MAX_CREATE_ORSERV_COUNT 5

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExExeUtilTdb;
class ExExeUtilDisplayExplainTdb;
class ExExeUtilDisplayExplainComplexTdb;
class ExExeUtilSuspendTdb;
class ExExeUtilSuspendTcb;
class ExpHbaseInterface;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;

// -----------------------------------------------------------------------
// ExExeUtilTdb
// -----------------------------------------------------------------------
class ExExeUtilTdb : public ComTdbExeUtil {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilTdb() {}

  virtual ~ExExeUtilTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

struct QueryString {
 public:
  const char *str;
};

//
// Task control block
//
class ExExeUtilTcb : public ex_tcb {
  friend class ExExeUtilTdb;
  friend class ExExeUtilPrivateState;

 public:
  enum Step { EMPTY_, PROCESSING_, DONE_, HANDLE_ERROR_, CANCELLED_ };

  enum ProcessQueryStep { PROLOGUE_, EXECUTE_, FETCH_ROW_, RETURN_ROW_, ERROR_RETURN_, CLOSE_, EPILOGUE_, ALL_DONE_ };

  // Constructor
  ExExeUtilTcb(const ComTdbExeUtil &exe_util_tdb,
               const ex_tcb *child_tcb,  // for child queue
               ex_globals *glob = 0);

  ~ExExeUtilTcb();

  virtual short work();

  ex_queue_pair getParentQueue() const;
  int orderedQueueProtocol() const;

  virtual void freeResources();

  virtual int numChildren() const;
  virtual const ex_tcb *getChild(int pos) const;

  void glueQueryFragments(int queryArraySize, const QueryString *queryArray, char *&gluedQuery, int &gluedQuerySize);

  // extract parts from 'objectName' and fixes up delimited names.
  int extractParts(char *objectName, char **parts0, char **parts1, char **parts2, NABoolean noValidate = FALSE);

  virtual short moveRowToUpQueue(const char *row, int len = -1, short *rc = NULL, NABoolean isVarchar = TRUE);

  NABoolean isUpQueueFull(short size);

  static char *getTimeAsString(long t, char *timeBuf, NABoolean noUsec = FALSE);
  char *getTimestampAsString(long t, char *timeBuf);

  short initializeInfoList(Queue *&infoList);
  short fetchAllRows(Queue *&infoList, char *query, int numOutputEntries, NABoolean varcharFormat, short &rc,
                     NABoolean monitorThis = FALSE);

  ex_expr::exp_return_type evalScanExpr(char *ptr, int len, NABoolean copyToVCbuf);

  char *getStatusString(const char *operation, const char *status, const char *object, char *outBuf,
                        NABoolean isET = FALSE, char *timeBuf = NULL, char *queryBuf = NULL, char *sqlcodeBuf = NULL);

  short executeQuery(char *step, char *object, char *query, NABoolean displayStartTime, NABoolean displayEndTime,
                     short &rc, short *warning, int *errorCode = NULL, NABoolean moveErrorRow = TRUE,
                     NABoolean continueOnError = FALSE, NABoolean monitorThis = FALSE);

  ExeCliInterface *cliInterface() { return cliInterface_; };
  ExeCliInterface *cliInterface2() { return cliInterface2_; };

  ComDiagsArea *&getDiagsArea() { return diagsArea_; }

  void setDiagsArea(ComDiagsArea *d) { diagsArea_ = d; }

  short setSchemaVersion(char *param1);

  short setSystemVersion();

  short holdAndSetCQD(const char *defaultName, const char *defaultValue, ComDiagsArea *globalDiags = NULL);

  short restoreCQD(const char *defaultName, ComDiagsArea *globalDiags = NULL);

  short setCS(const char *csName, char *csValue, ComDiagsArea *globalDiags = NULL);
  short resetCS(const char *csName, ComDiagsArea *globalDiags = NULL);

  void setMaintainControlTableTimeout(char *catalog);
  void restoreMaintainControlTableTimeout(char *catalog);

  static int holdAndSetCQD(const char *defaultName, const char *defaultValue, ExeCliInterface *cliInterface,
                           ComDiagsArea *globalDiags = NULL);

  static int restoreCQD(const char *defaultName, ExeCliInterface *cliInterface, ComDiagsArea *globalDiags = NULL);

  static int setCS(const char *csName, char *csValue, ExeCliInterface *cliInterface, ComDiagsArea *globalDiags = NULL);
  static int resetCS(const char *csName, ExeCliInterface *cliInterface, ComDiagsArea *globalDiags = NULL);

  short disableCQS();
  short restoreCQS();

  short doubleQuoteStr(char *str, char *newStr, NABoolean singleQuote);

  char *getSchemaVersion() { return (strlen(versionStr_) == 0 ? (char *)"" : versionStr_); }

  char *getSystemVersion() { return (strlen(sysVersionStr_) == 0 ? NULL : sysVersionStr_); }

  int getSchemaVersionLen() { return versionStrLen_; }

  short getObjectUid(char *catName, char *schName, char *objName, NABoolean isIndex, NABoolean isMv, char *uid);

  long getObjectFlags(long objectUID);

  short lockUnlockObject(char *tableName, NABoolean lock, NABoolean parallel, char *failReason);

  short alterObjectState(NABoolean online, char *tableName, char *failReason, NABoolean forPurgedata);
  short alterDDLLock(NABoolean add, char *tableName, char *failReason, NABoolean isMV, int lockType,
                     const char *lockPrefix = NULL, NABoolean skipDDLLockCheck = FALSE);
  short alterCorruptBit(short val, char *tableName, char *failReason, Queue *indexList);

  short alterAuditFlag(NABoolean audited, char *tableName, NABoolean isIndex);

  short handleError();

  short handleDone();

  NABoolean &infoListIsOutputInfo() { return infoListIsOutputInfo_; }

  // Error: -1, creation of IpcServerClass failed.
  //        -2, PROCESSNAME_CREATE_ failed.
  //        -3, allocateServerProcess failed.
  short createServer(char *serverName, const char *inPName, IpcServerTypeEnum serverType,
                     IpcServerAllocationMethod servAllocMethod, char *nodeName, short cpu, const char *partnName,
                     int priority, IpcServer *&ipcServer, NABoolean logError, const char *operation);

  void deleteServer(IpcServer *ipcServer);

  NABoolean isProcessObsolete(short cpu, pid_t pin, short segmentNum, long procCreateTime);

 protected:
  const ex_tcb *childTcb_;

  ex_queue_pair qparent_;
  ex_queue_pair qchild_;

  atp_struct *workAtp_;

  ComDiagsArea *diagsArea_;

  char *query_;

  unsigned short tcbFlags_;

  char *explQuery_;

  ExExeUtilTdb &exeUtilTdb() const { return (ExExeUtilTdb &)tdb; };
  void handleErrors(int error);

  CollHeap *getMyHeap() { return (getGlobals()->getDefaultHeap()); };

  void AddCommas(char *outStr, int &intSize) const;
  void FormatFloat(char *outStr, int &intSize, int &fullSize, double floatVal, NABoolean normalMode,
                   NABoolean expertMode) const;

  Queue *infoList_;
  NABoolean infoListIsOutputInfo_;
  char *childQueryId_;
  int childQueryIdLen_;
  SQL_QUERY_COST_INFO childQueryCostInfo_;
  SQL_QUERY_COMPILER_STATS_INFO childQueryCompStatsInfo_;
  char *outputBuf_;

  char failReason_[2000];

 private:
  ExeCliInterface *cliInterface_;
  ExeCliInterface *cliInterface2_;

  ProcessQueryStep pqStep_;

  char versionStr_[10];
  int versionStrLen_;

  char sysVersionStr_[10];
  int sysVersionStrLen_;

  NABoolean restoreTimeout_;

  AnsiName *extractedPartsObj_;

  long startTime_;
  long endTime_;
  long elapsedTime_;

  short warning_;
};

class ExExeUtilPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilTcb;
  friend class ExExeUtilCleanupVolatileTablesTcb;
  friend class ExExeUtilCreateTableAsTcb;
  friend class ExExeUtilAQRTcb;
  friend class ExExeUtilHBaseBulkLoadTcb;
  friend class ExExeUtilUpdataDeleteTcb;

 public:
  ExExeUtilPrivateState(const ExExeUtilTcb *tcb);  // constructor
  ~ExExeUtilPrivateState();                        // destructor
  ex_tcb_private_state *allocate_new(const ex_tcb *tcb);

 protected:
  ExExeUtilTcb::Step step_;
  long matches_;
};

// -----------------------------------------------------------------------
// ExExeUtilDisplayExplainTdb
// -----------------------------------------------------------------------
class ExExeUtilDisplayExplainTdb : public ComTdbExeUtilDisplayExplain {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilDisplayExplainTdb() {}

  virtual ~ExExeUtilDisplayExplainTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//
// Task control block
//
class ExExeUtilDisplayExplainTcb : public ExExeUtilTcb {
  friend class ExExeUtilDisplayExplainTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilDisplayExplainTcb(const ComTdbExeUtilDisplayExplain &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilDisplayExplainTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  short processExplainRows();

  // state machine states for work() method
  enum Step {
    EMPTY_,
    PREPARE_,
    SETUP_EXPLAIN_,
    FETCH_PROLOGUE_,
    FETCH_EXPLAIN_ROW_,
    FETCH_FIRST_EXPLAIN_ROW_,
    GET_COLUMNS_,
    DO_HEADER_,
    DO_OPERATOR_,
    RETURN_FMT_ROWS_,
    RETURN_EXPLAIN_ROW_,
    FETCH_EPILOGUE_,
    FETCH_EPILOGUE_AND_RETURN_ERROR_,
    DONE_,
    HANDLE_ERROR_,
    RETURN_ERROR_,
    CANCELLED_
  };

 protected:
 private:
  // Enums
  //  enum { MLINE = 74    };          // max number of lines in the array, 3000/50 + 14
  //  enum { MLEN  = 80    };          // max length of each line
  enum { MNAME = 60 };     // width of the returned SQL name columns
  enum { MOPER = 30 };     // width of the returned SQL operator column
  enum { MCOST = 200 };    // width of the returned SQL detailed_cost column
  enum { MDESC = 30000 };  // width of the returned SQL description column
  enum { MUSERSP = 20 };   // max output number space for user
  //  enum { MWIDE = MLEN-1};          // max line width in char for output
  enum { COL2 = 28 };  // position of column 2 (value) on the line
  enum Option          // which output format we are using
  { N_,
    E_,
    F_,
    M_ };

  // Variables
  SQLMODULE_ID *module_;
  SQLSTMT_ID *stmt_;
  SQLDESC_ID *sql_src_;
  SQLDESC_ID *input_desc_;
  SQLDESC_ID *output_desc_;
  char *outputBuf_;
  char *explainQuery_;

  Option optFlag_;  // option flag
  char **lines_;
  //  char  lines_[MLINE][MLEN];    // array of MLINE lines MLEN char wide
  int cntLines_;            // count of lines in lines_ array
  int nextLine_;            // number of next line to output from array
  char *parsePtr_;          // location in input string being parsed
  int header_;              // flag saying current node is root if 1
  int lastFrag_;            // previous row fragment number
  char lastOp_[MOPER + 1];  // previous row operator name

  char *optFOutput;
  //  char  optFOutput[MLEN];        // formatted line for optionsF

  // Next 13 are the local column data, storage for one node's info
  char moduleName_[MNAME + 1];     // module name column, sometimes null, MNAME=60
  char statementName_[MNAME + 1];  // stmt name column, sometimes null, MNAME=60
  long planId_;                    // large number, unique per plan
  int seqNum_;                     // number of this node
  char operName_[MOPER + 1];       // operator name, MOPER=30
  int leftChild_;                  // number of left child
  int rightChild_;                 // number of right child
  char tName_[MNAME + 1];          // table name, often null, MNAME=60
  float cardinality_;              // number of rows returned by this node
  float operatorCost_;             // cost of this node alone, in seconds
  float totalCost_;                // cost of this node and all children, in seconds
  char detailCost_[MCOST + 1];     // node cost in 5 parts in key-value format, MCOST=200
  char description_[MDESC + 1];    // other attributs in key-value format, MDESC=3000

  int MLINE;
  int MLEN;
  int MWIDE;

  // Methods
  short GetColumns();
  void FormatForF();  // formats the line for optionsF and places into optFOutput
  void DoHeader();
  void DoOperator();
  short OutputLines();
  void truncate_whitespace(char *str) const;
  void DoSeparator();
  // void  AddCommas (char *outStr, long &intSize) const;
  // void  FormatFloat (char *outStr, long &intSize, long &fullSize, float floatVal) const;
  void FormatNumber(char *outStr, int &intSize, int &fullSize, char *strVal) const;
  int GetField(char *col, const char *key, char *&fieldptr, int &fullSize) const;
  int ParseField(char *&keyptr, char *&fieldptr, int &keySize, int &fullSize, int &done);
  int IsNumberFmt(char *fieldptr) const;
  void FormatFirstLine(void);
  NABoolean filterKey(const char *key, int keySize, char *value, char *retVal, int &decLoc);
  void FormatLine(const char *key, const char *val, int keySize, int valSize, int indent = 0, int decLoc = 0);
  void FormatLongLine(const char *key, char *val, int keySize, int valSize, int indent = 0);
  void FormatSQL(const char *key, char *val, int keySize, int valSize, int indent = 0);
  int FindParens(char *inStr, int par[]) const;

  ExExeUtilDisplayExplainTdb &exeUtilTdb() const { return (ExExeUtilDisplayExplainTdb &)tdb; };
};

class ExExeUtilDisplayExplainPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilDisplayExplainTcb;

 public:
  ExExeUtilDisplayExplainPrivateState();
  ~ExExeUtilDisplayExplainPrivateState();  // destructor
 protected:
  ExExeUtilDisplayExplainTcb::Step step_;
  long matches_;
};

//////////////////////////////////////////////////////////////////////////

// -----------------------------------------------------------------------
// ExExeUtilDisplayExplainComplexTdb
// -----------------------------------------------------------------------
class ExExeUtilDisplayExplainComplexTdb : public ComTdbExeUtilDisplayExplainComplex {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilDisplayExplainComplexTdb() {}

  virtual ~ExExeUtilDisplayExplainComplexTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//
// Task control block
//
class ExExeUtilDisplayExplainComplexTcb : public ExExeUtilTcb {
  friend class ExExeUtilDisplayExplainComplexTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilDisplayExplainComplexTcb(const ComTdbExeUtilDisplayExplainComplex &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilDisplayExplainComplexTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

 private:
  // state machine states for work() method
  enum Step {
    EMPTY_,
    TURN_ON_IMOD_,
    IN_MEMORY_BASE_TABLE_CREATE_,
    IN_MEMORY_CREATE_,
    REGULAR_CREATE_,
    EXPLAIN_CREATE_,
    EXPLAIN_INSERT_SELECT_,
    DROP_AND_ERROR_,
    DROP_AND_DONE_,
    DONE_,
    ERROR_,
    CANCELLED_
  };

  ExExeUtilDisplayExplainComplexTdb &exeUtilTdb() const { return (ExExeUtilDisplayExplainComplexTdb &)tdb; };

  Step step_;
};

class ExExeUtilDisplayExplainComplexPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilDisplayExplainComplexTcb;

 public:
  ExExeUtilDisplayExplainComplexPrivateState();
  ~ExExeUtilDisplayExplainComplexPrivateState();  // destructor
 protected:
  long matches_;
};

class ExExeUtilDisplayExplainShowddlTcb : public ExExeUtilTcb {
  friend class ExExeUtilDisplayExplainComplexTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilDisplayExplainShowddlTcb(const ComTdbExeUtilDisplayExplainComplex &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilDisplayExplainShowddlTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

 private:
  // state machine states for work() method
  enum Step { EMPTY_, GET_LABEL_STATS_, EXPLAIN_CREATE_, DONE_, ERROR_, CANCELLED_ };

  ExExeUtilDisplayExplainComplexTdb &exeUtilTdb() const { return (ExExeUtilDisplayExplainComplexTdb &)tdb; };

  Step step_;

  char *newQry_;
};

class ExExeUtilDisplayExplainShowddlPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilDisplayExplainShowddlTcb;

 public:
  ExExeUtilDisplayExplainShowddlPrivateState();
  ~ExExeUtilDisplayExplainShowddlPrivateState();  // destructor
 protected:
  long matches_;
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilCreateTableAsTdb
// -----------------------------------------------------------------------
class ExExeUtilCreateTableAsTdb : public ComTdbExeUtilCreateTableAs {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilCreateTableAsTdb() {}

  virtual ~ExExeUtilCreateTableAsTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExExeUtilCreateTableAsTcb : public ExExeUtilTcb {
  friend class ExExeUtilCreateTableAsTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilCreateTableAsTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob = 0);

  virtual short work();

  ExExeUtilCreateTableAsTdb &ctaTdb() const { return (ExExeUtilCreateTableAsTdb &)tdb; };

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

 private:
  enum Step {
    INITIAL_,
    CREATE_,
    TRUNCATE_TABLE_,
    INSERT_ROWS_,
    INSERT_SIDETREE_,
    INSERT_VSBB_,
    UPD_STATS_,
    DONE_,
    HANDLE_ERROR_,
    TRUNCATE_TABLE_AND_ERROR_,
    ERROR_,
    DROP_AND_ERROR_,
    DROP_AND_DONE_,
    INSERT_SIDETREE_EXECUTE_,
    INSERT_VSBB_EXECUTE_
  };

  Step step_;

  NABoolean doSidetreeInsert_;

  NABoolean tableExists_;
};

class ExExeUtilCreateTableAsPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilCreateTableAsTcb;

 public:
  ExExeUtilCreateTableAsPrivateState();
  ~ExExeUtilCreateTableAsPrivateState();  // destructor
 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilCleanupVolatileTablesTdb
// -----------------------------------------------------------------------
class ExExeUtilCleanupVolatileTablesTdb : public ComTdbExeUtilCleanupVolatileTables {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilCleanupVolatileTablesTdb() {}

  virtual ~ExExeUtilCleanupVolatileTablesTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExExeUtilVolatileTablesTcb : public ExExeUtilTcb {
 public:
  // Constructor
  ExExeUtilVolatileTablesTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob = 0);

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

 protected:
  short isCreatorProcessObsolete(const char *name, NABoolean includesCat, NABoolean isCSETableName);
};

class ExExeUtilCleanupVolatileTablesTcb : public ExExeUtilVolatileTablesTcb {
  friend class ExExeUtilCleanupVolatileTablesTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilCleanupVolatileTablesTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob = 0);

  virtual short work();

  ExExeUtilCleanupVolatileTablesTdb &cvtTdb() const { return (ExExeUtilCleanupVolatileTablesTdb &)tdb; };

  static short dropVolatileSchema(ContextCli *currContext, char *schemaName, CollHeap *heap, ComDiagsArea *&diagsArea,
                                  ex_globals *globals = NULL);
  static short dropVolatileTables(ContextCli *currContext, CollHeap *heap);

 private:
  enum Step {
    INITIAL_,
    FETCH_SCHEMA_NAMES_,
    START_CLEANUP_,
    CHECK_FOR_OBSOLETE_CREATOR_PROCESS_,
    BEGIN_WORK_,
    DO_CLEANUP_,
    COMMIT_WORK_,
    END_CLEANUP_,
    CLEANUP_HIVE_TABLES_,
    DONE_,
    ERROR_
  };

  Step step_;

  Queue *schemaNamesList_;

  NABoolean someSchemasCouldNotBeDropped_;
  char errorSchemas_[1010];

  char *schemaQuery_;
};

class ExExeUtilVolatileTablesPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilVolatileTablesTcb;

 public:
  ExExeUtilVolatileTablesPrivateState();
  ~ExExeUtilVolatileTablesPrivateState();  // destructor
 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetVolatileInfoTdb
// -----------------------------------------------------------------------
class ExExeUtilGetVolatileInfoTdb : public ComTdbExeUtilGetVolatileInfo {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilGetVolatileInfoTdb() {}

  virtual ~ExExeUtilGetVolatileInfoTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetVolatileInfoTcb
// -----------------------------------------------------------------------
class ExExeUtilGetVolatileInfoTcb : public ExExeUtilVolatileTablesTcb {
  friend class ExExeUtilGetVolatileInfoTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetVolatileInfoTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob = 0);

  virtual short work();

  ExExeUtilGetVolatileInfoTdb &gviTdb() const { return (ExExeUtilGetVolatileInfoTdb &)tdb; };

 private:
  enum Step {
    INITIAL_,
    GET_SCHEMA_VERSION_,
    GET_ALL_NODE_NAMES_,
    APPEND_NEXT_QUERY_FRAGMENT_,
    FETCH_ALL_ROWS_,
    RETURN_ALL_SCHEMAS_,
    RETURN_ALL_TABLES_,
    RETURN_TABLES_IN_A_SESSION_,
    DONE_,
    ERROR_
  };

  Step step_;

  OutputInfo *prevInfo_;

  char *infoQuery_;
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetErrorInfoTdb
// -----------------------------------------------------------------------
class ExExeUtilGetErrorInfoTdb : public ComTdbExeUtilGetErrorInfo {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilGetErrorInfoTdb() {}

  virtual ~ExExeUtilGetErrorInfoTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetErrorInfoTcb
// -----------------------------------------------------------------------
class ExExeUtilGetErrorInfoTcb : public ExExeUtilTcb {
  friend class ExExeUtilGetErrorInfoTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetErrorInfoTcb(const ComTdbExeUtilGetErrorInfo &exe_util_tdb, ex_globals *glob = 0);

  virtual short work();

  ExExeUtilGetErrorInfoTdb &geiTdb() const { return (ExExeUtilGetErrorInfoTdb &)tdb; };

  ex_tcb_private_state *allocatePstates(int &numElems,      // inout, desired/actual elements
                                        int &pstateLength)  // out, length of one element
      ;

 private:
  enum Step { INITIAL_, RETURN_TEXT_, DONE_, ERROR_ };

  Step step_;

  char *outputBuf_;
};

class ExExeUtilGetErrorInfoPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilGetErrorInfoTcb;

 public:
  ExExeUtilGetErrorInfoPrivateState();
  ~ExExeUtilGetErrorInfoPrivateState();  // destructor
 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilLoadVolatileTableTdb
// -----------------------------------------------------------------------
class ExExeUtilLoadVolatileTableTdb : public ComTdbExeUtilLoadVolatileTable {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilLoadVolatileTableTdb() {}

  virtual ~ExExeUtilLoadVolatileTableTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExExeUtilLoadVolatileTableTcb : public ExExeUtilTcb {
  friend class ExExeUtilLoadVolatileTableTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilLoadVolatileTableTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob = 0);

  virtual short work();

  ExExeUtilLoadVolatileTableTdb &lvtTdb() const { return (ExExeUtilLoadVolatileTableTdb &)tdb; };

  ex_tcb_private_state *allocatePstates(int &numElems,      // inout, desired/actual elements
                                        int &pstateLength)  // out, length of one element
      ;

 private:
  enum Step { INITIAL_, INSERT_, UPD_STATS_, DONE_, ERROR_ };

  Step step_;
};

class ExExeUtilLoadVolatileTablePrivateState : public ex_tcb_private_state {
  friend class ExExeUtilLoadVolatileTableTcb;

 public:
  ExExeUtilLoadVolatileTablePrivateState();
  ~ExExeUtilLoadVolatileTablePrivateState();  // destructor
 protected:
};

// FIXME: Is this required? How is this used?
class ExExeUtilGetObjectEpochStatsPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilGetObjectEpochStatsTcb;

 public:
  ExExeUtilGetObjectEpochStatsPrivateState() {}
  ~ExExeUtilGetObjectEpochStatsPrivateState() {}

 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetObjectEpochStatsTdb
// -----------------------------------------------------------------------
class ExExeUtilGetObjectEpochStatsTdb : public ComTdbExeUtilGetObjectEpochStats {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilGetObjectEpochStatsTdb() {}

  virtual ~ExExeUtilGetObjectEpochStatsTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetObjectEpochStatsTcb
// -----------------------------------------------------------------------
class ExExeUtilGetObjectEpochStatsTcb : public ExExeUtilTcb {
  friend class ExExeUtilGetObjectEpochStatsTdb;

 public:
  // Constructor
  ExExeUtilGetObjectEpochStatsTcb(const ComTdbExeUtilGetObjectEpochStats &exe_util_tdb, ex_globals *glob = 0);
  ~ExExeUtilGetObjectEpochStatsTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  ExExeUtilGetObjectEpochStatsTdb &getObjectEpochStatsTdb() const { return (ExExeUtilGetObjectEpochStatsTdb &)tdb; };

 protected:
  enum Step { INITIAL_, RETURN_RESULT_, HANDLE_ERROR_, DONE_ };

  Step step_;
  ExStatisticsArea *stats_;
};

// FIXME: Is this required? How is this used?
class ExExeUtilGetObjectLockStatsPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilGetObjectLockStatsTcb;

 public:
  ExExeUtilGetObjectLockStatsPrivateState() {}
  ~ExExeUtilGetObjectLockStatsPrivateState() {}

 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetObjectLockStatsTdb
// -----------------------------------------------------------------------
class ExExeUtilGetObjectLockStatsTdb : public ComTdbExeUtilGetObjectLockStats {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilGetObjectLockStatsTdb() {}

  virtual ~ExExeUtilGetObjectLockStatsTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetObjectLockStatsTcb
// -----------------------------------------------------------------------
class ExExeUtilGetObjectLockStatsTcb : public ExExeUtilTcb {
  friend class ExExeUtilGetObjectLockStatsTdb;

 public:
  // Constructor
  ExExeUtilGetObjectLockStatsTcb(const ComTdbExeUtilGetObjectLockStats &exe_util_tdb, ex_globals *glob = 0);
  ~ExExeUtilGetObjectLockStatsTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  ExExeUtilGetObjectLockStatsTdb &getObjectLockStatsTdb() const { return (ExExeUtilGetObjectLockStatsTdb &)tdb; };

  ExStatisticsArea *distinctLockStats();

 protected:
  enum Step { INITIAL_, RETURN_RESULT_, HANDLE_ERROR_, DONE_ };

  Step step_;
  ExStatisticsArea *stats_;
  ExStatisticsArea *distinctStats_;
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetStatisticsTdb
// -----------------------------------------------------------------------
class ExExeUtilGetStatisticsTdb : public ComTdbExeUtilGetStatistics {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilGetStatisticsTdb() {}

  virtual ~ExExeUtilGetStatisticsTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetStatisticsTcb
// -----------------------------------------------------------------------
class ExExeUtilGetStatisticsTcb : public ExExeUtilTcb {
  friend class ExExeUtilGetStatisticsTdb;
  friend class ExExeUtilPrivateState;
  friend class ExExeUtilGetRTSStatisticsTcb;

 public:
  // Constructor
  ExExeUtilGetStatisticsTcb(const ComTdbExeUtilGetStatistics &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilGetStatisticsTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  ExExeUtilGetStatisticsTdb &getStatsTdb() const { return (ExExeUtilGetStatisticsTdb &)tdb; };

 protected:
  void moveCompilationStatsToUpQueue(CompilationStatsData *cmpStats);

  enum Step {
    INITIAL_,
    RETURN_COMPILER_STATS_,
    RETURN_EXECUTOR_STATS_,
    RETURN_OTHER_STATS_,
    SETUP_DETAILED_STATS_,
    FETCH_PROLOGUE_,
    FETCH_FIRST_STATS_ROW_,
    DISPLAY_HEADING_,
    FETCH_STATS_ROW_,
    RETURN_STATS_ROW_,
    FORMAT_AND_RETURN_PERTABLE_STATS_,
    FORMAT_AND_RETURN_ACCUMULATED_STATS_,
    FORMAT_AND_RETURN_ALL_STATS_,
    HANDLE_ERROR_,
    FETCH_EPILOGUE_,
    DONE_,
  };

  Step step_;

  ExStatisticsArea *stats_;

  char *statsQuery_;

  char *statsBuf_;

  char *statsRow_;
  int statsRowlen_;

  char detailedStatsCQDValue_[40];

  short statsMergeType_;

  short hdfsAccess_;
};

class ExExeUtilGetStatisticsPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilGetStatisticsTcb;

 public:
  ExExeUtilGetStatisticsPrivateState();
  ~ExExeUtilGetStatisticsPrivateState();  // destructor
 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetProcessStatisticsTdb
// -----------------------------------------------------------------------
class ExExeUtilGetProcessStatisticsTdb : public ComTdbExeUtilGetProcessStatistics {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilGetProcessStatisticsTdb() {}

  virtual ~ExExeUtilGetProcessStatisticsTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetUIDTdb
// -----------------------------------------------------------------------
class ExExeUtilGetUIDTdb : public ComTdbExeUtilGetUID {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilGetUIDTdb() {}

  virtual ~ExExeUtilGetUIDTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetUIDTcb
// -----------------------------------------------------------------------
class ExExeUtilGetUIDTcb : public ExExeUtilTcb {
  friend class ExExeUtilGetUIDTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetUIDTcb(const ComTdbExeUtilGetUID &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilGetUIDTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  ExExeUtilGetUIDTdb &getUIDTdb() const { return (ExExeUtilGetUIDTdb &)tdb; };

 private:
  enum Step {
    INITIAL_,
    RETURN_UID_,
    ERROR_,
    DONE_,
  };

  Step step_;
};

class ExExeUtilGetUIDPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilGetUIDTcb;

 public:
  ExExeUtilGetUIDPrivateState();
  ~ExExeUtilGetUIDPrivateState();  // destructor
 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetQIDTdb
// -----------------------------------------------------------------------
class ExExeUtilGetQIDTdb : public ComTdbExeUtilGetQID {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilGetQIDTdb() {}

  virtual ~ExExeUtilGetQIDTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetQIDTcb
// -----------------------------------------------------------------------
class ExExeUtilGetQIDTcb : public ExExeUtilTcb {
  friend class ExExeUtilGetQIDTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetQIDTcb(const ComTdbExeUtilGetQID &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilGetQIDTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  ExExeUtilGetQIDTdb &getQIDTdb() const { return (ExExeUtilGetQIDTdb &)tdb; };

 private:
  enum Step {
    INITIAL_,
    RETURN_QID_,
    ERROR_,
    DONE_,
  };

  Step step_;
};

class ExExeUtilGetQIDPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilGetQIDTcb;

 public:
  ExExeUtilGetQIDPrivateState();
  ~ExExeUtilGetQIDPrivateState();  // destructor
 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilPopulateInMemStatsTdb
// -----------------------------------------------------------------------
class ExExeUtilPopulateInMemStatsTdb : public ComTdbExeUtilPopulateInMemStats {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilPopulateInMemStatsTdb() {}

  virtual ~ExExeUtilPopulateInMemStatsTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilPopulateInMemStatsTcb
// -----------------------------------------------------------------------
class ExExeUtilPopulateInMemStatsTcb : public ExExeUtilTcb {
  friend class ExExeUtilPopulateInMemStatsTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilPopulateInMemStatsTcb(const ComTdbExeUtilPopulateInMemStats &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilPopulateInMemStatsTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  ExExeUtilPopulateInMemStatsTdb &pimsTdb() const { return (ExExeUtilPopulateInMemStatsTdb &)tdb; };

 private:
  enum Step {
    INITIAL_,
    PROLOGUE_,
    DELETE_STATS_,
    POPULATE_HISTOGRAMS_STATS_,
    POPULATE_HISTINTS_STATS_,
    EPILOGUE_,
    EPILOGUE_AND_ERROR_RETURN_,
    ERROR_,
    ERROR_RETURN_,
    DONE_,
  };

  Step step_;
};

class ExExeUtilPopulateInMemStatsPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilPopulateInMemStatsTcb;

 public:
  ExExeUtilPopulateInMemStatsPrivateState();
  ~ExExeUtilPopulateInMemStatsPrivateState();  // destructor
 protected:
};

///////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilAqrWnrInsertTdb
// -----------------------------------------------------------------------
class ExExeUtilAqrWnrInsertTdb : public ComTdbExeUtilAqrWnrInsert {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilAqrWnrInsertTdb() {}

  virtual ~ExExeUtilAqrWnrInsertTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

///////////////////////////////////////////////////////////////
// ExExeUtilAqrWnrInsertTcb
///////////////////////////////////////////////////////////////
class ExExeUtilAqrWnrInsertTcb : public ExExeUtilTcb {
 public:
  // Constructor
  ExExeUtilAqrWnrInsertTcb(const ComTdbExeUtilAqrWnrInsert &exe_util_tdb, const ex_tcb *child_tcb,
                           ex_globals *glob = 0);

  ~ExExeUtilAqrWnrInsertTcb();

  virtual int fixup();

  virtual short work();

  virtual short workCancel();

 protected:
  enum Step {
    INITIAL_,
    LOCK_TARGET_,
    IS_TARGET_EMPTY_,
    SEND_REQ_TO_CHILD_,
    GET_REPLY_FROM_CHILD_,
    CLEANUP_CHILD_,
    ERROR_,
    CLEANUP_TARGET_,
    DONE_
  };

 private:
  ExExeUtilAqrWnrInsertTdb &ulTdb() const { return (ExExeUtilAqrWnrInsertTdb &)tdb; }

  void setStep(Step s, int lineNum);

  Step step_;
  bool targetWasEmpty_;
};

// -----------------------------------------------------------------------
// ExExeUtilLongRunningTdb
// -----------------------------------------------------------------------
class ExExeUtilLongRunningTdb : public ComTdbExeUtilLongRunning {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilLongRunningTdb() {}

  virtual ~ExExeUtilLongRunningTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

///////////////////////////////////////////////////////////////
// ExExeUtilLongRunningTcb
///////////////////////////////////////////////////////////////
class ExExeUtilLongRunningTcb : public ExExeUtilTcb {
 public:
  // Constructor
  ExExeUtilLongRunningTcb(const ComTdbExeUtilLongRunning &exe_util_tdb,

                          ex_globals *glob = 0);

  ~ExExeUtilLongRunningTcb();

  virtual int fixup();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  void registerSubtasks();

  ExExeUtilLongRunningTdb &lrTdb() const { return (ExExeUtilLongRunningTdb &)tdb; };

  short doLongRunning();

  short executeLongRunningQuery();

  short processInitial(int &rc);

  short processContinuing(int &rc);

  short finalizeDoLongRunning();

  void addTransactionCount() { transactions_++; };
  long getTransactionCount() { return transactions_; };

  void addRowsDeleted(long rows) { rowsDeleted_ += rows; };
  long getRowsDeleted() { return rowsDeleted_; };

  void setInitial(short initial) { initial_ = initial; };
  short getInitial() { return initial_; };

  /*void setInitialOutputVarPtrList(Queue * outputVarPtrList)
  { initialOutputVarPtrList_ = outputVarPtrList; };
  Queue * getInitialOutputVarPtrList() { return initialOutputVarPtrList_; };

  void setContinuingOutputVarPtrList(Queue * outputVarPtrList)
  { continuingOutputVarPtrList_ = outputVarPtrList; };
  Queue * getContinuingOutputVarPtrList() { return continuingOutputVarPtrList_; };*/

  ComDiagsArea *getDiagAreaFromUpQueueTail();

 private:
  enum Step { INITIAL_, BEGIN_WORK_, LONG_RUNNING_, ERROR_, DONE_ };

  Step step_;
  long transactions_;
  long rowsDeleted_;
  short initial_;

  // Queue * initialOutputVarPtrList_;
  // Queue * continuingOutputVarPtrList_;
  char *lruStmtAndPartInfo_;
  // char    * lruStmtWithCKAndPartInfo_;
  ExTransaction *currTransaction_;
};

class ExExeUtilLongRunningPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilLongRunningTcb;

 public:
  ExExeUtilLongRunningPrivateState();
  ~ExExeUtilLongRunningPrivateState();  // destructor

 protected:
};

class ExExeUtilGetRTSStatisticsTcb : public ExExeUtilTcb {
  friend class ExExeUtilGetStatisticsTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetRTSStatisticsTcb(const ComTdbExeUtilGetStatistics &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilGetRTSStatisticsTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  ExExeUtilGetStatisticsTdb &getStatsTdb() const { return (ExExeUtilGetStatisticsTdb &)tdb; };

  void deleteSqlStatItems(SQLSTATS_ITEM *sqlStatsItem, int noOfStatsItem);
  void initSqlStatsItems(SQLSTATS_ITEM *sqlStatsItem, int noOfStatsItem, NABoolean initTdbIdOnly);
  NABoolean singleLineFormat() { return singleLineFormat_; }
  enum OperatorStatsOptions { ALL = 1, DATA_USED = 2 };

  OperatorStatsOptions getOperatorStatsOption() { return operatorStatsOption_; };

  void setFilePath(NAString &path) { filePath_ = path; };
  NAString &getFilePath() { return filePath_; };

 private:
  enum Step {
    INITIAL_,
    GET_NEXT_STATS_DESC_ENTRY_,
    GET_MASTER_STATS_ENTRY_,
    FORMAT_AND_RETURN_MASTER_STATS_,
    GET_MEAS_STATS_ENTRY_,
    FORMAT_AND_RETURN_MEAS_STATS_,
    GET_ROOTOPER_STATS_ENTRY_,
    FORMAT_AND_RETURN_ROOTOPER_STATS_,
    GET_PERTABLE_STATS_ENTRY_,
    DISPLAY_PERTABLE_STATS_HEADING_,
    FORMAT_AND_RETURN_PERTABLE_STATS_,
    GET_PARTITION_ACCESS_STATS_ENTRY_,
    FORMAT_AND_RETURN_PARTITION_ACCESS_STATS_,
    GET_OPER_STATS_ENTRY_,
    FORMAT_AND_RETURN_OPER_STATS_,
    GET_DP2_OPER_STATS_ENTRY_,
    FORMAT_AND_RETURN_DP2_OPER_STATS_,
    HANDLE_ERROR_,
    DONE_,
    EXPAND_STATS_ARRAY_,
    GET_RMS_STATS_ENTRY_,
    FORMAT_AND_RETURN_RMS_STATS_,
    GET_BMO_STATS_ENTRY_,
    DISPLAY_BMO_STATS_HEADING_,
    FORMAT_AND_RETURN_BMO_STATS_,
    GET_UDR_BASE_STATS_ENTRY_,
    DISPLAY_UDR_BASE_STATS_HEADING_,
    FORMAT_AND_RETURN_UDR_BASE_STATS_,
    GET_REPLICATE_STATS_ENTRY_,
    FORMAT_AND_RETURN_REPLICATE_STATS_,
    GET_REPLICATOR_STATS_ENTRY_,
    FORMAT_AND_RETURN_REPLICATOR_STATS_,
    GET_PROCESS_STATS_ENTRY_,
    FORMAT_AND_RETURN_PROCESS_STATS_,
    GET_HBASE_STATS_ENTRY_,
    GET_SE_STATS_ENTRY_ = GET_HBASE_STATS_ENTRY_,
    DISPLAY_HBASE_STATS_HEADING_,
    FORMAT_AND_RETURN_HBASE_STATS_,
    GET_HIVE_STATS_ENTRY_,
    DISPLAY_HIVE_STATS_HEADING_,
    FORMAT_AND_RETURN_HIVE_STATS_
  };

  Step step_;
  char *statsBuf_;
  short statsCollectType_;
  SQLSTATS_DESC *sqlStatsDesc_;
  int maxStatsDescEntries_;
  int retStatsDescEntries_;
  int currStatsDescEntry_;
  int currStatsItemEntry_;

  SQLSTATS_ITEM *masterStatsItems_;
  SQLSTATS_ITEM *measStatsItems_;
  SQLSTATS_ITEM *operatorStatsItems_;
  SQLSTATS_ITEM *rootOperStatsItems_;
  SQLSTATS_ITEM *partitionAccessStatsItems_;
  SQLSTATS_ITEM *pertableStatsItems_;
  SQLSTATS_ITEM *rmsStatsItems_;
  SQLSTATS_ITEM *bmoStatsItems_;
  SQLSTATS_ITEM *udrbaseStatsItems_;
  SQLSTATS_ITEM *replicateStatsItems_;
  SQLSTATS_ITEM *replicatorStatsItems_;
  SQLSTATS_ITEM *processStatsItems_;
  SQLSTATS_ITEM *hbaseStatsItems_;
  SQLSTATS_ITEM *hiveStatsItems_;
  int maxMasterStatsItems_;
  int maxMeasStatsItems_;
  int maxOperatorStatsItems_;
  int maxRootOperStatsItems_;
  int maxPartitionAccessStatsItems_;
  int maxPertableStatsItems_;
  int maxRMSStatsItems_;
  int maxBMOStatsItems_;
  int maxUDRBaseStatsItems_;
  int maxReplicateStatsItems_;
  int maxReplicatorStatsItems_;
  int maxProcessStatsItems_;
  int maxHbaseStatsItems_;
  int maxHiveStatsItems_;

  NABoolean isHeadingDisplayed_;
  NABoolean isBMOHeadingDisplayed_;
  NABoolean isUDRBaseHeadingDisplayed_;
  NABoolean isHbaseHeadingDisplayed_;
  NABoolean isHiveHeadingDisplayed_;

  OperatorStatsOptions operatorStatsOption_;

  // info to write out RT used data
  long queryHash_;
  NAString filePath_;

  static const int numOperStats = 14;
  void formatOperStatItems(SQLSTATS_ITEM operStatsItems[]);
  void formatOperStats(SQLSTATS_ITEM operStatsItems[]);
  void formatOperStatsDataUsed(SQLSTATS_ITEM operStatsItems[]);

  //  void formatDouble(SQLSTATS_ITEM stat, char* targetString);
  void formatInt64(SQLSTATS_ITEM stat, char *targetString);
  // void formatWDouble(SQLSTATS_ITEM stat, char* targetString);
  void formatWInt64(SQLSTATS_ITEM stat, char *targetString);
  char *formatTimestamp(char *buf, long inTime);
  char *formatElapsedTime(char *buf, long inTime);
  NABoolean singleLineFormat_;
  // Convert the long field to string. No comma characters will be
  // inserted.
  void convertInt64(SQLSTATS_ITEM stat, char *targetString);
};

class ExExeUtilGetRTSStatisticsPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilGetRTSStatisticsTcb;

 public:
  ExExeUtilGetRTSStatisticsPrivateState();
  ~ExExeUtilGetRTSStatisticsPrivateState();  // destructor
 protected:
};
//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetMetadataInfoTdb
// -----------------------------------------------------------------------
class ExExeUtilGetMetadataInfoTdb : public ComTdbExeUtilGetMetadataInfo {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilGetMetadataInfoTdb() {}

  virtual ~ExExeUtilGetMetadataInfoTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetMetadataInfoTcb
// -----------------------------------------------------------------------
class ExExeUtilGetMetadataInfoTcb : public ExExeUtilTcb {
  friend class ExExeUtilGetMetadataInfoTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetMetadataInfoTcb(const ComTdbExeUtilGetMetadataInfo &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilGetMetadataInfoTcb();

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  ExExeUtilGetMetadataInfoTdb &getMItdb() const { return (ExExeUtilGetMetadataInfoTdb &)tdb; };

 protected:
  enum { NUM_MAX_PARAMS_ = 25 };

  enum Step {
    INITIAL_,
    DISABLE_CQS_,
    CHECK_ACCESS_,
    GET_SCHEMA_VERSION_,
    GET_OBJECT_UID_,
    SETUP_QUERY_,
    SETUP_HBASE_QUERY_,
    FETCH_ALL_ROWS_,
    FETCH_ALL_ROWS_FOR_OBJECTS_,
    FETCH_ALL_ROWS_IN_SCHEMA_,
    DISPLAY_HEADING_,
    PROCESS_NEXT_ROW_,
    EVAL_EXPR_,
    RETURN_ROW_,
    ENABLE_CQS_,
    GET_USING_VIEWS_,
    GET_USED_OBJECTS_,
    HANDLE_ERROR_,
    DONE_,
  };

  enum ViewsStep {
    VIEWS_INITIAL_,
    VIEWS_FETCH_PROLOGUE_,
    VIEWS_FETCH_ROW_,
    VIEWS_FETCH_EPILOGUE_,
    VIEWS_ERROR_,
    VIEWS_DONE_
  };

  enum AuthIdType { USERS_ROLES_ = 0, ROLES_, USERS_, TENANTS_ };

  Step step_;
  ViewsStep vStep_;

  char *metadataQuery_;

  char objectUid_[25];

  char *queryBuf_;
  char *outputBuf_;
  char *headingBuf_;

  char *patternStr_;

  int numOutputEntries_;

  char *param_[NUM_MAX_PARAMS_];

  NABoolean headingReturned_;

  short displayHeading();

  int getUsingView(Queue *infoList,

                   // TRUE: shorthand view, FALSE: Materialized View
                   NABoolean isShorthandView,

                   char *&viewName, int &len);

  int getUsedObjects(Queue *infoList, NABoolean isShorthandView, char *&viewName, int &len);
  void setReturnRowCount(int n) { returnRowCount_ = n; }

  int getReturnRowCount() { return returnRowCount_; }

  void incReturnRowCount() { returnRowCount_++; }

  int returnRowCount_;

 private:
  NABoolean checkUserPrivs(ContextCli *currConnext, const ComTdbExeUtilGetMetadataInfo::QueryType queryType);

  int getAuthID(const char *authName, const char *catName, const char *schName, const char *objName);

  int getCurrentUserRoles(ContextCli *currContext, NAString &authList, NAString &granteeList);

  int colPrivsFrag(const char *authName, const char *catName, const NAString &privWhereClause, NAString &colPrivsStmt);

  NAString getGrantedPrivCmd(const NAString &roleList, const char *cat, const bool &getObjectsInSchema,
                             const NAString &qualifier = NAString(""),
                             const NAString &uidColumn = NAString("object_uid"), const char *qualifier2 = NULL);

  void getGroupList(const char *userName, NAString &groupList);

  char *getRoleList(bool &containsRootRole, const int userID, const char *catName, const char *schName,
                    const char *objName);

  long getObjectUID(const char *catName, const char *schName, const char *objName, const char *targetName,
                    const char *type);

  int colPrivsFrag(const char *authName, const NAString &privsWhereClause, NAString &colPrivsStmt);
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetMetadataInfoComplexTcb
// -----------------------------------------------------------------------
class ExExeUtilGetMetadataInfoComplexTcb : public ExExeUtilGetMetadataInfoTcb {
  friend class ExExeUtilGetMetadataInfoTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetMetadataInfoComplexTcb(const ComTdbExeUtilGetMetadataInfo &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilGetMetadataInfoComplexTcb();

  virtual short work();

 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetMetadataInfoVersionTcb
// -----------------------------------------------------------------------
class ExExeUtilGetMetadataInfoVersionTcb : public ExExeUtilGetMetadataInfoTcb {
  friend class ExExeUtilGetMetadataInfoTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetMetadataInfoVersionTcb(const ComTdbExeUtilGetMetadataInfo &exe_util_tdb, ex_globals *glob = 0);

  virtual short work();

 protected:
  UInt32 maxObjLen_;
  char formatStr_[100];
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetHbaseObjectsTcb
// -----------------------------------------------------------------------
class ExExeUtilGetHbaseObjectsTcb : public ExExeUtilGetMetadataInfoTcb {
  friend class ExExeUtilGetMetadataInfoTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetHbaseObjectsTcb(const ComTdbExeUtilGetMetadataInfo &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilGetHbaseObjectsTcb();

  virtual short work();

 private:
  ExpHbaseInterface *ehi_;
  NAArray<HbaseStr> *hbaseTables_;
  int currIndex_;

  NAString extTableName_;

  char *hbaseName_;
  char *hbaseNameBuf_;
  char *outBuf_;
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilGetNamespaceObjectsTcb
// -----------------------------------------------------------------------
class ExExeUtilGetNamespaceObjectsTcb : public ExExeUtilGetMetadataInfoTcb {
  friend class ExExeUtilGetMetadataInfoTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetNamespaceObjectsTcb(const ComTdbExeUtilGetMetadataInfo &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilGetNamespaceObjectsTcb();

  virtual short work();

 private:
  enum Step {
    INITIAL_,
    GET_NAMESPACES_,
    CHECK_NAMESPACE_EXISTS_,
    GET_OBJECTS_IN_NAMESPACE_,
    GET_NAMESPACE_CONFIG_,
    PROCESS_NEXT_NAMESPACE_,
    PROCESS_NEXT_OBJECT_IN_NAMESPACE_,
    EVAL_EXPR_,
    PROCESS_NEXT_ROW_,
    RETURN_ROW_,
    HANDLE_ERROR_,
    DONE_,
  };

  ExpHbaseInterface *ehi_;
  NAArray<HbaseStr> *namespaceObjects_;

  int currIndex_;

  NAString extTableName_;

  char *hbaseName_;
  char *hbaseNameBuf_;
  int hbaseNameMaxLen_;

  Step step_;
};

//////////////////////////////////////////////////////////////////////////
class ExExeUtilGetMetadataInfoPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilGetMetadataInfoTcb;

 public:
  ExExeUtilGetMetadataInfoPrivateState();
  ~ExExeUtilGetMetadataInfoPrivateState();  // destructor
 protected:
};

// See ExCancel.cpp

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilShowSetTdb
// -----------------------------------------------------------------------
class ExExeUtilShowSetTdb : public ComTdbExeUtilShowSet {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilShowSetTdb() {}

  virtual ~ExExeUtilShowSetTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilShowSetTcb
// -----------------------------------------------------------------------
class ExExeUtilShowSetTcb : public ExExeUtilTcb {
  friend class ExExeUtilShowSetTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilShowSetTcb(const ComTdbExeUtilShowSet &exe_util_tdb, ex_globals *glob = 0);

  virtual short work();

  ExExeUtilShowSetTdb &ssTdb() const { return (ExExeUtilShowSetTdb &)tdb; };

 protected:
  enum Step { EMPTY_, RETURN_HEADER_, RETURNING_DEFAULT_, DONE_, HANDLE_ERROR_, CANCELLED_ };

  Step step_;
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilAQRTdb
// -----------------------------------------------------------------------
class ExExeUtilAQRTdb : public ComTdbExeUtilAQR {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilAQRTdb() {}

  virtual ~ExExeUtilAQRTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilAQRTcb
// -----------------------------------------------------------------------
class ExExeUtilAQRTcb : public ExExeUtilTcb {
  friend class ExExeUtilAQRTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilAQRTcb(const ComTdbExeUtilAQR &exe_util_tdb, ex_globals *glob = 0);

  virtual short work();

  ExExeUtilAQRTdb &aqrTdb() const { return (ExExeUtilAQRTdb &)tdb; };

 protected:
  enum Step { EMPTY_, SET_ENTRY_, RETURN_HEADER_, RETURNING_ENTRY_, DONE_, HANDLE_ERROR_, CANCELLED_ };

  Step step_;
};

class ExExeUtilGetProcessStatisticsTcb : public ExExeUtilGetStatisticsTcb {
  friend class ExExeUtilGetProcessStatisticsTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilGetProcessStatisticsTcb(const ComTdbExeUtilGetProcessStatistics &exe_util_tdb, ex_globals *glob = 0);

  //  ~ExExeUtilGetProcessStatisticsTcb();

  virtual short work();

  ExExeUtilGetProcessStatisticsTdb &getStatsTdb() const { return (ExExeUtilGetProcessStatisticsTdb &)tdb; };

 private:
  enum ProcessStatsStep {
    INITIAL_,
    GET_PROCESS_STATS_AREA_,
    GET_PROCESS_STATS_ENTRY_,
    FORMAT_AND_RETURN_PROCESS_STATS_,
    HANDLE_ERROR_,
    DONE_
  };

  ProcessStatsStep step_;
  const ExStatisticsArea *statsArea_;
  ExProcessStats *processStats_;
};

class ExExeUtilHiveQueryPrivateState : public ex_tcb_private_state {
 public:
  ExExeUtilHiveQueryPrivateState();
  ~ExExeUtilHiveQueryPrivateState();  // destructor
 protected:
};
//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilHbaseLoadTdb
// -----------------------------------------------------------------------
class ExExeUtilHBaseBulkLoadTdb : public ComTdbExeUtilHBaseBulkLoad {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilHBaseBulkLoadTdb() {}

  virtual ~ExExeUtilHBaseBulkLoadTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExExeUtilHBaseBulkLoadTcb : public ExExeUtilTcb {
  friend class ExExeUtilHBaseBulkLoadTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilHBaseBulkLoadTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob = 0);
  ~ExExeUtilHBaseBulkLoadTcb();
  virtual short work();

  ExExeUtilHBaseBulkLoadTdb &hblTdb() const { return (ExExeUtilHBaseBulkLoadTdb &)tdb; };

  virtual short moveRowToUpQueue(const char *row, int len = -1, short *rc = NULL, NABoolean isVarchar = TRUE);

  short printLoggingLocation(int bufPos);

  void setEndStatusMsg(const char *operation, int bufPos = 0, NABoolean withtime = FALSE);

  short setStartStatusMsgAndMoveToUpQueue(const char *operation, short *rc, int bufPos = 0, NABoolean withtime = FALSE);
  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element
  void setLoggingLocation();
  NABoolean generateTrafSampleTable(const char *cTableName, const char *cSampleTableName);

 private:
  enum Step {
    // initial state
    INITIAL_,
    // cleanup leftover files
    PRE_LOAD_CLEANUP_,
    LOAD_START_,
    LOAD_END_,
    LOAD_END_ERROR_,
    PREPARATION_,
    LOADING_DATA_,
    COMPLETE_BULK_LOAD_,  // load incremental
    POST_LOAD_CLEANUP_,
    TRUNCATE_TABLE_,
    DISABLE_INDEXES_,
    ENABLE_INDEXES_AND_LOAD_END_,
    ENABLE_INDEXES_AND_ERROR_,
    POPULATE_INDEXES_,
    POPULATE_INDEXES_EXECUTE_,
    UPDATE_STATS_,
    UPDATE_STATS_EXECUTE_,
    UPDATE_STATS__END_,
    RETURN_STATUS_MSG_,
    DONE_,
    HANDLE_ERROR_,
    DELETE_DATA_AND_ERROR_,
    LOAD_ERROR_
  };

  Step step_;
  Step nextStep_;

  long startTime_;
  long endTime_;
  long rowsAffected_;
  char statusMsgBuf_[BUFFER_SIZE];
  ExpHbaseInterface *ehi_;
  char *loggingLocation_;
  short setCQDs();
  short restoreCQDs();
  NABoolean ustatNonEmptyTable_;
  NAString sSampleTableName_;

  short loadWithParams(ComDiagsArea *&diagsArea);
};

class ExExeUtilHbaseLoadPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilHBaseBulkLoadTcb;

 public:
  ExExeUtilHbaseLoadPrivateState();
  ~ExExeUtilHbaseLoadPrivateState();  // destructor
 protected:
};

class ExExeUtilHbaseUnLoadPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilHBaseBulkLoadTcb;

 public:
  ExExeUtilHbaseUnLoadPrivateState();
  ~ExExeUtilHbaseUnLoadPrivateState();  // destructor
 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilRegionStatsTdb
// -----------------------------------------------------------------------
class ExExeUtilRegionStatsTdb : public ComTdbExeUtilRegionStats {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilRegionStatsTdb() {}

  virtual ~ExExeUtilRegionStatsTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilRegionStatsTcb
// -----------------------------------------------------------------------
class ExExeUtilRegionStatsTcb : public ExExeUtilTcb {
  friend class ExExeUtilRegionStatsTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilRegionStatsTcb(const ComTdbExeUtilRegionStats &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilRegionStatsTcb();

  virtual short work();

  ExExeUtilRegionStatsTdb &getDLStdb() const { return (ExExeUtilRegionStatsTdb &)tdb; };

 private:
  enum Step {
    INITIAL_,
    EVAL_INPUT_,
    COLLECT_STATS_,
    POPULATE_STATS_BUF_,
    EVAL_EXPR_,
    RETURN_STATS_BUF_,
    HANDLE_ERROR_,
    DONE_
  };
  Step step_;

 protected:
  long getEmbeddedNumValue(char *&sep, char endChar, NABoolean adjustLen = TRUE);

  short collectStats(char *tableName, char *tableNameForUID, NABoolean replaceUID);
  short populateStats(int currIndex);

  char *hbaseRootdir_;

  char *tableName_;

  char *tableNameForUID_;

  char *inputNameBuf_;

  char *statsBuf_;
  int statsBufLen_;
  ComTdbRegionStatsVirtTableColumnStruct *stats_;

  ExpHbaseInterface *ehi_;
  NAArray<HbaseStr> *regionInfoList_;

  int currIndex_;

  int numRegionStatsEntries_;

  char *catName_;
  char *schName_;
  char *objName_;
  char *regionName_;

  NAString extNameForHbase_;
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilRegionStatsFormatTcb
// -----------------------------------------------------------------------
class ExExeUtilRegionStatsFormatTcb : public ExExeUtilRegionStatsTcb {
  friend class ExExeUtilRegionStatsTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilRegionStatsFormatTcb(const ComTdbExeUtilRegionStats &exe_util_tdb, ex_globals *glob = 0);

  virtual short work();

 private:
  enum Step {
    INITIAL_,
    COLLECT_STATS_,
    EVAL_INPUT_,
    COMPUTE_TOTALS_,
    RETURN_SUMMARY_,
    RETURN_DETAILS_,
    POPULATE_STATS_BUF_,
    RETURN_REGION_INFO_,
    HANDLE_ERROR_,
    DONE_
  };

  Step step_;

  char *statsTotalsBuf_;
  ComTdbRegionStatsVirtTableColumnStruct *statsTotals_;

  short initTotals();
  short computeTotals();
};

////////////////////////////////////////////////////////////////////////////
class ExExeUtilRegionStatsPrivateState : public ex_tcb_private_state {
  friend class ExExeUtilRegionStatsTcb;

 public:
  ExExeUtilRegionStatsPrivateState();
  ~ExExeUtilRegionStatsPrivateState();  // destructor
 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilClusterStatsTcb
// -----------------------------------------------------------------------
class ExExeUtilClusterStatsTcb : public ExExeUtilRegionStatsTcb {
  friend class ExExeUtilClusterStatsTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilClusterStatsTcb(const ComTdbExeUtilRegionStats &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilClusterStatsTcb();

  virtual short work();

 private:
  enum Step {
    INITIAL_,
    EVAL_INPUT_,
    COLLECT_STATS_,
    POPULATE_STATS_BUF_,
    EVAL_EXPR_,
    RETURN_STATS_BUF_,
    HANDLE_ERROR_,
    DONE_
  };
  Step step_;

  short collectStats();
  short populateStats(int currIndex, NABoolean nullTerminate = FALSE);

 protected:
  ComTdbClusterStatsVirtTableColumnStruct *stats_;

  int currObjectRegionNum_;
  NAString currObjectName_;
};

class connectByStackItem {
 public:
  connectByStackItem(CollHeap *h) {
    seedValue = NULL;
    pathItem = NULL;
    pathLen = 0;
    len = 0;
    level = 0;
    type = 0;
    parentId = 0;
    h_ = h;
  }
  ~connectByStackItem() {}
  void cleanup() {
    if (seedValue) {
      NADELETEBASIC(seedValue, h_);
      // printf("NADELETE20\n");
    }
    if (pathItem) {
      NADELETEBASIC(pathItem, h_);
      // printf("NADELETE21\n");
    }
  }

  char *seedValue;
  char *pathItem;
  int len;
  int level;
  int type;
  int pathLen;
  int parentId;
  CollHeap *h_;
};
class connectByOneRow {
 public:
  connectByOneRow() {
    data_ = NULL;
    len = 0;
    type = 0;
  }
  ~connectByOneRow() {}
  char *data_;
  int len;
  int type;
};

#define CONNECT_BY_DEFAULT_BATCH_SIZE 300
#define CONNECT_BY_MAX_LEVEL_SIZE     200
#define CONNECT_BY_MAX_SQL_TEXT_SIZE  20000
#define CONNECT_BY_MAX_PATH_SIZE      60

////////////////////////////////////////////////////////////////////////////
class ExExeUtilLobInfoTablePrivateState : public ex_tcb_private_state {
 public:
  ExExeUtilLobInfoTablePrivateState();
  ~ExExeUtilLobInfoTablePrivateState();  // destructor
 protected:
};

short ExExeUtilLobExtractLibrary(ExeCliInterface *cliInterface, char *libHandle, char *cachedLibName,
                                 ComDiagsArea *toDiags);

class ExExeUtilConnectbyTcb : public ExExeUtilTcb {
  friend class ExExeUtilConnectbyTdb;

 public:
  ExExeUtilConnectbyTcb(const ComTdbExeUtilConnectby &exe_util_tdb, ex_globals *glob = 0);

  virtual ~ExExeUtilConnectbyTcb() {}

  virtual short work();
  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element
  enum Step {
    INITIAL_,
    EVAL_INPUT_,
    EVAL_START_WITH_,
    DO_CONNECT_BY_,
    EVAL_OUTPUT_EXPR_,
    NEXT_LEVEL_,
    DUAL_,
    NEXT_ROOT_,
    ERROR_,
    DONE_
  };
  Step step_;
  ExExeUtilConnectbyTdb &exeUtilTdb() const { return (ExExeUtilConnectbyTdb &)tdb; };

  short emitRow(ExpTupleDesc *tDesc, int level, int isleaf, int iscycle, connectByStackItem *it,
                NABoolean chkForStartWith = FALSE);
  short emitCacheRow(char *);
  short emitPrevRow(ExpTupleDesc *tDesc, int level, int isleaf, int iscycle, Queue *q, int index);

  Queue *currArray[200];
  Queue *getCurrentQueue(int level) {
    if (level < 0 || level > 200) abort();
    return currArray[level];
  }
  short checkDuplicate(connectByStackItem *it, int len, int level);
  void replaceStartWithStringWithValue(Queue *v, const NAString s, char *out, int size);
  int currLevel_;
  long resultSize_;
  Queue *currQueue_;
  Queue *seedQueue_;
  Queue *thisQueue_;
  Queue *prevQueue_;
  Queue *tmpPrevQueue_;
  int currRootId_;
  int connBatchSize_;
  int upQueueIsFull_;
  int currSeedNum_;
  int nq21cnt_;
  connectByStackItem *resendIt_;
  Queue *resultCache_;
  short cachework_;
  short cacheptr_;
  char *inputDynParamBuf_;

 protected:
  ExExeUtilConnectbyTcb *tcb_;

 private:
  tupp tuppData_;
  char *data_;
};

class ExExeUtilConnectbyTdbState : public ex_tcb_private_state {
  friend class ExExeUtilConnectbyTcb;

 public:
  ExExeUtilConnectbyTdbState();
  ~ExExeUtilConnectbyTdbState();

 protected:
  ExExeUtilConnectbyTcb::Step step_;
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilCompositeUnnestTdb
// -----------------------------------------------------------------------
class ExExeUtilCompositeUnnestTdb : public ComTdbExeUtilCompositeUnnest {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilCompositeUnnestTdb() {}

  virtual ~ExExeUtilCompositeUnnestTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilCompositeUnnestTcb
// -----------------------------------------------------------------------
class ExExeUtilCompositeUnnestTcb : public ExExeUtilTcb {
  friend class ExExeUtilCompositeUnnestTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilCompositeUnnestTcb(const ComTdbExeUtilCompositeUnnest &exe_util_tdb, const ex_tcb *child_tcb,
                              ex_globals *glob = 0);

  virtual short work();

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

  ExExeUtilCompositeUnnestTdb &unnestTdb() const { return (ExExeUtilCompositeUnnestTdb &)tdb; };

 private:
  enum Step {
    INITIAL_,
    SEND_REQ_TO_CHILD_,
    GET_REPLY_FROM_CHILD_,
    EXTRACT_COL_TO_UNNEST_,
    SET_ARRAY_ELEM_NUM_,
    CREATE_ROW_TO_RETURN_,
    APPLY_PRED_,
    RETURN_UNNESTED_ROW_,
    GET_NEXT_ROW_,
    ERROR_FROM_CHILD_,
    CANCEL_,
    DONE_,
    HANDLE_ERROR_
  };

  Step step_;

  char *extractColRow_;
  char *returnColsRow_;
  char *elemNumRow_;
  int numElems_;
  int currElem_;
};

class ExExeUtilUpdataDeletePrivateState : public ex_tcb_private_state {
  friend class ExExeUtilUpdataDeleteTcb;

 public:
  ExExeUtilUpdataDeletePrivateState();
  ~ExExeUtilUpdataDeletePrivateState();  // destructor
 protected:
};

//////////////////////////////////////////////////////////////////////////
// -----------------------------------------------------------------------
// ExExeUtilUpdataDeleteTdb
// -----------------------------------------------------------------------
class ExExeUtilUpdataDeleteTdb : public ComTdbExeUtilUpdataDelete {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExeUtilUpdataDeleteTdb() {}

  virtual ~ExExeUtilUpdataDeleteTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbDLL instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExExeUtilUpdataDeleteTcb : public ExExeUtilTcb {
  friend class ExExeUtilUpdataDeleteTdb;
  friend class ExExeUtilPrivateState;

 public:
  // Constructor
  ExExeUtilUpdataDeleteTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob = 0);

  ~ExExeUtilUpdataDeleteTcb();

  void freeResources();

  virtual short work();

  int lockOperatingTable();
  int unLockOperatingTable();
  int isBRInProgress();

  ExExeUtilUpdataDeleteTdb &hblTdb() const { return (ExExeUtilUpdataDeleteTdb &)tdb; };

  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element

 private:
  enum Step {
    INITIAL_,
    EXE_GET_OBJNAME_,
    EXE_CLEANUP_,
    CHECK_FOR_LOCK_OBJECTS_,
    EXE_CREATE_SNAPSHOT_,
    EXE_RESTORE_SNAPSHOT_,
    EXE_DELETE_SNAPSHOT_,
    EXE_RUNSQL_,
    EXE_ERROR_,
    DONE_,
  };

  Step step_;
  Step nextStep_;

  long rowsAffected_;
  ExpHbaseInterface *ehi_;

  short setCQDs();
  short restoreCQDs();
};

#endif
