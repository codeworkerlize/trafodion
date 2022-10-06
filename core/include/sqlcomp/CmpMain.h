#pragma once
#include "cli/sqlcli.h"
#include "comexe/CmpMessage.h"
#include "common/ComSmallDefs.h"
#include "common/ComSysUtils.h"
#include "common/dfs2rec.h"
#include "optimizer/RelScan.h"

class Key;
class CacheKey;
class ComDiagsArea;
class NAMemory;
class NormWA;
class RelExpr;
class RelRoot;
class FragmentDir;
class QueryText;

const SQLATTR_TYPE SQL_ATTR_SQLPARSERFLAGS = (SQLATTR_TYPE)0;
// NB: SQL_ATTR_SQLPARSERFLAGS  must be distinct & different
//     from all other SQLATTR_TYPE values in cli/sqlcli.h
const SQLATTR_TYPE SQL_ATTR_NOT_SET = (SQLATTR_TYPE)666;
const int SQL_ATTR_NO_VALUE = 999;

class QryStmtAttribute : public NABasicObject {
  friend class QryStmtAttributeSet;
  friend class CmpMain;
  friend class CompilerEnv;
  SQLATTR_TYPE attr;  // name of query statement attribute
  int value;          // value of query statement attribute

 public:
  // default constructor
  QryStmtAttribute() : attr(SQL_ATTR_NOT_SET), value(SQL_ATTR_NO_VALUE) {}

  // constructor
  QryStmtAttribute(SQLATTR_TYPE a, int v) : attr(a), value(v) {}

  // copy constructor
  QryStmtAttribute(const QryStmtAttribute &s) : attr(s.attr), value(s.value) {}

  // destructor
  virtual ~QryStmtAttribute() {}

  // equality comparison used by CompilerEnv::isEqual
  NABoolean isEqual(const QryStmtAttribute &o) const { return attr == o.attr && value == o.value; }
};

const int MAX_STMT_ATTR = 4;

class QryStmtAttributeSet : public NABasicObject {
  friend class CmpMain;
  friend class CompilerEnv;
  // array of query statement attributes
  QryStmtAttribute attrs[MAX_STMT_ATTR];
  int nEntries;  // number of entries in attrs

 public:
  // constructor for query statement attribute settings
  QryStmtAttributeSet();

  // copy constructor for query statement attribute settings
  QryStmtAttributeSet(const QryStmtAttributeSet &s);

  // destructor frees all query statement attribute settings
  virtual ~QryStmtAttributeSet() {}

  // add a query statement attribute to attrs
  void addStmtAttribute(SQLATTR_TYPE a, int v);

  // return true if set has given statement attribute
  NABoolean has(const QryStmtAttribute &a) const;

  // return xth statement attribute
  const QryStmtAttribute &at(int x) const { return attrs[x]; }
};

class CmpMain {
 public:
  // The return code from the sqlcomp procedure.
  enum ReturnStatus {
    SUCCESS = 0,
    PARSERERROR,
    BINDERERROR,
    TRANSFORMERERROR,
    NORMALIZERERROR,
    CHECKRWERROR,
    OPTIMIZERERROR,
    PREGENERROR,
    GENERATORERROR,
    QCACHINGERROR,
    DISPLAYDONE,
    EXCEPTIONERROR,
    PRIVILEGEERROR,
  };

  // Optional phase after which to stop compiling.
  // Not entirely identical to enum PhaseEnum of common/BaseTypes.h
  // which is not really being used and could be gotten rid of...
  // (Also not identical to -- less finely grained than --
  // enum Sqlcmpdbg::CompilationPhase.)
  //
  enum CompilerPhase {
    PREPARSE,
    PARSE,
    BIND,
    TRANSFORM,
    NORMALIZE,
    SEMANTIC_OPTIMIZE,
    ANALYSIS,
    OPTIMIZE,
    PRECODEGEN,
    GENERATOR,
    END
  };

  enum QueryCachingOption {
    NORMAL  // normal operation, attempt to generate cacheable plan and cache it
    ,
    EXPLAIN  // attempt to generate a cacheable plan but do not cache it
    ,
    NOCACHE  // do not attempt a cacheable plan, and do not cache it
  };

  CmpMain();
  virtual ~CmpMain() {}

  // sqlcomp will compile a string of sql text into code from generator
  ReturnStatus sqlcomp(QueryText &input, int /*unused*/, char **gen_code, int *gen_code_len, NAMemory *h = NULL,
                       CompilerPhase = END, FragmentDir **framentDir = NULL,
                       IpcMessageObjType op = CmpMessageObj::SQLTEXT_COMPILE,
                       QueryCachingOption useQueryCache = NORMAL);

  // sqlcomp will compile a RelExpr into code from generator
  ReturnStatus sqlcomp(const char *input_str, int charset, RelExpr *&queryExpr, char **gen_code, int *gen_code_len,
                       NAMemory *h = NULL, CompilerPhase p = END, FragmentDir **fragmentDir = NULL,
                       IpcMessageObjType op = CmpMessageObj::SQLTEXT_COMPILE,
                       QueryCachingOption useQueryCache = NOCACHE, NABoolean *cacheable = NULL, TimeVal *begTime = NULL,
                       NABoolean shouldLog = FALSE);

  // sqlcomp will compile a string of sql text into code from generator.
  // This string is from a static program and is being recompiled
  // dynamically at runtime.
  ReturnStatus sqlcompStatic(QueryText &input, int /*unused*/, char **gen_code, int *gen_code_len, NAMemory *h = NULL,
                             CompilerPhase = END, IpcMessageObjType op = CmpMessageObj::SQLTEXT_COMPILE);

  RelExpr *transform(NormWA &normWA, RelExpr *queryExpr);

  // QSTUFF
  // needed to check when creating a view
  RelExpr *normalize(NormWA &normWA, RelExpr *queryExpr);
  // QSTUFF

  // throw EH_INTERNAL_EXCEPTION ,  in this case CmpStatement::exceptionRaised
  // method should be called for the current CmpStatement to perform cleanup,
  // the main logic should be able to compile next statement.
  // throw EH_BREAK_EXCEPTION , in this case the main program should exit,
  // the environment is not suitable for furthur compilation.
  //
  // will end the transaction if endTrans is TRUE. It should
  // be called before returning to caller in sqlcomp procedures.
  void sqlcompCleanup(const char *input_str, RelExpr *queryExpr, NABoolean endTrans);

  // input arrays maximum size. Required to bind rowsets from ODBC.
  // Value is obtained from CLI statement attribute.
  void setInputArrayMaxsize(const int maxsize);
  int getInputArrayMaxsize() const;

  // save non-zero parserFlags
  void setSqlParserFlags(int f);

  // get query statement attributes
  const QryStmtAttributeSet &getStmtAttributes() const { return attrs; }

  void setRowsetAtomicity(const short atomicity);
  RelExpr::AtomicityType getRowsetAtomicity() const;
  void setHoldableAttr(const short holdable);
  SQLATTRHOLDABLE_INTERNAL_TYPE getHoldableAttr();

  void FlushQueryCachesIfLongTime(int begTimeInSec);

#ifndef NDEBUG
  static int prev_QI_Priv_Value;
#endif

 private:
  CmpMain(const CmpMain &);
  const CmpMain &operator=(const CmpMain &);
  QryStmtAttributeSet attrs;

  ReturnStatus compile(const char *input_str, int charset, RelExpr *&queryExpr, char **gen_code, int *gen_code_len,
                       NAMemory *h, CompilerPhase p, FragmentDir **fragmentDir, IpcMessageObjType op,
                       QueryCachingOption useQueryCache, NABoolean *cacheable, TimeVal *begTime, NABoolean shouldLog);

  // try to compile a query using the cache
  NABoolean compileFromCache(const char *sText,     // (IN) : sql statement text
                             int charset,           // (IN) : character set of  sql statement text
                             RelExpr *queryExpr,    // (IN) : the query to be compiled
                             BindWA *bindWA,        // (IN) : work area (used by backpatchParams)
                             CacheWA &cachewa,      // (IN) : work area for normalizeForCache
                             char **plan,           // (OUT): compiled plan or NULL
                             int *pLen,             // (OUT): length of compiled plan or 0
                             NAHeap *heap,          // (IN) : heap to use for compiled plan
                             IpcMessageObjType op,  //(IN): SQLTEXT_COMPILE or SQLTEXT_RECOMPILE
                             NABoolean &bPatchOK,   //(OUT): true iff backpatch succeeded
                             TimeVal &begTime);     //(IN):  start time for this compile

  void getAndProcessAnySiKeys(TimeVal begTime);

  int getAnySiKeys(TimeVal begTime,             // (IN) start time for compilation
                   TimeVal prev_QI_inval_time,  // (IN) previous Query Invalidation time
                   int *retNumSiKeys,           // (OUT) Rtn'd size of results array
                   TimeVal *pMaxTimestamp,      // (OUT) Rtn'd max Time Stamp
                   SQL_QIKEY *qiKeyArray,       // (OUT) Rtn'd keys stored here
                   int qiKeyArraySize);         // (IN) Size of of results array

  void InvalidateNATableCacheEntries(int returnedNumSiKeys, SQL_QIKEY *qiKeyArray);
  void InvalidateNATableSchemaCacheEntries();
  void InvalidateNATableSchemaCacheEntries(int returnedNumQiKeys, SQL_QIKEY *qiKeyArray);
  void InvalidateNARoutineCacheEntries(int returnedNumSiKeys, SQL_QIKEY *qiKeyArray);
  void InvalidateHistogramCacheEntries(int returnedNumSiKeys, SQL_QIKEY *qiKeyArray);
  void RemoveMarkedNATableCacheEntries();
  void UnmarkMarkedNATableCacheEntries();
  void UpdateNATableCacheEntryStoredStats(int returnedNumQiKeys, SQL_QIKEY *qiKeyArray);


  int RestoreCqdsInHint();
};

RelRoot *CmpTransform(RelExpr *queryExpr);  // global func for convenience

const int CmpDescribeSpaceCountPrefix = sizeof(short);

short CmpDescribe(const char *describeStmt, const RelExpr *queryExpr, char *&outbuf, int &outbuflen, NAMemory *h);

short CmpDescribeControl(Describe *d, char *&outbuf, int &outbuflen, NAMemory *h);
