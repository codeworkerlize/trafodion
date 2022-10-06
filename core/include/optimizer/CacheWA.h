
#ifndef CacheWA__H
#define CacheWA__H
/* -*-C++-*-
*************************************************************************
*
* File:         CacheWA.h
* Description:  The workarea used by {Rel|Item}Expr::normalizeForCache.
*               It holds contextual information needed and generated by
*               isCacheableExpr and normalizeForCache as the compiler
*               decides whether a query is cacheable. For a cacheable
*               query, normalizeForCache replaces query constants into
*               ConstantParameters. ConstantParameters act like wildcards
*               matching all constants that can be safely coerced into
*               that constant's formal type. We cache only queries that
*               have been normalizedForCache. The goal is to maximize the
*               cache hit rate of every cached query plan.
*
*               A query is cacheable after parse iff it is a tuple or
*               tuplelist insert whose tuple values' types are known at
*               parse-time. Thus, tuple inserts that have hostvars, dynamic
*               parameters, subqueries, nulls are not cacheable after parse.
*               "insert into t values(1),(2),(3)" is cacheable after parse.
*               "insert into t values(?p1),(:hv2),(select a from s)" is not.
*               A query is cacheable after bind iff it is an insert, delete,
*               update, select whose compiled plan is insensitive to actual
*               values of literals in that query. Thus, "delete from t where
*               pk = 1" is cacheable after bind iff pk is a primary key or a
*               column with a unique constraint because its compiled plan does
*               not depend on the value "1". "delete from t where pk > 1"
*               is not cacheable because its compiled plan may depend on the
*               value "1".
*               Determining query cacheability is implemented by {Rel|Item}
*               Expr::isCacheableExpr(). Every ExprNode (the base class of
*               RelExpr and ItemExpr) has a protected cacheable_ attribute
*               that specifies that node's cacheability (returned by the
*               isCacheableNode() method).
*               Most ExprNodes start in the MAYBECAHEABLE state.
*               Certain ExprNodes (such as ControlAbstractClass, Describe,
*               StmtNode, ElemDDLNode, RelLock, RelTransaction) that
*               represent non-DML queries are always NONCACHEABLE.
*               (Tuple and tuplelist) Insert nodes (a class derived from
*               RelExpr) become CACHEABLE_PARSE (ie, cacheable after parse)
*               during their isCacheableExpr (top-down) query tree traversal.
*               Some Insert, Delete, Update, Scan nodes (classes derived from
*               RelExpr) become CACHEABLE_BIND (cacheable after bind) during
*               their isCacheableExpr traversal.
*               A query tree that has only MAYBECACHEABLE ExprNodes is not
*               cacheable (ie, its isCacheableExpr() traversal returns FALSE).
*               For a query tree to be cacheable after parse, its
*               isCacheableExpr traversal must encounter a CACHEABLE_PARSE
*               node which causes that query to be marked as
*               cachewa.conditionallyCacheable_ and its descendant nodes are
*               MAYBECACHEABLE or conditionallyCacheable_. No descendant's
*               isCacheableExpr() call must return FALSE. Note that hostvars'
*               and dynamic parameters' isCacheableExpr() return FALSE after
*               parse but return TRUE after bind.
*               For a query tree to be cacheable after bind, its
*               isCacheableExpr traversal must encounter a CACHEABLE_BIND node
*               (an insert, delete, update, scan) and its selection predicate,
*               if present, must have at least one conjunctive equality of a
*               literal against a primary key or a column with a unique
*               constraint.
*               To avoid an unnecessary isCacheableExpr traversal, say, after
*               parse and then after bind, we set a node's cacheable_
*               attribute to become NONCACHEABLE if that query has been
*               determined not to be cacheable after any phase. For example,
*                 "select a from t intersect select b from s where c >= 1"
*               is not cacheable after any phase.
* Created:      August 3, 2000
* Language:     C++
*
*
*
*
*************************************************************************
*/

#include "common/NAString.h"
#include "exp/exp_attrs.h"
#include "optimizer/ItemColRef.h"
#include "optimizer/RelMisc.h"
#include "sqlcomp/QCache.h"

// contents of this file
class CacheWA;
class ConstantParameters;
class SelParameters;

// Forward declarations

// ConstantParameters is used to represent a query's list of actual
// constant parameters after query has undergone normalizeForCache.
class ConstantParameters : public LIST(ConstantParameter *) {
 public:
  // create an empty list
  ConstantParameters(NAHeap *h);

  // copy constructor
  ConstantParameters(const ConstantParameters &s, NAHeap *h) : NAList<ConstantParameter *>(s, h) {}

  // free our allocated memory
  virtual ~ConstantParameters();

  // return our elements' total size in bytes
  int getSize() const;

  // return true & matching element if val is in this list
  NABoolean find(ConstValue *val, ConstantParameter **match);
};

// SelParameters is used to represent a query's list of actual SelParameters
// after query has undergone normalizeForCache.
class SelParameters : public LIST(SelParameter *) {
 public:
  // create an empty list
  SelParameters(NAHeap *h);

  // copy constructor
  SelParameters(const SelParameters &s, NAHeap *h) : NAList<SelParameter *>(s, h) {}

  // free our allocated memory
  virtual ~SelParameters();

  // return our elements' total size in bytes
  int getSize() const;

  // return true & matching element if val is in this list
  NABoolean find(ConstValue *val, SelParameter **match);
};

// CacheWA is the contextual work area for {Rel|Item}Expr::isCacheableExpr
// and {Rel|Item}Expr::normalizeForCache query tree traversals. It holds
// contextual information (such as the current phase of compilation,
// the cacheability of the current query, the
// parameterized query text, the query's cache key, the query root, etc)
// which is needed and generated by {Rel|Item}Expr::isCacheableExpr and
// {Rel|Item}Expr::normalizeForCache traversals of the query tree.
class CacheWA : public NABasicObject {
 public:
  // Constructor
  CacheWA(NAHeap *h);

 private:
  // copy constructor
  CacheWA(const CacheWA &s);  // not written and not used but is here
                              // nevertheless because code inspectors say it should.
                              // This will cause a link error if it's ever called.

 public:
  // Destructor function
  virtual ~CacheWA();

  // Dear code reader/inspector: Please do not (nit)pick on the following
  // accessors as violations of coding guideline 7.3 (a function must never
  // return a pointer to local data). The only purpose in life of cachewa
  // is to hold "global" state for {Rel|Item}Expr::isCacheableExpr and
  // {Rel|Item}Expr::normalizeForCache. Without these accessors, cachewa
  // is useless.

  // replace constant with new or existing ConstantParameter
  void replaceWithNewOrOldConstParam(ConstValue *val, const NAType *typ, ExprValueId &tgt, BindWA &bindWA);

  // replace constant with new or existing SelParameter
  void replaceWithNewOrOldSelParam(ConstValue *val, const NAType *typ, const Selectivity sel, ExprValueId &tgt,
                                   BindWA &bindWA);

  // add ConstantParameter to current query's actual parameters
  void addConstParam(ConstantParameter *p, BindWA &bindwa);

  // add SelParameter to current query's selection parameters
  void addSelParam(SelParameter *p, BindWA &bindwa);

  // add referenced table
  void addTable(NATable *t) { tables_.insert(t); }

  // get referenced tables
  LIST(NATable *) & getTables() { return tables_; }

  // return current query's actual parameters for backpatching cached plan
  ConstantParameters &getConstParams() { return actuals_; }

  // return current query's selection parameters for backpatching cached plan
  SelParameters &getSelParams() { return sels_; }

  LIST(int) & getConstParamPositionsInSql() { return sqlStmtConstParamPos_; }
  LIST(int) & getSelParamPositionsInSql() { return sqlStmtSelParamPos_; }
  LIST(int) & getHQCConstPositionsInSql() { return hqcSqlConstPos_; }

  void bindConstant2SQC(BaseColumn *base, ConstantParameter *cParameter) {
    if (HQCKey_ && HQCKey_->isCacheable()) HQCKey_->bindConstant2SQC(base, cParameter, hqcSqlConstPos_);
  }

  void setHQCKey(HQCParseKey *key) { HQCKey_ = key; }

  // return current query's formal parameter types
  const ParameterTypeList *getFormalParamTypes();

  // return current query's formal SelParamTypes
  const SelParamTypeList *getFormalSelParamTypes();

  // compose and return TextKey of current query
  TextKey *getTextKey(const char *sText, int charset, const QryStmtAttributeSet &attrs);

  // traverse queryExpr and put together its cacheKey
  void generateCacheKey(RelExpr *queryExpr, const char *sText, int charset, const NAString &viewsUsed);

  // save cqdsInHint into cwa.qryText_
  void generateCacheKeyFromCqdsInHint();
  // caller wants us to remember an occurrence of a view join
  void foundViewJoin();

  // compose and return CacheKey of current query
  CacheKey *getCacheKey(const QryStmtAttributeSet &attrs);

  // return current phase of compilation
  CmpPhase getPhase() const { return phase_; }

  // used by subsequent phases to check if query is definitely cacheable
  NABoolean isCacheable() const { return cacheable_; }

  // is current query conditionally cacheable?
  NABoolean isConditionallyCacheable() const { return conditionallyCacheable_; }

  // mark current query as definitely cacheable
  void setCacheable() { cacheable_ = TRUE; }

  // mark current query as definitely not cacheable
  void resetCacheable() { cacheable_ = FALSE; }

  // tilt current query towards cacheability
  void setConditionallyCacheable() { conditionallyCacheable_ = TRUE; }

  // record current phase of compilation
  void setPhase(CmpPhase p) { phase_ = p; }

  // remember current query's true root
  void setTopRoot(RelRoot *root) { topRoot_ = root; }

  // return compiler's statementHeap
  NAHeap *wHeap() const { return heap_; }

  // append string s to current query's parameterized sql text
  void operator+=(const char *s);

  // add referenced column's valueid to usedKeys
  void addToUsedKeys(BaseColumn *base);

  // is this column parameterizable?
  NABoolean isParameterizable(BaseColumn *base);

  void setHasPredicate() { hasPredicate_ = TRUE; }
  void setPredHasNoLit(NABoolean v) { predHasNoLit_ = v; }

  NABoolean inc_N_check_still_cacheable();
  void resetNofExprs() { numberOfExprs_ = 0; }

  void incNofScans(TableDesc *td);

  TransMode::IsolationLevel getIsoLvl() { return isoLvl_; }
  TransMode::AccessMode getAccessMode() { return accMode_; }

  TransMode::IsolationLevel getIsoLvlForUpdates() { return isoLvlIDU_; }

  void setIsoLvl(TransMode::IsolationLevel l) { isoLvl_ = l; }
  void setAccessMode(TransMode::AccessMode m) { accMode_ = m; }

  void setIsoLvlForUpdates(TransMode::IsolationLevel l) { isoLvlIDU_ = l; }

  Int16 getFlags() { return flags_; }
  void setFlags(Int16 f) { flags_ = f; }

  void setAutoCommit(TransMode::AutoCommit a) { autoCmt_ = a; }
  void setRollbackMode(TransMode::RollbackMode r) { rbackMode_ = r; }
  void setAutoabortInterval(int val) { autoabortInterval_ = val; }
  void setMultiCommit(TransMode::MultiCommit a) { multiCmt_ = a; }

  NABoolean hasRewriteEnabledMV() { return hasRewriteEnabledMV_; }
  void setRewriteEnabledMV(NABoolean x = TRUE) { hasRewriteEnabledMV_ = x; }

  NABoolean hasParameterizedPred() { return hasParameterizedPred_; }
  void setParameterizedPred(NABoolean x = TRUE) { hasParameterizedPred_ = x; }

  NABoolean isUpdate() { return isUpdate_; };
  void setIsUpdate(NABoolean x = TRUE) { isUpdate_ = x; };

  NABoolean usePartitionTable() { return usePartitionTable_; };
  void setUsePartitionTable(NABoolean x = TRUE) { usePartitionTable_ = x; };

  NAString reqdShape_;  // control query shape or empty string

 private:
  typedef TableDesc *TableDescPtr;

  HQCParseKey *HQCKey_;  // collect histogram info for a Hybrid Cache Key during NormalizeForCache.

  int numberOfExprs_;  // number of ExprNodes in current query
  // a query with more than N ExprNodes is not cacheable

  int numberOfScans_;  // number of Scan nodes in current query

  TableDescPtr *tabDescPtr_;  // array of TableDesc pointers
  ValueIdSet **usedKyPtr_;    // array of ValueIdSet pointers
  int tabArraySize_;          // size of the tabDescPtr_ and usedKyPtr_ arrays

  int requiredPrefixKeys_;

  NABoolean hasPredicate_;  // TRUE if query has a WHERE clause
  NABoolean predHasNoLit_;  // TRUE if predicate has no constants

  NAHeap *heap_;      // compiler's statementHeap
  NAString qryText_;  // parameterized sql statement text
  TextKey *tkey_;     // text key of current query
  CacheKey *ckey_;    // cache key of current query

  TransMode::IsolationLevel isoLvl_;     // tx isolation level
  TransMode::AccessMode accMode_;        // tx access mode
  TransMode::IsolationLevel isoLvlIDU_;  // tx isolation level for updates

  TransMode::AutoCommit autoCmt_;      // tx auto-commit
  TransMode::MultiCommit multiCmt_;    // tx multi-commit
  Int16 flags_;                        // tx flags
  TransMode::RollbackMode rbackMode_;  // tx rollback mode
  int autoabortInterval_;              // tx autoabortInterval

  SelParameters sels_;  // list of actual selection parameters
  LIST(int) sqlStmtConstParamPos_;
  // list of positions in sql stmt
  // that each constant occurs. Each is replaced
  // by a const parameter
  LIST(int) sqlStmtSelParamPos_;
  // list of positions in sql stmt
  // that each constant occurs. Each is replaced
  // by a selParameter
  LIST(int) hqcSqlConstPos_;
  // used by the HQC logic to keep track of the correct
  // order of the query constants
  UInt32 posCounter_;
  // used for keeping track of positions of aliased SelParams & ParamTypes
  // valid positions are 1 up to count of SelParams & ConstantParameters

  SelParamTypeList *selTypes_;  // types & selectivities of sel parameters
  // selTypes_ is always derived from sels_; it's only purpose in life
  // is to allow ~CacheWA() to deallocate selTypes_.

  ConstantParameters actuals_;    // list of actual constant parameters
  ParameterTypeList *parmTypes_;  // types of actual constant parameters
  // parmTypes_ is always derived from actuals_; it's only purpose in life
  // is to allow ~CacheWA() to deallocate parmTypes_.

  CmpPhase phase_;       // compiler cache phase (PARSE or BIND)
  RelRoot *topRoot_;     // has inputVarTree
  NABoolean cacheable_;  // true iff current query is cacheable

  // conditionallyCacheable_ is used by RelExpr::isCacheableExpr & its cohorts.
  // It is set to true iff a RelExpr::isCacheableExpr() traversal has found
  // a query node, such as a tuple insert that we definitely want to be
  // cacheable at this phase subject only to the condition that it does
  // not have any non-cacheable components. For example, we want all
  // tuple inserts whose constants' types are reasonably-defined such as
  //   insert into t values(1, 3.14);
  // to be cacheable after parse. However, dynamic-parameter-bearing
  // tuple inserts such as
  //   insert into t values(?p1, ?p2);
  // should be cacheable only after bind.
  NABoolean conditionallyCacheable_;

  NABoolean useView_;
  // must be set to true for cacheable query that references a view.
  // used to suppress text caching of such a query to avoid subverting
  // revocation of privilege on referenced view(s).

  NABoolean usePartitionTable_;

  NABoolean isViewJoin_;  // true iff current query is a view join
                          // we want to safely cache view joins. But, there are view joins such as
                          // these two joins from regress/qatdml07
                          //   select avg(firstt.large_int) from pvsel01 firstt, pvsel01 secondd
                          //     group by firstt.medium_int;
                          //   select avg(secondd.large_int) from pvsel01 firstt, pvsel01 secondd
                          //     group by firstt.medium_int;
                          // whose only difference is in their "avg(<basecolref>)". Unfortunately,
                          // after view expansion, both "firstt.large_int" and "secondd.large_int"
                          // resolve to the same fully qualified <basecolref> name
                          // "cat.sch.btsel01.large_int". The end result can be a false cache hit.
                          // To avoid this problem, isViewJoin_ is set true when such a view join
                          // is detected by RelCache.cpp's Join::isCacheableExpr() and later used
                          // to prepend the query's original text into its cachekey.

  NABoolean isUpdate_;  // true iff the current query contains an update

  // list of referenced tables for getting to their histograms' timestamps.
  // used for "passive invalidation" of oached plans.
  LIST(NATable *) tables_;

  NABoolean hasRewriteEnabledMV_;
  // true iff query references table that has rewrite enabled MV

  NABoolean hasParameterizedPred_;
  // true iff query has a parameterized equality predicate
};  // class CacheWA

#endif /* CacheWA__H */
