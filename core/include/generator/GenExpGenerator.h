
#ifndef GEN_EXPGENERATOR_H
#define GEN_EXPGENERATOR_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         GenExpGenerator.h
 * Description:  This class defines the expression generator.
 *
 *
 * Created:      4/15/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "GenMapTable.h"
#include "Generator.h"
#include "comexe/ComKeyRange.h"
#include "common/ComSpace.h"
#include "common/charinfo.h"
#include "exp/exp_expr.h"
#include "exp/exp_tuple_desc.h"
#include "sqlcomp/parser.h"

class RETDesc;
class IndexDesc;
class NAColumnArray;
class SearchKey;
class NAType;
class MdamKey;
class NAFileSet;
class Queue;

typedef const NAString *ConstNAStringPtr;
typedef const QualifiedName *ConstQualifiedNamePtr;

/////////////////////////////////////////////////////////////////////
// class AListNode.
//   This is a simple implementation of a list. Only operations
//   supported are appending to end of list and retrieving an
//   element. Used to keep track of generated clauses (to back patch
//   next clause address) and constants in the expression (to save
//   constants at the end of expression generation). The last node
//   of the list is the node whose element pointer is null.
/////////////////////////////////////////////////////////////////////
class AListNode0 : public NABasicObject {
  void *element;
  AListNode0 *next;
  CollHeap *wHeap_;

  void addLast(void *element_) {
    if (!next) {
      element = element_;
      next = new (wHeap_) AListNode0(wHeap_);
    }
  }

 public:
  AListNode0(CollHeap *wHeap = CmpCommon::statementHeap()) {
    element = 0;
    next = 0;
    wHeap_ = wHeap;
  };

  ~AListNode0() {
    if (next) {
      delete next;
      element = 0;
      next = 0;
    }
  };

  inline void *getElement() { return element; };
  inline AListNode0 *getNext() { return next; };
  void linkElement(void *element_) {
    // NB: prefer iteration over recursion because:
    //  1) iteration is more efficient
    //  2) recursion can overflow the stack
    if (!next) {
      addLast(element_);
    } else {
      AListNode0 *predecessor = next;
      AListNode0 *successor = next;
      while (successor) {
        predecessor = successor;
        successor = successor->getNext();
      }
      predecessor->addLast(element_);
    }
  };
};

/////////////////////////////////////////////////////////////////////
// AListNode appends take O(1) instead of O(n) time
/////////////////////////////////////////////////////////////////////
class AListNode : public NABasicObject {
  AListNode0 *head_;
  AListNode0 *tail_;

 public:
  AListNode(CollHeap *wHeap = CmpCommon::statementHeap()) : tail_(0) { head_ = new (wHeap) AListNode0(wHeap); };

  ~AListNode() {
    if (head_) {
      delete head_;
      head_ = NULL;
    }
    tail_ = NULL;
  };

  void *getElement() { return head_->getElement(); };

  AListNode0 *getNext() { return head_->getNext(); };

  AListNode0 *getHead() { return head_; };

  void linkElement(void *element_) {
    if (!tail_) {
      tail_ = head_;
    }
    tail_->linkElement(element_);
    tail_ = tail_->getNext();
  };
};

/////////////////////////////////////////////////////////////////
// class ExpGeneratorCacheSubstitution
//
// This class is used by ExpGeneratorCache to keep track of
// which expressions have been substituted for which in the
// course of common subexpression elimination. We need to keep
// track of this because we must undo it when we leave the
// context of an expression.
//
// For example, suppose that two equivalent ItemExprs are returned
// from DP2, and one of these is a characteristic output of the
// PartitionAccess node above the DP2 subtree. When PartitionAccess::codeGen
// generates the DP2 root move expression, it inserts Convert nodes
// on top of these ItemExprs so that the values can be moved into
// contiguous space. This has the effect of making these ItemExprs
// into temporary variables from the point of view of the move expression.
// and therefore we will do common subexpression elimination on them,
// setting the valueId in one to that of the other. Suppose by chance,
// the valueId that is in the characteristic output has been replaced.
//
// When the Generator pops back into Executor context (to generate
// the Executor side of the PartitionAccess), it wants to map the
// characteristic outputs and mark them as already generated. (We have
// to do this because the DP2 context is destroyed by the "pop", and
// we don't want to recompute what DP2 will already compute.) But,
// if we replaced the valueId with another which is not in the
// characteristic outputs, it won't be marked as already generated,
// and we may later attempt to generate it again. However, the values
// necessary to generate the value may not be mapped (having been left
// behind in the DP2 context), resulting in a Generator internal error.
//
// Thus, we must undo any valueId changes we make to ItemExpr nodes
// when we exit the context of generating a single expression.
/////////////////////////////////////////////////////////////////
class ExpGeneratorCacheSubstitution : public NABasicObject {
 private:
  ItemExpr *ie_;        // the ItemExpr that we changed
  ValueId oldValueId_;  // its valueId before common subexpression elimination
  ValueId newValueId_;  // its valueId after common subexpression elimination

 public:
  // the constructor performs valueId substitution on ie
  ExpGeneratorCacheSubstitution(ItemExpr *ie, ValueId &newValueId);

  ~ExpGeneratorCacheSubstitution(){};

  void undo(void);  // undoes a valueId substitution (idempotently)
};

/////////////////////////////////////////////////////////////////
// class ExpGeneratorCache
//
// This class is a helper class used by the expression generator
// to do common subexpression elimination local to a given
// expression. Of course, in the long run, it would be better
// to push common subexpression elimination earlier in the compile
// (e.g. to Bind time), but that will have to wait for now.
// When such support is available, this class and its methods
// will become obsolete and can be deleted.
////////////////////////////////////////////////////////////////
class ExpGeneratorCache : public NABasicObject {
 private:
  Generator *generator_;  // so we can get access to heaps, etc.

  // A note about storage policy: This class "owns" any objects
  // referenced by the substitutions_ list, and therefore is
  // responsible for deleting them. However, it does not "own"
  // any objects referenced by the cachedCasts_ or cachedBiAriths_
  // lists, and therefore must NOT delete them.

  // for now, we'll cache only Cast and BiArith expressions
  LIST(Cast *) cachedCasts_;
  LIST(BiArith *) cachedBiAriths_;

  // a list of any valueId substitutions made during the course
  // of common subexpression elimination
  LIST(ExpGeneratorCacheSubstitution *) substitutions_;

  // Some kinds of expressions involve short-circuit evaluation;
  // we have to be careful not to use a subexpression that may
  // have not been evaluated. The variable below is incremented
  // whenever we descend into a tree that is subject to short-
  // circuit evaluation, and decremented whenever we rise out
  // of that tree.
  int level_;

  // for testing purposes (so we can toggle old vs. new behavior)
  NABoolean enableCommonSubexpressionElimination_;

 public:
  ExpGeneratorCache(Generator *generator);
  ~ExpGeneratorCache();

  // add an expression (which has had code generated) to the cache
  void add(ItemExpr *ie);

  // attempts to eliminate the subexpression ie; returns TRUE if
  // successful (in which case ie's ValueId is side-effected
  // and the substitution is recorded in the substitutions_
  // member of this class)
  NABoolean expressionCanBeEliminated(ItemExpr *ie);

  // for suppressing caching of conditionally evaluated expressions
  // (could later generalize to a stack of ExpGeneratorCaches, so
  // that expressions could be eliminated in local context)
  void incrementLevel(void);

  void decrementLevel(void);

  // clear cache (which, as a side effect, undoes substitutions)
  void clear(void);

  void setEnableCommonSubexpressionElimination(NABoolean v) { enableCommonSubexpressionElimination_ = v; }

  NABoolean enableCommonSubexpressionElimination() { return enableCommonSubexpressionElimination_; }

 private:
  // find an expression which has already been generated equivalent
  // to the input parameter; if none exists, returns NULL
  ItemExpr *findEquivalentGeneratedExpr(ItemExpr *ie);

};  // class ExpGeneratorCache

/////////////////////////////////////////////////////////////////
// class ExpGenerator
////////////////////////////////////////////////////////////////
class ExpGenerator : public NABasicObject {
  // points to the high level generator
  Generator *generator;

  // points to cache of generated expressions (for common subexpression
  // elimination)
  ExpGeneratorCache *cache_;

  // set to true, if a new map table is allocated to generate
  // this expression.
  short mapTableAllocated;

  // the map table which was allocated.
  MapTable *mapTable_;

  // anchor to the list of generated clauses.
  AListNode *clause_list;

  // anchor to the list of generated constants
  AListNode *constant_list;

  // anchor to the list of generated persistent intializers.
  AListNode *persistentList_;

  // length of constants area
  int constants_length;

  // length of temps area
  long temps_length;

  // length of persistent area
  int persistentLength_;

  enum Flags {
    // this flag, if set, indicates that the 'address' of
    // constants and temps that are part of expression
    // could be computed once at fixup
    // time and assigned to the corresponding 'offset'
    // fields in Attributes. To be set if we know for sure
    // that the expression will not be shared among many
    // TCB's at runtime. If this flag is set when the expression
    // is being generated, then a similar flag is set in class
    // ex_expr.
    FIXUP_CONSTS_AND_TEMPS = 0x0001,

    SHOWPLAN_ = 0x0002,
    NOTRANSACTION_ = 0x0003,

    // This flag is set whenever a new clause is generated and
    // linked by calling ExpGenerator::linkClause method.
    // The linkClause method attaches the generated clause to
    // the ItemExpr node for which it was generated.
    // This flag is reset before and after we generate code for any
    // ItemExpr in ItemExpr::codegen_and_set_attrs.
    // It is used by the parent to access Attributes of
    // its children by looking at the appropriate operand of the
    // generated clause instead of searching for it in the Map
    // table. This is an optimization.
    CLAUSE_LINKED_ = 0x0004,

    // This flag is set for all inserts and simple non-optimized updates.
    // It is set before generating any expressions, then cleared at the end.
    FOR_INSERT_UPDATE_ = 0x0008,

    // This flag is used to indicate to the expression generator
    // that there is an expression (ex_expr)  within an expression (ex_expr)
    // and that both  expressions must be generated in one space - tempSpace or
    // fragment space.
    IN_CONTAINER_EXPR_ = 0x0010,

    // This flag is used to indicate that pcode can be generated for expressions
    // involving varchars.
    HANDLE_INDIRECT_VC_ = 0x0020,

    // This flag is used to indicate if a case statement was generated.
    // Predicate terms for case statements can't be reordered, and so PCODE
    // opts must account for this.
    CASE_STMT_GENERATED_ = 0x0040,

    // In specific situations we don't generate a header clause when
    // generating a contiguous move expression.  An optimized update is one
    // case - here the entire fetched row is copied to the update buffer, then
    // specific fields are updated.  The header clause is part of the entire
    // fetched row thus does not need to be generated.
    GEN_NO_HDR_ = 0x0080,

    // if pcode has been generated, then expression generator removes the
    // clauses (except for in case of showplan or clause_eval).
    // If this flag is set, then clauses are not removed.
    SAVE_CLAUSES_IN_EXPR = 0x0100,

    IN_SEQUENCE_FUNC_EXPR = 0x0200,

    // link constant even if it is found in map table. This is currently
    // needed for nested composite expressions which may have a constant
    // that exists in caller map table. For composite expressions, we want
    // to add the const to the nested expression.
    FORCE_LINK_CONST = 0x0400,
  };

  UInt16 pCodeMode_;
  Int16 savedPCodeMode_;

  int flags_;

  // For processing ROWS SINCE operator.

  ItemExpr *rowsSinceCounter_;

 public:
  ExpGenerator(Generator *generator_);
  ~ExpGenerator();

  unsigned short getPCodeMode() { return pCodeMode_; }
  void setPCodeMode(Int16 mode) { pCodeMode_ = mode; }
  void addPCodeMode(Int16 mode) { pCodeMode_ |= mode; }

  //
  // Disable and save PCode generation in specific cases.
  // Key encoding is the current scenario that does not work with pCode if
  // data rows are being expanded.
  void disablePCodeModeIfNeeded(Int16 genPCode, NABoolean expandShortRows, NABoolean addedFields) {
    if (genPCode && expandShortRows && addedFields) {
      // PCode can not work on short rows since the key may have shifted
      // due to:
      //   1) adding a varchar field when a fixed field is the key
      //   2) or adding a fixed field and a varchar is the key
      //   3) or with Aligned format if a nullable field is added
      //      such that the null bitmap must expand past the current size
      //      or the null bitmap must be added when it wasn't present
      savedPCodeMode_ = getPCodeMode();
      setPCodeMode(ex_expr::PCODE_NONE);
    }
  }

  void savePCodeMode() { savedPCodeMode_ = getPCodeMode(); }

  //
  // Restore any saved PCode mode ...
  void restorePCodeMode() {
    if (savedPCodeMode_ > 0) {
      pCodeMode_ = savedPCodeMode_;
      savedPCodeMode_ = -1;
    }
  }

  // input is a ValueIdSet of aggregate nodes
  short generateAggrExpr(const ValueIdSet &val_id_set, ex_expr::exp_node_type node_type, ex_expr **expr,
                         short gen_last_clause, NABoolean groupByOperation, const ValueIdSet *vid = NULL,
                         ExpTupleDesc **tupleDesc = NULL,
                         ExpTupleDesc::TupleDescFormat tdescF = ExpTupleDesc::LONG_FORMAT);

  // input is a ValueId which points to an arithmetic expression.
  short generateArithExpr(const ValueId &val_id, ex_expr::exp_node_type node_type, ex_expr **expr);

  // Generate a bulk move for Aligned Format
  short generateBulkMoveAligned(const ValueIdList &inValIdList, ValueIdList &outValIdList, UInt32 tupleLength,
                                int *bulkMoveSrcStartOffset = NULL  // IN(O)
  );

  // Generate a bulk move for Exploded Format
  short generateBulkMove(const ValueIdList &inValIdList, ValueIdList &outValIdList, int tupleLength,
                         int *bulkMoveSrcStartOffset = NULL);  // IN(O)

  // See GenExpGenerator.C for details.
  NABoolean processKeyEncodingOptimization(const NAColumnArray &allColumns, const NAColumnArray &indexKeyColumns,
                                           const ValueIdList &indexKey, const short keyTag, int &keyLen,
                                           int &firstKeyColumnOffset);

  NABoolean isKeyEncodingNeeded(const IndexDesc *indexDesc, int &keyLen, int &firstKeyColumnOffset);

  ItemExpr *generateKeyCast(const ValueId vid, ItemExpr *dataConversionErrorFlag, NABoolean desc_flag,
                            ExpTupleDesc::TupleDataFormat tf, int &possibleErrorCount,
                            NABoolean allChosenPredsAreEqualPreds, NABoolean castVarcharToAnsiChar);

  // get the key value. Currently used during hbase checkAndDelete/Update
  // calls. Used to validate that this value exists in the database.
  short generateKeyColValueExpr(const ValueId vid, int atp, int atp_index, int &len, ex_expr **colValExpr);

  // input is the index descriptor. Generate an expression to
  // extract and encode the key of the table and create a
  // contiguous row of encoded keys.
  // If optimizeKeyEncoding is TRUE, then check to see if encoding could
  // be avoided. If so, return the offset to the first key column in
  // the data row.
  short generateKeyEncodeExpr(const IndexDesc *indexDesc, int atp, int atp_index, ExpTupleDesc::TupleDataFormat tf,
                              int &keyLen, ex_expr **key_expr, NABoolean optimizeKeyEncoding, int &firstKeyColumnOffset,
                              const ValueIdList *keyList = NULL, NABoolean handleSerialization = FALSE);

  short generateExtractKeyColsExpr(const ValueIdList &colVidList,  // const IndexDesc * indexDesc,
                                   int atp, int atp_index, int &keyLen, ex_expr **key_expr);

  // input is a value id list of key predicates
  short generateKeyExpr(const NAColumnArray &indexKeyColumns, const ValueIdList &val_id_list, int atp, int atp_index,
                        ItemExpr *dataConversionErrorFlag, ExpTupleDesc::TupleDataFormat tf, int &keyLen,
                        ex_expr **key_expr, NABoolean allChosenPredsAreEqualPreds);

  short generateDeserializedMoveExpr(const ValueIdList &valIdList, int atp, int atpIndex,
                                     ExpTupleDesc::TupleDataFormat tdataF, int &tupleLength, ex_expr **moveExpr,
                                     ExpTupleDesc **tupleDesc, ExpTupleDesc::TupleDescFormat tdescF,
                                     ValueIdList &deserColVIDlist, ValueIdSet &alreadyDeserialized);

  // generate expression for an exclude flag for a key value
  short generateExclusionExpr(ItemExpr *expr, int atp, int atpindex, ex_expr **excl_expr);

  // generate sampling expression
  short generateSamplingExpr(const ValueId &valId, ex_expr **balanceExpr, int &returnFactorOffset);

  // input is a ValueIdSet
  short generateSetExpr(const ValueIdSet &val_id_set, ex_expr::exp_node_type node_type, ex_expr **expr,
                        Int16 gen_last_clause = -1, ExpHdrInfo *hdrInfo = NULL);

  short generateSequenceExpression(const ValueIdSet &sequenceItems, ex_expr *&expr);

  // input is a ValueIdList
  short generateListExpr(const ValueIdList &val_id_list, ex_expr::exp_node_type node_type, ex_expr **expr, int atp = -1,
                         int atpIndex = -1, ExpHdrInfo *hdrInfo = NULL);

  short genGuardedListExpr(const ValueIdSet guard, const ValueIdList &val_id_list, ex_expr::exp_node_type node_type,
                           ex_expr **expr);

  // input is a ValueId. Generate code for the ItemExprTree corresponding
  // to this ValueId.
  short generateExpr(const ValueId &val_id, ex_expr::exp_node_type node_type, ex_expr **expr);

  // input is a list of ValueIdUnion.
  short generateUnionExpr(const ValueIdList &val_id_list, ex_expr::exp_node_type node_type, ex_expr **left_expr,
                          ex_expr **right_expr);

  // generate code to do Describe Input and move input values
  // from user area to executor area.
  short generateInputExpr(const ValueIdList &val_id_list, ex_expr::exp_node_type node_type, ex_expr **expr);

  // generate code to do Describe Output and move output values
  // from executor area to user area.
  short generateOutputExpr(const ValueIdList &val_id_list, ex_expr::exp_node_type node_type, ex_expr **expr,
                           RETDesc *ret_desc, const ItemExprList *spOutParams, ConstNAStringPtr *colNamesForExpr,
                           ConstQualifiedNamePtr *tblNamesForExpr);

  //////////////////////////////////////////////////////////////////////
  // Input is a ValueIdList to be copied into a contiguous buffer.
  // Adds convert nodes before moving, if addConvNodes is true.
  // Otherwise, assign contiguous offsets to the root item expr nodes
  // for each value id in val_id_list.
  // The result row has the specified tuple format tf.
  // Generates and returns a new map table for the moved values.
  // Also, optionally returns the description of the generated
  // contiguous row in tupleDesc.
  // Note: IN = input parameter. OUT = output required value.
  //       OUT(O) = output optional value.
  ///////////////////////////////////////////////////////////////////////
  short generateContiguousMoveExpr(const ValueIdList &val_id_list,                                     // IN
                                   short addConvNodes,                                                 // IN
                                   int atp,                                                            // IN
                                   int atpIndex,                                                       // IN
                                   ExpTupleDesc::TupleDataFormat tf,                                   // IN
                                   int &tupleLength,                                                   // OUT
                                   ex_expr **moveExpr,                                                 // OUT
                                   ExpTupleDesc **tupleDesc = NULL,                                    // OUT(O)
                                   ExpTupleDesc::TupleDescFormat tdescF = ExpTupleDesc::SHORT_FORMAT,  // IN(O)
                                   MapTable **newMapTable = NULL,                                      // OUT(O)
                                   ValueIdList *tgtValues = NULL,                                      // OUT(O)
                                   int start_offset = 0,                                               // IN(O)
                                   int *bulkMoveSrcStartOffset = NULL,                                 // IN(O)
                                   NABoolean disableConstFolding = FALSE,                              // IN
                                   NAColumnArray *colArray = NULL,                                     // IN
                                   NABoolean doBulkMoves = TRUE,                                       // IN
                                   Attributes **offsets = NULL);                                       // IN

  short genGuardedContigMoveExpr(const ValueIdSet guard,
                                 const ValueIdList &val_id_list,                                     // IN
                                 short addConvNodes,                                                 // IN
                                 int atp,                                                            // IN
                                 int atpIndex,                                                       // IN
                                 ExpTupleDesc::TupleDataFormat tf,                                   // IN
                                 int &tupleLength,                                                   // OUT
                                 ex_expr **moveExpr,                                                 // OUT
                                 ExpTupleDesc **tupleDesc = NULL,                                    // OUT(O)
                                 ExpTupleDesc::TupleDescFormat tdescF = ExpTupleDesc::SHORT_FORMAT,  // IN(O)
                                 MapTable **newMapTable = NULL,                                      // OUT(O)
                                 ValueIdList *tgtValues = NULL,                                      // OUT(O)
                                 int start_offset = 0);                                              // IN(O)

  // static method to evaluate a predicate at compile time
  static ex_expr::exp_return_type genEvalPredicate(ItemExpr *rootPtr, ComDiagsArea *diagsArea);

  ///////////////////////////////////////////////////////////////////////
  // Take an array of Attributes and assign offsets to them. Also make an
  // (optional) tuple descriptor (either in long or in short format).
  // Don't generate expression, don't update any map tables.
  ///////////////////////////////////////////////////////////////////////
  short processAttributes(int numAttrs, Attributes **attrs, ExpTupleDesc::TupleDataFormat tdataF, int &tupleLength,
                          int atp, int atpIndex, ExpTupleDesc **tupleDesc = NULL,
                          ExpTupleDesc::TupleDescFormat tdescF = ExpTupleDesc::SHORT_FORMAT, int startOffset = 0,
                          ExpHdrInfo *hdrInfo = NULL, Attributes **offsets = NULL);

  ///////////////////////////////////////////////////////////////////////
  // similar to processAttributes, but this time we create new Attributes
  // and add them as a new map table to the current stack of map tables
  ///////////////////////////////////////////////////////////////////////
  short processValIdList(ValueIdList valIdList, ExpTupleDesc::TupleDataFormat tdataF, int &tupleLength, int atp,
                         int atpIndex, ExpTupleDesc **tupleDesc = NULL,
                         ExpTupleDesc::TupleDescFormat tdescF = ExpTupleDesc::SHORT_FORMAT, int startOffset = 0,
                         Attributes ***returnedAttrs = NULL,  // sorry about the *** :-(
                         NAColumnArray *colArray = NULL, NABoolean isIndex = FALSE,
                         NABoolean placeGuOutputFunctionsAtEnd = FALSE, ExpHdrInfo *hdrInfo = NULL);

  short computeTupleSize(const ValueIdList &valIdList, ExpTupleDesc::TupleDataFormat tdataF, int &tupleLength,
                         int startOffset = 0, UInt32 *varCharSize = NULL, UInt32 *headerSizePtr = NULL);

  short assignAtpAndAtpIndex(ValueIdList valIdList, int atp, int atpIndex);

  // RETURN: -1, if an error. 0, if all ok.
  int foldConstants(ItemExpr *inExpr, ItemExpr **outExpr);

  //////////////////////////////////////////////////////////////////////
  // Input is a ValueIdList to be exploded into a contiguous buffer.
  // Every other parameter is similar to generateContiguousMoveExpr.
  ///////////////////////////////////////////////////////////////////////
  short generateExplodeExpr(const ValueIdList &val_id_list,                                     // IN
                            int atp,                                                            // IN
                            int atpIndex,                                                       // IN
                            ExpTupleDesc::TupleDataFormat tf,                                   // IN
                            int &tupleLength,                                                   // OUT
                            ex_expr **moveExpr,                                                 // OUT
                            ExpTupleDesc **tupleDesc = NULL,                                    // OUT(O)
                            ExpTupleDesc::TupleDescFormat tdescF = ExpTupleDesc::SHORT_FORMAT,  // IN(O)
                            MapTable **newMapTable = NULL,                                      // OUT(O)
                            ValueIdList *tgtValues = NULL,                                      // OUT(O)
                            int start_offset = 0);                                              // IN(O)

  short generateHeaderClause(int atp, int atpIndex, ExpHdrInfo *hdrInfo);

  // init expr generation with space allocation
  void initExprGen();

  // called at the start of expr generation
  short startExprGen(ex_expr **expr, ex_expr::exp_node_type node_type);
  //		     short allocateMapTable = -1);

  // called at the end of expr generation
  short endExprGen(ex_expr **expr, short gen_last_clause = -1);

  void incrementLevel(void) { cache_->incrementLevel(); };
  void decrementLevel(void) { cache_->decrementLevel(); };

  void setEnableCommonSubexpressionElimination(NABoolean v) { cache_->setEnableCommonSubexpressionElimination(v); }

  NABoolean enableCommonSubexpressionElimination() { return cache_->enableCommonSubexpressionElimination(); }

  // proc defined in GenItemExpr.C
  short genItemExpr(ItemExpr *item_expr, Attributes ***out_attr, int num_attrs, short gen_child);

  inline MapTable *getMapTable() { return generator->getMapTable(); };
  inline Space *getSpace() { return generator->getSpace(); };
  NABoolean genLeanExpr() { return generator->genLeanExpr(); };

  inline AListNode0 *getClauseList() { return clause_list->getHead(); };
  void initClauseList();
  void linkClause(ItemExpr *node, ex_clause *clause);
  void fixupNextClause();

  void initConstantList();
  AListNode *getConstantList();
  void linkConstant(void *expr_tree);
  char *placeConstants(AListNode *list, int length);

  // generate a null constant and type it to input type
  ConstValue *generateNullConst(const NAType &type);

  // Methods for persistent expression storage
  //
  void initPersistentList();
  void linkPersistent(void *expTree);
  AListNode *getPersistentList();
  MapInfo *addPersistent(ValueId val, MapTable *mapTable);
  inline int &persistentLength() { return persistentLength_; };

  // static utility functions to create min/max constants and to convert
  // an NAType to an Attributes object
  static ItemExpr *getMinMaxValue(const NAType &type, short min_max /*0=min, -1=max, -2=max-without-null*/);

  static Attributes *convertNATypeToAttributes(const NAType &t, CollHeap *h = 0);

  Attributes::DefaultClass getDefaultClass(const NAColumn *col);

  short addDefaultValue(NAColumn *col, Attributes *attr, ComDiagsArea *diagsArea = NULL,
                        COM_VERSION objectSchemaVersion = COM_VERS_UNKNOWN);

  void addDefaultValues(const ValueIdList &val_id_list, const NAColumnArray &naColArr, ExpTupleDesc *tupleDesc,
                        NABoolean inputIsColumnList = FALSE);

  void copyDefaultValues(ExpTupleDesc *tgtTupleDesc, ExpTupleDesc *srcTupleDesc);

  short genColNameList(const NAColumnArray &naColArr, Queue *&colNameList);

  ItemExpr *createExprTree(const char *str, UInt32 strlength = 0,
                           /* CharInfo::CharSet strCharSet = CharInfo::UnknownCharSet, */
                           int num_params = 0, ItemExpr *p1 = 0, ItemExpr *p2 = 0, ItemExpr *p3 = 0, ItemExpr *p4 = 0,
                           ItemExpr *p5 = 0, ItemExpr *p6 = 0) {
    Parser parser(generator->currentCmpContext());
    return parser.getItemExprTree(str, strlength, /* for now */ CharInfo::ISO88591, num_params, p1, p2, p3, p4, p5, p6);
  }

  ItemExpr *createExprTree(const char *str, CharInfo::CharSet charSet = CharInfo::ISO88591, UInt32 strlength = 0,
                           int num_params = 0, ItemExpr *p1 = 0, ItemExpr *p2 = 0, ItemExpr *p3 = 0, ItemExpr *p4 = 0,
                           ItemExpr *p5 = 0, ItemExpr *p6 = 0)

  {
    Parser parser(generator->currentCmpContext());
    if (!CharInfo::isSingleByteCharSet(charSet)) {
      return parser.get_w_ItemExprTree((const NAWchar *)str, strlength, num_params, p1, p2, p3, p4, p5, p6);
    } else
      return parser.getItemExprTree(str, strlength, charSet, num_params, p1, p2, p3, p4, p5, p6);
  }

  // this function returns an expr tree that multiplies the source
  // by 10 ** exponent.  If exponent is negative, the returned expr
  // tree divides the source by 10 ** (- exponent).
  ItemExpr *scaleBy10x(const ValueId &source, int exponent);

  // if the scales of the source and target types are not
  // the same, this function returns an expr tree to upscale
  // or downscale the source to the target scale.
  ItemExpr *matchScales(const ValueId &source, const NAType &targetType);

  // This is like matchScales(), except it does not add a Cast of the
  // expr tree to the target datatype.  Useful in key building, where
  // we wish to handle truncation and overflow errors due to datatype
  // conversion specially.
  ItemExpr *matchScalesNoCast(const ValueId &source, const NAType &targetType);

  // if Cast from/to is not supported, convert to supported datatype.
  short handleUnsupportedCast(Cast *castNode);

  // if the source is an interval, this function returns an expr
  // tree to convert the source type to numeric.
  ItemExpr *convertIntervalToNumeric(const ValueId &source);

  // if the source is a numeric, this function returns an expr
  // tree to convert the source type to interval.
  ItemExpr *convertNumericToInterval(const ValueId &source, const NAType &targetType);

  // if the source and target are intervals with different end fields,
  // this function returns an expr tree to convert the source type to
  // match the target's end field.
  ItemExpr *matchIntervalEndFields(const ValueId &source, const NAType &targetType);

  // maptable allocated at the start of generating an expression.
  inline MapTable *getExprMapTable() { return mapTable_; };

  inline int getConstLength() { return constants_length; }
  inline void addConstLength(int length) { constants_length += length; }

  MapInfo *addTemporary(ValueId val, MapTable *mapTable);
  //
  // Routines to get/set the current ROWS SINCE counter.
  //
  inline ItemExpr *getRowsSinceCounter() { return rowsSinceCounter_; }
  inline void setRowsSinceCounter(ItemExpr *newCounter) { rowsSinceCounter_ = newCounter; }

  inline long getTempsLength() { return temps_length; }
  inline void addTempsLength(long length) { temps_length += length; }

  void setFixupConstsAndTemps(int v) {
    if (v != 0)
      flags_ |= FIXUP_CONSTS_AND_TEMPS;
    else
      flags_ &= ~FIXUP_CONSTS_AND_TEMPS;
  };

  int getFixupConstsAndTemps() { return flags_ & FIXUP_CONSTS_AND_TEMPS; }

  void setShowplan(int v) {
    if (v != 0)
      flags_ |= SHOWPLAN_;
    else
      flags_ &= ~SHOWPLAN_;
  };

  int getShowplan() { return flags_ & SHOWPLAN_; }

  void setNoTransaction(int v) {
    if (v != 0)
      flags_ |= NOTRANSACTION_;
    else
      flags_ &= ~NOTRANSACTION_;
  };

  int getNoTransaction() { return flags_ & NOTRANSACTION_; }

  void setClauseLinked(NABoolean v) {
    if (v)
      flags_ |= CLAUSE_LINKED_;
    else
      flags_ &= ~CLAUSE_LINKED_;
  };

  NABoolean clauseLinked() { return (flags_ & CLAUSE_LINKED_) != 0; }

  void setForInsertUpdate(NABoolean v) {
    if (v)
      flags_ |= FOR_INSERT_UPDATE_;
    else
      flags_ &= ~FOR_INSERT_UPDATE_;
  };

  NABoolean forInsertUpdate() { return (flags_ & FOR_INSERT_UPDATE_) != 0; }

  // In specific situations no header clause is needed when writing (or updating)
  // a disk format row - optimized updates.
  void setNoHeaderNeeded(NABoolean v) {
    if (v)
      flags_ |= GEN_NO_HDR_;
    else
      flags_ &= ~GEN_NO_HDR_;
  };

  // A header clause is only needed when writing one of the 2 SQL/MX disk
  // formats & the flag is not set saying no header clause is needed.
  NABoolean isHeaderNeeded(ExpTupleDesc::TupleDataFormat tdf) {
    return (ExpTupleDesc::isDiskFormat(tdf) && ((flags_ & GEN_NO_HDR_) == 0));
  }

  void setInContainerExpr(NABoolean v) {
    if (v)
      flags_ |= IN_CONTAINER_EXPR_;
    else
      flags_ &= ~IN_CONTAINER_EXPR_;
  };

  NABoolean inContainerExpr() { return (flags_ & IN_CONTAINER_EXPR_) != 0; }

  void setHandleIndirectVC(NABoolean v) {
    if (v)
      flags_ |= HANDLE_INDIRECT_VC_;
    else
      flags_ &= ~HANDLE_INDIRECT_VC_;
  };

  NABoolean handleIndirectVC() { return (flags_ & HANDLE_INDIRECT_VC_) != 0; }

  void setCaseStmtGenerated(NABoolean v) {
    if (v)
      flags_ |= CASE_STMT_GENERATED_;
    else
      flags_ &= ~CASE_STMT_GENERATED_;
  };

  NABoolean caseStmtGenerated() { return (flags_ & CASE_STMT_GENERATED_) != 0; }

  void setSaveClausesInExpr(NABoolean value) {
    if (value)
      flags_ |= SAVE_CLAUSES_IN_EXPR;
    else
      flags_ &= ~SAVE_CLAUSES_IN_EXPR;
  };

  NABoolean saveClausesInExpr() { return ((flags_ & SAVE_CLAUSES_IN_EXPR) != 0); };

  NABoolean forceLinkConstant() { return (flags_ & FORCE_LINK_CONST) != 0; }
  void setForceLinkConstant(NABoolean v) { (v ? flags_ |= FORCE_LINK_CONST : flags_ &= ~FORCE_LINK_CONST); }

  NABoolean inSequenceFuncExpr() { return (flags_ & IN_SEQUENCE_FUNC_EXPR) != 0; }
  void setInSequenceFuncExpr(NABoolean v) { (v ? flags_ |= IN_SEQUENCE_FUNC_EXPR : flags_ &= ~IN_SEQUENCE_FUNC_EXPR); }

  // The working heap for dynamic memory allocation, will be
  // be destroyed at the end of each statement.

  inline CollHeap *wHeap() { return ((generator) ? generator->wHeap() : 0); }

  // for generating key information (e.g. single subset, Mdam)
  short buildKeyInfo(keyRangeGen **keyInfo,  // out -- generated object
                     Generator *generator, const NAColumnArray &keyColumns, const ValueIdList &listOfKeyColumns,
                     const ValueIdList &beginKeyPred, const ValueIdList &endKeyPred, const SearchKey *searchKey,
                     const MdamKey *mdamKey, const NABoolean reverseScan, const ExpTupleDesc::TupleDataFormat tf);
};

// The next class is a helper used in the mdamCodeGen methods on
// ItemExprs.  It helps to hide some classes from ItemExpr.h that
// we would rather not teach it about, but that the mdamCodeGen
// methods need to know about (hence the fact that all members have
// public accessors).

class MdamCodeGenHelper {
 private:
  // the current disjunct number
  CollIndex disjunctNumber_;

  // the datatype of the key column in the key buffer
  NAType *targetType_;

  // TRUE if key is DESC, FALSE otherwise
  NABoolean isDescending_;

  // the atp number and index used for the result of the key expression
  int atp_;
  int atpIndex_;

  // the format of the key buffer
  ExpTupleDesc::TupleDataFormat tdataF_;

  // the data conversion error flag, used for run-time modification
  // of the Mdam predicate in the event of a data conversion error
  ItemExpr *dataConversionErrorFlag_;

  // the value ID of the key column that we are generating Mdam predicates
  // for -- this helps us figure out which side of a comparison operator
  // is the key column and which is the key value
  ValueId keyColumn_;

 public:
  MdamCodeGenHelper(CollIndex disjunctNumber, NAType *targetType, NABoolean isDescending, int atp, int atpIndex,
                    ExpTupleDesc::TupleDataFormat tdataF, ItemExpr *dataConversionErrorFlag, ValueId keyColumn)
      : disjunctNumber_(disjunctNumber),
        targetType_(targetType),
        isDescending_(isDescending),
        atp_(atp),
        atpIndex_(atpIndex),
        tdataF_(tdataF),
        dataConversionErrorFlag_(dataConversionErrorFlag),
        keyColumn_(keyColumn){};

  ~MdamCodeGenHelper(){};

  // accessor methods

  CollIndex getDisjunctNumber() { return disjunctNumber_; };

  NAType *getTargetType() { return targetType_; };

  NABoolean isDescending() { return isDescending_; };

  int getAtp() { return atp_; };

  int getAtpIndex() { return atpIndex_; };

  ExpTupleDesc::TupleDataFormat getTupleDataFormat() { return tdataF_; };

  ItemExpr *getDataConversionErrorFlag() { return dataConversionErrorFlag_; };

  ValueId getKeyColumn() { return keyColumn_; };

  // mutator method for atpIndex_, needed for processing 2ndary key value in
  // case of MDAM_BETWEEN predtype.
  void setAtpIndex(int atpInx) { atpIndex_ = atpInx; }
};

#endif
