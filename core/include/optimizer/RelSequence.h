
#ifndef RELSEQUENCE_H
#define RELSEQUENCE_H
/* -*-C++-*-
******************************************************************************
*
* File:         RelSequence.h
* Description:  Class definitions for the classes RelSequence
*               and PhysSequence
* Created:      6/17/97
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "optimizer/CostMethod.h"
#include "optimizer/RelExpr.h"

// forward references

class Generator;
class MapTable;
class Attributes;

// Class RelSequence
// --------------------------------------------------------

// The RelSequence RelExpr is the logical RelExpr used to implement the
// "sequence by clause" and is introduced by the parser.  The physical
// version of this node is the RelExpr class PhysSequence.  The data
// members of this class are:
//
// ItemExpr *requiredOrderTree_ : An ItemExpr list of columns
// representing the reqired sort order for this node.  A required sort
// order is specified withg th sort by clause.
//
// ValueIdList requiredOrder_ : The bound version of
// requiredOrderTree_.
//
// ValueIdSet sequenceFunctions_ : A set of all the sequence
// functions associated with this RelSequence node.  They are gathered
// and assigned to the RelSequence node during binding.
//
class RelSequence : public RelExpr {
 public:
  // The constructor
  //
  RelSequence(RelExpr *child = NULL, ItemExpr *requiredOrder = NULL, CollHeap *oHeap = CmpCommon::statementHeap());

  RelSequence(RelExpr *child, ItemExpr *partitionBy, ItemExpr *orderBy, CollHeap *oHeap = CmpCommon::statementHeap());

  // The destructor
  //
  virtual ~RelSequence();

  // RelSequence has one child.
  //
  virtual int getArity() const { return 1; };

  //++MV
  void addRequiredOrderTree(ItemExpr *orderExpr);

  // a virtual function for performing name binding within the query tree
  //
  RelExpr *bindNode(BindWA *bindWAPtr);

  // Each operator supports a (virtual) method for transforming its
  // scalar expressions to a canonical form
  //
  virtual void transformNode(NormWA &normWARef, ExprGroupId &locationOfPointerToMe);

  // a method used during subquery transformation for pulling up predicates
  // towards the root of the transformed subquery tree
  //
  // RelSequence cannot pull up any preds. so redefine this method.
  //
  virtual void pullUpPreds();

  // a method used for recomputing the outer references (external dataflow
  // input values) that are still referenced by each operator in the
  // subquery tree after the predicate pull up is complete.
  //
  virtual void recomputeOuterReferences();

  // Each operator supports a (virtual) method for rewriting its
  // value expressions.
  //
  virtual void rewriteNode(NormWA &normWARef);

  // Each operator supports a (virtual) method for performing
  // predicate pushdown and computing a "minimal" set of
  // characteristic input and characteristic output values.
  //
  virtual RelExpr *normalizeNode(NormWA &normWARef);

  // Method to push down predicates from a RelSequence node into the
  // children
  //
  virtual void pushdownCoveredExpr(const ValueIdSet &outputExprOnOperator, const ValueIdSet &newExternalInputs,
                                   ValueIdSet &predOnOperator, const ValueIdSet *nonPredExprOnOperator = NULL,
                                   int childId = (-MAX_REL_ARITY));

  // Return a the set of potential output values of this node.
  // For RelSequence, this is the output of the child, plus all
  // the values generated by RelSequence
  //
  virtual void getPotentialOutputValues(ValueIdSet &vs) const;

  // add all the expressions that are local to this
  // node to an existing list of expressions (used by GUI tool and Explain)
  //
  virtual void addLocalExpr(LIST(ExprNode *) & xlist, LIST(NAString) & llist) const;

  // Compute a hash value for a chain of derived RelExpr nodes.
  // Used by the Cascade engine as a quick way to determine if
  // two nodes are identical.
  // Can produce false positives, but should not produce false
  // negatives.
  //
  virtual HashValue topHash();

  // A more thorough method to compare two RelExpr nodes.
  // Used by the Cascades engine.
  //
  virtual NABoolean duplicateMatch(const RelExpr &other) const;

  // Copy a chain of derived nodes (Calls RelExpr::copyTopNode).
  // Needs to copy all relevant fields.
  // Used by the Cascades engine.
  //
  virtual RelExpr *copyTopNode(RelExpr *derivedNode = NULL, CollHeap *outHeap = NULL);

  // synthesize logical properties
  //
  virtual void synthLogProp(NormWA *normWAPtr = NULL);
  virtual void synthEstLogProp(const EstLogPropSharedPtr &inputEstLogProp);

  virtual Context *createContextForAChild(Context *myContext, PlanWorkSpace *pws, int &childIndex);

  // get a printable string that identifies the operator
  //
  virtual const NAString getText() const { return "Sequence"; };

  inline void setCancelExprTree(ItemExpr *expr) { cancelExprTree_ = expr; }

  ValueIdList mapSortKey(const ValueIdList &sortKey) const;

  // Accessor methods
  inline const ValueIdList &requiredOrder() const { return requiredOrder_; }

  inline const ValueIdList &partition() const { return partition_; }

  inline const ValueIdSet &sequenceFunctions() const { return sequenceFunctions_; }

  inline const ValueIdSet &returnSeqFunctions() const { return returnSeqFunctions_; }
  inline const ValueIdSet &movePartIdsExpr() const { return movePartIdsExpr_; }
  inline const ValueIdSet &sequencedColumns() const { return sequencedColumns_; }

  inline void addSequencedColumn(ValueId col) { sequencedColumns_ += col; }

  inline const ValueIdList &cancelExpr() const { return cancelExpr_; }

  // Add unresolved sequence functions to the RelSequence operator
  // found when binding the select list.  Also, if OLAP, bind the
  // partition by and order by clauses and add to RelSequence.
  //
  void addUnResolvedSeqFunctions(ValueIdSet &unresolvedSeqFuncs, BindWA *bindWA);

  inline void setRequiredOrder(const ValueIdList &reqOrder) { requiredOrder_ = reqOrder; }

  inline void setPartition(const ValueIdList &partition) { partition_ = partition; }

  ValueIdList getPartitionChange() { return partitionChange_; }

  ItemExpr *getPartitionBy() { return partitionBy_; }

  void setPartitionBy(ItemExpr *part) { partitionBy_ = part; }

  inline void setSequenceFunctions(const ValueIdSet &seqFuncs) { sequenceFunctions_ = seqFuncs; }

  inline void setSequencedColumns(const ValueIdSet &seqCols) { sequencedColumns_ = seqCols; }

  inline void setCancelExpr(const ValueIdList &expr) { cancelExpr_ = expr; }

  inline void setHasOlapFunctions(const NABoolean v) { hasOlapFunctions_ = v; }
  inline const NABoolean getHasOlapFunctions() const { return hasOlapFunctions_; }

  inline void setHasTDFunctions(const NABoolean v) { hasTDFunctions_ = v; }
  inline const NABoolean getHasTDFunctions() const { return hasTDFunctions_; }

  ItemExpr *getRequiredOrderTree() { return requiredOrderTree_; }

  int getCachedNumHistoryRows() { return cachedNumHistoryRows_; }
  void setCachedNumHistoryRows(int v) { cachedNumHistoryRows_ = v; }

  NABoolean getCachedUnboundedFollowing() { return cachedUnboundedFollowing_; }
  void setCachedUnboundedFollowing(NABoolean v) { cachedUnboundedFollowing_ = v; }

  int getCachedMinFollowingRows() { return cachedMinFollowingRows_; }
  void setCachedMinFollowingRows(int v) { cachedMinFollowingRows_ = v; }

  int getCachedHistoryRowLength() { return cachedHistoryRowLength_; }
  void setCachedHistoryRowLength(int v) { cachedHistoryRowLength_ = v; }

  NABoolean getHistoryInfoCached() { return historyInfoCached_; }
  void setHistoryInfoCached(NABoolean v) { historyInfoCached_ = v; }

  ItemExpr *buildDefaultOrderByForRownum(BindWA *bindWA);

  virtual void generateCacheKey(CacheWA &cwa) const;

 protected:
  inline ValueIdList &requiredOrder() { return requiredOrder_; }

  inline ValueIdList &partition() { return partition_; }

  inline ValueIdSet &sequenceFunctions() { return sequenceFunctions_; }

  inline ValueIdSet &readSeqFunctions() { return sequenceFunctions_; }
  inline ValueIdSet &returnSeqFunctions() { return returnSeqFunctions_; }
  inline ValueIdSet &sequencedColumns() { return sequencedColumns_; }

  inline ValueIdList &cancelExpr() { return cancelExpr_; }

  void addCancelExpr(CollHeap *wHeap);

  inline ValueIdSet &checkPartitionChangeExpr() { return checkPartitionChangeExpr_; }
  inline ValueIdSet &movePartIdsExpr() { return movePartIdsExpr_; }

 private:
  // Return a pointer to the required order tree after
  // setting it to NULL.
  //
  ItemExpr *removeRequiredOrderTree();

  // An ItemExpr list of columns representing the reqired sort order
  // for this node.
  //
  ItemExpr *requiredOrderTree_;

  // The ItemExpr list of columns representing the partition by clause.
  //
  ItemExpr *partitionBy_;

  // -- MV
  // During execution, when the cancelExpr evaluates to TRUE, the Sequence
  // node cancels the parent request.
  ItemExpr *cancelExprTree_;

  // The bound version of requiredOrderTree_.
  //
  ValueIdList requiredOrder_;

  // The bound version of partitionByTree_
  //
  ValueIdList partition_;

  ValueIdList partitionChange_;
  // A set of all the sequence functions associated with this
  // RelSequence node.  They are gathered and assigned to the
  // RelSequence node during binding.
  //
  ValueIdSet sequenceFunctions_;

  ValueIdSet returnSeqFunctions_;
  ValueIdSet sequencedColumns_;

  // -- MV
  ValueIdList cancelExpr_;

  // flag that defines whether this RelSequence was created for TD functions
  // or not
  NABoolean hasTDFunctions_;
  NABoolean hasOlapFunctions_;

  ValueIdSet checkPartitionChangeExpr_;
  ValueIdSet movePartIdsExpr_;

  // cached parameters
  NABoolean historyInfoCached_;
  int cachedNumHistoryRows_;
  NABoolean cachedUnboundedFollowing_;
  int cachedMinFollowingRows_;
  int cachedHistoryRowLength_;

};  // class RelSequence

// Class PhysSequence ----------------------------------------------------
// The PhysSequence node replaces the logical RelSequence node through the
// application of the PhysSequenceRule. This transformation is
// designed to present a purely physical verison of this operator
// that is both a logical and physical node. The PhysSequence node
// does not add any data members. It adds a few virtual methods.
// -----------------------------------------------------------------------
class PhysSequence : public RelSequence {
 public:
  // The constructor
  //
  PhysSequence(RelExpr *child = NULL, CollHeap *oHeap = CmpCommon::statementHeap()) : RelSequence(child, NULL, oHeap) {
    numHistoryRows_ = getDefault(DEF_MAX_HISTORY_ROWS);
    minFixedHistoryRows_ = 0;
    unboundedFollowing_ = FALSE;
    minFollowingRows_ = 0;
    if (oHeap)
      pCostMethod_ = new (oHeap) CostMethodRelSequence();
    else
      pCostMethod_ = new (CmpCommon::statementHeap()) CostMethodRelSequence();
  };

  // The destructor.
  //
  virtual ~PhysSequence();

  // methods to do code generation of the physical node.
  //
  virtual RelExpr *preCodeGen(Generator *generator, const ValueIdSet &externalInputs, ValueIdSet &pulledNewInputs);
  virtual short codeGen(Generator *);

  // generate CONTROL QUERY SHAPE fragment for this node.
  //
  virtual short generateShape(CollHeap *space, char *buf, NAString *shapeStr = NULL);

  // Copy a chain of derived nodes (Calls RelSequence::copyTopNode).
  // Needs to copy all relevant fields (in this case no fields
  // need to be copied)
  // Used by the Cascades engine.
  //
  virtual RelExpr *copyTopNode(RelExpr *derivedNode = NULL, CollHeap *outHeap = NULL);

  // cost functions
  //
  virtual PhysicalProperty *synthPhysicalProperty(const Context *context, const int planNumber, PlanWorkSpace *pws);
  virtual CostMethod *costMethod() const;

  // The method gets redefined since Sequnece may be a BMO depending
  // on its inputs.
  virtual NABoolean isBigMemoryOperator(const Context *context, const int planNumber);

  virtual CostScalar getEstimatedRunTimeMemoryUsage(Generator *generator, NABoolean perNode, int *numStreams = NULL);

  // Redefine these virtual methods to declare this node as a
  // physical node.
  //
  virtual NABoolean isLogical() const { return FALSE; };
  virtual NABoolean isPhysical() const { return TRUE; };

  ExplainTuple *addSpecificExplainInfo(ExplainTupleMaster *explainTuple, ComTdb *tdb, Generator *generator);

  // Helper function that traverses the set of root sequence functions
  // supplied by the compiler and constructs the set of all of the
  // attributes that must be materialized in the history row.
  //
  void getHistoryAttributes(const ValueIdSet &sequenceFunctions, const ValueIdSet &outputFromChild,
                            ValueIdSet &historyAttributes, NABoolean addConvNodes = FALSE, CollHeap *wHeap = NULL,
                            ValueIdMap *origAttributes = NULL) const;

  // A dedicated helper function that processes the offset and
  // default value expression for LEAD SQL function. On return
  // readFunctions, returnFunctions and historyAttributes (historyIds)
  // can be updated.
  void processHistoryAttributesForLead(ValueIdSet &readFunctions, ValueIdSet &returnFunctions,
                                       ValueIdSet &historyAttributes, CollHeap *wHeap);

  // Helper function to compute the attribute for the history buffer
  // based on the items projected from the child and the computed
  // history items.  Also, adds the attribute information the the map
  // table.
  //
  void computeHistoryAttributes(Generator *generator, MapTable *localMapTable, Attributes **attrs,
                                const ValueIdSet &historyIds) const;

  // Helper function that traverses the set of root sequence functions
  // supplied by the compiler and dynamically determines the size
  // of the history buffer.
  //
  void computeHistoryRows(const ValueIdSet &sequenceFunctions, int &computedHistoryRows, int &unableToCalculate,
                          // long &minFixedHistoryRows,
                          NABoolean &unboundedFollowing,  //&unlimitedHistoryRows,
                          int &minFollowingRows, const ValueIdSet &outputFromChild);

  // Return a READ-ONLY reference to the number of history rows to be
  // allocated in the history buffer.
  //
  inline const int &numHistoryRows() const { return numHistoryRows_; }

  // Set the number of history rows to be allowed in the history buffer.
  void setNumHistoryRows(int numHistoryRows);

  void setUnboundedFollowing(NABoolean v);
  NABoolean getUnboundedFollowing() const;

  void setMinFollowingRows(int v);

  // set the min following rows to max(v, minFollowingRows_);
  void computeAndSetMinFollowingRows(int v);

  int getMinFollowingRows() const;

  int getEstHistoryRowLength() const { return estHistoryRowLength_; }

  void setEstHistoryRowLength(int v) { estHistoryRowLength_ = v; }

  void updateEstHistoryRowLength(ValueIdSet &histIds, ValueId newValId, int &estimatedLength) {
    if (!histIds.contains(newValId)) {
      estimatedLength += newValId.getType().getTotalSize();
      histIds += newValId;
    }
  }

  void seperateReadAndReturnItems(CollHeap *wHeap);

  void computeReadNReturnItems(ValueId seqVid, ValueId vid, const ValueIdSet &outputFromChild, CollHeap *wHeap);

  void transformOlapFunctions(CollHeap *wHeap);

  void computeHistoryParams(int histRecLength, int &maxRowsInOLAPBuffer, int &minNumberOfOLAPBuffers,
                            int &numberOfWinOLAPBuffers, int &maxNumberOfOLAPBuffers, int &olapBufferSize);

  void retrieveCachedHistoryInfo(RelSequence *cacheRelSeq);

  void estimateHistoryRowLength(const ValueIdSet &sequenceFunctions, const ValueIdSet &outputFromChild,
                                ValueIdSet &histIds, int &estimatedLength);

  void addCheckPartitionChangeExpr(Generator *generator, NABoolean addConvNodes = FALSE,
                                   ValueIdMap *origAttributes = NULL);

 private:
  // The number of history rows to allocate for the history buffer.
  // Is initialized to the value of the default REF_MAX_HISTORY_ROWS.
  // Later it may be reduced if it can be determined that fewer rows
  // can safely be used.
  //
  int numHistoryRows_;

  // minimum number of rows that need to fit in the history buffer
  // unbounded to Bounded and bounded to bounded cases
  // if there is an unbounded following we still need to detremine the size for above cases
  int minFixedHistoryRows_;

  // do need to have unlimited size-- unbounded following cases
  NABoolean unboundedFollowing_;  // unlimitedHistoryRows_;

  int minFollowingRows_;

  // estimated history row length
  int estHistoryRowLength_;

 private:
  CostMethodRelSequence *pCostMethod_;
};  // class PhysSequence

#endif
