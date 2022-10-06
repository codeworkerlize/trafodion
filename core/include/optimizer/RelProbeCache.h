// **********************************************************************

// **********************************************************************
//
#ifndef RELPROBECACHE_H
#define RELPROBECACHE_H
// -----------------------------------------------------------------------
// ProbeCache is a physical operator inserted by NestedJoin between
// itself and its right child during preCodeGen.  Its characteristic
// outputs are the same as its child.  Its characteristic inputs are
// the same as its child with the addition of an ITM_EXEC_COUNT
// so that it can invalidate the probe cache between statement
// executions.  The ProbeCache operator has no item expressions
// until codeGen, and these are just temporary, used to create
// runtime ex_expr objects.
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------

// The following are physical operators
class ProbeCache;

// -----------------------------------------------------------------------
// The ProbeCache physical operator
// -----------------------------------------------------------------------

class ProbeCache : public RelExpr {
 public:
  // Constructor
  ProbeCache(RelExpr *child, int numCachedProbes, CollHeap *oHeap = CmpCommon::statementHeap())
      : RelExpr(REL_PROBE_CACHE, child,
                NULL,  // no right child.
                oHeap),
        numCachedProbes_(numCachedProbes),
        numInnerTuples_(0)  // set in the generator for now.
  {}

  virtual NABoolean isLogical() const;
  virtual NABoolean isPhysical() const;

  virtual int getArity() const;
  virtual RelExpr *copyTopNode(RelExpr *derivedNode = NULL, CollHeap *outHeap = 0);

  virtual RelExpr *preCodeGen(Generator *generator, const ValueIdSet &externalInputs, ValueIdSet &pulledNewInputs);

  virtual short codeGen(Generator *g);

  virtual CostScalar getEstimatedRunTimeMemoryUsage(Generator *generator, NABoolean perNode, int *numStreams = NULL);
  virtual double getEstimatedRunTimeMemoryUsage(Generator *generator, ComTdb *tdb);

  virtual const NAString getText() const;

  ExplainTuple *addSpecificExplainInfo(ExplainTupleMaster *explainTuple, ComTdb *tdb, Generator *generator);

 private:
  // Currently, both numCachedProbes_ and numInnerTuples_  are set in the generator
  // but in the future, the optimizer may set these, or at least supply
  // suggestions or initial settings.  That is why they are members of this
  // class.
  int numCachedProbes_;
  int numInnerTuples_;

};  // ProbeCache

#endif /* RELPROBECACHE_H */
