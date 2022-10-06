
#ifndef ELEMDDLSGOPTIONS_H
#define ELEMDDLSGOPTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLSGOptions.h
 * Description:  classes for sequence generator options specified in DDL statements
 *
 * Created:      4/22/08
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "common/BaseTypes.h"
#include "common/SequenceGeneratorAttributes.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLSGOptions;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// This enum has a similar one, ComSequenceGeneratorType, in
// common/ComSmallDefs.h. Should keep them in sync.
enum SG_TYPE { SG_UNKNOWN = 0, SG_INTERNAL, SG_EXTERNAL, SG_INTERNAL_COMPUTED, SG_SYSTEM };

enum CD_TYPE { CD_UNKNOWN = 0, CD_GENERATED_BY_DEFAULT, CD_GENERATED_ALWAYS };

enum { INDEX_SG_OPT_LIST = 0, MAX_ELEM_DDL_SG_OPTS_ARITY };

enum SEQUENCE_ORDER { ORDER_NOT_SPECIFIED = 0, ORDER, NOORDER };

// -----------------------------------------------------------------------
// definition of base class ElemDDLSGOptions
// -----------------------------------------------------------------------
class ElemDDLSGOptions : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLSGOptions();

  ElemDDLSGOptions(OperatorTypeEnum operType);

  ElemDDLSGOptions(int operType, ElemDDLNode *pSGOptList);

  // virtual destructor
  virtual ~ElemDDLSGOptions();

  // cast
  virtual ElemDDLSGOptions *castToElemDDLSGOptions();

  // method for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

  // Accessors

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline long getStartValue() const { return startValue_; }
  inline long getIncrement() const { return increment_; }
  inline long getMinValue() const { return minValue_; }
  inline long getMaxValue() const { return maxValue_; }

  inline SEQUENCE_ORDER getOrder() const { return seq_order_; }

  inline NABoolean getCycle() const { return cycle_; }
  inline long getCache() const { return cache_; }

  inline SG_TYPE getSGType() const { return sgType_; }

  inline CD_TYPE getCDType() const { return cdType_; }

  inline CollIndex getNumberOfOptions() const { return numOptions_; };

  inline NABoolean isStartValueSpecified() const { return isStartValueSpec_; }
  inline NABoolean isRestartValueSpecified() const { return isRestartValueSpec_; }
  inline NABoolean isIncrementSpecified() const { return isIncrementSpec_; }
  inline NABoolean isMinValueSpecified() const { return isMinValueSpec_; }
  inline NABoolean isMaxValueSpecified() const { return isMaxValueSpec_; }
  inline NABoolean isCycleSpecified() const { return isCycleSpec_; }
  inline NABoolean isCacheSpecified() const { return isCacheSpec_; }
  inline NABoolean isResetSpecified() const { return isResetSpec_; }
  inline NABoolean isOrderSpecified() const { return isOrderSpec_; }
  inline NABoolean isSystemSpecified() const { return isSystemSpec_; }
  inline NABoolean isReplSpecified() const { return isReplSpec_; }
  inline NABoolean isNextValSpecified() const { return isNextValSpec_; }
  inline NABoolean isNoMinValue() const { return isNoMinValue_; }
  inline NABoolean isNoMaxValue() const { return isNoMaxValue_; }
  inline NABoolean isCycle() const { return cycle_ == TRUE; }
  inline NABoolean isNoCycle() const { return cycle_ == FALSE; }
  inline NABoolean isCache() const { return cache_ > 0; }
  inline NABoolean isNoCache() const { return isNoCache_; }
  inline NABoolean isReset() const { return reset_; }
  inline NABoolean isOrder() const { return seq_order_ == ORDER; }
  inline NABoolean isInternalSG() const { return sgType_ == SG_INTERNAL; }
  inline NABoolean isExternalSG() const { return sgType_ == SG_EXTERNAL; }
  inline NABoolean isUnknownSG() const { return sgType_ == SG_UNKNOWN; }
  inline NABoolean isSystemSG() const { return sgType_ == SG_SYSTEM; }
  inline long getGlobalTimeoutVal() const { return globalTimeoutVal_; }
  inline ComReplType getReplType() const { return replType_; }
  inline NABoolean isGeneratedByDefault() const { return cdType_ == CD_GENERATED_BY_DEFAULT; }
  inline NABoolean isGeneratedAlways() const { return cdType_ == CD_GENERATED_ALWAYS; }
  inline NABoolean isUnknownCD() const { return cdType_ == CD_UNKNOWN; }

  // Mutators
  virtual void setChild(int index, ExprNode *pChildNode);

  inline void setStartValue(long startValue) { startValue_ = startValue; }
  inline void setIncrement(long increment) { increment_ = increment; }
  inline void setMinValue(long minValue) { minValue_ = minValue; }
  inline void setMaxValue(long maxValue) { maxValue_ = maxValue; }

  inline void setSGType(SG_TYPE sgType) { sgType_ = sgType; }
  inline void setCDType(CD_TYPE cdType) { cdType_ = cdType; }
  void setCDType(int cdType);
  inline void setNextVal(long val) { nextVal_ = val; }

  inline void setStartValueSpec(NABoolean startValue) { isStartValueSpec_ = startValue; }
  inline void setRestartValueSpec(NABoolean startValue) { isRestartValueSpec_ = startValue; }
  inline void setIncrementSpec(NABoolean increment) { isIncrementSpec_ = increment; }
  inline void setMinValueSpec(NABoolean minValue) { isMinValueSpec_ = minValue; }
  inline void setMaxValueSpec(NABoolean maxValue) { isMaxValueSpec_ = maxValue; }
  inline void setCycleSpec(NABoolean cycle) { isCycleSpec_ = cycle; }
  inline void setCacheSpec(NABoolean cache) { isCacheSpec_ = cache; }
  inline void setResetSpec() { isResetSpec_ = TRUE; }
  inline void setOrderSpec(NABoolean order) { isOrderSpec_ = order; }

  inline void setSystemSpec(NABoolean TorF) { isSystemSpec_ = TorF; }
  inline void setGlobalTimeoutVal(long timeout) { globalTimeoutVal_ = timeout; }
  inline void setReplSpec(NABoolean rs) { isReplSpec_ = rs; }
  inline void setReplType(ComReplType v) { replType_ = v; }
  inline void setNoMinValue(NABoolean minValue) { isNoMinValue_ = minValue; }
  inline void setNoMaxValue(NABoolean maxValue) { isNoMaxValue_ = maxValue; }
  inline void setCycle(NABoolean cycle) { cycle_ = cycle; }
  inline void setCache(long cache) { cache_ = cache; }
  inline void setReset(NABoolean reset) { reset_ = reset; }
  inline void setOrder(SEQUENCE_ORDER seq_order) { seq_order_ = seq_order; }
  inline void setNextValSpec(NABoolean nextValSpec) { isNextValSpec_ = nextValSpec; }

  ComFSDataType getFSDataType() { return fsDataType_; }
  void setFSDataType(ComFSDataType dt) { fsDataType_ = dt; }

  inline long getNextVal() const { return nextVal_; }

  //
  // method for binding
  //

  virtual ExprNode *bindNode(BindWA *pBindWA);

  // queryType:  0, create sequence.  1, alter sequence.  2, IDENTITY col.
  short validate(short queryType);

  short genSGA(SequenceGeneratorAttributes &sga);

  short importSGA(const SequenceGeneratorAttributes *sga);
  short importSGO(const ElemDDLSGOptions *sgo);
  void recomputeMaxValue(ComFSDataType datatype);

  //
  // pointer to child parse nodes
  //

  ElemDDLNode *children_[MAX_ELEM_DDL_SG_OPTS_ARITY];

  //
  // Method for tracing
  //

  NATraceList getDetailInfo() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  // accessor
  inline ElemDDLNode *getSGOptList() const;

  // mutators
  void initializeDataMembers();
  void setSGOpt(ElemDDLNode *pOptNode);

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  // StartValue RestartValue
  NABoolean isStartValueSpec_;
  NABoolean isRestartValueSpec_;
  long startValue_;

  // Increment
  NABoolean isIncrementSpec_;
  long increment_;

  // MinValue
  NABoolean isMinValueSpec_;
  NABoolean isNoMinValue_;
  long minValue_;

  // MaxValue
  NABoolean isMaxValueSpec_;
  NABoolean isNoMaxValue_;
  long maxValue_;

  // Cycle
  NABoolean isCycleSpec_;
  NABoolean cycle_;

  // Cache
  NABoolean isCacheSpec_;
  NABoolean isNoCache_;
  long cache_;

  // Order
  NABoolean isOrderSpec_;
  SEQUENCE_ORDER seq_order_;
  // Datatype
  NABoolean isDatatypeSpec_;
  ComFSDataType fsDataType_;

  // Reset
  NABoolean isResetSpec_;
  NABoolean reset_;

  NABoolean isSystemSpec_;
  // Internal or External or System  SG
  SG_TYPE sgType_;
  long globalTimeoutVal_;

  NABoolean isReplSpec_;
  ComReplType replType_;

  // COLUMN Default Type
  CD_TYPE cdType_;

  // Number of options in list
  CollIndex numOptions_;

  // nextVal
  NABoolean isNextValSpec_;
  long nextVal_;
};  // class ElemDDLSGOptions
#endif
