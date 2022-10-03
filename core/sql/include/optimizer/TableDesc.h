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
**********************************************************************/
#ifndef TABLEDESC_H
#define TABLEDESC_H
/* -*-C++-*-
**************************************************************************
*
* File:         TableDesc.h
* Description:  A table descriptor
* Created:      4/27/94
* Language:     C++
*
*
*
*
**************************************************************************
*/


#include "common/BaseTypes.h"
#include "optimizer/ObjectNames.h"
#include "ColStatDesc.h"
#include "IndexDesc.h"
#include "optimizer/ItemConstr.h"
#include "optimizer/ValueDesc.h"
#include "OptRange.h"
#include "optimizer/ItemColRef.h"


// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class TableDesc;
class TableDescList;
class SelectivityHint;
class CardinalityHint;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class BindWA;
class NATable;
class TableAnalysis;
class ItemList;

// ***********************************************************************
// TableDesc
//
// One TableDesc is allocated per reference to a qualified table name.
// One or more TableDescs may share a NATable. A TableDesc contains
// some attributes for the table that are specific to a particular
// reference, e.g., the lock mode or CONTROLs that are in effect.
//
// ***********************************************************************

class BoundaryExpr : public NABasicObject
{
public:
  BoundaryExpr()
                   :lowBound_(NULL)
                   ,highBound_(NULL)
  {}
  ItemExpr* lowBound_;    // lowValue is only used for the first 
                          // partition column of Range partition
  ItemExpr* highBound_;
};

class PartRangePerCol : public NABasicObject
{
public:
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  PartRangePerCol(
    NAMemory *heap,
    const Int32 partIdx,
    const Int32 partColPos) 
   :heap_(heap),
    partIdx_(partIdx),
    partColPos_(partColPos),
    intervalRange_(NULL),
    highBoundRange_(NULL),
    colName_(NULL),
    boundary_(NULL),
    lowBoundaryExpr_(NULL),
    intervalExpr_(NULL),
    highBoundaryExpr_(NULL),
    lowConstrnExpr_(NULL),
    highConstrnExpr_(NULL),
    constrnExpr_(NULL),
    isMaxVal_(FALSE)
    {}

  // ---------------------------------------------------------------------
  // Display function for debugging
  // ---------------------------------------------------------------------
  virtual void print( FILE *ofd = stdout,
                        const char *indent = DEFAULT_INDENT,
                        const char *title = "PartRangePerCol",
                        CollHeap *c=NULL, 
                        char *buf=NULL
                      ) const;

  void display();
  const Int32 getPartIndex() const { return partIdx_; }
  OptRangeSpec *& getInterval() { return intervalRange_;}
  OptRangeSpec *& getHighBoundary() { return highBoundRange_;}
  ItemExpr *& getLowBoundExpr() { return lowBoundaryExpr_; }
  ItemExpr *& getIntervalExpr() { return intervalExpr_; }
  ItemExpr *& getHighBoundExpr() { return highBoundaryExpr_; }
  ValueId & getPartBoundPred() { return partboundPred_; }
  BoundaryExpr *& getBoundary() { return boundary_; }
  NABoolean & isMaxValue() { return isMaxVal_; }
  void createBoundForPartCol(BindWA *bindWA, 
                                     const NATable *naTable, 
                                     BoundaryValue *boundary,
                                     BoundaryValue *nextBoundary,
                                     NABoolean genRange = TRUE);
  ItemExpr *& getLowConstrnExpr() { return lowConstrnExpr_; }
  ItemExpr *& getHighConstrnExpr() { return highConstrnExpr_; }
  ItemExpr *& getConstrnExpr() { return constrnExpr_; }
private:
  const NAMemory *heap_;
  //const NATable *table_;
  
  // create table tp (c1 int, c2 int, c3 varchar(10))
  // partition by range(c1, c2)
  // (
  //  partition p1 values less than (10, 60),
  //  partition p2 values less than (20, 30),
  //  partition p3 values less than (30, 10)
  // );
  // example for p2, col c1 0
  const Int32 partIdx_;           // part number is 1
  const Int32 partColPos_;        // c1 is the first partition column, position is 0
  OptRangeSpec *intervalRange_;   // c1 :[10...20)            | 
  OptRangeSpec *highBoundRange_;  // c1 :[20...20]            | 
  ItemExpr * lowBoundaryExpr_;    // c1 :c1 = 10     
  ItemExpr *intervalExpr_;        // c1 :c1 > 10 and c1 < 20  | 
  ItemExpr *highBoundaryExpr_;    // c1 :c1 = 20              | 
  ValueId partboundPred_;         // value id for if...then...else
  ColReference *colName_;         //  column name
  BoundaryExpr *boundary_;        // boundary expression
  NABoolean isMaxVal_;            // this boundary is a MAXVALUE
  ItemExpr * lowConstrnExpr_;     // for constraint
  ItemExpr * highConstrnExpr_;    // for constraint
  ItemExpr * constrnExpr_;        // for constraint
};

class PartRange : public LIST(PartRangePerCol*)
{
public:
  PartRange(CollHeap* h = CmpCommon::statementHeap(),
                const NATable *naTable = NULL,
                const Int32 partIdx = -1,
                const NAString partEntityName = "") 
              : LIST(PartRangePerCol*)(h),
                table_(naTable),
                partIdx_(partIdx),
                partEntityName_(partEntityName)
              {}

  virtual ~PartRange(){};
  virtual void print( FILE *ofd = stdout,
                        const char *indent = DEFAULT_INDENT,
                        const char *title = "PartRange",
                        CollHeap *c = NULL, 
                        char *buf = NULL
                      ) const;

  void display();
  const NAString & getPartEntityName() { return partEntityName_; }
  const Int32 getPartIndex() { return partIdx_; }
private:
  const NATable *table_;
  const Int32 partIdx_;
  const NAString partEntityName_;
}; 


class Partition : public NABasicObject
{
public:
  enum PartitionType{
    UNKNOWN,
    RANGE_PARTITION,
    LIST_PARTITION,
    HASH_PARTITION
  };
  Partition(CollHeap *heap = CmpCommon::statementHeap(),
                PartitionType partType = UNKNOWN,
                const Int32 partIdx = -1,
                const Int32 partCount = 0,
                const NAString partName = "",
                const NAString partEntityName = "") 
              : heap_(heap),
                partType_(partType),
                partIdx_(partIdx),
                partCount_(partCount),
                partName_(partName),
                partEntityName_(partEntityName),
                isLastPart_(FALSE),
                firstMaxvalIdx_(-1),
                firstLowMaxvalIdx_(-1),
                lowBoundary_(NULL),
                highBoundary_(NULL),
                colList_(NULL),
                listBoundary_(NULL),
                isDefaultPartition_(FALSE),
                isSubPartition_(FALSE)
              {}
  
  virtual void print( FILE *ofd = stdout,
                        const char *indent = DEFAULT_INDENT,
                        const char *title = "Partition",
                        CollHeap *c = NULL, 
                        char *buf = NULL
                      ) const ;

  void display();
  const NAString & getPartEntityName() { return partEntityName_; }
  const NAString & getPartName() { return partName_; }
  const Int32 getPartIndex() { return partIdx_; }
  ItemExpr *& getlowBoundary() { return lowBoundary_; }
  ItemExpr *& getHighBoundary() { return highBoundary_; }
  ValueIdList & getLowValueList() { return lowValueList_; }
  ValueIdList & getHighValueList() { return highValueList_; }
  ItemExpr *& getColList() { return colList_; }
  ItemExprList *& getListBoundary() { return listBoundary_; }
  NABoolean & isLastPart() { return isLastPart_; }
  Int32 & getFirstMaxvalIdx() { return firstMaxvalIdx_; }
  Int32 & getFirstLowMaxvalIdx() { return firstLowMaxvalIdx_; }
  NABoolean & isDefaultPartition() { return isDefaultPartition_; }
  NABoolean & isSubPartition() { return isSubPartition_; }
private:
  const NAMemory *heap_;
  PartitionType partType_;
  const Int32 partIdx_;
  const NAString partName_;
  const NAString partEntityName_;
  const Int32 partCount_;
  NABoolean isLastPart_;
  Int32 firstMaxvalIdx_;
  Int32 firstLowMaxvalIdx_;
  ItemExpr *lowBoundary_;
  ItemExpr *highBoundary_;
  ValueIdList lowValueList_;
  ValueIdList highValueList_;
  ItemExpr *colList_;
  ItemExprList *listBoundary_;
  NABoolean isDefaultPartition_;
  NABoolean isSubPartition_;
}; 


class TableDesc : public NABasicObject
{
public:

  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  TableDesc(BindWA *bindWA, const NATable *table, CorrName &corrName) ;

private:
  TableDesc (const TableDesc &) ; //memleak fix
public:

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------
  const CorrName &getCorrNameObj() const 	     	  { return corrName_; }
        CorrName &getCorrNameObj()       	     	  { return corrName_; }

  const NAString &getLocationName() const {return corrName_.getLocationName();}
  NABoolean isLocationNameSpecified() const {return corrName_.isLocationNameSpecified();}
  NABoolean isPartitionNameSpecified() const {return corrName_.isPartitionNameSpecified();}
  NABoolean isKeyIndex(const IndexDesc * idesc) const;
  const NATable *getNATable() const                          { return table_; }
  const TableAnalysis *getTableAnalysis() const            { return analysis_; }
  const SelectivityHint * getSelectivityHint()	const	    { return selectivityHint_; }
  SelectivityHint * selectivityHint()	{ return selectivityHint_; }
  const CardinalityHint * getCardinalityHint()	const	    { return cardinalityHint_; }
  CardinalityHint * cardinalityHint()	{ return cardinalityHint_; }

  CostScalar getMinRC() const     { return minRC_ ; };
  void setMinRC(CostScalar rc) { minRC_ = rc; };
  CostScalar getMaxRC() const     { return maxRC_ ; };
  void setMaxRC(CostScalar rc) { maxRC_ = rc; };

  ValueIdSet & predsExecuted() { return predsExecuted_; };
  void setPredsExecuted(ValueIdSet pe) { predsExecuted_ = pe; };

  NABoolean 
    computeMinMaxRowCountForLocalPred(const ValueIdSet& joinCols, const ValueIdSet& localPreds,
                                      CostScalar& count, 
                                      CostScalar& min, CostScalar& max
                                     );

  const ValueIdList &getColumnList() const { return colList_; }
  ValueIdList &getPartColList()       { return partColList_; }
  ValueIdList &getSubpartColList()       { return subpartColList_; }
  const ValueIdList &getColumnVEGList() const      	{ return colVEGList_; }

  void getUserColumnList(ValueIdList &userColList) const;
  void getSystemColumnList(ValueIdList &systemColList) const;
  void getIdentityColumn(ValueIdList &systemColList) const;
  void getAllExceptCCList(ValueIdList &columnList) const;
  void getCCList(ValueIdList &columnList) const;
  NABoolean isIdentityColumnGeneratedAlways(NAString * value = NULL) const; 
  
  const LIST(IndexDesc *) &getIndexes() const              { return indexes_; }
  const LIST(IndexDesc *) &getUniqueIndexes() const        { return uniqueIndexes_; }
  NABoolean hasUniqueIndexes() const       { return uniqueIndexes_.entries() > 0; }

  NABoolean hasSecondaryIndexes() const      { return indexes_.entries() > 1; }
  const LIST(IndexDesc *) &getVerticalPartitions() const { return vertParts_; }
  const IndexDesc * getClusteringIndex() const     { return clusteringIndex_; }

  const LIST(const IndexDesc *) &getHintIndexes() const{ return hintIndexes_; }
  NABoolean hasHintIndexes() const       { return hintIndexes_.entries() > 0; }

  const ValueIdList &getColUpdated() const              { return colUpdated_; }
  const ValueIdList &getCheckConstraints() const  { return checkConstraints_; }
  ValueIdList &checkConstraints() 		  { return checkConstraints_; }

  const NABoolean isAInternalcheckConstraints() { return isAInternalcheckConstraints_; }

  const ValueIdList &getPreconditions() const  { return preconditions_; }
  ValueIdList &getPreconditions() { return preconditions_; }

  ItemExpr * genPartv2BaseCheckConstraint(BindWA *bindWA);
  void addPartnPredToConstraint(BindWA *bindWA);
  void addPartPredToPrecondition(BindWA *bindWA, ItemExpr *selectPred);
  void addPartitions(BindWA *bindWA,
                           const NATable *naTable,
                           const NAPartitionArray &partList,
                           const NABoolean genRange = TRUE);
  void genPartitions(BindWA *bindWA,
                           const NAPartitionArray &partList);

  void addPartPredToConstraint(BindWA *bindWA);

  const LIST(PartRange *) &getPartitions() const { return partitions_; }
  LIST(PartRange *) &getPartitions() { return partitions_; }
  const LIST(Partition *) &getPartitionList() const { return partitionList_; }
  LIST(Partition *) &getPartitionList() { return partitionList_; }
  const ColStatDescList &getTableColStats();
  ColStatDescList &tableColStats()
                         { return (ColStatDescList &)getTableColStats(); }

  // accumulate a list of interesting expressions that might
  // have an expression histogram
  void addInterestingExpression(NAString & unparsedExpr, const ValueIdSet & baseColRefs);

  NABoolean areHistsCompressed() {return histogramsCompressed_;}

  void histsCompressed(NABoolean flag) { histogramsCompressed_ = flag;}

  // Given a list of base columns, return the corresponding VEG columns
  // which maps base columns to index columns.
  void getEquivVEGCols (const ValueIdList &columnList,
			ValueIdList &VEGColumnList) const;
  void getEquivVEGCols (const ValueIdSet &columnSet,
			ValueIdSet &VEGColumnSet) const;
  ValueId getEquivVEGCol (const ValueId &column) const;
  NABoolean isSpecialObj();

  CostScalar getBaseRowCntIfUniqueJoinCol(const ValueIdSet &joinedCols);

  const ValueIdSet getPrimaryKeyColumns()   { return primaryKeyColumns_; }

  // ---------------------------------------------------------------------
  // Mutator functions
  // ---------------------------------------------------------------------
  void addCheckConstraint(BindWA *bindWA,
  			  const NATable *naTable,
			  const CheckConstraint *constraint,
			  ItemExpr *constraintPred);

  void clearColumnList()                                 { colList_.clear(); }
  void addToColumnList(const ValueId &colId)       { colList_.insert(colId); }
  void addToColumnList(const ValueIdList &clist)   { colList_.insert(clist); }
  void addToColumnVEGList(const ValueId &colId) { colVEGList_.insert(colId); }
  void addColUpdated(const ValueId &colId)      { colUpdated_.insert(colId); }
  void addIndex(IndexDesc *idesc)                  { indexes_.insert(idesc); }
  void addUniqueIndex(IndexDesc *idesc)		   { uniqueIndexes_.insert(idesc); }
  void addVerticalPartition(IndexDesc *idesc)    { vertParts_.insert(idesc); }
  void addHintIndex(const IndexDesc *idesc)    { hintIndexes_.insert(idesc); }
  void setClusteringIndex(IndexDesc *idesc)      { clusteringIndex_ = idesc; }
  void setCorrName(const CorrName &corrName)	     { corrName_ = corrName; }
  void setLocationName(const NAString &locName) {corrName_.setLocationName(locName);}
  void setTableAnalysis(TableAnalysis *analysis)      {analysis_ = analysis; }
  void setSelectivityHint(SelectivityHint *hint)      {selectivityHint_ = hint; }
  void setCardinalityHint(CardinalityHint *hint)      {cardinalityHint_ = hint; }
  void setPrimaryKeyColumns();

  ValueIdList &hbaseTagList() { return hbaseTagList_; }
  const ValueIdList &hbaseTSList() const { return hbaseTSList_; }
  ValueIdList &hbaseTSList() { return hbaseTSList_; }
  ValueIdList &hbaseVersionList() { return hbaseVersionList_; }
  ValueIdList &hbaseAttrList() { return hbaseAttrList_; }

  const ValueIdList &hbaseRowidList() const {return hbaseRowidList_;}
  ValueIdList &hbaseRowidList() { return hbaseRowidList_; }

  // ---------------------------------------------------------------------
  // Needed by Collections classes
  // ---------------------------------------------------------------------
//  NABoolean operator == (const TableDesc & rhs) { return (table_ == rhs.table_) &&
//							 (corrName_ == rhs.corrName_); }
  NABoolean operator == (const TableDesc & rhs) { return (&(*this) == &rhs); }


  // ---------------------------------------------------------------------
  // Print/debug
  // ---------------------------------------------------------------------
  virtual void print( FILE* ofd = stdout,
		      const char* indent = DEFAULT_INDENT,
                      const char* title = "TableDesc");

  //
  // 64-bit Project: Cast 'this' to "long" first to avoid C++ error
  //
  ULng32 hash() const { return (ULng32) ((Long)this/8);}

  // get local predicates for this table
  ValueIdSet getLocalPreds();

  // Is there any column which has a local predicates and no or dirty stats
  NABoolean isAnyHistWithPredsFakeOrSmallSample(const ValueIdSet &localPreds);


  // This method computes the ratio of selectivity obtained with and without hint
  // and sets that in the Hint

  void setBaseSelectivityHintForScan(CardinalityHint *cardHint,
					 CostScalar baseSelectivity);

  void setBaseSelectivityHintForScan(SelectivityHint *selHint,
					 CostScalar baseSelectivity);

  ValueIdSet getDivisioningColumns() ;

  ValueIdSet getSaltColumnAsSet() ;
  NABoolean hasIdentityColumnInClusteringKey() const ;


  LIST(NAString) *getMatchedPartInfo(BindWA *bindWA, NAString partName);
  LIST(NAString) *getMatchedPartInfo(BindWA *bindWA, ItemExprList valList);
  LIST(NAString) *getMatchedPartInfo(BindWA *bindWA, ItemExpr *selectPred);
  LIST(NAString) *getMatchedPartInfo(CollHeap *heap);
  LIST(NAString) *getMatchedPartions(BindWA *bindWA, 
                                     ItemList *values);
  //LIST(NAString) *getMatchedPartions(BindWA *bindWA, ItemExpr *selectPred);

private:

  ValueIdSet getComputedColumns(NAColumnBooleanFuncPtrT fptr);

  // compress the histograms based on query predicates on this table
  void compressHistogramsForCurrentQuery();

  // ---------------------------------------------------------------------
  // The table name
  // ---------------------------------------------------------------------
  CorrName corrName_;

  // ---------------------------------------------------------------------
  // Table object
  // ---------------------------------------------------------------------
  const NATable *table_;

  // ---------------------------------------------------------------------
  // A List of ValueIds that contains the identifers for the columns
  // provided by this reference to the NATable.
  // ---------------------------------------------------------------------
  ValueIdList colList_;

  ValueIdList partColList_;
  ValueIdList subpartColList_;

  // ---------------------------------------------------------------------
  // A list of VEG expressions and/or base columns that show the
  // equivalences of the base columns with index columns
  // ---------------------------------------------------------------------
  ValueIdList  colVEGList_;

  // ---------------------------------------------------------------------
  // List of indexes (including clustering index and unique index)
  // ---------------------------------------------------------------------
  LIST(IndexDesc *) indexes_;
  IndexDesc *clusteringIndex_;
  LIST(IndexDesc *) uniqueIndexes_;

  // ---------------------------------------------------------------------
  // List of vertical partitions
  // ---------------------------------------------------------------------
  LIST(IndexDesc *) vertParts_;

  // ---------------------------------------------------------------------
  // List of recommended indexes by user hints
  // ---------------------------------------------------------------------
  LIST(const IndexDesc *) hintIndexes_;

  // ---------------------------------------------------------------------
  // List of available column statistics
  // ---------------------------------------------------------------------
  ColStatDescList colStats_;

  // -------------------------------------------------------------------
  // Are histograms of this table compressed
  // --------------------------------------------------------------------
  NABoolean histogramsCompressed_;

  // ---------------------------------------------------------------------
  // A list of expressions, each of which represents a check constraint.
  // ---------------------------------------------------------------------
  ValueIdList checkConstraints_;

  // ---------------------------------------------------------------------
  // flag the checkConstraints_ is added internal for partiion entity table.
  // ---------------------------------------------------------------------
  NABoolean isAInternalcheckConstraints_;

  // ---------------------------------------------------------------------
  // A precondition for scan node
  // ---------------------------------------------------------------------
  ValueIdList preconditions_;

  // ---------------------------------------------------------------------
  // A list of table partition, each of which represents a partition.
  // ---------------------------------------------------------------------
  LIST(PartRange *) partitions_;
  LIST(Partition *) partitionList_;


  // ---------------------------------------------------------------------
  // List of columns being updated
  // ---------------------------------------------------------------------
  ValueIdList colUpdated_;

  // ---------------------------------------------------------------------
  // The table analysis result from query analyzer
  // ---------------------------------------------------------------------
  TableAnalysis *analysis_;


  // ---------------------------------------------------------------------
  // primary key columns. This is used by GroupByAgg to compute dependency
  // of columns
  // ----------------------------------------------------------------------
  ValueIdSet primaryKeyColumns_;

  // selectivity hint contains the hint given by the user, and all the
  // local predicates on that table to which it corresponds to

  SelectivityHint * selectivityHint_;

  // cardinality hint contains the hint given by the user, and all the
  // local predicates on that table to which it corresponds to

  CardinalityHint * cardinalityHint_;

  // min and max rowcount based on actual cound obtained after executing the query
  // on the sample
  CostScalar minRC_;
  CostScalar maxRC_;

  ValueIdSet predsExecuted_;

  // ---------------------------------------------------------------------
  // Access mode
  // Lock mode
  // CONTROLs that are in effect
  // ---------------------------------------------------------------------

  // ---------------------------------------------------------------------
  // A List of ValueIds for hbase attributes for each of the column
  // in colList_. Attributes are: timestamp, version, tag
  // ---------------------------------------------------------------------
  ValueIdList  hbaseAttrList_;

  // ---------------------------------------------------------------------
  // A List of ValueIds for hbase tags for each of the column
  // in colList_.
  // ---------------------------------------------------------------------
  ValueIdList  hbaseTagList_;

  // ---------------------------------------------------------------------
  // A List of ValueIds for hbase timestamp values for each of the column
  // in colList_.
  // ---------------------------------------------------------------------
  ValueIdList  hbaseTSList_;

  // ---------------------------------------------------------------------
  // A List of ValueIds for hbase version values for each of the column
  // in colList_.
  // ---------------------------------------------------------------------
  ValueIdList  hbaseVersionList_;

  // ---------------------------------------------------------------------
  // A List of ValueIds for hbase rowid values for table
  // ---------------------------------------------------------------------
  ValueIdList hbaseRowidList_;

  // ---------------------------------------------------------------------
  // A dictionary of expressions that occur as children of comparison 
  // operators; expressions histograms might be helpful here. The
  // ValueIdSet contains the set of base table columns referenced
  // in each instance of an expression. (Note: There is a corresponding
  // dictionary in the NATable object, but that one doesn't contain the
  // ValueIdSet. The ValueIdSet depends on the instance of the table,
  // and therefore doesn't make sense to store at the NATable level.)
  // ---------------------------------------------------------------------
  NAHashDictionary<NAString, ValueIdSet> interestingExpressions_;

}; // class TableDesc

// ***********************************************************************
// Implementation for inline functions
// ***********************************************************************

// ***********************************************************************
// A list of TableDescs
// ***********************************************************************

class TableDescList : public LIST(TableDesc *)
{
public:
  TableDescList(CollHeap* h/*=0*/): LIST(TableDesc *)(h) { }

  // ---------------------------------------------------------------------
  // Print
  // ---------------------------------------------------------------------
  virtual void print( FILE* ofd = stdout,
		      const char* indent = DEFAULT_INDENT,
                      const char* title = "TableDescList");
}; // class TableDescList

class SelectivityHint : public NABasicObject
{
public:
  SelectivityHint(double selectivityFactor = -1.0);

  // Destructor
  virtual ~SelectivityHint()
  {}

  inline double getScanSelectivityFactor () const { return selectivityFactor_     ; }
  void setScanSelectivityFactor (double selectivityFactor);

  inline const ValueIdSet & localPreds() const { return localPreds_; };
  void setLocalPreds(const ValueIdSet &lop) { localPreds_ = lop; }

  inline double getBaseScanSelectivityFactor () const { return baseSelectivity_     ; }
  void setBaseScanSelectivityFactor (double baseSelectivity) {baseSelectivity_ = baseSelectivity; }

private:
  // selectivity hint given by the user in the Select statement
  double selectivityFactor_;

  // set of local predicates on the table for which the hint is given
  ValueIdSet localPreds_;

  // base selectivity obtained after applying all local predicates on a table
  double baseSelectivity_;

};

class CardinalityHint : public NABasicObject
{
public:
  CardinalityHint(CostScalar scanCardinality = 0.0);

  CardinalityHint(CostScalar scanCardinality,
    const ValueIdSet & localPreds);

  // Destructor
  virtual ~CardinalityHint()
  {}

  inline CostScalar getScanCardinality () const { return scanCardinality_     ; }
  void setScanCardinality (CostScalar scanCardinality) { scanCardinality_ = MIN_ONE_CS(scanCardinality); }

  inline const ValueIdSet & localPreds() const { return localPreds_; };
  void setLocalPreds(const ValueIdSet &lop) { localPreds_ = lop; }

  inline double getBaseScanSelectivityFactor () const { return baseSelectivity_     ; }
  void setBaseScanSelectivityFactor (double baseSelectivity) {baseSelectivity_ = baseSelectivity; }

  inline CostScalar getScanSelectivity () const { return scanSelectivity_     ; }
  void setScanSelectivity (CostScalar scanSelectivity) {scanSelectivity_ = scanSelectivity; }

private:
  // selectivity hint given by the user in the Select statement
  CostScalar scanCardinality_;

  // selectivity hint given by the user in the Select statement
  CostScalar scanSelectivity_;

  // set of local predicates on the table for which the hint is given
  ValueIdSet localPreds_;

    // base selectivity obtained after applying all local predicates on a table
  double baseSelectivity_;

};

#endif  /* TABLEDESC_H */
