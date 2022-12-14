#ifndef TABLENAMEMAP_H
#define TABLENAMEMAP_H
/* -*-C++-*-
**************************************************************************
*
* File:         TableNameMap.h
* Description:  A name map for a table
* Created:      4/27/94
* Language:     C++
*

*
**************************************************************************
*/

#include "optimizer/ColumnDesc.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class TableNameMap;
class XTNM;
class TableViewUsage;
class TableViewUsageList;

// ***********************************************************************
//
// TableNameMap : A table name map.
//
// A TableNameMap associates the exposed name for a table with its
// TableId. A TableNameMap is transient and is used only during the
// name binding phase.
//
// The XTNM (Exposed Table Name Map) cache is a hash table of
// TableNameMap's that permits an associative (exposed name) lookup.
//
// ***********************************************************************
class TableNameMap : public NABasicObject {
 public:
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  TableNameMap(const CorrName &tableName, ColumnDescList *columnList, CollHeap *h = 0)
      : tableName_(tableName, h), columnList_(columnList), hbaseColNameSet_(h) {}

  // copy ctor
  TableNameMap(const TableNameMap &, CollHeap *h = 0);  // not written

  // ---------------------------------------------------------------------
  // Destructor function
  // ---------------------------------------------------------------------
  ~TableNameMap() { delete columnList_; }

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------
  const CorrName &getTableName() const { return tableName_; }

  ColumnDescList *getColumnList() const { return columnList_; }

  // ---------------------------------------------------------------------
  // The following methods are required by the NAKeyLookup
  // ---------------------------------------------------------------------
  const CorrName *getKey() const { return &tableName_; }

  NABoolean operator==(const TableNameMap &other) const { return this == &other; }

 private:
  CorrName tableName_;
  ColumnDescList *columnList_;

  // columns retrieved from hbase external tables through the use of
  // COLUMN_LOOKUP of COLUMNS_DISPLAY function.
  // Valid only for external hbase tables in row format.
  //  (hbase."_ROW_".<tab>)
  //  NAList<NAString> hbaseColNameList_;
  NASet<NAString> hbaseColNameSet_;

};  // class TableNameMap

// ***********************************************************************
// XTNM
//
// A collection of TableNameMap
// ***********************************************************************
#define XTNM_INIT_SIZE 47  // Initial size of the XTNM cache

class XTNM : public NAKeyLookup<CorrName, TableNameMap> {
 public:
  XTNM(CollHeap *h /*=0*/)
      : NAKeyLookup<CorrName, TableNameMap>(XTNM_INIT_SIZE, NAKeyLookupEnums::KEY_INSIDE_VALUE, h) {}

  // copy ctor
  XTNM(const XTNM &orig, CollHeap *h = 0) : NAKeyLookup<CorrName, TableNameMap>(orig, h) {}

  ~XTNM();

  // XTNM clients must use this method, not the simplistic insert()
  // inherited from NAKeyLookup!
  void insertNames(BindWA *bindWA, CorrName &tableName, ColumnDescList *columnList = NULL);

};  // class XTNM

// ***********************************************************************
class TableViewUsage : public NABasicObject {
 public:
  TableViewUsage(const QualifiedName &tableName, ExtendedQualName::SpecialTableType type, NABoolean isView,
                 int viewCount, CollHeap *h = CmpCommon::statementHeap())
      : tableName_(tableName, h), type_(type), isView_(isView), viewCount_(viewCount) {}

  const QualifiedName &getTableName() const { return tableName_; }
  ExtendedQualName::SpecialTableType getSpecialType() const { return type_; }
  NABoolean isView() const { return isView_; }
  int viewCount() const { return viewCount_; }

 private:
  const QualifiedName tableName_;
  ExtendedQualName::SpecialTableType type_;
  NABoolean isView_;
  int viewCount_;

};  // class TableViewUsage

class TableViewUsageList : public LIST(TableViewUsage *) {
 public:
  TableViewUsageList(CollHeap *h = CmpCommon::statementHeap()) : LIST(TableViewUsage *)(h) {}

  void display(NABoolean newline, size_t indent) const;
  void display() const;  // useful in MSDEV

  int getViewsOnTable(CollIndex begIx, CollIndex endIx, int viewCount, const QualifiedName &baseName,
                      ExtendedQualName::SpecialTableType baseType, const QualifiedName *additionalNameToFormat,
                      NAString &formattedListOfViewsThatUseTheBaseTable) const;

};  // class TableViewUsageList

#endif /* TABLENAMEMAP_H */
