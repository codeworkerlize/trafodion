
#ifndef THBASE_H
#define THBASE_H 1

#include "common/Collections.h"
#include "optimizer/ValueDesc.h"
#include "export/NABasicObject.h"
#include "common/CmpCommon.h"

struct HbaseSearchSpec : public NABasicObject {
  HbaseSearchSpec(NAHeap *h = NULL) : colNames_(h ? h : STMTHEAP), rowTS_(0){};
  void addColumnNames(const ValueIdSet &vs);
  const NAString getText() const;

  // column names to be retrieved.
  // Same set to be retrieved for all row id entries in rowIds_
  // Format of each entry:
  //         colfam:colname      ==> to retrieve 'colname' from 'colfam'
  //         colfam:             ==> to retrieve all columns in that family
  NAList<NAString> colNames_;

  // row timestamp at which the row is to be retrieved
  // If -1, latest timestamp
  long rowTS_;

 protected:
};

// This struct is used to specify unique rowids at runtime by
// calling 'Get' methods of hbase api.
// A list of these structs is created and each entry is evaluated
// at runtime.
// If rowIds_ contains one entry, then 'getRow' methods are used.
// If rowIds_ contain multiple entries, then 'getRows' methods are used.

struct HbaseUniqueRows : public HbaseSearchSpec {
  HbaseUniqueRows(NAHeap *h = NULL) : HbaseSearchSpec(h), rowIds_(h ? h : STMTHEAP){};
  const NAString getText() const;

  // list of rowIds
  NAList<NAString> rowIds_;
};

// This struct is used to specify range of rowids at runtime by
// calling 'scanner' methods of hbase api.
// A list of these structs is created and each entry is evaluated
// at runtime.

struct HbaseRangeRows : public HbaseSearchSpec {
  // range of rowids .
  // If begin is null, start at beginning.
  // If end is null, stop at end.
  // If both are null, scan all rows.
  NAString beginRowId_;
  NAString endRowId_;

  NABoolean beginKeyExclusive_;
  NABoolean endKeyExclusive_;

  NABoolean beginKeyIsMixedExpr_;
  NABoolean endKeyIsMixedRxpr_;

  HbaseRangeRows(NAHeap *h = NULL) : HbaseSearchSpec(h){};

  const NAString getText() const;
};

struct ListOfUniqueRows : public NAList<HbaseUniqueRows> {
  ListOfUniqueRows(NAHeap *h = NULL) : NAList<HbaseUniqueRows>(h ? h : STMTHEAP){};
  const NAString getText() const;
};

struct ListOfRangeRows : public NAList<HbaseRangeRows> {
  ListOfRangeRows(NAHeap *h = NULL) : NAList<HbaseRangeRows>(h ? h : STMTHEAP){};
  const NAString getText() const;
};

#endif
