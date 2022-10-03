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
#ifndef COLUMNDESC_H
#define COLUMNDESC_H
/* -*-C++-*-
**************************************************************************
*
* File:         ColumnDesc.h
* Description:  Column descriptor
* Created:      5/19/95
* Language:     C++
*
*
**************************************************************************
*/

// -----------------------------------------------------------------------

#include "common/Collections.h"
#include "optimizer/ObjectNames.h"
#include "optimizer/ValueDesc.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ColumnDesc;
class ColumnDescList;

// ***********************************************************************
// ColumnDesc : A column descriptor
// ***********************************************************************
class ColumnDesc : public NABasicObject
{
public:
  
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  ColumnDesc(const ColRefName& colRefName, const ValueId valId,
	     const char * heading, CollHeap * h=0)
      : colRefName_(colRefName, h) , valId_(valId) , origValId_((CollIndex)0),
	heading_(heading), groupedFlag_(FALSE), rowidGroupedFlag_(FALSE),
        viewColPosition_(-1), viewFileName_(NULL), isRenameCol_(false) {}
  
  // copy ctor
  ColumnDesc (const ColumnDesc & col, CollHeap * h=0) :
       colRefName_(col.colRefName_, h),
       valId_(col.valId_),
       origValId_(col.origValId_),
       heading_(col.heading_),
       groupedFlag_(col.groupedFlag_),
       rowidGroupedFlag_(col.rowidGroupedFlag_),
       viewColPosition_(col.viewColPosition_),
       viewFileName_(col.viewFileName_),
       isRenameCol_(col.isRenameCol_)
  {}

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------
  const ColRefName& getColRefNameObj() const 	{ return colRefName_; }

  ValueId getValueId() const 			{ return valId_;  }
  void setValueId(ValueId valId)		{ valId_ = valId; }

  ValueId getOrigValueId() const 			{ return origValId_;  }
  void setOrigValueId(ValueId valId)		{ origValId_ = valId; }

  const char * getHeading() const               { return heading_; }
  void setHeading(const char * heading)		{ heading_ = heading;  }
  NABoolean isGrouped() const 			{ return groupedFlag_; }
  void setGroupedFlag() 			{ groupedFlag_ = TRUE; }

  NABoolean isRowidGrouped() const 			{ return rowidGroupedFlag_; }
  void setRowidGroupedFlag() 			{ rowidGroupedFlag_ = TRUE; }

  Lng32 getViewColPosition() const 	        { return viewColPosition_; }
  void setViewColPosition(Lng32 val) 	        { viewColPosition_ = val; }

  const char * getViewFileName() const          { return viewFileName_; }
  void setViewFileName(const char * name)       { viewFileName_ = name;  }

  NABoolean isRenameCol()                       { return isRenameCol_; }
  void setRenameCol( NABoolean renameCol )      { isRenameCol_ = renameCol; }

  // ---------------------------------------------------------------------
  // Display/print, for debugging.
  // ---------------------------------------------------------------------
  void print( FILE* ofd = stdout,
	      const char* indent = DEFAULT_INDENT,
              const char* title = "ColumnDesc") const
  {
    if (strcmp(title, "")) {
      BUMP_INDENT(indent);
      fprintf(ofd,"%s%s %s",NEW_INDENT,title,NEW_INDENT);
    }
    colRefName_.print(ofd, indent, "", TRUE /* brief */);
    fprintf(ofd," %u%s", 
      CollIndex(valId_), 
      groupedFlag_? " grp " : "");
    if (strcmp(title, "")) fprintf(ofd,"\n");
  } // ColumnDesc::print()

  void display() const { print(); }

private:

  // ---------------------------------------------------------------------
  // Column name.  For an unnamed expression, it's a null string.
  // ---------------------------------------------------------------------
  ColRefName colRefName_;

  // ---------------------------------------------------------------------
  // The value identifier.
  // ---------------------------------------------------------------------
  ValueId valId_;

  // ---------------------------------------------------------------------
  // The value identifier may be changed by castComputedColumnsToAnsiTypes
  // this is to save the original valId before cast.
  // ---------------------------------------------------------------------
  ValueId origValId_;

  // -------------------------------------------------------------
  // User specified heading for this column at CREATE TABLE time.
  // -------------------------------------------------------------
  const char * heading_;

  // ---------------------------------------------------------------------
  // Grouped flag - TRUE if this is a grouped column, i.e. the column was
  // listed in the GROUP BY clause.
  // ---------------------------------------------------------------------
  NABoolean groupedFlag_;

  // ---------------------------------------------------------------------
  // ROWID Grouped flag - TRUE if ROWID is a grouped column, i.e. the
  // HbaseRowid(col) was listed in the GROUP BY clause.
  // ---------------------------------------------------------------------
  NABoolean rowidGroupedFlag_;

  // ----------------------------------------------------------------------
  // The next two fields are used only for views. They record the position
  // of this column in the view definition, and the name of the view. These
  // two fields are used in ColumnReference::bindNode() as we collect all
  // columns that are explicitly referenced in a query, for privilege checking
  // -----------------------------------------------------------------------
  Lng32 viewColPosition_;
  const char * viewFileName_;
  NABoolean isRenameCol_;

}; // class ColumnDesc

// ***********************************************************************
// ColumnDescList : A list of column descriptors
// ***********************************************************************
class ColumnDescList : public LIST(ColumnDesc *)
{
public:

  ColumnDescList(CollHeap* h) : LIST(ColumnDesc *)(h) {}

  ColumnDescList (const ColumnDescList & cdlist, CollHeap * h) 
    : LIST(ColumnDesc *)(cdlist, h) {}

  ~ColumnDescList()
  {
    // does NOT call clearAndDestroy!
    // (doing so deletes pointers and leaves dangling references all over,
    // because ColumnDescLists are created as temporary lists all over)
  }
  
  void getValueIdList(ValueIdList &vidList) const
  {
    for (CollIndex i = 0; i < entries(); i++)
      vidList.insert(at(i)->getValueId());
  }

  // -- MV
  ColumnDesc *findColumn(const NAString& colName) const;

  // Remove all column descriptors from the list and call their destructors
  void clearAndDestroy()
  {
    for (CollIndex i = 0; i < entries(); i++)
      delete at(i);
    clear();
  }

  NAString getColumnDescListAsString(NABoolean removeIntCorrName = FALSE) const;

}; // class ColumnDescList

#endif /* COLUMNDESC_H */
