//*****************************************************************************
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
//// @@@ END COPYRIGHT @@@
//*****************************************************************************

#ifndef CMPSEABASEDDLMDCOMMON_H
#define CMPSEABASEDDLMDCOMMON_H

// -----------------------------------------------------------------------------
// File contains common structures that are shared between system metadata and
// PrivMgr metadata.  These structures previously existed in CmpSeabaseDDLmd.h
// and CmpSeabaseDDLupgrade.h.  They were placed in a separate file to eliminate
// build issues.
// For example, QString is a structure used as part of defining metadata tables.
// It is also an imported class we use as part of the compiler GUI that displays
// plans. By moving it to a separate file, we avoid compiler issues.
// -----------------------------------------------------------------------------

#include "common/Platform.h"
#include "common/NABoolean.h"

// structure describes a const char * str used to define metadata tables
struct QString {
 public:
  const char *str;
};

// structure containing information on old and new MD tables
// and how the new MD tables will be updated.
struct MDUpgradeInfo {
  // name of the new MD table.
  const char *newName;

  // name of the corresponding old MD table.
  const char *oldName;  // name of the old

  // ddl stmt corresponding to the current ddl.
  const QString *newDDL;
  Lng32 sizeOfnewDDL;

  // ddl stmt corresponding to the old ddl which is being upgraded.
  // If null, then old/new ddl are the same.
  const QString *oldDDL;
  Lng32 sizeOfoldDDL;

  // ddl stmt corresponding to index on this table, if one exists
  const QString *indexDDL;
  Lng32 sizeOfIndexDDL;

  const NABoolean upgradeNeeded;

  // if new and old col info is different, then data need to be copied using
  // explicit column names in insert and select part of the query.
  // upsert into tgt (insertedCols) select selectedCols from src;
  // Note that the OBJECTS table must always do this, since we need to modify
  // the table name for rows concerning the metadata tables themselves. That is,
  // we are copying in rows concerning the *old* metadata tables; rows concerning
  // the *new* metadata tables are already there from "initialize trafodion" and
  // we don't want to overwrite them as part of the UPSERT.
  const char *insertedCols;
  const char *selectedCols;

  // any where predicate to be applied to the select part of the query
  const char *wherePred;

  const NABoolean addedCols;
  const NABoolean droppedCols;

  const NABoolean addedTable;
  const NABoolean droppedTable;

  const NABoolean isIndex;

  // if true, indicates this objects exists in metadata only. There is no corresponding
  // object in hbase.
  const NABoolean mdOnly;
};

#endif
