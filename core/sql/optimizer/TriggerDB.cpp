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
/* -*-C++-*-
******************************************************************************
*
* File:         TriggerDB.cpp
* Description:  
* Created:      06/23/98
*
*
******************************************************************************
*/

#include "AllRelExpr.h"
#include "SchemaDB.h"
#include "Triggers.h"
#include "TriggerDB.h"
#include "BindWA.h"
#include "Context.h"
#include "Globals.h"

#include "../sqlcomp/CmpSeabaseDDL.h"
//-----------------------------------------------------------------------------
//
// -- class TableOp Methods

//
// -- TableOp::print
// 
// Debugging aid.
//
// For debugging only
void 
TableOp::print(ostream& os) const 
{
  os << "Triggers on ";
  switch (operation_) 
  {
    case COM_UPDATE: os << "Update";
      break;
    case COM_DELETE: os << "Delete";
      break;
    case COM_INSERT: os << "Insert";
      break;
  }
  os << "of Table " << subjectTable_.getQualifiedNameAsAnsiString() << " : " 
     << endl;
}

//-----------------------------------------------------------------------------
//
// -- class TriggerDB Methods

//
// -- TriggerDB::HashFunction 
//
// Make sure that triggers on different operations on the same table are hashed
// to different hash value.
//
ULng32 
TriggerDB::HashFunction(const TableOp & key) 
{
  ULng32 hval = key.subjectTable_.hash();
  if      (key.operation_ == COM_INSERT)
    return hval;
  else if (key.operation_ == COM_DELETE)
    return hval+1;
  else if (key.operation_ == COM_UPDATE)
    return hval+2;
  else 
  {
    CMPASSERT(FALSE);
    return 0;
  }
}

/*
ULng32 
TableOp::hash() const 
{ 
  return TriggerDB::HashFunction(*this);
}
*/
    
//
// --TriggerDB::getValidEntry
//
// return the BeforeAndAfterTriggers (ptr) given the key. 
// NULL is returned if no entry was found. In case of working across statements
// (Trigger::heap() == CmpCommon::contextHeap()) then this method
// VALIDATES (timestamp check) the return value before returning. 
// Note: a non-valid entry is first removed from the DB and a NULL is returned.
//
BeforeAndAfterTriggers* 
TriggerDB::getValidEntry(const TableOp * key, BindWA * bindWA)
{
  BeforeAndAfterTriggers* result = 
    NAHashDictionary<TableOp, BeforeAndAfterTriggers>::getFirstValue(key);

  if ((result != NULL) && (Trigger::Heap() == CmpCommon::contextHeap())) 
  { 
    // only used when triggers are allocated from the cntext heap. 
    // Currently triggers are allocated from the statement heap.
    // See method Trigger::Heap() in file Triggers.h for more details

    // entry exist in TriggerDB and we work ACROSS statements =>
    //   validate the entry: compare the entry's subject table timestamp
    //   against the actual subject table's timestamp 

    // convert to ExtendedQualName 
    ExtendedQualName exSubjectTable = 
      ExtendedQualName(key->subjectTable_, CmpCommon::statementHeap());
    // fetch the subject table from the NATableDB
    NATable *subjectTable = 
      bindWA->getSchemaDB()->getNATableDB()->get(&exSubjectTable);

    // the subject table MUST appear in the TableTB because it must have 
    // been fetched earlier by the binder (also in the case of cascaded 
    // triggers)
    CMPASSERT(subjectTable != NULL);
    ComTimestamp trigsTS = result->getSubjectTableTimeStamp();
    CMPASSERT(trigsTS != 0);	// must have been initialized by the catman

    if (subjectTable->getRedefTime() != trigsTS) 
    { 
      // no match => invalidate
      this->remove((TableOp*) key);
      result->clearAndDestroy();
      delete result; // destroy the entry
      return NULL;
    }
  } // end of validation

  // at this point, if result != NULL, then it is valid. Otherwise it is 
  // not in the TriggerDB
  return result;
}

// -- TriggerDB::getTriggers
//
// This method is the driver of getting triggers to the binder:
// It performs caching of triggers in triggerDB
// It accesses the Catman to get triggers (readTriggersDef())
//
BeforeAndAfterTriggers * 
TriggerDB::getTriggers(QualifiedName &subjectTable, 
		       ComOperation   operation, 
		       BindWA        *bindWA,
		       bool           allTrigger)
{
  BeforeAndAfterTriggers *triggers = NULL;

  return triggers;
}

BeforeAndAfterTriggers *TriggerDB::getTriggers(QualifiedName &subjectTable,
                                    ComOperation   operation)
{
  TableOp key(subjectTable, operation);
  TriggerDB * triggerDB = GetCliGlobals()->currContext()->getTriggerDB();
  if (triggerDB != NULL)
      return triggerDB->getFirstValue(&key);
  else
      return NULL;
}

void TriggerDB::putTriggers(QualifiedName &subjectTable,
                 ComOperation   operation,
                 BeforeAndAfterTriggers *triggers)
{
  TableOp *key = new (Trigger::Heap())
    TableOp(subjectTable, operation);
  TriggerDB * triggerDB = GetCliGlobals()->currContext()->getTriggerDB();
  if ((triggerDB != NULL) && (NULL == triggerDB->insert(key, triggers))) {
    delete key;
  }
}

void TriggerDB::removeTriggers(QualifiedName &subjectTable,
			       ComOperation   operation)
{
  TableOp key(subjectTable, operation);
  TriggerDB * triggerDB = GetCliGlobals()->currContext()->getTriggerDB();
  if (triggerDB != NULL)
      {
          triggerDB->remove(&key);
      }
}

BeforeAndAfterTriggers *TriggerDB::getAllTriggers(QualifiedName &subjectTable,
						  Int64 tableId,
						  NABoolean useTrigger)
{
  if (useTrigger == FALSE)
    return NULL;

  BeforeAndAfterTriggers *triggers = NULL;

  // get table info name
  NAString catalogName(subjectTable.getCatalogName());
  char *catName = const_cast<char*>(catalogName.data());
  char *nPos = strchr(catName, ':');
  if (nPos != NULL)
    {
      catName = nPos + 1;
    }
  NAString schemaName(subjectTable.getSchemaName());
  NAString tableName(subjectTable.getObjectName());
  QualifiedName tableQualifiedName(tableName.data(), schemaName.data(), catName);
  
  triggers = getTriggers(tableQualifiedName, COM_INSERT);
  if (NULL != triggers) {
    return triggers;
  }

  if (ComIsTrafodionReservedSchemaName(schemaName)) {
    return NULL;
  }

  ExeCliInterface cliInterface(STMTHEAP);

  if (tableId == -1 || tableId == 0)
    {
      CmpSeabaseDDL cmpSBD(STMTHEAP, FALSE);

      tableId = cmpSBD.getObjectUID(&cliInterface,
				    catName,
				    schemaName.data(),
				    tableName.data(),
				    COM_BASE_TABLE_OBJECT_LIT);

      // if object type is COM_INDEX_OBJECT, or can not find the object UID.
      if (tableId < 0)
	return NULL;
    }

  // get trigger info from table
  char query[8192];
  memset(query, 0, 8192);
  str_sprintf(query, "select udr_uid, language_type, deterministic_bool, sql_access, max_results from %s.\"%s\".%s where udr_type='TR' and library_uid = %ld;",
	      TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_ROUTINES, tableId);

  Queue * objectsInfo = NULL;
  Queue * objectsInfo1 = NULL;
  Queue * objectsInfo2 = NULL;

  TriggerList *beforeStatementTrigger = NULL;
  TriggerList *beforeRowTrigger = NULL;
  TriggerList *afterStatementTrigger = NULL;
  TriggerList *afterRowTrigger = NULL;
  int countTrigger = 0;

  ULng32 flagBits = GetCliGlobals()->currContext()->getSqlParserFlags();;
  GetCliGlobals()->currContext()->setSqlParserFlags(0x20000);
  Lng32 cliRC = cliInterface.fetchAllRows(objectsInfo, query, 0, FALSE, FALSE, TRUE);
  GetCliGlobals()->currContext()->assignSqlParserFlags(flagBits);

  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return NULL;
    }

  Trigger *trigger = NULL;
  objectsInfo->position();
  static int ncount = 0;
  ncount++;
  for (int i = 0; i < objectsInfo->numEntries(); i++)
    {
      OutputInfo * oi = (OutputInfo*)objectsInfo->getNext();
      Int64 triggerid = *(Int64*)oi->get(0);
      char *isafter = oi->get(1);
      char *isStatement = oi->get(2);
      char *sqlacc = oi->get(3);
      Lng32 op = *(Lng32*)oi->get(4);

      memset(query, 0, 8192);
      str_sprintf(query, "select text from %s.\"%s\".%s where text_uid = %ld",
		  TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT,
		  triggerid);

      flagBits = GetCliGlobals()->currContext()->getSqlParserFlags();
      GetCliGlobals()->currContext()->setSqlParserFlags(0x20000);
      cliRC = cliInterface.fetchAllRows(objectsInfo1, query, 0, FALSE, FALSE, TRUE);
      GetCliGlobals()->currContext()->assignSqlParserFlags(flagBits);
      if (cliRC < 0)
	{
	  cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
	  continue;;
	}
      objectsInfo1->position();
      OutputInfo *oi1 = (OutputInfo*)objectsInfo1->getNext();
      NAString text = oi1->get(0);

      //get trigger name from objects table
      memset(query, 0, 8192);
      str_sprintf(query, "select catalog_name, schema_name, object_name from %s.\"%s\".%s where object_uid = %ld and object_type = 'TR';",
		  TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
		  triggerid);
      flagBits = GetCliGlobals()->currContext()->getSqlParserFlags();
      GetCliGlobals()->currContext()->setSqlParserFlags(0x20000);
      cliRC = cliInterface.fetchAllRows(objectsInfo2, query, 0, FALSE, FALSE, TRUE);
      GetCliGlobals()->currContext()->assignSqlParserFlags(flagBits);
      if (cliRC < 0)
	{
	  cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
	  continue;
        }

      objectsInfo2->position();
      OutputInfo *oi2 = (OutputInfo*)objectsInfo2->getNext();
      char * catName = (char*)oi2->get(0);
      char * schName = (char*)oi2->get(1);
      char * objName = (char*)oi2->get(2);
      QualifiedName triggername(objName, schName, catName);

      trigger = new (Trigger::Heap()) Trigger(triggername,
					      subjectTable,
					      (ComOperation)op,
					      isafter[0] == 'B' ? ComActivationTime::COM_BEFORE : ComActivationTime::COM_AFTER,
					      isStatement[0] == 'Y' ? ComGranularity::COM_STATEMENT : ComGranularity::COM_ROW,
					      triggerid,
					      &text,
					      CharInfo::ISO88591);
      trigger->setTableId(tableId);

      if (isafter[0] == 'B' && isStatement[0]  == 'Y')
	{
	  if (beforeStatementTrigger == NULL)
	    {
	      beforeStatementTrigger = new(Trigger::Heap()) TriggerList(Trigger::Heap());
            }
	  beforeStatementTrigger->insert(trigger);
	}
      else if (isafter[0] == 'B')
	{
	  if (beforeRowTrigger == NULL)
	    {
	      beforeRowTrigger = new(Trigger::Heap()) TriggerList(Trigger::Heap());
	    }
	  beforeRowTrigger->insert(trigger);
	}
      else if (isafter[0] == 'A' && isStatement[0] == 'Y')
	{
	  if (afterStatementTrigger == NULL)
	    {
	      afterStatementTrigger = new(Trigger::Heap()) TriggerList(Trigger::Heap());
	    }
	  afterStatementTrigger->insert(trigger);
	}
      else
	{
	  if (afterRowTrigger == NULL)
	    {
              afterRowTrigger = new(Trigger::Heap()) TriggerList(Trigger::Heap());
	    }
	  afterRowTrigger->insert(trigger);
	}
      countTrigger++;
    }

  if (beforeStatementTrigger != NULL || beforeRowTrigger != NULL ||
      afterStatementTrigger != NULL || afterRowTrigger != NULL)
    {
      triggers = new (Trigger::Heap())BeforeAndAfterTriggers(NULL,
							     beforeStatementTrigger,
							     beforeRowTrigger,
							     afterStatementTrigger,
							     afterRowTrigger,
							     countTrigger,
							     NULL);
      putTriggers(tableQualifiedName, COM_INSERT, triggers);
    }

  return triggers;
}

// only used when triggers are allocated from the cntext heap. 
// Currently triggers are allocated from the statement heap.
// See method Trigger::Heap() in file Triggers.h for more details
void
TriggerDB::clearAndDestroy()
{
  NAHashDictionaryIterator<TableOp,BeforeAndAfterTriggers> iter(*this);
  
  TableOp * key = NULL;
  BeforeAndAfterTriggers* value = NULL;
  // iterate over all entries and destroy them
  for (Int32 i=0; i < iter.entries(); i++) 
  {
    iter.getNext(key, value);
    CMPASSERT(key != NULL);
    CMPASSERT(value != NULL);
    value->clearAndDestroy();
    delete value;
    value = NULL;
    delete key;
    key = NULL;
  }
  this->clear(FALSE);

  // now, TriggerDB should be empty
  CMPASSERT(this->entries() == 0);
}

//
// -- ResetRecursionCounter()
//
// When triggerDB is allocated from the contextHeap (see func heap()),
// then we must make sure that after each statement the recursion counter 
// of every Trigger object is 0. Called from TriggerDB::cleanupPerStatement().
//
// only used when triggers are allocated from the cntext heap. 
// Currently triggers are allocated from the statement heap.
// See method Trigger::Heap() in file Triggers.h for more details
static void 
ResetRecursionCounter(TriggerList* triggerList)
{
  if (triggerList == NULL)
    return;
  Trigger * trg;
  for (CollIndex i=0; i<triggerList->entries(); i++) 
  {
    trg=(*triggerList)[i];
    trg->resetRecursionCounter();
  }
}

//
// -- TriggerDB::cleanupPerStatement()
//
// Called only in case that trigger-persistence is active. Returns a boolean 
// indicating whether to deallocate triggerDB or not.
// If the triggerDB_ is too big (THRASHOLD exceeded) then clear triggerDB and
// dealocate it. Else, reset the recursion counters, since triggers persist 
// across statements.
//
// only used when triggers are allocated from the cntext heap. 
// Currently triggers are allocated from the statement heap.
// See method Trigger::Heap() in file Triggers.h for more details
NABoolean
TriggerDB::cleanupPerStatement()
{
  CMPASSERT(Trigger::Heap() == CmpCommon::contextHeap());

  if (this != NULL && (entries() >= TRIGGERDB_THRASHOLD)) 
  {
    clearAndDestroy();
    // caller will dealocate triggerDB itself
    return TRUE;
  } 
  else 
  {
    NAHashDictionaryIterator<TableOp,BeforeAndAfterTriggers> iter(*this);

    TableOp * key = NULL;
    BeforeAndAfterTriggers* curr = NULL;

    // iterate over all entries 
    for (Int32 i=0; i < iter.entries(); i++) 
    {
      iter.getNext(key, curr);
      CMPASSERT(curr != NULL);

      //reset recursion counter for all lists
      ResetRecursionCounter(curr->getAfterRowTriggers());
      ResetRecursionCounter(curr->getAfterStatementTriggers());
      ResetRecursionCounter(curr->getBeforeTriggers());

      curr = NULL;
    }
    // Do not deallocate
    return FALSE;
  }
}

//
// -- TriggerDB::print
//
// Debugging aid.
//
// For debugging only
void TriggerDB::print(ostream& os) const 
{
  NAHashDictionaryIterator<TableOp, BeforeAndAfterTriggers> iter (*this) ; 
  TableOp * top;
  BeforeAndAfterTriggers * triggers;

  iter.getNext(top,triggers);
  while (top) 
  {
    os << "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - " 
       << endl;
    top->print(os);
    os << "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - " 
       << endl;
    triggers->print(os, "", "");
    os << endl 
       << "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - " 
       << endl;
    iter.getNext(top, triggers);
  }
}

NABoolean TriggerDB::isHiveTable(QualifiedName& name)
{
   return strcmp(name.getSchemaName(), "HIVE") == 0;
}



//-----------------------------------------------------------------------------
//
// -- class SchemaDB methods

//
// -- SchemaDB::getRIs
//
// TBD
//
// not implemented
RefConstraintList * 
SchemaDB::getRIs(QualifiedName &subjectTable, 
		 ComOperation   operation)
{
  return NULL;
}

