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
* File:         GenRelExeUtil.cpp
* RCS:          $Id: GenRelMisc.cpp,v 1.62 1998/09/07 21:47:43  Exp $
* Description:  ExeUtil operators
*
*
* Created:      10/16/2008
* Modified:     $ $Date: 1998/09/07 21:47:43 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
*
*
******************************************************************************
*/
#define   SQLPARSERGLOBALS_FLAGS
#include "ComOptIncludes.h"
#include "GroupAttr.h"
#include "ItemColRef.h"
#include "RelEnforcer.h"
#include "RelJoin.h"
#include "RelExeUtil.h"
#include "RelMisc.h"
#include "RelSet.h"
#include "RelUpdate.h"
#include "RelScan.h"
#include "RelDCL.h"
#include "PartFunc.h"
#include "Cost.h"
#include "GenExpGenerator.h"
#include "GenResources.h"
#include "ComTdbRoot.h"
#include "ComTdbTuple.h"
#include "ComTdbUnion.h"
#include "ComTdbTupleFlow.h"
#include "ComTdbTranspose.h"
#include "ComTdbSort.h"
#include "ComTdbPackRows.h"
#include "ComTdbDDL.h"
#include "ComTdbExeUtil.h"
#include "ComTdbFirstN.h"
#include "ComTdbStats.h"
#include "ComTdbHbaseAccess.h"
#include "ExplainTuple.h"
#include "ComTdbExplain.h"
#include "SchemaDB.h"
#include "ControlDB.h"
#include "NATable.h"
#include "BindWA.h"
#include "ComTransInfo.h"
#include "DefaultConstants.h"
#include "FragDir.h"
#include "PartInputDataDesc.h"
#include "ExpSqlTupp.h"
#include "sql_buffer.h"
#include "ComQueue.h"
#include "ComSqlId.h"
#include "MVInfo.h"
#include "StmtDDLCreateTable.h"

#include "CmpDDLCatErrorCodes.h"

// need for authorization checks
#include "ComUser.h"
#include "CmpSeabaseDDL.h"
#include "PrivMgrCommands.h"
#include "PrivMgrComponentPrivileges.h"
// end authorization checks

#ifndef HFS2DM
#define HFS2DM
#endif // HFS2DM

#include "ComDefs.h"            // to get common defines (ROUND8)
#include "CmpStatement.h"
#include "ComSmallDefs.h"
#include "sql_buffer_size.h"
#include "TrafDDLdesc.h"

#include "ExpLOB.h"
#include "ExpLobOperV2.h"

#include "ComCextdecs.h"

#include "SqlParserGlobals.h"   // Parser Flags

//#include "HBaseClient_JNI.h"
//

// this comes from GenExplain.cpp (sorry, should have a header file)
TrafDesc * createVirtExplainTableDesc();
void deleteVirtExplainTableDesc(TrafDesc *);

NAString GenBulkloadSampleTableName(const NATable *pTable)
{
  NAString sName;
  if (!pTable)
    return sName;
  sName = pTable->getTableName().getCatalogName();
  sName.append(".")
       .append(pTable->getTableName()
       .getSchemaName())
       .append(".")
       .append(pTable->getTableName().getObjectName())
       .append("_BULK_LOAD_SAMPLE");
  /*char UID[30];
  sprintf(UID,"%ld",pTable->objectUid().castToInt64());
  sName.append(UID);*/
  return sName;
}

short GenericUtilExpr::processOutputRow(Generator * generator,
                                        const Int32 work_atp, 
                                        const Int32 output_row_atp_index,
                                        ex_cri_desc * returnedDesc,
                                        NABoolean noAtpOrIndexChange)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();
  TableDesc *virtTableDesc = getVirtualTableDesc();

  if (!virtTableDesc)
    // this operator produces no outputs (e.g. truncate used with a temp table)
    return 0;
  
  // Assumption (for now): retrievedCols contains ALL columns from
  // the table/index. This is because this operator does
  // not support projection of columns. Add all columns from this table
  // to the map table.
  //
  // The row retrieved from filesystem is returned as the last entry in
  // the returned atp.

  Attributes ** attrs =
    new(generator->wHeap())
    Attributes * [virtTableDesc->getColumnList().entries()];

  for (CollIndex i = 0; i < getVirtualTableDesc()->getColumnList().entries(); i++)
    {
      ItemExpr * col_node
	= (((getVirtualTableDesc()->getColumnList())[i]).getValueDesc())->
	  getItemExpr();

      attrs[i] = (generator->addMapInfo(col_node->getValueId(), 0))->
	getAttr();
    }

  ExpTupleDesc *tupleDesc = 0;
  ULng32 tupleLength = 0;
  expGen->processAttributes(getVirtualTableDesc()->getColumnList().entries(),
			    attrs, ExpTupleDesc::PACKED_FORMAT,
			    tupleLength,
			    work_atp, output_row_atp_index,
			    &tupleDesc, ExpTupleDesc::LONG_FORMAT);

  // delete [] attrs;
  // NADELETEBASIC is used because compiler does not support delete[]
  // operator yet. Should be changed back later when compiler supports
  // it.
  NADELETEBASIC(attrs, generator->wHeap());

  if (NOT noAtpOrIndexChange)
    {
      // The output row will be returned as the last entry of the returned atp.
      // Change the atp and atpindex of the returned values to indicate that.
      expGen->assignAtpAndAtpIndex(getVirtualTableDesc()->getColumnList(),
                                   0, returnedDesc->noTuples()-1);
    }

  return 0;
}
                                    
/////////////////////////////////////////////////////////
//
// ExeUtilProcessVolatileTable::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilProcessVolatileTable::codeGen(Generator * generator)
{
  Space * space = generator->getSpace();

  generator->verifyUpdatableTransMode(NULL, generator->getTransMode(), NULL);

  // remove trailing blanks and append a semicolon, if one is not present.
  char * ddlStmt = NULL;
  Int32 i = strlen(getDDLStmtText());
  while ((i > 0) && (getDDLStmtText()[i-1] == ' '))
    i--;

  if (getDDLStmtText()[i-1] != ';')
    {
      // add a semicolon to the end of str (required by the parser)
      ddlStmt = space->allocateAlignedSpace(i+1);
      strncpy(ddlStmt, getDDLStmtText(), i);
      ddlStmt[i]   = ';' ;
      ddlStmt[i+1] = '\0';
    }
  else
    {
      ddlStmt = space->allocateAlignedSpace(i+1);
      strncpy(ddlStmt, getDDLStmtText(), i);
      ddlStmt[i] = '\0';
    }

  char * volTabName = NULL;
  if (NOT isSchema_)
    {
      volTabName = 
	space->AllocateAndCopyToAlignedSpace(GenGetQualifiedName(volTabName_),
					     0);
    }

  NAString catSchName(generator->currentCmpContext()->schemaDB_->getDefaultSchema().getSchemaNameAsAnsiString());
  CMPASSERT(!catSchName.isNull());		     // not empty
  CMPASSERT(catSchName.first('.') != NA_NPOS);	     // quick test: 'cat.sch'

  char * catSchNameStr = space->allocateAlignedSpace(catSchName.length() + 1);
  strcpy(catSchNameStr, catSchName.data());
  ComTdbProcessVolatileTable * pvt_tdb = new(space)
    ComTdbProcessVolatileTable
    (ddlStmt,
     strlen(ddlStmt),
     getDDLStmtTextCharSet(),
     volTabName, (volTabName ? strlen(volTabName) : 0),
     isCreate_,
     isTable_,
     isIndex_,
     isSchema_,
     catSchNameStr, strlen(catSchNameStr),
     0, 0, // no input expr
     0, 0, // no output expr
     0, 0, // no work cri desc
     (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
     (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
     (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
     (queue_index)getDefault(GEN_DDL_SIZE_UP),
     getDefault(GEN_DDL_NUM_BUFFERS),
     getDefault(GEN_DDL_BUFFER_SIZE));
  generator->initTdbFields(pvt_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(pvt_tdb, 0, 0, generator));
  }

  if (isHbase_)
    {
      pvt_tdb->setHbaseDDL(TRUE);

      if ((NOT getExprNode()->castToStmtDDLNode()->ddlXns()) &&
          (NOT Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
        pvt_tdb->setHbaseDDLNoUserXn(TRUE);
    }

  // no tupps are returned
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, pvt_tdb);

  // Set the transaction flag.
  if ((xnNeeded()) && (! isHbase_))
    {
      generator->setTransactionFlag(-1);
    }

  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilExpr::codeGen()
//
/////////////////////////////////////////////////////////
const char * ExeUtilExpr::getVirtualTableName()
{ return ("EXE_UTIL_EXPR__"); }

TrafDesc *ExeUtilExpr::createVirtualTableDesc()
{
  TrafDesc * table_desc =
    Generator::createVirtualTableDesc(getVirtualTableName(),
				      NULL, // let it decide what heap to use
				      ComTdbExeUtil::getVirtTableNumCols(),
				      ComTdbExeUtil::getVirtTableColumnInfo(),
				      ComTdbExeUtil::getVirtTableNumKeys(),
				      ComTdbExeUtil::getVirtTableKeyInfo());
  return table_desc;
}

/////////////////////////////////////////////////////////
//
// ExeUtilDisplayExplain::codeGen()
//
/////////////////////////////////////////////////////////
const char * ExeUtilDisplayExplain::getVirtualTableName()
{ return "EXE_UTIL_DISPLAY_EXPLAIN__"; }

TrafDesc *ExeUtilDisplayExplain::createVirtualTableDesc()
{
  Lng32 outputRowSize =  
    (Lng32) CmpCommon::getDefaultNumeric(EXPLAIN_OUTPUT_ROW_SIZE);
  ComTdbVirtTableColumnInfo * vtci = NULL;
  
  if (NOT isOptionM())
    {
      vtci = ComTdbExeUtilDisplayExplain::getVirtTableOptionXColumnInfo();
      vtci->length = outputRowSize - 1;
    }
  else
    vtci = ComTdbExeUtilDisplayExplain::getVirtTableColumnInfo();

  TrafDesc * table_desc = 
    Generator::createVirtualTableDesc
    (getVirtualTableName(),
     NULL, // let it decide what heap to use
     ComTdbExeUtilDisplayExplain::getVirtTableNumCols(),
     vtci,
     ComTdbExeUtil::getVirtTableNumKeys(),
     ComTdbExeUtil::getVirtTableKeyInfo());
  return table_desc;
}

short ExeUtilDisplayExplain::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  char * stmtText = getStmtText();

  // remove trailing blanks and append a semicolon, if one is not present.
  char * stmt = NULL;
  CollIndex i = 0;
  if (stmtText)
    {
      i = strlen(stmtText);
      while ((i > 0) && (getStmtText()[i-1] == ' '))
	i--;
      
      if (stmtText[i-1] != ';')
	{
	  // add a semicolon to the end of str (required by the parser)
	  stmt = space->allocateAlignedSpace(i+1+1);
	  strncpy(stmt, stmtText, i);
	  stmt[i]   = ';' ;
	  stmt[i+1] = '\0';
	}
      else
	{
	  stmt = space->allocateAlignedSpace(i+1);
	  strncpy(stmt, stmtText, i);
	  stmt[i] = '\0';
	}
    }

  char * moduleName = NULL;
  char * stmtName = NULL;
  if (moduleName_)
    {
      moduleName = space->allocateAlignedSpace(strlen(moduleName_) +1);
      strcpy(moduleName, moduleName_);
    }

  if (stmtName_)
    {
      stmtName = space->allocateAlignedSpace(strlen(stmtName_) +1);
      strcpy(stmtName, stmtName_);
    }

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  ExplainFunc ef;
  TrafDesc * desc = ef.createVirtualTableDesc();

  TrafDesc * column_desc = desc->tableDesc()->columns_desc;
  Lng32 ij = 0;
  while (ij < EXPLAIN_DESCRIPTION_INDEX)
    {
      column_desc = column_desc->next;
      ij++;
    }

  Lng32 colDescSize =  column_desc->columnsDesc()->length;

  Lng32 outputRowSize =  
    (Lng32) CmpCommon::getDefaultNumeric(EXPLAIN_OUTPUT_ROW_SIZE);

  ComTdbExeUtilDisplayExplain * exe_util_tdb = new(space) 
    ComTdbExeUtilDisplayExplain(
	 stmt,
	 (stmt ? strlen(stmt) : 0),
	 getStmtTextCharSet(),
	 moduleName,
	 stmtName,
	 0, 0, // no input expr
	 0, 0, // no output expr
	 0, 0, // no work cri desc
	 colDescSize,
	 outputRowSize,
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)1024,
	 2, // num buffers
	 32000); // bufferSIze

  exe_util_tdb->setOptionE(isOptionE());
  exe_util_tdb->setOptionF(isOptionF());
  exe_util_tdb->setOptionM(isOptionM());
  exe_util_tdb->setOptionN(isOptionN());
  exe_util_tdb->setOptionC(isOptionC());
  exe_util_tdb->setOptionP(isOptionP());

  generator->initTdbFields(exe_util_tdb);
  
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  // no tupps are returned 
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Set the transaction flag.
  if (xnNeeded())
    {
      generator->setTransactionFlag(-1);
    }
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilDisplayExplainComplex::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilDisplayExplainComplex::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  char * objectName = NULL;
  objectName = 
    space->AllocateAndCopyToAlignedSpace(objectName_, 0);

  char * qry1 = NULL;
  char * qry2 = NULL;
  char * qry3 = NULL;
  char * qry4 = NULL;

  if (qry1_.length() > 0)
    {
      qry1 = space->allocateAlignedSpace(qry1_.length() + 1);
      strcpy(qry1, qry1_.data());
    }

  if (qry2_.length() > 0)
    {
      qry2 = space->allocateAlignedSpace(qry2_.length() + 1);
      strcpy(qry2, qry2_.data());
    }

  if (qry3_.length() > 0)
    {
      qry3 = space->allocateAlignedSpace(qry3_.length() + 1);
      strcpy(qry3, qry3_.data());
    }

  if (qry4_.length() > 0)
    {
      qry4 = space->allocateAlignedSpace(qry4_.length() + 1);
      strcpy(qry4, qry4_.data());
    }

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  ComTdbExeUtilDisplayExplainComplex * exe_util_tdb = new(space) 
    ComTdbExeUtilDisplayExplainComplex(
	 (Lng32)type_,
	 qry1, qry2, qry3, qry4,
	 objectName, strlen(objectName),
	 0, 0, // no input expr
	 0, 0, // no output expr
	 0, 0, // no work cri desc
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)1024,
	 2, // num buffers
	 32000); // bufferSIze

  exe_util_tdb->setIsVolatile(isVolatile_);

  if (getExprNode()->getOperatorType() == REL_DDL)
    {
      DDLExpr * ddlExpr = (DDLExpr*)getExprNode()->castToRelExpr();
      if (ddlExpr->showddlExplain())
	{
	  exe_util_tdb->setIsShowddl(TRUE);
	  
	  exe_util_tdb->setNoLabelStats(ddlExpr->noLabelStats());
	}
    }

  if (getExprNode()->getOperatorType() == REL_EXE_UTIL)
    {
      if ((((ExeUtilExpr*)getExprNode()->castToRelExpr())->getExeUtilType())
	  == ExeUtilExpr::CREATE_TABLE_AS_)
	{
	  ExeUtilCreateTableAs * ctas = 
	    (ExeUtilCreateTableAs*)getExprNode()->castToRelExpr();

	  StmtDDLCreateTable * pNode = 
	    ctas->getExprNode()->castToStmtDDLNode()->castToStmtDDLCreateTable();
	  if (pNode->loadIfExists())
	    exe_util_tdb->setLoadIfExists(TRUE);
	}
    }


  generator->initTdbFields(exe_util_tdb);
  
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  // no tupps are returned 
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Set the transaction flag.
  if (xnNeeded())
    {
      generator->setTransactionFlag(-1);
    }
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilWnrInsert::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilWnrInsert::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  char * tablename = NULL;
  if (getUtilTableDesc() && 
      getUtilTableDesc()->getNATable()->isVolatileTable())
    {
      tablename = space->AllocateAndCopyToAlignedSpace(
        getTableName().getQualifiedNameObj().
          getQualifiedNameAsAnsiString(TRUE), 0);
    }
  else
    {
      tablename =	space->AllocateAndCopyToAlignedSpace(
        GenGetQualifiedName(getTableName()),	0);
    }

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  // no tuples returned from this operator
  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc = givenDesc;

  ComTdb * child_tdb = NULL;
  ExplainTuple *childExplainTuple = NULL;

  // generate code for child tree
  child(0)->codeGen(generator);
  child_tdb = (ComTdb *)(generator->getGenObj());
  childExplainTuple = generator->getExplainTuple();

  ComTdbExeUtilAqrWnrInsert* exe_util_tdb = new(space) 
          ComTdbExeUtilAqrWnrInsert(
               tablename, strlen(tablename),
               NULL,  // no work ex_cri_desc
               0,     // no workAtpIndex
               (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
               (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
               (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
               (queue_index)getDefault(GEN_DDL_SIZE_UP),
               getDefault(GEN_DDL_NUM_BUFFERS),
               getDefault(GEN_DDL_BUFFER_SIZE)
         );

  generator->initTdbFields(exe_util_tdb);

  if (CmpCommon::getDefault(AQR_WNR_LOCK_INSERT_TARGET) == DF_ON)
    exe_util_tdb->lockTarget(true);

  if (child_tdb)
    exe_util_tdb->setChildTdb(child_tdb);

  // no tupps are returned 
  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  if(!generator->explainDisabled() &&
     (CmpCommon::getDefault(AQR_WNR_EXPLAIN_INSERT) == DF_ON)) 
  {
    generator->setExplainTuple(
          addExplainInfo(exe_util_tdb, childExplainTuple, 0, generator));
  }
  
  return 0;
}
/////////////////////////////////////////////////////////
//
// ExeUtilLoadVolatileTable::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilLoadVolatileTable::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  char * insertQuery = NULL;
  if (insertQuery_)
    {
      insertQuery = space->allocateAlignedSpace(insertQuery_->length() + 1);
      strcpy(insertQuery, insertQuery_->data());
    }

  char * updStatsQuery = NULL;
  if (updStatsQuery_)
    {
      updStatsQuery = space->allocateAlignedSpace(updStatsQuery_->length() + 1);
      strcpy(updStatsQuery, updStatsQuery_->data());
    }

  // character set attribute of both insertQuery and updStatsQuery
  Int16 queryCharSet = (Int16)SQLCHARSETCODE_UNKNOWN;
  if (insertQuery != NULL && updStatsQuery != NULL)
  {
    queryCharSet = (Int16)SQLCHARSETCODE_UTF8;
  }

 // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * tablename = 
    space->AllocateAndCopyToAlignedSpace
	(generator->genGetNameAsAnsiNAString(getTableName()), 0);

  // add a REAL CQD to change this.
  Int64 threshold = (ActiveSchemaDB()->getDefaults()).getAsLong(IMPLICIT_UPD_STATS_THRESHOLD);

  ComTdbExeUtil * exe_util_tdb = new(space) 
    ComTdbExeUtilLoadVolatileTable
    (tablename, strlen(tablename),
     insertQuery, 
     updStatsQuery,
     queryCharSet,
     threshold,
     0, 0, // no work cri desc
     (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
     (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
     (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
     (queue_index)getDefault(GEN_DDL_SIZE_UP),
     getDefault(GEN_DDL_NUM_BUFFERS),
     getDefault(GEN_DDL_BUFFER_SIZE));

  generator->initTdbFields(exe_util_tdb);
  
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  // no tupps are returned 
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Set the transaction flag.
  // if (xnNeeded())
  //{
  //  generator->setTransactionFlag(-1);
  //}
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilCleanupVolatileTables::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilCleanupVolatileTables::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * catName = NULL;
  if (NOT catName_.isNull())
    {
      catName = space->allocateAlignedSpace(catName_.length() + 1);
      strcpy(catName, catName_.data());
    }

  ComTdbExeUtilCleanupVolatileTables * exe_util_tdb = new(space) 
    ComTdbExeUtilCleanupVolatileTables(
	 catName, (catName ? strlen(catName) : 0),
	 0, 0, // no work cri desc
	 (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
	 (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
	 (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
	 (queue_index)getDefault(GEN_DDL_SIZE_UP),
	 getDefault(GEN_DDL_NUM_BUFFERS),
	 getDefault(GEN_DDL_BUFFER_SIZE));

  generator->initTdbFields(exe_util_tdb);
  
  if (type_ == ALL_TABLES_IN_ALL_CATS)
    exe_util_tdb->setCleanupAllTables(TRUE);

  if (CmpCommon::getDefault(CSE_CLEANUP_HIVE_TABLES) != DF_OFF)
    exe_util_tdb->setCleanupHiveCSETables(TRUE);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  // no tupps are returned 
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  // Transaction will be started during schema drop processing for
  // each schema.
  generator->setTransactionFlag(0);
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilGetVolatileInfo::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilGetVolatileInfo::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  NABoolean vtCatSpecified = FALSE;
  NAString defCatName = 
    //CmpCommon::context()->sqlSession()->volatileCatalogName();
    ActiveSchemaDB()->getDefaults().getValue(VOLATILE_CATALOG);
  char * param1 = NULL;
  if (NOT defCatName.isNull())
    {
      vtCatSpecified = TRUE;
      param1 = space->allocateAlignedSpace(defCatName.length() + 1);
      strcpy(param1, defCatName.data());
    }
  else
    {
      defCatName = ActiveSchemaDB()->getDefaults().getValue(CATALOG);
      if (NOT defCatName.isNull())
	{
	  param1 = space->allocateAlignedSpace(defCatName.length() + 1);
	  strcpy(param1, defCatName.data());
	}
    }

  char * param2 = NULL;
  if (NOT sessionId_.isNull())
    {
      param2 = space->allocateAlignedSpace(sessionId_.length() + 1);
      strcpy(param2, sessionId_.data());
    }
  
  ComTdbExeUtilGetVolatileInfo * exe_util_tdb = new(space) 
    ComTdbExeUtilGetVolatileInfo(
	 param1, param2,
	 0, 0, // no work cri desc
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)64,
	 2, //getDefault(GEN_DDL_NUM_BUFFERS),
	 32000);  //getDefault(GEN_DDL_BUFFER_SIZE));
 
  generator->initTdbFields(exe_util_tdb);
  
  if (type_ == ALL_SCHEMAS)
    exe_util_tdb->setAllSchemas(TRUE);
  else if (type_ == ALL_TABLES)
    exe_util_tdb->setAllTables(TRUE);
  else if (type_ == ALL_TABLES_IN_A_SESSION)
    exe_util_tdb->setAllTablesInASession(TRUE);

  if (vtCatSpecified)
    exe_util_tdb->setVTCatSpecified(TRUE);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  // no tupps are returned 
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  // Transaction will be started during schema drop processing for
  // each schema.
  generator->setTransactionFlag(0);
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilGetErrorInfo::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilGetErrorInfo::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  ComTdbExeUtilGetErrorInfo * exe_util_tdb = new(space) 
    ComTdbExeUtilGetErrorInfo(
         errType_,
				 errNum_,
				 0, 0, // no work cri desc
				 givenDesc,
				 returnedDesc,
				 (queue_index)8,
				 (queue_index)64,
				 2, //getDefault(GEN_DDL_NUM_BUFFERS),
				 32000);  //getDefault(GEN_DDL_BUFFER_SIZE));
  
  generator->initTdbFields(exe_util_tdb);
  
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  // no tupps are returned 
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  generator->setTransactionFlag(0);
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilCreateTableAs::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilCreateTableAs::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  char * ctQuery = NULL;
  if (ctQuery_.length() > 0)
    {
      ctQuery = space->allocateAlignedSpace(ctQuery_.length() + 1);
      strcpy(ctQuery, ctQuery_.data());
    }

  char * siQuery = NULL;
  if (siQuery_.length() > 0)
    {
      siQuery = space->allocateAlignedSpace(siQuery_.length() + 1);
      strcpy(siQuery, siQuery_.data());
    }

  char * viQuery = NULL;
  if (viQuery_.length() > 0)
    {
      viQuery = space->allocateAlignedSpace(viQuery_.length() + 1);
      strcpy(viQuery, viQuery_.data());
    }

  char * usQuery = 
    space->allocateAlignedSpace(usQuery_.length() + 1);
  strcpy(usQuery, usQuery_.data());

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * tablename = 
    space->AllocateAndCopyToAlignedSpace(generator->genGetNameAsAnsiNAString(getTableName()), 0);
  // add a REAL CQD to change this.
  Int64 threshold = (ActiveSchemaDB()->getDefaults()).getAsLong(IMPLICIT_UPD_STATS_THRESHOLD);

  ComTdbExeUtilCreateTableAs * exe_util_tdb = new(space) 
    ComTdbExeUtilCreateTableAs(
	 tablename, strlen(tablename),
	 ctQuery,
	 siQuery,
	 viQuery,
	 usQuery,
	 threshold,
	 0, 0, // no work cri desc
	 givenDesc,
	 returnedDesc,
	 (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
	 (queue_index)getDefault(GEN_DDL_SIZE_UP),
	 2, //getDefault(GEN_DDL_NUM_BUFFERS),
	 1024); //getDefault(GEN_DDL_BUFFER_SIZE));
  
  exe_util_tdb->setLoadIfExists(loadIfExists_);
  exe_util_tdb->setNoLoad(noLoad_);
  exe_util_tdb->setDeleteData(deleteData_);

  exe_util_tdb->setIsVolatile(isVolatile_);

  generator->initTdbFields(exe_util_tdb);
  
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Set the transaction flag.
  //if (xnNeeded())
   // {
    //  generator->setTransactionFlag(-1);
   // }
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilGetObjectEpochStats::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilGetObjectEpochStats::codeGen(Generator * generator)
{
  ExpGenerator *expGen = generator->getExpGenerator();
  Space *space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * tableName = NULL;
  Lng32 tableNameLen = 0;
  if (isObjectNameSpecified()) {
    tableName =
      space->AllocateAndCopyToAlignedSpace(generator->genGetNameAsAnsiNAString(getTableName()), 0);
    tableNameLen = strlen(tableName);
  }

  ComTdbExeUtilGetObjectEpochStats * exe_util_tdb = new (space)
    ComTdbExeUtilGetObjectEpochStats(tableName, tableNameLen,
                                     getCpu(),
                                     getLocked(),
				     givenDesc,
				     returnedDesc,
				     (queue_index)8,
				     (queue_index)512,
				     2, // getDeault(GEN_DDL_NUM_BUFFERS),
				     32000); // getDefault(GEN_DDL_BUFFER_SIZE));

  generator->initTdbFields(exe_util_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  // Transaction will be started during schema drop processing for
  // each schema.
  generator->setTransactionFlag(0);

  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilGetObjectLockStats::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilGetObjectLockStats::codeGen(Generator * generator)
{
  ExpGenerator *expGen = generator->getExpGenerator();
  Space *space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * tableName = NULL;
  Lng32 tableNameLen = 0;
  if (isObjectNameSpecified()) {
    tableName =
      space->AllocateAndCopyToAlignedSpace(generator->genGetNameAsAnsiNAString(getTableName()), 0);
    tableNameLen = strlen(tableName);
  }

  ComTdbExeUtilGetObjectLockStats * exe_util_tdb = new (space)
    ComTdbExeUtilGetObjectLockStats(tableName, tableNameLen,
                                    getCpu(),
                                    isEntry(),
                                    givenDesc,
                                    returnedDesc,
                                    (queue_index)8,
                                    (queue_index)512,
                                    2, // getDeault(GEN_DDL_NUM_BUFFERS),
                                    32000); // getDefault(GEN_DDL_BUFFER_SIZE));

  generator->initTdbFields(exe_util_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  // Transaction will be started during schema drop processing for
  // each schema.
  generator->setTransactionFlag(0);

  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilGetStatistics::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilGetStatistics::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * stmtName = NULL;
  if (NOT statementName_.isNull())
    {
      stmtName = space->allocateAlignedSpace(statementName_.length() + 1);
      strcpy(stmtName, statementName_.data());
    }
  ComTdbExeUtilGetStatistics * exe_util_tdb;
  if (statsReqType_ == SQLCLI_STATS_REQ_PROCESS_INFO) 
     exe_util_tdb = new (space) ComTdbExeUtilGetProcessStatistics(
	 stmtName,
         statsReqType_,
         statsMergeType_,
         activeQueryNum_,
	 0, 0, // no work cri desc
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)512,
	 2, //getDefault(GEN_DDL_NUM_BUFFERS),
	 32000); //getDefault(GEN_DDL_BUFFER_SIZE));
 
   else {

     // add the host and hdfs path to the tdb. The info will be
     // used after the execution to save the special run-time stats.
     NAString host;
     char * hostPtr = NULL;
     Int32 port = 0;
     char * pathPtr = NULL;


     exe_util_tdb = new(space) 
       ComTdbExeUtilGetStatistics(
	 stmtName,
         statsReqType_,
         statsMergeType_,
         activeQueryNum_,
	 0, 0, // no work cri desc
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)512,
	 2, //getDefault(GEN_DDL_NUM_BUFFERS),
	 32000, //getDefault(GEN_DDL_BUFFER_SIZE));
         hostPtr,
         port,
         pathPtr,
         generator->getQueryHash()
         );
    }
 
  generator->initTdbFields(exe_util_tdb);

  exe_util_tdb->setCompilerStats(compilerStats_);
  exe_util_tdb->setExecutorStats(executorStats_);
  exe_util_tdb->setOtherStats(otherStats_);
  exe_util_tdb->setDetailedStats(detailedStats_);

  exe_util_tdb->setOldFormat(oldFormat_);
  exe_util_tdb->setShortFormat(shortFormat_);
  exe_util_tdb->setTokenizedFormat(tokenizedFormat_);
  exe_util_tdb->setDataUsedStats(dataUsedStats_);
  exe_util_tdb->setSingleLineFormat(singleLineFormat_);
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  // Transaction will be started during schema drop processing for
  // each schema.
  generator->setTransactionFlag(0);
  
  return 0;
}

short ExeUtilGetProcessStatistics::codeGen(Generator * generator)
{
    return ExeUtilGetStatistics::codeGen(generator);
}

/////////////////////////////////////////////////////////
//
// ExeUtilGetUID::codeGen()
//
/////////////////////////////////////////////////////////
const char * ExeUtilGetUID::getVirtualTableName()
{ return "EXE_UTIL_GET_UID__"; }

TrafDesc *ExeUtilGetUID::createVirtualTableDesc()
{
  TrafDesc * table_desc =
    Generator::createVirtualTableDesc(getVirtualTableName(),
				      NULL, // let it decide what heap to use
				      ComTdbExeUtilGetUID::getVirtTableNumCols(),
				      ComTdbExeUtilGetUID::getVirtTableColumnInfo(),
				      ComTdbExeUtilGetUID::getVirtTableNumKeys(),
				      ComTdbExeUtilGetUID::getVirtTableKeyInfo());
  return table_desc;
}

short ExeUtilGetUID::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  ComTdbExeUtilGetUID * exe_util_tdb = new(space) 
    ComTdbExeUtilGetUID(
	 uid_,
	 0, 0, // no work cri desc
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)512,
	 2, //getDefault(GEN_DDL_NUM_BUFFERS),
	 32000); //getDefault(GEN_DDL_BUFFER_SIZE));

  generator->initTdbFields(exe_util_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  // Transaction will be started during schema drop processing for
  // each schema.
  generator->setTransactionFlag(0);
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilGetQID::codeGen()
//
/////////////////////////////////////////////////////////
const char * ExeUtilGetQID::getVirtualTableName()
{ return "EXE_UTIL_GET_QID__"; }

TrafDesc *ExeUtilGetQID::createVirtualTableDesc()
{
  TrafDesc * table_desc =
    Generator::createVirtualTableDesc(getVirtualTableName(),
				      NULL, // let it decide what heap to use
				      ComTdbExeUtilGetQID::getVirtTableNumCols(),
				      ComTdbExeUtilGetQID::getVirtTableColumnInfo(),
				      ComTdbExeUtilGetQID::getVirtTableNumKeys(),
				      ComTdbExeUtilGetQID::getVirtTableKeyInfo());
  return table_desc;
}

short ExeUtilGetQID::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * stmt_name =
    space->AllocateAndCopyToAlignedSpace(statement_, 0);

  ComTdbExeUtilGetQID * exe_util_tdb = new(space) 
    ComTdbExeUtilGetQID(
                        stmt_name,
                        0, 0, // no work cri desc
                        givenDesc,
                        returnedDesc,
                        (queue_index)8,
                        (queue_index)512,
                        2, //getDefault(GEN_DDL_NUM_BUFFERS),
                        32000); //getDefault(GEN_DDL_BUFFER_SIZE));
  
  generator->initTdbFields(exe_util_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilPopulateInMemStats::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilPopulateInMemStats::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  NAString inMemHistogramsStr;
  inMemHistogramsStr = CmpCommon::context()->sqlSession()->volatileCatalogName();
  inMemHistogramsStr += ".";
  inMemHistogramsStr += CmpCommon::context()->sqlSession()->volatileSchemaName();
  inMemHistogramsStr += ".";
  inMemHistogramsStr += "HISTOGRAMS";

  NAString inMemHistintsStr;
  inMemHistintsStr = CmpCommon::context()->sqlSession()->volatileCatalogName();
  inMemHistintsStr += ".";
  inMemHistintsStr += CmpCommon::context()->sqlSession()->volatileSchemaName();
  inMemHistintsStr += ".";
  inMemHistintsStr += "HISTOGRAM_INTERVALS";

  NAString sourceHistogramsStr;
  sourceHistogramsStr = sourceStatsSchemaName_.getSchemaNameAsAnsiString();
  sourceHistogramsStr += ".";
  sourceHistogramsStr += "HISTOGRAMS";

  NAString sourceHistintsStr;
  sourceHistintsStr = sourceStatsSchemaName_.getSchemaNameAsAnsiString();
  sourceHistintsStr += ".";
  sourceHistintsStr += "HISTOGRAM_INTERVALS";

  char * inMemHistogramsTableName  = NULL;
  char * inMemHistintsTableName = NULL;
  char * sourceTableCatName = NULL;
  char * sourceTableSchName = NULL;
  char * sourceTableObjName = NULL;
  char * sourceHistogramsTableName = NULL;
  char * sourceHistintsTableName   = NULL;

  inMemHistogramsTableName =
    space->AllocateAndCopyToAlignedSpace(
	 inMemHistogramsStr, 0);
  inMemHistintsTableName =
    space->AllocateAndCopyToAlignedSpace(
	 inMemHistintsStr, 0);

  sourceTableCatName = 
    space->AllocateAndCopyToAlignedSpace(
	 sourceTableName_.getQualifiedNameObj().getCatalogName(), 0);
  sourceTableSchName = 
    space->AllocateAndCopyToAlignedSpace(
	 sourceTableName_.getQualifiedNameObj().getSchemaName(), 0);
  sourceTableObjName = 
    space->AllocateAndCopyToAlignedSpace(
	 sourceTableName_.getQualifiedNameObj().getObjectName(), 0);
  sourceHistogramsTableName =
    space->AllocateAndCopyToAlignedSpace(
	 sourceHistogramsStr, 0);
  sourceHistintsTableName =
    space->AllocateAndCopyToAlignedSpace(
	 sourceHistintsStr, 0);

  ComTdbExeUtilPopulateInMemStats * exe_util_tdb = new(space) 
    ComTdbExeUtilPopulateInMemStats(
	 uid_,
	 inMemHistogramsTableName,
	 inMemHistintsTableName,
	 sourceTableCatName,
	 sourceTableSchName,
	 sourceTableObjName,
	 sourceHistogramsTableName,
	 sourceHistintsTableName,
	 0, 0, // no work cri desc
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)512,
	 2, //getDefault(GEN_DDL_NUM_BUFFERS),
	 32000); //getDefault(GEN_DDL_BUFFER_SIZE));

  generator->initTdbFields(exe_util_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  // Transaction will be started during schema drop processing for
  // each schema.
  generator->setTransactionFlag(0);
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilGetMetadataInfo::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilGetMetadataInfo::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  if ((CmpCommon::context()->isUninitializedSeabase()) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      if (CmpCommon::context()->uninitializedSeabaseErrNum() == -TRAF_HBASE_ACCESS_ERROR)
        *CmpCommon::diags() << DgSqlCode(CmpCommon::context()->uninitializedSeabaseErrNum())
                            << DgInt0(CmpCommon::context()->hbaseErrNum())
                            << DgString0(CmpCommon::context()->hbaseErrStr());
      else
        *CmpCommon::diags() << DgSqlCode(CmpCommon::context()->uninitializedSeabaseErrNum());
      
      GenExit();
    }

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc, TRUE);
  if (rc)
    {
      return -1;
    }

  // if pattern is specified, add it as a LIKE predicate to selectionPred.
  // "\" is the escape character.
  if (NOT pattern_.isNull())
    {
      ItemExpr * colNode
	= (((getVirtualTableDesc()->getColumnList())[0]).getValueDesc())->
	  getItemExpr();

      ItemExpr * patternNode =
	new(generator->wHeap()) ConstValue(pattern_.data());

      ItemExpr * escapeNode =
	new(generator->wHeap()) ConstValue("\\");

      ItemExpr * patternTree = 
	new(generator->wHeap()) Like(colNode, patternNode, escapeNode);
      
      patternTree->bindNode(generator->getBindWA());
      
      selectionPred().insert(patternTree->getValueId());
    }

  ex_expr *scanExpr = NULL;
  // generate tuple selection expression, if present
  if (NOT selectionPred().isEmpty())
    {
      ItemExpr* pred = selectionPred().rebuildExprTree(ITM_AND,TRUE,TRUE);
      expGen->generateExpr(pred->getValueId(),ex_expr::exp_SCAN_PRED,&scanExpr);
    }
  
  // The output row will be returned as the last entry of the returned atp.
  // Change the atp and atpindex of the returned values to indicate that.
  expGen->assignAtpAndAtpIndex(getVirtualTableDesc()->getColumnList(),
			       0, returnedDesc->noTuples()-1);

  struct QueryInfoStruct
  {
    const char * ausStr;
    const char * infoType;
    const char * iofStr;
    const char * objectType;
    Lng32   version;
    Lng32   maxParts;
    Lng32   groupBy;
    Lng32   orderBy;
    ComTdbExeUtilGetMetadataInfo::QueryType queryType;
  };

  static const QueryInfoStruct qis[] =
  {
    // AUSStr   InfoType     IOFStr   ObjectType  Version MaxParts  GroupBy OrderBy QueryType
    //==================================================================================================================================

    {  "USER",   "CATALOGS",  "",      "",         0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::CATALOGS_ },
    {  "SYSTEM", "CATALOGS",  "",      "",         0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::CATALOGS_ },
    {  "ALL",    "CATALOGS",  "",      "",         0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::CATALOGS_ },

    {  "USER",   "SCHEMAS",   "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_ },
    {  "SYSTEM", "SCHEMAS",   "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_ },
    {  "ALL",    "SCHEMAS",   "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_ },

    {  "USER",   "SEQUENCES", "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_SCHEMA_ },
    {  "ALL",    "SEQUENCES", "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_SCHEMA_ },

    {  "USER",   "TABLES",    "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_ },
    {  "SYSTEM", "TABLES",    "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_ },
    {  "ALL",    "TABLES",    "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_ },

    {  "ALL",    "COMPONENTS",  "",    "",          0,     0,        0,      0,      ComTdbExeUtilGetMetadataInfo::COMPONENTS_ },
    {  "ALL",    "PRIVILEGES",  "ON",  "COMPONENT", 0,     0,        0,      0,      ComTdbExeUtilGetMetadataInfo::COMPONENT_PRIVILEGES_ },
    {  "USER",   "PRIVILEGES",  "ON",  "COMPONENT", 0,     0,        0,      0,      ComTdbExeUtilGetMetadataInfo::COMPONENT_PRIVILEGES_ },    

    {  "USER",   "AROLES",   "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::ACTIVE_ROLES_ },
    {  "SYSTEM", "AROLES",   "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::ACTIVE_ROLES_ },
    {  "ALL",    "AROLES",   "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::ACTIVE_ROLES_ },
    {  "USER",   "ROLES",    "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::ROLES_ },
    {  "SYSTEM", "ROLES",    "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::ROLES_ },
    {  "ALL",    "ROLES",    "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::ROLES_ },
  
    {  "USER",   "TENANTS",  "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::TENANTS_ },
    {  "SYSTEM", "TENANTS",  "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::TENANTS_ },
    {  "ALL",    "TENANTS",  "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::TENANTS_ },
    {  "USER",   "TENANTS",  "ON",     "RGROUP",    0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::TENANTS_ON_RGROUP_ },
    {  "SYSTEM", "TENANTS",  "ON",     "RGROUP",    0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::TENANTS_ON_RGROUP_ },
    {  "ALL",    "TENANTS",  "ON",     "RGROUP",    0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::TENANTS_ON_RGROUP_ },

    {  "USER",   "GROUPS",   "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::GROUPS_ },
    {  "SYSTEM", "GROUPS",   "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::GROUPS_ },
    {  "ALL",    "GROUPS",   "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::GROUPS_ },
    {  "USER",   "GROUPS",   "FOR",     "TENANT",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_ },
    {  "SYSTEM", "GROUPS",   "FOR",     "TENANT",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_ },
    {  "ALL",    "GROUPS",   "FOR",     "TENANT",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_ },
    {  "USER",   "GROUPS",   "FOR",     "USER",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_USER_ },
    {  "SYSTEM", "GROUPS",   "FOR",     "USER",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_USER_ },
    {  "ALL",    "GROUPS",   "FOR",     "USER",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_USER_ },

    {  "USER",   "NODES",    "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::NODES_ },
    {  "SYSTEM", "NODES",    "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::NODES_ },
    {  "ALL",    "NODES",    "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::NODES_ },
    {  "USER",   "NODES",    "FOR",     "TENANT",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::NODES_FOR_TENANT_ },
    {  "SYSTEM", "NODES",    "FOR",     "TENANT",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::NODES_FOR_TENANT_ },
    {  "ALL",    "NODES",    "FOR",     "TENANT",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::NODES_FOR_TENANT_ },
    {  "USER",   "NODES",    "IN",      "RGROUP",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::NODES_IN_RGROUP_ },
    {  "SYSTEM", "NODES",    "IN",      "RGROUP",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::NODES_IN_RGROUP_ },
    {  "ALL",    "NODES",    "IN",      "RGROUP",   0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::NODES_IN_RGROUP_ },
    {  "USER",   "RGROUPS",  "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::RGROUPS_ },
    {  "SYSTEM", "RGROUPS",  "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::RGROUPS_ },
    {  "ALL",    "RGROUPS",  "",        "",         0,      0,        0,      0,     ComTdbExeUtilGetMetadataInfo::RGROUPS_ },

    {  "USER",   "USERS",       "",    "",         0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::USERS_ },
    {  "SYSTEM", "USERS",       "",    "",         0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::USERS_ },
    {  "ALL",    "USERS",       "",    "",         0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::USERS_ },
  
    {  "USER",   "INDEXES",   "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_IN_SCHEMA_ },
    {  "USER",   "VIEWS",     "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_IN_SCHEMA_ },
    {  "USER",   "LIBRARIES", "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_IN_SCHEMA_ },
    {  "USER",   "FUNCTIONS", "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::FUNCTIONS_IN_SCHEMA_ },
    {  "USER",   "TABLE_FUNCTIONS", "","",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_IN_SCHEMA_ },
    {  "USER",   "PACKAGES",  "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PACKAGES_IN_SCHEMA_ },
    {  "USER",   "PROCEDURES","",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_IN_SCHEMA_ },
    {  "USER",   "OBJECTS",   "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_ },
    {  "USER",   "TRIGGERS",  "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGGERS_IN_SCHEMA_ },    
//    {  "USER",   "MVS",       "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_IN_SCHEMA_ },
//    {  "USER",   "MVGROUPS",  "",      "",         0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVGROUPS_IN_SCHEMA_ },
//    {  "USER",   "SYNONYMS",  "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYNONYMS_IN_SCHEMA_ },

    {  "ALL",    "INDEXES",   "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_IN_SCHEMA_ },
    {  "ALL",    "VIEWS",     "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_IN_SCHEMA_ },
    {  "ALL",    "LIBRARIES", "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_IN_SCHEMA_ },
    {  "ALL",    "PACKAGES",  "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PACKAGES_IN_SCHEMA_ },
    {  "ALL",    "PROCEDURES","",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_IN_SCHEMA_ },
    {  "ALL",    "OBJECTS",   "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_ },
    {  "ALL",    "TRIGGERS",  "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGGERS_IN_SCHEMA_ },
//    {  "ALL",    "MVS",       "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_IN_SCHEMA_ },
//    {  "ALL",    "MVGROUPS",  "",      "",         0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVGROUPS_IN_SCHEMA_ },
//    {  "ALL",    "SYNONYMS",  "",      "",         1,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYNONYMS_IN_SCHEMA_ },

    {  "USER",   "SCHEMAS",   "IN",    "CATALOG",  1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_ },
    {  "SYSTEM", "SCHEMAS",   "IN",    "CATALOG",  1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_ },
    {  "ALL",    "SCHEMAS",   "IN",    "CATALOG",  1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_ },

    {  "ALL",    "VIEWS",     "IN",    "CATALOG",         1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_IN_CATALOG_ },
    {  "USER",   "VIEWS",     "IN",    "CATALOG",         1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_IN_CATALOG_ },

    {  "USER",   "SEQUENCES", "IN",    "CATALOG",  1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_CATALOG_ },
    {  "ALL",    "SEQUENCES", "IN",    "CATALOG",  1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_CATALOG_ },

    {  "USER",   "TABLES",    "IN",    "CATALOG",  1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_CATALOG_ },
    {  "USER",   "OBJECTS",   "IN",    "CATALOG",  1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_CATALOG_ },

    {  "USER",   "HIVE_REG_TABLES","IN", "CATALOG",  1,      1,        0,      0,    ComTdbExeUtilGetMetadataInfo::HIVE_REG_TABLES_IN_CATALOG_ },
    {  "USER",   "HIVE_REG_VIEWS", "IN", "CATALOG",  1,      1,        0,      0,    ComTdbExeUtilGetMetadataInfo::HIVE_REG_VIEWS_IN_CATALOG_ },
    {  "USER",   "HIVE_REG_SCHEMAS", "IN", "CATALOG",  1,      1,        0,      0,  ComTdbExeUtilGetMetadataInfo::HIVE_REG_SCHEMAS_IN_CATALOG_ },
    {  "USER",   "HIVE_REG_OBJECTS", "IN", "CATALOG",  1,      1,        0,      0,  ComTdbExeUtilGetMetadataInfo::HIVE_REG_OBJECTS_IN_CATALOG_ },
    {  "USER",   "HIVE_EXT_TABLES","IN", "CATALOG",  1,      1,        0,      0,    ComTdbExeUtilGetMetadataInfo::HIVE_EXT_TABLES_IN_CATALOG_ },
    {  "USER",   "HBASE_REG_TABLES","IN", "CATALOG",  1,      1,        0,      0,    ComTdbExeUtilGetMetadataInfo::HBASE_REG_TABLES_IN_CATALOG_ },
//    {  "ALL",    "INVALID_VIEWS",   "IN",      "CATALOG",         1,      1,        0,      0,      ComTdbExeUtilGetMetadataInfo::INVALID_VIEWS_IN_CATALOG_ },
    {  "USER",   "HBASE_MAP_TABLES","IN", "CATALOG",  1,      1,        0,      0,    ComTdbExeUtilGetMetadataInfo::HBASE_MAP_TABLES_IN_CATALOG_ },
    {  "USER",   "HBASE_MAP_TABLES","", "",  1,      0,        0,      0,            ComTdbExeUtilGetMetadataInfo::HBASE_MAP_TABLES_IN_CATALOG_ },

    {  "USER",   "TABLES",    "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_ },
    {  "SYSTEM", "TABLES",    "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_ },
    {  "ALL",    "TABLES",    "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_ },

    {  "ALL",    "SEQUENCES",    "IN", "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_SCHEMA_ },
    {  "ALL",    "PRIVILEGES",   "ON", "SEQUENCE", 1,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SEQUENCE_  },
    {  "USER",   "SEQUENCES",    "IN", "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_SCHEMA_ },
    {  "USER",   "PRIVILEGES",   "ON", "SEQUENCE", 1,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SEQUENCE_  },

    {  "USER",   "OBJECTS",   "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_ },
    {  "SYSTEM", "OBJECTS",   "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_ },
    {  "ALL",    "OBJECTS",   "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_ },
//    {  "ALL",   "INVALID_VIEWS",   "IN",      "SCHEMA",         1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::INVALID_VIEWS_IN_SCHEMA_ },

    {  "USER",   "INDEXES",   "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_IN_SCHEMA_ },
    {  "USER",   "VIEWS",     "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_IN_SCHEMA_ },
    {  "USER",   "PRIVILEGES","ON",    "SCHEMA",   0,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SCHEMA_ },
    {  "USER",   "LIBRARIES", "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_IN_SCHEMA_ },
    {  "USER",   "PACKAGES",  "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::PACKAGES_IN_SCHEMA_ },
    {  "USER",   "PROCEDURES","IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_IN_SCHEMA_ },
    {  "USER",   "FUNCTIONS", "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::FUNCTIONS_IN_SCHEMA_ },
    {  "USER",   "TRIGGERS",  "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGGERS_IN_SCHEMA_ },
    {  "USER",   "TABLE_FUNCTIONS", "IN","SCHEMA",  1,     2,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_IN_SCHEMA_ },
//    {  "USER",   "MVS",       "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_IN_SCHEMA_ },
//    {  "USER",   "MVGROUPS",  "IN",    "SCHEMA",   0,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVGROUPS_IN_SCHEMA_ },
//    {  "USER",   "SYNONYMS",  "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYNONYMS_IN_SCHEMA_ },

    {  "ALL",    "INDEXES",   "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_IN_SCHEMA_ },
    {  "ALL",    "VIEWS",     "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_IN_SCHEMA_ },
    {  "ALL",    "LIBRARIES", "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_IN_SCHEMA_ },
    {  "ALL",    "PACKAGES",  "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::PACKAGES_IN_SCHEMA_ },
    {  "ALL",    "PROCEDURES","IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_IN_SCHEMA_ },
    {  "ALL",    "FUNCTIONS", "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::FUNCTIONS_IN_SCHEMA_ },
    {  "ALL",    "TRIGGERS",  "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGGERS_IN_SCHEMA_ },
    {  "ALL",    "TABLE_FUNCTIONS", "IN","SCHEMA", 1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_IN_SCHEMA_ },
    {  "ALL",    "PRIVILEGES", "ON", "LIBRARY",    1,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_LIBRARY_ },
    {  "ALL",    "PRIVILEGES", "ON", "PROCEDURE",  1,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_ROUTINE_ },
    {  "ALL",    "PRIVILEGES", "ON", "ROUTINE",    1,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_ROUTINE_ },
//    {  "ALL",    "MVS",       "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_IN_SCHEMA_ },
//    {  "ALL",    "MVGROUPS",  "IN",    "SCHEMA",   0,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVGROUPS_IN_SCHEMA_ },
//    {  "ALL",    "SYNONYMS",  "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYNONYMS_IN_SCHEMA_ },
//    {  "IUDLOG",    "TABLES",  "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::IUDLOG_TABLES_IN_SCHEMA_ },
//    {  "RANGELOG",    "TABLES",  "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::RANGELOG_TABLES_IN_SCHEMA_ },
//    {  "TRIGTEMP",    "TABLES",  "IN",    "SCHEMA",   1,      2,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGTEMP_TABLES_IN_SCHEMA_ },

    {  "USER",   "INDEXES",   "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_ON_TABLE_ },
    {  "USER",   "VIEWS",     "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_ },
    {  "USER",   "OBJECTS",   "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_ON_TABLE_ },
//    {  "USER",   "INDEXES",   "ON",    "MV",       0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_ON_MV_ },
//    {  "USER",   "MVS",       "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_ON_TABLE_ },
//    {  "USER",   "MVGROUPS",  "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVGROUPS_ON_TABLE_ },
//    {  "USER",   "SYNONYMS",  "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYNONYMS_ON_TABLE_ },

    {  "ALL",    "INDEXES",   "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_ON_TABLE_ },
    {  "ALL",    "VIEWS",     "ON",    "TABLE",    0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_ },
    {  "ALL",    "OBJECTS",   "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_ON_TABLE_ },
    {  "ALL",    "PRIVILEGES","ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_TABLE_ },
    {  "ALL",    "PRIVILEGES","ON",    "VIEW",     0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_VIEW_ },
//    {  "ALL",    "INDEXES",   "ON",    "MV",       0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_ON_MV_ },
//    {  "ALL",    "MVS",       "ON",    "TABLE",    0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::MVS_ON_TABLE_ },
//    {  "ALL",    "MVGROUPS",  "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVGROUPS_ON_TABLE_ },
//    {  "ALL",    "SYNONYMS",  "ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYNONYMS_ON_TABLE_ },
//
//    {  "IUDLOG",    "TABLES",   "ON",  "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::IUDLOG_TABLE_ON_TABLE_ },
//    {  "RANGELOG",  "TABLES",   "ON",  "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::RANGELOG_TABLE_ON_TABLE_ },
//    {  "TRIGTEMP",  "TABLES",   "ON",  "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGTEMP_TABLE_ON_TABLE_ },
//    {  "IUDLOG",    "TABLES",   "ON",  "MV",       0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::IUDLOG_TABLE_ON_MV_ },
//    {  "RANGELOG",  "TABLES",   "ON",  "MV",       0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::RANGELOG_TABLE_ON_MV_ },
//    {  "TRIGTEMP",  "TABLES",   "ON",  "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGTEMP_TABLE_ON_MV_ },
//    {  "ALL",    "PRIVILEGES","ON",    "MV",       0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_MV_ },
//    {  "USER",   "MVS",       "ON",    "MV",       0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_ON_MV_ },
//    {  "ALL",    "MVS",       "ON",    "MV",       0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::MVS_ON_MV_ },


//    {  "USER",   "PARTITIONS","FOR",   "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PARTITIONS_FOR_TABLE_ },
//    {  "USER",   "PARTITIONS","ON",    "TABLE",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PARTITIONS_FOR_TABLE_ },
//    {  "USER",   "PARTITIONS","FOR",   "INDEX",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PARTITIONS_FOR_INDEX_ },
//    {  "USER",   "PARTITIONS","ON",    "INDEX",    0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PARTITIONS_FOR_INDEX_ },

    {  "USER",   "VIEWS",     "ON",    "VIEW",     0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_ },
    {  "ALL",    "VIEWS",     "ON",    "VIEW",     0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_ },

    {  "USER",   "TABLES",    "IN",    "VIEW",     0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_ },
    {  "USER",   "VIEWS",     "IN",    "VIEW",     0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_ },
    {  "USER",   "OBJECTS",   "IN",    "VIEW",     0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_ },

    {  "ALL",    "TABLES",    "IN",    "VIEW",     0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_ },
    {  "ALL",    "VIEWS",     "IN",    "VIEW",     0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_ },
    {  "ALL",    "OBJECTS",   "IN",    "VIEW",     0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_ },

//    {  "USER",   "TABLES",    "IN",    "MV",       0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_MV_ },
//    {  "USER",   "MVS",       "IN",    "MV",       0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_IN_MV_ },
//    {  "USER",   "OBJECTS",   "IN",    "MV",       0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_MV_ },

//    {  "ALL",    "TABLES",    "IN",    "MV",       0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::TABLES_IN_MV_ },
//    {  "ALL",    "MVS",       "IN",    "MV",       0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::MVS_IN_MV_ },
//    {  "ALL",    "OBJECTS",   "IN",    "MV",       0,      3,        1,      1,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_MV_ },

    {  "ALL",    "USERS",     "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::USERS_FOR_ROLE_ },
    {  "ALL",    "ROLES",     "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ROLES_FOR_ROLE_ },

    {  "SYSTEM", "USERS",     "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::USERS_FOR_ROLE_ },
    {  "SYSTEM", "ROLES",     "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ROLES_FOR_ROLE_ },

    {  "USER",   "USERS",     "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::USERS_FOR_ROLE_ },
    {  "USER",   "ROLES",     "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ROLES_FOR_ROLE_ },

    {  "ALL",    "PROCEDURES","FOR",   "LIBRARY",    0,    3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_LIBRARY_ },
    {  "USER",   "PROCEDURES","FOR",   "LIBRARY",    0,    3,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_LIBRARY_ },
    {  "ALL",    "FUNCTIONS", "FOR",   "LIBRARY",    0,    3,        0,      0,      ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_LIBRARY_ },
    {  "ALL",    "TABLE_FUNCTIONS","FOR","LIBRARY",  0,    3,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_LIBRARY_ },
 
    {  "ALL",    "INDEXES",   "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_USER_ },
    {  "ALL",    "INDEXES",   "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_ROLE_ },
    {  "ALL",    "LIBRARIES", "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_USER_ },
    {  "ALL",    "LIBRARIES", "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_ROLE_ },
    {  "ALL",    "OBJECTS",   "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_FOR_USER_ },
    {  "ALL",    "PACKAGES",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PACKAGES_FOR_USER_ },
    {  "ALL",    "PRIVILEGES","FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_USER_ },
    {  "ALL",    "PRIVILEGES","FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_ROLE_ },
    {  "ALL",    "PROCEDURES","FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_USER_ },
    {  "ALL",    "PROCEDURES","FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_ROLE_ },
    {  "ALL",    "ROLES",     "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_ },
    {  "ALL",    "ROLES",     "FOR",   "GROUP",    0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_ },
    {  "ALL",    "SCHEMAS",   "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_USER_ },
    {  "ALL",    "SCHEMAS",   "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_ROLE_ },
    {  "ALL",    "TABLES",    "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_ },
    {  "ALL",    "TABLES",    "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_ },
    {  "ALL",    "VIEWS",     "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_USER_ },
    {  "ALL",    "VIEWS",     "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_ROLE_ },
//    {  "ALL",    "MVS",       "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_FOR_USER_ },
//    {  "ALL",    "MVGROUPS",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVGROUPS_FOR_USER_ },
//    {  "ALL",    "SYNONYMS",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYNONYMS_FOR_USER_ },
//    {  "ALL",    "TRIGGERS",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGGERS_FOR_USER_ },

    {  "SYSTEM", "ROLES",     "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_ },
    {  "SYSTEM", "ROLES",     "FOR",   "GROUP",    0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_ },
    {  "SYSTEM", "SCHEMAS",   "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_USER_ },
    {  "SYSTEM", "SCHEMAS",   "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_ROLE_ },
    {  "SYSTEM", "TABLES",    "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_ },
    {  "SYSTEM", "TABLES",    "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_ },
    {  "SYSTEM", "INDEXES",   "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_USER_ },
    {  "SYSTEM", "INDEXES",   "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_ROLE_ },
    {  "SYSTEM", "LIBRARIES", "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_USER_ },
    {  "SYSTEM", "LIBRARIES", "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_ROLE_ },
    {  "SYSTEM", "OBJECTS",   "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_FOR_USER_ },
    {  "SYSTEM", "PROCEDURES","FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_USER_ },
    {  "SYSTEM", "PROCEDURES","FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_ROLE_ },
//  {  "SYSTEM", "MVS",       "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_FOR_USER_ },
//  {  "SYSTEM", "MVGROUPS",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVGROUPS_FOR_USER_ },
//  {  "SYSTEM", "SYNONYMS",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYNONYMS_FOR_USER_ },
//  {  "SYSTEM", "TRIGGERS",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGGERS_FOR_USER_ },
//  {  "SYSTEM", "VIEWS",     "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_USER_ },

    {  "USER",   "FUNCTIONS", "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_USER_ },
    {  "USER",   "FUNCTIONS", "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_ROLE_ },
    {  "USER",   "INDEXES",   "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_USER_ },
    {  "USER",   "INDEXES",   "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_ROLE_ },
    {  "USER",   "LIBRARIES", "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_USER_ },
    {  "USER",   "LIBRARIES", "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_ROLE_ },
    {  "USER",   "OBJECTS",   "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_FOR_USER_ },
    {  "USER",   "PROCEDURES","FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_USER_ },
    {  "USER",   "PRIVILEGES","FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_USER_ },
    {  "USER",   "PRIVILEGES","FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_ROLE_ },
    {  "USER",   "PROCEDURES","FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_USER_ },
    {  "USER",   "PROCEDURES","FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_ROLE_ },
    {  "USER",   "ROLES",     "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_ },
    {  "USER",   "ROLES",     "FOR",   "GROUP",    0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_ },
    {  "USER",   "SCHEMAS",   "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_USER_ },
    {  "USER",   "SCHEMAS",   "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_ROLE_ },
    {  "USER",   "TABLES",    "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_ },
    {  "USER",   "TABLES",    "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_ },
    {  "USER",   "TABLE_FUNCTIONS", "FOR", "USER", 0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_USER_ },
    {  "USER",   "TABLE_FUNCTIONS", "FOR", "ROLE", 0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_ROLE_ },
    {  "USER",   "VIEWS",     "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_USER_ },
    {  "USER",   "VIEWS",     "FOR",   "ROLE",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_ROLE_ },
//    {  "USER",   "MVS",       "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVS_FOR_USER_ },
//    {  "USER",   "MVGROUPS",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MVGROUPS_FOR_USER_ },
//    {  "USER",   "SYNONYMS",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYNONYMS_FOR_USER_ },
//    {  "USER",   "TRIGGERS",  "FOR",   "USER",     0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRIGGERS_FOR_USER_ },

    {  "USER",     "HBASE_OBJECTS",     "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::HBASE_OBJECTS_ },
    {  "ALL",      "HBASE_OBJECTS",     "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::HBASE_OBJECTS_ },
    {  "SYSTEM",   "HBASE_OBJECTS",     "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::HBASE_OBJECTS_ },
    {  "EXTERNAL", "HBASE_OBJECTS",     "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::HBASE_OBJECTS_ },
    {  "USER",     "MONARCH_OBJECTS",   "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MONARCH_OBJECTS_ },
    {  "ALL",      "MONARCH_OBJECTS",   "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MONARCH_OBJECTS_ },
    {  "SYSTEM",   "MONARCH_OBJECTS",   "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MONARCH_OBJECTS_ },
    {  "EXTERNAL", "MONARCH_OBJECTS",   "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::MONARCH_OBJECTS_ },
    {  "USER",     "BIGTABLE_OBJECTS",  "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::BIGTABLE_OBJECTS_ },
    {  "ALL",      "BIGTABLE_OBJECTS",  "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::BIGTABLE_OBJECTS_ },
    {  "SYSTEM",   "BIGTABLE_OBJECTS",  "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::BIGTABLE_OBJECTS_ },
    {  "EXTERNAL", "BIGTABLE_OBJECTS",  "",   "",  0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::BIGTABLE_OBJECTS_ },

    {  "TRAFODION", "NAMESPACES",        "",   "",           0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::TRAFODION_NAMESPACES_ },
    {  "EXTERNAL",  "NAMESPACES",        "",   "",           0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::EXTERNAL_NAMESPACES_ },
    {  "SYSTEM",    "NAMESPACES",        "",   "",           0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::SYSTEM_NAMESPACES_ },
    {  "ALL",       "NAMESPACES",        "",   "",           0,      0,        0,      0,      ComTdbExeUtilGetMetadataInfo::ALL_NAMESPACES_ },
    {  "ALL",       "OBJECTS",           "IN", "NAMESPACE",  0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_NAMESPACE_ },

    {  "TRAFODION", "CONFIG",            "FOR","NAMESPACE",  0,      3,        0,      0,      ComTdbExeUtilGetMetadataInfo::NAMESPACE_CONFIG_ },
    {  "USER",     "MEMBERS",            "IN","GROUP",      0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::USERS_FOR_MEMBERS_ },
    {  "SYSTEM",   "MEMBERS",            "IN","GROUP",      0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::USERS_FOR_MEMBERS_ },
    {  "ALL",      "MEMBERS",            "IN","GROUP",      0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::USERS_FOR_MEMBERS_ },
    {  "ALL",      "PWDPOLICY",          "",   "",   0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::PASSWORD_POLICY_ },
    {  "USER",     "PWDPOLICY",          "",   "",   0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::PASSWORD_POLICY_ },
    {  "SYSTEM",   "PWDPOLICY",          "",   "",   0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::PASSWORD_POLICY_ },
    {  "ALL",      "AUTHENTICATION",     "",   "",   0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::AUTH_TYPE_ },
    {  "USER",     "AUTHENTICATION",     "",   "",   0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::AUTH_TYPE_ },
    {  "SYSTEM",   "AUTHENTICATION",     "",   "",   0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::AUTH_TYPE_ },
    {  "ALL",      "AUTHORIZATION",      "",   "",   0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::AUTHZ_STATUS_ },
    {  "USER",     "AUTHORIZATION",     "",   "",   0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::AUTHZ_STATUS_ },
    {  "SYSTEM",   "AUTHORIZATION",      "",   "",   0,      0,      0,      0,      ComTdbExeUtilGetMetadataInfo::AUTHZ_STATUS_ }

//==================================================================================================================================
   // AUSStr   InfoType     IOFStr   ObjectType  Version MaxParts  GroupBy OrderBy QueryType
  };

  ausStr_.toUpper();
  iofStr_.toUpper();
  infoType_.toUpper();
  objectType_.toUpper();

  Int32 maxQueryInfoArraySize = sizeof(qis) / sizeof(QueryInfoStruct);
  NABoolean found = FALSE;
  Int32 j = 0;
  ComTdbExeUtilGetMetadataInfo::QueryType queryType = 
    ComTdbExeUtilGetMetadataInfo::NO_QUERY_;
  Lng32 maxParts = -1;
  NAString ausStr;
  Lng32 groupBy = 0;
  Lng32 orderBy = 0;
  NABoolean getVersionSupported = FALSE;
  while ((j < maxQueryInfoArraySize) && (NOT found))
    {
      if ((qis[j].ausStr == ausStr_) &&
	  (qis[j].infoType == infoType_) &&
	  (qis[j].iofStr == iofStr_) &&
	  (qis[j].objectType == objectType_))
	{
	  queryType = qis[j].queryType; 
	  maxParts = qis[j].maxParts;
	  ausStr = qis[j].ausStr;
	  groupBy = qis[j].groupBy;
	  orderBy = qis[j].orderBy;
	  getVersionSupported = qis[j].version;
	  found = TRUE;
	}
      j++;
    }

  if ((queryType == ComTdbExeUtilGetMetadataInfo::NO_QUERY_) ||
      (getVersion_ && (NOT getVersionSupported)))
    {
      *CmpCommon::diags() << DgSqlCode(-4218) << DgString0("GET <queryType> ");
      GenExit();
    }

  Lng32 numberExpanded = 0;
  if (NOT objectName_.getQualifiedNameObj().getCatalogName().isNull())
    numberExpanded = 3;
  else if (NOT objectName_.getQualifiedNameObj().getSchemaName().isNull())
    numberExpanded = 2;
  else if (NOT objectName_.getQualifiedNameObj().getObjectName().isNull())
    numberExpanded = 1;
  
  if ((maxParts > 0) && (numberExpanded > maxParts))
    {
      *CmpCommon::diags() << DgSqlCode(-4218) << DgString0("GET <maxParts>");
      GenExit();
    }

  NAString catName;
  NAString schName;
  NAString objName;
  
  // objectName_ is of the form <cat>.<sch>.<obj>
  if (maxParts == 1)
    {
      // objectname contains a 1-part catalog name
      catName = objectName_.getQualifiedNameObj().getObjectName();
    }
  else if (maxParts == 2)
    {
      // objectname contains a 2-part schema name
      catName = objectName_.getQualifiedNameObj().getSchemaName();
      schName = objectName_.getQualifiedNameObj().getObjectName();
    }
  else if (maxParts == 3)
    {
      // object name contains a 3-part object name
      catName = objectName_.getQualifiedNameObj().getCatalogName();
      schName = objectName_.getQualifiedNameObj().getSchemaName();
      objName = objectName_.getQualifiedNameObj().getObjectName();
    }
    
  // If this is a component request, set objName 
   if (objectType_ == "COMPONENT")
     objName = objectName_.getQualifiedNameObj().getObjectName();

  NAString hiveDefCatName = "";
  CmpCommon::getDefault(HIVE_CATALOG, hiveDefCatName, FALSE);
  hiveDefCatName.toUpper();
  
  NAString hiveDefSchName = "";
  CmpCommon::getDefault(HIVE_DEFAULT_SCHEMA, hiveDefSchName, FALSE);
  hiveDefSchName.toUpper();

  //  if ((catName.isNull() && schName.isNull()) &&
  if ((catName.isNull()) &&
      (generator->currentCmpContext()->schemaDB_->getDefaultSchema().
       getCatalogName() == HIVE_SYSTEM_CATALOG) &&
      ((queryType == ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_) ||
       (queryType == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_) ||
       (queryType == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_SCHEMA_) ||
       (queryType == ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_)))
    {
      catName = 
        generator->currentCmpContext()->schemaDB_->getDefaultSchema().
        getCatalogName();

      if (schName.isNull())
        schName = 
          generator->currentCmpContext()->schemaDB_->getDefaultSchema().
          getSchemaName();
    }

  if (((catName == hiveDefCatName) ||
       (catName == HIVE_SYSTEM_CATALOG)) &&
      ((queryType != ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_) &&
       (queryType != ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_) &&
       (queryType != ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_) &&
       (queryType != ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_) &&
       (queryType != ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_)))
    {
      setHiveObjects(TRUE);
    }
  
  if (hiveObjects())
    {
      if (queryType == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_)
        queryType = ComTdbExeUtilGetMetadataInfo::OBJECTNAMES_IN_SCHEMA_;
      else if (queryType == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_CATALOG_)
        queryType = ComTdbExeUtilGetMetadataInfo::OBJECTNAMES_IN_CATALOG_;
    }

  if (NOT hiveObjects())
    {
      if (catName.isNull())
        catName = 
          generator->currentCmpContext()->schemaDB_->getDefaultSchema().getCatalogName();
      
      if (schName.isNull())
        schName = 
          generator->currentCmpContext()->schemaDB_->getDefaultSchema().getSchemaName();
      
      if (objName.isNull())
        objName = "DUMMY__";
    }

  if (hiveObjects())
    {
      if (CmpCommon::getDefault(MODE_SEAHIVE) == DF_OFF)
	{
	  *CmpCommon::diags() << DgSqlCode(-4218) << DgString0("GET")
			      << DgString1("Reason: CQD MODE_SEAHIVE is not set.");
	  
	  GenExit();
	}

      // retrieval of tables, views and all objects in hive schema is supported.
      if (NOT((queryType == ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::OBJECTNAMES_IN_SCHEMA_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_SCHEMA_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::TABLES_IN_CATALOG_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_CATALOG_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_CATALOG_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::OBJECTNAMES_IN_CATALOG_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_) ||
              (queryType == ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_)))
	{
	  *CmpCommon::diags() << DgSqlCode(-4219);
	  GenExit();
	}
    }

  if (CmpSeabaseDDL::isSeabase(catName))
    setHbaseObjects(TRUE);

  if (catName == HBASE_SYSTEM_CATALOG)
    {
      if  (queryType == ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_)
        {
          *CmpCommon::diags() << DgSqlCode(-4218) << DgString0("GET")
                              << DgString1(" Reason: Cannot get native hbase tables using GET command. Use hbase shell interface to get this information.");
          GenExit();
        }
      else if (queryType == ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_)
        {
          *CmpCommon::diags() << DgSqlCode(-4218) << DgString0("GET");
          GenExit();
        }
      else if (schName == HBASE_MAP_SCHEMA)
        {
          *CmpCommon::diags() << DgSqlCode(-4218) << DgString0("GET");
          GenExit();
        }
    }
  
  if (
      (infoType_ == "MVS") ||
      (infoType_ == "MVGROUPS") ||
      (infoType_ == "SYNONYMS") ||
      (infoType_ == "PARTITIONS"))
    {
      NAString nas("GET ");
      nas += infoType_;
      *CmpCommon::diags() << DgSqlCode(-4222)
                          << DgString0(nas);

      GenExit();
    }
      

  if ((maxParts > 0) &&
      (NOT hiveObjects()) &&
      (NOT hbaseObjects()))
    {
      CorrName cn(objName, generator->wHeap(), schName, catName);
      if (objectType_ == "INDEX")
        cn.setSpecialType(ExtendedQualName::INDEX_TABLE);
      if (objectType_ == "LIBRARY")
        cn.setSpecialType(ExtendedQualName::LIBRARY_TABLE);
	    
      NATable *naTable = generator->getBindWA()->getNATableInternal(cn);
      if ((! naTable) || (generator->getBindWA()->errStatus()))
      {
	  CollIndex retIndex = NULL_COLL_INDEX;
	  if ((objectType_ == "CATALOG") && 
	      ((CmpCommon::diags()->containsError(-1003)) ||
	       (CmpCommon::diags()->containsError(-4082))))
	    {
	      if (CmpCommon::diags()->containsError(-1003))
		retIndex = CmpCommon::diags()->returnIndex(-1003);
	      else
		retIndex = CmpCommon::diags()->returnIndex(-4082);
	      
	      CmpCommon::diags()->removeError(retIndex);
	    }
	  else if (objectType_ == "SCHEMA")
	    {
	      if (CmpCommon::diags()->containsError(-4082))
		{
		  retIndex = CmpCommon::diags()->returnIndex(-4082);
		  
		  CmpCommon::diags()->removeError(retIndex);
		}
	    }

	  if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) > 0)
	    {
	      GenExit();
	    }
	  else
	    {
	      CmpCommon::diags()->clear();
	      generator->getBindWA()->resetErrStatus();
	    }
	}
      
      // if a hive table has an associated external table, get the name
      // of the external table. Use that to look for views created on
      // the hive table.
      if (((catName == hiveDefCatName) ||
           (catName == HIVE_SYSTEM_CATALOG)) &&
          ((queryType == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_) &&
           (naTable && (NOT naTable->isRegistered()) &&
            naTable->hasExternalTable())))
        {
          // Convert the native name to its Trafodion form
          NAString tabName = ComConvertNativeNameToTrafName
            (catName, schName, objName);
          
          ComObjectName externalName(tabName, COM_TABLE_NAME);
          catName = externalName.getCatalogNamePartAsAnsiString();
          schName = externalName.getSchemaNamePartAsAnsiString(TRUE);
          objName = externalName.getObjectNamePartAsAnsiString(TRUE);
        }

      if (objectType_ == "TABLE")
	{
	  if ((naTable->getViewFileName()) ||
	      (naTable->isAnMV()))
	    {
	      *CmpCommon::diags() << DgSqlCode(-4219);
	      GenExit();
	    }
	}
      else if (objectType_ == "VIEW")
	{
	  if (NOT naTable->getViewFileName())
	    {
	      *CmpCommon::diags() << DgSqlCode(-4219);
	      GenExit();
	    }
	}
      else if (objectType_ == "MV")
	{
	  if (NOT naTable->isAnMV())
	    {
	      *CmpCommon::diags() << DgSqlCode(-4219);
	      GenExit();
	    }
	}
    }

  char * cat = NULL;
  char * sch = NULL;
  char * obj = NULL;
  
  if (NOT catName.isNull())
    {
      cat = space->allocateAlignedSpace(catName.length() + 1);
      strcpy(cat, catName.data());
    }

  if (NOT schName.isNull())
    {
      sch = space->allocateAlignedSpace(schName.length() + 1);
      strcpy(sch, schName.data());
    }

  if (NOT objName.isNull())
    {
      obj = space->allocateAlignedSpace(objName.length() + 1);
      strcpy(obj, objName.data());
    }

  char * pattern = NULL;
  if (NOT pattern_.isNull())
    {
      pattern = space->allocateAlignedSpace(pattern_.length() + 1);
      strcpy(pattern, pattern_.data());
    }
  
  char * param1 = NULL;
  if (NOT param1_.isNull())
    {
      param1 = space->allocateAlignedSpace(param1_.length() + 1);
      strcpy(param1, param1_.data());
    }
  char *connectParam1;
  char *connectParam2;
  ComStorageType storageType;

  if (queryType == ComTdbExeUtilGetMetadataInfo::MONARCH_OBJECTS_) {
     storageType = COM_STORAGE_MONARCH;
  } else if (queryType == ComTdbExeUtilGetMetadataInfo::BIGTABLE_OBJECTS_) {
     storageType = COM_STORAGE_BIGTABLE;
  } else {
     storageType = COM_STORAGE_HBASE;
  }

  NATable::getConnectParams(storageType, &connectParam1, &connectParam2);
  char * server = space->allocateAlignedSpace(strlen(connectParam1) + 1);
  strcpy(server, connectParam1);
  char * zkPort = space->allocateAlignedSpace(strlen(connectParam2) + 1);
  strcpy(zkPort, connectParam2);

  ComTdb * exe_util_tdb = NULL;
  ComTdbExeUtilGetMetadataInfo * gm_exe_util_tdb = NULL;

  if (hiveObjects())
    {
      gm_exe_util_tdb = new(space) 
	ComTdbExeUtilGetHiveMetadataInfo(
					 queryType,
					 cat,
					 sch,
					 obj,
					 pattern,
					 param1,
					 scanExpr,
					 workCriDesc, exe_util_row_atp_index, // work cri desc
					 givenDesc,
					 returnedDesc,
					 (queue_index)8,
					 (queue_index)128,
					 2, 
					 32000); 

      exe_util_tdb = gm_exe_util_tdb;
    }
  else
    {
      gm_exe_util_tdb = new(space) 
	ComTdbExeUtilGetMetadataInfo(
				     queryType,
				     cat,
				     sch,
				     obj,
				     pattern,
				     param1,
				     scanExpr,
				     workCriDesc, exe_util_row_atp_index, // work cri desc
				     givenDesc,
				     returnedDesc,
				     (queue_index)8,
				     (queue_index)128,
				     2, 
				     32000,
                                     server,
                                     zkPort);

      if (hbaseObjects())
	{
	  gm_exe_util_tdb->setIsHbase(TRUE);
	}

      if (withNamespace_)
        gm_exe_util_tdb->setWithNamespace(TRUE);

      exe_util_tdb = gm_exe_util_tdb;
    }

  //  if (NOT hbaseObjects())
  // {
  gm_exe_util_tdb->setNoHeader(noHeader_);
  gm_exe_util_tdb->setReturnFullyQualNames(returnFullyQualNames_);
  if (ausStr == "USER")
    gm_exe_util_tdb->setUserObjs(TRUE);
  else if (ausStr == "SYSTEM")
    gm_exe_util_tdb->setSystemObjs(TRUE);
  else if (ausStr == "ALL")
    gm_exe_util_tdb->setAllObjs(TRUE);
  else if ((queryType == ComTdbExeUtilGetMetadataInfo::HBASE_OBJECTS_ || 
             queryType == ComTdbExeUtilGetMetadataInfo::BIGTABLE_OBJECTS_ || 
             queryType == ComTdbExeUtilGetMetadataInfo::MONARCH_OBJECTS_) &&
           (ausStr == "EXTERNAL"))
    gm_exe_util_tdb->setExternalObjs(TRUE);
  gm_exe_util_tdb->setGetVersion(getVersion_);
  gm_exe_util_tdb->setCascade(cascade_);
  
  if ((queryType == ComTdbExeUtilGetMetadataInfo::PARTITIONS_FOR_TABLE_) ||
      (queryType == ComTdbExeUtilGetMetadataInfo::PARTITIONS_FOR_INDEX_))
    {
      gm_exe_util_tdb->setGetObjectUid(TRUE);
      if (objectType_ == "INDEX")
	gm_exe_util_tdb->setIsIndex(TRUE);
    }
  
  if (! Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      if (groupBy)
	gm_exe_util_tdb->setGroupBy(TRUE);
      
      if (orderBy)
	gm_exe_util_tdb->setOrderBy(TRUE);
    }
  generator->initTdbFields(exe_util_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  generator->setTransactionFlag(0);
  
  return 0;
}

static short addReferencingMVs(Generator * generator,
			       char * mvName,
			       Queue * refreshMvsList,
			       Queue * reorgMvsList)
{
  short retcode = 0;

  Space * space = generator->getSpace();

  // for each mv, create a NATable and TableDesc.
  // This is used to get all MVs which are defined on the MVs.
  CorrName cn = CorrName(QualifiedName(mvName, 0));
  NATable *mvNATable =
    generator->getBindWA()->getNATable(cn);
  if (generator->getBindWA()->errStatus())
    {
      GenExit();
      return -1;
    }
  if (NOT mvNATable->getMvsUsingMe().isEmpty())
    {
      const UsingMvInfoList &mvMVList = mvNATable->getMvsUsingMe();
      for (CollIndex i=0; i<mvMVList.entries(); i++)
	{
	  char * mvMVname =
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(mvMVList[i]->getMvName()),0);

	  if (refreshMvsList)
	    refreshMvsList->insert(mvMVname);

	  if (reorgMvsList)
	    reorgMvsList->insert(mvMVname);

	  // now add all mvs referencing mvMVname
	  retcode = addReferencingMVs(generator, mvMVname,
				      refreshMvsList,
				      reorgMvsList);
	  if (retcode)
	    return retcode;
	}
    }

  //  delete mvNATable;

  return retcode;
}


////////////////////////////////////////////////////////
//
// ExeUtilMaintainObject::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilMaintainObject::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * tablename = NULL;
  char *schemaName = NULL;

  /*  if (type_ == CATALOG_)
    tablename = 
      space->AllocateAndCopyToAlignedSpace(
	   getTableName().getQualifiedNameObj().getCatalogName(), 0);
  else if (type_ == SCHEMA_)
    tablename = 
      space->AllocateAndCopyToAlignedSpace(
	   getTableName().getQualifiedNameObj().getSchemaName(), 0);
  else
  */

  if (getUtilTableDesc() && getUtilTableDesc()->getNATable()->isVolatileTable())
    {
      tablename = 
	space->AllocateAndCopyToAlignedSpace(getTableName().getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE), 0);
    }
  else
    {
      tablename = 
	space->AllocateAndCopyToAlignedSpace(
			    generator->genGetNameAsAnsiNAString(getTableName()), 0);
       
      /*	schemaName = 
	  space->AllocateAndCopyToAlignedSpace(
		    getTableName().getQualifiedNameObj().getSchemaName(), 0);
	catName = 
	  space->AllocateAndCopyToAlignedSpace(
		    getTableName().getQualifiedNameObj().getCatalogName(), 0);
      */
   
	QualifiedName q("X",  
			getTableName().getQualifiedNameObj().getSchemaName(), 
			getTableName().getQualifiedNameObj().getCatalogName());
	NAString schemaStr(q.getQualifiedNameAsAnsiString(),CmpCommon::statementHeap());
	CMPASSERT(schemaStr[schemaStr.length()-1] == 'X');
	schemaStr.remove(schemaStr.length()-2);		// remove the '.X'
      
	schemaName = new(space) char[schemaStr.length()+1];
	strcpy(schemaName, schemaStr);
    }
  
  char * parentTableName = NULL;
  if (getUtilTableDesc() && getUtilTableDesc()->getNATable()->getParentTableName())
    {
      parentTableName = new(generator->getSpace()) 
	char[strlen(getUtilTableDesc()->getNATable()->getParentTableName()) + 1];
      strcpy(parentTableName, getUtilTableDesc()->getNATable()->getParentTableName());
    }
    
 
  if (!parentTableName && getParentTableNameLen() > 0)
    {
      parentTableName = new(generator->getSpace()) 
      char[getParentTableNameLen() + 1];
      strcpy(parentTableName, getParentTableName());
    }

  UInt16 ot;
  switch (type_)
    {
    case TABLE_: ot = ComTdbExeUtilMaintainObject::TABLE_; break;
    case INDEX_: ot = ComTdbExeUtilMaintainObject::INDEX_; break;
    case MV_: ot = ComTdbExeUtilMaintainObject::MV_; break;
    case MV_INDEX_: ot = ComTdbExeUtilMaintainObject::MV_INDEX_; break;
    case MV_LOG_: ot = ComTdbExeUtilMaintainObject::MV_LOG_; break;
    case CATALOG_: ot = ComTdbExeUtilMaintainObject::CATALOG_; break;
    case SCHEMA_: ot = ComTdbExeUtilMaintainObject::SCHEMA_; break;
    case CLEAN_MAINTAIN_: ot = ComTdbExeUtilMaintainObject::CLEAN_MAINTAIN_; break;
    default: ot = ComTdbExeUtilMaintainObject::NOOP_; break;
    }

  ComTdbExeUtilMaintainObject * exe_util_tdb = new(space) 
    ComTdbExeUtilMaintainObject(
	 tablename, strlen(tablename),
	 schemaName, strlen(schemaName),
	 ot,
	 parentTableName, (parentTableName ? strlen(parentTableName) : 0),
	 0, 0, // no input expr
	 0, 0, // no output expr
	 0, 0, // no work cri desc
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)64,
	 2, 
	 10240);
 
  generator->initTdbFields(exe_util_tdb);

  // Set the maintained table creation time

  exe_util_tdb->setMaintainedTableCreateTime(getMaintainedTableCreateTime());

  // Set the parent table object UID

  exe_util_tdb->setParentTableObjectUID(getParentTableObjectUID());

  // Set the Object UID of the parent table for an index.
  // If a parent table is successfully found, use the 
  // value.   

  if (parentTableName && (getParentTableObjectUID() == 0))
    {
      CorrName cn = CorrName(QualifiedName(parentTableName, 0));
      
      NATable *naParentTable = 
        generator->getBindWA()->getNATable(cn);

      if (!generator->getBindWA()->errStatus()) 
        {
          // Set the parent table object UID 
          exe_util_tdb->setParentTableObjectUID(naParentTable->objectUid().get_value());

          // In the case of an index, the parent table
          // creation time is set
          exe_util_tdb->setMaintainedTableCreateTime(naParentTable->getCreateTime());
        }

    }

  char * tempName = NULL;
  NAString maintainCatName = 
    ActiveSchemaDB()->getDefaults().getValue(MAINTAIN_CATALOG);
  
  if (maintainCatName.isNull())
    maintainCatName = "NEO";
  
  tempName = new(space) char[maintainCatName.length() + 1];
  strcpy(tempName, maintainCatName.data());
  exe_util_tdb->setNEOCatalogName(tempName);
  
  Queue * indexList      = NULL;
  Queue * refreshMvgroupList  = NULL;
  Queue * refreshMvsList      = NULL;
  Queue * reorgMvgroupList  = NULL;
  Queue * reorgMvsList        = NULL;
  Queue * reorgMvsIndexList   = NULL;
  Queue * updStatsMvgroupList = NULL;
  Queue * updStatsMvsList     = NULL;
  Queue * multiTablesNamesList= new(space) Queue(space);
  Queue * multiTablesCreateTimeList = new(space) Queue(space);
  Queue * skippedMultiTablesNamesList= new(space) Queue(space);

  if (reorgIndex_ || getIndexLabelStats_)
    {
      indexList = new(space) Queue(space);

      char * indexName = NULL;
      if (type_ == INDEX_)
	{
	  indexName = 
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(getTableName()), 0);

	  indexList->insert(indexName);
	}
      else
	{
	  const LIST(IndexDesc *) indList = 
	    getUtilTableDesc()->getIndexes();
	  for (CollIndex i=0; i<indList.entries(); i++) 
	    {
	      IndexDesc *index = indList[i];
	      
	      // The base table itself is an index (the clustering index);
	      // obviously IM need not deal with it.
	      if (index->isClusteringIndex())
		continue;
	      
	      indexName = 
		space->AllocateAndCopyToAlignedSpace
		(index->getExtIndexName(), 0);

	      indexList->insert(indexName);
	    }
	}
    }

  Queue * allUsingMvsList = NULL;
  if ((type_ == TABLE_) &&
      ((refreshMvs_) ||
       (reorgMvs_) ||
       (updStatsMvs_) ||
       (reorgMvsIndex_)))
    {
      // create a list of all MVs referencing, directly or indirectly,
      // the given tablename.
      allUsingMvsList = new(generator->wHeap()) Queue(generator->wHeap());
      const UsingMvInfoList &usingMvList = 
	getUtilTableDesc()->getNATable()->getMvsUsingMe();
      for (CollIndex i=0; i<usingMvList.entries(); i++) 
	{
	  char * mvName = 
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(usingMvList[i]->getMvName()),
	     0);

	  allUsingMvsList->insert(mvName);
	  
	  if (addReferencingMVs(
	       generator, mvName,
	       allUsingMvsList, NULL))
	    return -1;
	}
    }

  if ((refreshMvs_) ||
      (reorgMvs_))
    {
      if (refreshMvs_)
	refreshMvsList = new(space) Queue(space);

      if (reorgMvs_)
	reorgMvsList = new(space) Queue(space);
      
      char * mvName = NULL;
      if (type_ == MV_)
	{
	  mvName = 
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(getTableName()), 0);

	  if (refreshMvs_)
	    refreshMvsList->insert(mvName);
	  
	  if (reorgMvs_)
	    reorgMvsList->insert(mvName);
	  
	}
      else
	{
	  // move MV names from allUsingMvsList to refresh and/or reorg
	  // lists
	  allUsingMvsList->position();
	  while (allUsingMvsList->atEnd() == 0)
	    {
	      char * mvName = (char*)allUsingMvsList->getNext();
	      if (refreshMvs_)
		refreshMvsList->insert(mvName);
	      
	      if (reorgMvs_)
		reorgMvsList->insert(mvName);
	    }
	}
    }
  
  if (refreshMvgroup_)
    {
      // if type is TABLE_, only refresh mvgroup if there are MVs in
      // the mvlist.
      char * mvName = NULL;
      if ((type_ == MVGROUP_) ||
	  ((type_ == TABLE_) &&
	   (refreshMvsList && (NOT refreshMvsList->isEmpty()))))
	{
	  refreshMvgroupList = new(space) Queue(space);

	  mvName = 
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(getTableName()), 0);
	  
	  refreshMvgroupList->insert(mvName);
	}
    }

  if (reorgMvgroup_)
    {
      // if type is TABLE_, only reorg mvgroup if there are MVs in
      // the mvlist.
      char * mvName = NULL;
      if ((type_ == MVGROUP_) ||
	  ((type_ == TABLE_) &&
	   (reorgMvsList && (NOT reorgMvsList->isEmpty()))))
	{
	  reorgMvgroupList = new(space) Queue(space);

	  mvName = 
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(getTableName()), 0);
	  
	  reorgMvgroupList->insert(mvName);
	}
    }

  if (updStatsMvs_)
    {
      if (updStatsMvs_)
	updStatsMvsList = new(space) Queue(space);

      
      char * mvName = NULL;
      if (type_ == MV_)
	{
	  mvName = 
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(getTableName()), 0);

	  if (updStatsMvs_)
	    updStatsMvsList->insert(mvName);
	  
	}
      else
	{
	  // move MV names from allUsingMvsList to updStats and/or reorg
	  // lists
	  allUsingMvsList->position();
	  while (allUsingMvsList->atEnd() == 0)
	    {
	      char * mvName = (char*)allUsingMvsList->getNext();
	      if (updStatsMvs_)
		updStatsMvsList->insert(mvName);
	      
	    }
	}
    }
 
  if (updStatsMvgroup_)
    {
      // if type is TABLE_, only update the statistics for the mvgroup if there are MVs in
      // the mvlist.
      char * mvName = NULL;
      if ((type_ == MVGROUP_) ||
	  ((type_ == TABLE_) &&
	   (updStatsMvsList && (NOT updStatsMvsList->isEmpty()))))
	{
	  updStatsMvgroupList = new(space) Queue(space);

	  mvName = 
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(getTableName()), 0);
	  
	  updStatsMvgroupList->insert(mvName);
	}
    }
/*

  if (updStatsMvgroup_)
    {
 
      // create a list of all MVs referencing, directly or indirectly,
      // the given tablename.
      updStatsMvgroupList = new(generator->wHeap()) Queue(generator->wHeap());
      char * mvName = 
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(getTableName()), 0);
      NATable *mavNaTable = generator->getBindWA()->getNATable(getTableName());
 
      // Now get the MVInfo.
      MVInfoForDML *mvInfo = mavNaTable->getMVInfo(generator->getBindWA());
      LIST (MVUsedObjectInfo*)& UsedObjList =
        mvInfo->getUsedObjectsList();

  MVUsedObjectInfo* pUsedTable = UsedObjList[0];

  // Get the NATable
  QualifiedName underlyingTableName = pUsedTable->getObjectName();
  CorrName corrTableName(underlyingTableName);
  NATable * pNaTable = generator->getBindWA()->getNATable(corrTableName, FALSE);
      const UsingMvInfoList &usingMvList = 
	getTableDesc()->getNATable()->getMvsUsingMe();
      for (CollIndex i=0; i<UsedObjList.entries(); i++) 
	{
	  char * mvName = 
	    space->AllocateAndCopyToAlignedSpace
	    (generator->genGetNameAsAnsiNAString(UsedObjList[i]->getObjectName()),
	     0);

	  updStatsMvgroupList->insert(mvName);
	  
	  if (addReferencingMVs(
	       generator, mvName,
	       updStatsMvgroupList, NULL))
	    return -1;
	}
    }
*/
  if (reorgMvsIndex_)
    {
      reorgMvsIndexList = new(space) Queue(space);

      char * mvName = 
	space->AllocateAndCopyToAlignedSpace
	(generator->genGetNameAsAnsiNAString(getTableName()), 0);
      if (type_ == MV_)
	{
	  allUsingMvsList = new(generator->wHeap()) Queue(generator->wHeap());
	  allUsingMvsList->insert(mvName);
	}

      if (type_ == MV_INDEX_)
	{
	  if (reorgMvsIndex_)
	    reorgMvsIndexList->insert(mvName);
	}
      else
	{
	  // move all indexes on all MVs present in allUsingMvsList
	  allUsingMvsList->position();
	  while (allUsingMvsList->atEnd() == 0)
	    {
	      mvName = (char*)allUsingMvsList->getNext();
	      
	      CorrName cn = CorrName(QualifiedName(mvName, 0));
	      NATable *mvNATable = 
		generator->getBindWA()->getNATable(cn);
	      if (generator->getBindWA()->errStatus()) 
		{
		  GenExit();
		  //GenAssert(! generator->getBindWA()->errStatus(),
		  //    "MaintainObject: Could not get NATable for MView");
		  return -1;
		}
	      
	      TableDesc *mvTableDesc =
		generator->getBindWA()->createTableDesc
		(mvNATable, cn);
	      if (generator->getBindWA()->errStatus()) 
		{
		  GenExit();
		  //GenAssert(! generator->getBindWA()->errStatus(),
		  //    "MaintainObject: Could not get TableDesc for MView");
		  return -1;
		}
	      
	      const LIST(IndexDesc *) indexList = 
		mvTableDesc->getIndexes();
	      for (CollIndex i=0; i<indexList.entries(); i++) 
		{
		  IndexDesc *index = indexList[i];
		  
		  // The base table itself is an index (the clustering index);
		  // obviously IM need not deal with it.
		  if (index->isClusteringIndex())
		    continue;
		  
		  char * indexName = 
		    space->AllocateAndCopyToAlignedSpace
		    (index->getExtIndexName(), 0);
		  
		  reorgMvsIndexList->insert(indexName);
		}
	    }
	}
    } // reorgMvsIndex


  NAString updStatsMvlogCols;
  if (  (type_ == TABLE_) && 
        (updStatsMvlog_) &&
        (NULL != getMvLogTable()) )
    {
      updStatsMvlogCols = " ON EVERY KEY, ";
 
      NABoolean firstColSeen = FALSE;
      const NAColumnArray &naColArr =
	getMvLogTable()->getNAColumnArray();
      for (CollIndex i = 0; i < naColArr.entries(); i++)
	{
	  NAColumn * naCol = naColArr[i];
	  if ((naCol->isSystemColumn()) ||
	      (naCol->isMvSystemAddedColumn()))
	    continue;

	  if (firstColSeen)
	    updStatsMvlogCols += ", ";

	  firstColSeen = TRUE;
	  updStatsMvlogCols += '(';
          updStatsMvlogCols += '"';
	  updStatsMvlogCols += naCol->getColName();
          updStatsMvlogCols += '"';
	  updStatsMvlogCols += ')';
	}

      updStatsMvlogCols += " SAMPLE";
    }

  if (type_ == TABLES_)
    {
      for (CollIndex i = 0; i < multiTablesNames_.entries(); i++)
	{
	  char * tableName = 
	    space->AllocateAndCopyToAlignedSpace(
		 generator->genGetNameAsAnsiNAString(
		      *(multiTablesNames_[i])), 0);

	  multiTablesNamesList->insert(tableName);
	  
	  Int64 ct = (*multiTablesDescs_)[i]->getNATable()->getCreateTime();
	  char * createTime = 
	    space->allocateAndCopyToAlignedSpace((char*)&ct, sizeof(Int64), 0);
	  multiTablesCreateTimeList->insert(createTime);
	}

      for (CollIndex i = 0; i < skippedMultiTablesNames_.entries(); i++)
	{
	  char * tableName = 
	    space->AllocateAndCopyToAlignedSpace(
		 generator->genGetNameAsAnsiNAString(
		      *(skippedMultiTablesNames_[i])), 0);

	  skippedMultiTablesNamesList->insert(tableName);
	}

    }

  char* reorgTableOptions = NULL;
  char* reorgIndexOptions = NULL;
  char* updStatsTableOptions = NULL;
  char* updStatsMvlogOptions = NULL;
  char* updStatsMvsOptions = NULL;
  char* updStatsMvgroupOptions = NULL;
  char* refreshMvgroupOptions = NULL;
  char* refreshMvsOptions = NULL;
  char* reorgMvgroupOptions = NULL;
  char* reorgMvsOptions = NULL;
  char* reorgMvsIndexOptions = NULL;
  char* cleanMaintainCITOptions = NULL;
  
  if (NOT reorgTableOptions_.isNull())
    {
      reorgTableOptions = 
	space->allocateAlignedSpace(strlen(reorgTableOptions_) +1);
      strcpy(reorgTableOptions, reorgTableOptions_);
    }
  if (NOT reorgIndexOptions_.isNull())
    {
      reorgIndexOptions = 
	space->allocateAlignedSpace(strlen(reorgIndexOptions_) +1);
      strcpy(reorgIndexOptions, reorgIndexOptions_);
    }
  if (NOT updStatsTableOptions_.isNull())
    {
      updStatsTableOptions = 
	space->allocateAlignedSpace(strlen(updStatsTableOptions_) +1);
      strcpy(updStatsTableOptions, updStatsTableOptions_);
    }
  if ((NOT updStatsMvlogOptions_.isNull()) ||
      (NOT updStatsMvlogCols.isNull()))
    {
      if (NOT updStatsMvlogCols.isNull())
	updStatsMvlogOptions = 
	  space->allocateAlignedSpace(strlen(updStatsMvlogCols) +1);
      else
	updStatsMvlogOptions = 
	  space->allocateAlignedSpace(strlen(updStatsMvlogOptions_) +1);
      if (NOT updStatsMvlogCols.isNull())
	strcpy(updStatsMvlogOptions, updStatsMvlogCols);
      else
	strcpy(updStatsMvlogOptions, updStatsMvlogOptions_);
    }

  if (NOT updStatsMvsOptions_.isNull())
    {
      updStatsMvsOptions = 
	space->allocateAlignedSpace(strlen(updStatsMvsOptions_) +1);
      strcpy(updStatsMvsOptions, updStatsMvsOptions_);
    }

  if (NOT updStatsMvgroupOptions_.isNull())
    {
      updStatsMvgroupOptions = 
	space->allocateAlignedSpace(strlen(updStatsMvgroupOptions_) +1);
      strcpy(updStatsMvgroupOptions, updStatsMvgroupOptions_);
    }

  if (NOT refreshMvgroupOptions_.isNull())
    {
      refreshMvgroupOptions = 
	space->allocateAlignedSpace(strlen(refreshMvgroupOptions_) +1);
      strcpy(refreshMvgroupOptions, refreshMvgroupOptions_);
    }
  if (NOT refreshMvsOptions_.isNull())
    {
      refreshMvsOptions = 
	space->allocateAlignedSpace(strlen(refreshMvsOptions_) +1);
      strcpy(refreshMvsOptions, refreshMvsOptions_);
    }
  if (NOT reorgMvgroupOptions_.isNull())
    {
      reorgMvgroupOptions = 
	space->allocateAlignedSpace(strlen(reorgMvgroupOptions_) +1);
      strcpy(reorgMvgroupOptions, reorgMvgroupOptions_);
    }
  if (NOT reorgMvsOptions_.isNull())
    {
      reorgMvsOptions = 
	space->allocateAlignedSpace(strlen(reorgMvsOptions_) +1);
      strcpy(reorgMvsOptions, reorgMvsOptions_);
    }
  if (NOT reorgMvsIndexOptions_.isNull())
    {
      reorgMvsIndexOptions = 
	space->allocateAlignedSpace(strlen(reorgMvsIndexOptions_) +1);
      strcpy(reorgMvsIndexOptions, reorgMvsIndexOptions_);
    }

  if (NOT cleanMaintainCITOptions_.isNull())
    {
      cleanMaintainCITOptions = 
	space->allocateAlignedSpace(strlen(cleanMaintainCITOptions_) +1);
      strcpy(cleanMaintainCITOptions, cleanMaintainCITOptions_);
    }

  if (cleanMaintainCIT_)
    exe_util_tdb->setCleanMaintainCIT(TRUE);

  exe_util_tdb->setParams(reorgTable_, 
			  (reorgIndex_&& indexList && (NOT indexList->isEmpty())),
			  updStatsTable_, 
			  (((type_ == TABLE_) && (updStatsMvlog_) 
                              && (NULL != getMvLogTable()))
			   ? TRUE : FALSE),
			  (updStatsMvsList && (NOT updStatsMvsList->isEmpty())),
			  (updStatsMvgroupList && (NOT updStatsMvgroupList->isEmpty())),
			  (refreshMvgroupList && (NOT refreshMvgroupList->isEmpty())),
			  (refreshMvsList && (NOT refreshMvsList->isEmpty())),
			  (reorgMvgroupList && (NOT reorgMvgroupList->isEmpty())),
			  (reorgMvsList && (NOT reorgMvsList->isEmpty())),
			  (reorgMvsIndexList && (NOT reorgMvsIndexList->isEmpty())),
			  continueOnError_,
			  cleanMaintainCIT_,			  
			  getSchemaLabelStats_,
			  getLabelStats_,
			  getTableLabelStats_,
			  getIndexLabelStats_ && indexList &&(NOT indexList->isEmpty()),
			  getLabelStatsIncIndexes_,
			  getLabelStatsIncInternal_,
			  getLabelStatsIncRelated_
			  
);

  if (display_)
    exe_util_tdb->setDisplay(TRUE);
  if (displayDetail_)
    exe_util_tdb->setDisplayDetail(TRUE);
  if (doTheSpecifiedTask_)
    exe_util_tdb->setDoSpecifiedTask(TRUE);
  if (getStatus_)
    exe_util_tdb->setGetStatus(TRUE);
  if (getDetails_)
    exe_util_tdb->setGetDetails(TRUE);
  if (noOutput_)
    exe_util_tdb->setNoOutput(TRUE);
  
  if (getUtilTableDesc() && getUtilTableDesc()->getNATable()->isVolatileTable())
    exe_util_tdb->setNoControlInfoUpdate(TRUE);

  if (CmpCommon::getDefault(USE_MAINTAIN_CONTROL_TABLE) == DF_OFF)
    exe_util_tdb->setNoControlInfoTable(TRUE);

  if (run_)
    {
      exe_util_tdb->setRun(TRUE);
      exe_util_tdb->setRunFrom(runFrom_);
      exe_util_tdb->setRunTo(runTo_);

      exe_util_tdb->setIfNeeded(ifNeeded_);
    }
  if (initialize_)
    exe_util_tdb->setInitializeMaintain(TRUE);
  if (reinitialize_)
    exe_util_tdb->setReInitializeMaintain(TRUE);
  if (drop_)
    exe_util_tdb->setDropMaintain(TRUE);
  if (dropView_)
    exe_util_tdb->setDropView(TRUE);
  if (createView_)
    exe_util_tdb->setCreateView(TRUE);

  if (all_)
    exe_util_tdb->setAllSpecified(TRUE);

  if ((getStatus_) || (getDetails_))
    {
      if (shortFormat_)
	exe_util_tdb->setShortFormat(TRUE);
      else if (longFormat_)
	exe_util_tdb->setLongFormat(TRUE);
      else if (detailFormat_)
	exe_util_tdb->setDetailFormat(TRUE);
      else if (tokenFormat_)
	exe_util_tdb->setTokenFormat(TRUE);
      else if (commandFormat_)
	exe_util_tdb->setCommandFormat(TRUE);
    }
   
   if (getTableLabelStats_)
    exe_util_tdb->setTableLabelStats(TRUE);
   if (getIndexLabelStats_)
     exe_util_tdb->setIndexLabelStats(TRUE);

  if (getLabelStatsIncIndexes_)
    {
      exe_util_tdb->setTableLabelStats(TRUE);
      exe_util_tdb->setLabelStatsIncIndexes(TRUE);
    }
 if (getLabelStatsIncInternal_)
    {
      exe_util_tdb->setTableLabelStats(TRUE);
      exe_util_tdb->setLabelStatsIncInternal(TRUE);
    }
 if (getLabelStatsIncRelated_)
    {
      exe_util_tdb->setTableLabelStats(TRUE);
      exe_util_tdb->setLabelStatsIncRelated(TRUE);
      }
 
  if (getSchemaLabelStats_)
      exe_util_tdb->setSchemaLabelStats(TRUE);
 
  if ((refreshMvgroup_) &&
      (refreshMvs_) &&
      (type_ == TABLE_))
    {
      // If individual MV's are to be refresh, then skip the group refresh
      exe_util_tdb->setSkipRefreshMvs(TRUE);
    }
 
  if ((reorgMvgroup_) &&
      (reorgMvs_) &&
      (type_ == TABLE_))
    {
      // for QCD4, assume that mvgroup name is the same as tablename.
      // If reorg of mvgroup succeeds at runtime, then skip reorg
      // of MVs. If it fails, then reorg MVs.
      // Set a flag indicating that.
      exe_util_tdb->setSkipReorgMvs(TRUE);
    }

  if ((updStatsMvgroup_) &&
      (updStatsMvs_) &&
      (type_ == TABLE_))
    {
      // for QCD4, assume that mvgroup name is the same as tablename.
      // If updStats of mvgroup succeeds at runtime, then skip updStats
      // of MVs. If it fails, then updStats MVs.
      // Set a flag indicating that.
      exe_util_tdb->setSkipUpdStatsMvs(TRUE);
    }

  exe_util_tdb->setLists(indexList,
			 refreshMvgroupList,
			 refreshMvsList,
			 reorgMvgroupList,
			 reorgMvsList,
			 reorgMvsIndexList,
			 updStatsMvgroupList,
			 updStatsMvsList,
			 multiTablesNamesList,
			 skippedMultiTablesNamesList);
  exe_util_tdb->setMultiTablesCreateTimeList(multiTablesCreateTimeList);

  exe_util_tdb->setOptionsParams(reorgTableOptions, reorgIndexOptions, 
				 updStatsTableOptions, updStatsMvlogOptions,
				 updStatsMvsOptions, updStatsMvgroupOptions,
				 refreshMvgroupOptions, refreshMvsOptions, 
				 reorgMvgroupOptions, reorgMvsOptions,
				 reorgMvsIndexOptions,
				 cleanMaintainCITOptions);

  exe_util_tdb->setControlParams
    (disableReorgTable_, 
     enableReorgTable_,
     disableReorgIndex_,
     enableReorgIndex_,
     disableUpdStatsTable_,
     enableUpdStatsTable_,
     disableUpdStatsMvs_,
     enableUpdStatsMvs_,
     disableRefreshMvs_,
     enableRefreshMvs_,
     disableReorgMvs_,
     enableReorgMvs_,
     resetReorgTable_,
     resetUpdStatsTable_,
     resetUpdStatsMvs_,
     resetRefreshMvs_,
     resetReorgMvs_,
     resetReorgIndex_,
     enableUpdStatsMvlog_,
     disableUpdStatsMvlog_,
     resetUpdStatsMvlog_,
     enableReorgMvsIndex_,
     disableReorgMvsIndex_,
     resetReorgMvsIndex_,
     enableRefreshMvgroup_,
     disableRefreshMvgroup_,
     resetRefreshMvgroup_,
     enableReorgMvgroup_,
     disableReorgMvgroup_,
     resetReorgMvgroup_,
     enableUpdStatsMvgroup_,
     disableUpdStatsMvgroup_,
     resetUpdStatsMvgroup_,
     enableTableLabelStats_,
     disableTableLabelStats_,
     resetTableLabelStats_,
     enableIndexLabelStats_,
     disableIndexLabelStats_,
     resetIndexLabelStats_);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Set the transaction flag.
  if (xnNeeded())
    {
      generator->setTransactionFlag(-1);
    }
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilHiveTruncate::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilHiveTruncate::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * tablename = NULL;
  tablename = space->AllocateAndCopyToAlignedSpace
    (generator->genGetNameAsAnsiNAString(getTableName()), 0);

  char * hiveTableName = NULL;
  hiveTableName = space->AllocateAndCopyToAlignedSpace(getHiveTableName(), 0);

  char * hiveTruncQuery = NULL;
  hiveTruncQuery = space->AllocateAndCopyToAlignedSpace(getHiveTruncQuery(), 0);

  ComTdbExeUtilHiveTruncate * exe_util_tdb = new(space) 
    ComTdbExeUtilHiveTruncate(tablename, strlen(tablename),
                              hiveTableName,
                              NULL, NULL,
                              NULL, -1,
                              -1,
                              hiveTruncQuery,
                              (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
                              (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
                              (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
                              (queue_index)getDefault(GEN_DDL_SIZE_UP),
                              getDefault(GEN_DDL_NUM_BUFFERS),
                              getDefault(GEN_DDL_BUFFER_SIZE));

  generator->initTdbFields(exe_util_tdb);

  if (getDropTableOnDealloc())
    exe_util_tdb->setDropOnDealloc(TRUE);

  if (getHiveExternalTable())
    exe_util_tdb->setIsExternal(TRUE);

  if (getIfExists())
    exe_util_tdb->setIfExists(TRUE);

  if (getTableNotExists())
    exe_util_tdb->setTableNotExists(TRUE);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  // no tupps are returned 
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  generator->setTransactionFlag(0); // transaction is not needed.
  
  return 0;
}

short ExeUtilHiveQuery::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();
  generator->appendAtEnd();
  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);
  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);
  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;
  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }
  char * hive_query = 
    space->AllocateAndCopyToAlignedSpace (hiveQuery(), 0);
  Lng32 hive_query_len = hiveQuery().length();
  ComTdbExeUtilHiveQuery * exe_util_tdb = 
    new(space) 
    ComTdbExeUtilHiveQuery(hive_query, hive_query_len,
                           (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
                           (ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
                           (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
                           (queue_index)getDefault(GEN_DDL_SIZE_UP),
                           getDefault(GEN_DDL_NUM_BUFFERS),
                           getDefault(GEN_DDL_BUFFER_SIZE));
  generator->initTdbFields(exe_util_tdb);
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);
  generator->setTransactionFlag(0); // transaction is not needed.
  return 0;
}
////////////////////////////////////////////////////////////////////
// class ExeUtilRegionStats
////////////////////////////////////////////////////////////////////
const char * ExeUtilRegionStats::getVirtualTableName()
{ return (clusterView_ ? "EXE_UTIL_CLUSTER_STATS__" : "EXE_UTIL_REGION_STATS__"); }

TrafDesc *ExeUtilRegionStats::createVirtualTableDesc()
{
  TrafDesc * table_desc = NULL;

  ComTdbExeUtilRegionStats rs;
  rs.setClusterView(clusterView_);

  if (displayFormat_)
    table_desc = ExeUtilExpr::createVirtualTableDesc();
  else
    table_desc = Generator::createVirtualTableDesc(
	 getVirtualTableName(),
	 NULL, // let it decide what heap to use
	 rs.getVirtTableNumCols(),
	 rs.getVirtTableColumnInfo(),
	 rs.getVirtTableNumKeys(),
	 rs.getVirtTableKeyInfo());

  return table_desc;
}
short ExeUtilRegionStats::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const int work_atp = 1;
  const int exe_util_row_atp_index = 2;

  // if selection pred, then assign work atp/atpindex to attrs.
  // Once selection pred has been generated, atp/atpindex will be
  // changed to returned atp/atpindex.
  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc, (NOT selectionPred().isEmpty()));
  if (rc)
    {
      return -1;
    }

  ex_expr * input_expr = 0;
  ULng32 inputRowLen = 0;

  if (inputColList_)
    {
      ValueIdList inputVIDList;
      ItemExpr * inputExpr = new(generator->wHeap())
        Cast(inputColList_, 
             new (generator->wHeap())
             SQLVarChar(generator->wHeap(), inputColList_->getValueId().getType().getNominalSize(),
                        inputColList_->getValueId().getType().supportsSQLnull()));
      
      inputExpr->bindNode(generator->getBindWA());
      inputVIDList.insert(inputExpr->getValueId());
      
      expGen->
        processValIdList(inputVIDList,
                         ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                         inputRowLen,
                         work_atp,
                         exe_util_row_atp_index
                         );
      
      expGen->
        generateContiguousMoveExpr(inputVIDList,
                                   0, // don't add conv nodes
                                   work_atp,
                                   exe_util_row_atp_index,
                                   ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                                   inputRowLen,
                                   &input_expr);
    }

  ex_expr *scanExpr = NULL;
  // generate tuple selection expression, if present
  if (NOT selectionPred().isEmpty())
    {
      ItemExpr* pred = selectionPred().rebuildExprTree(ITM_AND,TRUE,TRUE);
      expGen->generateExpr(pred->getValueId(),ex_expr::exp_SCAN_PRED,&scanExpr);

      // The output row will be returned as the last entry of the returned atp.
      // Change the atp and atpindex of the returned values to indicate that.
      expGen->assignAtpAndAtpIndex(getVirtualTableDesc()->getColumnList(),
                                   0, returnedDesc->noTuples()-1);
    }
  
  char * tableName = NULL;
  const NATable *naTable = 
    (getUtilTableDesc() ? getUtilTableDesc()->getNATable() : NULL);
  if (naTable &&
      (naTable->isHbaseRowTable() || naTable->isHbaseCellTable()))
    {
      tableName = space->AllocateAndCopyToAlignedSpace(
           GenGetQualifiedName(getTableName(),
                               FALSE, TRUE), 0);
    }
  else
    {
      if (FileScan::genTableName(generator, space, getTableName(),
                                 (getUtilTableDesc() ? getUtilTableDesc()->getNATable() : NULL),
                                 NULL, 
                                 TRUE, // quoted format
                                 tableName))
        {
          GenAssert(0,"genTableName failed");
        }
    }

  char *catName = space->allocateAndCopyToAlignedSpace(
       getTableName().getQualifiedNameObj().getCatalogName().data(),
       getTableName().getQualifiedNameObj().getCatalogName().length(), 0);
  char *schName = space->allocateAndCopyToAlignedSpace(
       getTableName().getQualifiedNameObj().getSchemaName().data(),
       getTableName().getQualifiedNameObj().getSchemaName().length(), 0);
  char *objName = space->allocateAndCopyToAlignedSpace(
       getTableName().getQualifiedNameObj().getObjectName().data(),
       getTableName().getQualifiedNameObj().getObjectName().length(), 0);
       
  ComTdbExeUtilRegionStats * exe_util_tdb = new(space) 
    ComTdbExeUtilRegionStats(
         tableName,
         catName, schName, objName,
	 input_expr,
	 inputRowLen,
         scanExpr,
	 workCriDesc,
	 exe_util_row_atp_index,
	 givenDesc,
	 returnedDesc,
	 (queue_index)64,
	 (queue_index)64,
	 4, 
	 64000); 
  generator->initTdbFields(exe_util_tdb);

  if (USE_UUID_AS_HBASE_TABLENAME && ComIsUserTable(getTableName()))
  {
    char * tablenameForUID = NULL;
    if (FileScan::genTableName(generator, space, getTableName(),
                  (getUtilTableDesc() ? getUtilTableDesc()->getNATable() : NULL),
                  NULL,
                  FALSE,
                  tablenameForUID,
                  TRUE))
    {
      GenAssert(0,"genTableName failed");
    }

    if (tablenameForUID)
    {
      exe_util_tdb->setReplaceNameByUID(TRUE);
      exe_util_tdb->setDataUIDName(tablenameForUID);
    }
  }

  exe_util_tdb->setIsIndex(isIndex_);

  exe_util_tdb->setDisplayFormat(displayFormat_);

  exe_util_tdb->setSummaryOnly(summaryOnly_);

  exe_util_tdb->setClusterView(clusterView_);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);
  
  // users should not start a transaction.
  generator->setTransactionFlag(0);
  
  return 0;
}



////////////////////////////////////////////////////////////////////
// class ExeUtilAvroStats
////////////////////////////////////////////////////////////////////
const char * ExeUtilAvroStats::getVirtualTableName()
{ return ("EXE_UTIL_AVRO_STATS__"); }

TrafDesc *ExeUtilAvroStats::createVirtualTableDesc()
{
  TrafDesc * table_desc = NULL;

  ComTdbExeUtilAvroStats rs;
  if (displayFormat_)
    table_desc = ExeUtilExpr::createVirtualTableDesc();
  else
    table_desc = Generator::createVirtualTableDesc(
         getVirtualTableName(),
	 NULL, // let it decide what heap to use
         rs.getVirtTableNumCols(),
         rs.getVirtTableColumnInfo(),
         rs.getVirtTableNumKeys(),
         rs.getVirtTableKeyInfo());
  
  return table_desc;
}
short ExeUtilAvroStats::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const int work_atp = 1;
  const int exe_util_row_atp_index = 2;

  // if selection pred, then assign work atp/atpindex to attrs.
  // Once selection pred has been generated, atp/atpindex will be
  // changed to returned atp/atpindex.
  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc, (NOT selectionPred().isEmpty()));
  if (rc)
    {
      return -1;
    }

  ex_expr * input_expr = 0;
  ULng32 inputRowLen = 0;

  ex_expr *scanExpr = NULL;
  // generate tuple selection expression, if present
  if (NOT selectionPred().isEmpty())
    {
      ItemExpr* pred = selectionPred().rebuildExprTree(ITM_AND,TRUE,TRUE);
      expGen->generateExpr(pred->getValueId(),ex_expr::exp_SCAN_PRED,&scanExpr);

      // The output row will be returned as the last entry of the returned atp.
      // Change the atp and atpindex of the returned values to indicate that.
      expGen->assignAtpAndAtpIndex(getVirtualTableDesc()->getColumnList(),
                                   0, returnedDesc->noTuples()-1);
    }
  
  const HHDFSTableStats* hTabStats = 
    getUtilTableDesc()->getClusteringIndex()->
    getNAFileSet()->getHHDFSTableStats();

  const NAString &tableDir = hTabStats->tableDir();
  char * rootDirLoc =
    space->allocateAndCopyToAlignedSpace
    (tableDir.data(), tableDir.length(), 0);

  char * tableName = NULL;
  tableName = space->AllocateAndCopyToAlignedSpace
    (generator->genGetNameAsAnsiNAString(getTableName()), 0);

  ComTdbExeUtilAvroStats * exe_util_tdb = new(space) 
    ComTdbExeUtilAvroStats(
         tableName,
	 input_expr,
	 inputRowLen,
         scanExpr,
         rootDirLoc,
	 workCriDesc,
	 exe_util_row_atp_index,
	 givenDesc,
	 returnedDesc,
	 (queue_index)64,
	 (queue_index)64,
	 4, 
	 64000); 

  generator->initTdbFields(exe_util_tdb);

  exe_util_tdb->setDisplayFormat(displayFormat_);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);
  
  // users should not start a transaction.
  generator->setTransactionFlag(0);

  return 0;
}

short ExeUtilGetExtSchema::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const int work_atp = 1;
  const int exe_util_row_atp_index = 2;

  // if selection pred, then assign work atp/atpindex to attrs.
  // Once selection pred has been generated, atp/atpindex will be
  // changed to returned atp/atpindex.
  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc, (NOT selectionPred().isEmpty()));
  if (rc)
    {
      return -1;
    }

  const HHDFSTableStats* hTabStats = 
    getUtilTableDesc()->getClusteringIndex()->
    getNAFileSet()->getHHDFSTableStats();

  const NAString &tableDir = hTabStats->tableDir();
  char * rootDirLoc =
    space->allocateAndCopyToAlignedSpace
    (tableDir.data(), tableDir.length(), 0);

  Queue * colNameList = NULL;
  Queue * colTypeList = NULL;
  char  * parqSchStr = NULL;
  const NATable * naTable = getUtilTableDesc()->getNATable();
  if (forWrite_)
    {
      FileScan::createHiveColNameAndTypeLists(
           generator, 
           (naTable->getHiveNAColumnArray().entries() > 0 ?
            naTable->getHiveNAColumnArray() :
            naTable->getNAColumnArray()),
           colNameList, colTypeList);
    }

  if (type_ == PARQUET)
    {
      NAString tab = 
        GenGetQualifiedName(naTable->getTableName());
      if (FileScan::createParqTableSchStr(
               generator, 
               tab,
               (naTable->getHiveNAColumnArray().entries() > 0 ?
                naTable->getHiveNAColumnArray() :
                naTable->getNAColumnArray()),
               parqSchStr))
        return -1;

      if (forWrite_ && naTable->hasCompositeColumns())
        colNameList = NULL;
    }

  char * tableName = NULL;
  tableName = space->AllocateAndCopyToAlignedSpace
    (generator->genGetNameAsAnsiNAString(getTableName()), 0);

  short type = ComTdbExeUtilGetExtSchema::UNKNOWN;
  if (type_ == PARQUET)
    type = ComTdbExeUtilGetExtSchema::PARQUET;
  else if (type_ == AVRO)
    type = ComTdbExeUtilGetExtSchema::AVRO;
  
  ComTdbExeUtilGetExtSchema * exe_util_tdb = new(space) 
    ComTdbExeUtilGetExtSchema(
         tableName,
         type,
         colNameList,
         colTypeList,
         parqSchStr,
         rootDirLoc,
	 workCriDesc,
	 exe_util_row_atp_index,
	 givenDesc,
	 returnedDesc,
	 (queue_index)64,
	 (queue_index)64,
	 4, 
	 64000); 

  generator->initTdbFields(exe_util_tdb);
  
  exe_util_tdb->setIsRead(forRead_);
  exe_util_tdb->setIsWrite(forWrite_);

  if ((type_ == PARQUET) &&
      (CmpCommon::getDefault(PARQUET_LEGACY_TIMESTAMP_FORMAT) == DF_ON))
    {
      exe_util_tdb->setParquetLegacyTS(TRUE);
    }

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);
  
  // users should not start a transaction.
  generator->setTransactionFlag(0);

  return 0;
}

////////////////////////////////////////////////////////////////////
// class ExeUtilLobInfo
////////////////////////////////////////////////////////////////////
const char * ExeUtilLobInfo::getVirtualTableName()
{ return ("EXE_UTIL_LOB_INFO__"); }

TrafDesc *ExeUtilLobInfo::createVirtualTableDesc()
{
  TrafDesc * table_desc = NULL;
   if (tableFormat_)
    table_desc = Generator::createVirtualTableDesc(
	 getVirtualTableName(),
	 NULL, // let it decide what heap to use
	 ComTdbExeUtilLobInfo::getVirtTableNumCols(),
	 ComTdbExeUtilLobInfo::getVirtTableColumnInfo(),
	 ComTdbExeUtilLobInfo::getVirtTableNumKeys(),
	 ComTdbExeUtilLobInfo::getVirtTableKeyInfo());
   else
     table_desc = ExeUtilExpr::createVirtualTableDesc();
  
  return table_desc;
}

short ExeUtilLobInfo::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const int work_atp = 1;
  const int exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  NAString tn = "\"";
  tn += getTableName().getQualifiedNameObj().getCatalogName();
  tn += "\".";
  tn += getTableName().getQualifiedNameObj().getSchemaName();
  tn += ".";
  tn += getTableName().getQualifiedNameObj().getObjectName();
  char * tablename = space->AllocateAndCopyToAlignedSpace(tn, 0);
  

  char * schemaName = 
    space->AllocateAndCopyToAlignedSpace
    (getTableName().getQualifiedNameObj().getSchemaName(), 0);
  char * catalogName =
     space->AllocateAndCopyToAlignedSpace
    (getTableName().getQualifiedNameObj().getCatalogName(), 0);
  char * objectName =
    space->AllocateAndCopyToAlignedSpace
    (getTableName().getQualifiedNameObj().getObjectName(), 0);

  char *lobColArray = NULL;
  char * lobNumArray = NULL;
  char * lobLocArray = NULL;
  char * lobTypeArray = NULL;
  char * lobInlineMaxLenArray = NULL;
  const NATable * naTable = getUtilTableDesc()->getNATable();
  Lng32 numLOBs = 0;

  if (naTable->hasLobColumn())
    {
      for (CollIndex i = 0; i < naTable->getNAColumnArray().entries(); i++)
	{
	  
	  NAColumn *col = naTable->getNAColumnArray()[i];
	  if (col->getType()->isLob())
	    {
	      numLOBs++;	     
	    } // if
	} // for
    }

  if (numLOBs > 0)
    {
      lobColArray = space->allocateAlignedSpace(numLOBs*LOBINFO_MAX_FILE_LEN);
      lobNumArray = space->allocateAlignedSpace(numLOBs*2);
      lobLocArray = space->allocateAlignedSpace(numLOBs * LOBINFO_MAX_FILE_LEN);
      lobTypeArray = space->allocateAlignedSpace(numLOBs * 4);
      lobInlineMaxLenArray = space->allocateAlignedSpace(numLOBs * sizeof(Int32));

      const NATable * naTable = getUtilTableDesc()->getNATable();
      CollIndex j = 0;

      for (CollIndex i = 0; i < naTable->getNAColumnArray().entries(); i++)
	{

	  NAColumn *col = naTable->getNAColumnArray()[i];
	  if (col->getType()->isLob())
	    {
              strcpy(&lobColArray[j*LOBINFO_MAX_FILE_LEN], col->getColName());
	      *(short*)(&lobNumArray[2*j]) = col->lobNum();

	      strcpy(&lobLocArray[j*LOBINFO_MAX_FILE_LEN], col->lobStorageLocation());
              *(Int32 *)(&lobTypeArray[4*j]) = col->lobStorageType();

              
              *(Int32 *)(&lobInlineMaxLenArray[4*j]) = col->lobInlinedDataMaxLen();

	      j++;
	    }
	}

    } // lobs

  Lng32 hdfsPort = (Lng32)CmpCommon::getDefaultNumeric(LOB_HDFS_PORT);
  const char* f = ActiveSchemaDB()->getDefaults().
    getValue(LOB_HDFS_SERVER);
  char * hdfsServer = space->allocateAlignedSpace(strlen(f) + 1);
  strcpy(hdfsServer, f);
  
  ComTdbExeUtilLobInfo *exe_util_tdb = new(space) 
    ComTdbExeUtilLobInfo(
         tablename,
         objectUID_,
         numLOBs,
         lobColArray,
         lobNumArray,
         lobLocArray,
         lobTypeArray,
         lobInlineMaxLenArray,
         hdfsPort,
         hdfsServer,
         tableFormat_,
	 workCriDesc,
	 exe_util_row_atp_index,
	 givenDesc,
	 returnedDesc,
	 (queue_index)64,
	 (queue_index)64,
	 4, 
	 64000); 

  exe_util_tdb->setUseLibHdfs(CmpCommon::getDefault(USE_LIBHDFS) == DF_ON);
  exe_util_tdb->setNumLOBdatafiles(naTable->getNumLOBdatafiles());

  if ((naTable->hasLobColumn()) && (naTable->lobV2()))
    {
      exe_util_tdb->setLobV2(TRUE);

      char *lobDmlOptionsStr = space->allocateAlignedSpace(sizeof(ExpLobOperV2::DMLOptions));
      memset(lobDmlOptionsStr, sizeof(ExpLobOperV2::DMLOptions), '\0');
      
      ExpLobOperV2::DMLOptions *lobDmlOptions = 
        (ExpLobOperV2::DMLOptions *)lobDmlOptionsStr;  

      lobDmlOptions->lobDataInHbaseColLen =
        getUtilTableDesc()->getNATable()->getLobChunksTableDataInHbaseColLen();
      lobDmlOptions->numLOBdatafiles = naTable->getNumLOBdatafiles();
      
      const NAString &schName = getUtilTableDesc()->getNATable()->getTableName().getSchemaNameAsAnsiString();
      lobDmlOptions->chunkTableSchNameLen = schName.length();
      lobDmlOptions->chunkTableSch[0] = 0;
      if ((schName.length() > 0) && (schName.length() <= 512) && (NOT schName.isNull()))
        strcpy(lobDmlOptions->chunkTableSch, schName.data());
      
      exe_util_tdb->setLobV2(TRUE);
      exe_util_tdb->setLobDmlOptions(lobDmlOptionsStr);
    }
  else if ((naTable->hasLobColumn()) && (NOT naTable->lobV2()))
    {
      exe_util_tdb->setNumLOBdatafiles(1);
    }

  generator->initTdbFields(exe_util_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);
  
  // users should not start a transaction.
  generator->setTransactionFlag(0);
  return 0;
}


// See ControlRunningQuery

/////////////////////////////////////////////////////////
//
// ExeUtilLongRunning::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilLongRunning::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  char * tablename =  tablename = space->AllocateAndCopyToAlignedSpace
    (GenGetQualifiedName(getTableName()),0);

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples(), space);
  
   ComTdbExeUtilLongRunning * exe_util_tdb = new(space) 
     ComTdbExeUtilLongRunning(tablename, 
			      strlen(tablename),
			      returnedDesc,  // no rows returned hence using the returnedDesc 
			                     // and temporary tupp
			      1,	     // work_atp_index,
			      givenDesc,
			      returnedDesc,
			      (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
			      (queue_index)getDefault(GEN_DDL_SIZE_UP),
			      (Lng32) getDefault(GEN_DDL_NUM_BUFFERS),
			      getDefault(GEN_DDL_BUFFER_SIZE));
   
   exe_util_tdb->setMultiCommitSize(getMultiCommitSize());
   generator->initTdbFields(exe_util_tdb);
   
   if(!generator->explainDisabled()) {
     generator->setExplainTuple
       (addExplainInfo(exe_util_tdb, 0, 0, generator));
   }
   
  // Set the LongRunningType and construct the corresponding queries.
   if (type_ == ExeUtilLongRunning::LR_DELETE) 
    {
      exe_util_tdb->setLongRunningDelete(TRUE);

      // Construct the Query without  the CK and set the
      // datamember lruStmt_
      NAString lruStmtString = constructLRUDeleteStatement
	(FALSE); // withCK 

      lruStmt_ = space->allocateAlignedSpace(lruStmtString.length() + 1);
      lruStmtLen_ = lruStmtString.length();
      strcpy(lruStmt_, lruStmtString.data());

      
      exe_util_tdb->setLruStmt(lruStmt_);
      exe_util_tdb->setLruStmtLen(lruStmtLen_);
      
      // Construct the Query with the CK and set the
      // datamember lruStmtWithCK_      
      NAString lruStmtWithCKString = constructLRUDeleteStatement
	(TRUE); // withCK

      lruStmtWithCK_ = space->allocateAlignedSpace
	(lruStmtWithCKString.length() + 1);
      lruStmtWithCKLen_ = lruStmtWithCKString.length();
      strcpy(lruStmtWithCK_, lruStmtWithCKString.data());
      
      exe_util_tdb->setLruStmtWithCK(lruStmtWithCK_);
      exe_util_tdb->setLruStmtWithCKLen(lruStmtWithCKLen_);
      exe_util_tdb->setPredicate(space, predicate_);
      
      NAString defaultSch = ActiveSchemaDB()->getDefaultSchema().getSchemaName();
      char *pDefaultSch = space->allocateAlignedSpace
      (defaultSch.length() + 1);
      strcpy(pDefaultSch,defaultSch.data());
      exe_util_tdb->setDefaultSchemaName(pDefaultSch);
      NAString defaultCat = ActiveSchemaDB()->getDefaultSchema().getCatalogName();
      char *pDefaultCat = space->allocateAlignedSpace
      (defaultCat.length() + 1);
      strcpy(pDefaultCat,defaultCat.data());
      exe_util_tdb->setDefaultCatalogName(pDefaultCat);
      // If the table we are deleting from is an IUD log table,
      // we need to set the parserflags to accept the special
      // table type and the quoted column names
      if (getTableName().getSpecialType() == ExtendedQualName::IUD_LOG_TABLE)
        exe_util_tdb->setUseParserflags(TRUE);
    }  
  else if (type_ == ExeUtilLongRunning::LR_UPDATE)
    exe_util_tdb->setLongRunningUpdate(TRUE);
  else if (type_ == ExeUtilLongRunning::LR_INSERT_SELECT)
    exe_util_tdb->setLongRunningInsertSelect(TRUE);

   if (CmpCommon::getDefault(COMP_BOOL_190) == DF_ON)
     exe_util_tdb->setLongRunningQueryPlan(TRUE);

   // no tupps are returned 
   generator->setCriDesc(givenDesc, Generator::DOWN);
   generator->setCriDesc(returnedDesc, Generator::UP);
   generator->setGenObj(this, exe_util_tdb);
   
   // Set the transaction flag.
   if (xnNeeded())
     {
       generator->setTransactionFlag(-1);
     }

   return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilShowSet::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilShowSet::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * param1 = NULL;
  char * param2 = NULL;
  
  ssdName_.toUpper();
  if (NOT ssdName_.isNull())
    {
      param1 = space->allocateAlignedSpace(ssdName_.length() + 1);
      strcpy(param1, ssdName_.data());
    }

  UInt16 type = 0;
  if (type_ == ALL_DEFAULTS_)
    type = ComTdbExeUtilShowSet::ALL_;
  else if (type_ == EXTERNALIZED_DEFAULTS_)
    type = ComTdbExeUtilShowSet::EXTERNALIZED_;
  else if (type_ == SINGLE_DEFAULT_)
    type = ComTdbExeUtilShowSet::SINGLE_;
  
  ComTdbExeUtilShowSet * exe_util_tdb = new(space) 
    ComTdbExeUtilShowSet(
	 type,
	 param1, param2,
	 0, 0, // no work cri desc
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)64,
	 2, 
	 32000); //getDefault(GEN_DDL_BUFFER_SIZE));
  generator->initTdbFields(exe_util_tdb);
  
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  // no tupps are returned 
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  generator->setTransactionFlag(0);
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilAQR::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilAQR::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  short rc = processOutputRow(generator, 0, returnedDesc->noTuples()-1,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  Lng32 task = (Lng32)ComTdbExeUtilAQR::NONE_;
  if (task_ == GET_)
    task = (Lng32)ComTdbExeUtilAQR::GET_;
  else if (task_ == ADD_)
    task = (Lng32)ComTdbExeUtilAQR::ADD_;
  else if (task_ == DELETE_)
    task = (Lng32)ComTdbExeUtilAQR::DELETE_;
  else if (task_ == UPDATE_)
    task = (Lng32)ComTdbExeUtilAQR::UPDATE_;
  else if (task_ == CLEAR_)
    task = (Lng32)ComTdbExeUtilAQR::CLEAR_;
  else if (task_ == RESET_)
    task = (Lng32)ComTdbExeUtilAQR::RESET_;

  ComTdbExeUtilAQR * exe_util_tdb = new(space) 
    ComTdbExeUtilAQR(
	 task,
	 givenDesc,
	 returnedDesc,
	 (queue_index)8,
	 (queue_index)64,
	 2, 
	 32000); 
  generator->initTdbFields(exe_util_tdb);
  
  exe_util_tdb->setParams(sqlcode_, nskcode_, retries_, delay_, type_);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  // no tupps are returned 
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)),
			Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Reset the transaction flag.
  generator->setTransactionFlag(0);
  
  return 0;
}


/////////////////////////////////////////////////////////
//
// ExeUtilLobExtract::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilLobExtract::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;
  const Int32 work_row_atp_index = 2;

  ComTdb * child_tdb = NULL;
  ExplainTuple *childExplainTuple = NULL;
  if (child(0))
    {
      GenAssert(0, "LobExtract cannot have a child.");
    }

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  // Use stringParam1 to pass in target file .
  char * stringParam1 = NULL;
  if (NOT stringParam_.isNull())
    {
      stringParam1 = space->allocateAlignedSpace(stringParam_.length() + 1);
      strcpy(stringParam1, stringParam_.data());
    }

  const char *f = lobLocation_.data();

  char * stringParam2 = NULL;
  stringParam2 = space->allocateAlignedSpace(strlen(f) + 1);
  strcpy(stringParam2, f);

  char * stringParam3 = NULL;
  if (NOT stringParam3_.isNull())
    {
     stringParam3 =  space->allocateAlignedSpace(stringParam3_.length() + 1);
      strcpy(stringParam3, stringParam3_.data());
    }

  char * handle = NULL;
  Lng32 handleLen = 0;

  ExpTupleDesc * inputValsTupleDesc = 0;
  ValueIdList inputVIDList;
  ItemExpr *ie = NULL;
  if (objectUIDexpr_)
    {
      ie = new(generator->wHeap())
	Cast(handle_, 
             new (generator->wHeap()) 
             SQLChar(generator->wHeap(), 
                     handle_->getValueId().getType().getNominalSize(), FALSE));
      ie->bindNode(generator->getBindWA());
      inputVIDList.insert(ie->getValueId());      

      ie = new(generator->wHeap())
	Cast(bufAddrExpr_, 
             new (generator->wHeap()) 
             SQLLargeInt(generator->wHeap(), TRUE, FALSE));
      ie->bindNode(generator->getBindWA());
      inputVIDList.insert(ie->getValueId());            

      if (extractSizeAddrExpr_)
        {
          ie = new(generator->wHeap())
            Cast(extractSizeAddrExpr_, 
                 new (generator->wHeap()) 
                 SQLLargeInt(generator->wHeap(), TRUE, FALSE));
          ie->bindNode(generator->getBindWA());
          inputVIDList.insert(ie->getValueId());            
        }

      if (positionAddrExpr_)
        {
          ie = new(generator->wHeap())
            Cast(positionAddrExpr_, 
                 new (generator->wHeap()) 
                 SQLLargeInt(generator->wHeap(), TRUE, FALSE));
          ie->bindNode(generator->getBindWA());
          inputVIDList.insert(ie->getValueId());            
        }
    }
  else if (handle_)
    {
      ConstValue * cv = (ConstValue*)handle_;
      
      NAString h = *(cv->getRawText());
      handleLen = h.length();
      
      handle = space->allocateAlignedSpace(handleLen + 1);
      strcpy(handle, h.data());
    }

  ex_expr * input_expr = 0;
  ULng32 inputRowLen = 0;
  if (inputVIDList.entries() > 0)
    {
      expGen->
	processValIdList(inputVIDList,
			 ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
			 inputRowLen,
			 work_atp,
			 work_row_atp_index
			 );
      
      expGen->
	generateContiguousMoveExpr(inputVIDList,
				   0, // don't add conv nodes
				   work_atp,
				   work_row_atp_index,
				   ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
				   inputRowLen,
				   &input_expr,
                                   &inputValsTupleDesc,
                                   ExpTupleDesc::LONG_FORMAT);

      workCriDesc->setTupleDescriptor(work_row_atp_index, inputValsTupleDesc);
    }

  Lng32 lst = 0; 
  lst = (Lng32)(CmpCommon::getDefaultNumeric(LOB_STORAGE_TYPE));
  Lng32 hdfsPort = (Lng32)CmpCommon::getDefaultNumeric(LOB_HDFS_PORT);
  const char* hs = ActiveSchemaDB()->getDefaults().
    getValue(LOB_HDFS_SERVER);
  char * hdfsServer = space->allocateAlignedSpace(strlen(hs) + 1);
  strcpy(hdfsServer, hs);

  ComTdbExeUtilLobExtract * exe_util_tdb = new(space) 
    ComTdbExeUtilLobExtract
    (
     handle,
     handleLen,
     (toType_ == TO_BUFFER_ ? ComTdbExeUtilLobExtract::TO_BUFFER_ :
      (toType_ == RETRIEVE_LENGTH_ ? ComTdbExeUtilLobExtract::RETRIEVE_LENGTH_ :
       (toType_ == RETRIEVE_HDFSFILENAME_ ? ComTdbExeUtilLobExtract::RETRIEVE_HDFSFILENAME_ :
        (toType_ == RETRIEVE_OFFSET_ ? ComTdbExeUtilLobExtract::RETRIEVE_OFFSET_ :
       (toType_ == TO_STRING_ ? ComTdbExeUtilLobExtract::TO_STRING_ :
	(toType_ == TO_FILE_ ? ComTdbExeUtilLobExtract::TO_FILE_ :
	(toType_ == TO_EXTERNAL_FROM_STRING_ ? ComTdbExeUtilLobExtract::TO_EXTERNAL_FROM_STRING_ :
	 (toType_ == TO_EXTERNAL_FROM_FILE_ ? ComTdbExeUtilLobExtract::TO_EXTERNAL_FROM_FILE_ :
	  ComTdbExeUtilLobExtract::NOOP_)))))))),
     bufAddr_,
     extractSizeAddr_,
     intParam_,
     intParam2_,
     lst,
     stringParam1,
     stringParam2,
     stringParam3,
     hdfsServer,
     hdfsPort,
     input_expr,
     inputRowLen,
     workCriDesc,
     work_row_atp_index,
     givenDesc,
     returnedDesc,
     (queue_index)8,
     (queue_index)128,
     2,
     32000);

  exe_util_tdb->setUseLibHdfs(CmpCommon::getDefault(USE_LIBHDFS) == DF_ON);
  if (handleInStringFormat_)
    exe_util_tdb->setHandleInStringFormat(TRUE);

  if (handle_ == NULL)
    exe_util_tdb->setSrcIsFile(TRUE);

  if (intParam_ == ExtractFileActionType::ERROR_IF_NOT_EXISTS)
    exe_util_tdb->setErrorIfNotExists(TRUE);   
  else
    exe_util_tdb->setErrorIfNotExists(FALSE);

  if (intParam2_ == ExtractFileActionType::TRUNCATE_EXISTING)
    exe_util_tdb->setTruncateExisting(TRUE);
  else
    exe_util_tdb->setTruncateExisting(FALSE);
  if (intParam2_ == ExtractFileActionType::APPEND_OR_CREATE)
    exe_util_tdb->setAppendOrCreate(TRUE);
  else
    exe_util_tdb->setAppendOrCreate(FALSE);

  exe_util_tdb->setWithCreate(withCreate_);

  exe_util_tdb->setNumLOBdatafiles(numLOBdatafiles_);

  if (getUtilTableDesc()->getNATable()->lobV2())
    {
      char *lobDmlOptionsStr = space->allocateAlignedSpace(sizeof(ExpLobOperV2::DMLOptions));
      memset(lobDmlOptionsStr, sizeof(ExpLobOperV2::DMLOptions), '\0');
      ExpLobOperV2::DMLOptions *lobDmlOptions = (ExpLobOperV2::DMLOptions *)lobDmlOptionsStr;  

      NAColumn *nac = NULL;
      if (handleInStringFormat_)
        {
          char  lobHandle[2050];
          Lng32 lobHandleLen = 2000;
          
          Int32 rc = ExpLOBoper::genLOBhandleFromHandleString
            (handle, handleLen,
             lobHandle,
             lobHandleLen);
          
          Int32 lobNum = 0;
          ExpLOBoper::extractFromLOBhandle(
               NULL, NULL, &lobNum, NULL, NULL, NULL, NULL, NULL,
               lobHandle, lobHandleLen);
          
          nac = getUtilTableDesc()->getNATable()->getNAColumnArray().getColumn(lobNum);
          
          lobDmlOptions->lobInlinedDataMaxLen = nac->lobInlinedDataMaxLen();
          lobDmlOptions->lobHbaseDataMaxLen = nac->lobHbaseDataMaxLen();
        }
      
      lobDmlOptions->lobHbaseDataMaxLen = ((NATable*)(getUtilTableDesc()->getNATable()))->lobHbaseDataMaxLen();
      lobDmlOptions->lobInlinedDataMaxLen = ((NATable*)(getUtilTableDesc()->getNATable()))->lobInlinedDataMaxLen();

      lobDmlOptions->lobMaxChunkMemSize = 
        (Int64)CmpCommon::getDefaultNumeric(LOB_MAX_CHUNK_MEM_SIZE)*1024*1024;
      lobDmlOptions->lobDataInHbaseColLen =
        getUtilTableDesc()->getNATable()->getLobChunksTableDataInHbaseColLen();
      lobDmlOptions->numLOBdatafiles = numLOBdatafiles_;

      lobDmlOptions->setLobXnOper(CmpCommon::getDefault(TRAF_LOB_XN_OPER) == DF_ON);
      const NAString &schName = getUtilTableDesc()->getNATable()->getTableName().getSchemaNameAsAnsiString();
      lobDmlOptions->chunkTableSchNameLen = schName.length();
      lobDmlOptions->chunkTableSch[0] = 0;
      if ((schName.length() > 0) && (schName.length() <= 512) && (NOT schName.isNull()))
        strcpy(lobDmlOptions->chunkTableSch, schName.data());
 
      exe_util_tdb->setLobV2(TRUE);
      exe_util_tdb->setLobDmlOptions(lobDmlOptionsStr);
    }

  generator->initTdbFields(exe_util_tdb);

  if (child_tdb)
    exe_util_tdb->setChildTdb(child_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, childExplainTuple, 0, generator));
  }
  if (toType_ == RETRIEVE_LENGTH_)
    {
      exe_util_tdb->setRetrieveLength(TRUE);
    }
  if (toType_ == RETRIEVE_HDFSFILENAME_)
    {
      exe_util_tdb->setRetrieveHdfsFileName(TRUE);
    }
  if (toType_ == RETRIEVE_OFFSET_)
    {
      exe_util_tdb->setRetrieveOffset(TRUE);
    }
  exe_util_tdb->setTotalBufSize(CmpCommon::getDefaultNumeric(LOB_MAX_CHUNK_MEM_SIZE)*1024*1024);

  if (getUtilTableDesc()->getNATable()->lobV2())
    exe_util_tdb->setLobV2(TRUE);

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  generator->setTransactionFlag(0);
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilLobUpdate::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilLobUpdate::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;
  const Int32 work_row_atp_index = 2;

  ComTdb * child_tdb = NULL;
  ExplainTuple *childExplainTuple = NULL;
  if (child(0))
    {
      GenAssert(0, "LobUpdate cannot have a child.");
    }

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * handle = NULL;
  Lng32 handleLen = 0;
  ExpTupleDesc * inputValsTupleDesc = 0;
  ValueIdList inputVIDList;
  ItemExpr *ie = NULL;
  if (objectUIDexpr_)
    {
      ie = new(generator->wHeap())
	Cast(handle_, 
             new (generator->wHeap()) 
             SQLChar(generator->wHeap(), 
                     handle_->getValueId().getType().getNominalSize(), FALSE));
      ie->bindNode(generator->getBindWA());
      inputVIDList.insert(ie->getValueId());      

      ie = new(generator->wHeap())
	Cast(bufAddrExpr_, 
             new (generator->wHeap()) 
             SQLLargeInt(generator->wHeap(), TRUE, FALSE));
      ie->bindNode(generator->getBindWA());
      inputVIDList.insert(ie->getValueId());            

      if (extractSizeAddrExpr_)
        {
          ie = new(generator->wHeap())
            Cast(extractSizeAddrExpr_, 
                 new (generator->wHeap()) 
                 SQLLargeInt(generator->wHeap(), TRUE, FALSE));
          ie->bindNode(generator->getBindWA());
          inputVIDList.insert(ie->getValueId());            
        }

      if (positionAddrExpr_)
        {
          ie = new(generator->wHeap())
            Cast(positionAddrExpr_, 
                 new (generator->wHeap()) 
                 SQLLargeInt(generator->wHeap(), TRUE, FALSE));
          ie->bindNode(generator->getBindWA());
          inputVIDList.insert(ie->getValueId());            
        }
    }
  else if (handle_)
    {
      ConstValue * cv = (ConstValue*)handle_;
      
      NAString h = *(cv->getRawText());
      handleLen = h.length();
  
      handle = space->allocateAlignedSpace(handleLen + 1);
      strcpy(handle, h.data());
    }

  const char *f = lobLocation_.data();
  char *lobLoc = space->allocateAlignedSpace(strlen(f) + 1);
  strcpy(lobLoc, f);

  ex_expr * input_expr = 0;
  ULng32 inputRowLen = 0;
  if (inputVIDList.entries() > 0)
    {
      expGen->
	processValIdList(inputVIDList,
			 ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
			 inputRowLen,
			 work_atp,
			 work_row_atp_index
			 );
      
      expGen->
	generateContiguousMoveExpr(inputVIDList,
				   0, // don't add conv nodes
				   work_atp,
				   work_row_atp_index,
				   ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
				   inputRowLen,
				   &input_expr,
                                   &inputValsTupleDesc,
                                   ExpTupleDesc::LONG_FORMAT);

      workCriDesc->setTupleDescriptor(work_row_atp_index, inputValsTupleDesc);
    }

  Lng32 lst = 0;

  Lng32 hdfsPort = (Lng32)CmpCommon::getDefaultNumeric(LOB_HDFS_PORT);
  const char* hs = ActiveSchemaDB()->getDefaults().
    getValue(LOB_HDFS_SERVER);
  char * hdfsServer = space->allocateAlignedSpace(strlen(hs) + 1);
  strcpy(hdfsServer, hs);

  ComTdbExeUtilLobUpdate * exe_util_lobupdate_tdb = new(space) 
    ComTdbExeUtilLobUpdate
    (
     handle,
     handleLen,
     (fromType_ == FROM_BUFFER_ ? ComTdbExeUtilLobUpdate::FROM_BUFFER_ :
       (fromType_ == FROM_STRING_ ? ComTdbExeUtilLobUpdate::FROM_STRING_ :
	(fromType_ == FROM_EXTERNAL_ ? ComTdbExeUtilLobUpdate::FROM_EXTERNAL_ :
	  ComTdbExeUtilLobUpdate::NOOP_))),
     bufAddr_,
     updateSize_,
     lst,
     hdfsServer,
     hdfsPort,
     lobLoc,
     input_expr,
     inputRowLen,
     workCriDesc,
     work_row_atp_index,
     givenDesc,
     returnedDesc,
     (queue_index)8,
     (queue_index)128,
     2,
     32000);

  exe_util_lobupdate_tdb->setUseLibHdfs(CmpCommon::getDefault(USE_LIBHDFS) == DF_ON);
  if (handleInStringFormat_)
    exe_util_lobupdate_tdb->setHandleInStringFormat(TRUE);
  exe_util_lobupdate_tdb->setUseHdfsWriteLock(CmpCommon::getDefault(USE_HDFS_WRITE_LOCK) == DF_ON);
  exe_util_lobupdate_tdb->setHdfsWriteLockTimeout(CmpCommon::getDefaultLong(HDFS_WRITE_LOCK_TIMEOUT_IN_SECS));
  if (updateAction_ == UpdateActionType::ERROR_IF_EXISTS_)
    exe_util_lobupdate_tdb->setErrorIfExists(TRUE);   
  else
    exe_util_lobupdate_tdb->setErrorIfExists(FALSE);

  if (updateAction_ == UpdateActionType::TRUNCATE_EXISTING_)
    exe_util_lobupdate_tdb->setTruncate(TRUE);
  else
    exe_util_lobupdate_tdb->setTruncate(FALSE);
  if (updateAction_ == UpdateActionType::REPLACE_)
    exe_util_lobupdate_tdb->setReplace(TRUE);
  else
    exe_util_lobupdate_tdb->setReplace(FALSE);
  if (updateAction_ == UpdateActionType::APPEND_)
    exe_util_lobupdate_tdb->setAppend(TRUE);
  else
    exe_util_lobupdate_tdb->setAppend(FALSE);
  if((ActiveSchemaDB()->getDefaults()).getToken(LOB_LOCKING) == DF_ON)
    exe_util_lobupdate_tdb->setLobLocking(TRUE);
  else
    exe_util_lobupdate_tdb->setLobLocking(FALSE);

  exe_util_lobupdate_tdb->setNumLOBdatafiles(numLOBdatafiles_);
  if (getUtilTableDesc()->getNATable()->lobV2())
    {
      char *lobDmlOptionsStr = space->allocateAlignedSpace(sizeof(ExpLobOperV2::DMLOptions));
      memset(lobDmlOptionsStr, sizeof(ExpLobOperV2::DMLOptions), '\0');
      ExpLobOperV2::DMLOptions *lobDmlOptions = (ExpLobOperV2::DMLOptions *)lobDmlOptionsStr;  

      if (handleInStringFormat_)
        {
          char  lobHandle[2050];
          Lng32 lobHandleLen = 2000;
          
          Int32 rc = ExpLOBoper::genLOBhandleFromHandleString
            (handle, handleLen,
             lobHandle,
             lobHandleLen);
          
          Int32 lobNum = 0;
          ExpLOBoper::extractFromLOBhandle(
               NULL, NULL, &lobNum, NULL, NULL, NULL, NULL, NULL,
               lobHandle, lobHandleLen);
          
          NAColumn *nac = getUtilTableDesc()->getNATable()->getNAColumnArray().getColumn(lobNum);
          
          lobDmlOptions->lobInlinedDataMaxLen = nac->lobInlinedDataMaxLen();
          lobDmlOptions->lobHbaseDataMaxLen = nac->lobHbaseDataMaxLen();
        }

      lobDmlOptions->lobHbaseDataMaxLen = ((NATable*)(getUtilTableDesc()->getNATable()))->lobHbaseDataMaxLen();
      lobDmlOptions->lobInlinedDataMaxLen = ((NATable*)(getUtilTableDesc()->getNATable()))->lobInlinedDataMaxLen();

      lobDmlOptions->lobMaxChunkMemSize = 
        (Int64)CmpCommon::getDefaultNumeric(LOB_MAX_CHUNK_MEM_SIZE)*1024*1024;
      lobDmlOptions->lobDataInHbaseColLen =
        getUtilTableDesc()->getNATable()->getLobChunksTableDataInHbaseColLen();
      lobDmlOptions->numLOBdatafiles = numLOBdatafiles_;

      lobDmlOptions->setLobXnOper(CmpCommon::getDefault(TRAF_LOB_XN_OPER) == DF_ON);
 
      const NAString &schName =  getUtilTableDesc()->getNATable()->getTableName().getSchemaNameAsAnsiString();
      lobDmlOptions->chunkTableSchNameLen = schName.length();
      lobDmlOptions->chunkTableSch[0] = 0;
      if ((schName.length() > 0) && (schName.length() <= 512) && (NOT schName.isNull()))
        strcpy(lobDmlOptions->chunkTableSch, schName.data());
      
      exe_util_lobupdate_tdb->setLobV2(TRUE);
      exe_util_lobupdate_tdb->setLobDmlOptions(lobDmlOptionsStr);
    }

  generator->initTdbFields(exe_util_lobupdate_tdb);

  if (child_tdb)
    exe_util_lobupdate_tdb->setChildTdb(child_tdb);

  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_lobupdate_tdb, childExplainTuple, 0, generator));
  }
 
  exe_util_lobupdate_tdb->setTotalBufSize(CmpCommon::getDefaultNumeric(LOB_MAX_CHUNK_MEM_SIZE)*1024*1024);
  exe_util_lobupdate_tdb->setLobMaxSize( CmpCommon::getDefaultNumeric(LOB_MAX_SIZE) * 1024 * 1024);
  exe_util_lobupdate_tdb->setLobMaxChunkSize(CmpCommon::getDefaultNumeric(LOB_MAX_CHUNK_MEM_SIZE)*1024*1024);
  if ((CmpCommon::getDefaultNumeric(LOB_GC_LIMIT_SIZE) >=0))
    exe_util_lobupdate_tdb->setLobGCLimit(CmpCommon::getDefaultNumeric(LOB_GC_LIMIT_SIZE)*1024*1024);
  else
    exe_util_lobupdate_tdb->setLobGCLimit(CmpCommon::getDefaultNumeric(LOB_GC_LIMIT_SIZE));
                                           
                                        
  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_lobupdate_tdb);

  // Set the transaction flag.
  if (xnNeeded())
    {
       generator->setTransactionFlag(-1);
    }
  
  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilLobShowddl::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilLobShowddl::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }
  
  NAString tn = "\"";
  tn += getTableName().getQualifiedNameObj().getCatalogName();
  tn += "\".";
  tn += "\"";
  tn += getTableName().getQualifiedNameObj().getSchemaName();
  tn += "\"";
  tn += ".";
  tn += "\"";
  tn += getTableName().getQualifiedNameObj().getObjectName();
  tn += "\"";
  char * tablename0 = space->AllocateAndCopyToAlignedSpace(tn, 0);

  char * tablename = 
    space->AllocateAndCopyToAlignedSpace
	(generator->genGetNameAsAnsiNAString(getTableName()), 0);

  char * schemaName = 
    space->AllocateAndCopyToAlignedSpace
    (getTableName().getQualifiedNameObj().getSchemaNameAsAnsiString(), 0);

  char * catname = 
    space->AllocateAndCopyToAlignedSpace
    (getTableName().getQualifiedNameObj().getCatalogName(), 0);

  char * objectName = 
    space->AllocateAndCopyToAlignedSpace
    (getTableName().getQualifiedNameObj().getObjectName(), 0);

  char * lobNumArray = NULL;
  char * lobLocArray = NULL;
  char * lobTypeArray = NULL;

  const NATable * naTable = getUtilTableDesc()->getNATable();
  Lng32 numLOBs = 0;
  short maxLocLen = 0;
  if (naTable->hasLobColumn())
    {
      for (CollIndex i = 0; i < naTable->getNAColumnArray().entries(); i++)
	{
	  
	  NAColumn *col = naTable->getNAColumnArray()[i];
	  if (col->getType()->isLob())
	    {
	      numLOBs++;

	      maxLocLen = MAXOF(maxLocLen, 
				(short)strlen(col->lobStorageLocation()) + 1);
              
	    } // if
	} // for
    }

  if (numLOBs > 0)
    {
      lobNumArray = space->allocateAlignedSpace(numLOBs*2);
      lobLocArray = space->allocateAlignedSpace(numLOBs * maxLocLen);
      lobTypeArray = space->allocateAlignedSpace(numLOBs * 4);
      const NATable * naTable = getUtilTableDesc()->getNATable();
      CollIndex j = 0;

      for (CollIndex i = 0; i < naTable->getNAColumnArray().entries(); i++)
	{

	  NAColumn *col = naTable->getNAColumnArray()[i];
	  if (col->getType()->isLob())
	    {
	      *(short*)(&lobNumArray[2*j]) = col->lobNum();
	      strcpy(&lobLocArray[j*maxLocLen], col->lobStorageLocation());
              *(Int32 *)(&lobTypeArray[4*j]) =  col->lobStorageType();
	      j++;
	    }
	}
    }

  ComTdbExeUtil * exe_util_tdb = new(space) 
    ComTdbExeUtilLobShowddl
    (
     tablename,
     schemaName,
     strlen(schemaName),
     objectUID_,
     numLOBs,
     lobNumArray,
     lobLocArray,
     lobTypeArray,
     naTable->getNumLOBdatafiles(),
     maxLocLen,
     (short)sdOptions_,
     givenDesc,
     returnedDesc,
     (queue_index)8,
     (queue_index)128,
     2,
     32000);

  exe_util_tdb->setUseLibHdfs(CmpCommon::getDefault(USE_LIBHDFS) == DF_ON);

  if ((naTable->hasLobColumn()) && (naTable->lobV2()))
    exe_util_tdb->setLobV2(TRUE);

  generator->initTdbFields(exe_util_tdb);
  
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  // Set the transaction flag.
  generator->setTransactionFlag(0);
  
  return 0;
}
// sss #endif

// -----------------------------------------------------------------------
// HiveMDaccessFunc methods
// -----------------------------------------------------------------------
const char * HiveMDaccessFunc::getVirtualTableName()
{
  if (mdType_ == "TABLES")
    return "HIVEMD_TABLES__";
  else if (mdType_ == "COLUMNS")
    return "HIVEMD_COLUMNS__";
  else if (mdType_ == "PKEYS")
    return "HIVEMD_PKEYS__";
  else if (mdType_ == "FKEYS")
    return "HIVEMD_FKEYS__";
  else if (mdType_ == "VIEWS")
    return "HIVEMD_VIEWS__";
  else if (mdType_ == "ALIAS")
    return "HIVEMD_ALIAS__";
  else if (mdType_ == "SYNONYMS")
    return "HIVEMD_SYNONYMS__";
  else if (mdType_ == "SYSTEM_TABLES")
    return "HIVEMD_SYSTEM_TABLES__";
  else if (mdType_ == "SCHEMAS")
    return "HIVEMD_SCHEMAS__";
  else if (mdType_ == "OBJECTNAMES")
    return "HIVEMD_OBJECTNAMES__";
  else if (mdType_ == "OBJECTNAMETYPES")
    return "HIVEMD_OBJECTNAMETYPES__";
  else
    return "HIVEMD__"; 
}

NABoolean HiveMDaccessFunc::isHiveMD(const NAString &name)
{
  if (memcmp(name.data(), (char*)"HIVEMD_", strlen("HIVEMD_")) == 0)
    return TRUE;

  return FALSE;
}

NAString HiveMDaccessFunc::getMDType(const NAString &name)
{
  NAString mdType(name);
  mdType = mdType.remove(0, strlen("HIVEMD_"));
  mdType = mdType.strip(NAString::trailing, '_');

  return mdType;
}

TrafDesc *HiveMDaccessFunc::createVirtualTableDesc()
{
  TrafDesc * table_desc =
    Generator::createVirtualTableDesc(
				      getVirtualTableName(),
				      NULL, // let it decide what heap to use
				      ComTdbExeUtilHiveMDaccess::getVirtTableNumCols((char*)mdType_.data()),
				      ComTdbExeUtilHiveMDaccess::getVirtTableColumnInfo((char*)mdType_.data()),
				      ComTdbExeUtilHiveMDaccess::getVirtTableNumKeys((char*)mdType_.data()),
				      ComTdbExeUtilHiveMDaccess::getVirtTableKeyInfo((char*)mdType_.data()));

  return table_desc;
}

NABoolean HiveMDaccessFunc::isHiveSelExpr(ItemExpr * ie, Generator * generator)
{
  if (! ie)
    return FALSE;

  if (NOT (((ie->getOperatorType() >= ITM_AND) &&
	    (ie->getOperatorType() <= ITM_EXPONENT)) ||
	   (ie->getOperatorType() == ITM_LIKE) ||
	   (ie->getOperatorType() == ITM_BETWEEN) ||
	   (ie->getOperatorType() == ITM_CAST) ||
	   ((ie->getOperatorType() >= ITM_CONSTANT) &&
	    (ie->getOperatorType() <= ITM_DYN_PARAM))))
    return FALSE;

  if (ie->getOperatorType() == ITM_BASECOLUMN)
    {
      BaseColumn * bc = (BaseColumn*)ie;

      if (bc->getNAColumn()->getColName() != "TABLE_NAME")
	return FALSE;
    }

  if (ie->getOperatorType() == ITM_REFERENCE)
    {
      ColReference * cr = (ColReference *)ie;

      if (cr->getCorrNameObj().getQualifiedNameObj().getObjectName() != "TABLE_NAME")
	return FALSE;
    } 

  if (ie->getOperatorType() == ITM_DYN_PARAM)
    {
      // for now, no support for params to be passed to hivd md layer
      return FALSE;
    }

  for (Lng32 i = 0; i < ie->getArity(); i++)
    {
      if (NOT isHiveSelExpr(ie->child(i), generator))
	return FALSE;
    }

  return TRUE;
}

ItemExpr * HiveMDaccessFunc::createHiveSelExpr(ItemExpr * ie, Lng32 &paramNum,
					       Generator * generator)
{
  if (! ie)
    return NULL;
  
  for (Lng32 i = 0; i < ie->getArity(); i++)
    {
      ItemExpr * newChild = createHiveSelExpr(ie->child(i), paramNum, generator);
      
      if (newChild != ie->child(i))
	{
	  ie->setChild(i, newChild);
	}
    }
  
  if (ie->getOperatorType() == ITM_BASECOLUMN)
    {
      if (((BaseColumn*)ie)->getNAColumn()->getColName() == "TABLE_NAME")
	{
	  NAString col("TBL_NAME");
	  
	  RenameCol * rc = 
	    new(generator->wHeap()) 
	    RenameCol(NULL, 
		      new(generator->wHeap()) ColRefName(col, generator->wHeap()));
	  rc->synthTypeAndValueId(TRUE);
	  return rc;
	}
    }
  
  if (ie->getOperatorType() == ITM_REFERENCE)
    {
      if (((ColReference*)ie)->getCorrNameObj().getQualifiedNameObj().getObjectName() == "TABLE_NAME")
	{
	  NAString col("TBL_NAME");

	  RenameCol * rc = 
	    new(generator->wHeap()) 
	    RenameCol(NULL, 
		      new(generator->wHeap()) ColRefName(col, generator->wHeap()));
	  rc->synthTypeAndValueId(TRUE);

	  return rc;
	}
    }

  if (ie->getOperatorType() == ITM_DYN_PARAM)
    {
      paramNum++;
      char buf[200];
      str_sprintf(buf, "@param%d@", paramNum);
      
      NAString constStr(buf);

      ConstValue * cv = new(generator->wHeap()) ConstValue(constStr);
      cv->synthTypeAndValueId(TRUE);
      return cv;
    }

  return ie;
}

/////////////////////////////////////////////////////////
//
// HiveMDaccessFunc::codeGen()
//
/////////////////////////////////////////////////////////
short HiveMDaccessFunc::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_expr *scanExpr = 0;

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
  const Int32 work_atp = 1;
  const unsigned short work_atp_index = 2;

  // Assumption (for now): retrievedCols contains ALL columns from
  // the table/index. This is because this operator does
  // not support projection of columns. Add all columns from this table
  // to the map table.
  //
  // The row retrieved from filesystem is returned as the last entry in
  // the returned atp.

  const ValueIdList & columnList = getTableDesc()->getColumnList();
  const CollIndex numColumns = columnList.entries();

  Attributes ** attrs = new(generator->wHeap()) Attributes * [numColumns];

  for (CollIndex i = 0; i < numColumns; i++)
    {
     ItemExpr * col_node = ((columnList[i]).getValueDesc())->getItemExpr();

      attrs[i] = (generator->addMapInfo(col_node->getValueId(), 0))->
	getAttr();
    }

  ExpTupleDesc *tupleDesc = 0;
  ULng32 tupleLength = 0;
  expGen->processAttributes(numColumns,
			    attrs, ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
			    tupleLength,
			    (selectionPred().isEmpty() ? 0 : work_atp), 
			    (selectionPred().isEmpty() ? returnedDesc->noTuples() - 1 : work_atp_index),
			    &tupleDesc, ExpTupleDesc::SHORT_FORMAT);

  // delete [] attrs;
  // NADELETEBASIC is used because compiler does not support delete[]
  // operator yet. Should be changed back later when compiler supports
  // it.
  NADELETEBASIC(attrs, generator->wHeap());

  // generate explain selection expression, if present
  char * hivePredStr = NULL;
  if (! selectionPred().isEmpty())
   {
     ItemExpr * newPredTree = selectionPred().rebuildExprTree(ITM_AND,TRUE,TRUE);
     expGen->generateExpr(newPredTree->getValueId(), ex_expr::exp_SCAN_PRED,
			   &scanExpr);

     // The md row will be returned as the last entry of the returned atp.
     // Change the atp and atpindex of the returned values to indicate that.
     expGen->assignAtpAndAtpIndex(getTableDesc()->getColumnList(),
				  0, returnedDesc->noTuples()-1);

     ValueId vid;
     NABoolean hiveSelExpr = FALSE;
     ItemExpr * ie = NULL;
     for (vid = selectionPred().init(); 
	  (NOT hiveSelExpr && selectionPred().next(vid)); 
	  selectionPred().advance(vid))
       {
	 ie = vid.getItemExpr();

	 hiveSelExpr = isHiveSelExpr(ie, generator);
       } // for
     
     if (hiveSelExpr)
       {
	 Lng32 paramNum = 0;
	 ItemExpr * hse = createHiveSelExpr(ie, paramNum, generator);

	 NAString str(generator->wHeap());
	 hse->unparse(str, DEFAULT_PHASE, HIVE_MD_FORMAT);

	 if (NOT str.isNull())
	   {
	     hivePredStr = space->allocateAlignedSpace(str.length() + 1);
	     strcpy(hivePredStr, str.data());
	   }
       }
   }

  char * catalogName = NULL;
  NAString catalogNameInt;
  catalogNameInt = CmpCommon::getDefaultString(HIVE_CATALOG);
  catalogNameInt.toLower();
  catalogName = space->allocateAlignedSpace(catalogNameInt.length() + 1);
  strcpy(catalogName, catalogNameInt.data());

  char * schemaName = NULL;
  NAString schemaNameInt ;
  NAString schemas;
  NABoolean useSchemasFromCQD = FALSE;
  if (NOT schemaName_.isNull())
    {
      schemaNameInt = schemaName_;
      schemaName = space->allocateAlignedSpace(schemaNameInt.length() + 1);
      strcpy(schemaName, schemaNameInt.data());
    }
  else 
    {
      schemas = CmpCommon::getDefaultString(TRAF_HIVEMD_SCHEMAS);
      if (((mdType_ == "OBJECTNAMES") ||
           (mdType_ == "OBJECTNAMETYPES") ||
           (mdType_ == "SCHEMAS") ||
           (mdType_ == "TABLES") ||
           (mdType_ == "COLUMNS")) &&
          (NOT schemas.isNull()))
        {
          useSchemasFromCQD = TRUE;
          schemaName = space->allocateAlignedSpace(schemas.length() + 1);
          strcpy(schemaName, schemas.data());      
        }
    }

  char * objectName = NULL;
  if (NOT objectName_.isNull()) {
    objectName = space->allocateAlignedSpace(objectName_.length() + 1);
    strcpy(objectName, objectName_.data());
  }

  // add this descriptor to the work cri descriptor.
  returnedDesc->setTupleDescriptor(returnedDesc->noTuples()-1, tupleDesc);

  ComTdbExeUtilHiveMDaccess::MDType type = ComTdbExeUtilHiveMDaccess::NOOP_;
  if (mdType_ == "TABLES")
    type = ComTdbExeUtilHiveMDaccess::TABLES_;
  else if (mdType_ == "OBJECTNAMES")
    type = ComTdbExeUtilHiveMDaccess::OBJECTNAMES_;
   else if (mdType_ == "OBJECTNAMETYPES")
    type = ComTdbExeUtilHiveMDaccess::OBJECT_NAMETYPES_;
   else if (mdType_ == "COLUMNS")
    type = ComTdbExeUtilHiveMDaccess::COLUMNS_;
  else if (mdType_ == "PKEYS")
    type = ComTdbExeUtilHiveMDaccess::PKEYS_;
  else if (mdType_ == "FKEYS")
    type = ComTdbExeUtilHiveMDaccess::FKEYS_;
  else if (mdType_ == "VIEWS")
    type = ComTdbExeUtilHiveMDaccess::VIEWS_;
  else if (mdType_ == "ALIAS")
    type = ComTdbExeUtilHiveMDaccess::ALIAS_;
  else if (mdType_ == "SYNONYMS")
    type = ComTdbExeUtilHiveMDaccess::SYNONYMS_;
  else if (mdType_ == "SYSTEM_TABLES")
    type = ComTdbExeUtilHiveMDaccess::SYSTEM_TABLES_;
  else if (mdType_ == "SCHEMAS")
    type = ComTdbExeUtilHiveMDaccess::SCHEMAS_;
   
  ComTdbExeUtilHiveMDaccess *hiveTdb
    = new(space)
      ComTdbExeUtilHiveMDaccess(
           type,
           tupleLength,
           givenDesc,	                 // given_cri_desc
           returnedDesc,		 // returned cri desc
           workCriDesc,
           work_atp_index,
           8,				 // Down queue size
           16,				 // Up queue size0
           3,				 // Number of buffers to allocate   
           36000,			 // Size of each buffer
           scanExpr,			 // predicate
           hivePredStr,
           catalogName,
           schemaName,
           objectName);

  generator->initTdbFields(hiveTdb);

  if (CmpCommon::getDefault(TRAF_HIVEMD_SPECIAL_OBJECTNAMETYPE) == DF_ON)
    hiveTdb->setSpecialObjectnametype(TRUE);
  if ((useSchemasFromCQD) && (NOT schemas.isNull()))
    hiveTdb->setSchemasSpecified(TRUE);

  // Add the explain Information for this node to the EXPLAIN
  // Fragment.  Set the explainTuple pointer in the generator so
  // the parent of this node can get a handle on this explainTuple.
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(hiveTdb, 0, 0, generator));
  }
  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, hiveTdb);

  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilHbaseDDL::codeGen()
//
/////////////////////////////////////////////////////////
short ExeUtilHbaseDDL::codeGen(Generator * generator)
{
  Space * space = generator->getSpace();

  ex_cri_desc * givenDesc 
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc 
    = generator->getCriDesc(Generator::DOWN);

  Queue * colFamNameList = new(space) Queue(space);
  
  char * colFamNameInList = NULL;
  if (csl_)
    {
      for (Lng32 i = 0; i < csl_->entries(); i++)
	{
	  const NAString *cs = (*csl_)[i];
	  
	  colFamNameInList = 
	    space->allocateAndCopyToAlignedSpace(cs->data(), cs->length(), 0);
	  
	  colFamNameList->insert(colFamNameInList);
	}
    }

  char * tablename = 
    space->AllocateAndCopyToAlignedSpace(
					 getTableName().getQualifiedNameObj().getObjectName(), 0);
  char * baseTableName = 
    space->AllocateAndCopyToAlignedSpace(
					 getTableName().getQualifiedNameObj().getObjectName(), 0);

  ULng32 buffersize = getDefault(GEN_DDL_BUFFER_SIZE);
  queue_index upqueuelength = (queue_index)getDefault(GEN_DDL_SIZE_UP);
  queue_index downqueuelength = (queue_index)getDefault(GEN_DDL_SIZE_DOWN);
  Int32 numBuffers = getDefault(GEN_DDL_NUM_BUFFERS);

  NAString serverNAS = ActiveSchemaDB()->getDefaults().getValue(HBASE_SERVER);
  NAString zkPortNAS = ActiveSchemaDB()->getDefaults().getValue(HBASE_ZOOKEEPER_PORT);
  char * server = space->allocateAlignedSpace(serverNAS.length() + 1);
  strcpy(server, serverNAS.data());
  char * zkPort = space->allocateAlignedSpace(zkPortNAS.length() + 1);
  strcpy(zkPort, zkPortNAS.data());

  ComTdbHbaseAccess *ddl_tdb = new(space) 
    ComTdbHbaseAccess(
		      (type_ == CREATE_ ? ComTdbHbaseAccess::CREATE_ :
		       (type_ == DROP_ ? ComTdbHbaseAccess::DROP_ :
			(type_ == INIT_MD_ ? ComTdbHbaseAccess::INIT_MD_ :
			 (type_ == DROP_MD_ ? ComTdbHbaseAccess::DROP_MD_ :
			  ComTdbHbaseAccess::NOOP_)))),
		      tablename,
		      baseTableName,
		      0,
		      colFamNameList,
		      NULL,
		      givenDesc,
		      returnedDesc,
		      downqueuelength,
		      upqueuelength,
                      0,  // Cardinality expectedRows,
		      numBuffers,
		      buffersize,
		      server,
                      zkPort
		      );

  generator->initTdbFields(ddl_tdb);
      
  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(ddl_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);

  generator->setGenObj(this, ddl_tdb);

  return 0;
}

const char * ExeUtilConnectby::getVirtualTableName()
{ 
  if(isDual() == TRUE) return "DUAL_CONNECTBY";
  return myTableName_.data();
}

TrafDesc *ExeUtilConnectby::createVirtualTableDesc()
{
  if(isDual() == TRUE)
  {
     TrafDesc * table_desc = TrafAllocateDDLdesc(DESC_TABLE_TYPE, NULL);
     table_desc->tableDesc()->tablename = new HEAP char[strlen("DUAL_CONNECTBY")+1];
     strcpy(table_desc->tableDesc()->tablename, "DUAL_CONNECTBY");
     Lng32 colnumber = 0, offset = 0;

     TrafDesc * column_desc = TrafMakeColumnDesc(
     table_desc->tableDesc()->tablename,
       "LEVEL",
       colnumber,      // INOUT
           REC_BIN32_UNSIGNED,
           4,
           offset,
           FALSE,
           SQLCHARSETCODE_UNKNOWN,
           NULL);
     column_desc->columnsDesc()->character_set    = CharInfo::UTF8;
     column_desc->columnsDesc()->encoding_charset = CharInfo::UTF8;

     int tmpOffset = 0;
     tmpOffset = column_desc->columnsDesc()->offset + 4;
     //Mantis 11520, this is a special treatment to quickly support that mantis
     //Given the limited dev time, not properly integrated with ROWNUM feature
     //Need to study how ROWNUM works and see how to support it properly
     TrafDesc * col_desc = TrafMakeColumnDesc(
     table_desc->tableDesc()->tablename,
           "ROWNUM",
           colnumber,      // INOUT
           REC_BIN32_UNSIGNED,
           4,
           tmpOffset,
           FALSE,
           SQLCHARSETCODE_UNKNOWN,
           NULL);

     column_desc->next = col_desc;  //add new column

     table_desc->tableDesc()->colcount = colnumber;
     table_desc->tableDesc()->record_length = tmpOffset;

     TrafDesc * index_desc = TrafAllocateDDLdesc(DESC_INDEXES_TYPE, NULL);
     index_desc->indexesDesc()->tablename = table_desc->tableDesc()->tablename;
     index_desc->indexesDesc()->indexname = table_desc->tableDesc()->tablename;
     index_desc->indexesDesc()->keytag = 0; // primary index
     index_desc->indexesDesc()->record_length = table_desc->tableDesc()->record_length;
     index_desc->indexesDesc()->colcount = table_desc->tableDesc()->colcount;
     index_desc->indexesDesc()->blocksize = 4096; // anything > 0

     TrafDesc * i_files_desc = TrafAllocateDDLdesc(DESC_FILES_TYPE, NULL);
     index_desc->indexesDesc()->files_desc = i_files_desc;

     TrafDesc * key_desc = TrafAllocateDDLdesc(DESC_KEYS_TYPE, NULL);
     key_desc->keysDesc()->keyseqnumber = 1;
     key_desc->keysDesc()->tablecolnumber = 0;
     key_desc->keysDesc()->setDescending(FALSE);

     index_desc->indexesDesc()->keys_desc = key_desc;
     table_desc->tableDesc()->columns_desc = column_desc;
     table_desc->tableDesc()->indexes_desc = index_desc;

     tblDesc_ = table_desc;
     return tblDesc_;
  }

  CmpSeabaseDDL cmpSBD((NAHeap *)CmpCommon::statementHeap());
  NAString cat;
  NAString sch;
  NAString tbl;
  Scan * scanNode = (Scan*)scan_;
  cat= (scanNode->getTableName()).getQualifiedNameObj().getCatalogName();
  sch= (scanNode->getTableName()).getQualifiedNameObj().getSchemaName();
  tbl= (scanNode->getTableName()).getQualifiedNameObj().getObjectName();

  tblDesc_ = cmpSBD.getSeabaseTableDesc(cat,sch,tbl,COM_BASE_TABLE_OBJECT);

  // Sometime it will return NULL from above call due to maybe HBase issue
  // So need a protection here
  // mantis 12037
  if(tblDesc_ == NULL)
    return NULL; 

  //rename
  TrafTableDesc * td = tblDesc_->tableDesc();
  td->tablename = (char*)getVirtualTableName();
  //add LEVEL column
  TrafDesc * column_desc = tblDesc_->tableDesc()->columns_desc;
  tblDesc_->tableDesc()->colcount++;
  //go to the last entry
  int i = 0;
  int tmpOffset = 0;
  while(column_desc->next) { i++; column_desc = column_desc->next; }
  tmpOffset = column_desc->columnsDesc()->offset + 4;
  TrafDesc * col_desc = TrafMakeColumnDesc(
           getVirtualTableName(),
           "LEVEL", //info->colName,
           i,
           REC_BIN32_UNSIGNED,
           4,
           tmpOffset,
           FALSE,
           SQLCHARSETCODE_UNKNOWN,
           NULL);
  column_desc->next = col_desc;
  col_desc->columnsDesc()->colclass='S';

  column_desc = tblDesc_->tableDesc()->columns_desc;
  tblDesc_->tableDesc()->colcount++;  
  i =0;
  tmpOffset = 0;
  //As I learned more, this below line of code is not needed
  //But keep it here for now, will optimize this in the future
  //the value i is defined as INOUT, so it will be automatically update to the last column pos
  while(column_desc->next) { i++; column_desc = column_desc->next; }

  tmpOffset = column_desc->columnsDesc()->offset + 4;
  col_desc = TrafMakeColumnDesc(
           getVirtualTableName(),
           "CONNECT_BY_ISCYCLE", //info->colName,
           i,
           REC_BIN32_UNSIGNED,
           4,
           tmpOffset,
           FALSE,
           SQLCHARSETCODE_UNKNOWN,
           NULL);
  column_desc->next = col_desc;
  col_desc->columnsDesc()->colclass='S';

  column_desc = tblDesc_->tableDesc()->columns_desc;
  tblDesc_->tableDesc()->colcount++;  
  i =0;
  tmpOffset = 0;
  while(column_desc->next) { i++; column_desc = column_desc->next; }

  tmpOffset = column_desc->columnsDesc()->offset + 4;
  col_desc = TrafMakeColumnDesc(
           getVirtualTableName(),
           "CONNECT_BY_ISLEAF", //info->colName,
           i,
           REC_BIN32_UNSIGNED,
           4,
           tmpOffset,
           FALSE,
           SQLCHARSETCODE_UNKNOWN,
           NULL);
  column_desc->next = col_desc;
  col_desc->columnsDesc()->colclass='S';

  column_desc = tblDesc_->tableDesc()->columns_desc;
  tblDesc_->tableDesc()->colcount++;  
  i =0;
  tmpOffset = 0;
  while(column_desc->next) { i++; column_desc = column_desc->next; }

  tmpOffset = column_desc->columnsDesc()->offset + 4;
  col_desc = TrafMakeColumnDesc(
           getVirtualTableName(),
           "CONNECT_BY_PATH", //info->colName,
           i,
           REC_BYTE_V_ASCII,
           3000,
           tmpOffset,
           FALSE,
           SQLCHARSETCODE_UTF8,
           NULL);
  column_desc->next = col_desc;
  col_desc->columnsDesc()->colclass='S';

  return tblDesc_;
}

short ExeUtilConnectby::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);
  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);  
  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(5, space);
  
  //there will only one expression used in ConnectBy execution TCB class
  //So there only need one ATP index, using 2 as the index
  const Int32 work_atp = 1;
  Int32 theAtpIndex = 2; 
  Int32 theAtpIndexForDynParam = 3;


  ValueIdList userColList;
  int attridx = 0;

  Attributes **   attrs =
    new(generator->wHeap())
    Attributes * [getVirtualTableDesc()->getColumnList().entries()];

  for (CollIndex i = 0; i < getVirtualTableDesc()->getColumnList().entries(); i++)
    {
      NAColumn *colt = ((getVirtualTableDesc()->getColumnList())[i]).getNAColumn();
      if(! colt->isSyskeyColumn() )
      {
        userColList.insert( (getVirtualTableDesc()->getColumnList())[i] );
        ItemExpr * col_node = (((getVirtualTableDesc()->getColumnList())[i]).getValueDesc())->getItemExpr();
        attrs[attridx] = (generator->addMapInfo(col_node->getValueId(), 0))->getAttr();
        attridx++;
      }
    }

  ExpTupleDesc *tupleDescReturn = 0;
  ULng32 tupleLength = 0;
 
  ExpTupleDesc::TupleDataFormat tupleFormat = ExpTupleDesc::SQLARK_EXPLODED_FORMAT;
  
  expGen->processAttributes(userColList.entries(),
			    attrs, tupleFormat,
			    tupleLength,
			    work_atp, theAtpIndex,
			    &tupleDescReturn, ExpTupleDesc::LONG_FORMAT);

  NADELETEBASIC(attrs, generator->wHeap());

  workCriDesc->setTupleDescriptor(theAtpIndex, tupleDescReturn);

  //handle dynamic params

  ULng32 dtupleLength = 0;
  ExpTupleDesc *dtupleDesc = 0;
  ex_expr * input_expr = 0;
  ValueIdList vidL;

  if(hasDynParamInStart()) {
    //go through the dynParam valueID list and convert to cast to char
    for (ValueId dp= dynParmas_.init();
              dynParmas_.next(dp);
              dynParmas_.advance(dp) )
    {
      ItemExpr * iep = dp.getItemExpr();
      const NAType &oldNAT = dp.getType();

      Int32 dlen = 128; //default size

      switch (oldNAT.getTypeQualifier() ) {
        case NA_CHARACTER_TYPE:
          dlen =  oldNAT.getTotalSize();
          break;
        default:
          dlen = 32;
      }

      NAType * newNAT =  new(generator->wHeap()) SQLVarChar(generator->wHeap(), dlen);
      newNAT->setNullable(FALSE, FALSE);
      Cast * fnp = new(generator->wHeap()) Cast(iep, newNAT);
      fnp->bindNode(generator->getBindWA());
      vidL.insert(fnp->getValueId());
    }

    expGen->generateContiguousMoveExpr(vidL,
                                   1, // don't add conv nodes
                                   work_atp,
                                   theAtpIndexForDynParam,
                                   ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                                   dtupleLength,
                                   &input_expr );
     expGen->processValIdList (
       vidL,                              // [IN] ValueIdList
       ExpTupleDesc::SQLARK_EXPLODED_FORMAT,  // [IN] tuple data format
       dtupleLength,                          // [OUT] tuple length
       work_atp,                                     // [IN] atp number
       theAtpIndexForDynParam,         // [IN] index into atp
       &dtupleDesc,                      // [optional OUT] tuple desc
       ExpTupleDesc::LONG_FORMAT              // [optional IN] tuple desc format
       );
     workCriDesc->setTupleDescriptor(theAtpIndexForDynParam , dtupleDesc);
  }


  TrafDesc * column_desc = tblDesc_->tableDesc()->columns_desc;
  Lng32 colDescSize =  column_desc->columnsDesc()->length;

  Scan * scanNode = (Scan*)scan_;

  char * stmtText;
  char tblnm[1024] ; 
  memset(tblnm, 0,1024);
  NAString tbl; 

  if(isDual() == TRUE)
  {
     NAString tbld = "DUAL_CONNECTBY";
     strcpy(tblnm, "DUAL_CONNECTBY");
  }
  else  {
    //TODO: need a better way to get the table name
    tbl= (scanNode->getTableName()).getQualifiedNameAsString();
    strncpy(tblnm, (char*) tbl.data(), tbl.length());
  }

  stmtText = getStmtText();

  ex_expr * selectPred = NULL;
  //ex_expr * selectPred1 = NULL;

  //if some predicate push down, add them
  if( !selectionPred().isEmpty() )
  {
        mypredicates_+=selectionPred();
  }

  if (!mypredicates_.isEmpty())
    {
      ItemExpr * selPredTree =
        mypredicates_.rebuildExprTree(ITM_AND,TRUE,TRUE);
      //selectionPred().rebuildExprTree(ITM_AND,TRUE,TRUE);
      expGen->generateExpr(selPredTree->getValueId(),
                            ex_expr::exp_SCAN_PRED,
                            &selectPred);

    }
#if 0
  if( hasDynParamInStart())
  {
    if ( !startWithPredicates_.isEmpty())
    {
      ItemExpr * selPredTree =
        startWithPredicates_.rebuildExprTree(ITM_AND,TRUE,TRUE);
      expGen->generateExpr(selPredTree->getValueId(),
                            ex_expr::exp_SCAN_PRED,
                            &selectPred1);
    }
  }
#endif
  //set the return row format
  expGen->assignAtpAndAtpIndex(userColList, 0 , returnedDesc->noTuples()-1);

  ComTdbExeUtilConnectby  * exe_util_tdb = new(space)  
  ComTdbExeUtilConnectby (stmtText , (stmtText ? strlen(stmtText) : 0), getStmtTextCharSet(), tblnm , tbl.length(),NULL, 
      input_expr ,dtupleLength ,
      0 , 0,
      selectPred,
      workCriDesc , theAtpIndex,
      colDescSize,
      tupleLength,
      givenDesc,
      returnedDesc,
      (queue_index)2048,
      (queue_index)2048,
      30,
      320000,
      workCriDesc,
      NULL 
      );

  exe_util_tdb->sourceDataTuppIndex_ = theAtpIndex;
  exe_util_tdb->dynParamTuppIndex= theAtpIndexForDynParam;
  exe_util_tdb->parentColName_ = parentColName_; // ((BiConnectBy*)getBiConnectBy())->getConnectBy()->getParentColName();
  exe_util_tdb->childColName_ = childColName_; // ((BiConnectBy*)getBiConnectBy())->getConnectBy()->getChildColName();
  exe_util_tdb->hasStartWith_ = hasStartWith_;
  exe_util_tdb->startWithExprString_ = startWithExprString_;
  exe_util_tdb->noCycle_ = noCycle_;
  exe_util_tdb->nodup_ = nodup_;
  exe_util_tdb->hasDynParamsInStartWith_ = hasDynParamInStart();
  if(isDual() == TRUE) exe_util_tdb->setDual(TRUE);

  if(generator->getBindWA()->connectByHasPath_)
  {
     exe_util_tdb->hasPath_ = TRUE;
     exe_util_tdb->pathColName_ = ((BiConnectBy*)getBiConnectBy())->pathString_;
     exe_util_tdb->delimiter_ = generator->getBindWA()->connectByPathDel_;
  }
  else
  {
     exe_util_tdb->hasPath_ = FALSE;
     exe_util_tdb->pathColName_ = "0";
     exe_util_tdb->delimiter_ = " ";
  }
  if(generator->getBindWA()->connectByHasIsLeaf_)
  {
    exe_util_tdb->hasIsLeaf_ = TRUE;
  }
  else
    exe_util_tdb->hasIsLeaf_ = FALSE;

  exe_util_tdb->orderSiblingsByCol_ = generator->getBindWA()->orderSiblingsByCol_;

  generator->initTdbFields(exe_util_tdb);

  if (!generator->explainDisabled())
  {
    generator->setExplainTuple(addExplainInfo(exe_util_tdb, 0, 0, generator));
  }
  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  
  generator->setGenObj(this, exe_util_tdb);

  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilCompositeUnnest
//
/////////////////////////////////////////////////////////
const char * ExeUtilCompositeUnnest::getVirtualTableName()
{ 
  return virtTableName_.data();
}

short ExeUtilCompositeUnnest::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);
  ex_cri_desc * returnedDesc
    = new(space) ex_cri_desc(givenDesc->noTuples() + 1, space);  
  ex_cri_desc * workCriDesc = new(space) ex_cri_desc(7, space);
  
  const Int32 work_atp = 1;
  Int32 unnestColsAtpIndex = 2;
  Int32 extractColAtpIndex = 3;
  Int32 elemNumAtpIndex = 4;
  Int32 workAtpIndex = 5; 
  
  short returnedAtpIndex = (short) (returnedDesc->noTuples() - 1);

  // generate code for child tree
  child(0)->codeGen(generator);
  ComTdb *childTdb = (ComTdb *)(generator->getGenObj());
  ExplainTuple *childExplainTuple = generator->getExplainTuple();

  ////////////////////////////////////////////////////////////////////////////
  // column to be unnested could be an ArrayOfPrimitive or ArrayOfStruct type.
  // -- First extract the composite array column from the table row
  // -- Then for each array element within that column:
  //    if ArrayOfPrimitive, then return the primitive field.
  //    if ArrayOfStruct, then return the primitive fields within the struct
  ////////////////////////////////////////////////////////////////////////////
  ex_expr *extractColExpr = NULL;
  ULng32 extractColRowLen = 0;
  ExpTupleDesc * extractColTupleDesc = 0;
  ValueIdList extractColVIDList;
  extractColVIDList.insert(colNameExpr_->getValueId());

  expGen->
    generateContiguousMoveExpr(extractColVIDList,
                               1, // add conv node
                               work_atp,
                               extractColAtpIndex,
                               ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                               extractColRowLen,
                               &extractColExpr,
                               &extractColTupleDesc,
                               ExpTupleDesc::LONG_FORMAT);
  workCriDesc->setTupleDescriptor(extractColAtpIndex, extractColTupleDesc);

  // create attribute that points to the location of extracted col
  ItemExpr *extractColIE = 
    new(generator->wHeap()) NATypeToItem((NAType*)&colNameExpr_->getValueId().getType());
  extractColIE->bindNode(generator->getBindWA());
  if (generator->getBindWA()->errStatus())
    {
      GenExit();
    }

  extractColVIDList.clear();
  extractColVIDList.insert(extractColIE->getValueId());
  ULng32 extractColLen = 0;
  expGen->processValIdList(extractColVIDList,
                           ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                           extractColLen,
                           work_atp, extractColAtpIndex);

  // retrieve each array element from extracted col and return fields
  // (primitive or struct) from each arr elem.
  // elemNum operand will be populated at runtime (1 to max num of elem).
  NAType *elemNumType = new(generator->wHeap()) 
    SQLInt(generator->wHeap(), FALSE, FALSE);
  ItemExpr *elemNumIE = new(generator->wHeap()) NATypeToItem(elemNumType);
  elemNumIE->bindNode(generator->getBindWA());
  if (generator->getBindWA()->errStatus())
    {
      GenExit();
    }
  
  ValueIdList elemNumList;
  elemNumList.insert(elemNumIE->getValueId());
  ULng32 elemNumLen = 0;
  expGen->processValIdList(elemNumList,
                           ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                           elemNumLen,
                           work_atp, elemNumAtpIndex);

  // extract array elements
  ItemExpr *compArrayElem = 
    new(generator->wHeap()) CompositeExtract(extractColIE, elemNumIE);
  compArrayElem->bindNode(generator->getBindWA());
  if (generator->getBindWA()->errStatus())
    {
      GenExit();
    }

  ValueIdList unnestColsVIDList;

  if (showPos_)
    {
      ItemExpr *ie = elemNumIE;
      if (CmpCommon::getDefault(HIVE_ARRAY_INDEX_MODE) == DF_ON)
        {
          // elemNum is 1-based.
          // hive POS is 0-based.
          // subtract 1 from elemNum.
          ConstValue *cv = new(generator->wHeap()) ConstValue(1);

          ie = new(generator->wHeap()) BiArith(ITM_MINUS, elemNumIE, cv);
        }

      ItemExpr *cast = new(generator->wHeap()) 
        Cast(ie, &elemNumIE->getValueId().getType());
      
      cast->bindNode(generator->getBindWA());
      if (generator->getBindWA()->errStatus())
        {
          GenExit();
          return -1;
        }

      unnestColsVIDList.insert(cast->getValueId());
    }

  // extract fields out of array element row. array element fields could be
  // composite(struct/row) or primitive
  if (compArrayElem->getValueId().getType().isComposite())
    {
      for (Int32 elem = 1; elem <= ((CompositeType&)compArrayElem->getValueId().getType()).getNumElements(); elem++)
        {
          ItemExpr *elemIE = 
            new(generator->wHeap()) CompositeExtract(compArrayElem, elem);
          elemIE->bindNode(generator->getBindWA());
          if (generator->getBindWA()->errStatus())
            {
              GenExit();
            }
          
          ItemExpr *cast = new(generator->wHeap()) 
            Cast(elemIE, &elemIE->getValueId().getType());
          
          cast->bindNode(generator->getBindWA());
          if (generator->getBindWA()->errStatus())
            {
              GenExit();
              return -1;
            }

          unnestColsVIDList.insert(cast->getValueId());
        } // for
    }
  else
    {
      const NAType &nat = compArrayElem->getValueId().getType();

      ItemExpr *cast = new(generator->wHeap()) Cast(compArrayElem, &nat);
      
      cast->bindNode(generator->getBindWA());
      if (generator->getBindWA()->errStatus())
        {
          GenExit();
          return -1;
        }
      
      unnestColsVIDList.insert(cast->getValueId());
    }

  if (addnlColsVIDList_.entries() > 0)
    {
      for (Int32 i = 0; i < addnlColsVIDList_.entries(); i++)
        {
          ValueId vid = addnlColsVIDList_[i];
          ItemExpr *cast = new(generator->wHeap()) 
            Cast(vid.getItemExpr(), &vid.getType());
          
          cast->bindNode(generator->getBindWA());
          if (generator->getBindWA()->errStatus())
            {
              GenExit();
              return -1;
            }
      
          unnestColsVIDList.insert(cast->getValueId());
        }
    }

  ex_expr *unnestColsExpr = NULL;
  ULng32 unnestColsRowLen = 0;
  ExpTupleDesc * unnestColsTupleDesc = 0;
  expGen->
    generateContiguousMoveExpr(unnestColsVIDList,
                               0, // dont add conv nodes
                               work_atp,
                               unnestColsAtpIndex,
                               ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                               unnestColsRowLen,
                               &unnestColsExpr,
                               &unnestColsTupleDesc,
                               ExpTupleDesc::LONG_FORMAT);
  workCriDesc->setTupleDescriptor(unnestColsAtpIndex, unnestColsTupleDesc);

  TableDesc *virtTableDesc = getVirtualTableDesc();
  if (unnestColsVIDList.entries() != virtTableDesc->getColumnList().entries())
    {
      GenAssert(0,"Error: returnVIDList.entries() must be equal to virtTableDesc->getColumnList().entries()");
    }

  // assign data location of values in unnestColsVIDList to the original cols
  // in descriptor for this operator.
  for (CollIndex i = 0; i < virtTableDesc->getColumnList().entries(); i++)
    {
      ItemExpr * col_node
	= (((getVirtualTableDesc()->getColumnList())[i]).getValueDesc())->
	  getItemExpr();
      ValueId colValId = col_node->getValueId();
      Attributes *colAttr = generator->addMapInfo(colValId,0)->getAttr();

      ValueId unnestColValId = unnestColsVIDList[i];
      Attributes *unnestColAttr = (generator->getMapInfo(unnestColValId))->getAttr();

      colAttr->copyLocationAttrs(unnestColAttr);
    }

  // generate tuple selection expression, if present
  ex_expr *scanExpr = NULL;
  if (NOT selectionPred().isEmpty())
    {
      ItemExpr* pred = selectionPred().rebuildExprTree(ITM_AND,TRUE,TRUE);
      expGen->generateExpr(pred->getValueId(),ex_expr::exp_SCAN_PRED,&scanExpr);
    }

  // change atp and atpindex to the location where unnested row will be 
  // returned.
  for (CollIndex i = 0; i < virtTableDesc->getColumnList().entries(); i++)
    {
      ItemExpr * col_node
	= (((getVirtualTableDesc()->getColumnList())[i]).getValueDesc())->
	  getItemExpr();
      ValueId colValId = col_node->getValueId();
      Attributes *colAttr = generator->getMapInfo(colValId,0)->getAttr();

      colAttr->setAtp(0);
      colAttr->setAtpIndex(returnedAtpIndex);
    }

  ComTdbExeUtilCompositeUnnest * exe_util_tdb = new(space)  
  ComTdbExeUtilCompositeUnnest(
       scanExpr,
       extractColExpr,
       extractColAtpIndex,
       extractColRowLen,
       elemNumAtpIndex,
       elemNumLen,
       unnestColsExpr,
       unnestColsAtpIndex,
       unnestColsRowLen,
       workCriDesc, 
       workAtpIndex,
       givenDesc,
       returnedDesc,
       (queue_index)8,
       (queue_index)1024,
       10,
       100000
      );

  generator->initTdbFields(exe_util_tdb);

  exe_util_tdb->setChildTdb(childTdb);
  
  if (!generator->explainDisabled())
    {
      generator->setExplainTuple(addExplainInfo(exe_util_tdb, childExplainTuple, 0, generator));
    }
  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  
  generator->setGenObj(this, exe_util_tdb);

  return 0;
}

/////////////////////////////////////////////////////////
//
// ExeUtilHbaseLoad::codeGen()
//
/////////////////////////////////////////////////////////
extern Int64 getDefaultSampleRowSize(Int64 tblRowCount);

short ExeUtilHBaseBulkLoad::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  NAString ldQueryNAS = this->getStmtText();
  char * ldQuery = NULL;

  if (ldQueryNAS.length() > 0)
  {
    ldQuery = space->allocateAlignedSpace(ldQueryNAS.length() + 1);
    strcpy(ldQuery, ldQueryNAS.data());
  }

  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;
  const Int32 work_row_atp_index = 2;

  ex_cri_desc * workCriDesc = NULL;
  ExpTupleDesc * inputParamsTupleDesc = 0;
  ex_expr * input_expr = 0;
  ULng32 inputRowLen = 0;
  if (inputParams().entries() > 0)
    {
      workCriDesc = new(space) ex_cri_desc(4, space);

      expGen->
	generateContiguousMoveExpr(inputParams(),
				   1, // add conv nodes
				   work_atp,
				   work_row_atp_index,
				   ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
				   inputRowLen,
				   &input_expr,
                                   &inputParamsTupleDesc,
                                   ExpTupleDesc::LONG_FORMAT);

      workCriDesc->setTupleDescriptor(work_row_atp_index, inputParamsTupleDesc);
    }

  char * errCountTab = NULL;
  NAString errCountTabNAS = TRAF_LOAD_ERROR_COUNT_TABLE;
  if (errCountTabNAS.length() > 0)
  {
    errCountTab = space->allocateAlignedSpace(errCountTabNAS.length() + 1);
    strcpy(errCountTab, errCountTabNAS.data());
  }
  char * logLocation = NULL;
  NAString logLocationNAS = logErrorRowsLocation_;
  if (logLocationNAS.length() > 0)
  {
    logLocation = space->allocateAlignedSpace(logLocationNAS.length() + 1);
    strcpy(logLocation, logLocationNAS.data());
  }
  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc = generator->getCriDesc(Generator::DOWN);
  ex_cri_desc * returnedDesc
  = new (space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                              returnedDesc);
  if (rc)
    {
      return -1;
    }

  char * tablename =
    space->AllocateAndCopyToAlignedSpace(generator->genGetNameAsAnsiNAString(getTableName()), 0);
  
  ComTdbExeUtilHBaseBulkLoad * exe_util_tdb = new(space)
  ComTdbExeUtilHBaseBulkLoad(
         tablename, strlen(tablename),
         ldQuery,
         input_expr, inputRowLen,
         workCriDesc, work_row_atp_index,
         givenDesc,
         returnedDesc,
         (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
         (queue_index)getDefault(GEN_DDL_SIZE_UP),
         getDefault(GEN_DDL_NUM_BUFFERS),
         1024,          //getDefault(GEN_DDL_BUFFER_SIZE));
         errCountTab,
         logLocation);

  exe_util_tdb->setPreloadCleanup(CmpCommon::getDefault(TRAF_LOAD_PREP_CLEANUP) == DF_ON);
  exe_util_tdb->setPreparation(TRUE);
  exe_util_tdb->setKeepHFiles(keepHFiles_);
  exe_util_tdb->setTruncateTable(truncateTable_);
  exe_util_tdb->setNoRollback(noRollback_);
  exe_util_tdb->setLogErrorRows(logErrorRows_);
  exe_util_tdb->setContinueOnError(continueOnError_);
  exe_util_tdb->setMaxErrorRows(maxErrorRows_);
  exe_util_tdb->setNoDuplicates(noDuplicates_);
  exe_util_tdb->setRebuildIndexes(rebuildIndexes_);
  exe_util_tdb->setHasUniqueIndexes(hasUniqueIndexes_);
  exe_util_tdb->setConstraints(constraints_);
  exe_util_tdb->setNoOutput(noOutput_);
  exe_util_tdb->setIndexTableOnly(indexTableOnly_);
  exe_util_tdb->setUpsertUsingLoad(upsertUsingLoad_);
  exe_util_tdb->setForceCIF(CmpCommon::getDefault(TRAF_LOAD_FORCE_CIF) == DF_ON);
  exe_util_tdb->setUpdateStats(updateStats_);
  exe_util_tdb->setSecure(FALSE);
  if ( updateStats_ && getUtilTableDesc() && getUtilTableDesc()->getNATable() )
    {
      NAString sName = GenBulkloadSampleTableName(getUtilTableDesc()->getNATable());
      char * sampleTableName = NULL;
      sampleTableName = space->allocateAlignedSpace(sName.length() + 1);
      strcpy(sampleTableName, sName.data());
      sampleTableName[sName.length()] = '\0';
      exe_util_tdb->setSampleTableName(sampleTableName);
    }

  generator->initTdbFields(exe_util_tdb);

  if (!generator->explainDisabled())
  {
    generator->setExplainTuple(addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  return 0;
}
short ExeUtilHBaseBulkLoadTask::codeGen(Generator * generator)
{
  Space * space = generator->getSpace();

  ex_cri_desc * givenDesc
    = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
    = generator->getCriDesc(Generator::DOWN);


  char * tablename = NULL;
  if (FileScan::genTableName(generator, space, getTableName(),
                             getUtilTableDesc()->getNATable(), 
                             NULL,
                             FALSE,
                             tablename))
    {
      GenAssert(0,"genTableName failed");
    }

  char * baseTableName = NULL;
  if (FileScan::genTableName(generator, space, getUtilTableDesc()->getCorrNameObj(),
                             getUtilTableDesc()->getNATable(), 
                             NULL,
                             FALSE,
                             baseTableName))
    {
      GenAssert(0,"genTableName failed");
    }

  Cardinality expectedRows = (Cardinality) getEstRowsUsed().getValue();
  ULng32 buffersize = getDefault(GEN_DDL_BUFFER_SIZE);
  queue_index upqueuelength = (queue_index)getDefault(GEN_DDL_SIZE_UP);
  queue_index downqueuelength = (queue_index)getDefault(GEN_DDL_SIZE_DOWN);
  Int32 numBuffers = getDefault(GEN_DDL_NUM_BUFFERS);

  NAString serverNAS = ActiveSchemaDB()->getDefaults().getValue(HBASE_SERVER);
  NAString zkPortNAS = ActiveSchemaDB()->getDefaults().getValue(HBASE_ZOOKEEPER_PORT);

  char * server = space->allocateAlignedSpace(serverNAS.length() + 1);
  strcpy(server, serverNAS.data());
  char * zkPort = space->allocateAlignedSpace(zkPortNAS.length() + 1);
  strcpy(zkPort, zkPortNAS.data());


  Queue * indexList = NULL;
  Queue * indexListForUID = NULL;
  const LIST(IndexDesc *) indList = getUtilTableDesc()->getIndexes();

  indexList = new(space) Queue(space);
  indexListForUID = new(space) Queue(space);
  char * indexName = NULL;
  char * indexNameForUID = NULL;
  {
    // base table is included
    for (CollIndex i=0; i<indList.entries(); i++) 
      {
        IndexDesc *index = indList[i];
        if (FileScan::genTableName(generator, space, index->getIndexName(),
                                   getUtilTableDesc()->getNATable(), 
                                   NULL,
                                   FALSE,
                                   indexName))
          {
            GenAssert(0,"genTableName failed");
          }
        indexList->insert(indexName);

        if (USE_UUID_AS_HBASE_TABLENAME && ComIsUserTable(index->getIndexName()))
        {
          char * tablenameForUID = NULL;
          if (FileScan::genTableName(generator, space, index->getIndexName(),
                        getUtilTableDesc()->getNATable(), 
                        NULL,
                        FALSE,
                        tablenameForUID,
                        TRUE))
          {
            GenAssert(0,"genTableName failed");
          }

          if (tablenameForUID)
          {
            indexListForUID->insert(tablenameForUID);
          }
        }
      }
  }

  ComTdbHbaseAccess *load_tdb = new(space)
    ComTdbHbaseAccess(
                      ComTdbHbaseAccess::BULK_LOAD_TASK_,
                      tablename,
                      baseTableName,
                      0,
                      0,
                      NULL,
                      givenDesc,
                      returnedDesc,
                      downqueuelength,
                      upqueuelength,
                      expectedRows,
                      numBuffers,
                      buffersize,
                      server,
                      zkPort
                      );

  generator->initTdbFields(load_tdb);

  NAString tlpTmpLocationNAS = ActiveSchemaDB()->getDefaults().getValue(TRAF_LOAD_PREP_TMP_LOCATION);
  char * tlpTmpLocation = space->allocateAlignedSpace(tlpTmpLocationNAS.length() + 1);
  strcpy(tlpTmpLocation, tlpTmpLocationNAS.data());
  load_tdb->setLoadPrepLocation(tlpTmpLocation);

  if (USE_UUID_AS_HBASE_TABLENAME && !indexListForUID->isEmpty())
  {
    load_tdb->setReplaceNameByUID(TRUE);
    load_tdb->setListOfIndexesAndTableForUID(indexListForUID);
  }

  if(withUstats_)
  {
    NAString sampleTmpLocationNAS = ActiveSchemaDB()->getDefaults().getValue(TRAF_SAMPLE_TABLE_LOCATION);
    char * sampleTmpLocation = space->allocateAlignedSpace(sampleTmpLocationNAS.length() + 1);
    strcpy(sampleTmpLocation, sampleTmpLocationNAS.data());
    load_tdb->setSampleLocation(sampleTmpLocation);

    Int64 totalRows = (Int64)(getInputCardinality().getValue());
    Int64 sampleRows = getDefaultSampleRowSize(totalRows);
    Float32 sampleRate = (Float32)sampleRows / (Float32)totalRows;
    load_tdb->setSamplingRate(sampleRate);
  }
  load_tdb->setQuasiSecure(FALSE);
  load_tdb->setTakeSnapshot((CmpCommon::getDefault(TRAF_LOAD_TAKE_SNAPSHOT) == DF_ON));

  if (taskType_ == PRE_LOAD_CLEANUP_)
    load_tdb->setIsTrafLoadCleanup(TRUE);
  else   if (taskType_ == COMPLETE_BULK_LOAD_ || taskType_ == COMPLETE_BULK_LOAD_N_KEEP_HFILES_)
  {
    load_tdb->setIsTrafLoadCompetion(TRUE);
    load_tdb->setIsTrafLoadKeepHFiles(taskType_ == COMPLETE_BULK_LOAD_N_KEEP_HFILES_ ? TRUE: FALSE);
  }
  load_tdb->setListOfIndexesAndTable(indexList);


  if(!generator->explainDisabled()) {
    generator->setExplainTuple(
       addExplainInfo(load_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);

  generator->setGenObj(this, load_tdb);

  return 0;
}



////////////////////////////////////////////////////////
//
// ExeUtilHbaseUnLoad::codeGen()
//
/////////////////////////////////////////////////////////

short ExeUtilHBaseBulkUnLoad::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  NAString uldQueryNAS = this->getStmtText();
  char * uldQuery = NULL;

  if (uldQueryNAS.length() > 0)
  {
    uldQuery = space->allocateAlignedSpace(uldQueryNAS.length() + 1);
    strcpy(uldQuery, uldQueryNAS.data());
  }
  char * mergePathStr = NULL;
  if (mergePath_.length()>0){
    mergePathStr = space->allocateAlignedSpace(mergePath_.length() + 1);
    strcpy(mergePathStr, mergePath_.data());
  }

  char * snapSuffixStr = NULL;
  if (snapSuffix_.length()>0){
    snapSuffixStr = space->allocateAlignedSpace(snapSuffix_.length() + 1);
    strcpy(snapSuffixStr, snapSuffix_.data());
  }

  char * extractLocationStr = NULL;
  if (extractLocation_.length()>0){
    extractLocationStr = space->allocateAlignedSpace(extractLocation_.length() + 1);
    strcpy(extractLocationStr, extractLocation_.data());
  }

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc
  = new (space) ex_cri_desc(givenDesc->noTuples() + 1, space);

    ////ex_cri_desc * workCriDesc = new(space) ex_cri_desc(4, space);
    const Int32 work_atp = 1;
    const Int32 exe_util_row_atp_index = 2;

    short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index,
                                returnedDesc);
    if (rc)
      {
        return -1;
      }
    
    char * tablename =
      space->AllocateAndCopyToAlignedSpace(generator->genGetNameAsAnsiNAString(getTableName()), 0);

  ComTdbExeUtilHBaseBulkUnLoad * exe_util_tdb = new(space)
  ComTdbExeUtilHBaseBulkUnLoad(
         tablename,
         strlen(tablename),
         uldQuery,
         extractLocationStr,
         0, 0, // no work cri desc
         givenDesc,
         returnedDesc,
         (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
         (queue_index)getDefault(GEN_DDL_SIZE_UP),
         getDefault(GEN_DDL_NUM_BUFFERS),
         1024); //getDefault(GEN_DDL_BUFFER_SIZE));

  exe_util_tdb->setEmptyTarget(emptyTarget_);
  exe_util_tdb->setLogErrors(logErrors_);
  exe_util_tdb->setNoOutput(noOutput_);
  exe_util_tdb->setCompressType(compressType_);
  exe_util_tdb->setOneFile(oneFile_);
  exe_util_tdb->setMergePath(mergePathStr);
  exe_util_tdb->setOverwriteMergeFile(overwriteMergeFile_);
  exe_util_tdb->setScanType(scanType_);
  exe_util_tdb->setSnapshotSuffix(snapSuffixStr);

  generator->initTdbFields(exe_util_tdb);

  if (!generator->explainDisabled())
  {
    generator->setExplainTuple(addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  return 0;
}

////////////////////////////////////////////////////////
//
// ExeUtilSnapShotUpdataDelete::codeGen()
//
/////////////////////////////////////////////////////////

short ExeUtilSnapShotUpdataDelete::codeGen(Generator * generator)
{
  ExpGenerator * expGen = generator->getExpGenerator();
  Space * space = generator->getSpace();

  NAString queryNAS = this->getStmtText();
  char * uldQuery = NULL;
  if (queryNAS.length() > 0)
  {
    uldQuery = space->allocateAlignedSpace(queryNAS.length() + 1);
    strcpy(uldQuery, queryNAS.data());
  }

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc * givenDesc = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc * returnedDesc = new (space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  Queue * indexList = NULL;
  const LIST(IndexDesc *) indList = getUtilTableDesc()->getIndexes();
  indexList = new(space) Queue(space);
  char * indexName = NULL;

  // base table is included
  for (CollIndex i=0; i<indList.entries(); i++) 
  {
    IndexDesc *index = indList[i];
    if (FileScan::genTableName(generator, space, index->getIndexName(),
                               getUtilTableDesc()->getNATable(), 
                               NULL,
                               FALSE,
                               indexName))
    {
      GenAssert(0,"genTableName failed");
    }

    indexList->insert(indexName);
  }

  const Int32 work_atp = 1;
  const Int32 exe_util_row_atp_index = 2;

  short rc = processOutputRow(generator, work_atp, exe_util_row_atp_index, returnedDesc);
  if (rc)
  {
    return -1;
  }

  char *connectParam1;
  char *connectParam2;
  ComStorageType storageType = COM_STORAGE_HBASE;
  NATable::getConnectParams(storageType, &connectParam1, &connectParam2);
  char * server = space->allocateAlignedSpace(strlen(connectParam1) + 1);
  strcpy(server, connectParam1);
  char * zkPort = space->allocateAlignedSpace(strlen(connectParam2) + 1);
  strcpy(zkPort, connectParam2);
    
  char * tablename = space->AllocateAndCopyToAlignedSpace(generator->genGetNameAsAnsiNAString(getTableName()), 0);

  ComTdbExeUtilUpdataDelete * exe_util_tdb = new(space)
  ComTdbExeUtilUpdataDelete(
         tablename,
         strlen(tablename),
         uldQuery,
         0, 0, // no work cri desc
         givenDesc,
         returnedDesc,
         (queue_index)getDefault(GEN_DDL_SIZE_DOWN),
         (queue_index)getDefault(GEN_DDL_SIZE_UP),
         getDefault(GEN_DDL_NUM_BUFFERS),
         getDefault(GEN_DDL_BUFFER_SIZE),
         storageType,
         server,
         zkPort
         );

  generator->initTdbFields(exe_util_tdb);

  if (incrBackupEnabled_)
    exe_util_tdb->setIncrBackupEnabled(TRUE);

  exe_util_tdb->setObjectUid(objUID_);

  exe_util_tdb->setListOfIndexesAndTable(indexList);

  if (!generator->explainDisabled())
  {
    generator->setExplainTuple(addExplainInfo(exe_util_tdb, 0, 0, generator));
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, exe_util_tdb);

  return 0;
}