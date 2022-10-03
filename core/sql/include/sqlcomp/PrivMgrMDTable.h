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

#ifndef PRIVMGR_MDTABLE_H
#define PRIVMGR_MDTABLE_H

#include <string>
#include <vector>
#include "sqlcomp/PrivMgrDefs.h"
class ExeCliInterface;

// *****************************************************************************
// *
// * File:         PrivMgrMDTable.h
// * Description:  This file contains the base classes used for Privilege Manager  
// *                metadata tables
// *               
// * Language:     C++
// *
// *****************************************************************************

#include <string>
class ComDiagsArea;
class Queue;
class PrivMgrMDTable;

class PrivMgrRowInfo
{
public:
  PrivMgrRowInfo()
  : grantorID_(NA_UserIdDefault),
    granteeID_(NA_UserIdDefault),
    columnOrdinal_(-1)
  {
    privsBitmap_.reset();
    grantableBitmap_.reset();
  }

  PrivMgrRowInfo (const int32_t grantorID)
  : grantorID_(grantorID),
    granteeID_(NA_UserIdDefault),
    columnOrdinal_(-1)
  {
    privsBitmap_.reset();
    grantableBitmap_.reset();
  }

  PrivMgrRowInfo( const int32_t grantorID,
             const std::string grantorName,
             const int32_t granteeID,
             const std::string granteeName,
             const int32_t columnOrdinal,
             const PrivObjectBitmap privsBitmap,
             const PrivObjectBitmap grantableBitmap)
  : grantorID_(grantorID),
    grantorName_(granteeName),
    granteeID_(granteeID),
    granteeName_(granteeName),
    columnOrdinal_(columnOrdinal),
    privsBitmap_(privsBitmap),
    grantableBitmap_(grantableBitmap)
  {}

  PrivMgrRowInfo(const PrivMgrRowInfo &other);
  
  virtual ~PrivMgrRowInfo() {}

   int32_t getGrantorID() { return grantorID_; }
   std::string getGrantorName() { return grantorName_; }
   int32_t getGranteeID() { return granteeID_; }
   std::string getGranteeName() { return granteeName_; }
   int32_t getColumnOrdinal() { return columnOrdinal_; }
   PrivObjectBitmap getPrivsBitmap() { return privsBitmap_; }
   PrivObjectBitmap getGrantableBitmap() { return grantableBitmap_; }

   void setGrantorID(int32_t grantorID) { grantorID_ = grantorID; }
   void setGrantorName(std::string grantorName) { grantorName_ = grantorName; }
   void setGranteeID(int32_t granteeID) { granteeID_ = granteeID; }
   void setGranteeName(std::string granteeName) { granteeName_ = granteeName; }
   void setColumnOrdinal(int32_t columnOrdinal) { columnOrdinal_ = columnOrdinal; }
   void setPrivsBitmap(PrivObjectBitmap bitmap) { privsBitmap_ = bitmap; }
   void setGrantableBitmap(PrivObjectBitmap bitmap) { grantableBitmap_ = bitmap; }
  
private:
   int32_t grantorID_;
   std::string grantorName_;
   int32_t granteeID_;
   std::string granteeName_;
   int32_t columnOrdinal_;
   PrivObjectBitmap privsBitmap_;
   PrivObjectBitmap grantableBitmap_;
};

  
// *****************************************************************************
// * Class:         PrivMgrMDRow
// * Description:  This is the base class for rows of metadata tables in the 
// *               PrivMgrMD schema.
// *****************************************************************************
class PrivMgrMDRow
{
public:
   PrivMgrMDRow(std::string myTableName, PrivMgrTableEnum myTableEnum);
   PrivMgrMDRow(const PrivMgrMDRow &other);
   virtual ~PrivMgrMDRow();

   virtual PrivMgrRowInfo getPrivRowInfo() {PrivMgrRowInfo row; return row;}

protected:
   std::string myTableName_;
   
private:
   PrivMgrMDRow();

};

// *****************************************************************************
// * Class:         PrivMgrMDTable
// * Description:  This is the base class for metadata tables in the 
// *               PrivMgrMD schema.
// *****************************************************************************
class PrivMgrMDTable
{
public:

// -------------------------------------------------------------------
// Constructors and destructors:
// -------------------------------------------------------------------
   PrivMgrMDTable(
      const std::string & tableName,
      const PrivMgrTableEnum myTableEnum_,
      ComDiagsArea * pDiags = NULL);
   PrivMgrMDTable(const PrivMgrMDTable &other);
   virtual ~PrivMgrMDTable();

// -------------------------------------------------------------------
// Public functions:
// -------------------------------------------------------------------
   PrivStatus CLIFetch(
      ExeCliInterface & cliInterface, 
      const std::string & SQLStatement);
   
   PrivStatus CLIImmediate(const std::string & SQLStatement);
   
   PrivStatus executeFetchAll(
      ExeCliInterface & cliInterface,
      const std::string & SQLStatement,
      Queue * & queue);

   virtual PrivStatus insert(const PrivMgrMDRow &row) = 0;
   
   virtual PrivStatus selectCountWhere(
      const std::string & whereClause,
      int64_t & rowCount);
   
   virtual PrivStatus selectWhereUnique(
      const std::string & whereClause,
      PrivMgrMDRow & row) = 0;
      
   virtual PrivStatus deleteWhere(const std::string & whereClause);
   
   virtual PrivStatus update(const std::string & setClause);
   
   virtual PrivStatus updateWhere(
      const std::string & setClause,
      const std::string & whereClause,
      int64_t &rowCount);
   

protected: 
// -------------------------------------------------------------------
// Data Members:
// -------------------------------------------------------------------
// Fully qualified table name      
std::string      tableName_;
PrivMgrTableEnum myTableEnum_;
ComDiagsArea *   pDiags_;

private:
   PrivMgrMDTable();

};
#endif // PRIVMGR_MDTABLE_H









