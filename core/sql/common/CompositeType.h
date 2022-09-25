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
#ifndef COMPOSITETYPE_H
#define COMPOSITETYPE_H
/* -*-C++-*-
**************************************************************************
*
* File:         CompositeType.h
* Description:  Composite types(ARRAY, ROW/STRUCT, MAP)
* Created:      
* Language:     C++
*
*
*
*
**************************************************************************
*/

// -----------------------------------------------------------------------

#include <limits.h>
#include "BaseTypes.h"
#include "NAType.h"
#include "ComDiags.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class SQLArray;
class SQLRow;

//-----------------------------------------------------------------------
static NAString LiteralArray("ARRAY");
static NAString LiteralRow("ROW");


// ***********************************************************************
//
//  CompositeType: Base class for composite types (array, row)
//
// ***********************************************************************
class CompositeType : public NAType
{
public:

  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  CompositeType(NAMemory *heap ,const NAString &adtName,
                NABuiltInTypeEnum ev, Int32 compFormat = COM_UNINITIALIZED_FORMAT);
  
  NABoolean isComposite() const 
  {
    return TRUE;
  }

  virtual NAString genCompDefnStr() const = 0;

  virtual NAType *newCopy2(Int32 compFormat, CollHeap * h=0) const = 0;

  // ---------------------------------------------------------------------
  // A method which tells if a conversion error can occur when converting
  // a value of this type to the target type.
  // ---------------------------------------------------------------------
  virtual NABoolean errorsCanOccur (const NAType& target, NABoolean lax=TRUE) const
  {
    return TRUE;
  }

  // TRUE, validation passed. 
  // FALSE, validation failed. Error set in diags.
  virtual NABoolean validate(ComDiagsArea *diags)
  {
    return FALSE;
  }

  // ---------------------------------------------------------------------
  // Accesor methods
  // ---------------------------------------------------------------------
  virtual UInt32 getNumElements() { return 0; }
  virtual const UInt32 getNumElements() const { return 0; }
  virtual NAType *getElementType(Lng32 elemNum) { return NULL; }
  virtual const NAType *getElementType(Lng32 elemNum) const { return NULL; }

  // Returns max length of data returned from hive.
  // See method fillCompositeType in TrafParquetFileReader.java for data
  // layout of returned row and its length.
  // See ExpCompositeHiveCast::eval for how hive source data is interpreted.
  virtual Int32 getHiveSourceMaxLen() { return 0; }

  Int32 getCompFormat() { return compFormat_; }
  Int32 const getCompFormat() const { return compFormat_; }
  void setCompFormat(Int32 cf) { compFormat_ = cf; }

  NAString &getCompDefnStr();
  const NAString &getCompDefnStr() const;
  void setCompDefnStr(NAString v) { compDefnStr_ = v; }
private:
  Int32 compFormat_;
  NAString compDefnStr_;
}; // class CompositeType

// ***********************************************************************
//
//  SQLArray : The array data type
//
// ***********************************************************************
class SQLArray : public CompositeType
{
public:

  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  SQLArray(NAMemory *heap, const NAType *elementType, const Lng32 arraySize,
           Int32 compFormat = COM_UNINITIALIZED_FORMAT);

  virtual short getMyTypeAsText(NAString * outputStr,  // output
                                NABoolean addNullability = TRUE,
                                NABoolean addCollation = TRUE) const;

  virtual short getHiveTypeStr(Lng32 hiveType, Lng32 precision, Lng32 scale,
                               NAString * outputStr/*out*/) const;

  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(CollHeap * h=0) const;
  virtual NAType *newCopy2(Int32 compFormat, CollHeap * h=0) const;

  virtual short getFSDatatype() const;

  // ---------------------------------------------------------------------
  // Get a simpler textual description of the type.  
  // ---------------------------------------------------------------------
  virtual NAString getSimpleTypeName() const;

  // ---------------------------------------------------------------------
  // Get the external/SQL name of the Type.
  // ---------------------------------------------------------------------
  virtual NAString getTypeSQLname(NABoolean terse = FALSE) const;

  // returns info about fields of this array in a string format.
  //  Ex:  "int ARRAY[10]" or "ROW(a int, b int) ARRAY[10]"
  virtual NAString genCompDefnStr() const;

  // ---------------------------------------------------------------------
  // A virtual function for synthesizing the type of a binary operator.
  // ---------------------------------------------------------------------
  virtual const NAType* synthesizeType(enum NATypeSynthRuleEnum synthRule,
                                       const NAType& operand1,
                                       const NAType& operand2,
				       CollHeap* h,
				       UInt32 *flags = NULL) const;

  // ---------------------------------------------------------------------
  // Are the two types compatible?
  // ---------------------------------------------------------------------
  virtual NABoolean isCompatible(const NAType& other, UInt32 * flags = NULL) const;
  virtual NABoolean operator==(const NAType& other) const;

  // ---------------------------------------------------------------------
  // Accesor methods
  // ---------------------------------------------------------------------
  const NAType * getElementType() const { return elementType_; }
  const Lng32 getArraySize() const { return arraySize_; }
  virtual UInt32 getNumElements() { return arraySize_; }
  virtual const UInt32 getNumElements() const { return arraySize_; }

  virtual NAType *getElementType(Lng32 elemNum) { return (NAType*)getElementType(); }
  virtual const NAType *getElementType(Lng32 elemNum) const { return getElementType(); }

  virtual Lng32 getNumLevels() const
  {
    return (elementType_->getNumLevels() + 1);
  }

  // TRUE, validation passed. 
  // FALSE, validation failed. Error set in diags.
  virtual NABoolean validate(ComDiagsArea *diags);

  virtual Lng32 getDisplayLength() const;

  virtual Int32 getHiveSourceMaxLen();

private:
  const NAType *elementType_;
  Lng32 arraySize_;
}; // class SQLArray

// ***********************************************************************
//
//  SQLRow : The ROW(struct) data type
//
// ***********************************************************************
class SQLRow : public CompositeType
{
public:

  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  SQLRow(NAMemory *heap);
  SQLRow(NAMemory *heap, 
         NAArray<NAString> *fieldNames,
         NAArray<NAType*> *fieldTypes,
         Int32 compFormat = COM_UNINITIALIZED_FORMAT);
  
  virtual short getMyTypeAsText(NAString * outputStr,  // output
                                NABoolean addNullability = TRUE,
                                NABoolean addCollation = TRUE) const;

  virtual short getHiveTypeStr(Lng32 hiveType, Lng32 precision, Lng32 scale,
                               NAString * outputStr/*out*/) const;

  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(CollHeap * h=0) const;
  virtual NAType *newCopy2(Int32 compFormat, CollHeap * h=0) const;

  virtual short getFSDatatype() const;

  // ---------------------------------------------------------------------
  // Get a simpler textual description of the type.  
  // ---------------------------------------------------------------------
  virtual NAString getSimpleTypeName() const;

  // ---------------------------------------------------------------------
  // Get the external/SQL name of the Type.
  // ---------------------------------------------------------------------
  virtual NAString getTypeSQLname(NABoolean terse = FALSE) const;

  // returns info about fields of this struct in a string format.
  //  Ex:  "ROW(a int, b char(10))"
  virtual NAString genCompDefnStr() const;

  // ---------------------------------------------------------------------
  // A virtual function for synthesizing the type of a binary operator.
  // ---------------------------------------------------------------------
  virtual const NAType* synthesizeType(enum NATypeSynthRuleEnum synthRule,
                                       const NAType& operand1,
                                       const NAType& operand2,
				       CollHeap* h,
				       UInt32 *flags = NULL) const;

  // ---------------------------------------------------------------------
  // Are the two types compatible?
  // ---------------------------------------------------------------------
  virtual NABoolean isCompatible(const NAType& other, UInt32 * flags = NULL) const;
  virtual NABoolean operator==(const NAType& other) const;

  // TRUE, validation passed. 
  // FALSE, validation failed. Error set in diags.
  virtual NABoolean validate(ComDiagsArea *diags);

  // ---------------------------------------------------------------------
  // Accesor methods
  // ---------------------------------------------------------------------
  NAArray<NAString> &fieldNames() {return *fieldNames_;}
  NAArray<NAType*>  &fieldTypes() {return *fieldTypes_;}
  NAArray<NAString> *fieldNamesPtr() {return fieldNames_;}
  NAArray<NAType*>  *fieldTypesPtr() {return fieldTypes_;}

  virtual UInt32 getNumElements() { return (fieldTypes_ ? fieldTypes_->entries() : 0); }
  virtual const UInt32 getNumElements() const { return (fieldTypes_ ? fieldTypes_->entries() : 0); }

  virtual NAType *getElementType(Lng32 elemNum) 
  { 
    return (NAType*)getFieldType(elemNum); 
  }
  virtual const NAType *getElementType(Lng32 elemNum) const 
  { 
    return getFieldType(elemNum); // elemNum is 1-based
  }

  // returned elem number is 1-based. 
  // return: TRUE, if found. FALSE, if not found.
  NABoolean getElementInfo(const NAString &fieldName,
                           const NAType* &elemType, Int32 &elemNum);

  virtual Lng32 getDisplayLength() const;

  virtual Lng32 getNumLevels() const
  {
    return numLevels_;
  }

  virtual Int32 getHiveSourceMaxLen();

private:
  // passed in idx is 1-based
  const NAString &getFieldName(CollIndex idx) const;
  const NAType * getFieldType(CollIndex idx) const;

  NAArray<NAString> *fieldNames_;
  NAArray<NAType*>  *fieldTypes_;

  Lng32 numLevels_;
}; // class SQLRow

#endif
