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
#ifndef ELEMDDLSGOPTION_H
#define ELEMDDLSGOPTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLSGOption.h
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

#include "common/BaseTypes.h"
#include "ElemDDLNode.h"
#include "parser/ElemDDLSGOptions.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLSGOption;
class ElemDDLSGOptionStartValue;
class ElemDDLSGOptionIncrement;
class ElemDDLSGOptionMinValue;
class ElemDDLSGOptionMaxValue;
class ElemDDLSGOptionCacheOption;
class ElemDDLSGOptionOrderOption;
class ElemDDLSGOptionCycleOption;
class ElemDDLSGOptionDatatype;
class ElemDDLSGOptionResetOption;
class ElemDDLSGOptionSystemOption;
class ElemDDLSGOptionNextValOption;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of base class ElemDDLSGOption
// -----------------------------------------------------------------------
class ElemDDLSGOption : public ElemDDLNode  
{

public:

  // default constructor
  ElemDDLSGOption(OperatorTypeEnum operatorType);

  // virtual destructor
  virtual ~ElemDDLSGOption();

  // cast
  virtual ElemDDLSGOption * castToElemDDLSGOption();

  // method for tracing
  virtual const NAString getText() const;
 
}; // class ElemDDLSGOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionStartValue
// -----------------------------------------------------------------------
class ElemDDLSGOptionStartValue : public ElemDDLSGOption
{

public:

  // constructors
  ElemDDLSGOptionStartValue(Int64 value);
  
  // virtual destructor
  virtual ~ElemDDLSGOptionStartValue();

  // cast
  virtual ElemDDLSGOptionStartValue * castToElemDDLSGOptionStartValue();

  // accessors
  inline Int64 getValue() const;
   
  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;


  // method for building text
  virtual NAString getSyntax() const;

private:

  //
  // data member(s)
  //

  Int64 value_;
  
}; // class ElemDDLSGOptionStartValue

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionRestartValue
// -----------------------------------------------------------------------
class ElemDDLSGOptionRestartValue : public ElemDDLSGOption
{

public:

  // constructors
  ElemDDLSGOptionRestartValue(Int64 value);
  
  // virtual destructor
  virtual ~ElemDDLSGOptionRestartValue();

  // cast
  virtual ElemDDLSGOptionRestartValue * castToElemDDLSGOptionRestartValue();

  // accessors
  inline Int64 getValue() const;
   
  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;


  // method for building text
  virtual NAString getSyntax() const;

private:

  //
  // data member(s)
  //

  Int64 value_;
  
}; // class ElemDDLSGOptionRestartValue

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionIncrement
// -----------------------------------------------------------------------
class ElemDDLSGOptionIncrement : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionIncrement(Int64 value);

  // virtual destructor
  virtual ~ElemDDLSGOptionIncrement();

  // cast
  virtual ElemDDLSGOptionIncrement * castToElemDDLSGOptionIncrement();

  // accessors
  inline Int64 getValue() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

private:

  //
  // data member(s)
  //

  Int64 value_;

}; // class ElemDDLSGOptionIncrement


// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionMinValue
// -----------------------------------------------------------------------
class ElemDDLSGOptionMinValue : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionMinValue(Int64 value);
  ElemDDLSGOptionMinValue(NABoolean noMinValue);

  // virtual destructor
  virtual ~ElemDDLSGOptionMinValue();

  // cast
  virtual ElemDDLSGOptionMinValue * castToElemDDLSGOptionMinValue();

  // accessors
  inline Int64 getValue() const;
  inline NABoolean isNoMinValue() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;


  // method for building text
  virtual NAString getSyntax() const;

private:

  //
  // data member(s)
  //

  Int64 value_;
  NABoolean isNoMinValue_;

}; // class ElemDDLSGOptionMinValue

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionMaxValue
// -----------------------------------------------------------------------
class ElemDDLSGOptionMaxValue : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionMaxValue(Int64 value);
  ElemDDLSGOptionMaxValue(NABoolean noMaxValue);

  // virtual destructor
  virtual ~ElemDDLSGOptionMaxValue();

  // cast
  virtual ElemDDLSGOptionMaxValue * castToElemDDLSGOptionMaxValue();

  // accessors
  inline Int64 getValue() const;
  inline NABoolean isNoMaxValue() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

private:

  //
  // data member(s)
  //

  Int64 value_;
  NABoolean isNoMaxValue_;

}; // class ElemDDLSGOptionMaxValue

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionCycleOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionCycleOption : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionCycleOption(NABoolean cycle);
  ElemDDLSGOptionCycleOption();

  // virtual destructor
  virtual ~ElemDDLSGOptionCycleOption();

  // cast
  virtual ElemDDLSGOptionCycleOption * castToElemDDLSGOptionCycleOption();

  // accessors
  inline NABoolean isCycle()   const { return cycle_ == TRUE; };
  inline NABoolean isNoCycle() const { return cycle_ == FALSE; };
  inline NABoolean getValue()  const { return cycle_; };

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

private:

  //
  // data member(s)
  //

  NABoolean cycle_;

}; // class ElemDDLSGOptionCycleOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionCacheOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionCacheOption : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionCacheOption(NABoolean cache, Int64 cacheSize);
  ElemDDLSGOptionCacheOption();

  // virtual destructor
  virtual ~ElemDDLSGOptionCacheOption();

  // cast
  virtual ElemDDLSGOptionCacheOption * castToElemDDLSGOptionCacheOption();

  // accessors
  inline NABoolean isCache()   const { return cache_ == TRUE; };
  inline NABoolean isNoCache() const { return cache_ == FALSE; };
  inline Int64 getCacheSize()  const { return cacheSize_; };

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

private:

  //
  // data member(s)
  //
  NABoolean cache_;
  Int64 cacheSize_;

}; // class ElemDDLSGOptionCacheOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionOrderOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionOrderOption : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionOrderOption(NABoolean isNoOrder = false);

  // virtual destructor
  virtual ~ElemDDLSGOptionOrderOption();
  
  virtual ElemDDLSGOptionOrderOption * castToElemDDLSGOptionOrderOption();

  NABoolean isNoOrder() { return isNoOrder_; }

private:

  NABoolean isNoOrder_;

}; // class ElemDDLSGOptionOrderOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionSystemOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionSystemOption : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionSystemOption(Int64 timeout=-1): ElemDDLSGOption(ELM_SG_OPT_SYSTEM_OPTION_ELEM) {timeout_ = timeout;};

  // virtual destructor
  virtual ~ElemDDLSGOptionSystemOption(){};
  
  virtual ElemDDLSGOptionSystemOption * castToElemDDLSGOptionSystemOption(){return this;};

 // methods for tracing
  virtual const NAString displayLabel1() const { return (NAString("GLOBAL "));};
  virtual const NAString getText() const {return "ElemDDLSGOptionSystemOption";};

  // method for building text
  virtual NAString getSyntax() const  ;
  Int64 getTimeout() { return timeout_;};
private :
  Int64 timeout_; // in microseconds.

}; // class ElemDDLSGOptionOrderOption
// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionDatatype
// -----------------------------------------------------------------------
class ElemDDLSGOptionDatatype : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionDatatype(ComFSDataType dt);
  ElemDDLSGOptionDatatype();

  // virtual destructor
  virtual ~ElemDDLSGOptionDatatype();

  // cast
  virtual ElemDDLSGOptionDatatype * castToElemDDLSGOptionDatatype();

  // accessors
  inline ComFSDataType getDatatype() { return dt_;}

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

private:

  ComFSDataType dt_;

}; // class ElemDDLSGOptionDatatype

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionResetOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionResetOption : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionResetOption();

  // virtual destructor
  virtual ~ElemDDLSGOptionResetOption();

  // cast
  virtual ElemDDLSGOptionResetOption * castToElemDDLSGOptionResetOption();

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

private:
}; // class ElemDDLSGOptionResetOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionNextValOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionNextValOption : public ElemDDLSGOption
{

public:

  // constructor
  ElemDDLSGOptionNextValOption(Int64 nextVal);

  // virtual destructor
  virtual ~ElemDDLSGOptionNextValOption();

  // cast
  virtual ElemDDLSGOptionNextValOption * castToElemDDLSGOptionNextValOption();

  // accessors
  inline Int64 getNextVal()  const { return nextVal_; };

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

private:

  //
  // data member(s)
  //
  Int64 nextVal_;

}; // class ElemDDLSGOptionNextValOption


// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionStartValue
// -----------------------------------------------------------------------

inline Int64
ElemDDLSGOptionStartValue::getValue() const
{
  return value_;
}

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionRestartValue
// -----------------------------------------------------------------------

inline Int64
ElemDDLSGOptionRestartValue::getValue() const
{
  return value_;
}

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionIncrement
// -----------------------------------------------------------------------

inline Int64
ElemDDLSGOptionIncrement::getValue() const
{
  return value_;
}

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionMaxValue
// -----------------------------------------------------------------------

inline Int64
ElemDDLSGOptionMaxValue::getValue() const
{
  return value_;
}

inline NABoolean
ElemDDLSGOptionMaxValue::isNoMaxValue() const
{
  return isNoMaxValue_;
}

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionMinValue
// -----------------------------------------------------------------------

inline Int64
ElemDDLSGOptionMinValue::getValue() const
{
  return value_;
}

inline NABoolean
ElemDDLSGOptionMinValue::isNoMinValue() const
{
  return isNoMinValue_;
}

#endif // ELEMDDLSGOPTION_H
