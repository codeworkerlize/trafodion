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
 *****************************************************************************
 *
 * File:         value_filter_lessthan.cc
 *
 * Description:  All lessthan and lessthanequal value filter 
 *               related code.
 *
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include "parquet/value_filter.h"
#include "parquet/types.h"
#include <iostream>

template <typename DType, typename VType>
class TypedValueFilterLessThan : public TypedValueFilter<DType> 
{

public:
  TypedValueFilterLessThan(VType max) 
    { v_max_ = max; };

  ~TypedValueFilterLessThan() {};

  void display() 
    { std::cout << "LessThan(): max=" << v_max_;  }

protected:
   bool compare(VType x)
    { return x < v_max_; }

protected:
   VType v_max_;
};

ValueFilter* ValueFilter::MakeLessThan(bool max)
{ return new TypedValueFilterLessThan<arrow::BooleanType, bool>(max); }

ValueFilter* ValueFilter::MakeLessThan(int8_t max)
{ return new TypedValueFilterLessThan<arrow::Int8Type, int8_t>(max); }

ValueFilter* ValueFilter::MakeLessThan(int16_t max)
{ return new TypedValueFilterLessThan<arrow::Int16Type, int16_t>(max); }

ValueFilter* ValueFilter::MakeLessThan(int32_t max)
{ return new TypedValueFilterLessThan<arrow::Int32Type, int32_t>(max); }

ValueFilter* ValueFilter::MakeLessThan(int64_t max)
{ return new TypedValueFilterLessThan<arrow::Int64Type, int64_t>(max); }

ValueFilter* ValueFilter::MakeLessThan(float max)
{ return new TypedValueFilterLessThan<arrow::FloatType, float>(max); }

ValueFilter* ValueFilter::MakeLessThan(double max)
{ return new TypedValueFilterLessThan<arrow::DoubleType, double>(max); }

ValueFilter* ValueFilter::MakeLessThan(std::string& max)
{ return new TypedValueFilterLessThan<std::string, std::string>(max); }

ValueFilter* ValueFilter::MakeLessThan(parquet::Int96 max)
{ return new TypedValueFilterLessThan<arrow::Time96Type, parquet::Int96>(max); }


template <>
bool TypedValueFilterLessThan<std::string, std::string>::compare(std::string x)
{ 
   // SQL semantics. Strings are padded if necessary.
   int rangeMaxLen = v_max_.length();
   int xLen = x.length();
   bool ok = false;

   if ( rangeMaxLen == xLen )
     return x.compare(v_max_) < 0; 
   else if ( xLen < rangeMaxLen ) {

     x.resize(rangeMaxLen, ' ');
     ok = (x.compare(v_max_) < 0); 
     x.resize(xLen);
   } else {
     v_max_.resize(xLen, ' ');
     ok = (x.compare(v_max_) < 0); 
     v_max_.resize(rangeMaxLen);
   }
   return ok;
}

//////////////////////////////////////////////////////////////////
template <typename DType, typename VType>
class TypedValueFilterLessThanEqual : public TypedValueFilter<DType> 
{

public:
  TypedValueFilterLessThanEqual(VType max) 
    { v_max_ = max; };


  ~TypedValueFilterLessThanEqual() {};

  void display() 
    { std::cout << "LessThanEqual(): max=" << v_max_;  }

protected:
   bool compare(VType x)
      { return x <= v_max_; }

protected:
  VType v_max_;
};

template <>
bool TypedValueFilterLessThanEqual<std::string, std::string>::compare(std::string x)
{
   int rangeMaxLen = v_max_.length();
   int xLen = x.length();
   bool ok = false;

   if ( rangeMaxLen == xLen )
     return x.compare(v_max_) <= 0; 
   else if ( xLen < rangeMaxLen ) {

     x.resize(rangeMaxLen, ' ');
     ok = (x.compare(v_max_) <= 0); 
     x.resize(xLen);

   } else {
     v_max_.resize(xLen, ' ');
     ok = (x.compare(v_max_) <= 0); 
     v_max_.resize(rangeMaxLen, ' ');
   }
   return ok;
}

ValueFilter* ValueFilter::MakeLessThanEqual(bool max)
{ return new TypedValueFilterLessThanEqual<arrow::BooleanType, bool>(max); }

ValueFilter* ValueFilter::MakeLessThanEqual(int8_t max)
{ return new TypedValueFilterLessThanEqual<arrow::Int8Type, int8_t>(max); }

ValueFilter* ValueFilter::MakeLessThanEqual(int16_t max)
{ return new TypedValueFilterLessThanEqual<arrow::Int16Type, int16_t>(max); }

ValueFilter* ValueFilter::MakeLessThanEqual(int32_t max)
{ return new TypedValueFilterLessThanEqual<arrow::Int32Type, int32_t>(max); }

ValueFilter* ValueFilter::MakeLessThanEqual(int64_t max)
{ return new TypedValueFilterLessThanEqual<arrow::Int64Type, int64_t>(max); }

ValueFilter* ValueFilter::MakeLessThanEqual(float max)
{ return new TypedValueFilterLessThanEqual<arrow::FloatType, float>(max); }

ValueFilter* ValueFilter::MakeLessThanEqual(double max)
{ return new TypedValueFilterLessThanEqual<arrow::DoubleType, double>(max); }

ValueFilter* ValueFilter::MakeLessThanEqual(std::string& max)
{ return new TypedValueFilterLessThanEqual<std::string, std::string>(max); }

ValueFilter* ValueFilter::MakeLessThanEqual(parquet::Int96 max)
{ return new TypedValueFilterLessThanEqual<arrow::Time96Type, parquet::Int96>(max); }

