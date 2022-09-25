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
 * File:         value_filter_greaterthan.cc
 *
 * Description:  All greaterthan and greaterthanequal value 
 *               filter related code.
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
class TypedValueFilterGreaterThan : public TypedValueFilter<DType> 
{

public:
  TypedValueFilterGreaterThan(VType min) 
    { v_min_ = min; };

  ~TypedValueFilterGreaterThan() {};

  void display()
   { std::cout << "GreaterThan: min=" << v_min_; }

protected:
  bool compare(VType x)
   { return v_min_ < x; }

protected:
  VType v_min_;
};

ValueFilter* ValueFilter::MakeGreaterThan(bool min)
{ return new TypedValueFilterGreaterThan<arrow::BooleanType, bool>(min); }

ValueFilter* ValueFilter::MakeGreaterThan(int8_t min)
{ return new TypedValueFilterGreaterThan<arrow::Int8Type, int8_t>(min); }

ValueFilter* ValueFilter::MakeGreaterThan(int16_t min)
{ return new TypedValueFilterGreaterThan<arrow::Int16Type, int16_t>(min); }

ValueFilter* ValueFilter::MakeGreaterThan(int32_t min)
{ return new TypedValueFilterGreaterThan<arrow::Int32Type, int32_t>(min); }

ValueFilter* ValueFilter::MakeGreaterThan(int64_t min)
{ return new TypedValueFilterGreaterThan<arrow::Int64Type, int64_t>(min); }

ValueFilter* ValueFilter::MakeGreaterThan(float min)
{ return new TypedValueFilterGreaterThan<arrow::FloatType, float>(min); }

ValueFilter* ValueFilter::MakeGreaterThan(double min)
{ return new TypedValueFilterGreaterThan<arrow::DoubleType, double>(min); }

ValueFilter* ValueFilter::MakeGreaterThan(parquet::Int96 min)
{ return new TypedValueFilterGreaterThan<arrow::Time96Type, parquet::Int96>(min); }

ValueFilter* ValueFilter::MakeGreaterThan(std::string& min)
{ return new TypedValueFilterGreaterThan<std::string, std::string>(min); }


template <>
bool TypedValueFilterGreaterThan<std::string, std::string>::compare(std::string x)
{
   // SQL space padding version
   int minLen = v_min_.length();
   int xLen = x.length();
   bool ok = false;

   if ( minLen == xLen )
     return v_min_.compare(x) < 0;
   else if ( xLen < minLen ) {
     x.resize(minLen, ' '); // space padding
     ok = (v_min_.compare(x) < 0);
     x.resize(xLen); // restore 

   } else {
     v_min_.resize(xLen, ' '); // space padding
     ok = (v_min_.compare(x) < 0);
     v_min_.resize(minLen);  // restore
   }
   return ok;
}

//////////////////////////////////////////////////////////////////
template <typename DType, typename VType>
class TypedValueFilterGreaterThanEqual : public TypedValueFilter<DType> 
{

public:
  TypedValueFilterGreaterThanEqual(VType min) 
    { v_min_ = min; };

  ~TypedValueFilterGreaterThanEqual() {};

  void display() 
    { std::cout << "GreaterThanEqual: min=" << v_min_; }

protected:
  bool compare(VType v) 
   { return v_min_ <= v; }

protected:
  VType v_min_;
};

ValueFilter* ValueFilter::MakeGreaterThanEqual(bool min)
{ return new TypedValueFilterGreaterThanEqual<arrow::BooleanType, bool>(min); }

ValueFilter* ValueFilter::MakeGreaterThanEqual(int8_t min)
{ return new TypedValueFilterGreaterThanEqual<arrow::Int8Type, int8_t>(min); }

ValueFilter* ValueFilter::MakeGreaterThanEqual(int16_t min)
{ return new TypedValueFilterGreaterThanEqual<arrow::Int16Type, int16_t>(min); }

ValueFilter* ValueFilter::MakeGreaterThanEqual(int32_t min)
{ return new TypedValueFilterGreaterThanEqual<arrow::Int32Type, int32_t>(min); }

ValueFilter* ValueFilter::MakeGreaterThanEqual(int64_t min)
{ return new TypedValueFilterGreaterThanEqual<arrow::Int64Type, int64_t>(min); }

ValueFilter* ValueFilter::MakeGreaterThanEqual(float min)
{ return new TypedValueFilterGreaterThanEqual<arrow::FloatType, float>(min); }

ValueFilter* ValueFilter::MakeGreaterThanEqual(double min)
{ return new TypedValueFilterGreaterThanEqual<arrow::DoubleType, double>(min); }

ValueFilter* ValueFilter::MakeGreaterThanEqual(parquet::Int96 min)
{ return new TypedValueFilterGreaterThanEqual<arrow::Time96Type, parquet::Int96>(min); }

ValueFilter* ValueFilter::MakeGreaterThanEqual(std::string& min)
{ return new TypedValueFilterGreaterThanEqual<std::string, std::string>(min); }


template <>
bool TypedValueFilterGreaterThanEqual<std::string, std::string>::compare(std::string x)
{
   // Non SQL space padding version
   // return v_min_.compare(x) <= 0;
   //
   // SQL space padding version
   int minLen = v_min_.length();
   int xLen = x.length();
   bool ok = false;

   if ( minLen == xLen )
     return v_min_.compare(x) <= 0;
   else if ( xLen < minLen ) {

     x.resize(minLen, ' '); // space padding
     ok = (v_min_.compare(x) <= 0);
     x.resize(xLen); // restore 

   } else {
     v_min_.resize(xLen, ' '); // space padding
     ok = (v_min_.compare(x) <= 0);
     v_min_.resize(minLen); // restore
   }
   return ok;
}
