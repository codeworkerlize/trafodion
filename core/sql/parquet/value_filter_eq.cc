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
 * File:         value_filter_eq.cc
 *
 * Description:  All equi value filter related code.
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

//====================================================================
// Value filter for EQ predicates
template <typename DType, typename VType>
class TypedValueFilterEQ : public TypedValueFilter<DType> 
{

public:
  TypedValueFilterEQ(VType v) { v_ = v; };
  ~TypedValueFilterEQ() {};

  void display() { std::cout << "ValueFilterEQ(): v=" << v_; }

protected:
  bool compare(VType x);

protected:
  VType v_;
};

ValueFilter* ValueFilter::MakeEQ(bool v)
{ return new TypedValueFilterEQ<arrow::BooleanType, bool>(v); }

ValueFilter* ValueFilter::MakeEQ(int8_t v)
{ return new TypedValueFilterEQ<arrow::Int8Type, int8_t>(v); }

ValueFilter* ValueFilter::MakeEQ(int16_t v)
{ return new TypedValueFilterEQ<arrow::Int16Type, int16_t>(v); }

ValueFilter* ValueFilter::MakeEQ(int32_t v)
{ return new TypedValueFilterEQ<arrow::Int32Type, int32_t>(v); }

ValueFilter* ValueFilter::MakeEQ(int64_t v)
{ return new TypedValueFilterEQ<arrow::Int64Type, int64_t>(v); }

ValueFilter* ValueFilter::MakeEQ(float v)
{ return new TypedValueFilterEQ<arrow::FloatType, float>(v); }

ValueFilter* ValueFilter::MakeEQ(double v)
{ return new TypedValueFilterEQ<arrow::DoubleType, double>(v); }

ValueFilter* ValueFilter::MakeEQ(std::string& v)
{ return new TypedValueFilterEQ<std::string, std::string>(v); }

ValueFilter* ValueFilter::MakeEQ(parquet::Int96 v)
{ return new TypedValueFilterEQ<arrow::Time96Type, parquet::Int96>(v); }

template <typename DType, typename VType>
bool TypedValueFilterEQ<DType, VType>::compare(VType x)
{ return x == v_; }

template <>
bool TypedValueFilterEQ<std::string, std::string>::compare(std::string x)
{
   int vLen = v_.length();
   int xLen = x.length();
   bool ok = false;

   if ( vLen == xLen )
     ok = x.compare(v_) == 0 ;
   else if ( xLen < vLen ) {

     x.resize(vLen, ' ');
     ok = x.compare(v_) == 0 ;
     x.resize(xLen);

   } else {

     v_.resize(xLen, ' ');
     ok = x.compare(v_) == 0 ;
     v_.resize(vLen, ' ');

   }
   return ok;
}



