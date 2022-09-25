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
 * File:         value_filter_range.cc
 *
 * Description:  All range value filter related code.
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


//
// Range value filter
//
template <typename DType, typename VType>
class TypedValueFilterRange : public TypedValueFilter<DType> 
{

public:
  TypedValueFilterRange(VType min, VType max) 
    { min_ = min; max_ = max; };

  ~TypedValueFilterRange() {};

  void display()
    { std::cout << "ValueFilterRange: " 
                << "min=" << min_ 
                << ", max=" << max_; }

protected:
   bool compare(VType x);

protected:
  VType min_;
  VType max_;
};

ValueFilter* ValueFilter::MakeRange(bool min, bool max)
{ return new TypedValueFilterRange<arrow::BooleanType, bool>(min, max); }

ValueFilter* ValueFilter::MakeRange(int8_t min, int8_t max)
{ return new TypedValueFilterRange<arrow::Int8Type, int8_t>(min, max); }

ValueFilter* ValueFilter::MakeRange(int16_t min, int16_t max)
{ return new TypedValueFilterRange<arrow::Int16Type, int16_t>(min, max); }

ValueFilter* ValueFilter::MakeRange(int32_t min, int32_t max)
{ return new TypedValueFilterRange<arrow::Int32Type, int32_t>(min, max); }

ValueFilter* ValueFilter::MakeRange(int64_t min, int64_t max)
{ return new TypedValueFilterRange<arrow::Int64Type, int64_t>(min, max); }

ValueFilter* ValueFilter::MakeRange(float min, float max)
{ return new TypedValueFilterRange<arrow::FloatType, float>(min, max); }

ValueFilter* ValueFilter::MakeRange(double min, double max)
{ return new TypedValueFilterRange<arrow::DoubleType, double>(min, max); }

ValueFilter* ValueFilter::MakeRange(parquet::Int96 min, parquet::Int96 max)
{ return new TypedValueFilterRange<arrow::Time96Type, parquet::Int96>(min, max); }

ValueFilter* ValueFilter::MakeRange(std::string& min, std::string& max)
{ return new TypedValueFilterRange<std::string, std::string>(min, max); }




template <typename DType, typename VType>
bool TypedValueFilterRange<DType, VType>::compare(VType x)
{ return min_ <= x && x <= max_; }

template <>
bool TypedValueFilterRange<std::string, std::string>::compare(std::string x)
{
   // Assume the length for min_ and max_ is properly adjusted for the
   // SQL data type and they are equal.
   int32_t xLen = x.length();
   int32_t minLen = min_.length();
   bool ok = false;

   if ( xLen == minLen ) {
     ok = ( min_.compare(x) <= 0 && 
            x.compare(max_) <= 0
          );
   } 
   else if ( xLen < minLen ) {

     x.resize(minLen, ' '); // space padding

     ok = ( min_.compare(x) <= 0 && 
            x.compare(max_) <= 0);

     x.resize(xLen); // restore

   } else {

     min_.resize(xLen, ' '); // space padding

     ok = ( min_.compare(x) <= 0 && 
            x.compare(max_) <= 0 );

     min_.resize(minLen); // restore
   }

   return ok;
}



