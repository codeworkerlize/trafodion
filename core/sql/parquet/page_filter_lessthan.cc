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
 * File:         page_filter_lessthan.cc
 *
 * Description:  All 'lessthan' page filter related code.
 *
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include "page_filter.h"
#include <iostream>
#include <assert.h>

template <typename DType, typename VType>
class TypedPageFilterLessThan : public TypedPageFilter<DType, VType> {

public:
  // Only need to support the native/physical data types.
  TypedPageFilterLessThan(VType max) 
    { range_max_ = max; };

  ~TypedPageFilterLessThan() {};

  skipType::reasonCode canSkip(const std::string& min, const std::string& max) ;

  void display() 
   { std::cout << "PageFilterLessThan: max=" << range_max_; }

protected:
  // The boundary values of a low half range. 
  VType range_max_;
};

PageFilter* PageFilter::MakeLessThan(int32_t max)
{ return new TypedPageFilterLessThan<parquet::Int32Type, int32_t>(max); }

PageFilter* PageFilter::MakeLessThan(int64_t max)
{ return new TypedPageFilterLessThan<parquet::Int64Type, int64_t>(max); }

PageFilter* PageFilter::MakeLessThan(float max)
{ return new TypedPageFilterLessThan<parquet::DoubleType, float>(max); }

PageFilter* PageFilter::MakeLessThan(double max)
{ return new TypedPageFilterLessThan<parquet::DoubleType, double>(max); }

PageFilter* PageFilter::MakeLessThan(std::string& max)
{ return new TypedPageFilterLessThan<parquet::ByteArrayType, std::string>(max); }


template <typename DType, typename VType>
skipType::reasonCode
TypedPageFilterLessThan<DType, VType>::canSkip(const std::string& min, const std::string& max)
{ 
   VType p_min = *((VType*)min.data());  
   if ( range_max_ <= p_min )
     return skipType::REJECT;

   VType p_max  = *((VType*)max.data()); 
   
   // When [p_min, p_max ] < [range_max, inf), it is IN_RANGE
   return ( p_max < range_max_ ) ?  
            skipType::IN_RANGE : skipType::OVERLAP;
}

template <>
skipType::reasonCode
TypedPageFilterLessThan<parquet::Int32Type, int32_t>::canSkip(const std::string& min, const std::string& max)
{ 
   // need to be careful here since the data length in min/max depends
   // on the actual data type coded. For example, for BOOL, the length
   // is 1. We can not assume the length is always sizeof(int32_t).
   int32_t p_min = 0;

   switch (min.size())
   { 
     case 1:
        p_min = *(int8_t*)min.data();
        break;
     case 2:
        p_min = *(int16_t*)min.data();
        break;
     case 4:
        p_min = *(int32_t*)min.data();
        break;

     default:
        assert(false);
   }

   if ( range_max_ <= p_min )
     return skipType::REJECT;
   
   int32_t p_max = 0;

   switch (max.size())
   { 
     case 1:
        p_max= *(int8_t*)max.data();
        break;
     case 2:
        p_max = *(int16_t*)max.data();
        break;
     case 4:
        p_max = *(int32_t*)max.data();
        break;

     default:
        assert(false);
   }

   // When [p_min, p_max ] < [range_max, inf), it is IN_RANGE
   return ( p_max < range_max_ ) ?  
            skipType::IN_RANGE : skipType::OVERLAP;
}

template <>
skipType::reasonCode
TypedPageFilterLessThan<parquet::ByteArrayType, std::string>::canSkip(const std::string& min, const std::string& max)
{ 
   // To test: rangeMax <= min
   //
   int rangeMaxLen = range_max_.length();
   int minLen = min.length();
   skipType::reasonCode ok;

   if ( rangeMaxLen == minLen )
     ok = ( range_max_.compare(min) <= 0 ) ? skipType::REJECT : skipType::OVERLAP;
   else if ( minLen < rangeMaxLen ) {

     // Since min is a constant, need to make a copy of 
     // it first before padding.
     std::string min_padded(rangeMaxLen, ' ');
     min_padded.replace(0, minLen, min);

     ok = ( range_max_.compare(min_padded) <= 0 ) ? skipType::REJECT : skipType::OVERLAP;

   } else {
     range_max_.resize(minLen, ' ');
     ok = ( range_max_.compare(min) <= 0 ) ? skipType::REJECT : skipType::OVERLAP;
     range_max_.resize(rangeMaxLen);
   }

   return ok;
}

///////////////////////////////////////////////////////
template <typename DType, typename VType>
class TypedPageFilterLessThanEqual : public TypedPageFilter<DType, VType> {

public:
  TypedPageFilterLessThanEqual(VType max) 
    { range_max_ = max; };

  ~TypedPageFilterLessThanEqual() {};

  skipType::reasonCode canSkip(const std::string& min, const std::string& max);

  void display() 
    { std::cout << "PageFilterLessThanEqual: max=" << range_max_; }

protected:
  // the boundary values of a low half range. 
  VType range_max_;
};

PageFilter* PageFilter::MakeLessThanEqual(int32_t max)
{ return new TypedPageFilterLessThanEqual<parquet::Int32Type, int32_t>(max); }

PageFilter* PageFilter::MakeLessThanEqual(int64_t max)
{ return new TypedPageFilterLessThanEqual<parquet::Int64Type, int64_t>(max); }

PageFilter* PageFilter::MakeLessThanEqual(float max)
{ return new TypedPageFilterLessThanEqual<parquet::FloatType, float>(max); }

PageFilter* PageFilter::MakeLessThanEqual(double max)
{ return new TypedPageFilterLessThanEqual<parquet::DoubleType, double>(max); }

PageFilter* PageFilter::MakeLessThanEqual(std::string& max)
{ return new TypedPageFilterLessThanEqual<parquet::ByteArrayType, std::string>(max); }

template <typename DType, typename VType>
skipType::reasonCode
TypedPageFilterLessThanEqual<DType, VType>::canSkip(const std::string& min, const std::string& max)
{ 
   VType p_min = *((VType*)min.data()); // 

   if ( range_max_ < p_min )
     return skipType::REJECT;
   
   VType p_max = *((VType*)max.data()); // 

   // When [p_min, p_max ] <= [range_max, inf), it is IN_RANGE
   return ( p_max <= range_max_ ) ?  
            skipType::IN_RANGE : skipType::OVERLAP;
}

template <>
skipType::reasonCode
TypedPageFilterLessThanEqual<parquet::Int32Type, int32_t>::canSkip(const std::string& min, const std::string& max)
{ 
   // need to be careful here since the data length in min/max depends
   // on the actual data type coded. For example, for BOOL, the length
   // is 1. We can not assume the length is always sizeof(int32_t).
   int32_t p_min = 0;

   switch (min.size())
   { 
     case 1:
        p_min = *(int8_t*)min.data();
        break;
     case 2:
        p_min = *(int16_t*)min.data();
        break;
     case 4:
        p_min = *(int32_t*)min.data();
        break;

     default:
        assert(false);
   }

   if ( range_max_ < p_min )
     return skipType::REJECT;
   
   int32_t p_max = 0;
   switch (max.size())
   { 
     case 1:
        p_max = *(int8_t*)max.data();
        break;
     case 2:
        p_max = *(int16_t*)max.data();
        break;
     case 4:
        p_max = *(int32_t*)max.data();
        break;

     default:
        assert(false);
   }

   // When [p_min, p_max ] <= [range_max, inf), it is IN_RANGE
   return ( p_max <= range_max_ ) ?  
            skipType::IN_RANGE : skipType::OVERLAP;
}


template <>
skipType::reasonCode
TypedPageFilterLessThanEqual<parquet::ByteArrayType, std::string>::canSkip(const std::string& min, const std::string& max)
{ 
   // To test: rangeMax < min
   //
   int rangeMaxLen = range_max_.length();
   int minLen = min.length();
   skipType::reasonCode ok;

   if ( rangeMaxLen == minLen )
     ok = ( range_max_.compare(min) < 0 ) ? skipType::REJECT : skipType::OVERLAP;
   else if ( minLen < rangeMaxLen ) {

     std::string min_padded(rangeMaxLen, ' ');
     min_padded.replace(0, minLen, min);

     ok = ( range_max_.compare(min_padded) < 0 ) ? skipType::REJECT : skipType::OVERLAP;

   } else {
     range_max_.resize(minLen, ' ');
     ok = ( range_max_.compare(min) < 0 ) ? skipType::REJECT : skipType::OVERLAP;
     range_max_.resize(rangeMaxLen);
   }

   return ok;
}

