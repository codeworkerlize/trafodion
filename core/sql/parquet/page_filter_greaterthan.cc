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
 * File:         page_filter_greaterthan.cc
 *
 * Description:  All 'greaterthan' page filter related code.
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

//====================================================================
template <typename DType, typename VType>
class TypedPageFilterGreaterThan : public TypedPageFilter<DType, VType> {

public:
  TypedPageFilterGreaterThan(VType min) 
    { range_min_ = min; };

  ~TypedPageFilterGreaterThan() {};

  skipType::reasonCode canSkip(const std::string& min, const std::string& max);

  void display() 
    {  std::cout << "PageFilterGreaterThan: min=" << range_min_; }

protected:
  // the boundary values of the high half range. 
  VType range_min_;
};

PageFilter* PageFilter::MakeGreaterThan(int32_t min)
{ return new TypedPageFilterGreaterThan<parquet::Int32Type, int32_t>(min); }

PageFilter* PageFilter::MakeGreaterThan(int64_t min)
{ return new TypedPageFilterGreaterThan<parquet::Int64Type, int64_t>(min); }

PageFilter* PageFilter::MakeGreaterThan(float min)
{ return new TypedPageFilterGreaterThan<parquet::FloatType, float>(min); }

PageFilter* PageFilter::MakeGreaterThan(double min)
{ return new TypedPageFilterGreaterThan<parquet::DoubleType, double>(min); }

PageFilter* PageFilter::MakeGreaterThan(std::string& min)
{ return new TypedPageFilterGreaterThan<parquet::ByteArrayType, std::string>(min); }

template <typename DType, typename VType>
skipType::reasonCode
TypedPageFilterGreaterThan<DType, VType>::canSkip(const std::string& min, const std::string& max)
{ 
   VType p_max = *((VType*)max.data()); // page max

   // When [p_min, p_max] <= [range_min, inf), it is REJECT
   if ( p_max <= range_min_ )
     return skipType::REJECT;

   VType p_min = *((VType*)min.data()); // page min
   
   // When range_min < [p_min, p_max], it is IN_RANGE
   return ( range_min_ < p_min ) ?  
            skipType::IN_RANGE : skipType::OVERLAP;
}

template <>
skipType::reasonCode
TypedPageFilterGreaterThan<parquet::Int32Type, int32_t>::canSkip(const std::string& min, const std::string& max)
{ 
   // need to be careful here since the data length in min/max depends
   // on the actual data type coded. For example, for BOOL, the length
   // is 1. We can not assume the length is always sizeof(int32_t).
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

   // When [p_min, p_max] <= [range_min, inf), it is REJECT
   if ( p_max <= range_min_ )
     return skipType::REJECT;

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
   
   // When range_min < [p_min, p_max], it is IN_RANGE
   return ( range_min_ < p_min ) ?  
            skipType::IN_RANGE : skipType::OVERLAP;
}

template <>
skipType::reasonCode
TypedPageFilterGreaterThan<parquet::ByteArrayType, std::string>::canSkip(const std::string& min, const std::string& max)
{ 
   // Compute max <= range_min
   
   int rangeMinLen = range_min_.length();
   int maxLen = max.length();
   skipType::reasonCode ok;

   if ( rangeMinLen == maxLen )
     ok = ( max.compare(range_min_) <= 0 ) ? skipType::REJECT : skipType::OVERLAP;
   else if ( maxLen < rangeMinLen ) {

     // can not change max, have to make a copy first
     std::string max_padded(rangeMinLen, ' ');
     max_padded.replace(0, maxLen, max);

     ok = ( max_padded.compare(range_min_) <= 0 ) ? skipType::REJECT : skipType::OVERLAP;

   } else {
     range_min_.resize(maxLen, ' ');
     ok = ( max.compare(range_min_) <= 0 ) ? skipType::REJECT : skipType::OVERLAP;
     range_min_.resize(rangeMinLen);
   }
   return ok;
}

//====================================================================
template <typename DType, typename VType>
class TypedPageFilterGreaterThanEqual : public TypedPageFilter<DType, VType> {

public:
  TypedPageFilterGreaterThanEqual(VType min) 
    { range_min_ = min; };

  ~TypedPageFilterGreaterThanEqual() {};

  skipType::reasonCode canSkip(const std::string& min, const std::string& max);

  void display()
    {  std::cout << "PageFilterGreaterThanEqual: min=" << range_min_; }

protected:
  // the boundary values of the high half range. 
  VType range_min_;
};

PageFilter* PageFilter::MakeGreaterThanEqual(int32_t min)
{ return new TypedPageFilterGreaterThanEqual<parquet::Int32Type, int32_t>(min); }

PageFilter* PageFilter::MakeGreaterThanEqual(int64_t min)
{ return new TypedPageFilterGreaterThanEqual<parquet::Int64Type, int64_t>(min); }

PageFilter* PageFilter::MakeGreaterThanEqual(float min)
{ return new TypedPageFilterGreaterThanEqual<parquet::FloatType, float>(min); }

PageFilter* PageFilter::MakeGreaterThanEqual(double min)
{ return new TypedPageFilterGreaterThanEqual<parquet::DoubleType, double>(min); }

PageFilter* PageFilter::MakeGreaterThanEqual(std::string& min)
{ return new TypedPageFilterGreaterThanEqual<parquet::ByteArrayType, std::string>(min); }


template <typename DType, typename VType>
skipType::reasonCode
TypedPageFilterGreaterThanEqual<DType, VType>::canSkip(const std::string& min, const std::string& max)
{ 
   VType p_max = *((VType*)max.data()); 

   // When [p_min, p_max] < [range_min, inf), it is REJECT
   if ( p_max < range_min_ )
     return skipType::REJECT;
   
   VType p_min = *((VType*)min.data()); 
   // When range_min <= [p_min, p_max], it is IN_RANGE
   return ( range_min_ <= p_min ) ?  
            skipType::IN_RANGE : skipType::OVERLAP;
}

template <>
skipType::reasonCode
TypedPageFilterGreaterThanEqual<parquet::Int32Type, int32_t>::canSkip(const std::string& min, const std::string& max)
{ 
   // need to be careful here since the data length in min/max depends
   // on the actual data type coded. For example, for BOOL, the length
   // is 1. We can not assume the length is always sizeof(int32_t).
   int32_t p_max = 0;

   switch (min.size())
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

   // When [p_min, p_max] < [range_min, inf), it is REJECT
   if ( p_max < range_min_ )
     return skipType::REJECT;
   
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
   // When range_min <= [p_min, p_max], it is IN_RANGE
   return ( range_min_ <= p_min ) ?  
            skipType::IN_RANGE : skipType::OVERLAP;
}

template <>
skipType::reasonCode
TypedPageFilterGreaterThanEqual<parquet::ByteArrayType, std::string>::canSkip(const std::string& min, const std::string& max)
{ 
   // To compute max < range_min
   
   int rangeMinLen = range_min_.length();
   int maxLen = max.length();
   skipType::reasonCode ok ;

   if ( rangeMinLen == maxLen )
     ok = ( max.compare(range_min_) < 0 ) ? skipType::REJECT : skipType::OVERLAP;
   else if ( maxLen < rangeMinLen ) {

     // can not change max, have to make a copy
     std::string max_padded(rangeMinLen, ' ');
     max_padded.replace(0, maxLen, max);

     ok = ( max_padded.compare(range_min_) < 0 ) ? skipType::REJECT : skipType::OVERLAP;

   } else {
     range_min_.resize(maxLen, ' ');
     ok = ( max.compare(range_min_) < 0 ) ? skipType::REJECT : skipType::OVERLAP;
     range_min_.resize(rangeMinLen);
   }
   return ok;
}


