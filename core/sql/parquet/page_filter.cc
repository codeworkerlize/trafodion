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
 * File:         page_filter.cc
 *
 * Description:  The base page filter class that is used to filter
 *               out Parquet page. The class contains static 
 *               methods to construct a page filer object of a
 *               specific flavor, and the canSkip() method to 
 *               test the possibility to skip a page (if return 
 *               TRUE).
 *
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include "parquet/page_filter.h"
#include "parquet/types.h"
#include <iostream>
#include <assert.h>

#include <sys/types.h>
#include <unistd.h>

int64_t Int96ToInt64Timestamp(parquet::Int96 x)
{
 // Interpret Int96 as a Julian timestamp in micro-seconds. 
 //
 // INT96 value x stores a Julian timestamp in nano seconds as follows. 
 //  x.value[0,1]: fractional part (in nano seconds);
 //  x.value[2]: # of days since Julian epoch.
 // For details, please refer to impala_timestamp_to_nanoseconds().
 
  int64_t julianTimestamp = 
        (*(reinterpret_cast<const int64_t*>(&(x.value)))) / 1000;

  // 86400000LL is ms in a day
  julianTimestamp += x.value[2] * 86400000LL * 1000;

  return julianTimestamp;
}

//====================================================================
// Range page filter
template <typename DType, typename VType>
class TypedPageFilterRange : public TypedPageFilter<DType, VType> {

public:
  TypedPageFilterRange(VType min, VType max) 
    { min_ = min; max_ = max; };

  ~TypedPageFilterRange() {};

  skipType::reasonCode canSkip(const std::string& min, const std::string& max);

  void display() 
   { std::cout << "PageRangeFilter: min=" << min_ << ", max=" << max_; }

protected:
  // the boundary values of a range. If a page's range [p_min, p_max]
  // does not overlap  with [range_min_, range_max_], then the page 
  // can be skipped.
  VType min_;
  VType max_;
};

//////////////////////////////////////////////////
// MakeRange(): The general version
//////////////////////////////////////////////////
PageFilter* PageFilter::MakeRange(int32_t min, int32_t max)
{ return new TypedPageFilterRange<parquet::Int32Type, int32_t>(min, max); }

PageFilter* PageFilter::MakeRange(int64_t min, int64_t max)
{ return new TypedPageFilterRange<parquet::Int64Type, int64_t>(min, max); }

PageFilter* PageFilter::MakeRange(float min, float max)
{ return new TypedPageFilterRange<parquet::FloatType, float>(min, max); }

PageFilter* PageFilter::MakeRange(double min, double max)
{ return new TypedPageFilterRange<parquet::DoubleType, double>(min, max); }

PageFilter* PageFilter::MakeRange(std::string& min, std::string& max)
{ return new TypedPageFilterRange<parquet::ByteArrayType, std::string>(min, max); }


#define RETURN_SKIP_REASON_CODE \
   { \
     if ( p_max < min_ || max_ < p_min ) \
       return skipType::REJECT; \
     return ( min_ <= p_min && p_max <= max_ ) ?   \
               skipType::IN_RANGE : skipType::OVERLAP; \
   }
//////////////////////////////////////////////////
// canSkip(): The general version
//////////////////////////////////////////////////
template <typename DType, typename VType>
skipType::reasonCode 
TypedPageFilterRange<DType, VType>::canSkip(const std::string& min, const std::string& max)
{ 
   VType p_min = *((VType*)min.data()); 
   VType p_max = *((VType*)max.data()); 

   RETURN_SKIP_REASON_CODE;
}

template <>
skipType::reasonCode 
TypedPageFilterRange<parquet::Int32Type, int32_t>::canSkip(const std::string& min, const std::string& max)
{ 
   // need to be careful here since the data length in min/max depends
   // on the actual data type coded. For example, for BOOL, the length
   // is 1. We can not assume the length is always sizeof(int32_t).
   int32_t p_min = 0;
   int32_t p_max = 0;

   switch (min.size())
   { 
     case 1:
        p_min = *(int8_t*)min.data();
        p_max = *(int8_t*)max.data();
        break;
     case 2:
        p_min = *(int16_t*)min.data();
        p_max = *(int16_t*)max.data();
        break;
     case 4:
        p_min = *(int32_t*)min.data();
        p_max = *(int32_t*)max.data();
        break;

     default:
        assert(false);
   }

   RETURN_SKIP_REASON_CODE;
}

// Not support IN_RANGE yet. 
template <>
skipType::reasonCode 
TypedPageFilterRange<parquet::ByteArrayType, std::string>::canSkip(const std::string& min, const std::string& max)
{ 
   // Space padded version. SQL semantics.
   // Range min and range max are from the pushed down predicates.
   // Argument min and argument max are from the page stats.
   int32_t maxLen = max.length();
   int32_t rangeMinLen = min_.length();

   if ( maxLen == rangeMinLen ) {

     if ( max.compare(min_) < 0 )
       return skipType::REJECT;
   } 
   else if ( maxLen < rangeMinLen ) {

     // Make a space-padded version
     std::string max_padded(rangeMinLen, ' ');
     max_padded.replace(0, maxLen, max);

     if ( max_padded.compare(min_) < 0 )
       return skipType::REJECT;

   } else {
      // lengthen the range_min
      min_.resize(maxLen, ' ');

      bool ok = ( max.compare(min_) < 0 );

      // restore range_min back to the original length.
      // This should not cause any memory re-allocation.
      min_.resize(rangeMinLen, ' ');

      if ( ok )
       return skipType::REJECT;
   }

   // need to check the other possibility: 
   // (*range_max_.str_).compare(min) < 0 )
   int32_t rangeMaxLen = max_.length();
   int32_t minLen = min.length();

   if ( rangeMaxLen == minLen ) {

     return ( max_.compare(min) < 0 ) ? 
              skipType::REJECT : skipType::OVERLAP;

   } 
   else if ( minLen < rangeMaxLen ) {

     // space padding 
     std::string min_padded(rangeMaxLen, ' ');
     min_padded.replace(0, minLen, min);

     return ( max_.compare(min_padded) < 0 ) ? 
              skipType::REJECT : skipType::OVERLAP;

   } else {
      // lengthen the range_max
      max_.resize(minLen, ' ');

      bool ok = (max_.compare(min) < 0 );

      // Restore range_max back to the original length.
      // This should not cause any memory re-allocation.
      max_.resize(rangeMaxLen, ' ');

      if ( ok )
        return skipType::REJECT;
   }

   return skipType::OVERLAP;
}

/////////////////////////////////////////////////////////
// displayValue(): The general version
/////////////////////////////////////////////////////////
template <typename DType, typename VType>
void TypedPageFilter<DType, VType>::displayValue(const char* msg, const std::string& val)
{ 
   std::cout << (msg ? msg : "")  << val.c_str() << std::endl; 
}

template <>
void TypedPageFilter<parquet::Int32Type, int32_t>::displayValue(const char* msg, const std::string& val)
{ 
   int32_t p_val = 0;

   switch (val.size())
   { 
     case 1:
        p_val = *(int8_t*)val.data();
        break;
     case 2:
        p_val = *(int16_t*)val.data();
        break;
     case 4:
        p_val = *(int32_t*)val.data();
        break;

     default:
        assert(false);
   }

   std::cout << (msg ? msg : "") << p_val;
}

template <>
void TypedPageFilter<parquet::FloatType, float>::displayValue(const char* msg, const std::string& val)
{ 
   float p_val = *(float*)val.data();

   std::cout << (msg ? msg : "") << p_val;
}

template <>
void TypedPageFilter<parquet::DoubleType, float>::displayValue(const char* msg, const std::string& val)
{ 
  // this method is added to make the following un-defined function
  // go away
  //     U TypedPageFilter<parquet::DataType<(parquet::Type::type)5>, float>::displayValue(char const*, std::string const&)
  // 
  // 0000000000632a9e T TypedPageFilter<parquet::DataType<(parquet::Type::type)4>, float>::displayValue(char const*, std::string const&)
  // 0000000000632b06 T TypedPageFilter<parquet::DataType<(parquet::Type::type)5>, double>::displayValue(char const*, std::string const&)
  assert(0);
}

template <>
void TypedPageFilter<parquet::DoubleType, double>::displayValue(const char* msg, const std::string& val)
{ 
   double p_val = *(double*)val.data();

   std::cout << (msg ? msg : "") << p_val;
}

template <>
void TypedPageFilter<parquet::Int64Type, int64_t>::displayValue(const char* msg, const std::string& val)
{ 
   int64_t p_val = *(int64_t*)val.data();

   std::cout << (msg ? msg : "") << p_val;
}

template <>
void TypedPageFilter<parquet::ByteArrayType, std::string>::displayValue(const char* msg, const std::string& val)
{ 
   std::cout << (msg ? msg : "") << val.c_str();
}

/////////////////////////////////////////////////////////
// Other member functions
/////////////////////////////////////////////////////////
int64_t PageFilter::rowsSelected()
{
   int64_t ct = 0;
   for (int i=0; i<skipVec_.size(); i++ ) {
     ct += skipVec_[i].len;
   }
   return ct;
}

void PageFilter::resetCounter() 
{ 
   ParquetFilter::resetCounter(); 

   skipVec_.clear(); 

   delete mergedSkipVec_;
   mergedSkipVec_ = NULL;
}
 
void addToKeep(skipVecType& vec, int64_t x, skipType::reasonCode c)
{
   if ( vec.size() > 0 ) 
   {
     skipType& last = vec.back();

     if ( last.len > 0 && last.code == c ) {
       last.len += x;
       return;
     }
   }
   vec.push_back(skipType(x, c));
}

void addToSkip(skipVecType& vec, int64_t x)
{
   if ( vec.size() > 0 ) 
   {
     skipType& last = vec.back();

      if ( last.len < 0 ) {
        last.len -= x;
        return;
      }
   }
   vec.push_back(skipType(-x, skipType::REJECT));
}

// merge two skip vector source1 and source2 into result.
// Each vector represets the result of filters applied to 
// a sequence of values from a column in t table.
//
// Each element in the vector has the following possible state.
//
// len_:
//   negative: |len_| values have been filtered out;
//   positive: |len_| values are retained;
//  
// code_ (only applicable when len_ positive):
//    IN_RANGE: all len_ values pass the range page filter predicate
//    OVERLAP:  otherwise
//
// For source1 and source2, assume 
// Sum(abs(source1[i])) == Sum(abs(source2[j]))
//
// The merge algorithm below implements the idea of merging two 
// skip vectors. 
//
//   1). Traverse from left to right on each of the two vectors
//   2). For the current element e1 and e2
//   3). If one of them is negative, output -min(|e1|, |e2|) and
//       advance the current pointers to vectors accordingly;
//   4). If both of them are positive:
//         if both are IN_RANGE, output ([min(|e1|, |e2|), IN_RANGE) 
//         if both are OVERLAP, output ([min(|e1|, |e2|), OVERLAP) 
//         else, output ([min(|e1|, |e2|), OVERLAP)  and
//       advance the current pointers to vectors accordingly;
//
// Examples:
//
// Assume all positive elments are OVERLAP
//  source1 = [4, -4], source2 = [2, -6], result = [2, -2, -4] = [2, -6]
//  source1 = [4, -4], source2 = [8], result = [4, -4]
//  source1 = [4, -4], source2 = [-8], result = [-4, -4] = [-8]
//  source1 = [4, -4], source2 = [-5, 3], result = [-4, -4] = [-8]
//  source1 = [4, -4], source2 = [5, -3], result = [4, -1, -3] = [4, -4]
//
// Assume some positive elments are OVERLAP and some are IN_RANGE, 
//  denoted as <n><c>, such as 4I (len=4, IN_RANGE), 5O (len=5, OVERLAP)
//
//  source1 = [4O, -4], source2 = [2I, -6], result = [2O, -2, -4] = [2O, -6]
//  source1 = [4I, -4], source2 = [8I], result = [4I, -4]
//  source1 = [4I, -4], source2 = [-8], result = [-4, -4] = [-8]
//  source1 = [4O, -4], source2 = [-5, 3O], result = [-4, -4] = [-8]
//  source1 = [4I, -4], source2 = [5I, -3], result = [4I, -1, -3] = [4I, -4]
//
skipType::reasonCode skipType::computeReasonCode(const skipType& e1, const skipType& e2)
{
   switch ( e1.code )  
   {
     case REJECT:
       return REJECT;

     case IN_RANGE:
       return e2.code;

     case OVERLAP:
       if ( e2.code == REJECT)
         return REJECT;
       else
         return OVERLAP;
   }
   return REJECT;
}

skipVecType* 
mergeSkipVec(const skipVecType* source1, const skipVecType* source2) 
{
   if ( !source1 || !source2 )
     return NULL;

   if ( source1->size() == 0 )
     return (source2) ? new skipVecType(*source2) : NULL;

   if ( source2->size() == 0 )
     return (source1) ? new skipVecType(*source1) : NULL;

   skipVecType* result = new skipVecType();

   int p = 0; // index into source1
   int q = 0; // index into source2

   skipType m = (*source1)[p];
   skipType n = (*source2)[q];

   int64_t abs_m = 0;
   int64_t abs_n = 0;

   int64_t x = 0;

   bool done = false;

   while ( !done ) {

      abs_m = abs(m.len);
      abs_n = abs(n.len);

      int64_t value = (abs_m > abs_n) ? abs_n : abs_m;
      int sign = ( m.len < 0 || n.len < 0 ) ? -1 : 1;

      // store the MIN of the two quantities and keep the
      // sign of the larger, with the right reason code.
      if ( sign > 0 ) {
         addToKeep(*result, value, skipType::computeReasonCode(m, n));
      } else
         addToSkip(*result, value);

      //  advance pointers
      if ( abs_m < abs_n ) {

        // adjust n only to refect abs_m elements have 
        // been taken from it
        x = abs_n - abs_m;
        n.len = ( n.len > 0 ) ? x : -x;

        // advance p and update m
        p++;
        m = (*source1)[p];
      } else 
      if ( abs_m > abs_n ) {

        // adjust m only to refect abs_n elements have 
        // been taken from it
        x = abs_m - abs_n;
        m.len = ( m.len > 0 ) ? x : -x;

        // advance q and update n
        q++;
        n = (*source2)[q];

      } else {

        // advance both p and q and check termination
        // condition
        p++;

        if ( p == source1->size() ) 
          done = true;
        else
          m = (*source1)[p];

        q++;

        if ( q == source2->size() ) 
          done = true;
        else
          m = (*source2)[q];
      }
   }

   return result;
}

skipVecType* PageFilter::getMergedSkipVec() 
{ 
   if ( mergedSkipVec_ )
     return mergedSkipVec_;

   mergedSkipVec_ = new skipVecType(*getSkipVec());

   PageFilter* pf = (PageFilter*)(this->getNext());

   skipVecType* newlyMerged = NULL;
   
   while (pf) {
      newlyMerged = mergeSkipVec(mergedSkipVec_, pf->getSkipVec());

      delete mergedSkipVec_;
      mergedSkipVec_ = newlyMerged;

      pf = (PageFilter*)(pf->getNext());
   }

   return mergedSkipVec_;
}
 
//#define DEBUG_PAGE_FILTER_EVAL 1

//
// Evaluate all page filters linked from this page filter, and 
// update each filter's skip vec.
//
// return
//     true: if the page can be skipped due to some filter does not
//           overlap with the page at all;
//     false: otherwise
bool PageFilter::evalAndUpdateSkipVec(const parquet::DataPage* page)
{
   PageFilter* pf = this;

   const parquet::EncodedStatistics& stats = page->statistics();

#ifdef DEBUG_PAGE_FILTER_EVAL
   std::cout << "PageFilter to eval a page: " << std::endl;
   std::cout << "DataPage: # of values=" << page->num_values() << ", ";
   displayValue("stats.min=", stats.min());
   displayValue(", stats.max=", stats.max());
   std::cout << ", ";
#endif

   bool canSkip = false;
   skipType::reasonCode code;

   while ( pf ) {

#ifdef DEBUG_PAGE_FILTER_EVAL
     pf->display();
     std::cout << ", ";
#endif

     if ( (code=pf->canSkip(stats.min(), stats.max())) == skipType::REJECT ) {

        pf->addNumToSkip(page->num_values());

        // treat as conjunct predicates (AND predicates) for now.
        // Need to evaluate all page filters to setup each's skip vec.
        canSkip = true; 

     } else {

        pf->addNumToKeep(page->num_values(), code);

     }

     pf = (PageFilter*)(pf->getNext());
   }
        
#ifdef DEBUG_PAGE_FILTER_EVAL
   if ( canSkip ) 
     std::cout << "--> to skip.";
   else
     std::cout << "--> to keep."; 
  
   std::cout << std::endl << std::endl;
#endif

   return canSkip;
}

//#define DEBUG_PAGE_FILTER_ACCEPT_ROWGROUP 1

// Return 
//    false: if some of the page filters rejects the row group with
//           range stats [min, max];
//    true:  otherwise
bool PageFilter::canAcceptRowGroup(const std::string& min, const std::string& max)
{
#ifdef DEBUG_PAGE_FILTER_ACCEPT_ROWGROUP 
   std::cout << "PageFilter to eval a row group: " << std::endl;
   displayValue("stats.min=", min);
   displayValue(", stats.max=", max);
   std::cout << ", ";
#endif
   PageFilter* pf = this;
   bool canAccept = true;

   while ( pf ) {

#ifdef DEBUG_PAGE_FILTER_ACCEPT_ROWGROUP 
     pf->display();
     std::cout << ", ";
#endif

     if ( pf->canSkip(min, max) == skipType::REJECT ) {
        canAccept = false;
        break;
     } 

     pf = (PageFilter*)(pf->getNext());
   }
        
#ifdef DEBUG_PAGE_FILTER_ACCEPT_ROWGROUP 
   if ( canAccept )
     std::cout << " -->  to keep";
   else
     std::cout << " -->  to skip";

   std::cout << std::endl << std::endl;
#endif
   return canAccept;
}

std::ostream& operator<<(std::ostream& output, const skipType& s)
{
   output << s.len;
   switch (s.code) 
   {
     case skipType::REJECT: 
       output << "R";
       break;
     case skipType::IN_RANGE: 
       output << "I";
       break;
     case skipType::OVERLAP: 
       output << "O";
       break;
     default:
       break;
   }
   return output;
}

bool operator==(const skipType& s1, const skipType& s2)
{
   return (s1.len == s2.len && s1.code == s2.code);
}
