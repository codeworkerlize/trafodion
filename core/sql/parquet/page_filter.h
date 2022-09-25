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
 * File:         page_filter.h
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
#ifndef PARQUET_PAGE_FILTER_H
#define PARQUET_PAGE_FILTER_H

#include <string>
#include <vector>
#include "parquet/types.h"
#include "parquet/column_page.h"
#include "parquet/filter.h"

// Page filter is an Esgyn Extension to the parquet reader
// that allows pages to be skipped from further processing. The
// filtering compares the min/max stats of a page againast the 
// range of the desirable values the query is looking for.
//
// The PageFilter class contains the desirable range in several 
// forms and its method canSkip() compares the range against 
// the min/max of a particular page. If true is returned from the
// method, the page will be skipped.

int64_t Int96ToInt64Timestamp(parquet::Int96 x);


struct skipType {
public:

  //
  // Assume page is of boundary Rp=[x, y]  and a page filter 
  // predicate is of boundary Rf=[u, v]
  //
  // REJECT: Rp and Rf do not overlap. The page can be skipped.
  //
  // IN_RANGE: Rp is completely in range of Rf. The page does not
  //           have to be evaluated by the same value filter as
  //           the page filter.
  //
  // OVERLAP : Rp and Rf overlap. The page has to be evaluated by
  //           the same value filter as the page filter.
  //
  enum reasonCode { REJECT, IN_RANGE, OVERLAP };

  int64_t len;
  enum reasonCode code;

  skipType(int64_t x, reasonCode c) : len(x), code(c) {};

public:
  static reasonCode 
    computeReasonCode(const skipType& e1, const skipType& e2);

};

typedef std::vector<skipType> skipVecType;

void addToKeep(skipVecType&, int64_t, skipType::reasonCode);
void addToSkip(skipVecType&, int64_t);

skipVecType* 
mergeSkipVec(const skipVecType* source1, const skipVecType* source2);

std::ostream& operator<<(std::ostream& output, const skipType& s);
bool operator==(const skipType& s1, const skipType& s2);

static inline
std::ostream& operator<<(std::ostream& output, const skipVecType& values)
{
    for (auto const& value : values)
    {
        output << value << " ";
    }
    return output;
}

template <typename T>
bool operator==(std::vector<T> const& vec1, std::vector<T> const& vec2)
{
    if ( vec1.size() != vec2.size() )
      return false;

    for (int i=0; i<vec1.size(); i++ )
    {
      if ( !(vec1[i] == vec2[i]) )
        return false;
    }
    return true;
}


class PageFilter : public ParquetFilter {

public:
 PageFilter() : mergedSkipVec_(NULL) { skipVec_.clear(); };
 ~PageFilter() { delete mergedSkipVec_; };

 void resetCounter();

 // eval as a single PageFilter
 virtual skipType::reasonCode 
      canSkip(const std::string& p_min, const std::string& p_max)
      { return skipType::OVERLAP; }

 //
 // To answer the question if the page filters accept the range.
 //
 // return true: if none of the page filters can reject the range [min, max]
 //        false: otherwise
 virtual bool canAcceptRowGroup(const std::string& min, const std::string& max);

 //
 // eval all page filters linked from this and update each page filter's
 // skip vec
 //
 virtual bool evalAndUpdateSkipVec(const parquet::DataPage* page);

 static PageFilter* MakeRange(int32_t min, int32_t max);
 static PageFilter* MakeRange(int64_t min, int64_t max);
 static PageFilter* MakeRange(float min, float max);
 static PageFilter* MakeRange(double min, double max);
 static PageFilter* MakeRange(std::string& min, std::string& max);

 static PageFilter* MakeLessThan(int32_t max);
 static PageFilter* MakeLessThan(int64_t max);
 static PageFilter* MakeLessThan(float max);
 static PageFilter* MakeLessThan(double max);
 static PageFilter* MakeLessThan(std::string& max);

 static PageFilter* MakeLessThanEqual(int32_t max);
 static PageFilter* MakeLessThanEqual(int64_t max);
 static PageFilter* MakeLessThanEqual(float max);
 static PageFilter* MakeLessThanEqual(double max);
 static PageFilter* MakeLessThanEqual(std::string& max);

 static PageFilter* MakeGreaterThan(int32_t min);
 static PageFilter* MakeGreaterThan(int64_t min);
 static PageFilter* MakeGreaterThan(float min);
 static PageFilter* MakeGreaterThan(double min);
 static PageFilter* MakeGreaterThan(std::string& min);

 static PageFilter* MakeGreaterThanEqual(int32_t min);
 static PageFilter* MakeGreaterThanEqual(int64_t min);
 static PageFilter* MakeGreaterThanEqual(float min);
 static PageFilter* MakeGreaterThanEqual(double min);
 static PageFilter* MakeGreaterThanEqual(std::string& min);

 int64_t rowsSelected();
 skipVecType* getSkipVec() { return &skipVec_; }

 void addNumToKeep(int64_t x, skipType::reasonCode r) 
      { addToKeep(skipVec_, x, r); }

 void addNumToSkip(int64_t x) { addToSkip(skipVec_, x); }

 skipVecType* getMergedSkipVec();

protected:

  // The skip vector records spans of column values that are either 
  // to keep or to skip.  A positive value indicates the # of values 
  // to keep, and a negative value indicates teh # of values to skip. 
  //
  // For example, for the vector { 10, -30, 20 }
  //  first 10 values are to keep,
  //  the next 30 values are to skip,
  //  the next 20 values are to keep,
  skipVecType skipVec_;

  skipVecType* mergedSkipVec_;

};

// typed page filter
template <typename DType, typename VType>
class TypedPageFilter : public PageFilter {

public:
  TypedPageFilter() {} 
  ~TypedPageFilter() {};

  void displayValue(const char* msg, const std::string& val);

protected:
};

void testPageFilters();

#endif
