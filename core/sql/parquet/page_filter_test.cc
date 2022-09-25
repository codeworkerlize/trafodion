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
 * File:         page_filter_test.cc
 *
 * Description:  All page filter related test code.
 *
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include <iostream>
#include <assert.h>
#include "page_filter.h"


using std::cout;
using std::endl;

// Evaluate a page filter against a page boundary 
// [min, max] and compare the result with 'expected'.
//
// Return:
//    0: if the test result is the same as the expected.
//    1: otherwise
//       Also output to cout the filter, the min and the max.
template <class T>
int passIf(T min, T max, PageFilter* filter, skipType::reasonCode expected)
{
  std::string minStr((char*)&min, sizeof(T));
  std::string maxStr((char*)&max, sizeof(T));

  skipType::reasonCode result = filter->canSkip(minStr, maxStr);
  int failed = 0;

  if ( result != expected ) {
     filter->display(); 
     cout << ", canSkip(" << min << ", " << max << ")=" << result ;
     cout << ", FAIL";
     failed = 1;
     cout << endl;
  }
  return failed;
}

// The string version of the above
template <>
int passIf(std::string min, std::string max, PageFilter* filter, skipType::reasonCode expected)
{
  skipType::reasonCode result = filter->canSkip(min, max);
  int failed = 0;

  if ( result != expected ) {
     filter->display(); 
     cout << ", canSkip(" << min << ", " << max << ")=" << result ;
     cout << ", FAIL";
     failed = 1;
     cout << endl;
  }
  return failed;
}

// The expected result is a vector of skipType which is composed of
// an integer number and a reason code. The reason code is REJECTED, 
// IN_RANGE or OVERLAP. The following three macros help define 
// these codes through abbreviations.
//
#define O skipType::OVERLAP
#define R skipType::REJECT
#define I skipType::IN_RANGE 

//////////////////////////////////////////////
// Range
//
// A page range filter is boundary inclusive.
//
// Subject a range page filter to a sequence 
// of page boundary values.
//
// Definition of the arguments:
//
// 1. [low, high] defines the range page filter.
// 2. inputsMin defines the lower bounds of the 
//    sequence and inputsMax defines the upper 
//    bounds.
// 3. The expected results are in results.
//
//////////////////////////////////////////////
template <class T>
void testPageFilterRange(T low, T high, 
                          std::vector<T>& inputsMin, 
                          std::vector<T>& inputsMax, 
                          std::vector<skipType::reasonCode>& results, int& failed)
{
  assert(inputsMin.size() == inputsMax.size());
  assert(inputsMin.size() == results.size());

  PageFilter* filter = PageFilter::MakeRange(low, high);

  for (int i=0; i<inputsMin.size(); i++) {
     failed += passIf(inputsMin[i], inputsMax[i], filter, results[i]);
  }

  delete filter;
}

void testPageFilterRange(int& failed)
{
  std::string low("abc ");
  std::string high("bcd ");

  // string 
  std::vector<std::string> inputsMin = {"ab", "c   ", "abc"};
  std::vector<std::string> inputsMax = {"ab", "e    ", "bce"};
  std::vector<skipType::reasonCode> resultsRRO = {R, R, O};

  // IN_RANGE is not supported yet for string type
  testPageFilterRange(low, high, inputsMin, inputsMax, resultsRRO, failed);


  std::vector<skipType::reasonCode> resultsRRI = {R, R, I};
  // int32
  // Specify three different page configuraions:
  // [-2, -1], [14, 100] and [1, 9]
  std::vector<int32_t> minsInt32 = {-2, 14, 1};
  std::vector<int32_t> maxsInt32 = {-1, 100, 9};

  // Subject a page filter [1, 10] to the above 
  // three pages
  testPageFilterRange(int32_t(1), int32_t(10), 
                      minsInt32, maxsInt32, resultsRRI, failed);

  // int64
  // Specify three different page configuraions:
  // [-2, -1], [14, 100] and [1, 9]
  std::vector<int64_t> minsInt64 = {-2, 14,  2};
  std::vector<int64_t> maxsInt64 = {-1, 100, 9};

  // Subject a page filter [1, 10] to the above 
  // three pages
  testPageFilterRange(int64_t(1), int64_t(10), 
                      minsInt64, maxsInt64, resultsRRI, failed);

  // float
  // Specify three different page configuraions:
  // [-2.0, -1.5], [14.4877, 100.989] and [-1.0, 9.599]
  std::vector<float> minsFloat = {-2.0, 14.4877, -1.0};
  std::vector<float> maxsFloat = {-1.5, 100.989, 9.599};

  // Subject a page filter [1.1, 10.99] to the above 
  // three pages
  testPageFilterRange(float(1.1), float(10.99), 
                      minsFloat, maxsFloat, resultsRRO, failed);

  // double
  // Specify three different page configuraions:
  // [-2.0, -1.5], [14.4877, 100.989] and [-1.0, 9.599]
  std::vector<double> minsDouble = {-2.0, 14.4877, -1.0};
  std::vector<double> maxsDouble = {-1.5, 100.989, 9.599};

  // Subject a page filter [1.1, 10.99] to the above 
  // three pages
  testPageFilterRange(double(1.1), double(10.99), 
                      minsDouble, maxsDouble, resultsRRO, failed);
}


////////////////////////////////////
// Test the less than page filter.
////////////////////////////////////
template <class T>
void testPageFilterLessThan(T high, 
                          std::vector<T>& inputsMin, 
                          std::vector<T>& inputsMax, 
                          std::vector<skipType::reasonCode>& results, int& failed)
{
  assert(inputsMin.size() == inputsMax.size());
  assert(inputsMin.size() == results.size());

  PageFilter* filter = PageFilter::MakeLessThan(high);

  for (int i=0; i<inputsMin.size(); i++) {
     failed += passIf(inputsMin[i], inputsMax[i], filter, results[i]);
  }

  delete filter;
}

void testPageFilterLessThan(int& failed)
{
  std::string high("bcd");

  // Specify three different page configuraions:
  std::vector<std::string> inputsMin = {"ab", "c   ", "bcd   "};
  std::vector<std::string> inputsMax = {"ab", "e    ", "bce"};

  // Subject a page filter [-inf, 'bcd'] to the above 3 pages
  std::vector<skipType::reasonCode> resultsForStrings  = {O, R, R};

  testPageFilterLessThan(high, inputsMin, inputsMax, resultsForStrings , failed);

  std::vector<skipType::reasonCode> results = {O, I, R, R};

  // int32_t
  // Specify four different page configuraions:
  // [0, 5], [0, 1], [3, 5]  and [2, 10]
  std::vector<int32_t> minInt32 = {0, 0, 3, 2};
  std::vector<int32_t> maxInt32 = {5, 1, 5, 10};

  // Subject a page filter [-inf, 2] to the above 4 pages
  testPageFilterLessThan((int32_t)2, minInt32, maxInt32, results, failed);

  // int64_t
  // Specify four different page configuraions:
  // [0, 6], [0, 1], [3, 5]  and [2, 10]
  std::vector<int64_t> minInt64 = {0, 0, 3, 2};
  std::vector<int64_t> maxInt64 = {6, 1, 5, 10};

  // Subject a page filter [-inf, 2] to the above 4 pages
  testPageFilterLessThan((int64_t)2, minInt64, maxInt64, results, failed);

  // float
  // Specify four different page configuraions:
  // [0.0, 4.0], [0.1, 1.33], [3.99, 5]  and [2.0, 10]
  std::vector<float> minFloat = {0.0, 0.1, 3.99, 2.0};
  std::vector<float> maxFloat = {4.0, 1.33, 5, 10};

  // Subject a page filter [-inf, 2.0] to the above 4 pages
  testPageFilterLessThan((float)2.0, minFloat, maxFloat, results, failed);

  // double
  // Specify four different page configuraions:
  // [0.0, 9.0], [0.1, 1.33], [3.99, 5]  and [2.0, 10]
  std::vector<double> minDouble = {1.0, 0.1, 3.99, 2.0};
  std::vector<double> maxDouble = {9.0, 1.33, 5, 10};

  // Subject a page filter [-inf, 2.0] to the above 4 pages
  testPageFilterLessThan((double)2.0, minDouble, maxDouble, results, failed);
}


///////////////////////
// test less than equal
///////////////////////
template <class T>
void testPageFilterLessThanEqual(T high, 
                          std::vector<T>& inputsMin, 
                          std::vector<T>& inputsMax, 
                          std::vector<skipType::reasonCode>& results, int& failed)
{
  assert(inputsMin.size() == inputsMax.size());
  assert(inputsMin.size() == results.size());

  PageFilter* filter = PageFilter::MakeLessThanEqual(high);

  for (int i=0; i<inputsMin.size(); i++) {
     failed += passIf(inputsMin[i], inputsMax[i], filter, results[i]);
  }

  delete filter;
}

void testPageFilterLessThanEqual(int& failed)
{
  std::string high("bcd  ");

  // Specify three different page configuraions:
  std::vector<std::string> inputsMin = {"ab", "c   ", "bcd"};
  std::vector<std::string> inputsMax = {"ab", "e    ", "bce"};
  std::vector<skipType::reasonCode> resultsForStrings = {O, R, O};

  // Subject a page filter [-inf, "bcd  "] to the above 3 pages
  testPageFilterLessThanEqual(high, inputsMin, inputsMax, resultsForStrings, failed);

  std::vector<skipType::reasonCode> results = {O, I, R, O};

  // int32_t
  // Specify four different page configuraions:
  // [0, 4], [0, 1], [3, 5]  and [2, 10]
  std::vector<int32_t> minInt32 = {0, 0, 3, 2};
  std::vector<int32_t> maxInt32 = {4, 1, 5, 10};

  // Subject a page filter [-inf, 2] to the above 4 pages
  testPageFilterLessThanEqual((int32_t)2, minInt32, maxInt32, results, failed);

  // int64_t
  // Specify four different page configuraions:
  // [0, 5], [0, 1], [3, 5]  and [2, 10]
  std::vector<int64_t> minInt64 = {0, 0, 3, 2};
  std::vector<int64_t> maxInt64 = {5, 1, 5, 10};

  // Subject a page filter [-inf, 2] to the above 4 pages
  testPageFilterLessThanEqual((int64_t)2, minInt64, maxInt64, results, failed);

  // float
  // Specify four different page configuraions:
  // [0.0, 2.0], [0.1, 1.33], [3.99, 5]  and [2.0, 10]
  std::vector<float> minFloat = {0.0, 0.1, 3.99, 2.0};
  std::vector<float> maxFloat = {2.1, 1.33, 5, 10};

  // Subject a page filter [-inf, 2.0] to the above 4 pages
  testPageFilterLessThanEqual((float)2.0, minFloat, maxFloat, results, failed);

  // double
  // Specify four different page configuraions:
  // [0.0, 2.1], [0.1, 1.33], [3.99, 5]  and [2.0, 10]
  std::vector<double> minDouble = {-0.3, 0.1, 3.99, 2.0};
  std::vector<double> maxDouble = {2.1, 1.33, 5, 10};

  // Subject a page filter [-inf, 2.0] to the above 4 pages
  testPageFilterLessThanEqual((double)2.0, minDouble, maxDouble, results, failed);
}

///////////////////////
// test greater than 
///////////////////////
template <class T>
void testPageFilterGreaterThan(T low, 
                          std::vector<T>& inputsMin, 
                          std::vector<T>& inputsMax, 
                          std::vector<skipType::reasonCode>& results, int& failed)
{
  assert(inputsMin.size() == inputsMax.size());
  assert(inputsMin.size() == results.size());

  PageFilter* filter = PageFilter::MakeGreaterThan(low);

  for (int i=0; i<inputsMin.size(); i++) {
     failed += passIf(inputsMin[i], inputsMax[i], filter, results[i]);
  }

  delete filter;
}

void testPageFilterGreaterThan(int& failed)
{
  std::string low("bcd");

  // string
  // Specify three different page configuraions
  std::vector<std::string> inputsMin = {"ab", "c   ",  "bcd  "};
  std::vector<std::string> inputsMax = {"ab", "e    ", "bce  "};

  std::vector<skipType::reasonCode> stringResults = {R, O, O};

  // Subject a page filter ["bcd", inf] to the above 3 pages
  testPageFilterGreaterThan(low, inputsMin, inputsMax, stringResults, failed);

  std::vector<skipType::reasonCode> results = {R, I, O};

  // int32_t
  // Specify three different page configuraions:
  // [0, 1], [3, 5]  and [2, 10]
  std::vector<int32_t> minInt32 = {0, 3, 2};
  std::vector<int32_t> maxInt32 = {1, 5, 10};

  // Subject a page filter [2, inf] to the above 3 pages
  testPageFilterGreaterThan((int32_t)2, minInt32, maxInt32, results, failed);

  // int64_t
  // Specify three different page configuraions:
  // [0, 1], [3, 5]  and [2, 10]
  std::vector<int64_t> minInt64 = {0, 3, 2};
  std::vector<int64_t> maxInt64 = {1, 5, 10};

  // Subject a page filter [2, inf] to the above 3 pages
  testPageFilterGreaterThan((int64_t)2, minInt64, maxInt64, results, failed);

  // float
  // Specify three different page configuraions:
  // [0.1, 1.33], [3.99, 5]  and [2.0, 10]
  std::vector<float> minFloat = {0.1, 3.99, 2.0};
  std::vector<float> maxFloat = {1.33, 5, 10};

  // Subject a page filter [2.0, inf] to the above 3 pages
  testPageFilterGreaterThan((float)2.0, minFloat, maxFloat, results, failed);

  // double
  // Specify three different page configuraions:
  // [0.1, 1.33], [3.99, 5]  and [2.0, 10]
  std::vector<double> minDouble = {0.1, 3.99, 2.0};
  std::vector<double> maxDouble = {1.33, 5, 10};

  // Subject a page filter [2.0, inf] to the above 3 pages
  testPageFilterGreaterThan((double)2.0, minDouble, maxDouble, results, failed);
}

///////////////////////////
// test greater than equal
///////////////////////////
template <class T>
void testPageFilterGreaterThanEqual(T low, 
                          std::vector<T>& inputsMin, 
                          std::vector<T>& inputsMax, 
                          std::vector<skipType::reasonCode>& results, int& failed)
{
  assert(inputsMin.size() == inputsMax.size());
  assert(inputsMin.size() == results.size());

  PageFilter* filter = PageFilter::MakeGreaterThanEqual(low);

  for (int i=0; i<inputsMin.size(); i++) {
     failed += passIf(inputsMin[i], inputsMax[i], filter, results[i]);
  }

  delete filter;
}

void testPageFilterGreaterThanEqual(int& failed)
{
  std::string low("bcd");

  // string
  // Specify four different page configuraions
  std::vector<std::string> inputsMin = {"ab", "c   ",  "bca", "bcd"};
  std::vector<std::string> inputsMax = {"ab", "e    ", "bcc", "xyz"};

  std::vector<skipType::reasonCode> stringResults = {R, O, R, O};

  // Subject a page filter ["bcd", inf] to the above 4 pages
  testPageFilterGreaterThanEqual(low, inputsMin, inputsMax, stringResults, failed);

  std::vector<skipType::reasonCode> results = {R, I, R, O};

  // int32_t
  // Specify four different page configuraions:
  // [0, 1], [3, 5], [1, 1]  and [1, 10]
  std::vector<int32_t> minInt32 = {0, 3, 1, 1};
  std::vector<int32_t> maxInt32 = {1, 5, 1, 10};

  // Subject a page filter [2, inf] to the above 4 pages
  testPageFilterGreaterThanEqual((int32_t)2, minInt32, maxInt32, results, failed);

  // int64_t
  // Specify four different page configuraions:
  // [0, 1], [3, 5], [1, 1]  and [1, 10]
  std::vector<int64_t> minInt64 = {0, 3, 1, 1};
  std::vector<int64_t> maxInt64 = {1, 5, 1, 10};

  // Subject a page filter [2, inf] to the above 4 pages
  testPageFilterGreaterThanEqual((int64_t)2, minInt64, maxInt64, results, failed);

  // float
  // Specify four different page configuraions:
  // [0.1, 1.33], [3.99, 5], [-1, 1.9] and [0, 10]
  std::vector<float> minFloat = {0.1, 3.99, -1, 0};
  std::vector<float> maxFloat = {1.33, 5, 1.9,  10};

  // Subject a page filter [2.0, inf] to the above 4 pages
  testPageFilterGreaterThanEqual((float)2.0, minFloat, maxFloat, results, failed);

  // double
  // Specify four different page configuraions:
  // [0.1, 1.33], [3.99, 5], [-1, 1.9] and [0, 10]
  std::vector<double> minDouble = {0.1, 3.99, -1, 0};
  std::vector<double> maxDouble = {1.33, 5, 1.9, 10};

  // Subject a page filter [2.0, inf] to the above 4 pages
  testPageFilterGreaterThanEqual((double)2.0, minDouble, maxDouble, results, failed);
}


void testPageFilters()
{
  int failed = 0;

  testPageFilterRange(failed);
  testPageFilterLessThan(failed);
  testPageFilterLessThanEqual(failed);
  testPageFilterGreaterThan(failed);
  testPageFilterGreaterThanEqual(failed);

  cout << "Page filter test:";

  if ( failed == 0 )
     cout << " PASS";
  else 
     cout << " FAILED(" << failed << ")";

  cout  << endl;
}


/////////////////////////////////////////////
// Test the merge of skip vectors s1 and s2.
//
// Return
//    0: success
//    1: fail and output to cout the two
//       vectors, and expected result.
//
// Please refer to function mergeSkipVec()
// for the merging semantics.
/////////////////////////////////////////////
//
int passIf(skipVecType& s1, skipVecType& s2, skipVecType& expected)
{
  skipVecType* r = mergeSkipVec(&s1, &s2);

  int failed = 0;

  if ( !(*r == expected) ) {
     cout << "s1=" << s1 << endl;
     cout << "s2=" << s2 << endl;
     cout << "result=" << *r << endl;
     cout << "expected=" << expected << endl;

     cout << ", FAIL";
     cout << endl;

     failed = 1;
  }

  delete r;
  return failed;
}

int test_skip_vec1()
{
   skipVecType s1 = {{4, O}, {-4, R} };
   skipVecType s2 = {{5, O}, {-3, R} };
   skipVecType expected = {{4, O}, {-4, R}};
   return passIf(s1, s2, expected);
}

int test_skip_vec2()
{
   skipVecType s1 = { {4, O}, {-4, R}};
   skipVecType s2 = { {8, O} };
   skipVecType expected = {{4, O}, {-4, R}};
   return passIf(s1, s2, expected);
}

int test_skip_vec3()
{
   skipVecType s1 = {{4,O}, {-4,R}};
   skipVecType s2 = {{2,O}, {-3,R}, {3,O}};
   skipVecType expected = {{2,O}, {-6,R}};
   return passIf(s1, s2, expected);
}

int test_skip_vec4()
{
   skipVecType s1 = {{-4,R}, {5, O}};
   skipVecType s2 = {{2,O}, {-3,R}, {3,O}, {-1,R}};
   skipVecType expected = {{-5,R}, {3,O}, {-1,R}};
   return passIf(s1, s2, expected);
}

int test_skip_vec5()
{
   skipVecType s1 = {{-4,R}, {5, I}};
   skipVecType s2 = {{2,I}, {-3,R}, {3,I}, {-1,R}};
   skipVecType expected = {{-5,R}, {3,I}, {-1,R}};
   return passIf(s1, s2, expected);
}

int test_skip_vec6()
{
   skipVecType s1 = {{4,I}, {5, O}};
   skipVecType s2 = {{2,I}, {6,O}, {-1,R}};
   skipVecType expected = {{2,I}, {6,O}, {-1,R}};
   return passIf(s1, s2, expected);
}


int testSkipVectors()
{
  int failed = 0;

  failed += test_skip_vec1();
  failed += test_skip_vec2();
  failed += test_skip_vec3();
  failed += test_skip_vec4();
  failed += test_skip_vec5();
  failed += test_skip_vec6();

  cout << "Skip vector merging test:";

  if ( failed == 0 )
     cout << " PASS";
  else 
     cout << " FAILED(" << failed << ")";

  cout << endl;

  return failed;
}

