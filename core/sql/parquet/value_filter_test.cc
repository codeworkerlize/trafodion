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
 * File:         value_filter_test.cc
 *
 * Description:  All value filter testing related code.
 *
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include <iostream>
#include "value_filter.h"
#include <assert.h>

using std::cout;
using std::endl;

///////////////////////////////////////////////////////////
// Test the compare() method for various types of value 
// filters. The test is self contained in that the invoking
// function testValueFilters() will report PASS for FAIL
// status.
//
// Run test utility filter_test witout any argument.
///////////////////////////////////////////////////////////

template <class T>
int passIf(T value, ValueFilter* filter, bool expected)
{
  bool result = filter->compare(value);
  int failed = 0;

  if ( result != expected ) {
     filter->display(); 
     cout << ", compare(" << value << ")=" << result ;
     cout << ", FAIL";
     failed = 1;
     cout << endl;
  }
  return failed;
}

//////////////////////////////////////////////
// Range
//
// A Value range filter is boundary inclusive
//////////////////////////////////////////////
template <class T>
void testValueFilterRange(T low, T high, 
                          std::vector<T>& inputs, 
                          std::vector<bool>& results, int& failed)
{
  assert(inputs.size() == results.size());

  ValueFilter* filter = ValueFilter::MakeRange(low, high);

  for (int i=0; i<inputs.size(); i++) {
     failed += passIf(inputs[i], filter, results[i]);
  }

  delete filter;
}

void testValueFilterRange(int& failed)
{
  // std::string
  std::vector<std::string> inputs = {"abc", "c  ", "a"};
  std::vector<bool> results = {true, false, false};

  std::string low("abc ");
  std::string high("bcd ");
  testValueFilterRange(low, high, inputs, results, failed);

  // int32_t
  std::vector<int32_t> inputsInt32 = {2, -1, 11};

  int32_t lowInt32(1);
  int32_t highInt32(10);
  testValueFilterRange(lowInt32, highInt32, inputsInt32, results, failed);

  // int64_t
  std::vector<int64_t> inputsInt64 = {2, -1, 11};

  int64_t lowInt64(1);
  int64_t highInt64(10);
  testValueFilterRange(lowInt64, highInt64, inputsInt64, results, failed);

  // double
  std::vector<double> inputsDouble = {2, -1, 11};

  double lowDouble(1.33);
  double highDouble(10.93);
  testValueFilterRange(lowDouble, highDouble, inputsDouble, results, failed);

  // parquet Int96
  std::vector<parquet::Int96> inputsInt96 = {{100, 200}, {1, 1}, {300, 2}};

  parquet::Int96 lowInt96(100, 200);
  parquet::Int96 highInt96(100, 1000);
  testValueFilterRange(lowInt96, highInt96, inputsInt96, results, failed);

}

///////////////////
// less than 
///////////////////
template <class T>
void testValueFilterLessThan(T max, std::vector<T> inputs, 
                                    std::vector<bool> results,
                                    int& failed)
{
  assert(inputs.size() == results.size());

  ValueFilter* filter = ValueFilter::MakeLessThan(max);

  for (int i=0; i<inputs.size(); i++) {
     failed += passIf(inputs[i], filter, results[i]);
  }

  delete filter;
}

void testValueFilterLessThan(int& failed)
{
  // std::string
  std::vector<std::string> inputs = {"bcd    ", "c  ", "a"};
  std::vector<bool> results = {false, false, true};

  std::string max("bcd ");
  testValueFilterLessThan(max, inputs, results, failed);

  // int32_t
  std::vector<int32_t> inputsInt32 = {2, 11, -1};

  int32_t highInt32(2);
  testValueFilterLessThan(highInt32, inputsInt32, results, failed);

  // int64_t
  std::vector<int64_t> inputsInt64 = {2, 11, -1};

  int64_t highInt64(2);
  testValueFilterLessThan(highInt64, inputsInt64, results, failed);

  // float 
  std::vector<float> inputsFloat= {2, 1220, -1.0};

  float highFloat(2);
  testValueFilterLessThan(highFloat, inputsFloat, results, failed);

  // parquet Int96
  std::vector<parquet::Int96> inputsInt96 = {{100, 2000}, {1000, 1}, {100, 20}};

  parquet::Int96 highInt96(100, 1000);
  testValueFilterLessThan(highInt96, inputsInt96, results, failed);
}

///////////////////
// less than equal
///////////////////
template <class T>
void testValueFilterLessThanEqual(T max, std::vector<T> inputs, 
                                    std::vector<bool> results,
                                    int& failed)
{
  assert(inputs.size() == results.size());

  ValueFilter* filter = ValueFilter::MakeLessThanEqual(max);

  for (int i=0; i<inputs.size(); i++) {
     failed += passIf(inputs[i], filter, results[i]);
  }

  delete filter;
}

void testValueFilterLessThanEqual(int& failed)
{
  // std::string
  std::vector<std::string> inputs = {"bcd    ", "c  ", "a"};
  std::vector<bool> results = {true, false, true};

  std::string max("bcd ");
  testValueFilterLessThanEqual(max, inputs, results, failed);

  // int32_t
  std::vector<int32_t> inputsInt32 = {2, 11, -1};

  int32_t highInt32(2);
  testValueFilterLessThanEqual(highInt32, inputsInt32, results, failed);

  // int64_t
  std::vector<int64_t> inputsInt64 = {2, 11, -1};

  int64_t highInt64(2);
  testValueFilterLessThanEqual(highInt64, inputsInt64, results, failed);

  // float 
  std::vector<float> inputsFloat= {2.1, 11, -1};

  float highFloat(2.1);
  testValueFilterLessThanEqual(highFloat, inputsFloat, results, failed);

  // parquet Int96
  std::vector<parquet::Int96> inputsInt96 = {{100, 1000}, {1000, 1}, {100, 20}};

  parquet::Int96 highInt96(100, 1000);
  testValueFilterLessThanEqual(highInt96, inputsInt96, results, failed);
}

///////////////////
// greater than 
///////////////////
template <class T>
void testValueFilterGreaterThan(T value, std::vector<T> inputs, 
                                   std::vector<bool> results,
                                   int& failed)
{
  assert(inputs.size() == results.size());

  ValueFilter* filter = ValueFilter::MakeGreaterThan(value);

  for (int i=0; i<inputs.size(); i++) {
     failed += passIf(inputs[i], filter, results[i]);
  }

  delete filter;
}

void testValueFilterGreaterThan(int& failed)
{
  // std::string
  std::vector<std::string> inputs = {"bcd    ", "c  ", "a"};
  std::vector<bool> results = {false, true, false};

  std::string low("bcd ");
  testValueFilterGreaterThan(low, inputs, results, failed);

  // int32_t
  std::vector<int32_t> inputsInt32 = {2, 11, -1};

  int32_t lowInt32(2);
  testValueFilterGreaterThan(lowInt32, inputsInt32, results, failed);

  // int64_t
  std::vector<int64_t> inputsInt64 = {2, 11, -1};

  int64_t lowInt64(2);
  testValueFilterGreaterThan(lowInt64, inputsInt64, results, failed);

  // float 
  std::vector<float> inputsFloat= {2.1, 11, -1};

  float lowFloat(2.1);
  testValueFilterGreaterThan(lowFloat, inputsFloat, results, failed);

  // parquet Int96
  std::vector<parquet::Int96> inputsInt96 = {{100, 1000}, {1000, 1}, {100, 20}};

  parquet::Int96 lowInt96(100, 1000);
  testValueFilterGreaterThan(lowInt96, inputsInt96, results, failed);
}

//  Greater Than Equal
template <class T>
void testValueFilterGreaterThanEqual(T value, std::vector<T> inputs, 
                                   std::vector<bool> results,
                                   int& failed)
{
  assert(inputs.size() == results.size());

  ValueFilter* filter = ValueFilter::MakeGreaterThanEqual(value);

  for (int i=0; i<inputs.size(); i++) {
     failed += passIf(inputs[i], filter, results[i]);
  }

  delete filter;
}

void testValueFilterGreaterThanEqual(int& failed)
{
  // std::string
  std::vector<std::string> inputs = {"bcd    ", "c  ", "a"};
  std::vector<bool> results = {true, true, false};

  std::string low("bcd ");
  testValueFilterGreaterThanEqual(low, inputs, results, failed);

  // int32_t
  std::vector<int32_t> inputsInt32 = {2, 11, -1};

  int32_t lowInt32(2);
  testValueFilterGreaterThanEqual(lowInt32, inputsInt32, results, failed);

  // int64_t
  std::vector<int64_t> inputsInt64 = {2, 11, -1};

  int64_t lowInt64(2);
  testValueFilterGreaterThanEqual(lowInt64, inputsInt64, results, failed);

  // float 
  std::vector<float> inputsFloat= {2.1, 11, -1};

  float lowFloat(2.1);
  testValueFilterGreaterThanEqual(lowFloat, inputsFloat, results, failed);

  // parquet Int96
  std::vector<parquet::Int96> inputsInt96 = {{100, 1000}, {1000, 1}, {10, 20}};

  parquet::Int96 lowInt96(100, 1000);
  testValueFilterGreaterThanEqual(lowInt96, inputsInt96, results, failed);
}

// ////////////////
// test EQUAL
// ////////////////
template <class T>
void testValueFilterEqual(T value, std::vector<T> inputs, 
                                   std::vector<bool> results,
                                   int& failed)
{
  assert(inputs.size() == results.size());

  ValueFilter* filter = ValueFilter::MakeEQ(value);

  for (int i=0; i<inputs.size(); i++) {
     failed += passIf(inputs[i], filter, results[i]);
  }

  delete filter;
}

void testValueFilterEqual(int& failed)
{
  // std::string
  std::vector<std::string> inputs = {"bcd    ", "c  ", "a"};
  std::vector<bool> results = {true, false, false};

  std::string value("bcd ");
  testValueFilterEqual(value, inputs, results, failed);

  // int32_t
  std::vector<int32_t> inputsInt32 = {2, 11, -1};

  int32_t valueInt32(2);
  testValueFilterEqual(valueInt32, inputsInt32, results, failed);

  // int64_t
  std::vector<int64_t> inputsInt64 = {2, 11, -1};

  int64_t valueInt64(2);
  testValueFilterEqual(valueInt64, inputsInt64, results, failed);

  // float 
  std::vector<float> inputsFloat= {2.1, 11, -1};

  float valueFloat(2.1);
  testValueFilterEqual(valueFloat, inputsFloat, results, failed);

  // parquet Int96
  std::vector<parquet::Int96> inputsInt96 = {{100, 1000}, {1, 1}, {300, 2}};

  parquet::Int96 valueInt96(100, 1000);
  testValueFilterEqual(valueInt96, inputsInt96, results, failed);
}

void testValueFilters()
{
  int failed = 0;

  testValueFilterRange(failed);
  testValueFilterLessThan(failed);
  testValueFilterLessThanEqual(failed);
  testValueFilterGreaterThan(failed);
  testValueFilterGreaterThanEqual(failed);
  testValueFilterEqual(failed);

  cout << "Value filter test:";

  if ( failed == 0 )
     cout << " PASS";
  else 
     cout << " FAILED(" << failed << ")";

  cout  << endl;
}

