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
 * File:         filter.h
 * Description:  The base class ParquetFilter for the filter 
 *               class hierarchy, and the Filters class that
 *               holds both a page filter and a value file.
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#ifndef PARQUET_FILTER_H
#define PARQUET_FILTER_H


class PageFilter;
class ValueFilter;

class ParquetFilter {

public:
  ParquetFilter() : filterId_(-1), 
                    valuesRemoved_(0), 
                    valuesKept_(0), 
                    rowsToSurvive_(0), 
                    next_(NULL) {};

  virtual ~ParquetFilter() {};

  void setNext(ParquetFilter* x) { next_ = x; }
  ParquetFilter* getNext() { return next_; }

  virtual void display() {};
  virtual void displayValue(const char* msg, const std::string& val) {};

  virtual void resetCounter() { valuesRemoved_= 0; };

  int32_t getFilterId() const { return filterId_; }
  void setFilterId(int32_t id) { filterId_ = id; }

  void incValuesRemoved(int64_t x) { valuesRemoved_ += x; }
  int64_t getValuesRemoved() { return valuesRemoved_; }

  void incValuesKept(int64_t x)  { valuesKept_ += x; }
  int64_t getValuesKept()  { return valuesKept_; }

  void setRowsToSurvive(int64_t x) { rowsToSurvive_ = x; }
  int64_t getRowsToSurvive() { return rowsToSurvive_;}

protected:

  int32_t filterId_;
  int64_t valuesKept_;
  int64_t valuesRemoved_;
  int64_t rowsToSurvive_; // expected # of rows to survive
  ParquetFilter* next_;
};

class Filters 
{

public:
  Filters() : pageFilter_(NULL), valueFilter_(NULL) {};
  ~Filters();

  void setPageFilter(PageFilter* x) { pageFilter_ = x; }
  void setValueFilter(ValueFilter* x) { valueFilter_ = x; }

  PageFilter* getPageFilter() { return pageFilter_ ; }
  ValueFilter* getValueFilter() { return valueFilter_ ; }

  bool canFilter() 
    { return ( pageFilter_ || valueFilter_ ); };

  virtual void display(const char* msg);

  void resetCounters();
  //void disableBloomFilters();

protected:
   PageFilter* pageFilter_;
   ValueFilter* valueFilter_;
};


#endif
