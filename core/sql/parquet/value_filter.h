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
 * File:         value_filter.h
 *
 * Description:  
 *
 * Value filter is an Esgyn Extension to the arrow/parquet reader
 * that allows a value to be skipped from further processing, 
 * if the value does not satisfy a comparision test against
 * a desirable range of values.
 *
 * The ValueFilter class contains the desirable range of values
 * in several forms and its method comoare() compares the value 
 * against the range. Either a true of a false result from the 
 * method is recorded in the is_selected bit map embedded in 
 * the arrray for the column. A bit of 0 indicates that the value
 * is skipped and 1 otherwise. All is_selected bit maps associated
 * with the data arrays of all relevant columns are consulted 
 * to produce the final values from the reader.
 *
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#ifndef PARQUET_VALUE_FILTER_H
#define PARQUET_VALUE_FILTER_H

#include <string>
#include "parquet/types.h"
#include "parquet/filter.h"
#include "arrow/array.h"


class ValueFilter : public ParquetFilter {

public:
  ValueFilter() : skipInRangeSpan_(false) {};
  ~ValueFilter() {};

 void display() {};

 static ValueFilter* MakeEQ(bool v);
 static ValueFilter* MakeEQ(int8_t v);
 static ValueFilter* MakeEQ(int16_t v);
 static ValueFilter* MakeEQ(int32_t v);
 static ValueFilter* MakeEQ(int64_t v);
 static ValueFilter* MakeEQ(float v);
 static ValueFilter* MakeEQ(double v);
 static ValueFilter* MakeEQ(std::string& v);
 static ValueFilter* MakeEQ(parquet::Int96 v);

 static ValueFilter* MakeRange(bool min, bool max);
 static ValueFilter* MakeRange(int8_t min, int8_t max);
 static ValueFilter* MakeRange(int16_t min, int16_t max);
 static ValueFilter* MakeRange(int32_t min, int32_t max);
 static ValueFilter* MakeRange(int64_t min, int64_t max);
 static ValueFilter* MakeRange(float min, float max);
 static ValueFilter* MakeRange(double min, double max);
 static ValueFilter* MakeRange(std::string& min, std::string& max);
 static ValueFilter* MakeRange(parquet::Int96 min, parquet::Int96 max);

 static ValueFilter* MakeLessThan(bool v);
 static ValueFilter* MakeLessThan(int8_t v);
 static ValueFilter* MakeLessThan(int16_t v);
 static ValueFilter* MakeLessThan(int32_t v);
 static ValueFilter* MakeLessThan(int64_t v);
 static ValueFilter* MakeLessThan(float v);
 static ValueFilter* MakeLessThan(double v);
 static ValueFilter* MakeLessThan(std::string& v);
 static ValueFilter* MakeLessThan(parquet::Int96 v);

 static ValueFilter* MakeLessThanEqual(bool v);
 static ValueFilter* MakeLessThanEqual(int8_t v);
 static ValueFilter* MakeLessThanEqual(int16_t v);
 static ValueFilter* MakeLessThanEqual(int32_t v);
 static ValueFilter* MakeLessThanEqual(int64_t v);
 static ValueFilter* MakeLessThanEqual(float v);
 static ValueFilter* MakeLessThanEqual(double v);
 static ValueFilter* MakeLessThanEqual(std::string& v);
 static ValueFilter* MakeLessThanEqual(parquet::Int96 v);

 static ValueFilter* MakeGreaterThan(bool v);
 static ValueFilter* MakeGreaterThan(int8_t v);
 static ValueFilter* MakeGreaterThan(int16_t v);
 static ValueFilter* MakeGreaterThan(int32_t v);
 static ValueFilter* MakeGreaterThan(int64_t v);
 static ValueFilter* MakeGreaterThan(float v);
 static ValueFilter* MakeGreaterThan(double v);
 static ValueFilter* MakeGreaterThan(std::string& v);
 static ValueFilter* MakeGreaterThan(parquet::Int96 v);

 static ValueFilter* MakeGreaterThanEqual(bool v);
 static ValueFilter* MakeGreaterThanEqual(int8_t v);
 static ValueFilter* MakeGreaterThanEqual(int16_t v);
 static ValueFilter* MakeGreaterThanEqual(int32_t v);
 static ValueFilter* MakeGreaterThanEqual(int64_t v);
 static ValueFilter* MakeGreaterThanEqual(float v);
 static ValueFilter* MakeGreaterThanEqual(double v);
 static ValueFilter* MakeGreaterThanEqual(std::string& v);
 static ValueFilter* MakeGreaterThanEqual(parquet::Int96 v);

 static ValueFilter* MakeIsNotNull(bool v);
 static ValueFilter* MakeIsNotNull(int8_t v);
 static ValueFilter* MakeIsNotNull(int16_t v);
 static ValueFilter* MakeIsNotNull(int32_t v);
 static ValueFilter* MakeIsNotNull(int64_t v);
 static ValueFilter* MakeIsNotNull(float v);
 static ValueFilter* MakeIsNotNull(double v);
 static ValueFilter* MakeIsNotNull(std::string& v);
 static ValueFilter* MakeIsNotNull(parquet::Int96 v);

 static ValueFilter* MakeIsNull(bool v);
 static ValueFilter* MakeIsNull(int8_t v);
 static ValueFilter* MakeIsNull(int16_t v);
 static ValueFilter* MakeIsNull(int32_t v);
 static ValueFilter* MakeIsNull(int64_t v);
 static ValueFilter* MakeIsNull(float v);
 static ValueFilter* MakeIsNull(double v);
 static ValueFilter* MakeIsNull(std::string& v);
 static ValueFilter* MakeIsNull(parquet::Int96 v);

 static ValueFilter* MakeBloomFilterEval(bool, const char* data, int len);
 static ValueFilter* MakeBloomFilterEval(int8_t, const char* data, int len);
 static ValueFilter* MakeBloomFilterEval(int16_t, const char* data, int len);
 static ValueFilter* MakeBloomFilterEval(int32_t, const char* data, int len);
 static ValueFilter* MakeBloomFilterEval(int64_t, const char* data, int len);
 static ValueFilter* MakeBloomFilterEval(float, const char* data, int len);
 static ValueFilter* MakeBloomFilterEval(double, const char* data, int len);
 static ValueFilter* MakeBloomFilterEval(std::string&, const char* data, int len);
 static ValueFilter* MakeBloomFilterEval(parquet::Int96, const char* data, int len);

 virtual bool compare(bool x) { return false; };
 virtual bool compare(int8_t x) { return false; };
 virtual bool compare(int16_t x) { return false; };
 virtual bool compare(int32_t x) { return false; };
 virtual bool compare(int64_t x) { return false; };

 virtual bool compare(float x) { return false; };
 virtual bool compare(double x) { return false; };

 virtual bool compare(std::string x) { return false; };
 virtual bool compare(parquet::Int96 x) { return false; };

 virtual bool canSkipInRangeSpan() { return skipInRangeSpan_; }
 virtual void setSkipInRangeSpan(bool x) { skipInRangeSpan_ = x; }

protected:
 bool skipInRangeSpan_;
};

template <typename DType>
class TypedValueFilter: public ValueFilter {

public:
  TypedValueFilter() {};
  ~TypedValueFilter() {};

  virtual void eval(std::shared_ptr<arrow::NumericArray<DType>>* values) ;
  virtual void eval(std::shared_ptr<arrow::StringArray>* values) ;
  virtual void eval(std::shared_ptr<arrow::BooleanArray>* values) ;

protected:
};

// boilerplate template to avoid compilation errors.
#define eval_boilerplateA(className, elemType, arrayType) \
        template <> \
        void className<elemType>::eval( \
           std::shared_ptr<arrayType<elemType>>* values \
                                  ) {}

#define eval_boilerplateB(className, elemType, arrayType) \
        template <> \
        void className<elemType>::eval( \
           std::shared_ptr<arrayType>* values \
                                  ) {}

#endif
