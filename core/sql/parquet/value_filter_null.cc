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
 * File:         value_filter_null.cc
 *
 * Description:  All isnull or is not null value filter 
 *               related code.
 *
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include "parquet/value_filter.h"
#include "parquet/types.h"
#include "parquet/util/stopwatch.h"
#include <cstdint>
#include <iostream>

template <typename DType>
class ValueFilterIsNotNull: public TypedValueFilter<DType>
{
public:
  ValueFilterIsNotNull() {};
  ~ValueFilterIsNotNull() {};

  void eval(std::shared_ptr<arrow::NumericArray<DType>>* values) ;
  void eval(std::shared_ptr<arrow::StringArray>* values) ;
  void eval(std::shared_ptr<arrow::BooleanArray>* values) ;

  void display();

protected:
};

ValueFilter* ValueFilter::MakeIsNotNull(bool)
{ return new ValueFilterIsNotNull<arrow::BooleanType>(); }

ValueFilter* ValueFilter::MakeIsNotNull(int8_t)
{ return new ValueFilterIsNotNull<arrow::Int8Type>(); }

ValueFilter* ValueFilter::MakeIsNotNull(int16_t)
{ return new ValueFilterIsNotNull<arrow::Int16Type>(); }

ValueFilter* ValueFilter::MakeIsNotNull(int32_t)
{ return new ValueFilterIsNotNull<arrow::Int32Type>(); }

ValueFilter* ValueFilter::MakeIsNotNull(int64_t)
{ return new ValueFilterIsNotNull<arrow::Int64Type>(); }

ValueFilter* ValueFilter::MakeIsNotNull(float)
{ return new ValueFilterIsNotNull<arrow::FloatType>(); }

ValueFilter* ValueFilter::MakeIsNotNull(double)
{ return new ValueFilterIsNotNull<arrow::DoubleType>(); }

ValueFilter* ValueFilter::MakeIsNotNull(parquet::Int96)
{ return new ValueFilterIsNotNull<arrow::Time96Type>(); }

ValueFilter* ValueFilter::MakeIsNotNull(std::string&)
{ return new ValueFilterIsNotNull<std::string>(); }

template <typename DType>
void ValueFilterIsNotNull<DType>::display()
{
   std::cout << "IsNotNull" ;
}

//#define DEBUG_ISNOTNULL_FILTER_EVAL 1

template <typename DType>
void ValueFilterIsNotNull<DType>::eval(
   std::shared_ptr<arrow::NumericArray<DType>>* values
                                  )
{
   if ( !values ) return;

   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits =
     const_cast<uint8_t*>((*values)->null_bitmap_data());
  
   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
      arrow::BitUtil::applySqlIsNotNull(selectedBits, nullBits, len);

}

template <typename DType>
void ValueFilterIsNotNull<DType>::eval(
  std::shared_ptr<arrow::StringArray>* values
                                      )
{
   if ( !values ) return;

   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits =
     const_cast<uint8_t*>((*values)->null_bitmap_data());

#ifdef DEBUG_ISNOTNULL_FILTER_EVAL 
   std::cout << "Apply isNotNull for StringArray:";
   this->display();
   std::cout << ", source len=" << len;

   ::arrow::internal::BitmapReader reader(selectedBits, 0, len);

   std::cout << ", #bits set in selected bitmap=" 
             << reader.totalBitsSet() << std::endl;

   ::arrow::internal::BitmapReader readerN(nullBits, 0, len);
   std::cout << ", #bits set in null bitmap=" 
             << readerN.totalBitsSet() << std::endl;

   parquet::StopWatch x;
   x.Start();
#endif
  
   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
      arrow::BitUtil::applySqlIsNotNull(selectedBits, nullBits, len);

#ifdef DEBUG_ISNOTNULL_FILTER_EVAL 
   std::cout << "Done! total eval time (us)=" << x.Stop()
             << ", #bits set in selected bitmap=" << reader.totalBitsSet() 
             << std::endl 
             << std::endl;
#endif
}

template <typename DType>
void ValueFilterIsNotNull<DType>::eval(
  std::shared_ptr<arrow::BooleanArray>* values
                                      )
{
   if ( !values ) return;

   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits =
     const_cast<uint8_t*>((*values)->null_bitmap_data());
  
   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
      arrow::BitUtil::applySqlIsNotNull(selectedBits, nullBits, len);
}

// about NumericArray
eval_boilerplateA(ValueFilterIsNotNull, std::string, arrow::NumericArray);
eval_boilerplateA(ValueFilterIsNotNull, arrow::BooleanType, arrow::NumericArray);

// about StringArray 
eval_boilerplateB(ValueFilterIsNotNull, arrow::Int8Type, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::Int16Type, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::Int32Type, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::Int64Type, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::FloatType, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::DoubleType, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::Time96Type, arrow::StringArray);


// about BooleanArray
eval_boilerplateB(ValueFilterIsNotNull, arrow::Int8Type, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::Int16Type, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::Int32Type, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::Int64Type, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::FloatType, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::DoubleType, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNotNull, arrow::Time96Type, arrow::BooleanArray);


////////////////
//
template <typename DType>
class ValueFilterIsNull: public TypedValueFilter<DType> 
{
public:
  ValueFilterIsNull() {};
  ~ValueFilterIsNull() {};

  void display();

  void eval(std::shared_ptr<arrow::NumericArray<DType>>* values) ;
  void eval(std::shared_ptr<arrow::StringArray>* values) ;
  void eval(std::shared_ptr<arrow::BooleanArray>* values) ;

protected:
};

ValueFilter* ValueFilter::MakeIsNull(bool)
{ return new ValueFilterIsNull<arrow::BooleanType>(); }

ValueFilter* ValueFilter::MakeIsNull(int8_t)
{ return new ValueFilterIsNull<arrow::Int8Type>(); }

ValueFilter* ValueFilter::MakeIsNull(int16_t)
{ return new ValueFilterIsNull<arrow::Int16Type>(); }

ValueFilter* ValueFilter::MakeIsNull(int32_t)
{ return new ValueFilterIsNull<arrow::Int32Type>(); }

ValueFilter* ValueFilter::MakeIsNull(int64_t)
{ return new ValueFilterIsNull<arrow::Int64Type>(); }

ValueFilter* ValueFilter::MakeIsNull(float)
{ return new ValueFilterIsNull<arrow::FloatType>(); }

ValueFilter* ValueFilter::MakeIsNull(double)
{ return new ValueFilterIsNull<arrow::DoubleType>(); }

ValueFilter* ValueFilter::MakeIsNull(parquet::Int96)
{ return new ValueFilterIsNull<arrow::Time96Type>(); }

ValueFilter* ValueFilter::MakeIsNull(std::string&)
{ return new ValueFilterIsNull<std::string>(); }

template <typename DType>
void ValueFilterIsNull<DType>::display()
{
   std::cout << "IsNull";
}

template <typename DType>
void ValueFilterIsNull<DType>::eval(
   std::shared_ptr<arrow::NumericArray<DType>>* values
                                  )
{
   if ( !values ) return;

   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits =
     const_cast<uint8_t*>((*values)->null_bitmap_data());
  
   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
      arrow::BitUtil::applySqlIsNull(selectedBits, nullBits, len);
}

template <typename DType>
void ValueFilterIsNull<DType>::eval(
  std::shared_ptr<arrow::StringArray>* values
                                  )
{
   if ( !values ) return;

   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits =
     const_cast<uint8_t*>((*values)->null_bitmap_data());
  
   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
      arrow::BitUtil::applySqlIsNull(selectedBits, nullBits, len);
}

template <typename DType>
void ValueFilterIsNull<DType>::eval(
  std::shared_ptr<arrow::BooleanArray>* values
                                  )
{
   if ( !values ) return;

   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits =
     const_cast<uint8_t*>((*values)->null_bitmap_data());
  
   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
      arrow::BitUtil::applySqlIsNull(selectedBits, nullBits, len);
}

// about NumericArray
eval_boilerplateA(ValueFilterIsNull, std::string, arrow::NumericArray);
eval_boilerplateA(ValueFilterIsNull, arrow::BooleanType, arrow::NumericArray);

// about StringArray 
eval_boilerplateB(ValueFilterIsNull, arrow::Int8Type, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNull, arrow::Int16Type, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNull, arrow::Int32Type, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNull, arrow::Int64Type, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNull, arrow::FloatType, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNull, arrow::DoubleType, arrow::StringArray);
eval_boilerplateB(ValueFilterIsNull, arrow::Time96Type, arrow::StringArray);


// about BooleanArray
eval_boilerplateB(ValueFilterIsNull, arrow::Int8Type, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNull, arrow::Int16Type, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNull, arrow::Int32Type, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNull, arrow::Int64Type, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNull, arrow::FloatType, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNull, arrow::DoubleType, arrow::BooleanArray);
eval_boilerplateB(ValueFilterIsNull, arrow::Time96Type, arrow::BooleanArray);

