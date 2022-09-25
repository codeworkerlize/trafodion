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
 * File:         value_filter.cc
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
#include "parquet/value_filter.h"
#include "parquet/types.h"
#include "parquet/util/stopwatch.h"
#include "arrow/util/bit-util.h"
#include <iostream>

//#define DEBUG_VALUE_FILTER 1
      
#define SHOW_RESULT \
      if ( result[j] ) { \
        std::cout << "<<< At idx=" << idx+j; \
        std::cout << ", bit=" << result[j]; \
        std::cout << ", value=" << theValue << std::endl; \
        num_hits++;  \
      }

#define SHOW_RESULT_FOR_VALUE \
      auto theValue = valueArray[idx+j]; \
      SHOW_RESULT;

#define SHOW_RESULT_FOR_STRING \
      auto theValue = (*values)->GetString(idx+j); \
      SHOW_RESULT;

#define SHOW_RESULT_FOR_BOOLEAN \
      auto theValue = (*values)->Value(idx+j); \
      SHOW_RESULT;

template <class DType>
void evalOneNumericSpan(DType* valueArray, int64_t offset, int64_t len, 
                        ValueFilter* vFilter, uint8_t* selectedBits) 
{
   int64_t num_hits = 0;

   int64_t idx = offset;     // bit index to valueArray[]

   int firstFew = offset % 8;

   if ( firstFew > 0 )
     firstFew = 8 - firstFew;

   for (int j=0; j<firstFew; j++ ) {
     arrow::BitUtil::AndBit(selectedBits, idx, 
                            vFilter->compare(valueArray[idx]));

     idx++;
   }

   const int numSmallEvals = (len-firstFew) / 8;
   int leftOver = (len-firstFew) % 8;
   uint8_t oneSmallEval = 0;

   for (int j=0; j<numSmallEvals; j++ ) {

        if ( selectedBits[j] ) {

           // Evaluate the predicate for the current 
           // 8 values
           oneSmallEval = 0;
   
           if ( vFilter->compare(valueArray[idx++]) )
             oneSmallEval |= arrow::BitUtil::kBitmask[0];
   
           if ( vFilter->compare(valueArray[idx++]) )
             oneSmallEval |= arrow::BitUtil::kBitmask[1];
   
           if ( vFilter->compare(valueArray[idx++]) )
             oneSmallEval |= arrow::BitUtil::kBitmask[2];
   
           if ( vFilter->compare(valueArray[idx++]) )
             oneSmallEval |= arrow::BitUtil::kBitmask[3];
   
           if ( vFilter->compare(valueArray[idx++]) )
             oneSmallEval |= arrow::BitUtil::kBitmask[4];
   
           if ( vFilter->compare(valueArray[idx++]) )
             oneSmallEval |= arrow::BitUtil::kBitmask[5];
   
           if ( vFilter->compare(valueArray[idx++]) )
             oneSmallEval |= arrow::BitUtil::kBitmask[6];
   
           if ( vFilter->compare(valueArray[idx++]) )
             oneSmallEval |= arrow::BitUtil::kBitmask[7];
   
           selectedBits[j] &= oneSmallEval;
        } else
           idx += 8;
   }

   for (int j=0; j<leftOver; j++ ) {
     arrow::BitUtil::AndBit(selectedBits, idx, 
                            vFilter->compare(valueArray[idx]));
     idx++;
   }
}

template <typename DType>
void TypedValueFilter<DType>::eval(
         std::shared_ptr<arrow::NumericArray<DType>>* values
                                  ) 
{
   if ( !values ) return;

   auto valueArray = (*values)->raw_values();
   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits = 
         const_cast<uint8_t*>((*values)->null_bitmap_data());

   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
     arrow::BitUtil::applySqlIsNotNull(selectedBits, nullBits, len);

   const skipVecType& vec = (*values)->getSkipVec();

#ifdef DEBUG_VALUE_FILTER
   std::cout << "Apply value predicate(s):";
   this->display();
   std::cout << ", len(bits)=" << len;

   ::arrow::internal::BitmapReader reader(selectedBits, 0, len);
   std::cout << ", #bits set in selected bitmap=" 
             << reader.totalBitsSet() << std::endl;

   ::arrow::internal::BitmapReader readerN(nullBits, 0, len);
   std::cout << ", #bits set in null bitmap=" 
             << readerN.totalBitsSet() << std::endl;

   std::cout << "skipVec=" << vec << std::endl;

   parquet::StopWatch x;
   x.Start();
#endif


   int64_t offset = 0;

   for (auto skip : vec) {

      switch (skip.code) {
        case skipType::REJECT:
          break;

        case skipType::IN_RANGE:

          if ( this->canSkipInRangeSpan() )
             break;
          // otherwise fall through to the OVERLAP case 
          // below to evaluate

        case skipType::OVERLAP:
          evalOneNumericSpan(valueArray, offset, skip.len, this, selectedBits);
          break;
      }

      offset += skip.len;
   }

#ifdef DEBUG_VALUE_FILTER
   std::cout << "Done! total eval time (us)=" << x.Stop()
             << ", final #bits set in selected bitmap=" 
             << reader.totalBitsSet() 
             << std::endl
             << std::endl;
#endif
}

// ------------------------------------------------------------------
// String data type
// ------------------------------------------------------------------
void evalOneStringSpan(std::shared_ptr<arrow::StringArray>* values, 
                       int64_t offset, int64_t len, 
                       ValueFilter* vFilter, uint8_t* selectedBits) 
{
   int64_t num_hits = 0;

   int64_t idx = offset;     // bit index to valueArray[]

   int firstFew = offset % 8;

   if ( firstFew > 0 )
     firstFew = 8 - firstFew;

   for (int j=0; j<firstFew; j++ ) {
     arrow::BitUtil::AndBit(selectedBits, idx, 
                            vFilter->compare((*values)->GetString(idx)));
     idx++;
   }

   const int numSmallEvals = (len-firstFew) / 8;
   int leftOver = (len-firstFew) % 8;
   uint8_t oneSmallEval = 0;

   for (int j=0; j<numSmallEvals; j++ ) {

        if ( selectedBits[j] ) {

           // Compute the current 8 values
           oneSmallEval = 0;
   
           if ( vFilter->compare((*values)->GetString(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[0];
   
           if ( vFilter->compare((*values)->GetString(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[1];
   
           if ( vFilter->compare((*values)->GetString(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[2];
   
           if ( vFilter->compare((*values)->GetString(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[3];
   
           if ( vFilter->compare((*values)->GetString(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[4];
   
           if ( vFilter->compare((*values)->GetString(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[5];
   
           if ( vFilter->compare((*values)->GetString(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[6];
   
           if ( vFilter->compare((*values)->GetString(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[7];
   
           selectedBits[j] &= oneSmallEval;
        } else
           idx += 8;
   }

   for (int j=0; j<leftOver; j++ ) {
     arrow::BitUtil::AndBit(selectedBits, idx, 
                            vFilter->compare((*values)->GetString(idx)));
     idx++;
   }
}


template <>
void TypedValueFilter<std::string>::eval(std::shared_ptr<arrow::StringArray>* values) 
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

   const skipVecType& vec = (*values)->getSkipVec();

#ifdef DEBUG_VALUE_FILTER
   std::cout << "Apply value predicate(s):";
   this->display();
   std::cout << ", len(bits)=" << len;

   ::arrow::internal::BitmapReader reader(selectedBits, 0, len);
   std::cout << ", #bits set in selected bitmap=" 
             << reader.totalBitsSet() << std::endl;

   ::arrow::internal::BitmapReader readerN(nullBits, 0, len);
   std::cout << ", #bits set in null bitmap=" 
             << readerN.totalBitsSet();

   std::cout << "skipVec=" << vec << std::endl;

   parquet::StopWatch x;
   x.Start();
#endif
   int64_t offset = 0;

   for (auto skip : vec) {

      switch (skip.code) {
        case skipType::REJECT:
          break;

        case skipType::IN_RANGE:

          if ( this->canSkipInRangeSpan() )
             break;
          // otherwise fall through to the OVERLAP case 
          // below to evaluate

        case skipType::OVERLAP:
          evalOneStringSpan(values, offset, skip.len, this, selectedBits);
          break;
      }

      offset += skip.len;
   }

#ifdef DEBUG_VALUE_FILTER
   std::cout << "Done! total eval time (us)=" << x.Stop()
             << ", final #bits set in selected bitmap=" 
             << reader.totalBitsSet() 
             << std::endl
             << std::endl;
#endif
}

// ------------------------------------------------------------------
// Bool data type
// ------------------------------------------------------------------
void evalOneBooleanSpan(std::shared_ptr<arrow::BooleanArray>* values, 
                       int64_t offset, int64_t len, 
                       ValueFilter* vFilter, uint8_t* selectedBits) 
{
   int64_t num_hits = 0;

   int64_t idx = offset;     // bit index to valueArray[]

   int firstFew = offset % 8;

   if ( firstFew > 0 )
     firstFew = 8 - firstFew;

   for (int j=0; j<firstFew; j++ ) {
     arrow::BitUtil::AndBit(selectedBits, idx, 
                            vFilter->compare((*values)->Value(idx)));
     idx++;
   }

   const int numSmallEvals = (len-firstFew) / 8;
   int leftOver = (len-firstFew) % 8;
   uint8_t oneSmallEval = 0;

   for (int j=0; j<numSmallEvals; j++ ) {

        if ( selectedBits[j] ) {

           // Compute the current 8 values
           oneSmallEval = 0;
   
           if ( vFilter->compare((*values)->Value(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[0];
   
           if ( vFilter->compare((*values)->Value(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[1];
   
           if ( vFilter->compare((*values)->Value(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[2];
   
           if ( vFilter->compare((*values)->Value(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[3];
   
           if ( vFilter->compare((*values)->Value(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[4];
   
           if ( vFilter->compare((*values)->Value(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[5];
   
           if ( vFilter->compare((*values)->Value(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[6];
   
           if ( vFilter->compare((*values)->Value(idx++)) )
             oneSmallEval |= arrow::BitUtil::kBitmask[7];
   
           selectedBits[j] &= oneSmallEval;
        } else
           idx += 8;
   }

   for (int j=0; j<leftOver; j++ ) {
     arrow::BitUtil::AndBit(selectedBits, idx, 
                            vFilter->compare((*values)->Value(idx)));
     idx++;
   }
}

template <>
void TypedValueFilter<arrow::BooleanType>::eval(std::shared_ptr<arrow::BooleanArray>* values) 
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

   const skipVecType& vec = (*values)->getSkipVec();

#ifdef DEBUG_VALUE_FILTER
   std::cout << "Apply value predicate(s):";
   this->display();
   std::cout << ", len(bits)=" << len;

   ::arrow::internal::BitmapReader reader(selectedBits, 0, len);
   std::cout << ", #bits set in selected bitmap=" 
             << reader.totalBitsSet() << std::endl;

   ::arrow::internal::BitmapReader readerN(nullBits, 0, len);
   std::cout << ", #bits set in null bitmap=" 
             << readerN.totalBitsSet();

   std::cout << "skipVec=" << vec << std::endl;

   parquet::StopWatch x;
   x.Start();
#endif

   int64_t offset = 0;

   for (auto skip : vec) {

      switch (skip.code) {
        case skipType::REJECT:
          break;

        case skipType::IN_RANGE:

          if ( this->canSkipInRangeSpan() )
             break;
          // otherwise fall through to the OVERLAP case 
          // below to evaluate

        case skipType::OVERLAP:
          evalOneBooleanSpan(values, offset, skip.len, this, selectedBits);
          break;
      }

      offset += skip.len;
   }

#ifdef DEBUG_VALUE_FILTER
   std::cout 
             << "Done!  total eval time (us)=" << x.Stop()
             << ", final #bits set in selected bitmap=" 
             << reader.totalBitsSet() 
             << std::endl
             << std::endl;
#endif
}

//
// Boiler plates to avoid compilation error
//
// about NumericArray
eval_boilerplateA(TypedValueFilter, std::string, arrow::NumericArray);
eval_boilerplateA(TypedValueFilter, arrow::BooleanType, arrow::NumericArray);

// about StringArray 
eval_boilerplateB(TypedValueFilter, arrow::Int8Type, arrow::StringArray);
eval_boilerplateB(TypedValueFilter, arrow::Int16Type, arrow::StringArray);
eval_boilerplateB(TypedValueFilter, arrow::Int32Type, arrow::StringArray);
eval_boilerplateB(TypedValueFilter, arrow::Int64Type, arrow::StringArray);
eval_boilerplateB(TypedValueFilter, arrow::FloatType, arrow::StringArray);
eval_boilerplateB(TypedValueFilter, arrow::DoubleType, arrow::StringArray);
eval_boilerplateB(TypedValueFilter, arrow::Time96Type, arrow::StringArray);
eval_boilerplateB(TypedValueFilter, arrow::BooleanType, arrow::StringArray);

// about BooleanArray
eval_boilerplateB(TypedValueFilter, arrow::Int8Type, arrow::BooleanArray);
eval_boilerplateB(TypedValueFilter, arrow::Int16Type, arrow::BooleanArray);
eval_boilerplateB(TypedValueFilter, arrow::Int32Type, arrow::BooleanArray);
eval_boilerplateB(TypedValueFilter, arrow::Int64Type, arrow::BooleanArray);
eval_boilerplateB(TypedValueFilter, arrow::FloatType, arrow::BooleanArray);
eval_boilerplateB(TypedValueFilter, arrow::DoubleType, arrow::BooleanArray);
eval_boilerplateB(TypedValueFilter, arrow::Time96Type, arrow::BooleanArray);
eval_boilerplateB(TypedValueFilter, std::string, arrow::BooleanArray);


template class TypedValueFilter<arrow::BooleanType>;
template class TypedValueFilter<arrow::Int8Type>;
template class TypedValueFilter<arrow::Int16Type>;
template class TypedValueFilter<arrow::Int32Type>;
template class TypedValueFilter<arrow::FloatType>;
template class TypedValueFilter<arrow::DoubleType>;
template class TypedValueFilter<arrow::Int64Type>;
template class TypedValueFilter<arrow::Time96Type>;
template class TypedValueFilter<std::string>;

