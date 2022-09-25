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

#include "ExpParquetArrowInterface.h"
#include "ex_ex.h"
#include "exp_attrs.h"
#include "ExpCriDesc.h"
#include "ComRtUtils.h"
#include "ComCextdecs.h"
#include "exp_datetime.h"
#include "ex_tcb_private.h"
#include "sql_buffer.h"
#include "ex_globals.h"
#include "ExStats.h"
#include "ComTdbOrcAccess.h"
#include <assert.h>
#include "arrow/util/decimal.h"
#include "statistics.h"
#include "ttime.h"
#include "QRLogger.h"
//#include "metadata.h"

//#define DEBUG_COLUMN_VISITOR 1
//#define DEBUG_SCAN_OPEN 1

ExpParquetArrowInterface* ExpParquetArrowInterface::newInstance(
                                                    CollHeap* heap,
                                                    NABoolean fullScan)
{
  if ( fullScan )
     return new (heap) ExpParquetArrowInterfaceTable(heap);
  else
     return new (heap) ExpParquetArrowInterfaceBatch(heap);
}

ExpParquetArrowInterface::ExpParquetArrowInterface(CollHeap * heap):
   ExpParquetInterface(heap, "", 0),
   rowBuffer_(NULL),
   rowBufferLen_(0),
   mergedSelectedBits_(NULL),
   visitor_(NULL),
   totalRowsToRead_(0),
   totalRowsToAccess_(0),
   skipCCStatsCheck_(FALSE),
   filterRowgroups_(FALSE),
   filterPages_(FALSE)
{
  // to do:
  // swhdfs getconf -confKey fs.defaultFS
  //  hdfs://localhost:30000
}

// We can not disconnect from HDFS here as
// the destructor for some of the data mebers
// associated with Arrow reader relies on the 
// conection. It is OK to let hdfsClient_ go
// here since it is a shared_ptr. In the hdfs
// cache in CliContext, there is another share_ptr
// poting at the same hdfs client object. If the
// context is deleted, then the hdfs client
// may or may not destructed, depending on whether
// the context is the last one referencing the 
// hdfs client. 
ExpParquetArrowInterface::~ExpParquetArrowInterface()
{ 
   releaseResources();

}

Lng32 ExpParquetArrowInterface::scanOpen(
                                ExHdfsScanStats *hdfsStats,
                                char* tableName,
                                char* fileName,
                                const int expectedRowSize,
                                const int maxRowsToFill,
                                const int maxAllocationInMB,
                                const int maxVectorBatchSize,
                                const Int64 startRowNum, 
                                const Int64 stopRowNum,
                                Lng32 numCols,
                                Lng32* whichCols,
                                TextVec* ppiVec,
                                TextVec* ppiAllCols,
                                TextVec* colNameVec,
                                TextVec* colTypeVec,
                                std::vector<UInt64>* expectedRowsVec,
                                char * parqSchStr,
                                Lng32 flags,
                                Int32 numThreads)
{
  fileName_ = fileName;

  hss_ = hdfsStats;

  skipCCStatsCheck_ = 
         ((flags & ComTdbExtStorageScan::PARQUET_CPP_SKIP_COLUMN_STATS_CHECK) != 0);

  filterRowgroups_ = 
         ((flags & ComTdbExtStorageScan::PARQUET_CPP_FILTER_ROWGROUPS) != 0);

  filterPages_ = 
         ((flags & ComTdbExtStorageScan::PARQUET_CPP_FILTER_PAGES) != 0);

  Lng32 rc = OFR_OK;

  columnsToReturn_.clear();

  if ( numCols > 0 ) {
     columnsToReturn_.resize(numCols);

     for (int i=0; i<numCols; i++ ) {
       // Column index is 0-based with arrow reader
       columnsToReturn_[i] = whichCols[i]-1; 
     }
  } else if ( colTypeVec  ) {
     columnsToReturn_.resize(1);
     columnsToReturn_[0] = findIndexForNarrowestColumn(colTypeVec); 
  }


  std::shared_ptr<arrow::io::HdfsReadableFile> hdfsFile;
  std::string path(fileName);

  const Int32 ONE_MB=1048576;
  Int32 bufSize = 128 * ONE_MB;

  arrowStatus_ = hdfsClient_->OpenReadable(path, bufSize, &hdfsFile);

  if (!arrowStatus_.ok()) {
    return -OFR_ERROR_ARROW_READER_OPEN_READABLE;
  }

  arrowStatus_ = OpenFile(
                hdfsFile,
                ::arrow::default_memory_pool(),
                ::parquet::default_reader_properties(), 
                metadata_, // metadata
                &reader_);

  if (!arrowStatus_.ok()) {
    return -OFR_ERROR_ARROW_READER_OPEN_FILE;
  }

  reader_->set_num_threads(numThreads);

  // maxRowsToFill is the max number of rows to fill, from CQD
  // ORC_READER_MAX_ROWS_TO_FILL, default to 128. This argument 
  // is not used here because the scan node ExExtStorageScanTcb::work
  // can only process one row at a time, in step PROCESS_EXT_ROW.
  // We do not do anything for maxRowsToFill here.

  rc = setupFilters(tableName, fileName, ppiVec, 
                    colNameVec, colTypeVec, expectedRowsVec);

  // Translate the startRowNum (offset) and stopRowNum (len)
  // into a vector of row group indices (0-based). Store the 
  // result in rowGroups_.
  computeRowGroups(startRowNum, stopRowNum);

  startRowNum_ = startRowNum;
  stopRowNum_ = -1;
  currRowNum_ = 0;

  // reset the num IO count for my subclass
  resetNumIOs();

  // expectedRowSize is, by default, the estimated row length 
  // in generator. The estimation is the sum of sizeof(Int32) + y, 
  // where y is the displayLength() of a column selected.
  // It can be overwitten by CQD ORC_READER_ALLOCATE_ROW_LENGTH.
  // Here we allocate a buffer of that size plus 100 bytes extra.
  if ( rowBufferLen_ < expectedRowSize+100 ) {

     NADELETEBASIC(rowBuffer_, heap_);

     rowBufferLen_ = expectedRowSize+100;

     rowBuffer_ = new (heap_) char[rowBufferLen_];
  }

#if DEBUG_SCAN_OPEN
  cout <<  "ExpParquetArrowInterface::scanOpen() called for " <<  fileName_ << endl;
#endif
  
  return rc;
}

int HiveTypeToParquetType(int hiveType)
{
   switch (hiveType) {

     case HIVE_INT_TYPE:
         return parquet::Type::INT32;

     case HIVE_LONG_TYPE:
         return parquet::Type::INT64;

     case HIVE_FLOAT_TYPE:
         return parquet::Type::FLOAT;

     case HIVE_DOUBLE_TYPE:
         return parquet::Type::DOUBLE;

     case HIVE_TIMESTAMP_TYPE:
         return parquet::Type::INT96;

     case HIVE_DECIMAL_TYPE:
     case HIVE_CHAR_TYPE:
     case HIVE_VARCHAR_TYPE:
     case HIVE_STRING_TYPE:
     case HIVE_BINARY_TYPE:
     case HIVE_DATE_TYPE:
     case HIVE_BOOLEAN_TYPE:
     case HIVE_BYTE_TYPE:
     case HIVE_SHORT_TYPE:
     case HIVE_UNKNOWN_TYPE:
     default:
       return -1;
  }
}
     
//
typedef short short8Array[8];


// convert the date-time in Ascii into a short[8] format
//  year (short)
//  month (short)
//  day (short)
//  hour (short)
//  min (short)
//  sec (short)
//  fractional seconds (2xshort)
short 
convertToJulianTimestampShortArray(const OrcSearchArg& pred, int scale, 
                                    short8Array& dateAndTime)
{
  ComDiagsArea* diagsArea = NULL;
  ULng32 flags = 0;

  char tempData[11];
  memset(tempData, 0, sizeof(tempData));

  Int32 dataScale = scale; 

  // first convert the timestamp (in ascii format) to char[11] format
  short rc = 
   ExpDatetime::convAsciiToDatetime((char*)pred.operand(),
                                    pred.operandLen(),
                                    (char*)tempData,
                                    sizeof(tempData),
                                    REC_DATE_YEAR,
                                    REC_DATE_SECOND,
                                    ExpDatetime::DATETIME_FORMAT_NONE,
                                    dataScale,
                                    NULL,
                                    &diagsArea,
				    flags);

  if ( rc != 0 )
    return rc;

  //  Next convert to the short[8]
  char* ptr = tempData;
  dateAndTime[0] = *(short*)ptr; // year
  ptr += sizeof(short);

  dateAndTime[1] = (short)*ptr; // month
  ptr++;

  dateAndTime[2] = (short)*ptr; // day
  ptr++;

  dateAndTime[3] = (short)*ptr; // hour
  ptr++;

  dateAndTime[4] = (short)*ptr; // min
  ptr++;

  dateAndTime[5] = (short)*ptr; // seconds
  ptr++;

  UInt32 fraction = *(UInt32*)ptr;

  if ( scale <= 6 ) {
    dateAndTime[6] = fraction / 1000; // milliseconds
    dateAndTime[7] = fraction % 1000; // microseconds
  } else {

    assert (scale == 9);
    assert (dataScale >= 0);

    // The fraction is store in short[6] and short[7] 
    // as uint32_t in the scale 'scale'. 
    *(UInt32*)(dateAndTime+6) = fraction;  
  }

  return rc;
}

short 
convertToJulianTimestamp(const OrcSearchArg& pred, int scale, int64_t& julianTimestamp)
{
  short8Array dateAndTime;

  short error = convertToJulianTimestampShortArray(pred, scale, dateAndTime);

  // convert the date-time value from short 8 array format to
  // int64_t  format
  julianTimestamp = COMPUTETIMESTAMP(dateAndTime, &error);

  return error;
}

// Convert a timestamp in short 8 format to a Int96 format
// utilizing the formulare from https://en.wikipedia.org/wiki/Julian_day
short
convertShort8ArrayTimestampToTime96(short8Array& source, parquet::Int96& x)
{
  uint16_t year = source[0];
  uint16_t month = source[1];
  uint16_t day = source[2];
  uint16_t hour = source[3];
  uint16_t minute = source[4];
  uint16_t second = source[5];
  uint32_t fraction = *(uint32_t*)(source+6);

  uint32_t jdn = 0;

  if ( year < 1582 ||
       (year == 1582 && month<10) ||
       (year == 1582 && month == 10 && day <= 4) )
  {
     // For Julian calendar date
     jdn = 367 * year -
           (7 * (year + 5001 + (month - 9)/7)) / 4 +
           (275 * month) / 9 + day +
           1729777;
  } else {
     // For Gregorian calendar date
     jdn = (1461 * (year + 4800 + (month - 14)/12))/4 +
           (367 * (month - 2 - 12 * ((month - 14)/12)))/12 -
           (3 * ((year + 4900 + (month - 14)/12)/100))/4 + day - 32075;
  }

  int64_t nanoseconds = hour * 3600LL + minute * 60LL + second;
  nanoseconds *= 1000000000LL; // nanoseconds for hour:minute:second

  // fraction of a second as nanoseconds.
  nanoseconds += fraction;

  x = parquet::Int96(jdn, nanoseconds);

  return 0;
}

short 
convertToJulianTimestamp96(const OrcSearchArg& pred, int scale, parquet::Int96& julianTimestamp)
{
  short8Array dateAndTime;

  short error = 
     convertToJulianTimestampShortArray(pred, scale, dateAndTime);

  // convert the date-time value from short-8 array format to
  // parquet::Int96 format
  if ( !error )
     error = 
       convertShort8ArrayTimestampToTime96(dateAndTime, julianTimestamp);

  return error;
}

// Convert the time literal in pred to unix timestamp in millisecond.
short 
convertToUnixTimestamp(const OrcSearchArg& pred, int scale, int64_t& unixTimestamp)
{
   // The resolution of julianTimestamp is in micro seconds.
   int64_t julianTimestamp = 0;
   short rc = convertToJulianTimestamp(pred, scale, julianTimestamp);

   if ( rc == 0 ) {
     // 210866803200000000 is Unix epoch time in micro seconds since the 
     // start of the Julian time.
     julianTimestamp -= COM_EPOCH_TIMESTAMP;  // in microseconds
     unixTimestamp = julianTimestamp * 1000; // convert to nanosecond
   } else 
     rc = -1;

   return rc;
}

// This function breaks a Juliean timestamp in parquet::Int96 
// (number of Julian days stored in uint32_t and Julian nanoseconds 
// in uint32_t[2]) into a date-time-hour-minute-secconds-fraction
// (stored in an array of 11 bytes). 
//
// The conversion takes the approach of converting to a date in
// Gregorian calander when the number of Julian days component 
// is less than 10/5/1582 12:00:00AM  and to a date in Julian 
// calander otherwwise.  The number of Julian days 
// corresponds to date 10/4/1582 12:00:00AM is 2299160.
//
// Richards' formular to calculate a date/time in either calendars 
// from a value of Julian days is used. The formular can be found at
// https://en.wikipedia.org/wiki/Julian_day.
//

Lng32 
convertTime96TimestampToByte11Array(const parquet::Int96& x, char* target)
{
// Some constants used by the method.
  static constexpr int64_t kMillisecondsInADay = 86400000LL;
  static constexpr int64_t kMicrosecondsInADay = 
                                      kMillisecondsInADay * 1000LL;

  static constexpr int64_t kMicrosecondsInAnHour = 
                                      3600LL * 1000LL * 1000LL;
  static constexpr int64_t kMicrosecondsInAMinute = 60LL * 1000LL * 1000LL;
  static constexpr int64_t kMicrosecondsInASecond = 1000LL * 1000LL;

  static const int64_t y = 4716;
  static const int64_t j = 1401;
  static const int64_t m = 2;
  static const int64_t n = 12;
  static const int64_t r = 4;
  static const int64_t p = 1461;
  static const int64_t v = 3;
  static const int64_t u = 5;
  static const int64_t s = 153;
  static const int64_t w = 2;
  static const int64_t B = 274277;
  static const int64_t C = -38;

  // Julian timestamp in # of days 
  int64_t jd = (int64_t)(x.value[2]);

  // All steps are the same for either the Gregorian or Julian calendar
  // except the step coded in IF block below.
  int64_t f = jd + j;

  if ( jd >= 2299160 ) {
    // for Gregorian calandar
    f += (((( (jd<<2) + B) / 146097) * 3) >> 2) + C; 
  }
  
  int64_t e = r * f + v;
  int64_t g = (e % p) / r;
  int64_t h = u * g + w;

  int64_t month = (char)(((h / s) + m)  % n + 1);
  int64_t day = (char)((h % s ) / u + 1);
  short year = (e / p) - y + (n + m - month) / n;

  // The fractional part in ns
  int64_t nanoseconds = *(reinterpret_cast<const int64_t*>(&(x.value)));
  int64_t milliseconds = nanoseconds / 1000;
  int32_t hour = milliseconds / kMicrosecondsInAnHour;

  // A fast way to compute milliseconds % kMicrosecondsInAnHour;
  int32_t leftOver = milliseconds - hour * kMicrosecondsInAnHour;

  int32_t minute = leftOver / kMicrosecondsInAMinute;

  // A fast way to compute leftOver %= kMicrosecondsInAMinute;
  leftOver -= minute * kMicrosecondsInAMinute;

  int32_t second = leftOver / kMicrosecondsInASecond;

  // A fast way to compute leftOver %= kMicrosecondsInASecond;
  leftOver -= second * kMicrosecondsInASecond;

  int32_t ms = leftOver / 1000;

  // A fast way to compute us = leftOver % 1000;
  int32_t us = leftOver - ms * 1000;

  str_cpy_all(target, (char *) &year, sizeof(year));
  target += sizeof(year);
  *target++ = (char)month;
  *target++ = (char)day;

  *target++ = (char)hour;
  *target++ = (char)minute;
  *target++ = (char)second;

  uint32_t fraction = (ms * 1000 + us) * 1000 + 
                      // A fast way to compute nanoseconds % 1000
                      (nanoseconds - milliseconds * 1000);

  str_cpy_all(target, (char *) &fraction, sizeof(fraction));

  return 11;
}


#undef DEBUG_FILTER_SETUP 
//#define DEBUG_FILTER_SETUP 1

#define CONDITIONAL_GEN_BLOOMFILTER(x, y, data, len) \
           if (x == FilterType::VALUE)  \
             filter = ValueFilter::MakeBloomFilterEval(y, data, len)

ParquetFilter* MakeFilterForBloomFilter(
              const EncodedHiveType& colType, 
              const OrcSearchArg& pred, FilterType ftype,
              const char* data, int len)
{
   ParquetFilter* filter = NULL;
   switch (colType.typeCode()) {
        // Page filters work against data types native to Parquet format,
        // such as Boolean, int32, int64, int96, float, double, 
        // byte_array and fixed_len_byte_array, we can only supply 
        // such types (in C++) to any page filters.
        
        case HIVE_BOOLEAN_TYPE:
          { CONDITIONAL_GEN_BLOOMFILTER(ftype, bool(1), data, len); }
          break;
        case HIVE_BYTE_TYPE:
#if 0
          { CONDITIONAL_GEN_BLOOMFILTER(ftype, int8_t(1), data, len); }
#else
          { CONDITIONAL_GEN_BLOOMFILTER(ftype, int(1), data, len); }
#endif
          break;
        case HIVE_SHORT_TYPE:
          { CONDITIONAL_GEN_BLOOMFILTER(ftype, short(1), data, len); }
          break;
        case HIVE_INT_TYPE:
          { CONDITIONAL_GEN_BLOOMFILTER(ftype, int(1), data, len); }
          break;
        case HIVE_LONG_TYPE:
          { CONDITIONAL_GEN_BLOOMFILTER(ftype, int64_t(1), data, len); }
          break;
        case HIVE_FLOAT_TYPE:
          { CONDITIONAL_GEN_BLOOMFILTER(ftype, float(1), data, len); }
          break;
        case HIVE_DOUBLE_TYPE:
          { CONDITIONAL_GEN_BLOOMFILTER(ftype, double(1), data, len); }
          break;

        case HIVE_DATE_TYPE: 
          { }
          break;

        case HIVE_CHAR_TYPE: 
        case HIVE_VARCHAR_TYPE: 
        case HIVE_STRING_TYPE: 
          {
           std::string dummy("");
           CONDITIONAL_GEN_BLOOMFILTER(ftype, dummy, data, len);
          }
          break;

        case HIVE_TIMESTAMP_TYPE: 
          {
           switch ( colType.scale() ) 
           {
             case 9:
              {
                parquet::Int96 value;
                if ( ftype == FilterType::VALUE &&
                     convertToJulianTimestamp96(pred, colType.scale(), value) == 0 )
                  CONDITIONAL_GEN_BLOOMFILTER(ftype, value, data, len); 
                break;
              }

             default:
              {
              int64_t value = 0;
              if ( ftype == FilterType::VALUE &&
                   convertToUnixTimestamp(pred, colType.scale(), value) == 0 )
                CONDITIONAL_GEN_BLOOMFILTER(ftype, value, data, len); 
              }
           }
          break;
          }

        default:
          break;
   }

   if ( filter ) {
      filter->setRowsToSurvive(pred.getExpectedRows());
   }

   return filter;
}

#define CONDITIONAL_GEN_ISNOTNULL(x, y) \
           if (x == FilterType::VALUE)  \
             filter = ValueFilter::MakeIsNotNull(y)

ParquetFilter* MakeFilterForIsNotNull(const EncodedHiveType& colType, const OrcSearchArg& pred, FilterType ftype)
{
   ParquetFilter* filter = NULL;
   switch (colType.typeCode()) {
        case HIVE_BOOLEAN_TYPE:
          { CONDITIONAL_GEN_ISNOTNULL(ftype, bool(1)); }
          break;
        case HIVE_BYTE_TYPE:
#if 0
          { CONDITIONAL_GEN_ISNOTNULL(ftype, int8_t(1)); }
#else
          { CONDITIONAL_GEN_ISNOTNULL(ftype, int(1)); }
#endif
          break;
        case HIVE_SHORT_TYPE:
          { CONDITIONAL_GEN_ISNOTNULL(ftype, short(1)); }
          break;
        case HIVE_INT_TYPE:
          { CONDITIONAL_GEN_ISNOTNULL(ftype, int(1)); }
          break;
        case HIVE_LONG_TYPE:
          { CONDITIONAL_GEN_ISNOTNULL(ftype, (int64_t)(1)); }
          break;
        case HIVE_FLOAT_TYPE:
          { CONDITIONAL_GEN_ISNOTNULL(ftype, (float)(1)); }
          break;
        case HIVE_DOUBLE_TYPE:
          { CONDITIONAL_GEN_ISNOTNULL(ftype, (double)(1)); }
          break;

        case HIVE_DATE_TYPE: 
          { }
          break;

        case HIVE_CHAR_TYPE: 
        case HIVE_VARCHAR_TYPE: 
        case HIVE_STRING_TYPE: 
          {
           std::string dummy("");
           CONDITIONAL_GEN_ISNOTNULL(ftype, dummy);
          }
          break;

        case HIVE_TIMESTAMP_TYPE: 
          {
           switch ( colType.scale()  ) 
           {
             case 9:
              {
                parquet::Int96 value;
                if ( ftype == FilterType::VALUE &&
                     convertToJulianTimestamp96(pred, colType.scale(), value) == 0 )
                  CONDITIONAL_GEN_ISNOTNULL(ftype, value);
                break;
              }

             default:
              {
              int64_t value= 0;
              if ( ftype == FilterType::VALUE &&
                   convertToUnixTimestamp(pred, colType.scale(), value) == 0 )
                CONDITIONAL_GEN_ISNOTNULL(ftype, value);
              }
           }
          }
          break;

        default:
          break;
   }
   return filter;
}

#define CONDITIONAL_GEN_ISNULL(x, y) \
           if (x == FilterType::VALUE)  \
             filter = ValueFilter::MakeIsNull(y)

ParquetFilter* MakeFilterForIsNull(const EncodedHiveType& colType, const OrcSearchArg& pred, FilterType ftype)
{
   ParquetFilter* filter = NULL;
   switch (colType.typeCode()) {
        case HIVE_BOOLEAN_TYPE:
          { CONDITIONAL_GEN_ISNULL(ftype, bool(1)); }
          break;
        case HIVE_BYTE_TYPE:
#if 0
          { CONDITIONAL_GEN_ISNULL(ftype, int8_t(1)); }
#else
          { CONDITIONAL_GEN_ISNULL(ftype, int(1)); }
#endif
          break;
        case HIVE_SHORT_TYPE:
          { CONDITIONAL_GEN_ISNULL(ftype, short(1)); }
          break;
        case HIVE_INT_TYPE:
          { CONDITIONAL_GEN_ISNULL(ftype, int(1)); }
          break;
        case HIVE_LONG_TYPE:
          { CONDITIONAL_GEN_ISNULL(ftype, (int64_t)(1)); }
          break;
        case HIVE_FLOAT_TYPE:
          { CONDITIONAL_GEN_ISNULL(ftype, (float)(1)); }
          break;
        case HIVE_DOUBLE_TYPE:
          { CONDITIONAL_GEN_ISNULL(ftype, (double)(1)); }
          break;

        case HIVE_DATE_TYPE: 
          { }
          break;

        case HIVE_CHAR_TYPE: 
        case HIVE_VARCHAR_TYPE: 
        case HIVE_STRING_TYPE: 
          {
           std::string dummy("");
           CONDITIONAL_GEN_ISNULL(ftype, dummy);
          }
          break;

        case HIVE_TIMESTAMP_TYPE: 
          {
           switch ( colType.scale()  ) 
           {
             case 9:
              {
                parquet::Int96 value;
                if ( ftype == FilterType::VALUE &&
                     convertToJulianTimestamp96(pred, colType.scale(), value) == 0 )
                  CONDITIONAL_GEN_ISNULL(ftype, value);
                break;
              }

             default:
              {
              int64_t value= 0;
              if ( ftype == FilterType::VALUE &&
                   convertToUnixTimestamp(pred, colType.scale(), value) == 0 )
                CONDITIONAL_GEN_ISNULL(ftype, value);
              }
           }
          }
          break;

        default:
          break;
   }
   return filter;
}

#define CONDITIONAL_GEN_EQUAL(x, y) \
           if (x == FilterType::PAGE)  \
             filter = PageFilter::MakeRange(y, y); \
           else if (x == FilterType::VALUE)  \
             filter = ValueFilter::MakeEQ(y)

#define CONDITIONAL_GEN_EQUAL2(x, y, z) \
           if (x == FilterType::PAGE)  \
             filter = PageFilter::MakeRange(y, y); \
           else if (x == FilterType::VALUE)  \
             filter = ValueFilter::MakeEQ(z)

ParquetFilter* MakeFilterForEqual(const EncodedHiveType& colType, const OrcSearchArg& pred, FilterType ftype)
{
   ParquetFilter* filter = NULL;
   switch (colType.typeCode()) {
        case HIVE_BOOLEAN_TYPE:
          {
           bool max_bool = pred.checkOperandAsBool(TRUE);
           int max_int = (max_bool)? 1 : 0;

           CONDITIONAL_GEN_EQUAL2(ftype, max_int, max_bool);
          }
          break;
        case HIVE_BYTE_TYPE:
          {
           int max_int = std::stoi(pred.operandAsStdstring());
#if 0
           int8_t max_byte = (int8_t)max_int;
           CONDITIONAL_GEN_EQUAL2(ftype, max_int, max_byte);
#else
           // Assume BYTE (aka tinyint) is stored as 
           // int32 inside a Parquet file
           CONDITIONAL_GEN_EQUAL(ftype, max_int);
#endif
          }
          break;
        case HIVE_SHORT_TYPE:
          {
           int max_int = std::stoi(pred.operandAsStdstring());
           short max_short = (short)max_int;
           CONDITIONAL_GEN_EQUAL2(ftype, max_int, max_short);
          }
          break;
        case HIVE_INT_TYPE:
          {
           int max = std::stoi(pred.operandAsStdstring());
           CONDITIONAL_GEN_EQUAL(ftype, max);
          }
          break;
        case HIVE_LONG_TYPE:
          {
           int64_t max = std::stol(pred.operandAsStdstring());
           CONDITIONAL_GEN_EQUAL(ftype, max);
          }
          break;
        case HIVE_FLOAT_TYPE:
          {
           float max = std::stof(pred.operandAsStdstring());
           CONDITIONAL_GEN_EQUAL(ftype, max);
          }
          break;
        case HIVE_DOUBLE_TYPE:
          {
           double max = std::stod(pred.operandAsStdstring());
           CONDITIONAL_GEN_EQUAL(ftype, max);
          }
          break;
        case HIVE_TIMESTAMP_TYPE:
          {
           // only construct a value filter for TIMESTAMP, since
           // stats is not stored in a page's metadata.
           switch ( colType.scale()  ) 
           {
             case 9:
              {
                parquet::Int96 max;
                if ( ftype == FilterType::VALUE &&
                     convertToJulianTimestamp96(pred, colType.scale(), max) == 0 )
                  filter = ValueFilter::MakeEQ(max);
                break;
              }

             default:
              {
              int64_t max = 0;
              if ( ftype == FilterType::VALUE &&
                   convertToUnixTimestamp(pred, colType.scale(), max) == 0 )
                filter = ValueFilter::MakeEQ(max);
              }
           }
          }
          break;

        case HIVE_DATE_TYPE: 
          {
          }
          break;

        case HIVE_CHAR_TYPE: 
        case HIVE_VARCHAR_TYPE: 
        case HIVE_STRING_TYPE: 
          {
           std::string max = pred.operandAsStdstring();
           CONDITIONAL_GEN_EQUAL(ftype, max);
          }
          break;

        default:
          break;
   }
   return filter;
}

#define CONDITIONAL_GEN_LESSTHAN(x, y) \
           if (x == FilterType::PAGE)  \
             filter = PageFilter::MakeLessThan(y); \
           else if (x == FilterType::VALUE)  \
             filter = ValueFilter::MakeLessThan(y)

#define CONDITIONAL_GEN_LESSTHAN2(x, y, z) \
           if (x == FilterType::PAGE)  \
             filter = PageFilter::MakeLessThan(y); \
           else if (x == FilterType::VALUE)  \
             filter = ValueFilter::MakeLessThan(z)

ParquetFilter* MakeFilterForLessThan(const EncodedHiveType& colType, const OrcSearchArg& pred, FilterType ftype)
{
   ParquetFilter* filter = NULL;
   switch (colType.typeCode()) {
        case HIVE_BOOLEAN_TYPE:
          {
           bool max_bool = pred.checkOperandAsBool(TRUE);
           int max_int = (max_bool)? 1 : 0;

           CONDITIONAL_GEN_LESSTHAN2(ftype, max_int, max_bool);
          }
          break;
        case HIVE_BYTE_TYPE:
          {
           int max_int = std::stoi(pred.operandAsStdstring());
#if 0
           int8_t max_byte = (int8_t)max_int;
           CONDITIONAL_GEN_LESSTHAN2(ftype, max_int, max_byte);
#else
           CONDITIONAL_GEN_LESSTHAN(ftype, max_int);
#endif
          }
          break;
        case HIVE_SHORT_TYPE:
          {
           int max_int = std::stoi(pred.operandAsStdstring());
           short max_short = (short)max_int;
           CONDITIONAL_GEN_LESSTHAN2(ftype, max_int, max_short);
          }
          break;
        case HIVE_INT_TYPE:
          {
           int max = std::stoi(pred.operandAsStdstring());
           CONDITIONAL_GEN_LESSTHAN(ftype, max);
          }
          break;
        case HIVE_LONG_TYPE:
          {
           int64_t max = std::stol(pred.operandAsStdstring());
           CONDITIONAL_GEN_LESSTHAN(ftype, max);
          }
          break;
        case HIVE_FLOAT_TYPE:
          {
           float max = std::stof(pred.operandAsStdstring());
           CONDITIONAL_GEN_LESSTHAN(ftype, max);
          }
          break;
        case HIVE_DOUBLE_TYPE:
          {
           double max = std::stod(pred.operandAsStdstring());
           CONDITIONAL_GEN_LESSTHAN(ftype, max);
          }
          break;
        case HIVE_TIMESTAMP_TYPE: 
          {
           switch ( colType.scale()  ) 
           {
             case 9:
              {
                parquet::Int96 max;
                if ( ftype == FilterType::VALUE &&
                     convertToJulianTimestamp96(pred, colType.scale(), max) == 0 )
                  filter = ValueFilter::MakeLessThan(max);
                break;
              }

             default:
              {
              int64_t max= 0;
              if ( ftype == FilterType::VALUE &&
                   convertToUnixTimestamp(pred, colType.scale(), max) == 0 )
                filter = ValueFilter::MakeLessThan(max);
              }
           }
          }
          break;

        case HIVE_DATE_TYPE: 
          {
          }
          break;

        case HIVE_CHAR_TYPE: 
        case HIVE_VARCHAR_TYPE: 
        case HIVE_STRING_TYPE: 
          {
           std::string max = pred.operandAsStdstring();
           CONDITIONAL_GEN_LESSTHAN(ftype, max);
          }
          break;

        default:
          break;
   }
   return filter;
}

#define CONDITIONAL_GEN_LESSTHANEQUAL(x, y) \
           if (x == FilterType::PAGE)  \
             filter = PageFilter::MakeLessThanEqual(y); \
           else if (x == FilterType::VALUE)  \
             filter = ValueFilter::MakeLessThanEqual(y)

#define CONDITIONAL_GEN_LESSTHANEQUAL2(x, y, z) \
           if (x == FilterType::PAGE)  \
             filter = PageFilter::MakeLessThanEqual(y); \
           else if (x == FilterType::VALUE)  \
             filter = ValueFilter::MakeLessThanEqual(z)

ParquetFilter* MakeFilterForLessThanEqual(const EncodedHiveType& colType, const OrcSearchArg& pred, FilterType ftype)
{
   ParquetFilter* filter = NULL;
   switch (colType.typeCode()) {
        case HIVE_BOOLEAN_TYPE:
          {
           bool max_bool = pred.checkOperandAsBool(TRUE);
           int max_int = (max_bool)? 1 : 0;

           CONDITIONAL_GEN_LESSTHANEQUAL2(ftype, max_int, max_bool);
          }
          break;
        case HIVE_BYTE_TYPE:
          {
           int max_int = std::stoi(pred.operandAsStdstring());
#if 0
           int8_t max_byte = (int8_t)max_int;
           CONDITIONAL_GEN_LESSTHANEQUAL2(ftype, max_int, max_byte);
#else
           CONDITIONAL_GEN_LESSTHANEQUAL(ftype, max_int);
#endif
          }
          break;
        case HIVE_SHORT_TYPE:
          {
           int max_int = std::stoi(pred.operandAsStdstring());
           short max_short = (short)max_int;
           CONDITIONAL_GEN_LESSTHANEQUAL2(ftype, max_int, max_short);
          }
          break;
        case HIVE_INT_TYPE:
          {
           int max = std::stoi(pred.operandAsStdstring());
           CONDITIONAL_GEN_LESSTHANEQUAL(ftype, max);
          }
          break;
        case HIVE_LONG_TYPE:
          {
           int64_t max = std::stol(pred.operandAsStdstring());
           CONDITIONAL_GEN_LESSTHANEQUAL(ftype, max);
          }
          break;
        case HIVE_FLOAT_TYPE:
          {
           float max = std::stof(pred.operandAsStdstring());
           CONDITIONAL_GEN_LESSTHANEQUAL(ftype, max);
          }
          break;
        case HIVE_DOUBLE_TYPE:
          {
           double max = std::stod(pred.operandAsStdstring());
           CONDITIONAL_GEN_LESSTHANEQUAL(ftype, max);
          }
          break;

        case HIVE_TIMESTAMP_TYPE: 
          {
           switch ( colType.scale()  ) 
           {
             case 9:
              {
                parquet::Int96 max;
                if ( ftype == FilterType::VALUE &&
                     convertToJulianTimestamp96(pred, colType.scale(), max) == 0 )
                  filter = ValueFilter::MakeLessThanEqual(max);
                break;
              }

             default:
              {
              int64_t max= 0;
              if ( ftype == FilterType::VALUE &&
                   convertToUnixTimestamp(pred, colType.scale(), max) == 0 )
                filter = ValueFilter::MakeLessThanEqual(max);
              }
           }
          }
          break;

        case HIVE_DATE_TYPE: 
          {
          }
          break;

        case HIVE_CHAR_TYPE: 
        case HIVE_VARCHAR_TYPE: 
        case HIVE_STRING_TYPE: 
          {
           std::string max = pred.operandAsStdstring();
           CONDITIONAL_GEN_LESSTHANEQUAL(ftype, max);
          }
          break;
        default:
          break;
   }
   return filter;
}


#define CONDITIONAL_GEN_GREATERTHAN(x,y) \
           if (x == FilterType::PAGE) \
              filter = PageFilter::MakeGreaterThan(y); \
           else if (x == FilterType::VALUE) \
              filter = ValueFilter::MakeGreaterThan(y)

#define CONDITIONAL_GEN_GREATERTHAN2(x,y,z) \
           if (x == FilterType::PAGE) \
              filter = PageFilter::MakeGreaterThan(y); \
           else if (x == FilterType::VALUE) \
              filter = ValueFilter::MakeGreaterThan(z)

ParquetFilter* MakeFilterForGreaterThan(const EncodedHiveType& colType, const OrcSearchArg& pred, FilterType ftype)
{
   ParquetFilter* filter = NULL;
   switch ( colType.typeCode() ) {
        case HIVE_BOOLEAN_TYPE:
          {
           bool min_bool = pred.checkOperandAsBool(TRUE);
           int min_int = (min_bool)? 1 : 0;

           CONDITIONAL_GEN_GREATERTHAN2(ftype, min_int, min_bool);
          }
          break;
        case HIVE_BYTE_TYPE:
          {
           int min_int = std::stoi(pred.operandAsStdstring());
#if 0
           int8_t min_byte = (int8_t)min_int;
           CONDITIONAL_GEN_GREATERTHAN2(ftype, min_int, min_byte);
#else
           CONDITIONAL_GEN_GREATERTHAN(ftype, min_int);
#endif
          }
          break;
        case HIVE_SHORT_TYPE:
          {
           int min_int = std::stoi(pred.operandAsStdstring());
           short min_short = (short)min_int;
           CONDITIONAL_GEN_GREATERTHAN2(ftype, min_int, min_short);
          }
          break;
        case HIVE_INT_TYPE:
          {
           int min = std::stoi(pred.operandAsStdstring());
           CONDITIONAL_GEN_GREATERTHAN(ftype, min);
          }
          break;
        case HIVE_LONG_TYPE:
          {
           int64_t min = std::stol(pred.operandAsStdstring());
           CONDITIONAL_GEN_GREATERTHAN(ftype, min);
          }
          break;
        case HIVE_FLOAT_TYPE:
          {
           float min = std::stof(pred.operandAsStdstring());
           CONDITIONAL_GEN_GREATERTHAN(ftype, min);
          }
          break;
        case HIVE_DOUBLE_TYPE:
          {
           double min = std::stod(pred.operandAsStdstring());
           CONDITIONAL_GEN_GREATERTHAN(ftype, min);
          }
          break;

        case HIVE_TIMESTAMP_TYPE: 
          {
           switch ( colType.scale()  ) 
           {
             case 9:
              {
                parquet::Int96 min;
                if ( ftype == FilterType::VALUE &&
                     convertToJulianTimestamp96(pred, colType.scale(), min) == 0 )
                   filter = ValueFilter::MakeGreaterThan(min);    
                break;
              }

             default:
              {
              int64_t min= 0;
              if ( ftype == FilterType::VALUE &&
                   convertToUnixTimestamp(pred, colType.scale(), min) == 0 )
                   filter = ValueFilter::MakeGreaterThan(min);    
              }
           }
          }
          break;
        case HIVE_DATE_TYPE: 
          break;

        case HIVE_CHAR_TYPE: 
        case HIVE_VARCHAR_TYPE: 
        case HIVE_STRING_TYPE: 
          {
           std::string min = pred.operandAsStdstring();
           CONDITIONAL_GEN_GREATERTHAN(ftype, min);
          }
          break;

        default:
          break;
   }
   return filter;
}

#define CONDITIONAL_GEN_GREATERTHANEQUAL(x,y) \
           if (x == FilterType::PAGE) \
              filter = PageFilter::MakeGreaterThanEqual(y); \
           else if (x == FilterType::VALUE) \
              filter = ValueFilter::MakeGreaterThanEqual(y)

#define CONDITIONAL_GEN_GREATERTHANEQUAL2(x,y,z) \
           if (x == FilterType::PAGE) \
              filter = PageFilter::MakeGreaterThanEqual(y); \
           else if (x == FilterType::VALUE) \
              filter = ValueFilter::MakeGreaterThanEqual(z)

ParquetFilter* MakeFilterForGreaterThanEqual(const EncodedHiveType& colType, const OrcSearchArg& pred, FilterType ftype)
{
   ParquetFilter* filter = NULL;
   switch ( colType.typeCode() ) {
        case HIVE_BOOLEAN_TYPE:
          {
           bool min_bool = pred.checkOperandAsBool(TRUE);
           int min_int = (min_bool)? 1 : 0;

           CONDITIONAL_GEN_GREATERTHANEQUAL2(ftype, min_int, min_bool);
          }
          break;
        case HIVE_BYTE_TYPE:
          {
           int min_int = std::stoi(pred.operandAsStdstring());
#if 0
           int8_t min_byte = (int8_t)min_int;
           CONDITIONAL_GEN_GREATERTHANEQUAL2(ftype, min_int, min_byte);
#else
           CONDITIONAL_GEN_GREATERTHANEQUAL(ftype, min_int);
#endif
          }
          break;
        case HIVE_SHORT_TYPE:
          {
           int min_int = std::stoi(pred.operandAsStdstring());
           short min_short = (short)min_int;
           CONDITIONAL_GEN_GREATERTHANEQUAL2(ftype, min_int, min_short);
          }
          break;
        case HIVE_INT_TYPE:
          {
           int min = std::stoi(pred.operandAsStdstring());
           CONDITIONAL_GEN_GREATERTHANEQUAL(ftype, min);
          }
          break;
        case HIVE_LONG_TYPE:
          {
           int64_t min = std::stol(pred.operandAsStdstring());
           CONDITIONAL_GEN_GREATERTHANEQUAL(ftype, min);
          }
          break;
        case HIVE_FLOAT_TYPE:
          {
           float min = std::stof(pred.operandAsStdstring());
           CONDITIONAL_GEN_GREATERTHANEQUAL(ftype, min);
          }
          break;
        case HIVE_DOUBLE_TYPE:
          {
           double min = std::stod(pred.operandAsStdstring());
           CONDITIONAL_GEN_GREATERTHANEQUAL(ftype, min);
          }
          break;

        case HIVE_TIMESTAMP_TYPE: 
          {
           switch ( colType.scale()  ) 
           {
             case 9:
              {
                parquet::Int96 min;
                if ( ftype == FilterType::VALUE &&
                     convertToJulianTimestamp96(pred, colType.scale(), min) == 0 )
                  filter = ValueFilter::MakeGreaterThanEqual(min);    
                break;
              }

             default:
              {
              int64_t min= 0;
              if ( ftype == FilterType::VALUE &&
                   convertToUnixTimestamp(pred, colType.scale(), min) == 0 )
                filter = ValueFilter::MakeGreaterThanEqual(min);    
              }
           }
          }
          break;

        case HIVE_DATE_TYPE: 
          break;

        case HIVE_CHAR_TYPE: 
        case HIVE_VARCHAR_TYPE: 
        case HIVE_STRING_TYPE: 
          {
           std::string min = pred.operandAsStdstring();
           CONDITIONAL_GEN_GREATERTHANEQUAL(ftype, min);
          }
          break;

        default:
          break;
   }
   return filter;
}

#define CONDITIONAL_GEN_RANGE(x, y, z) \
        if (x == FilterType::PAGE)  \
           filter = PageFilter::MakeRange(y, z); \
        else if (x == FilterType::VALUE)  \
           filter = ValueFilter::MakeRange(y, z)

#define CONDITIONAL_GEN_RANGE2(x, y, z, u, v) \
        if (x == FilterType::PAGE)  \
           filter = PageFilter::MakeRange(y, z); \
        else if (x == FilterType::VALUE)  \
           filter = ValueFilter::MakeRange(u, v)

ParquetFilter* MakeFilterForRange(const EncodedHiveType& colType, 
                               const OrcSearchArg& greaterThanPred,
                               const OrcSearchArg& lessThanPred,
                               FilterType ftype)
{
   ParquetFilter* filter = NULL;
   switch ( colType.typeCode() ) {
        case HIVE_BOOLEAN_TYPE:
          {
           bool min_bool = greaterThanPred.checkOperandAsBool(TRUE);
           bool max_bool = lessThanPred.checkOperandAsBool(TRUE);

           int min_int = (min_bool)? 1 : 0;
           int max_int = (max_bool)? 1 : 0;

           CONDITIONAL_GEN_RANGE2(ftype, min_int, max_int, min_bool, max_bool);
          }
          break;
        case HIVE_BYTE_TYPE:
          {
           int min_int = std::stoi(greaterThanPred.operandAsStdstring());
           int max_int = std::stoi(lessThanPred.operandAsStdstring());
#if 0
           int8_t max_byte = (int8_t)max_int;
           int8_t min_byte = (int8_t)min_int;
           CONDITIONAL_GEN_RANGE2(ftype, min_int, max_int, min_byte, max_byte);
#else
#endif
           CONDITIONAL_GEN_RANGE(ftype, min_int, max_int);
          }
          break;
        case HIVE_SHORT_TYPE:
          {
           int min_int = std::stoi(greaterThanPred.operandAsStdstring());
           int max_int = std::stoi(lessThanPred.operandAsStdstring());
           short max_short = (short)max_int;
           short min_short = (short)min_int;
           CONDITIONAL_GEN_RANGE2(ftype, min_int, max_int, min_short, max_short);
          }
          break;
        case HIVE_INT_TYPE:
          {
           int min = std::stoi(greaterThanPred.operandAsStdstring());
           int max = std::stoi(lessThanPred.operandAsStdstring());
           CONDITIONAL_GEN_RANGE(ftype, min, max);
          }
          break;
        case HIVE_LONG_TYPE:
          {
           int64_t min = std::stol(greaterThanPred.operandAsStdstring());
           int64_t max = std::stol(lessThanPred.operandAsStdstring());
           CONDITIONAL_GEN_RANGE(ftype, min, max);
          }
          break;
        case HIVE_FLOAT_TYPE:
          {
           float min = std::stof(greaterThanPred.operandAsStdstring());
           float max = std::stof(lessThanPred.operandAsStdstring());
           CONDITIONAL_GEN_RANGE(ftype, min, max);
          }
          break;
        case HIVE_DOUBLE_TYPE:
          {
           double min = std::stod(greaterThanPred.operandAsStdstring());
           double max = std::stod(lessThanPred.operandAsStdstring());
           CONDITIONAL_GEN_RANGE(ftype, min, max);
          }
          break;
        case HIVE_TIMESTAMP_TYPE:
          {

           // Only generate it for value typed filter, since
           // there is no page stats available for timestamps.
           //
           switch ( colType.scale()  ) 
           {
             case 9:
              {
                parquet::Int96 min;
                parquet::Int96 max;
                if ( ftype == FilterType::VALUE &&
                     convertToJulianTimestamp96(greaterThanPred, colType.scale(), min) == 0  &&
                     convertToJulianTimestamp96(lessThanPred, colType.scale(), max) == 0 )
                   filter = ValueFilter::MakeRange(min, max);
                break;
              }

             default:
              {
              int64_t min = 0;
              int64_t max = 0;

              if (ftype == FilterType::VALUE &&
                  convertToUnixTimestamp(greaterThanPred, colType.scale(), min) == 0 &&
                  convertToUnixTimestamp(lessThanPred, colType.scale(), max) == 0 )
              {
                   filter = ValueFilter::MakeRange(min, max);
              }
              }
              break;
             }
          }
          break;
        case HIVE_DATE_TYPE: 
          break;
        case HIVE_CHAR_TYPE: 
        case HIVE_VARCHAR_TYPE: 
        case HIVE_STRING_TYPE: 
          {
           std::string min = greaterThanPred.operandAsStdstring();
           std::string max = lessThanPred.operandAsStdstring();
           CONDITIONAL_GEN_RANGE(ftype, min, max);
          }
          break;
        default:
          break;
   }
   return filter;
}

NABoolean
findPredicatesForColumn(const std::string& colName,
                        OrcSearchArgVec& predVec,
                        OrcSearchArgVec& result)
{
   int numPreds = predVec.size();

   if ( numPreds < 2 || 
        !predVec[0].isSTART_AND() || !predVec[numPreds-1].isEND() )
     return FALSE;

   for (Int32 i=1; i<=numPreds-2; i++) 
   {
      const OrcSearchArg& pred = predVec[i];

      if ( pred.isSimplePushdownPredicate() ) {
        if ( pred.matchColName(colName) ) {
           result.push_back(pred);
        }
        continue;
      } else
      if ( pred.isBloomFilterPredicate() )  {
        if ( pred.matchColName(colName) ) {
           result.push_back(pred);
        }
        continue;
      } else
      if ( pred.isSTART_NOT() ) 
      {
         i++;
         if ( i<=numPreds-2 ) {
            OrcSearchArg& predNext = predVec[i];

            if ( predNext.isSimplePushdownPredicate() ) {
               if ( predNext.matchColName(colName) ) 
               {
                 OrcSearchArg copy(predVec[i]);
                 copy.reverseOperatorTypeForNot();
                 result.push_back(copy);
               }
            } else {
               if ( !predNext.isNullPredicate() ) 
                  return FALSE;
            }
         }
            
         if ( i<numPreds-2 ) {
            OrcSearchArg& endMark = predVec[i+1];
            if ( endMark.isEND() )
              i++;
            else
              return FALSE;
          } else
              return FALSE;
      } else 
      if ( !pred.isNullPredicate() )
         return FALSE;
    }
    return TRUE;
}

void*
ExpParquetArrowInterface::buildFilter(
            OrcSearchArgVec& preds, maskVecType& masks, 
            const EncodedHiveType& colType, 
            FilterType ftype, Lng32& errorCode)
{
   errorCode = OFR_OK;

   ParquetFilter* firstFilter = NULL;
   ParquetFilter* lastFilter = NULL;
   ParquetFilter* filter = NULL;

   for ( int i=0; i<preds.size(); i++ )  
   {
     filter = NULL;
     const OrcSearchArg& pred = preds[i];

     bool combined = false;
     if ( i<preds.size()-1 ) 
     {
        const OrcSearchArg& pred2 = preds[i+1];

        // Combine
        //    pred: <col> </<= <value>
        //    pred2: <col> >/>= <value>
        // into a range predicate: (pred2.data(), pred.data())
        if ( pred.isLessThanEqualPred() && 
             pred2.isGreaterThanEqualPred() ) 
        {
           filter = MakeFilterForRange(colType, pred2, pred, ftype);
           combined = true;

        } else

        // Combine
        //    pred2: <col> </<= <value>
        //    pred: <col> >/>= <value>
        // into a range predicate: (pred2.data(), pred.data())
        if ( pred2.isLessThanEqualPred() && 
             pred.isGreaterThanEqualPred() ) 
        {
           filter = MakeFilterForRange(colType, pred, pred2, ftype);
           combined = true;
        } 

        //
        // Setup masks when ftype is PAGE so that a corresponding 
        // value filter can skip a span when the page filter determines
        // the entire page is contained within the range, or 
        // page_filter->compare(page) = IN_RANGE. 
        //
        // For example, for a page X of stats [0, 100] and a range page 
        // filter predicate P of -100 <= min <= 100, X is entirely within 
        // the boundary of P.
        //
        // For a different range page filter predicate Q
        // of 10 <= min <= 200, X is not in range of Q. Rather it overlaps 
        // with the boundary of Q.
        //
        if ( combined ) {
          switch ( ftype ) {
            case PAGE:
              masks[i] = true;
              masks[i+1]= true;
              break;
            case VALUE:
              ValueFilter* vFilter = dynamic_cast<ValueFilter*>(filter);
              vFilter->setSkipInRangeSpan(true);
              break;
          }

          i++;
        }
     } // i is less than preds.size() - 1

     if ( !combined ) {

        bool canBePageFilter = true;

        switch (pred.getOperatorType())
        {
          case OrcSearchArg::BLOOMFILTER:
            canBePageFilter = false;
            if ( ftype == VALUE )     
              filter = MakeFilterForBloomFilter(
                           colType, pred, ftype,
                           pred.operand(), pred.operandLen()
                                               );
            break;
   
          case OrcSearchArg::ISNULL:
            canBePageFilter = false;
            if ( ftype == VALUE )     
              filter = MakeFilterForIsNull(colType, pred, ftype);
            break;
   
          case OrcSearchArg::ISNOTNULL:
            canBePageFilter = false;
            if ( ftype == VALUE )     
              filter = MakeFilterForIsNotNull(colType, pred, ftype);
            break;
   
          case OrcSearchArg::EQUALS:
            filter = MakeFilterForEqual(colType, pred, ftype);
            break;
   
          case OrcSearchArg::LESSTHAN:
            filter = MakeFilterForLessThan(colType, pred, ftype);
            break;
   
          case OrcSearchArg::LESSTHANEQUALS:
            filter = MakeFilterForLessThanEqual(colType, pred, ftype);
            break;
   
          case OrcSearchArg::GREATERTHAN:
            filter = MakeFilterForGreaterThan(colType, pred, ftype);
            break;
   
          case OrcSearchArg::GREATERTHANEQUALS:
            filter = MakeFilterForGreaterThanEqual(colType, pred, ftype);
            break;
   
          default:
           setColNameForError(pred.colName(), pred.colNameLen());
           errorCode = -OFR_ERROR_ARROW_READER_PUSHDOWN_PRED;
           return NULL;
       }

       if ( !filter && ftype == VALUE) {
         setColNameForError(pred.colName(), pred.colNameLen());
         errorCode = -OFR_ERROR_ARROW_READER_PUSHDOWN_PRED;
         delete filter;
         return NULL;
       } 

       switch ( ftype ) {
         case PAGE:
           if ( canBePageFilter )
             masks[i] = true;
           break;
         case VALUE:
           if ( masks[i] ) {
             ValueFilter* vFilter = dynamic_cast<ValueFilter*>(filter);
             vFilter->setSkipInRangeSpan(true);
           }
           break;
       }
    } // !combined case

    if ( filter ) {

      filter->setFilterId(pred.getFilterId());

      if ( lastFilter ) {
         lastFilter->setNext(filter);
         lastFilter = filter;
      } else {
         firstFilter = lastFilter = filter;
      }
    }

  } // for loop

  return firstFilter;
}

      
Int32 findColumnIndex(const std::string& colName, TextVec* colNameVec)
{
   for (Int32 i=0; i<colNameVec->size(); i++) {
      if ( colName.compare((*colNameVec)[i]) == 0 )
        return i;
   }
   return -1;
}

Lng32 
ExpParquetArrowInterface::setupFilters(
             const char* tableName, const char* fileName,
             TextVec* ppiVec, TextVec *colNameVec, TextVec *colTypeVec,
             std::vector<UInt64>* expectedRowsVec
                                      )
{
   if ( !colTypeVec ) 
     return OFR_OK;

   // setup the filters when it is for a scan of a table.
   if ( tableName_.compare(tableName) ) {

     // A new table
     tableName_ = tableName;

#ifdef DEBUG_FILTER_SETUP
     std::cout << "Filters in existence to be removed:" << std::endl;
     char msg[50];
  
     for (int i=0; i<filters_.size(); i++ ) {
        if ( filters_[i].canFilter() ) {
           sprintf(msg, "filters_[%d]=", i);
           filters_[i].display(msg);
        }
     }
#endif
  
     // swap out the current filters so that each can be 
     // deleted when prevFilters is out of the current scope.
     std::vector<Filters> prevFilters; 
     filters_.swap(prevFilters);
  
     // Allocate # column number of filters
     filters_.clear();
     filters_.resize(colNameVec->size());
  
     // find unique col names that are associated with some
     // predicates in colTypeVec.
     OrcSearchArgVec source;

     // expected rows vec is populated for every bloom filter only
     // and in sync with the order of appearance of all bloom filters.
     int rowsVecIdx = 0; 

     for (Int32 i=0; i<ppiVec->size(); i++ ) 
     {
        const Text& data = ((*ppiVec)[i]);
        OrcSearchArg pred(data);

        if ( pred.isBloomFilterPredicate() && expectedRowsVec ) {
          const char* colName = pred.colName();
          pred.setExpectedRows((*expectedRowsVec)[rowsVecIdx++]);
        }

        source.push_back(pred);
     }
  
     // Collect unique colNames with predicates via std::set.
     std::set<std::string> uniqueColNames;
     for (Int32 i=0; i<source.size(); i++ ) 
     {
        const OrcSearchArg& pred = source[i];
        if ( pred.isSimplePushdownPredicate() || pred.isBloomFilterPredicate()) 
        {
           std::string colName(pred.colName(), pred.colNameLen());
           uniqueColNames.insert(colName);
        } 
     }
  
     // for each column, setup the page/value filter, if the supporting data
     // is available
     std::set<std::string>::iterator it;
     for (it=uniqueColNames.begin(); it!=uniqueColNames.end(); ++it) {
  
        const std::string& colName = *it;
        int k = findColumnIndex(colName, colNameVec);

        assert (k>=0);
  
        EncodedHiveType colType((*colTypeVec)[k]);
  
        OrcSearchArgVec preds;
        if ( !findPredicatesForColumn(colName, source, preds) )
        {
           setColNameForError(colName);
           return OFR_ERROR_ARROW_READER_PUSHDOWN_PRED;
        }
  
        Lng32 error = OFR_OK;

        maskVecType masks(preds.size(), false);

        PageFilter* pFilter = 
              (PageFilter*)buildFilter(preds, masks, colType, PAGE, error);
  
        if ( error != OFR_OK )
          return error;
  
        ValueFilter* vFilter = 
              (ValueFilter*)buildFilter(preds, masks, colType, VALUE, error);
  
        if ( error != OFR_OK )
          return error;

        filters_[k].setPageFilter(pFilter);
        filters_[k].setValueFilter(vFilter);
     }

#ifdef DEBUG_FILTER_SETUP
     cout << "Final filters to use:" << endl;
     for (int i=0; i<filters_.size(); i++ ) {
        if ( filters_[i].canFilter() ) {
           sprintf(msg, "filters_[%d]=", i);
           filters_[i].display(msg);
        }
     }
#endif
   } // end of need to setup filters

   // Reset counters, if any, for the filters.
   for (int i=0; i<filters_.size(); i++ ) {
      if ( filters_[i].canFilter() ) {
         filters_[i].resetCounters();
      }
   }

   reader_->setFilters(&filters_);

   return OFR_OK;
}

Lng32 ExpParquetArrowInterface::advanceToNextRow()
{
  if ( rowGroups_.size() == 0 ||
       startRowNum_ == -1 ||
       ((stopRowNum_ != -1) && (currRowNum_ > stopRowNum_))
     )
    return 100;

  Lng32 rc = prefetchIfNeeded();
  if ( rc != OFR_OK )
    return rc;

  if ( noMoreData() ) {
    return 100;
  }

  int64_t newPos = 0;
  selectedBitsReader_.NextBitSet(newPos);

#if DEBUG_COLUMN_VISITOR
  cout << "pid=" << getpid()
       << ", newPos=" <<  newPos 
       << ", remainingRowsToRead_=" << remainingRowsToRead_
       << endl;
#endif


  // Advance to the row to read.
  visitor_->advance(newPos);

  return 0;
}

void ExpParquetArrowInterface::doneNextRow(Lng32 totalRowLen)
{
  remainingRowsToRead_--;
  currRowNum_++;

  if (hss_) {
    hss_->incBytesRead(totalRowLen);
  }
}

Lng32 ExpParquetArrowInterface::scanFetch(
                                 char** row,
                                 Int64 &rowLen, Int64 &rowNum,
                                 Lng32 &numCols,
				 ExHdfsScanStats *hss
				 )
{
  Lng32 rc = nextRow();

  if ( rc ) 
    return rc; // no more data or error

  // Assemble a row from the data in result set, which is an arrow
  // table from which column groups can be fetched (as columns).
  rc = visitor_->convertToTrafodion(rowBuffer_, rowLen, colName_);

  doneNextRow(rowLen);

  rowNum = currRowNum_;

  *row = rowBuffer_;
  numCols = columnsToReturn_.size();

  return rc;
}

void ExpParquetArrowInterface::releaseResourcesForCurrentBatch()
{
   delete visitor_; 
   visitor_ = NULL;

   delete mergedSelectedBits_;
   mergedSelectedBits_ = NULL;
}

void ExpParquetArrowInterface::releaseResources()
{
   ExpParquetArrowInterface::releaseResourcesForCurrentBatch();

   if ( reader_ )
     reader_.reset();       

   NADELETEBASIC(rowBuffer_, heap_);
   rowBuffer_ = NULL;
   rowBufferLen_ = 0;
}

void ExpParquetArrowInterfaceBatch::releaseResourcesForCurrentBatch()
{
   if ( batch_ )
      batch_.reset();       // no more use of batch_

   // release resources for the base class
   ExpParquetArrowInterface::releaseResourcesForCurrentBatch();
}

void ExpParquetArrowInterfaceBatch::releaseResources()
{
   if ( batchReader_ )
      batchReader_.reset(); // no more use of batchReader_

   // release other resources 
   releaseResourcesForCurrentBatch();

   ExpParquetArrowInterface::releaseResources();
}

void ExpParquetArrowInterfaceTable::releaseResources()
{
   if ( resultSet_ )
      resultSet_.reset();

   // release resources for the base class
   ExpParquetArrowInterface::releaseResources();
}

Lng32 ExpParquetArrowInterface::scanClose()
{
#if DEBUG_SCAN_OPEN 
  cout <<  "ExpParquetArrowInterface::scanClose() called" << endl;
#endif

   if (hss_) {
     hss_->incAccessedRows(totalRowsToAccess_);
     hss_->incFilteredRows(totalRowsToAccess_ - totalRowsToRead_);
     hss_->incHdfsCalls(numIOs());
     updateStatsForScanFilters();
   }

   releaseResources();
   return 0;
}

Lng32 ExpParquetArrowInterfaceTable::scanClose()
{
   releaseResources();
   return ExpParquetArrowInterface::scanClose();
}

Lng32 ExpParquetArrowInterfaceBatch::scanClose()
{
   releaseResources();
   return ExpParquetArrowInterface::scanClose();
}

char * ExpParquetArrowInterface::getErrorText(Lng32 errEnum)
{
   if ( errEnum < 0 )
      errEnum *= (-1);

   switch ( errEnum ) {
      case OFR_ERROR_ARROW_READER_CONNECT:
      case OFR_ERROR_ARROW_READER_DISCONNECT:
      case OFR_ERROR_ARROW_READER_OPEN_READABLE:
      case OFR_ERROR_ARROW_READER_OPEN_FILE:
      case OFR_ERROR_ARROW_READER_READTABLE:
      case OFR_ERROR_ARROW_READER_READ_NEXT_BATCH:
        {
          std::string msg = arrowStatus_.ToString();
          snprintf(errorMsgBuffer_, sizeof(errorMsgBuffer_), "Arrow Reader error: %s for table %s (data file=%s)", msg.c_str(), tableName_.c_str(), fileName_.c_str());
          break;
        }
      case OFR_ERROR_ARROW_READER_PUSHDOWN_PRED:
        {
          snprintf(errorMsgBuffer_, sizeof(errorMsgBuffer_),
                    "Failed to process a pushdown predicate for table %s on column %s", tableName_.c_str(), colName_.c_str());
          break;
        }
      case OFR_ERROR_ARROW_READER_GET_BATCH_READER:
        {
          snprintf(errorMsgBuffer_, sizeof(errorMsgBuffer_),
                    "Failed to obtain a batch reader for table %s and data file %s", tableName_.c_str(), fileName_.c_str());
          break;
        }
      case OFR_ERROR_ARROW_TRAFODION_CONVERSION:
        {
          snprintf(errorMsgBuffer_, sizeof(errorMsgBuffer_), 
                   "Failed to convert a Parquet type to Trafodion for table %s on column %s", tableName_.c_str(), colName_.c_str());
          break;
        }
      default: 
         snprintf(errorMsgBuffer_, sizeof(errorMsgBuffer_),
                    "Unhandled error number %d in ExpParquetArrowInterface::getErrorText() for table %s (data file=%s)", errEnum, tableName_.c_str(), fileName_.c_str());
   }
         
   return errorMsgBuffer_;
}

Int32 convertDecimalAsLong(
        std::shared_ptr<arrow::DecimalType> dt,  // input
        Int16 dataType, 
        std::shared_ptr<arrow::Array> chk,       // input
        Int64 pos,     // input
        char* target,  // input/output
        Int32& len,    // output
        bool storeLen  // whether to store the total length at beginning
                   ) 
{
  // Store the decimal in a total of 12 bytes when 
  // the precision is equal or less than 18, as follows.
  //  4 bytes for the total length (12)
  //  2 bytes for precision
  //  2 bytes for scale
  //  8 bytes for value
  // See TrafParquetFileReader.java, method 
  // fillNextRow(), fillDecimalWithLong(), and 
  // fillDecimalWithBinary().
  //
  auto numChk = std::dynamic_pointer_cast<arrow::Decimal128Array>(chk);

  // data length
  switch ( dataType ) {

    case REC_BIN16_SIGNED:
        len = sizeof(int16_t);
        break;

    case REC_BIN32_SIGNED:
        len = sizeof(int32_t);
        break;

    case REC_BIN64_SIGNED:
        len = sizeof(int64_t);
        break;

    default:
        return OFR_ERROR_ARROW_TRAFODION_CONVERSION;
  }

  if ( storeLen ) {

    len += 2*sizeof(int16_t);

    ((int32_t*)target)[0] = len;
    target += sizeof(int32_t);

    len += sizeof(int32_t); // total length for the payload

    ((int16_t*)target)[0] = (int16_t)(dt->precision());
    target += sizeof(int16_t);

    ((int16_t*)target)[0] = (int16_t)(dt->scale());
    target += sizeof(int16_t);
  }
   
  arrow::Decimal128 value(numChk->Value(pos));

  int64_t value64 = (value.operator int64_t());

  switch ( dataType ) {

    case REC_BIN16_SIGNED:
     ((int16_t*)target)[0] = value64;
     break;

    case REC_BIN32_SIGNED:
     ((int32_t*)target)[0] = value64;
     break;

    case REC_BIN64_SIGNED:
     ((int64_t*)target)[0] = value64;
     break;

    default:
     return OFR_ERROR_ARROW_TRAFODION_CONVERSION;
  }
   
  return 0;
}

Int32 convertDecimalAsString(
        std::shared_ptr<arrow::DecimalType> dt,  // input
        Int16 dataType, 
        std::shared_ptr<arrow::Array> chk,       // input
        Int64 pos,     // input
        char* target,  // input/output
        Int32& len,    // output
        bool storeLen  // whether to store the total length at beginning
                   ) 
{
  // Store the decimal in a total of L bytes when 
  // the precision is equal or less than 18, as follows.
  //  4 bytes of length (L)
  //  2 bytes of precision
  //  2 bytes of scale
  //  X bytes of the decimal value as a string
  //  L = 2 + 2 + X
  // See TrafParquetFileReader.java, method 
  // fillNextRow(), fillDecimalWithLong(), and 
  // fillDecimalWithBinary().

  auto numChk = std::dynamic_pointer_cast<arrow::Decimal128Array>(chk);

  arrow::Decimal128 value(numChk->Value(pos));

  std::string str = value.ToString(dt->scale());

  if ( dt->scale() == 0 )
     str.append(".");

  len = str.length();

  if ( storeLen ) {

    len += 2*sizeof(int16_t) + sizeof(int32_t); 

    ((int32_t*)target)[0] = len;

    target += sizeof(int32_t);

    ((int16_t*)target)[0] = (int16_t)(dt->precision());
    target += sizeof(int16_t);
   
    ((int16_t*)target)[0] = (int16_t)(dt->scale());
    target += sizeof(int16_t);
  }
   
  str_cpy_all(target, str.data(), str.length());
           
  return 0;
}

// support conversion to trafodion through a middle man
Lng32 convertDecimal(
        std::shared_ptr<arrow::DataType> type,  // arrow type, input
        Int16 dataType, // esgynType , input
        std::shared_ptr<arrow::Array> chk,      // input
        Int64 pos,       // input
        char* target,    // input/output
        Int32& len       // output
        )
{
  std::shared_ptr<arrow::DecimalType> dt = 
        std::dynamic_pointer_cast<arrow::DecimalType>(type);

  if ( dt->precision() <= 18 ) 
  {
     return convertDecimalAsLong(dt, dataType, chk, pos, target, len, true); 
  } else {
     return convertDecimalAsString(dt, dataType, chk, pos, target, len, true); 
  }
  return 0;
}

// support direct conversion to trafodion 
Lng32 convertDecimal(
        std::shared_ptr<arrow::DataType> type,  // arrow type, input
        Int16 dataType, // esgynType , input
        std::shared_ptr<arrow::Array> chk,      // input
        Int64 pos,       // input
        char* target,    // input/output
        Int32& len,      // output
        char *tempStrVal, Lng32 &tempStrValLen, 
        bool &convFromStr)
{
  std::shared_ptr<arrow::DecimalType> dt = 
        std::dynamic_pointer_cast<arrow::DecimalType>(type);

  convFromStr = ( dt->precision() <= 18 ) ;
  if ( !convFromStr )
  {
     return convertDecimalAsLong(dt, dataType, chk, pos, target, len, false); 
  } else {

     return convertDecimalAsString(dt, dataType, chk, pos, tempStrVal, tempStrValLen, false); 
  }
  return 0;
}

// This method converts an ARROW Parquet value into a format
// stuitable for processing by 
// ExExtStorageScanTcb::extractAndTransformExtSourceToSqlRow().
// The converted value is placed in target with a 4-byte length
// field prior to it. When the value is a NULL, the length is set to 
// -1.


Lng32 
ResultVisitor::doConversionToTrafodion(
        std::shared_ptr<arrow::Array> chk,
        std::shared_ptr<arrow::DataType> type,
        const std::string& name,
        char* target, Int64& len)
{
#if DEBUG_COLUMN_VISITOR
   cout << "Enter convertToTrafo(), ";
   cout <<  "column name=" <<  name.c_str();
   cout << ", pos_=" << pos_ ;
   cout << ", IsNull(pos_)=" << chk->IsNull(pos_) << endl;
#endif

   if (chk->IsNull(pos_)) {

       ((Lng32*)target)[0] = -1;
       len = sizeof(Lng32);
   } else {
     switch ( type->id() ) 
      {
        case arrow::Type::INT8:
        {
           ((Lng32*)target)[0] = sizeof(Int8);
           target += sizeof(Lng32);

           auto numChk = std::dynamic_pointer_cast<arrow::Int8Array>(chk);
           const Int8* raw = (const Int8*)(numChk->raw_values());
           *(Int8*)(target) = raw[pos_];
           len = sizeof(Lng32) + sizeof(Int8);
           break;
        }

        case arrow::Type::UINT8:
        {
           ((Lng32*)target)[0] = sizeof(UInt8);
           target += sizeof(Lng32);

           auto numChk = std::dynamic_pointer_cast<arrow::UInt8Array>(chk);
           const UInt8* raw = numChk->raw_values();
           *(UInt8*)(target) = raw[pos_];
           len = sizeof(Lng32) + sizeof(UInt8);
           break;
        }

        case arrow::Type::INT16:
        {
           ((Lng32*)target)[0] = sizeof(Int16);
           target += sizeof(Lng32);

           auto numChk = std::dynamic_pointer_cast<arrow::Int16Array>(chk);
           const Int16* raw = numChk->raw_values();
           *(Int16*)(target) = raw[pos_];
           len = sizeof(Lng32) + sizeof(Int16);
           break;
        }

        case arrow::Type::UINT16:
        {
           ((Lng32*)target)[0] = sizeof(UInt16);
           target += sizeof(Lng32);

           auto numChk = std::dynamic_pointer_cast<arrow::UInt16Array>(chk);
           const UInt16* raw = numChk->raw_values();
           *(UInt16*)(target) = raw[pos_];
           len = sizeof(Lng32) + sizeof(UInt16);
           break;
        }

        case arrow::Type::INT32:
        {
           ((Lng32*)target)[0] = sizeof(Int32);
           target += sizeof(Lng32);

           auto numChk = std::dynamic_pointer_cast<arrow::Int32Array>(chk);
           const Int32* raw = numChk->raw_values();
           *(Int32*)(target) = raw[pos_];
           len = sizeof(Lng32) + sizeof(Int32);
           break;
        }

        case arrow::Type::UINT32:
        {
           ((Lng32*)target)[0] = sizeof(UInt32);
           target += sizeof(Lng32);

           auto numChk = std::dynamic_pointer_cast<arrow::UInt32Array>(chk);
           const UInt32* raw = numChk->raw_values();
           *(UInt32*)(target) = raw[pos_];
           len = sizeof(Lng32) + sizeof(UInt32);
           break;
        }

        case arrow::Type::INT64:
        {
           ((Lng32*)target)[0] = sizeof(Int64);
           target += sizeof(Lng32);

           auto numChk = std::dynamic_pointer_cast<arrow::Int64Array>(chk);
           const Int64* raw = numChk->raw_values();
           *(Int64*)(target) = raw[pos_];
           len = sizeof(Lng32) + sizeof(Int64);
           break;
        }

        case arrow::Type::FLOAT:
        {
           ((Lng32*)target)[0] = sizeof(float);
           target += sizeof(Lng32);

           auto numChk = std::dynamic_pointer_cast<arrow::FloatArray>(chk);
           const float* raw = numChk->raw_values();
           *(float*)(target) = raw[pos_];

           len = sizeof(Lng32) + sizeof(float);
           break;
        }

        case arrow::Type::DOUBLE:
        {
           ((Lng32*)target)[0] = sizeof(double);
           target += sizeof(Lng32);

           auto numChk = std::dynamic_pointer_cast<arrow::DoubleArray>(chk);
           const double* raw = numChk->raw_values();
           *(double*)(target) = raw[pos_];
           len = sizeof(Lng32) + sizeof(double);
           break;
        }

        case arrow::Type::TIMESTAMP:
        {
           ((Lng32*)target)[0] = 11;
           target += sizeof(Lng32);

           auto tsChk = std::dynamic_pointer_cast<arrow::TimestampArray>(chk);
           const int64_t* raw = tsChk->raw_values();

           //  raw[pos_]: a timestamp in nanoseconds since the Unix epoch time 
           //  Need to convert it to a Julian timestamp as follows.
           //    year: 2 bytes
           //    month: 1 byte 
           //    day: 1 byte 
           //    hour: 1 byte 
           //    min: 1 byte 
           //    second: 1-byte 
           //    fraction: integer (4-bytes, in nano-seconds)
           //
           //    COM_EPOCH_TIMESTAMP: the Unix epoch time in micro seconds 
           //                        since the epoch of the Julian time
           //
           int64_t julianTimestamp = raw[pos_]/1000 + COM_EPOCH_TIMESTAMP;

           Int32 tsLen = convertJulianTimestamp(julianTimestamp, target);

           if ( tsLen != 11 )
              return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

           len = sizeof(Lng32) + 11;

           // Adjust the fraction part to nano-seconds 
           UInt32 fraction = *(Int32*)(target+7);
           fraction *= 1000;
           fraction += raw[pos_] % 1000;
           *(UInt32*)(target+7) = fraction;
           break;
        }

        case arrow::Type::TIME96:
        {
           ((Lng32*)target)[0] = 11;
           target += sizeof(Lng32);

           auto tsChk = std::dynamic_pointer_cast<arrow::NumericTime96ArrayType>(chk);
           const parquet::Int96* raw = tsChk->raw_values();

           // convert to 11-byte format
           Int32 tsLen = 
               convertTime96TimestampToByte11Array(raw[pos_], target);
            
           if ( tsLen != 11 )
              return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

           len = sizeof(Lng32) + 11;

           break;
        }
#if 0
        case arrow::Type::BINARY:
        {
           auto binaryChk = 
              std::dynamic_pointer_cast<arrow::BinaryArray>(chk);
           len = sizeof(Lng32) + sizeof(double);
           break;
        }

        case arrow::Type::FIXED_SIZE_BINARY:
        {
           auto fsbChk = 
               std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(chk);
           len = sizeof(Lng32) + sizeof(double);
           break;
        }
#endif

        case arrow::Type::STRING:
        {
           auto stringChk = std::dynamic_pointer_cast<arrow::StringArray>(chk);
           // Fast path to get the value.
           Lng32 strlen = 0;
           stringChk->GetStringAndLength(pos_, target, strlen);
           len = sizeof(Lng32) + strlen;
           break;
        }

        case arrow::Type::BOOL:
        {
           auto boolChk = std::dynamic_pointer_cast<arrow::BooleanArray>(chk);

           ((Lng32*)target)[0] = 1;
           target += sizeof(int32_t);

           ((char*)target)[0] = boolChk->Value(pos_);
           len = sizeof(Lng32) + 1;

           break;
        }

        case arrow::Type::DECIMAL:
        {
           // do include length at beginning
           Int32 len32 = 0;
           Lng32 rc = 
             convertDecimal(type, REC_BIN64_SIGNED, chk, pos_, target, len32);

           len = len32;

           if ( rc == 0 )
             break;
           else
             return rc;
        }

        default:
          return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;
     }
   }

   return OFR_OK;
}

Lng32 
ResultVisitor::getBytesColImp(
          std::shared_ptr<arrow::Array> chk,
          BYTE *colVal, Lng32 &colValLen)
{
   switch ( chk->type()->id() ) 
   {
        case arrow::Type::STRING:
        {


           auto stringChk = 
               std::dynamic_pointer_cast<arrow::StringArray>(chk);

           // Fast path to get the value.
           stringChk->GetString(pos_, (char*)colVal, colValLen);
        }
        break;

        default:
          return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;
   }

   return 0;
}

Lng32 
ResultVisitor::getNumericColImp(
         std::shared_ptr<arrow::Array> chk,
         Int16 dataType, BYTE* colVal, Lng32 &colValLen)
{
   switch ( chk->type()->id() ) 
      {
        case arrow::Type::INT8:
        {
           if ( dataType != REC_BIN8_SIGNED )
             return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

           auto numChk = std::dynamic_pointer_cast<arrow::Int8Array>(chk);
           const Int8* raw = (const Int8*)(numChk->raw_values());
           *(int8_t*)colVal = raw[pos_];
           colValLen = sizeof(Int8);
           break;
        }

        case arrow::Type::UINT8:
        {
           if ( dataType != REC_BIN8_UNSIGNED )
             return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

           auto numChk = std::dynamic_pointer_cast<arrow::UInt8Array>(chk);
           const UInt8* raw = numChk->raw_values();
           *(uint8_t*)colVal = raw[pos_];
           colValLen = sizeof(UInt8);
           break;
        }
        case arrow::Type::INT16:
        {
           if ( dataType != REC_BIN16_SIGNED )
             return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

           auto numChk = std::dynamic_pointer_cast<arrow::Int16Array>(chk);
           const int16_t* raw = numChk->raw_values();
           *(int16_t*)colVal = raw[pos_];
           colValLen = sizeof(int16_t);
           break;
        }

        case arrow::Type::UINT16:
        {
           if ( dataType != REC_BIN16_UNSIGNED )
             return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

           auto numChk = std::dynamic_pointer_cast<arrow::UInt16Array>(chk);
           const uint16_t* raw = numChk->raw_values();
           *(uint16_t*)colVal = raw[pos_];
           colValLen = sizeof(uint16_t);
           break;
        }

        case arrow::Type::INT32:
        {
           if ( dataType != REC_BIN32_SIGNED )
             return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

           auto numChk = std::dynamic_pointer_cast<arrow::Int32Array>(chk);
           const int32_t* raw = numChk->raw_values();

           *(int32_t*)colVal = raw[pos_];
           colValLen = sizeof(int32_t);

           break;
        }

        case arrow::Type::UINT32:
        {
           if ( dataType != REC_BIN32_UNSIGNED )
             return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

           auto numChk = std::dynamic_pointer_cast<arrow::UInt32Array>(chk);
           const uint32_t* raw = numChk->raw_values();

           *(uint32_t*)colVal = raw[pos_];
           colValLen = sizeof(uint32_t);

           break;
        }

        case arrow::Type::INT64:
        {
           if ( dataType != REC_BIN64_SIGNED )
             return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

           auto numChk = std::dynamic_pointer_cast<arrow::Int64Array>(chk);
           const Int64* raw = numChk->raw_values();

           *(int64_t*)colVal = raw[pos_];
           colValLen = sizeof(uint64_t);

           break;
         }

         default:
           return -1;
     }
   return 0;
}

Lng32 ResultVisitor::getDecimalColImp(
                      std::shared_ptr<arrow::Array> chk,
                      Int16 dataType, BYTE *colVal, 
                      Lng32 &colValLen,
                      char *tempStrVal, Lng32 &tempStrValLen, 
                      bool &convFromStr
                      )
{
   return 
     convertDecimal(chk->type(), dataType, chk, pos_, (char*)colVal, colValLen, 
                    tempStrVal, tempStrValLen, convFromStr
                   );
}

Lng32 ResultVisitor::getDoubleOrFloatColImp(
                      std::shared_ptr<arrow::Array> chk,
                      Int16 dataType, BYTE* colVal, 
                      Lng32 &colValLen)
{
   if ( !DFS2REC::isFloat(dataType) )
     return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;

   switch ( chk->type()->id() ) 
   {
        case arrow::Type::FLOAT:
        {
           auto numChk = std::dynamic_pointer_cast<arrow::FloatArray>(chk);
           const float* raw = numChk->raw_values();

           *(float*)(colVal) = raw[pos_];
           colValLen = sizeof(float);

           break;
        }

        case arrow::Type::DOUBLE:
        {
           auto numChk = std::dynamic_pointer_cast<arrow::DoubleArray>(chk);
           const double* raw = numChk->raw_values();

           *(double*)(colVal) = raw[pos_];
           colValLen = sizeof(double);

           break;
        }

        default:
          return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;
   }
   return 0;
}

Lng32 ResultVisitor::getTimestampColImp(
                     std::shared_ptr<arrow::Array> chk,
                     BYTE *colVal, Lng32 &colValLen)
{
   colValLen = 11;

   char* target = (char*)colVal;
 
   auto tsChk = std::dynamic_pointer_cast<arrow::TimestampArray>(chk);

   if (tsChk) {
      const int64_t* raw = tsChk->raw_values();
    
      //  raw[pos_]: a timestamp in nanoseconds since the Unix epoch time 
      //  Need to convert it to a Julian timestamp as follows.
      //    year: 2 bytes
      //    month: 1 byte 
      //    day: 1 byte 
      //    hour: 1 byte 
      //    min: 1 byte 
      //    second: 1-byte 
      //    fraction: integer (4-bytes, in nano-seconds)
      //
      //    COM_EPOCH_TIMESTAMP: the Unix epoch time in micro seconds 
      //                        since the epoch of the Julian time
      //
      int64_t julianTimestamp = raw[pos_]/1000 + COM_EPOCH_TIMESTAMP;
    
      Int32 tsLen = convertJulianTimestamp(julianTimestamp, target);
    
      if ( tsLen != 11 )
         return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;
    
      // Adjust the fraction part to nano-seconds 
      UInt32 fraction = *(Int32*)(target+7);
      fraction *= 1000;
      fraction += raw[pos_] % 1000;
      *(UInt32*)(colVal+7) = fraction;

      return 0;

   } 
           
   auto time96Chk = 
         std::dynamic_pointer_cast<arrow::NumericTime96ArrayType>(chk);

   if ( time96Chk ) {
      const parquet::Int96* raw = time96Chk->raw_values();

      // convert to 11-byte format
      Int32 tsLen = 
          convertTime96TimestampToByte11Array(raw[pos_], target);
            
      if ( tsLen == 11 )
         return 0;
   } 

   return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;
}

Lng32 ResultVisitor::getDateOrTimestampColImp(
                     std::shared_ptr<arrow::Array> chk,
                     Int16 datetimeCode,
                     BYTE *colVal, Lng32 &colValLen)
{
  switch (datetimeCode) {
    case REC_DTCODE_DATE:
      return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;
      break;

    case REC_DTCODE_TIMESTAMP:
      return getTimestampColImp(chk, colVal, colValLen);
      break;

    default:
      return -OFR_ERROR_ARROW_TRAFODION_CONVERSION;
  }
  return 0;
}

Lng32 
TableVisitor::convertToTrafodion(char* target, Int64& len, std::string& colName)
{
   Lng32 rc = 0;
   Int64 colLen = 0;
   len = 0;
   for (Int32 i=0; i<table_->num_columns(); i++) 
   {
      std::shared_ptr<arrow::Column> column = table_->column(i);
      rc = doConversionToTrafodion(column->data()->chunk(0),
                         column->type(),
                         column->name(),
                         target, colLen);

      if ( rc != OFR_OK ) {
         colName = column->name();
         break;
      }

      target += colLen;
      len += colLen;
   }

   return rc;
}


#define TABLE_VISITOR_PROCESS_PROLOG() \
   /* non-existing column */ \
   if ( colNo >= table_->num_columns() ) { \
      processNull(true, colValLen, nullVal); \
      return 0; \
   } \
 \
   std::shared_ptr<arrow::Column> column = table_->column(colNo); \
   std::shared_ptr<arrow::Array> chk = column->data()->chunk(0); \
 \
   /* really is null */ \
   if ( processNull(chk->IsNull(pos_), colValLen, nullVal) ) \
     return 0

#define TABLE_VISITOR_PROCESS_GET(func) \
   TABLE_VISITOR_PROCESS_PROLOG(); \
   return func(chk, dataType, colVal, colValLen)

Lng32 
TableVisitor::getBytesCol(
          int colNo, BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   TABLE_VISITOR_PROCESS_PROLOG();
   return getBytesColImp(chk, colVal, colValLen);
}

Lng32 
TableVisitor::getNumericCol(
         int colNo, Int16 dataType, BYTE* colVal, 
         Lng32 &colValLen, short &nullVal)
{
   TABLE_VISITOR_PROCESS_GET(getNumericColImp);
}

Lng32 TableVisitor::getDecimalCol(
                      int colNo, Int16 dataType, BYTE *colVal, 
                      Lng32 &colValLen, short &nullVal,
                      char *tempStrVal, Lng32 &tempStrValLen, 
                      bool &convFromStr)
{
   TABLE_VISITOR_PROCESS_PROLOG();

   return 
     getDecimalColImp(chk, dataType, colVal, colValLen,
                      tempStrVal, tempStrValLen, convFromStr);
}

Lng32 TableVisitor::getDoubleOrFloatCol(
                      int colNo, Int16 dataType, BYTE* colVal, 
                      Lng32 &colValLen, short &nullVal)
{
   TABLE_VISITOR_PROCESS_GET(getDoubleOrFloatColImp);
}

Lng32 TableVisitor::getDateOrTimestampCol(
                     int colNo, Int16 datetimeCode, 
                     BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   TABLE_VISITOR_PROCESS_PROLOG();

   return 
     getDateOrTimestampColImp(chk, datetimeCode, colVal, colValLen);
}

Lng32 ExpParquetArrowInterfaceTable::prefetchIfNeeded()
{
  if ( resultSet_ == nullptr ) {

     if (hss_) 
       hss_->getHdfsTimer().start();
     
     if (columnsToReturn_.size() > 0) {
        arrowStatus_  = reader_->ReadTable(columnsToReturn_, &resultSet_);
     } else {
          // Read all columns 
        arrowStatus_  = reader_->ReadTable(&resultSet_);
     }

     if (hss_) {
       hss_->incMaxHdfsIOTime(hss_->getHdfsTimer().stop());
     }

     if ( !resultSet_ || !arrowStatus_.ok() )
       return -OFR_ERROR_ARROW_READER_READTABLE;

     numReadCalls_++;

     std::shared_ptr<arrow::Column> column = nullptr;

     // Merge all selected bitmaps into a single one.
     int64_t totalNumValues = resultSet_->column(0)->data()->length();

     int64_t bytes = arrow::BitUtil::lengthInBytes(totalNumValues);
     mergedSelectedBits_ = new uint8_t[bytes];
     arrow::BitUtil::SETAllBits(mergedSelectedBits_, totalNumValues);

//     skipVecType* finalSkipVec = NULL;

     for (Int32 i=0; i<columnsToReturn_.size(); ++i) 
     {
        NABoolean canFilter = filters_[columnsToReturn_[i]].canFilter();

        if ( !canFilter )
           continue;

        column = resultSet_->column(i);
        assert (column->data()->num_chunks() == 1);
        assert(totalNumValues == column->data()->length());

#if 0
        // Have not figured out a good way to make use of the skip
        // vector for iterating over selected bits. The work is TBD.
        const skipVecType& columnSkipVec = 
             column->data()->chunk(0)->getSkipVec();

        if ( !finalSkipVec )
          finalSkipVec = new skipVecType(columnSkipVec);
        else {
          skipVecType* newSkipVec = mergeSkipVec(finalSkipVec, &columnSkipVec);
          delete finalSkipVec;
          finalSkipVec = newSkipVec;
        }
#endif

        auto selectedBits = column->data()->chunk(0)->selected_bitmap_data();
        arrow::BitUtil::ANDAllBits(
                const_cast<uint8_t*>(mergedSelectedBits_), 
                selectedBits, totalNumValues);
     }
              
     selectedBitsReader_.init(mergedSelectedBits_, 0, totalNumValues);

     remainingRowsToRead_ = totalRowsToRead_ = 
           selectedBitsReader_.totalBitsSet();

     visitor_ = new TableVisitor(resultSet_); 
  }

  return OFR_OK;
}

Lng32 
RecordBatchVisitor::convertToTrafodion(char* target, Int64& len, std::string& colName)
{
   Lng32 rc = 0;
   Int64 colLen = 0;

   len = 0;
   for (Int32 i=0; i<batch_->num_columns(); i++) 
   {
      std::shared_ptr<arrow::Array> array = batch_->column(i);
      rc = doConversionToTrafodion(array,
                              array->type(),
                              batch_->column_name(i),
                              target, colLen);

      if ( rc != OFR_OK ) {
         colName = batch_->column_name(i);
         break;
      }

      target += colLen;
      len += colLen;
   }

   return rc;
}

#define RECORD_BATCH_VISITOR_PROCESS_PROLOG() \
   /* non-existing column */ \
   if ( colNo >= batch_->num_columns() ) { \
      processNull(true, colValLen, nullVal); \
      return 0; \
   } \
 \
   std::shared_ptr<arrow::Array> chk= batch_->column(colNo); \
 \
   /* really is null */ \
   if ( processNull(chk->IsNull(pos_), colValLen, nullVal) ) \
     return 0

#define RECORD_BATCH_VISITOR_PROCESS_GET(func) \
   RECORD_BATCH_VISITOR_PROCESS_PROLOG(); \
   return func(chk, dataType, colVal, colValLen)

Lng32 
RecordBatchVisitor::getBytesCol(
          int colNo, BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   RECORD_BATCH_VISITOR_PROCESS_PROLOG();
   return getBytesColImp(chk, colVal, colValLen);
}

Lng32 
RecordBatchVisitor::getNumericCol(
         int colNo, Int16 dataType, BYTE* colVal, 
         Lng32 &colValLen, short &nullVal)
{
   RECORD_BATCH_VISITOR_PROCESS_GET(getNumericColImp);
}

Lng32 RecordBatchVisitor::getDecimalCol(
                      int colNo, Int16 dataType, BYTE *colVal, 
                      Lng32 &colValLen, short &nullVal,
                      char *tempStrVal, Lng32 &tempStrValLen, 
                      bool &convFromStr)
{
   RECORD_BATCH_VISITOR_PROCESS_PROLOG();

   return 
     getDecimalColImp(chk, dataType, colVal, colValLen,
                      tempStrVal, tempStrValLen, convFromStr);
}

Lng32 RecordBatchVisitor::getDoubleOrFloatCol(
                      int colNo, Int16 dataType, BYTE* colVal, 
                      Lng32 &colValLen, short &nullVal)
{
   RECORD_BATCH_VISITOR_PROCESS_GET(getDoubleOrFloatColImp);
}

Lng32 RecordBatchVisitor::getDateOrTimestampCol(
                     int colNo, Int16 datetimeCode, 
                     BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   RECORD_BATCH_VISITOR_PROCESS_PROLOG();

   return 
     getDateOrTimestampColImp(chk, datetimeCode, colVal, colValLen);
}

//#define DEBUG_COLUMN_CHUNK_STATS 1

void
ExpParquetArrowInterface::computeRowGroups(const Int64 offset, 
                                           const Int64 length)
{
  std::shared_ptr<parquet::FileMetaData> fileMeta = reader_->getFileMetaData
();

#ifdef DEBUG_COLUMN_CHUNK_STATS
            const parquet::ApplicationVersion& appVersion = 
                           fileMeta->writer_version();

            cout << "Writer version: "
                 <<  appVersion.application_
                 <<  " " 
                 << appVersion.version.major
                 <<  "." 
                 << appVersion.version.minor
                 <<  "." 
                 << appVersion.version.patch
                 <<  endl;
#endif

  totalRowsToAccess_ = fileMeta->num_rows();

  int numGroups = fileMeta->num_row_groups();
  rowGroups_.clear();
  rowGroups_.reserve(numGroups);

  for ( int i=0; i<numGroups; i++ )
   {
      std::unique_ptr<parquet::RowGroupMetaData> rgMeta = fileMeta->RowGroup(i);

      int64_t bytes = rgMeta->total_byte_size();

      std::unique_ptr<parquet::ColumnChunkMetaData> 
           ccMeta = rgMeta->ColumnChunk(0);

      int64_t dataPageOffset = ccMeta->data_page_offset();

      // When offset is 0, it means the data file stats is not available
      // due to sampling and we need to take in every row group in the 
      // data file. 
      if ( offset == 0 || (offset == dataPageOffset && bytes == length) )
      {
         // Further check if the rowgroup can be filtered out
         // by page filters.
         //
         NABoolean keepRowGroup = TRUE;
         // Do it by asking the question for each column <C>: 
         // can the column min/max stats be never satisfied 
         // by the page filters on <C>?
         if ( applyRowGroupFilters() ) 
         { 
            for (int j=0; j<rgMeta->num_columns(); j++ ) {
      
               std::unique_ptr<parquet::ColumnChunkMetaData> 
                   ccMeta = rgMeta->ColumnChunk(j);
      
               std::shared_ptr<parquet::RowGroupStatistics> 
                  stats = ccMeta->statistics(skipCCStatsCheck_);
   
               if ( stats && stats->HasMinMax() ) {
   
                  PageFilter* pf = filters_[j].getPageFilter();
   
                  if ( pf ) {
#ifdef DEBUG_COLUMN_CHUNK_STATS
                    const parquet::ColumnDescriptor* cdesc = stats->descr();
                    const std::string& cname = cdesc->name();
   
                    cout << "To apply rowgroup filtering, column chunk stats: cname=" << cname 
                         << ", index=" << j
                         << ", hasMinMax=" << stats->HasMinMax()
                         << endl;
#endif
   
                    if ( !(pf->canAcceptRowGroup(stats->EncodeMin(), stats->EncodeMax())) )
                     {
                        keepRowGroup = FALSE;
                        break;
                     }
                  }
               }
            }
         }

         // If no page filter can reject any column, keep the row
         // group for scan. And there are two conditions to worry
         // about here.
         //
         // 1. When offset > 0. In this case, there is only one 
         // row group requested and we have found such a row group and
         // it has passed the filter test above. We keep this row group
         // and return. 
         //
         // 2. When offset == 0. In this case, we need to process
         // next row group.
         //
         // Note that for Parquet and ORC files, the compiler does
         // not allow split at the block level, see method
         // HHDFSSplittableFileStats::splitsOfPrimaryUnitsAllowed().
         //
         if ( keepRowGroup ) {
           rowGroups_.push_back(i);

           // Return only if a single valid row group is requested.
           if ( offset > 0 ) 
             return;
         }
      }
   }

#ifdef DEBUG_COLUMN_CHUNK_STATS
   cout << "File=" << fileName_
        << ", total #rowGroups=" << numGroups
        << ", #rowGroups to scan=" << rowGroups_.size()
        << endl;
#endif

   // The row group is filtered out. 
}

Lng32 ExpParquetArrowInterfaceBatch::prefetchIfNeeded()
{
  if ( batch_ == nullptr && rowGroups_.size() > 0 ) {

     //std::vector<int> rowGroupVec{0};
     if (hss_) 
       hss_->getHdfsTimer().start();

     if (columnsToReturn_.size() > 0) {
          arrowStatus_ = reader_->GetRecordBatchReader(
                          rowGroups_, columnsToReturn_, &batchReader_);
     } else {
          arrowStatus_ = reader_->GetRecordBatchReader(
                          rowGroups_, &batchReader_);
     }

     if ( !arrowStatus_.ok() )
        return -OFR_ERROR_ARROW_READER_GET_BATCH_READER;

     Lng32 rc = readNextBatch();

     if (hss_) {
       hss_->incMaxHdfsIOTime(hss_->getHdfsTimer().stop());
     }

     return rc;
  }
  return OFR_OK;
}

Lng32 ExpParquetArrowInterfaceBatch::readNextBatch()
{
   releaseResourcesForCurrentBatch(); 

   // batchReader_ is nullptr when all row groups are
   // filtered out by page filters.
   if ( batchReader_ == nullptr )
      return OFR_OK;

   arrowStatus_ = batchReader_->ReadNext(&batch_);

   if ( !arrowStatus_.ok() )
      return -OFR_ERROR_ARROW_READER_READ_NEXT_BATCH;

   if ( batch_ == nullptr )
      return OFR_OK;

   numBatchesRead_++;

   // Merge all selected bitmaps into a single one.
   int64_t totalNumValues = batch_->num_rows();

   int64_t bytes = arrow::BitUtil::lengthInBytes(totalNumValues);
   mergedSelectedBits_ = new uint8_t[bytes];

   arrow::BitUtil::SETAllBits(mergedSelectedBits_, totalNumValues);

//   skipVecType* finalSkipVec = NULL;

   if ( columnsToReturn_.size() > 0 ) {
      for (Int32 i=0; i<batch_->num_columns(); ++i) 
      {
         NABoolean canFilter = filters_[columnsToReturn_[i]].canFilter();
   
         if ( !canFilter )
            continue;
   
         std::shared_ptr<arrow::Array> array = batch_->column(i);
         assert(totalNumValues == array->length());

#if 0
         // Have not figured out a good way to make use of the skip
         // vector for iterating over selected bits. The work is TBD.
         const skipVecType& columnSkipVec = array->getSkipVec();

         if ( !finalSkipVec )
           finalSkipVec = new skipVecType(columnSkipVec);
         else {
           skipVecType* newSkipVec = mergeSkipVec(finalSkipVec, &columnSkipVec);
           delete finalSkipVec;
           finalSkipVec = newSkipVec;
         }

         //cout << "finalSkipVec=" << *finalSkipVec << endl;
#endif
   
         auto selectedBits = array->selected_bitmap_data();

         arrow::BitUtil::ANDAllBits(
                 const_cast<uint8_t*>(mergedSelectedBits_), 
                 selectedBits, totalNumValues);

#ifdef NEED_INSTRUMENT_ARROW_INTERFACE
         arrow::internal::BitmapReader dummyReader;
         dummyReader.init(selectedBits, 0, totalNumValues);

         QRLogger::log(CAT_SQL_EXE_MINMAX,
                       LL_DEBUG,
                       "In ExpParquetArrowInterfaceBatch::readNextBatch(): valueFilterEval: time=%s (us=%lld), rows(total, survived)=%lld, %lld",
                       reportTimeDiff(array->getValueFilterEvalTime()),
                       array->getValueFilterEvalTime(),
                       totalNumValues, 
                       dummyReader.totalBitsSet() 
                      );
#endif
      }
   }
            
   selectedBitsReader_.init(mergedSelectedBits_, 0, totalNumValues);

   remainingRowsToRead_ = totalRowsToRead_ 
                 = selectedBitsReader_.totalBitsSet();

   visitor_ = new RecordBatchVisitor(batch_); 
  
   return OFR_OK;
}

NABoolean ExpParquetArrowInterfaceBatch::noMoreData()
{
   if ( !ExpParquetArrowInterface::noMoreData() )
     return FALSE;

   // check if there are more arrays to be read from
   if ( readNextBatch() != OFR_OK )
     return TRUE;

   return batch_ == nullptr;
}

void 
ExpParquetArrowInterface::updateStatsForScanFilters()
{
   if ( !hss_ )
      return;

#ifdef DEBUG_SCANFILTER_STATS
   cout << "ArrowAPI: updateStats, getpid=" << getpid() << ", file=" << fileName_ << endl;
#endif

   for (int i=0; i<filters_.size(); i++ )
   {
      ParquetFilter* vf = filters_[i].getValueFilter();

      while ( vf ) {

         // Non-scan filters have a filter Id of -1. Ignore them.
         // Also ignore page filters since they are non-bloom.
         if ( vf->getFilterId() >= 0 ) {
            ScanFilterStats stats(vf->getFilterId(), 
                                  vf->getValuesRemoved(),
                                  TRUE, TRUE);

            hss_->addScanFilterStats(stats, ScanFilterStats::ADD);

#ifdef DEBUG_SCANFILTER_STATS
            cout << "  pageFilter: filterId=" << vf->getFilterId()
                 << ", #valuesRemoved=" << vf->getValuesRemoved() << endl;
#endif
         }

         vf=vf->getNext();
      }
   }

#ifdef DEBUG_SCANFILTER_STATS
   hss_->getScanFilterStats()->dump(cout, "Arrow API: aftering adding one stat, done");
#endif
}

Int32 ExpParquetArrowInterface::findIndexForNarrowestColumn(TextVec *colTypeVec)
{
   Lng32 idx = -1;
   Lng32 maxLen = 0;

   for ( Int32 i=0; i<colTypeVec->size(); i++ )
   {
      EncodedHiveType colType((*colTypeVec)[i]);

      Int32 len = colType.length();

      if ( maxLen == 0 || len < maxLen )  {
        maxLen = len;
        idx = i;
      }
   }
   return idx;
}
         
void 
ExpParquetArrowInterface::setColNameForError(const char* name, 
                                             int32_t len)
{
   colName_.clear();
   colName_.insert(0, name, len);
}

void 
ExpParquetArrowInterface::setColNameForError(const std::string& colName)
{
   setColNameForError(colName.data(), colName.length());
}

  
Lng32 
ExpParquetArrowInterface::getBytesCol(
          int colNo, BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   return visitor_->getBytesCol(colNo, colVal, colValLen, nullVal);
}

Lng32 
ExpParquetArrowInterface::getNumericCol(
         int colNo, Int16 dataType, BYTE *colVal, 
         Lng32 &colValLen, short &nullVal)
{
   return visitor_-> getNumericCol( colNo, dataType, colVal, colValLen, nullVal);
}

Lng32 ExpParquetArrowInterface::getDecimalCol(
                      int colNo, Int16 dataType, BYTE *colVal, 
                      Lng32 &colValLen, short &nullVal,
                      char *tempStrVal, Lng32 &tempStrValLen, 
                      bool &convFromStr)
{
   return visitor_-> getDecimalCol(
                      colNo, dataType, colVal, 
                      colValLen, nullVal, 
                      tempStrVal, tempStrValLen, convFromStr);
}

Lng32 ExpParquetArrowInterface::getDoubleOrFloatCol(
                      int colNo, Int16 dataType, BYTE *colVal, 
                      Lng32 &colValLen, short &nullVal)
{
   return visitor_-> 
           getDoubleOrFloatCol(colNo, dataType, colVal, colValLen, nullVal);
}

Lng32 ExpParquetArrowInterface::getDateOrTimestampCol(
                     int colNo, Int16 datetimeCode, 
                     BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   return visitor_-> 
      getDateOrTimestampCol(colNo, datetimeCode, colVal, colValLen, nullVal);
}
