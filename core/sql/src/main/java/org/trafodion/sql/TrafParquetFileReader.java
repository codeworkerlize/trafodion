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

package org.trafodion.sql;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;

import static java.lang.Math.toIntExact;
import java.lang.Math;
import java.lang.Process;
import java.lang.reflect.Method;

import java.lang.management.ManagementFactory;

import java.math.BigInteger;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ByteOrder;

import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.BitSet;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockLocation;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hive.common.type.HiveDecimal;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.statistics.*;

import org.apache.parquet.example.*;
import org.apache.parquet.example.data.*;
import org.apache.parquet.example.data.simple.*;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.*;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterApi.*;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.*;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;

import org.apache.parquet.filter2.predicate.UserDefinedPredicate;

import java.net.MalformedURLException;

import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext;
import org.apache.parquet.hadoop.example.*;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.TrafParquetReader;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import org.apache.parquet.Log;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;

import org.trafodion.sql.TrafConfiguration;
import org.trafodion.sql.NanoTimeUtils;

import org.trafodion.sql.parquet.TrafFilter;
import org.trafodion.sql.parquet.TrafGroup;
import org.trafodion.sql.parquet.TrafReadSupport;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.log4j.MDC;

public class TrafParquetFileReader extends TrafExtStorageUtils
{
    public class timer {

       timer()
       {
	   this("");
       }

       timer(String p_timerDescription)
       {
          timerDescription_ = p_timerDescription;

          init();
       }

       void init()
       {
          startTime_ = 0;
          elapsedTime_ = 0;
	  count_ = 0;
       }

       public long start() 
       { 
          startTime_ = System.nanoTime() / 1000;
          return startTime_;
       };

       public long stop()
       { 
          elapsedTime_ = System.nanoTime() / 1000 - startTime_;
          return elapsedTime_;
       };
       
       public void start2() 
       { 
          startTime_ = System.nanoTime() / 1000;
       };

       public void stop2()
       { 
          elapsedTime_ += System.nanoTime() / 1000 - startTime_;
	  count_++;
       };
       
	public void increment_count()
       { 
	  count_++;
       };
       
       public void report(String msg)
       {
          System.out.println(getElapsedTime(msg));
       }

       public String getElapsedTime(String msg)
       {
          if ( elapsedTime_ > 1000 )
             return String.format("%s%.2f ms", msg, (double)(elapsedTime_/1000));
          else
             return msg + elapsedTime_ + "us";
       }
	
       public String toString() 
       {
	   return getElapsedTime(timerDescription_) + ", count: " + count_;
       }

	String timerDescription_;
	long startTime_;
	long elapsedTime_;
	long count_;
    };

    static ExecutorService sv_executorService = null;
    Future m_future = null;
    boolean m_request_submitted = false;

    timer m_bf_i_timer = null;
    timer m_bf_l_timer = null;
    timer m_serialization_timer = null;
    timer m_parquet_reader_timer = null;
    timer m_parquet_initreader_timer = null;
    timer m_process_block_timer = null;
    timer m_open_timer = null;
    timer m_scan_timer = null;

    int m_row_offset = 0;

    static Logger logger = Logger.getLogger(TrafParquetFileReader.class.getName());

    static Configuration conf              = null;

    // variable used to access parquet file system.
    TrafParquetReader.Builder<Group> pb = null;
    TrafParquetReader<Group> reader     = null;
    GroupReadSupport readSupport    = null;
    
    String m_table_name             = null;

    Path filePath                   = null;
    FileStatus[] inputFileStatus    = null;
    MessageType schema              = null;
 
    int[] listOfFieldIndex      = null;  
    boolean m_field_index_populated = false;
 
    // these variable are used to handle predicate pushdown.
    int currFilterPPIidx = -1;
    int filterColumnType = -1;
    String filterColName = null;
    Object filterOperandObject = null;
    FilterPredicate filterPred = null;

    static boolean m_dump_debug_data = false;

    boolean m_more_rows = true;

    static int     sv_concat_blocks_max = 4;
    static boolean sv_concat_blocks_enabled = false;
    static int     sv_concat_blocksize_in_mb = 16;
    static boolean sv_double_buffer_enabled = false;
    static boolean sv_fast_serialize_enabled = false;
    static boolean sv_faster_serialize_enabled = false;

    static boolean sv_timer_on = false;
    static boolean sv_serialization_timer_on = false;
    static boolean sv_bf_timer_on = false;
    static boolean sv_scan_timer_on = false;

    static {
	conf = TrafConfiguration.create(TrafConfiguration.HBASE_CONF);
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        MDC.put("PID", pid);
	System.setProperty("hostName", System.getenv("HOSTNAME"));

	sv_fast_serialize_enabled = conf.getBoolean("org.trafodion.sql.parquet.fastserialize.enabled", sv_fast_serialize_enabled);
	if (sv_fast_serialize_enabled) {
	    sv_faster_serialize_enabled = conf.getBoolean("org.trafodion.sql.parquet.fasterserialize.enabled", sv_faster_serialize_enabled);
	}
	sv_double_buffer_enabled = conf.getBoolean("org.trafodion.sql.parquet.doublebuffer.enabled", sv_double_buffer_enabled);
	sv_concat_blocks_enabled = conf.getBoolean("org.trafodion.sql.parquet.concatblocks.enabled", sv_concat_blocks_enabled);
	sv_concat_blocksize_in_mb = conf.getInt("org.trafodion.sql.parquet.concatblocks.size_mb", sv_concat_blocksize_in_mb);
	sv_concat_blocks_max = conf.getInt("org.trafodion.sql.parquet.concatblocks.max", sv_concat_blocks_max);
	sv_timer_on = conf.getBoolean("org.trafodion.sql.parquet.timer.enabled", sv_timer_on);
	sv_serialization_timer_on = conf.getBoolean("org.trafodion.sql.parquet.ser_timer.enabled", sv_serialization_timer_on);
	sv_bf_timer_on = conf.getBoolean("org.trafodion.sql.parquet.bftimer.enabled", sv_bf_timer_on);

	sv_executorService = Executors.newCachedThreadPool();

        String debugScanTime = System.getenv(new String("DEBUG_SCAN_TIME"));
	sv_scan_timer_on = (debugScanTime != null);

        String debugFilters = System.getenv(new String("DEBUG_FILTERS"));
        m_dump_debug_data = (debugFilters != null);
    }

    TrafParquetFileReader() throws IOException {
	if (logger.isTraceEnabled()) logger.trace("Enter TrafParquetFileReader()");
    }

    // format of bb buffer for each ppi_vec entry
    //    operatorType: 4 bytes. 
    //    colNameLen:   4 bytes. 
    //    colName:      colNameLen bytes.
    //    operandType:  4 bytes
    //    operandLen:   4 bytes
    //    operandVal:   operandLen bytes
     private boolean extractFilterInfo(int colIdx,
                                       Object[] ppi_vec)
    {
        filterColumnType = -1;
        filterColName = null;
        filterOperandObject = null;

        ByteBuffer bb = ByteBuffer.wrap((byte[])ppi_vec[colIdx]);
        bb.order(m_byteorder);
        
        int operatorType = bb.getInt();

        int colNameLen = bb.getInt();
        byte[] colName = null;
        int colIndex = -1;
        filterColumnType = -1;
        if (colNameLen > 0) {
            colName = new byte[colNameLen];
            bb.get(colName, 0, colNameLen);
            filterColName = Bytes.toString(colName);

            colIndex = listOfAllColName.indexOf(filterColName);
            if (colIndex != -1) {
                filterColumnType = listOfAllColType.get(colIndex);
            } // if
        } // if
        
        if (filterColumnType == -1)
            return false; 

        int operandType = bb.getInt();
        int operandLen = bb.getInt();
        byte[] operator_ba = null;
        
        if (operandLen > 0) {
            operator_ba = new byte[operandLen];
            bb.get(operator_ba, 0, operandLen);
        }
        
        //System.out.println("fct = " + filterColumnType + " operandType = " + operandType + " operandLen " + operandLen);

        boolean supported = true;
        switch (filterColumnType) {
        case HIVE_BOOLEAN_TYPE:
            if (operator_ba != null)
                filterOperandObject = Boolean.valueOf(new String(operator_ba));
            break;
        case HIVE_BYTE_TYPE:
            if (operator_ba != null)
                filterOperandObject = Integer.valueOf(new String(operator_ba));
            break;
        case HIVE_SHORT_TYPE:
            if (operator_ba != null)
                filterOperandObject = Integer.valueOf(new String(operator_ba));
            break;
        case HIVE_INT_TYPE:
            if (operator_ba != null)
                filterOperandObject = Integer.valueOf(new String(operator_ba));
            break;
        case HIVE_LONG_TYPE:
            if (operator_ba != null)
                filterOperandObject = Long.valueOf(new String(operator_ba));
            break;
        case HIVE_FLOAT_TYPE:
            if (operator_ba != null)
                filterOperandObject = Float.valueOf(new String(operator_ba));
            break;
        case HIVE_DOUBLE_TYPE:
            if (operator_ba != null)
                filterOperandObject = Double.valueOf(new String(operator_ba));
            break;
        case HIVE_BINARY_TYPE:
            if (operator_ba != null)
                filterOperandObject = Binary.fromByteArray(operator_ba);
            break;
        case HIVE_CHAR_TYPE:
        case HIVE_VARCHAR_TYPE:
        case HIVE_STRING_TYPE:
            if ((operandLen == 0) && (operator_ba == null)) {
                operator_ba = new byte[0];
                String strVal = Bytes.toString(operator_ba);
                filterOperandObject = Binary.fromString(strVal);
            } else if (operator_ba != null) {
                String strVal = Bytes.toString(operator_ba);
                strVal = strVal.replaceAll("\\s*$", "");
                filterOperandObject = Binary.fromString(strVal);
            }
            break;
        case HIVE_TIMESTAMP_TYPE:
            if (! isParquetLegacyTS()) {
                if (operator_ba != null) {
                    Timestamp ts = Timestamp.valueOf(Bytes.toString(operator_ba));
                    long tsVal = (ts.getTime()/1000)*1000000 
                        + (ts.getNanos());
                    
                    filterOperandObject = Long.valueOf(tsVal);
                }
            } else {
                if (operator_ba != null) {
                    Timestamp ts = Timestamp.valueOf(Bytes.toString(operator_ba));

		    // The following creates a NanoTime that factors in the timezone
		    // Assumption: timetamp data in Parquet is in GMT
		    NanoTime nt = 
			NanoTimeUtils.getNanoTime(ts,
						  sv_parquet_timestamp_skip_conversion // default is false: do not skip time zone conversion
						  );
                    Binary ntBin = nt.toBinary();

		    // The following flag is set so that the comparator method 
		    // (Binary#compareTwoByteArrays) compares it as timestamps
		    ntBin.isInt96Timestamp = true;

                    filterOperandObject = ntBin;
		    if (logger.isDebugEnabled()) logger.debug("extractFilterInfo"
							      + ", timestamp: " + ts
							      + ", ts.year: " + ts.getYear()
							      + ", ts.month: " + ts.getMonth()
							      + ", ts.date: " + ts.getDate()
							      + ", ntBin: " + ntBin
							      + ", filterOperand: " + filterOperandObject
							      );

		}
            }
            break;

        case HIVE_DECIMAL_TYPE:
            {
                if (operator_ba == null)
                    break;

                int i = 0;

                String decStr = Bytes.toString(operator_ba, 0, operandLen);
                HiveDecimal hiveDec = HiveDecimal.create(decStr);
                int hiveScale = hiveDec.scale();
                BigInteger decBi = hiveDec.unscaledValue();
                int scale = hiveScale;
                if (scale > hiveScale) {
                    BigInteger powBi = BigInteger.valueOf(10);
                    powBi = powBi.pow(scale-hiveScale);
                    decBi = decBi.multiply(powBi);
                }

                byte[] decBiBytes = decBi.toByteArray();
                byte[] decBiAllBytes = null;
                
                // int flbaSize = primType.getTypeLength();
                int flbaSize = decBiBytes.length;
                
                if (decBiBytes.length < flbaSize) {
                    decBiAllBytes = new byte[flbaSize];
                    for (i = 0; i < (flbaSize - decBiBytes.length); i++) {
                        decBiAllBytes[i] = 0;
                    }
                    int lv_src_offset = 0;
                    for (i = (flbaSize - decBiBytes.length); i < decBiAllBytes.length; i++) {
                        decBiAllBytes[i] = decBiBytes[lv_src_offset++];
                    }
                }
                else {
                    decBiAllBytes = decBiBytes;
                }
                
                Binary decBin = Binary.fromByteArray(decBiAllBytes);
		if (logger.isDebugEnabled())
		    logger.debug("extractFilterInfo DECIMAL TYPE"
				 + ", decStr: " + decStr
				 + ", hiveDec: " + hiveDec
				 + ", decBi: " + decBi
				 + ", decBin: " + decBin
				 );
                filterOperandObject = decBin;
            }
            break;

        case HIVE_DATE_TYPE:
            supported = false;
            break;
            
        default:
            // error
            supported = false;
            break;
        } // switch
        
        return supported;
    }



    public static class hdpHash {

      public final int[] randomHashValues = {
        0x905ebe29, 0x95ff0b84, 0xe5357ed6, 0x2cffae90,
        0x8350b3f1, 0x1748a7eb, 0x2a0695db, 0x1e7ca00c,
        0x60f80c24, 0x9a41fe1c, 0xa985a647, 0x0ed7e512,
        0xcd34ef43, 0xe06325a6, 0xecbf735a, 0x76540d38,
        0x35cba55d, 0xff539efc, 0x64545d45, 0xd7112c0d,
        0x17e09e1c, 0x02359d32, 0x45976350, 0xd630a578,
        0x34cd0c12, 0x754546f6, 0x1bf4f249, 0xbc65c34f,
        0x5c932f44, 0x6cb0d8d0, 0xfd0e7030, 0x2b160e3b,
        0x101daff6, 0x25bbcb9d, 0xe7eca21f, 0x6d3b24ca,
        0xaef7e6b9, 0xd212f049, 0x2de2817e, 0x2792bcd5,
        0x67f794b2, 0xaec6f7cc, 0x79a3e367, 0xd5a85114,
        0xa98ecc2d, 0xf373e266, 0x58ae2757, 0xd8faa0ff,
        0x45e7eb61, 0xbd72ba1e, 0xc28f6b16, 0x804bc2e6,
        0xfed74984, 0x881cd177, 0xa02647e8, 0xd799d053,
        0xbe143d12, 0x49177474, 0xbbc0c5f4, 0x99f7fe9f,
        0x24fc1559, 0xce0925cf, 0x1dded5f4, 0x1d1a2cd3,
        0xafe3ef48, 0x6fd5d075, 0x4a63bc1d, 0x93aa36c0,
        0x2942d778, 0xb26a2444, 0x5616cc50, 0x7565c161,
        0xa006197b, 0xee700b07, 0x4a236a82, 0x693db870,
        0x9a919e64, 0x995b05b1, 0xd4659569, 0x90e45846,
        0xbca11996, 0x3e345cd9, 0xb29a9967, 0x7e9e66f7,
        0x9ce136d0, 0xcde74e76, 0xde56e4bb, 0xba4dc6ae,
        0xf9d40779, 0x4e5c0bdb, 0xde14f9e5, 0x278f8745,
        0x13ce0128, 0x8bb308f5, 0x4c41a359, 0x273d1927,
        0x50338e76, 0xdfceb7c2, 0xf1b86f68, 0xc8b12d6a,
        0xf4cb0e08, 0xa74b4b14, 0x81571c6a, 0xebc4a928,
        0x1d6d5fd6, 0x7f4bbc87, 0x61ba542f, 0x9b06d11d,
        0xb53ae1c1, 0xdcc2a6c0, 0x7f04f8a8, 0x8da9d186,
        0xa168e054, 0x21ed0ce7, 0x9ca9e9d1, 0x0e01fb38,
        0xd8b6b1d9, 0xb8d10266, 0x203a9de1, 0x37ba3ffe,
        0x9fefb09f, 0x5e4cb3e2, 0xcecd03b4, 0xcc270838,
        0xa1619089, 0x22995679, 0x6dcd6b78, 0x8c50f9b1,
        0x1c354ada, 0x48a0f13e, 0xca7b4696, 0x5c1fe8bf,
        0xdd0f433f, 0x8aa411f1, 0x149b2ee3, 0x181d16a1,
        0x3b84b01d, 0xee745103, 0x0f230907, 0x663d1014,
        0xd614181b, 0xb1b88cc9, 0x015f672c, 0x660ea636,
        0x4107c7f3, 0x6f0d8afe, 0xf0aeffeb, 0x93b25fa0,
        0x620c9075, 0x155a4d7e, 0x10fdbd73, 0xb162eabe,
        0xaf9605db, 0xba35d441, 0xde327cfa, 0x15a6fd70,
        0x0f2f4b54, 0xfb1b4995, 0xec092e68, 0x37ebade6,
        0x850f63ca, 0xe72a879f, 0xc823f741, 0xc6f114b8,
        0x74e461f6, 0x1d01ad14, 0xfe1ed7d3, 0x306b9444,
        0x9ebd40a6, 0x3275b333, 0xa8540ca1, 0xeb8d394c,
        0xa2aef54c, 0xf12d0705, 0x8974e70e, 0x59ae82cf,
        0x32469aca, 0x973325d8, 0x27ba604d, 0x9aeb7827,
        0xaf0af97c, 0x9783e6f8, 0xe0725a87, 0x2f02d864,
        0x717a0587, 0x0c90d7b0, 0x6828b84e, 0xba08ebe7,
        0x65cf8360, 0x63132f80, 0xbb8d4a41, 0xbd5b8b41,
        0x459f019f, 0x5e68369f, 0xe855f000, 0xa79a634c,
        0x172c7704, 0x07337ab3, 0xb2926453, 0x11084c8a,
        0x328689ca, 0xa7e3efcf, 0x8b9a5695, 0x76b65bbe,
        0x87bb5a2a, 0x5f73e6ad, 0xcf59b265, 0x4fe46ec9,
        0x52561232, 0x70db002c, 0xc21d1b8f, 0xd7ceb1c6,
        0xff4a97c8, 0xdd21c90b, 0x48c14c38, 0x64262c68,
        0x74c5d3f9, 0x66bf60e7, 0xce804348, 0x98585792,
        0x7619fc86, 0x91de3f72, 0x57f5191c, 0x576d9737,
        0x5f4535b0, 0xb9ee8ef5, 0x2e9eff6c, 0xc7c9f874,
        0xe6ac0843, 0xd93b8c08, 0x2f34a779, 0x407799eb,
        0x2b9904e0, 0x14bb018f, 0x1fcf367b, 0x7975c362,
        0xba31448f, 0xa59286f7, 0x1255244a, 0xd685169b,
        0xc791ec84, 0x3b5461b1, 0x4822924a, 0x26d86175,
        0x596e6b2f, 0x6a157bef, 0x8bc98a9b, 0xa8220343,
        0x91eaad8a, 0x42b89a9e, 0x7c9b5f81, 0xb5f9ec6c,
        0xd999ef9e, 0xa547f6a3, 0xc391f010, 0xe9d8bb43
      };

      public static final int NO_FLAGS      = 0x0000;
      public static final int SWAP_TWO      = 0x0001;
      public static final int SWAP_FOUR     = 0x0002;
      public static final int SWAP_EIGHT    = 0x0004;
      public static final int SWAP_FIRSTTWO = 0x0008;
      public static final int SWAP_LASTFOUR = 0x0010;

      hdpHash() {}

      int hash8(byte[] data, int flags)
      {
          int valp = 0;
          int hashValue = 0;

          switch(flags) {
          case NO_FLAGS:
            {
              hashValue = randomHashValues[data[valp++]];
              hashValue = (hashValue << 1 | hashValue >>> 31) ^ 
                       randomHashValues[Byte.toUnsignedInt(data[valp++])];
              hashValue = (hashValue << 1 | hashValue >>> 31) ^ 
                       randomHashValues[Byte.toUnsignedInt(data[valp++])];
              hashValue = (hashValue << 1 | hashValue >>> 31) ^ 
                       randomHashValues[Byte.toUnsignedInt(data[valp++])];
              hashValue = (hashValue << 1 | hashValue >>> 31) ^ 
                       randomHashValues[Byte.toUnsignedInt(data[valp++])];
              hashValue = (hashValue << 1 | hashValue >>> 31) ^ 
                       randomHashValues[Byte.toUnsignedInt(data[valp++])];
              hashValue = (hashValue << 1 | hashValue >>> 31) ^ 
                       randomHashValues[Byte.toUnsignedInt(data[valp++])];
              hashValue = (hashValue << 1 | hashValue >>> 31) ^ 
                       randomHashValues[Byte.toUnsignedInt(data[valp++])];
              break;
            }

            default:
              assert(false);
          }

          return hashValue;
      }

      public int computeHash(byte[] data, int flags)
      {
         int hashValue = 0;
         int valp = 0;
         int iter = 0; // iterator over the key bytes, if needed
  
         switch(flags) {
           case NO_FLAGS:
           {

           // Speedup for long keys - compute first 8 bytes fast (the rest with a loop)
             if ( data.length >= 8 ) {
               hashValue = hash8(data, flags);  // do the first 8 bytes fast
               // continue with the 9-th byte (only when length > 8 )
               valp = 8;
               iter = 8; 
              }

              for(; iter < data.length; iter++) {


               // Make sure the hashValue is sensitive to the byte position.
               // One bit circular shift.
                hashValue = 
                  (hashValue << 1 | hashValue >>> 31) ^ 
                       randomHashValues[Byte.toUnsignedInt(data[valp++])];

              }
              break;
           }
           case SWAP_EIGHT:
           case SWAP_TWO:
           case SWAP_FOUR:
           case (SWAP_FIRSTTWO | SWAP_LASTFOUR):
           case SWAP_FIRSTTWO:
           case SWAP_LASTFOUR:
           default:
             assert(false);
          }
         
        return hashValue;
      }


      public int computeFinalHash(int hash, int ith, int m)
      {
         int h2 = randomHashValues[ith % 256];
       
         int res = ((hash<<1 | hash>>>31) ^ h2);

         // extend res to a positive long before %, as % is
         // a remainder operator in Java. The remainder 
         // operator computes the same result as mod for
         // positive numbers.
         return toIntExact((res & 0xFFFFFFFFL) % m);
      }
    }

    public static void testHash(hdpHash hash, int value, byte[] buf)
    {
       // Cast to big endian byte order
       buf[0] =  (byte)(value >> 24);
       buf[1] =  (byte)(value >> 16);
       buf[2] =  (byte)(value >> 8);
       buf[3] =  (byte)value;


       int hv = hash.computeHash(buf, hdpHash.NO_FLAGS);
       System.out.println("hash(" + value + ", 0x" +
                               Integer.toHexString(value) +
                               ")=" + 
                               Integer.toHexString(hv));
    }

    public static void testHash(String file)
       throws FileNotFoundException
    {
       hdpHash hash = new hdpHash();
       byte[] data = new byte[4];

       if ( file == null ) { 
         testHash(hash, 256, data);
         return;
       }

       Scanner scanner = new Scanner(new File(file));

       while(scanner.hasNextInt())
       {
          int value = scanner.nextInt();
          testHash(hash, value, data);
       }
    }

    private static class bloomFilter {

        // grab BloomFilter data
	int m_;
	short k_;
	int keyInfo_;

        //  VarUIntArray data:
	int numEntries_;
	int bits_;
	int maxVal_;
	int numWords_;

        // the hash function 
        hdpHash hash_;
        

        // The hash table
        BitSet bitSet_;


       // bb holds the original source data from which
       // this bloom filter is created. 
       public bloomFilter(ByteBuffer bb) 
       {

        // BloomFilter data
	m_ = bb.getInt();
	k_ = bb.getShort();
	keyInfo_ = bb.getInt();

        // skip the estimated number of keys inserted
	bb.getInt();

        //  VarUIntArray data:
	numEntries_ = bb.getInt();
	bits_ = bb.getInt();
	maxVal_ = bb.getInt();
	numWords_ = bb.getInt();


        hash_ = new hdpHash();
        bitSet_ = new BitSet(numEntries_);

        int start = bb.position();

        // turn on bits in the hash table by going
        // through each word. The bits are arranged
        // in the following manner: 
        //
        // 0 1 2 3 4 5 .... 30 31
        //
        int wordPos = 0;
        for (int i=0; i<numWords_; i++ ) {
	   int word = bb.getInt();

           if ( word != 0 ) {
                  
             for ( int bit=0; bit<32; bit++ ) {
                if ( (word & 0x80000000) == 0x80000000 ) {
                   bitSet_.set(wordPos + bit);
                }
                word <<= 1; // shift left 
             }
           }
           wordPos += 32;
        }

        //report(bb, start);
        
       } // end of cstr

       int entries() { return numEntries_; }
       short hashFuncs() { return k_; }

       boolean lookup(byte[] key)
       {
          int hashValueCommon = 
                hash_.computeHash(key, hdpHash.NO_FLAGS);

          for(int i=0; i<k_; i++)
          {
             int hash_index = 
                hash_.computeFinalHash(hashValueCommon, i, m_);

             if ( !bitSet_.get(toIntExact(hash_index)) ) // is the bit on?
               return false;
          }
          return true;
       }

       // buf is passed to help alter the byte order 
       // without allocating a new buf array every time 
       // of the call.
       boolean lookup(int value, byte[] buf)
       {
          // Cast to big endian byte order
          buf[0] =  (byte)(value >> 24);
          buf[1] =  (byte)(value >> 16);
          buf[2] =  (byte)(value >> 8);
          buf[3] =  (byte)value;
          
          return lookup(buf);
       }

       // compute a signature for the bits data
       public long computeFNVhash(ByteBuffer bb, int start)
       {
          if (bb == null)
             return 0;

          bb.rewind();
          bb.position(start);

          long prime = 16777619;
          long hash = 0x811c9dc5;  // offset basis
   
   	  for (int i = 0; i < numWords_; i++ ) {
              long uint32Word = Integer.toUnsignedLong(bb.getInt());
   
              hash ^= uint32Word;
   
              hash *= prime;
              hash &= 0xFFFFFFFFL;
           }

           return hash;
       }
   
       // produce a report for the bloom filter.to help
       // verify that the data is intact. 
       public void report(ByteBuffer bb, int start)
       {
          long hash = computeFNVhash(bb, start);

          System.out.println("Bloomfilter"
   			 + ", m= " + m_
   			 + ", k= " + k_ 
   			 + ", keyInfo= " + keyInfo_ 
   			 + ", numEntries=" + numEntries_
   			 + ", bits=" + bits_
   			 + ", maxVal=" + maxVal_
   			 + ", numWords=" + numWords_
   			 + ", FNV hash(hex)=" + Long.toHexString(hash)
                      );

          if ( bb != null ) {
             bb.rewind();
             bb.position(start);

             for (int i=0; i<numWords_; i++ ) {
                int x = bb.getInt();
                if ( x != 0 ) {
                   System.out.println(
                        "word[" + i + "]=" +
   			 Integer.toHexString(x) 
                                     );
                }
             }
          }

          for (int i=bitSet_.nextSetBit(0); i >= 0; 
                             i=bitSet_.nextSetBit(i+1)) {
             System.out.println("bits[" + i + "]=1");
          } 
       }
    }

    // test multi hash, a step needed to compute k hash functions
    public static void testMultiHashInt(String datafile, int funcs, int bytes)
          throws FileNotFoundException, IOException
    {
          Scanner scanner = new Scanner(new File(datafile));

          byte[] buf = new byte[4];

          hdpHash hashGen = new hdpHash();

          int ct = 0;
          while(scanner.hasNextInt())
          {
             scanner.nextInt();
             ct++;
          }

          int[] ints = new int[ct];
          int j = 0;

          scanner = new Scanner(new File(datafile));
          while(scanner.hasNextInt())
          {
             ints[j] = scanner.nextInt();
             j++;
          }

          System.out.print("multi hash test result for " + ct + " ints");

          long startTime = System.currentTimeMillis();
          for (j=0; j<ct; j++ )
          {
             int value = ints[j];

             System.out.println("value=" + value);

             // Cast to big endian byte order
             buf[0] =  (byte)(value >> 24);
             buf[1] =  (byte)(value >> 16);
             buf[2] =  (byte)(value >> 8);
             buf[3] =  (byte)value;

             int hashValueCommon = 
                hashGen.computeHash(buf, hdpHash.NO_FLAGS);

             System.out.println(", commonHash=" +
                             Integer.toHexString(hashValueCommon));

             for(int i=0; i<funcs; i++)
             {
                int hash_index = hashGen.computeFinalHash(hashValueCommon, i, bytes*8);

                System.out.println("hash(" + i + ")=" + 
                            Integer.toHexString(hash_index));
             }
          }
          long elapsedTime = System.currentTimeMillis() - startTime;
          System.out.println(", ET=" + elapsedTime + "ms");
     }

    public static void testMultiHashTimestamp(String datafile, int funcs, int numBytes)
          throws FileNotFoundException, IOException
    {
          Scanner scanner = new Scanner(new File(datafile));

          byte[] buf = new byte[4];

          hdpHash hashGen = new hdpHash();

          int ct = 0;
          while(scanner.hasNextLine())
          {
             scanner.nextLine();
             ct++;
          }

          Timestamp[] timestamps = new Timestamp[ct];
          int j = 0;

          scanner = new Scanner(new File(datafile));
          while(scanner.hasNextLine())
          {
             String str = scanner.nextLine();
                    
             Timestamp ts = Timestamp.valueOf(str);
             long microsec = (ts.getTime()/1000)*1000000 + (ts.getNanos()/1000);

             System.out.println("timestamp=" + str + " (" + microsec + " us)");

             timestamps[j++] = ts;
          }

          System.out.println("multi hash test result for " + ct + " timestamps.");

          long startTime = System.currentTimeMillis();

          for (j=0; j<ct; j++ )
          {
             String str = timestamps[j].toString();

             System.out.println("data=" + str);

             byte[] bytes = str.getBytes();

             int hashValueCommon = 
                hashGen.computeHash(bytes, hdpHash.NO_FLAGS);

             System.out.println(": commonHash=" +
                                   Integer.toHexString(hashValueCommon));

             for(int i=0; i<funcs; i++)
             {
                int hash_index = hashGen.computeFinalHash(hashValueCommon, i, numBytes*8);

                System.out.println("hash(" + i + ")=" + 
                            Integer.toHexString(hash_index));
             }
          }

          long elapsedTime = System.currentTimeMillis() - startTime;
          System.out.println(", ET=" + elapsedTime + "ms");
     }

     public void testInts(bloomFilter bf, String datafile)
          throws FileNotFoundException
     {
          Scanner scanner = new Scanner(new File(datafile));
          byte[] buf = new byte[4];

          while(scanner.hasNextInt())
          {
             int value = scanner.nextInt();
             System.out.print(value);
             if ( bf.lookup(value, buf) )  
                 System.out.println(" in bf.");
             else
                 System.out.println(" not in bf.");
          }
      }

    // The format of byte buffer for a bloom filter 
    // ppi_vec entry: 
    //
    //    operatorType: 4 bytes. 
    //    colNameLen:   4 bytes. 
    //    colName:      colNameLen bytes.
    //    operandType:  4 bytes
    //    total length of the min value (4 bytes) and the min value
    //    total length of the max value (4 bytes) and the max value
    //    total length of the BloomFilter and 
    //      VarUIntArray data:  4 bytes
    //    BloomFilter data:
    //       m_ (hash table size in bits): 4 bytes 
    //       k_ (# of hash functions): 2 bytes 
    //       keyLenInfo_ (FLAGS): 4 bytes 
    //    VarUIntArray data:
    //       numEntries(total # of bits): 4 bytes  
    //       bits/entry: 4 bytes  
    //       maxValue in each entry: 4 bytes  
    //       # of 32-bit words: 4 bytes  
    //       hash table: 4 * (# of 32-bit words)
    private FilterPredicate genBloomFilterPred(int colIdx,
				      Object[] ppi_vec)
        throws FileNotFoundException
    {
        filterColumnType = -1;
        filterColName = null;
        filterOperandObject = null;

        ByteBuffer bb = ByteBuffer.wrap((byte[])ppi_vec[colIdx]);
        bb.order(m_byteorder);
        
        int operatorType = bb.getInt();
        int colNameLen = bb.getInt();
        byte[] colName = null;
        int colIndex = -1;
        filterColumnType = -1;

        if (colNameLen > 0) {
            colName = new byte[colNameLen];
            bb.get(colName, 0, colNameLen);
            filterColName = Bytes.toString(colName);

            colIndex = listOfAllColName.indexOf(filterColName);
            if (colIndex != -1) {
                filterColumnType = listOfAllColType.get(colIndex);
		if (logger.isTraceEnabled()) 
		    logger.trace("BLOOMFILTER: " 
				 + ", colIndex: " + colIndex
				 + ", colName: " + filterColName
				 + ", colType: " + filterColumnType
				 );
            } // if
        } // if
        
        if (filterColumnType == -1)
            return null; 

	Column lv_column_name = null;
        int operandType = bb.getInt();

        int filterId = 0;


        int filterIdLen = bb.getInt();
        if ( filterIdLen > 0) {
            byte[] filterIdArray = new byte[filterIdLen];
            bb.get(filterIdArray, 0, filterIdLen);
            filterId = Integer.parseInt(Bytes.toString(filterIdArray));
        }

        String minVal = new String();
        int minValLen = bb.getInt();
        if ( minValLen > 0) {
            byte[] minValArray = new byte[minValLen];
            bb.get(minValArray, 0, minValLen);
            minVal = Bytes.toString(minValArray);
        }

        String maxVal = new String();
        int maxValLen = bb.getInt();
        if ( maxValLen > 0) {
            byte[] maxValArray = new byte[maxValLen];
            bb.get(maxValArray, 0, maxValLen);
            maxVal = Bytes.toString(maxValArray);
        }

	int lv_num_bytes = bb.getInt(); // size of the BF

	if (logger.isTraceEnabled()) 
	    logger.trace("BLOOMFILTER"
			 + ", fc type: " + filterColumnType 
			 + ", operand type: " + filterColumnType 
			 + ", colName: " + filterColName
			 + ", minVal: " + minVal
			 + ", maxVal: " + maxVal
			 + ", num bytes: " + lv_num_bytes
			 );

        bloomFilter bf = new bloomFilter(bb);

	FilterPredicate fapi = null;
	switch (filterColumnType) {
	    /*
	case HIVE_BOOLEAN_TYPE:
	    lv_column_name = FilterApi.booleanColumn(filterColName);
	    fapi = FilterApi.userDefined(lv_column_name, new SetBloomFilterBoolean(bf));
	    break;

	case HIVE_BYTE_TYPE:
	    lv_column_name = FilterApi.intColumn(filterColName);
	    fapi = FilterApi.userDefined(lv_column_name, new SetBloomFilterByte(bf));
	    break;
	    */

	case HIVE_SHORT_TYPE:
	    lv_column_name = FilterApi.intColumn(filterColName);
	    fapi = FilterApi.userDefined(lv_column_name, new SetBloomFilterShort(bf, Short.parseShort(minVal), Short.parseShort(maxVal), filterId, m_dump_debug_data));
	    break;

	case HIVE_INT_TYPE:
	    lv_column_name = FilterApi.intColumn(filterColName);
	    SetBloomFilterInteger lv_bf_i = new SetBloomFilterInteger(bf, Integer.parseInt(minVal), Integer.parseInt(maxVal), filterId, m_dump_debug_data);
	    fapi = FilterApi.userDefined(lv_column_name, lv_bf_i);
	    lv_bf_i.setTimer(m_bf_i_timer);
	    break;

	case HIVE_LONG_TYPE:
	    lv_column_name = FilterApi.longColumn(filterColName);
	    SetBloomFilterLong lv_bf_l = new SetBloomFilterLong(bf, Long.parseLong(minVal), Long.parseLong(maxVal), filterId, m_dump_debug_data);
	    fapi = FilterApi.userDefined(lv_column_name, lv_bf_l);
	    lv_bf_l.setTimer(m_bf_l_timer);
	    break;

	case HIVE_FLOAT_TYPE:
	    lv_column_name = FilterApi.floatColumn(filterColName);
	    fapi = FilterApi.userDefined(lv_column_name, new SetBloomFilterFloat(bf, Float.parseFloat(minVal), Float.parseFloat(maxVal), filterId));
	    break;

	case HIVE_DOUBLE_TYPE:
	    lv_column_name = FilterApi.doubleColumn(filterColName);
	    fapi = FilterApi.userDefined(lv_column_name, new SetBloomFilterDouble(bf, Double.parseDouble(minVal), Double.parseDouble(maxVal), filterId));
	    break;

	case HIVE_CHAR_TYPE:
	case HIVE_VARCHAR_TYPE:
	case HIVE_STRING_TYPE:
	case HIVE_BINARY_TYPE:
        case HIVE_DECIMAL_TYPE:
	case HIVE_TIMESTAMP_TYPE:
	    lv_column_name = FilterApi.binaryColumn(filterColName);
	    fapi = FilterApi.userDefined(lv_column_name, new SetBloomFilterBinary(bf, minVal, maxVal, filterId, filterColumnType, m_dump_debug_data));
	    break;
	}

        //bf.report(null, 0);
        //testInts(bf, "h3a.data");

        return fapi;
    }

    // format of bb buffer for each ppi_vec entry
    //    operatorType: 4 bytes. 
    //    colNameLen:   4 bytes. 
    //    colName:      colNameLen bytes.
    //    operandType:  4 bytes
    //    numInEntries  4 bytes
    //    <operand type, operandVal>...
    private FilterPredicate genInPred(int colIdx,
				      Object[] ppi_vec)
    {
        filterColumnType = -1;
        filterColName = null;
        filterOperandObject = null;

        ByteBuffer bb = ByteBuffer.wrap((byte[])ppi_vec[colIdx]);
        bb.order(m_byteorder);
        
        int operatorType = bb.getInt();
        int colNameLen = bb.getInt();
        byte[] colName = null;
        int colIndex = -1;
        filterColumnType = -1;

        if (colNameLen > 0) {
            colName = new byte[colNameLen];
            bb.get(colName, 0, colNameLen);
            filterColName = Bytes.toString(colName);

            colIndex = listOfAllColName.indexOf(filterColName);
            if (colIndex != -1) {
                filterColumnType = listOfAllColType.get(colIndex);
		if (logger.isTraceEnabled()) 
		    logger.trace("IN Filter: " 
				 + ", colIndex: " + colIndex
				 + ", colName: " + filterColName
				 + ", colType: " + filterColumnType
				 );
            } // if
        } // if
        
        if (filterColumnType == -1)
            return null; 

	Column lv_column_name = null;
	HashSet lv_hash_set = null;
	switch (filterColumnType) {
	case HIVE_BOOLEAN_TYPE:
	    lv_column_name = FilterApi.booleanColumn(filterColName);
	    lv_hash_set = new HashSet<Boolean>();
	    break;
        case HIVE_BYTE_TYPE:
        case HIVE_SHORT_TYPE:
	case HIVE_INT_TYPE:
	    lv_column_name = FilterApi.intColumn(filterColName);
	    lv_hash_set = new HashSet<Integer>();
	    break;
	case HIVE_LONG_TYPE:
	    lv_column_name = FilterApi.longColumn(filterColName);
	    lv_hash_set = new HashSet<Long>();
	    break;
        case HIVE_FLOAT_TYPE:
	    lv_column_name = FilterApi.floatColumn(filterColName);
	    lv_hash_set = new HashSet<Float>();
	    break;
        case HIVE_DOUBLE_TYPE:
	    lv_column_name = FilterApi.doubleColumn(filterColName);
	    lv_hash_set = new HashSet<Double>();
	    break;
        case HIVE_CHAR_TYPE:
        case HIVE_VARCHAR_TYPE:
        case HIVE_STRING_TYPE:
	case HIVE_BINARY_TYPE:
	    lv_column_name = FilterApi.binaryColumn(filterColName);
	    lv_hash_set = new HashSet<Binary>();
	    break;
	}

        int operandType = bb.getInt();
	int lv_num_entries = bb.getInt();
	if (logger.isTraceEnabled()) 
	    logger.trace("IN"
			 + ", fc type: " + filterColumnType 
			 + ", operand type: " + filterColumnType 
			 + ", colName: " + filterColName
			 + ", num IN entries: " + lv_num_entries
			 );

	for (int i = 0; i < lv_num_entries; i++ ) {
      
          int operandLen = bb.getInt();
          byte[] operator_ba = null;

          if (logger.isTraceEnabled()) 
             logger.trace("operand length= " + operandLen);
  
          if (operandLen > 0) {
             operator_ba = new byte[operandLen];
             bb.get(operator_ba, 0, operandLen);
          } else {
              operator_ba = new byte[0];
          }
  
          if ( operandType == 1 ) {
	      // TBD             operandObject = 
	      //  java.sql.Timestamp.valueOf(Bytes.toString(operator_ba));
          } else {
	      switch (filterColumnType) {
	      case HIVE_BOOLEAN_TYPE:
    	          boolean lv_boolean = Boolean.parseBoolean(new String(operator_ba));
		  lv_hash_set.add(lv_boolean);
		  break;

	      case HIVE_BYTE_TYPE:
	      case HIVE_SHORT_TYPE:
	      case HIVE_INT_TYPE:
    	          int lv_int = Integer.parseInt(new String(operator_ba));
		  lv_hash_set.add(lv_int);
		  break;

	      case HIVE_LONG_TYPE:
    	          long lv_long = Long.parseLong(new String(operator_ba));
		  lv_hash_set.add(lv_long);
		  break;

	      case HIVE_FLOAT_TYPE:
    	          float lv_float = Float.parseFloat(new String(operator_ba));
		  lv_hash_set.add(lv_float);
		  break;

	      case HIVE_DOUBLE_TYPE:
    	          double lv_double = Double.parseDouble(new String(operator_ba));
		  lv_hash_set.add(lv_double);
		  break;

	      case HIVE_BINARY_TYPE:
		  lv_hash_set.add(Binary.fromByteArray(operator_ba));
		  break;

	      case HIVE_CHAR_TYPE:
	      case HIVE_VARCHAR_TYPE:
	      case HIVE_STRING_TYPE:
		  String strVal = null;
		  if ((operandLen == 0) && (operator_ba == null)) {
		      operator_ba = new byte[0];
		      strVal = Bytes.toString(operator_ba);
		  } else if (operator_ba != null) {
		      strVal = Bytes.toString(operator_ba);
		      strVal = strVal.replaceAll("\\s*$", "");
		  }
		  lv_hash_set.add(Binary.fromString(strVal));
		  break;

	      }
          }
	}

	if (logger.isTraceEnabled()) 
	    logger.trace("genInPred"
			 + ", #entries in hashset: " + lv_hash_set.size()
			 );

	FilterPredicate fapi = null;
	if (lv_hash_set.size() > 0) {
	    switch (filterColumnType) {
	    case HIVE_BOOLEAN_TYPE:
		fapi = FilterApi.userDefined(lv_column_name, new SetInFilter<Boolean>(lv_hash_set));
	    break;

	    case HIVE_BYTE_TYPE:
	    case HIVE_SHORT_TYPE:
	    case HIVE_INT_TYPE:
		fapi = FilterApi.userDefined(lv_column_name, new SetInFilter<Integer>(lv_hash_set));
	    break;

	    case HIVE_LONG_TYPE:
		fapi = FilterApi.userDefined(lv_column_name, new SetInFilter<Long>(lv_hash_set));
		break;

	    case HIVE_FLOAT_TYPE:
		fapi = FilterApi.userDefined(lv_column_name, new SetInFilter<Float>(lv_hash_set));
		break;

	    case HIVE_DOUBLE_TYPE:
		fapi = FilterApi.userDefined(lv_column_name, new SetInFilter<Double>(lv_hash_set));
		break;

	    case HIVE_CHAR_TYPE:
	    case HIVE_VARCHAR_TYPE:
	    case HIVE_STRING_TYPE:
	    case HIVE_BINARY_TYPE:
		fapi = FilterApi.userDefined(lv_column_name, new SetInFilter<Binary>(lv_hash_set));
		break;
	    }
	}

        return fapi;
    }

    public void checkAndThrowIfNull(int columnType, String columnName, Object operVal, String operation)
	throws IOException
    {
	if (operVal == null) {
	    throw new IOException("Null value received when creating filter for parquet"
				  + ", table: " + m_table_name
				  + ", column name: " + columnName
				  + ", column type: " + columnType
				  + ", operation: " + operation
				  );
	}

    }

    // Used for '<col> = <val>' and '<col> is null' predicates.
    // For 'is null', operVal will be null.    
    private FilterPredicate genEqPred(int columnType, String columnName, Object operVal) 
	throws IOException
    {
        FilterPredicate fapi = null;
        switch (columnType) {
        case HIVE_BOOLEAN_TYPE:
            fapi = FilterApi.eq(FilterApi.booleanColumn(columnName), (Boolean)operVal);
            break;

        case HIVE_BYTE_TYPE:
        case HIVE_SHORT_TYPE: 
        case HIVE_INT_TYPE: 
            fapi = FilterApi.eq(FilterApi.intColumn(columnName), (Integer)operVal);
            break;
 
        case HIVE_LONG_TYPE: 
            fapi = FilterApi.eq(FilterApi.longColumn(columnName), (Long)operVal);
            break;
           
        case HIVE_FLOAT_TYPE: 
            fapi = FilterApi.eq(FilterApi.floatColumn(columnName), (Float)operVal);
            break;

        case HIVE_DOUBLE_TYPE: 
            fapi = FilterApi.eq(FilterApi.doubleColumn(columnName), (Double)operVal);
            break;

        case HIVE_BINARY_TYPE:
        case HIVE_CHAR_TYPE:
        case HIVE_VARCHAR_TYPE:
        case HIVE_STRING_TYPE:
        case HIVE_DECIMAL_TYPE:
            fapi = FilterApi.eq(FilterApi.binaryColumn(columnName), (Binary)operVal);
            break;

	    /* for 64bit timestamp
        case HIVE_TIMESTAMP_TYPE:
            fapi = FilterApi.eq(FilterApi.longColumn(columnName), (Long)operVal);
	    break;
	    */

	case HIVE_TIMESTAMP_TYPE:
	    Binary bin = (Binary)operVal;
	    if (logger.isDebugEnabled()) logger.debug("genEqPred"
						      + ", bin: " + bin
						      );
	    fapi = FilterApi.eq(FilterApi.binaryColumn(columnName), bin);
            break;

        } // switch
        return fapi;
    } // genEqPred

    private FilterPredicate genLessThanPred(int columnType, String columnName, Object operVal) 
       throws IOException
    {
	checkAndThrowIfNull(columnType, columnName, operVal, "LessThan");

        FilterPredicate fapi = null;
        switch (columnType) {
        case HIVE_BYTE_TYPE:
        case HIVE_SHORT_TYPE: 
        case HIVE_INT_TYPE: 
            fapi = FilterApi.lt(FilterApi.intColumn(columnName), (Integer)operVal);
            break;
 
        case HIVE_LONG_TYPE: 
            fapi = FilterApi.lt(FilterApi.longColumn(columnName), (Long)operVal);
            break;
           
        case HIVE_FLOAT_TYPE: 
            fapi = FilterApi.lt(FilterApi.floatColumn(columnName), (Float)operVal);
            break;

        case HIVE_DOUBLE_TYPE: 
            fapi = FilterApi.lt(FilterApi.doubleColumn(columnName), (Double)operVal);
            break;

        case HIVE_BINARY_TYPE:
        case HIVE_CHAR_TYPE:
        case HIVE_VARCHAR_TYPE:
        case HIVE_STRING_TYPE:
        case HIVE_DECIMAL_TYPE:
            fapi = FilterApi.lt(FilterApi.binaryColumn(columnName), (Binary)operVal);
            break;

	    /* for 64bit timestamp
        case HIVE_TIMESTAMP_TYPE:
          fapi = FilterApi.lt(FilterApi.longColumn(columnName), (Long)operVal);                
            break;
	    */

	case HIVE_TIMESTAMP_TYPE:
	    Binary bin = (Binary)operVal;
	    if (logger.isDebugEnabled()) logger.debug("genLessThanPred"
						      + ", bin: " + bin
						      );
	    fapi = FilterApi.lt(FilterApi.binaryColumn(columnName), bin);
            break;
        } // switch
        return fapi;
    } // genLessThanPred

    private FilterPredicate genLessThanEqualsPred(int columnType, String columnName, Object operVal) 
	throws IOException
    {
	checkAndThrowIfNull(columnType, columnName, operVal, "LessThanEqual");

        FilterPredicate fapi = null;
        switch (columnType) {
        case HIVE_BYTE_TYPE:
        case HIVE_SHORT_TYPE: 
        case HIVE_INT_TYPE: 
            fapi = FilterApi.ltEq(FilterApi.intColumn(columnName), (Integer)operVal);
            break;
 
        case HIVE_LONG_TYPE: 
            fapi = FilterApi.ltEq(FilterApi.longColumn(columnName), (Long)operVal);
            break;
           
        case HIVE_FLOAT_TYPE: 
            fapi = FilterApi.ltEq(FilterApi.floatColumn(columnName), (Float)operVal);
            break;

        case HIVE_DOUBLE_TYPE: 
            fapi = FilterApi.ltEq(FilterApi.doubleColumn(columnName), (Double)operVal);
            break;

        case HIVE_BINARY_TYPE:
        case HIVE_CHAR_TYPE:
        case HIVE_VARCHAR_TYPE:
        case HIVE_STRING_TYPE:
        case HIVE_DECIMAL_TYPE:
            fapi = FilterApi.ltEq(FilterApi.binaryColumn(columnName), (Binary)operVal);
            break;

	    /* for 64bit timestamp
        case HIVE_TIMESTAMP_TYPE:
            fapi = FilterApi.ltEq(FilterApi.longColumn(columnName), (Long)operVal);
	    break;
	    */

	case HIVE_TIMESTAMP_TYPE:
	    Binary bin = (Binary)operVal;
	    if (logger.isDebugEnabled()) logger.debug("genLessThanEqualsPred"
						      + ", bin: " + bin
						      );
	    fapi = FilterApi.ltEq(FilterApi.binaryColumn(columnName), bin);
            break;

        } // switch
        return fapi;
    } // genLessThanEqualsPred

    private FilterPredicate makeFilterPred(Object[] ppi_vec) 
	throws IOException
    {
        currFilterPPIidx++;
        if (currFilterPPIidx == ppi_vec.length) {
            return null;
        }

        ByteBuffer bb = ByteBuffer.wrap((byte[])ppi_vec[currFilterPPIidx]);
        bb.order(m_byteorder);
        
        int operatorType = bb.getInt();

        FilterPredicate fp = null;

        // System.out.print("operatortype=" + operatorType);
        // System.out.println(", current idx=" + currFilterPPIidx);
        //


        switch (operatorType) {

        case STARTAND: 
            {
                // System.out.println("type: STARTAND");
                FilterPredicate andFP = makeFilterPred(ppi_vec);

                if (andFP == null) 
                    break;

                FilterPredicate rightFP = null;

                while ( (rightFP = makeFilterPred(ppi_vec)) != null ) {
                   andFP = FilterApi.and(andFP, rightFP);
                }
                
                fp = filterPred = andFP;

                // If we reach here, it means that we have consume
                // the matching END clause in the while loop above.
            }
            break;
            
        case STARTOR: 
            {
                // System.out.println("type: STARTOR");
                FilterPredicate orFP = makeFilterPred(ppi_vec);

                if (orFP == null)
                    break;

                FilterPredicate rightFP = null;

                while ( (rightFP = makeFilterPred(ppi_vec)) != null ) {
                   orFP = FilterApi.or(orFP, rightFP);
                }

                filterPred = fp = orFP;
                
                // If we reach here, it means that we have consume
                // the matching END clause in the while loop above.
            }
            break;

        case STARTNOT:
            {
                // System.out.println("type: STARTNOT");

                FilterPredicate childFP = makeFilterPred(ppi_vec);

                if (childFP == null)
                    break;

                filterPred = fp = FilterApi.not(childFP);
                
                // skip the END clause
                makeFilterPred(ppi_vec);
            }
            break;
            
        case EQUALS:
            {
                // System.out.println("type: EQUALS");

                if (! extractFilterInfo(currFilterPPIidx, ppi_vec))
                    break;

                fp = genEqPred(filterColumnType, filterColName, filterOperandObject);
            }
            break;

        case LESSTHAN:
            {
                // System.out.println("type: LESSTHAN");

                if (! extractFilterInfo(currFilterPPIidx, ppi_vec))
                    break;

                fp = genLessThanPred(filterColumnType, filterColName, filterOperandObject);
            }
            break;

        case LESSTHANEQUALS:
            {
                //System.out.println("type: LESSTHANEQUALS");

                if (! extractFilterInfo(currFilterPPIidx, ppi_vec))
                    break;

                fp = genLessThanEqualsPred(filterColumnType, filterColName, filterOperandObject);
            }
            break;

        case ISNULL:
            {
                // System.out.println("type: ISNULL");
                
                if (! extractFilterInfo(currFilterPPIidx, ppi_vec))
                    break;

                fp = genEqPred(filterColumnType, filterColName, null);
            }
            break;
	    
	case IN:
	    {
                fp = genInPred(currFilterPPIidx, ppi_vec);

		if (logger.isDebugEnabled()) 
		    logger.debug("makeFilterPred"
				 + ", operatorType: IN"
				 + ", filterColType: " + filterColumnType
				 + ", filterColName: " + filterColName
				 );
	    }
	    break;

	case BF:
	    {
                fp = genBloomFilterPred(currFilterPPIidx, ppi_vec);

		if (logger.isDebugEnabled()) 
		    logger.debug("makeFilterPred"
				 + ", operatorType: BLOOMFILTER"
				 + ", filterColType: " + filterColumnType
				 + ", filterColName: " + filterColName
				 );
	    }
	    break;

        case END:
            {
                // System.out.println("type: END");
                fp = null;
            }
            break;

        } // switch

        // System.out.println("fp to return=" + fp);
        return fp;
    }


    public static class SetInFilter<T extends Comparable<T>> extends UserDefinedPredicate<T> implements Serializable {
	
	private HashSet<T> hSet_;

	public SetInFilter(HashSet<T> phSet) {
	    hSet_ = phSet;
	}
	
	@Override
	    public boolean keep(T value) {
	    if (value == null) {
		return false;
	    }

	    if (hSet_ == null) {
		return false;
	    }

	    return hSet_.contains(value);
	}

	@Override
	    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistics) {
	    return false;
	}

	@Override
	    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistics) {
	    return false;
	}
    }

    public static String getPrintHandle()
    {
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null)
        {
           String pid = ManagementFactory.getRuntimeMXBean().getName();

           // pid looks like "15502@a.b.com"
           String[] parts = pid.split("@");

           String handle = new String(hostname + "." + parts[0]);

           return handle;
        } else
        {
           return "default";
        }
    }

    public static int[] fetchDatetimeFields(Binary pv_val) 
    {
	int[] dtFields = getDatetimeFields(pv_val);
        return dtFields;
    }

    public static void dumpKeep(int filterId, int value, boolean result)
    {
       try {
         FileWriter writer = new FileWriter(getPrintHandle(), true);
         writer.append("RV" + filterId + ": lookupInParqueR(" + value + ")=" + result + System.lineSeparator());
         writer.close();
       }
       catch (java.io.IOException e)
       {
       }
    }

    public static void dumpKeep(int filterId, short value, boolean result)
    {
       try {
         FileWriter writer = new FileWriter(getPrintHandle(), true);
         writer.append("RV" + filterId + ": lookupInParqueR(" + value + ")=" + result + System.lineSeparator());
         writer.close();
       }
       catch (java.io.IOException e)
       {
       }
    }

    public static void dumpKeep(int filterId, Long value, boolean result)
    {
       try {
         FileWriter writer = new FileWriter(getPrintHandle(), true);
         writer.append("RV" + filterId + ": lookupInParqueR(" + value + ")=" + result + System.lineSeparator());
         writer.close();
       }
       catch (java.io.IOException e)
       {
       }
    }

    public static void dumpKeep(int filterId, Binary value, boolean result)
    {
       try {
         FileWriter writer = new FileWriter(getPrintHandle(), true);
         writer.append("RV" + filterId + ": lookupInParqueR(" + 
                       value.toStringUsingUTF8() + 
                       ")=" + result + System.lineSeparator());
         writer.close();
       }
       catch (java.io.IOException e)
       {
       }
    }

    public static void dumpKeep(int filterId, String value, boolean result)
    {
       try {
         FileWriter writer = new FileWriter(getPrintHandle(), true);
         writer.append("RV" + filterId + ": lookupInParqueR(" + 
                       value + 
                       ")=" + result + System.lineSeparator());
         writer.close();
       }
       catch (java.io.IOException e)
       {
       }
    }

    public static void dumpCanDrop(int filterId, int min, int max, 
                             int stats_min, int stats_max, boolean result)
    {
       try {
         FileWriter writer = new FileWriter(getPrintHandle(), true);
         writer.append("RV" + filterId + ": canDropInParqueR(" + 
                       "rg(min,max)=" + min + ", " + max + 
                       ", stats(min,max)=" +
                       stats_min + ", " + stats_max + 
                       ")=" + result + 
                      System.lineSeparator());
         writer.close();
       }
       catch (java.io.IOException e)
       {
       }
    }

    public static void dumpCanDrop(int filterId, short min, short max, 
                                short stats_min, short stats_max,
                                boolean result)
    {
       try {
         FileWriter writer = new FileWriter(getPrintHandle(), true);
         writer.append("RV" + filterId + ": canDropInParqueR(" + 
                       "rg(min,max)=" + min + ", " + max + 
                       ", stats(min,max)=" +
                       stats_min + ", " + stats_max + 
                       ")=" + result + 
                      System.lineSeparator());
         writer.close();
       }
       catch (java.io.IOException e)
       {
       }
    }

    public static void dumpCanDrop(int filterId, long min, long max, 
                                long stats_min, long stats_max,
                                boolean result)
    {
       try {
         FileWriter writer = new FileWriter(getPrintHandle(), true);
         writer.append("RV" + filterId + ": canDropInParqueR(" + 
                       "rg(min,max)=" + min + ", " + max + 
                       ", stats(min,max)=" +
                       stats_min + ", " + stats_max + 
                       ")=" + result + 
                      System.lineSeparator());
         writer.close();
       }
       catch (java.io.IOException e)
       {
       }
    }

    public static void dumpCanDrop(int filterId, String min, String  max, 
                                Binary stats_min, Binary stats_max,
                                boolean result)
    {
       try {
         FileWriter writer = new FileWriter(getPrintHandle(), true);
         writer.append("RV" + filterId + ": canDropInParqueR(" + 
                       "rg(min,max)=" + min + ", " + 
                       max + 
                       ", stats(min,max)=" +
                       stats_min.toStringUsingUTF8() + ", " + 
                       stats_max.toStringUsingUTF8() + 
                       ")=" + result + 
                      System.lineSeparator());
         writer.close();
       }
       catch (java.io.IOException e)
       {
       }
    }

    public static class SetBloomFilterShort extends UserDefinedPredicate<Integer> implements Serializable {

	private short min_;
	private short max_;
	private bloomFilter m_bf_;
        private ByteBuffer m_bb_ = null;
        private int m_filterId_;
        private boolean m_dump_debug_data_;
	
	public SetBloomFilterShort(bloomFilter p_bf, short min, short max, int filterId, boolean debug) {
	    min_ = min;
	    max_ = max;
	    m_bf_ = p_bf;
	    m_bb_ = ByteBuffer
		.allocate(2)
		.order(ByteOrder.LITTLE_ENDIAN);
	    m_filterId_ = filterId;
            m_dump_debug_data_ = debug;
	}
	
	@Override
	    public boolean keep(Integer value) {
	    if (value == null) {
		return false;
	    }
	    
	    m_bb_.clear();
	    m_bb_.putShort(value.shortValue());
	    boolean result = m_bf_.lookup(m_bb_.array());

            if ( m_dump_debug_data_ )
              dumpKeep(m_filterId_, value, result);

            return result;
	}

	@Override
	    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics) {

            if ( m_dump_debug_data_ )
               dumpCanDrop(m_filterId_, min_, max_,
                         statistics.getMin(), statistics.getMax(),
                         statistics.getMax() < min_ || max_ < statistics.getMin() 
                          );

            return ( statistics.getMax() < min_ ||
                     max_ < statistics.getMin() );
	}

	@Override
	    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics) {
	    return false;
	}
    }

    public static class SetBloomFilterInteger extends UserDefinedPredicate<Integer> implements Serializable {

	private int min_;
	private int max_;
	private bloomFilter m_bf_;
        private ByteBuffer m_bb_ = null;
	timer   m_timer_ = null;
        private int m_filterId_;
        private boolean m_dump_debug_data_;
	
	public SetBloomFilterInteger(bloomFilter p_bf, int min, int max, int filterId, boolean debug) {
	    min_ = min;
	    max_ = max;
	    m_bf_ = p_bf;
	    m_bb_ = ByteBuffer
		.allocate(4)
		//		.order(ByteOrder.LITTLE_ENDIAN)
		;
	    m_filterId_ = filterId;
            m_dump_debug_data_ = debug;
	}

	public void setTimer(timer p_timer) {
	    m_timer_ = p_timer;
	}

	@Override
	    public boolean keep(Integer value) {

	    if (value == null) {
		return false;
	    }
	    
	    if (m_timer_ != null) {
		m_timer_.start2();
	    }
	    
	    m_bb_.clear();
	    m_bb_.putInt(value);

	    boolean lv_result = m_bf_.lookup(m_bb_.array());

            if ( m_dump_debug_data_ )
               dumpKeep(m_filterId_, value, lv_result);

	    if (m_timer_ != null) {
		m_timer_.stop2();
	    }
	    
	    return lv_result;
	    /* return m_bf_.lookup(value, m_ba_); */
	}

	@Override
	    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics) {

            if ( m_dump_debug_data_ )
             dumpCanDrop(m_filterId_, min_, max_,
                statistics.getMin(), statistics.getMax(),
                statistics.getMax() < min_ || max_ < statistics.getMin() 
               );

            return ( statistics.getMax() < min_ ||
                     max_ < statistics.getMin() );
	}

	@Override
	    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics) {
	    return false;
	}
    }

    public static class SetBloomFilterLong extends UserDefinedPredicate<Long> implements Serializable {

	private long min_;
	private long max_;
	private bloomFilter m_bf_;
        private ByteBuffer m_bb_ = null;
	timer   m_timer_ = null;
        private int m_filterId_;
        private boolean m_dump_debug_data_;
	
	public SetBloomFilterLong(bloomFilter p_bf, long min, long max, int filterId, boolean debug) {
	    min_ = min;
	    max_ = max;
	    m_bf_ = p_bf;
	    m_bb_ = ByteBuffer
		.allocate(8)
		//		.order(ByteOrder.LITTLE_ENDIAN)
		;
            m_filterId_ = filterId;
            m_dump_debug_data_ = debug;
	}
	
	public void setTimer(timer p_timer) {
	    m_timer_ = p_timer;
	}

	@Override
	    public boolean keep(Long value) {
	    if (value == null) {
		return false;
	    }

	    if (m_timer_ != null) {
		m_timer_.start2();
	    }
	    
	    m_bb_.clear();
	    m_bb_.putLong(value);
	    boolean lv_result = m_bf_.lookup(m_bb_.array());

	    if (m_timer_ != null) {
		m_timer_.stop2();
	    }

            if ( m_dump_debug_data_ )
              dumpKeep(m_filterId_, value, lv_result);
	    
	    return lv_result;
	}

	@Override
	    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Long> statistics) {

            if ( m_dump_debug_data_ )
                dumpCanDrop(m_filterId_, min_, max_,
                   statistics.getMin(), statistics.getMax(),
                   statistics.getMax() < min_ || max_ < statistics.getMin() 
                  );
            return ( statistics.getMax() < min_ ||
                     max_ < statistics.getMin() );
	}

	@Override
	    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Long> statistics) {
	    return false;
	}
    }

    public static class SetBloomFilterFloat extends UserDefinedPredicate<Float> implements Serializable {

	private float min_;
	private float max_;
	private bloomFilter m_bf_;
        private ByteBuffer m_bb_ = null;
        private int m_filterId_;
	
	public SetBloomFilterFloat(bloomFilter p_bf, float min, float max, int filterId) {
            min_ = min;
            max_ = max;
	    m_bf_ = p_bf;
	    m_bb_ = ByteBuffer
		.allocate(4)
		//		.order(ByteOrder.LITTLE_ENDIAN)
		;
            m_filterId_ = filterId;
	}
	
	@Override
	    public boolean keep(Float value) {
	    if (value == null) {
		return false;
	    }
	    
	    m_bb_.clear();
	    m_bb_.putFloat(value);
	    return m_bf_.lookup(m_bb_.array());
	}

	@Override
	    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Float> statistics) {

            return ( statistics.getMax() < min_ ||
                     max_ < statistics.getMin() );
	}

	@Override
	    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Float> statistics) {
	    return false;
	}
    }

    public static class SetBloomFilterDouble extends UserDefinedPredicate<Double> implements Serializable {

	private double min_;
	private double max_;
	private bloomFilter m_bf_;
        private ByteBuffer m_bb_ = null;
        private int m_filterId_;
	
	public SetBloomFilterDouble(bloomFilter p_bf, double min, double max, int filterId) {
            min_ = min;
            max_ = max;
	    m_bf_ = p_bf;
	    m_bb_ = ByteBuffer
		.allocate(8)
		//		.order(ByteOrder.LITTLE_ENDIAN)
		;
            m_filterId_ = filterId;
	}
	
	@Override
	    public boolean keep(Double value) {
	    if (value == null) {
		return false;
	    }
	    
	    m_bb_.clear();
	    m_bb_.putDouble(value);
	    return m_bf_.lookup(m_bb_.array());
	}

	@Override
	    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Double> statistics) {

            return ( statistics.getMax() < min_ ||
                     max_ < statistics.getMin() );
	}

	@Override
	    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Double> statistics) {
	    return false;
	}
    }

    public static class SetBloomFilterBinary extends UserDefinedPredicate<Binary> implements Serializable {

	private String min_;
	private String max_;
	private bloomFilter m_bf_;
        private int m_filterId_;
        private int m_columntype_;
        private boolean m_dump_debug_data_;

        private static ByteBuffer longBuf = ByteBuffer.allocate(Long.BYTES);
	
	public SetBloomFilterBinary(bloomFilter p_bf, String min, String max, int filterId, int coltype, boolean debug) {
            min_ = min; 
            max_ = max;
	    //System.out.println("SBFL called");
	    m_bf_ = p_bf;
            m_filterId_ = filterId;
            m_columntype_= coltype;
            m_dump_debug_data_ = debug;
	}
	
	@Override
	    public boolean keep(Binary value) {
	    if (value == null) {
		return false;
	    }
		    
	    boolean result = false;
            switch ( m_columntype_ ) {

	      case HIVE_CHAR_TYPE:
	      case HIVE_VARCHAR_TYPE:
	      case HIVE_STRING_TYPE:
	      case HIVE_BINARY_TYPE:
	        result = m_bf_.lookup(value.getBytes());

                if ( m_dump_debug_data_ )
                   dumpKeep(m_filterId_, value, result);

                break;

              // only allow a decimal with 0 scale for bloom filtering.
              // Please refer to exp_clause.cpp@ line 2511
              case HIVE_DECIMAL_TYPE:

                // the following is the reverse of conversion from
                // DECIMAL to HiveDecimal to BigInteger to Binary
                // Please refer to extractFilterInfo() in this file
                // at line 420.
                BigInteger bInt = new BigInteger(value.getBytes());
                HiveDecimal hvDec = HiveDecimal.create(bInt);
                
                // convert from long to byte[]
                longBuf.putLong(0, hvDec.longValue());
	        result = m_bf_.lookup(longBuf.array());
                break;

	      case HIVE_TIMESTAMP_TYPE:
                int[] tsf = fetchDatetimeFields(value);
	        String tsStr = String.format
	    	("%04d-%02d-%02d %02d:%02d:%02d.%06d",
	    	 tsf[0], tsf[1], tsf[2], tsf[3], tsf[4], tsf[5], tsf[6]);
	    
	        result = m_bf_.lookup(tsStr.getBytes());

                if ( m_dump_debug_data_ )
                   dumpKeep(m_filterId_, tsStr, result);

                break;

	      case HIVE_DATE_TYPE:
                int[] dtf = fetchDatetimeFields(value);
	        String dtStr = String.format
	    	("%04d-%02d-%02d", dtf[0], dtf[1], dtf[2]);
	    
	        result = m_bf_.lookup(dtStr.getBytes());

                if ( m_dump_debug_data_ )
                   dumpKeep(m_filterId_, dtStr, result);

                break;

              default:
                break;
            }

            return result;
	}

	@Override
	    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics) {

            String groupMin = new String(statistics.getMin().getBytes());
            String groupMax = new String(statistics.getMax().getBytes());

            
            if ( m_dump_debug_data_ )
              dumpCanDrop(m_filterId_, min_, max_, 
                          statistics.getMin(), statistics.getMax(),
                          groupMax.compareTo(min_) == -1 || 
                          max_.compareTo(groupMin) == -1 
                         );

            return ( groupMax.compareTo(min_) == -1 || 
                     max_.compareTo(groupMin) == -1 );
	}

	@Override
	    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics) {
	    return false;
	}
    }

    private FilterCompat.Filter buildFilterNewForTest(Object[] ppi_vec)
	throws IOException
    {

	IntColumn lv_column_name = FilterApi.intColumn("s_store_sk");

	HashSet<Integer> lv_hash_set = new HashSet<Integer>();

	lv_hash_set.add(1); 
	lv_hash_set.add(5);
	lv_hash_set.add(11);

        FilterCompat.Filter filter = null;

	filterPred = FilterApi.userDefined(lv_column_name, new SetInFilter<Integer>(lv_hash_set));

        if (filterPred != null)
            filter = TrafFilter.get(filterPred);

	if (logger.isTraceEnabled()) 
	    logger.trace("buildParquet"
			 + ", filter: " + filter
			 + ", filterPred: " +  filterPred
			 );

        return filter;
    } 

    private FilterCompat.Filter buildFilter(Object[] ppi_vec)
	throws IOException
    {
        currFilterPPIidx = -1;
        filterPred = null;
         
        makeFilterPred(ppi_vec);
        FilterCompat.Filter filter = null;
        if (filterPred != null)
            filter = TrafFilter.get(filterPred);

/*	    
System.out.println("buildParquet"
			 + ", filter: " + filter
			 + ", filterPred: " +  filterPred
			 );
*/

	if (logger.isTraceEnabled()) 
	    logger.trace("buildParquet"
			 + ", filter: " + filter
			 + ", filterPred: " +  filterPred
			 );

        return filter;
    } 

    void setupTimersPreOpen()
    {
        if ( sv_scan_timer_on ) {
          m_scan_timer = new timer("scan timer");
          m_scan_timer.start2();
        }

	if (sv_timer_on) { 
	    m_open_timer = new timer("Parquet OpenFile");

	    m_parquet_initreader_timer = new timer("Parquet InitReader");
	    m_parquet_reader_timer = new timer("Parquet Reader");
	    m_process_block_timer = new timer("Parquet ProcessBlock");


	    if (sv_bf_timer_on) {
		m_bf_i_timer = new timer("Parquet BloomFilterInteger");
		m_bf_l_timer = new timer("Parquet BloomFilterLong");
	    }

	    m_open_timer.start2();
	}
    }

    void setupTimersPostOpen() 
    {
        if ( sv_scan_timer_on ) {
          m_scan_timer.stop2();
        }

	if (sv_timer_on) {
	    m_open_timer.stop2();
	}
    }

    // tableName: name of parquet table
    // fileName: path to the file that need to be read.
    // offset: not used for parqet
    // pv_num_cols_to_project: number of columns to project and return
    // pv_which_cols: array containing index of columns that are to be
    //         returned. Index points to entries in col_name_vec. Index
    //         is 1-based.
    // length: not used for parquet
    // expected_row_size, max_rows_to_fil, max_allocation_in_mb:
    //    used for returned buffer allocation
    // ppi_vec: byte array containing info needed for predicate evaluation
    // ppi_all_cols: unused
    // col_name_vec: array containing all needed columns 
    //        (used to return data and evaluate predicates)
    // col_type_vec: array containing type info for needed cols
    // flags: bit flags passed in. See TrafExtStorageUtils.java for details.
    public String open(String tableName,
                       String fileName, 
		       long   offset, 
		       long   length,
		       int    pv_num_cols_to_project,
		       int[]  pv_which_cols,
		       int    expected_row_size,
		       int    max_rows_to_fill,
		       int    max_allocation_in_mb,
                       Object[] ppi_vec,
                       Object[] ppi_all_cols,
                       Object[] col_name_vec,
                       Object[] col_type_vec,
                       String   parqSchStr,
                       int    flags) throws IOException {
 
	if (logger.isDebugEnabled()) logger.debug("Enter open()"
						  + " table: " + tableName
						  + ", file: " + fileName
						  + ", offset: " + offset
						  + ", length: " + length
						  + ", pv_num_cols_to_project: " + pv_num_cols_to_project
						  + ", pv_which_cols: " + (pv_which_cols==null?"null":pv_which_cols.length)
						  + ", expected_row_size: " + expected_row_size
						  + ", max_rows_to_fill: " + max_rows_to_fill
						  + ", max_allocation_in_mb: " + max_allocation_in_mb
						  + ", ppi_vec: " + (ppi_vec==null?"null":ppi_vec.length)
						  + ", ppi_all_cols: " + (ppi_all_cols==null?"null":ppi_all_cols.length)
						  + ", col_name_vec: " + (col_name_vec==null?"null":col_name_vec.length)
						  + ", col_type_vec: " + (col_type_vec==null?"null":col_type_vec.length)
						  );

	setupTimersPreOpen();

	m_table_name = tableName;

	m_more_rows = true;

        setFlags(flags);
        if (isParquetSignedMinMax())            
            conf.setBoolean("parquet.strings.signed-min-max.enabled", true); 
        else
            conf.setBoolean("parquet.strings.signed-min-max.enabled", false);  

        setupBufferAllocVars(expected_row_size, max_rows_to_fill,
                             max_allocation_in_mb);

	setupColInfo(pv_num_cols_to_project,
		     pv_which_cols, 
		     col_name_vec,
		     col_type_vec);

        // this flag is set in setupColInfo method
        if (hasCompositeCol)
            {
			    // disable serialize opt for composite columns.
                sv_concat_blocks_enabled = false;
                sv_concat_blocksize_in_mb = 16;
                sv_double_buffer_enabled = false;
                sv_fast_serialize_enabled = false;
                sv_faster_serialize_enabled = false;
            }

	if (logger.isDebugEnabled()) logger.debug("open "
						  + ", listOfReturnedColName: " 
						  + (listOfReturnedColName==null?"null":listOfReturnedColName.size())
						  + ", listOfReturnedColType: " 
						  + (listOfReturnedColType==null?"null":listOfReturnedColType.length)
						  + ", usedLength: " + m_returned_col_type_length
						  + ", listOfReturnedColPrec: " 
						  + (listOfReturnedColPrec==null?"null":listOfReturnedColPrec.length)
						  + ", listOfReturnedColScale: " 
						  + (listOfReturnedColScale==null?"null":listOfReturnedColScale.length)
						  );
	listOfFieldIndex = null;

	m_field_index_populated = false;

        if (!hasCompositeCol)
	    schema = generateParquetSchema(tableName,
					   listOfReturnedColName,
					   listOfReturnedColType,
					   listOfReturnedColPrec,
					   listOfReturnedColScale);
	else
	    schema = generateParquetSchema(parqSchStr);

	if (logger.isDebugEnabled()) logger.debug("generated schema "
						  + ", schema: " + schema
						  );

        filePath = new Path(fileName); 
	if (sv_fast_serialize_enabled) {
	    TrafReadSupport lv_trs = new TrafReadSupport(); 
	    if (sv_faster_serialize_enabled) {
		lv_trs.setTPFR(this);
	    }
	    readSupport = lv_trs;
	}
	else {
	    readSupport = new GroupReadSupport(); 
	}
	
        FilterCompat.Filter filter = null;
        if (ppi_vec != null) {
	    try {
		filter = buildFilter(ppi_vec);
	    }
	    catch (IOException ioe) {
		throw new IOException(ioe.getMessage());
	    }

            if (filter == null) {
                throw new IOException("TrafParquetFileReader.open: Could not create filter predicate.");
            }

        }

        pb = TrafParquetReader.builder(readSupport, filePath);
        pb.withConf(conf);
        if (schema != null) {
	    ReadContext lv_readContext = readSupport.init(conf, null, schema);
	    if (logger.isTraceEnabled()) logger.trace("after readsupport.init"
						      + ", readContext.requested schema: " + lv_readContext.getRequestedSchema()
						      );
	    pb.withReadContext(lv_readContext);
        }
        if (filter != null)
            pb.withFilter(filter);
	if (offset > 0) pb.withBlockOffsetInfo(offset, length);
	pb.withTimer(m_parquet_initreader_timer);
        reader = pb.build();

	if ( sv_double_buffer_enabled ) {
	    if (logger.isDebugEnabled()) logger.debug("submitting fetch @ open");
	    submitDoubleBufferRequest();
	}

	if (sv_serialization_timer_on) {
	    m_serialization_timer = new timer("Parquet Serialization");
	}

	setupTimersPostOpen();

        return null;
    }

    public byte[] fetchNextRow() throws IOException 
    {
	if (logger.isTraceEnabled()) logger.trace("Enter fetchNextRow()");

        if ( sv_scan_timer_on ) m_scan_timer.start2();
	    
        Group value = reader.read();
        if (value == null)
            return null;

        String valueToString = value.getValueToString(0, 0);

	byte[] lv_row_ba = valueToString.getBytes();

        if ( sv_scan_timer_on ) m_scan_timer.stop2();

	return lv_row_ba;
    }

    private void submitDoubleBufferRequest()
    {

	allocateMemoryDoubleBuffer();

	m_future = sv_executorService.submit(new Callable() {
		public Object call() throws IOException {
		    return internalFetchNextBlock();
		}
	    });

	m_request_submitted = true;
	
    }
    
    public ByteBuffer fetchNextBlock() throws IOException, InterruptedException, ExecutionException
    {
	if (sv_timer_on) m_process_block_timer.start2();

        if ( sv_scan_timer_on ) m_scan_timer.start2();
	
	ByteBuffer lv_result = null;

	if ( ! sv_double_buffer_enabled ) {
	    allocateMemory();
	    lv_result = internalFetchNextBlock();
	    if (sv_timer_on) m_process_block_timer.stop2();
            if ( sv_scan_timer_on ) m_scan_timer.stop2();

	    return lv_result;
	}

	if ( ! m_request_submitted) {
	    if (logger.isDebugEnabled()) logger.debug("submitting 1st fetch");
	    submitDoubleBufferRequest();
	}

	if (logger.isDebugEnabled()) logger.debug("getting future");

        lv_result = (ByteBuffer) m_future.get();

	m_request_submitted = false;

	if (m_more_rows) {
	    if (logger.isDebugEnabled()) logger.debug("return from future: " + lv_result + "submitting another one");
	    submitDoubleBufferRequest();
	}

        if (sv_timer_on) m_process_block_timer.stop2();
        if ( sv_scan_timer_on ) m_scan_timer.stop2();

	return lv_result;
    }

    public ByteBuffer internalFetchNextBlock() throws IOException 
    {
	//	if (sv_timer_on) m_process_block_timer.start2();
	if (logger.isDebugEnabled()) logger.debug("Enter fetchNextBlock()");

        Group group = null;

	int lv_num_rows = 0;
	//	int lv_row_offset = 8; // Initial offset to store the number of rows
	m_row_offset = 8; // Initial offset to store the number of rows
	int lv_filled_bytes = 0;
	boolean lv_done = false;

        while ((lv_num_rows < m_max_rows_to_fill_in_block) &&
	       (m_allocation_size - m_row_offset >= m_max_row_size_seen) &&
	       (!lv_done)) {
	    if (logger.isTraceEnabled()) 
                logger.trace("fetchNextBlock (in the loop):");

	    //	    System.out.println("TPFR.internalFetchNextBlock()");
            if (sv_timer_on) m_parquet_reader_timer.increment_count();
            group = reader.read();
	    //            if (sv_timer_on) m_parquet_reader_timer.stop2();
            if (group == null)
                {
                    lv_done = true;
		    m_more_rows = false;
                    break;
                }

	    if (sv_serialization_timer_on) m_serialization_timer.start2();
	    lv_filled_bytes  = fillNextRow(group);
	    if (sv_serialization_timer_on) m_serialization_timer.stop2();

	    if (lv_filled_bytes > 0) {
		m_row_offset += lv_filled_bytes;
		lv_num_rows++;
		if (lv_filled_bytes > m_max_row_size_seen) {
		    m_max_row_size_seen = lv_filled_bytes;
		}
	    }
	    else {
		lv_done = true;
	    }
	}
	
	// Set the number of rows in the block header
	m_block_bb.putInt(0, lv_num_rows);
	m_block_bb.putInt(4, m_row_offset);

	if (logger.isDebugEnabled()) 
	    logger.debug("fetchNextBlock (out of the loop):" 
			 + " #rows filled: " + lv_num_rows
			 + " row offset: " + m_row_offset
			 );

	//        if (sv_timer_on) m_process_block_timer.stop2();
	return m_block_bb;
    }

    void validateParquetColSchema(String colName, int colType, Type parqType)
        throws IOException {

        OriginalType origType = parqType.getOriginalType();
        PrimitiveType primType = null;
        GroupType groupType = null;
        String groupTypeName = null;
        PrimitiveType.PrimitiveTypeName primTypeName = null;
        if (parqType.isPrimitive()) {
            primType = parqType.asPrimitiveType();
            primTypeName = primType.getPrimitiveTypeName();
        }
        else {
            groupType = parqType.asGroupType();
            groupTypeName = groupType.getName();
            //throw new IOException("Datatype '" + colType + "' for field '" + colName + " is not primitive type.");
        }

        PrimitiveType.PrimitiveTypeName neededType = null;
        boolean isError = false;
        String expectedType = null;
        switch (colType) {
        case HIVE_BOOLEAN_TYPE: 
            isError = (primTypeName != PrimitiveType.PrimitiveTypeName.BOOLEAN);
            expectedType = "BOOLEAN";
            break;
        case HIVE_BYTE_TYPE:
        case HIVE_SHORT_TYPE:
        case HIVE_INT_TYPE:
            isError = (primTypeName != PrimitiveType.PrimitiveTypeName.INT32);
            expectedType = "INT32";
            break;
        case HIVE_LONG_TYPE:
            isError = (primTypeName != PrimitiveType.PrimitiveTypeName.INT64);
            expectedType = "INT64";
            break;
        case HIVE_FLOAT_TYPE:
            isError = (primTypeName != PrimitiveType.PrimitiveTypeName.FLOAT);
            expectedType = "FLOAT";
            break;
        case HIVE_DOUBLE_TYPE:
            isError = (primTypeName != PrimitiveType.PrimitiveTypeName.DOUBLE);
            expectedType = "DOUBLE";
            break;
        case HIVE_CHAR_TYPE:
        case HIVE_VARCHAR_TYPE:
        case HIVE_STRING_TYPE:
            if (primTypeName != null) {
                if (primTypeName != PrimitiveType.PrimitiveTypeName.BINARY) {
                    isError = true;
                    expectedType = "BINARY";
                }
            }
            else if ((origType == null) ||
                     (origType != OriginalType.UTF8)) {
                isError = true;
                expectedType = "UTF8";
            }
            break;
        case HIVE_DECIMAL_TYPE:
            if (primTypeName != null) {
                if ((primTypeName != PrimitiveType.PrimitiveTypeName.INT32) &&
                    (primTypeName != PrimitiveType.PrimitiveTypeName.INT64) &&
                    (primTypeName != PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                    isError = true;
                    expectedType = "INT32 or INT64 or FIXED_LEN_BYTE_ARRAY";
                }
            }
            else if ((origType == null) ||
                     (origType != OriginalType.DECIMAL)) {
                isError = true;
                expectedType = "DECIMAL";
            }
            break;
        case HIVE_DATE_TYPE:
            isError = true;
            expectedType = "UNKNOWN";
            break;
        case HIVE_TIMESTAMP_TYPE:
            isError = ((primTypeName != PrimitiveType.PrimitiveTypeName.INT96) &&
                       (primTypeName != PrimitiveType.PrimitiveTypeName.INT64));
            expectedType = "INT64 or INT96";
            break;
        case HIVE_BINARY_TYPE:
            isError = (primTypeName != PrimitiveType.PrimitiveTypeName.BINARY);
            expectedType = "BINARY";
            break;
        case HIVE_ARRAY_TYPE:
            break;
        case HIVE_STRUCT_TYPE:
            break;
        default:
            isError = true;
            expectedType = "UNKNOWN";
            break;
        } // switch

        if (isError == true) {
            if (expectedType.equals("UNKNOWN"))
                throw new IOException("Datatype '" + colType + "' for field '" + colName + " not supported.");
            
            String errType = null;
            if (primTypeName != null)
                errType = primType.toString(); //primTypeName;
            else if (origType != null)
                errType = origType.toString();
            else
                errType = "UNKNOWN";

            throw new IOException("Invalid type '" + errType + "' for field '" + colName + 
                                  "'. Expected type '" + expectedType +"'.");
        }
        
        return;
    }

    public int getColTypeFromParqType(Type parqType) {
        
        if (! parqType.isPrimitive())
            return -1;

        int colType = -1;
        PrimitiveType primType = parqType.asPrimitiveType();
        PrimitiveType.PrimitiveTypeName primTypeName = 
            primType.getPrimitiveTypeName();
        
        if (primTypeName == null)
            return -1;

        if (primTypeName == PrimitiveType.PrimitiveTypeName.BINARY)
            colType = HIVE_BINARY_TYPE;
        else if (primTypeName == PrimitiveType.PrimitiveTypeName.INT32)
            colType = HIVE_INT_TYPE;
        else if (primTypeName == PrimitiveType.PrimitiveTypeName.INT64)
            colType = HIVE_LONG_TYPE;
        else if (primTypeName == PrimitiveType.PrimitiveTypeName.BOOLEAN)
            colType = HIVE_BOOLEAN_TYPE;
        else if (primTypeName == PrimitiveType.PrimitiveTypeName.FLOAT)
            colType = HIVE_FLOAT_TYPE;
        else if (primTypeName == PrimitiveType.PrimitiveTypeName.DOUBLE)
            colType = HIVE_DOUBLE_TYPE;
        else if (primTypeName == PrimitiveType.PrimitiveTypeName.INT96)
            colType = HIVE_TIMESTAMP_TYPE;
        else if (primTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            colType = HIVE_DECIMAL_TYPE;

        // TBD: check INT32/INT64 DECIMAL and INT64 TIMESTAMP. 
        // Look at method validateParquetColSchema.

        return colType;
    }

    public ByteBuffer getRecordBuffer() {
	return m_block_bb;
    }

    // inits the byte buffer
    public void initRow()
    {
	m_block_bb.position(m_row_offset);

	m_block_bb.putInt(m_row_offset);
	m_block_bb.putInt(m_col_count);

	m_block_bb.putLong(1);
    }

    public int endRow()
    {
        int lv_filled_bytes = m_block_bb.position() - m_row_offset;
	m_block_bb.putInt(m_row_offset, lv_filled_bytes - 16);

	return lv_filled_bytes;
    }

    public void fillNull() {
	m_block_bb.putInt(-1);
    }

    public void fillBoolean(boolean pv_boolean) {
	byte lv_byte = (byte)(pv_boolean ? 1 : 0);
        
	m_block_bb.putInt(1);
	m_block_bb.put(lv_byte);
    }

    public void fillShort(short pv_short) {
	m_block_bb.putInt(2);
	m_block_bb.putShort(pv_short);
    }

    public void fillInt(int pv_field_index, int pv_int) {

	switch (listOfReturnedColType[pv_field_index]) {
	case HIVE_BYTE_TYPE:
	case HIVE_SHORT_TYPE:
	    {
		fillShort((short) pv_int);
		break;
	    }
	case HIVE_INT_TYPE:
	    {
		fillInt(pv_int);
		break;
	    }

	case HIVE_DECIMAL_TYPE:
	    {
		int lv_precision = listOfReturnedColPrec[pv_field_index];
		int lv_scale     = listOfReturnedColScale[pv_field_index];
		fillDecimalWithLong(lv_precision, lv_scale, pv_int);
		break;
	    }

	default:
	    break;
	    
	}
    }

    public void fillLong(int pv_field_index, long pv_long) {

	switch (listOfReturnedColType[pv_field_index]) {
	case HIVE_LONG_TYPE:
	    {
		fillLong(pv_long);
		break;
	    }

	case HIVE_DECIMAL_TYPE:
	    {
		int lv_precision = listOfReturnedColPrec[pv_field_index];
		int lv_scale     = listOfReturnedColScale[pv_field_index];
		fillDecimalWithLong(lv_precision, lv_scale, pv_long);
		break;
	    }

	case HIVE_TIMESTAMP_TYPE:
	    {
		int[] lv_dtFields = null;
		fillTimestampDatefields(lv_dtFields);
		break;
	    }

	default:
	    break;
	    
	}
    }

    public void fillInt(int pv_int) {
	m_block_bb.putInt(4);
	m_block_bb.putInt(pv_int);
    }

    public void fillLong(long pv_long) {
	m_block_bb.putInt(8);
	m_block_bb.putLong(pv_long);
    }

    public void fillFloat(float pv_float) {
	m_block_bb.putInt(4);
	m_block_bb.putFloat(pv_float);
    }

    public void fillDouble(double pv_double) {
	m_block_bb.putInt(8);
	m_block_bb.putDouble(pv_double);
    }

    public void fillDecimalWithLong(int  pv_precision,
				    int  pv_scale,
				    long pv_val
				    )
    {
	if (logger.isDebugEnabled()) logger.debug("fillDecimalWithLong"
						  + ", precision: " + pv_precision
						  + ", scale: " + pv_scale
						  + ", val: " + pv_val
						  );
	m_block_bb.putInt(2+2+8);
	m_block_bb.putShort((short)pv_precision);
	m_block_bb.putShort((short)pv_scale);
	m_block_bb.putLong(pv_val);
    }

    public void fillDecimalWithBinary(Binary pv_val,
				      int    pv_field_index
				    )
    {
	int lv_precision = listOfReturnedColPrec[pv_field_index];
	int lv_scale     = listOfReturnedColScale[pv_field_index];
	
	fillDecimalWithBinary(lv_precision,
			      lv_scale,
			      pv_val,
			      pv_field_index);
	
    }

    public void fillBinary(Binary pv_val,
			   int    pv_field_index
			   )
    {
	switch (listOfReturnedColType[pv_field_index]) {
	case HIVE_CHAR_TYPE:
	case HIVE_VARCHAR_TYPE:
	case HIVE_STRING_TYPE:
	    fillBinary(pv_val);
	    break;

	default:
	    System.out.println("TPFR fillBinary called - not CHAR/VARCHAR/STRING TYPE - no person's land??");
	    break;
	}
    }
    public void fillBinary(Binary pv_val
			   )
    {
	byte[] lv_bytes = pv_val.getBytesUnsafe();
        
	m_block_bb.putInt(lv_bytes.length);
	m_block_bb.put(lv_bytes);
	
    }
    
    public void fillDecimalWithBinary(int    pv_precision,
				      int    pv_scale,
				      Binary pv_val,
				      int    pv_field_index
				      )
    {
	byte[] lv_bytes = pv_val.getBytesUnsafe();
	BigInteger bi = new BigInteger(lv_bytes);
	if (logger.isDebugEnabled()) 
	    logger.debug("Hive decimal type (fixed length byte array)"
			 + ", colName: " + listOfReturnedColName.get(pv_field_index)
			 + ", precision: " + pv_precision
			 + ", scale: " + pv_scale
			 + ", colVal (Binary): " + pv_val
			 + ", colVal (byte[]): " + Hex.encodeHexString(lv_bytes)
			 + ", (byte[]).length: " + lv_bytes.length
			 + ", colVal (BigInteger): " + bi
			 );

	if (pv_precision <= 18) {
	    m_block_bb.putInt(2+2+8); 
                                        
	    long lv_l = bi.longValue();
	    m_block_bb.putShort((short)pv_precision);
	    m_block_bb.putShort((short)pv_scale);
	    m_block_bb.putLong(lv_l);
	} else {
	    // return data as string
	    String lv_string = bi.toString();
                                        
	    if (logger.isDebugEnabled()) 
		logger.debug("Hive decimal type (fixed length byte array)"
			     + ", colName: " + listOfReturnedColName.get(pv_field_index)
			     + ", precision: " + pv_precision
			     + ", scale: " + pv_scale
			     + ", colVal (Binary): " + pv_val
			     + ", colVal (byte[]): " + Hex.encodeHexString(lv_bytes)
			     + ", (byte[]).length: " + lv_bytes.length
			     + ", colVal (BigInteger): " + bi
			     + ", lv_string length: " + lv_string.length()
			     + ", lv_string: " + lv_string 
			     );

	    // insert a decimal point for scale.
	    int lv_sign = bi.compareTo(java.math.BigInteger.ZERO);
	    if (logger.isDebugEnabled()) logger.debug("BigInteger: " + bi + ", lv_sign: " + lv_sign);
	    int lv_offset = 0;
	    if (lv_sign < 0) {
		lv_offset = 1;
	    }

	    StringBuilder lv_sb = new StringBuilder(lv_string);
	    if (lv_string.length() >= (pv_scale + lv_offset)) {
		lv_sb.insert(lv_string.length() - pv_scale, ".");
	    }
	    else {
		int lv_string_len_diff = pv_scale - lv_string.length() + lv_offset;
		for (int j = 0; j < lv_string_len_diff; j++) {
		    lv_sb.insert(lv_offset, "0");
		}
		lv_sb.insert(lv_offset, ".");
		lv_sb.insert(lv_offset, "0");
	    }
	    lv_string = lv_sb.toString();
	    if (logger.isDebugEnabled()) logger.debug("Hive decimal type(flba), output string(with decimals): " + lv_string);
                                        
	    // 2 bytes precision & scale, 8 bytes unscaled value
	    m_block_bb.putInt(2+2+lv_string.getBytes().length); 
                                        
	    m_block_bb.putShort((short)pv_precision);
	    m_block_bb.putShort((short)pv_scale);
                                        
	    m_block_bb.put(lv_string.getBytes());
	}
	
    }

    public void fillTimestampwithBinary(Binary pv_val) 
    {
	int[] lv_dtFields = getDatetimeFields(pv_val);

	fillTimestampDatefields(lv_dtFields);
	
    }
    
    public void fillTimestampDatefields(int[] pv_dtFields) 
    {
	m_block_bb.putInt(11);
	
	// year
	m_block_bb.putShort((short)pv_dtFields[0]); 
        
	// month
	m_block_bb.put((byte)pv_dtFields[1]);
	
	// day
	m_block_bb.put((byte)pv_dtFields[2]);
	
	// hour
	m_block_bb.put((byte)pv_dtFields[3]);

	// min
	m_block_bb.put((byte)pv_dtFields[4]);

	// second
	m_block_bb.put((byte)pv_dtFields[5]);
                            
	// microsec
	m_block_bb.putInt(pv_dtFields[6]);

        //        System.out.println("fillTimestampDatefields: "
        //                           + pv_dtFields[3] + ":" + pv_dtFields[4] + ":" + pv_dtFields[5] + ":" + pv_dtFields[6]);
    //       System.out.println("fillTimestampDatefields: "
        //                 + pv_dtFields[0] + ":" + pv_dtFields[1] + ":" + pv_dtFields[2]);
	if (logger.isDebugEnabled()) 
	    logger.debug("fillTimestampDatefields: "
			 + pv_dtFields[3] + ":" + pv_dtFields[4] + ":" + pv_dtFields[5] + ":" + pv_dtFields[6]
			 );

    }

    public void fillPrimitiveType(Group group, int colType, int fieldIdx, int index,
                                  Type parqType, int ii)
        throws IOException 
    {
        PrimitiveType primType = null;
	PrimitiveType.PrimitiveTypeName primTypeName = null;

        int frc = group.getFieldRepetitionCount(fieldIdx);
        if (frc == 0) // null value
            {
                fillNull();
                return;
            }

        switch (colType)
            {
            case HIVE_BOOLEAN_TYPE:
                {
                    boolean lv_boo = group.getBoolean(fieldIdx, index);
                    fillBoolean(lv_boo);
                }
                break;
                
            case HIVE_BYTE_TYPE:
            case HIVE_SHORT_TYPE:
                {
                    int lv_i = group.getInteger(fieldIdx, index);
                    fillShort((short)lv_i);
                }
                break;
                
            case HIVE_INT_TYPE:
                {
                    int lv_i = group.getInteger(fieldIdx, index);
                    fillInt(lv_i);
                }
                break;
                
            case HIVE_LONG_TYPE:
                {
                    long lv_l = group.getLong(fieldIdx, index);
                    
                    fillLong(lv_l);
                }
                break;
                
            case HIVE_FLOAT_TYPE:
                {
                    float lv_f = group.getFloat(fieldIdx, index);
                    
                    fillFloat(lv_f);
                }
                break;
                
            case HIVE_DOUBLE_TYPE:
                {
                    double lv_d = group.getDouble(fieldIdx, index);
                    
                    fillDouble(lv_d);
                }
                break;
                
            case HIVE_DECIMAL_TYPE:
                {
                    // the 'file' specific values are initialized to -1
                    int precision = -1;
                    int scale     = -1;
			    
                    if (ii >= 0) {
                        precision = listOfFileColPrec[ii];
                        scale     = listOfFileColScale[ii];
                    }

                    if (precision == -1) {
                        
                        // first set the values to the one passed in to this object (via the open() method)
                        int lv_precision = -1;
                        int lv_scale     = -1;
                        if (ii >= 0) {
                            lv_precision = listOfReturnedColPrec[ii];
                            lv_scale     = listOfReturnedColScale[ii];
                        }
                        // get the value from the type read in from the file
                        if (parqType.asPrimitiveType() != null) {
                            primType = parqType.asPrimitiveType();
                            DecimalMetadata dm = primType.getDecimalMetadata();
                            if (dm != null) {
                                lv_precision = dm.getPrecision();
                                lv_scale = dm.getScale();
                            }
                        }
			
                        precision = lv_precision;
                        scale     = lv_scale;
                        if (ii >= 0) {
                            listOfFileColPrec[ii] = lv_precision;
                            listOfFileColScale[ii] = lv_scale;
                        }
                        
                        if (logger.isDebugEnabled()) {
                            if (ii >= 0) {
                                logger.debug("TPFR, decimal(precision,scale), column#: " + ii
                                             + ", from the file:(" + listOfFileColPrec[ii]
                                             + "," + listOfFileColScale[ii] + ")"
                                             + ", passed in:" 
                                             + "("+listOfReturnedColPrec[ii]
                                             + ","+listOfReturnedColScale[ii]+")"
                                             );
                            } else {
                                //logger.debug("TPFR, decimal(precision,scale): " + lv_precision + "," lv_scale);
                            }
                        } // logger enabled
                    }
                    
                    if (precision > 38) {
                        throw new IOException("TrafParquetFileReader.fillNextRow: Unsupported Type: DECIMAL with precision > 38");
                    }

                    if (parqType.isPrimitive()) {
                        primTypeName = parqType.asPrimitiveType().getPrimitiveTypeName();
                        if (primTypeName == PrimitiveType.PrimitiveTypeName.INT32) {
                            long lv_l = group.getInteger(fieldIdx, index);

                            fillDecimalWithLong(precision, scale, lv_l);
                        } else if (primTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                            long lv_l = group.getLong(fieldIdx, index);
                                    
                            fillDecimalWithLong(precision, scale, lv_l);
                        } else if (primTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                            Binary lv_bin = group.getBinary(fieldIdx, index);

                            fillDecimalWithBinary(precision, scale, lv_bin, ii);
                        }
                                                       
                        break;
                    }
                        
                    System.out.println("fillNextRow HIVE_DECIMAL_TYPE - no person's land??");
                    primType = parqType.asPrimitiveType();
                    DecimalMetadata dm = primType.getDecimalMetadata();
                    Binary lv_bin = group.getBinary(fieldIdx, index);
                    byte[] lv_bytes = lv_bin.getBytesUnsafe();
                            
                    BigInteger bi = new BigInteger(lv_bytes);
                            
                    if ((dm.getPrecision() >= 1) && (dm.getPrecision() <= 18))
                        {
                            // return data as Int64
                                    
                            long lv_l = bi.longValue();
                            //System.out.println("bi = " + bi.toString() + " bil = " + lv_l);

                            // 2 bytes precision & scale, 8 bytes unscaled value
                            m_block_bb.putInt(2+2+8); 
                                    
                            m_block_bb.putShort((short)dm.getPrecision());
                            m_block_bb.putShort((short)dm.getScale());
                            m_block_bb.putLong(lv_l);
                        }
                    else // precision >= 19 and <= 38
                        {
                            // return data as string
                            String lv_string = bi.toString();

                            // insert a decimal point for scale.
                            lv_string = 
                                new StringBuilder(lv_string)
                                .insert(lv_string.length()-dm.getScale(), ".")
                                .toString();
                            //System.out.println("lv_string = " + lv_string);
                            // 2 bytes precision & scale, 8 bytes unscaled value
                            m_block_bb.putInt(2+2+lv_string.getBytes().length); 
                                    
                            m_block_bb.putShort((short)dm.getPrecision());
                            m_block_bb.putShort((short)dm.getScale());
                                    
                            m_block_bb.put(lv_string.getBytes());
                        }

                }
                break;

            case HIVE_CHAR_TYPE:
            case HIVE_VARCHAR_TYPE:
            case HIVE_STRING_TYPE:
                {
                    if (parqType.isPrimitive())
                        {
                            Binary lv_bin = group.getBinary(fieldIdx, index);
                            fillBinary(lv_bin);
                        }
                    else 
                        {
                            System.out.println("fillNextRow CHAR/VARCHAR/STRING TYPE - no person's land??");
                            String lv_string = group.getValueToString(fieldIdx, index);
                                    
                            m_block_bb.putInt(lv_string.getBytes().length);
                            m_block_bb.put(lv_string.getBytes());
                        }
                }
                break;

            case HIVE_BINARY_TYPE:
                {
                    Binary lv_bin = group.getBinary(fieldIdx, index);
                    fillBinary(lv_bin);
                }
                break;

            case HIVE_DATE_TYPE:
                {
                    // should not reach here
                }
                break;

            case HIVE_TIMESTAMP_TYPE:
                {
                    int[] dtFields = null;
                    primTypeName = parqType.asPrimitiveType().getPrimitiveTypeName();
                    if (primTypeName == PrimitiveType.PrimitiveTypeName.INT96)
                        {
                            Binary ts_bin = group.getInt96(fieldIdx, index);
                            if (logger.isDebugEnabled()) logger.debug("fillRow TS"
                                                                      + ", bin: " + ts_bin
                                                                      );

                            fillTimestampwithBinary(ts_bin);
                        }
                    else
                        {
                            long lv_l = group.getLong(fieldIdx, index);
                            dtFields = getDatetimeFields(lv_l);
                            fillTimestampDatefields(dtFields);
                        }
                }
                break;

            case HIVE_ARRAY_TYPE:
            case HIVE_STRUCT_TYPE:
            default:
                {
                    if (true)
                        throw new IOException("TrafParquetFileReader.fillPrimitiveType: datatype must be primitive. colType = " + colType);
                }
                break;

            } // switch
     
        return;
    }

    public void fillCompositeType(Group group, int colType, int fieldIdx, int index,
                                  Type parqType, int i) throws IOException 
    {
        if (parqType.isPrimitive())
            {
                // error
                throw new IOException("TrafParquetFileReader.fillCompositeType: Cannot be primitive.");
            }

        GroupType groupType = group.getType();        
        switch (colType)
            {
            case HIVE_ARRAY_TYPE:
                {
                    int numArrElems = group.getFieldRepetitionCount(0);

                    int position = m_block_bb.position();
                    m_block_bb.putInt(0); // dummy length, will be updated later in this code

                    int position1 = m_block_bb.position();
                    m_block_bb.putInt(numArrElems);

                    for (int arrIdx = 0; arrIdx < numArrElems; arrIdx++) {
                        Group group2 = group.getGroup(0,arrIdx);
                        int fieldCount = group2.getType().getFieldCount();
                        Type fieldParqType = group2.getType().getType(0);
                        String fieldName = fieldParqType.getName();

                        if (fieldParqType.isPrimitive()) {
                            int fieldColType = getColTypeFromParqType(fieldParqType);
                            fillPrimitiveType(group2, fieldColType, 0, 0,
                                              fieldParqType, -1);
                        } else {
                            Group group3 = group2.getGroup(0,0);
                            Type.Repetition rep = 
                                group3.getType().getType(0).getRepetition();
                            if (rep == Type.Repetition.REPEATED)
                                fillCompositeType(group3, HIVE_ARRAY_TYPE, 0, 0, fieldParqType, 0);
                            else                            
                                fillCompositeType(group3, HIVE_STRUCT_TYPE, 0, 0, fieldParqType, 0);
                        }                 
                    } // for

                   // update length
                    int position2 = m_block_bb.position();
                    m_block_bb.putInt(position, (position2-position1));
                }
                break;

           case HIVE_STRUCT_TYPE:
                {
                    int fieldCount = groupType.getFieldCount();
                    int position = m_block_bb.position();
                            
                    m_block_bb.putInt(0); // dummy length, will be update later in this code

                    int position1 = m_block_bb.position();
                    m_block_bb.putInt(fieldCount);

                    for (int field = 0; field < fieldCount; field++) {
                        Type fieldParqType = groupType.getType(field);
                        if (fieldParqType.isPrimitive()) {
                            int fieldColType = getColTypeFromParqType(fieldParqType);
                            fillPrimitiveType(group, fieldColType, field, 0,
                                              fieldParqType, -1);
                        } else {
                            Group group3 = group.getGroup(field, 0);
                            Type.Repetition rep = 
                                group3.getType().getType(0).getRepetition();
                            if (rep == Type.Repetition.REPEATED)
                                fillCompositeType(group3, HIVE_ARRAY_TYPE, 0, 0, fieldParqType, 0);
                            else
                                fillCompositeType(group3, HIVE_STRUCT_TYPE, 0, 0, fieldParqType, 0);
                        }                        
                    } // for
                    
                    // update length
                    int position2 = m_block_bb.position();
                    m_block_bb.putInt(position, (position2-position1));
                    //System.out.println("position1 = " + position1 + ", position2 = " + position2 + " p2-p1 = " + (position2-position1));
                }
            } // switch
    }

    // fills the row in the given ByteBuffer
    public int fillNextRow(
			       Group     group
			       ) throws IOException 
    {
	if (logger.isTraceEnabled()) logger.trace("Enter fillNextRow(),"
						  + " offset: " 
						  + m_row_offset
						  + " length of array: " 
						  + (m_block_bb == null ? 0 : m_block_bb.capacity())
						  );
	
	if (sv_faster_serialize_enabled) {
	    TrafGroup lv_traf_group = (TrafGroup) group;
	    return lv_traf_group.getNumFilledBytes();
	}

	Object lv_field_val = null;

	initRow();

	if (logger.isTraceEnabled()) 
	    logger.trace("Bytebuffer length1: " + m_block_bb.position());

	int fieldIdx;
	int frc;
	int colType;
	Type parqType;
	    
	PrimitiveType primType = null;
	PrimitiveType.PrimitiveTypeName primTypeName = null;

	GroupType groupType = group.getType();
	if (logger.isTraceEnabled()) logger.trace("fillNextRow"
						  + ", grouptype: " + groupType
						  );
	
	if ( ! m_field_index_populated ) {
	    
	    if (logger.isTraceEnabled()) logger.trace("storing field index");
	    m_field_index_populated = true;
	    listOfFieldIndex = new int[m_returned_col_type_length];
	    for (int i = 0; i < m_returned_col_type_length; i++) {
		fieldIdx = -1;
		String colName = listOfReturnedColName.get(i);
		try {
		    fieldIdx = groupType.getFieldIndex(colName);
		}
		catch (org.apache.parquet.io.InvalidRecordException lv_ire) {
                    /*
                     * There is a possibility that this exception was raised because the
                     * column names are of different cases. The following code tries to 
                     * check if there are matching columns if we ignore the case of the
                     * characters in the column names.
                     */
		    List<Type> lv_fields_in_file_schema = groupType.getFields();
		    for (Type lv_file_field : lv_fields_in_file_schema) {
			String lv_file_field_name = lv_file_field.getName();
			if (lv_file_field_name.compareToIgnoreCase(colName) == 0) {
			    fieldIdx = groupType.getFieldIndex(lv_file_field_name);
			    break;
			}
		    }
		    // If we didn't find it, fieldIdx will still be -1. The column is
		    // missing. We'll keep fieldIdx as -1 and supply a null for this
		    // column.
		}
		listOfFieldIndex[i] = fieldIdx;
	    }
	}
	
	for (int i = 0; i < m_returned_col_type_length; i++)
	    {
		fieldIdx = listOfFieldIndex[i];
		if (fieldIdx == -1) // missing column
		    {
			fillNull();  // treat missing column as a null
			continue;
		    }

		frc = group.getFieldRepetitionCount(fieldIdx);
		if (frc == 0) // null value
		    {
			fillNull();
			continue;
		    }

                //System.out.println("fieldIdx1 = " + fieldIdx);
		colType = listOfReturnedColType[i];
		parqType = groupType.getType(fieldIdx);

		// validate column if it has not yet been validated.
		if (listOfFirstColRef[i] == true) {
		    validateParquetColSchema(listOfReturnedColName.get(i),
					     colType, 
					     parqType); 
		    listOfFirstColRef[i] = false;
		}
 
		switch (colType)
		    {
		    case HIVE_BOOLEAN_TYPE:
		    case HIVE_BYTE_TYPE:
		    case HIVE_SHORT_TYPE:
		    case HIVE_INT_TYPE:
		    case HIVE_LONG_TYPE:
		    case HIVE_FLOAT_TYPE:
		    case HIVE_DOUBLE_TYPE:
		    case HIVE_DECIMAL_TYPE:
		    case HIVE_CHAR_TYPE:
		    case HIVE_VARCHAR_TYPE:
		    case HIVE_STRING_TYPE:
		    case HIVE_BINARY_TYPE:
		    case HIVE_TIMESTAMP_TYPE:
			{
                            fillPrimitiveType(group, colType, fieldIdx, 0, parqType, i);
			}
			break;

                    case HIVE_ARRAY_TYPE:
                        {
                            Group compGroup = group.getGroup(fieldIdx, 0);
                            fillCompositeType(compGroup, colType, 0, 0, parqType, i);
                         }
                        break;

                    case HIVE_STRUCT_TYPE:
                        {
                            Group compGroup = group.getGroup(fieldIdx, 0);
                            fillCompositeType(compGroup, colType, 0, 0, parqType, i);
                         }
                        break;

		    case HIVE_DATE_TYPE:
		    default:
			{
			    if (true)
				throw new IOException("TrafParquetFileReader.fillNextRow: datatype not yet supported");
			}
			break;

		    } // switch
	    } // for

	return endRow();
    }
	
    public String close() throws IOException 
    {
        String msg = new String();

	if ( logger.isDebugEnabled() ) 
        {
          msg += "TrafParquetFileReader::close(): file=" + filePath;

	  if (sv_timer_on) {
	    if ( m_serialization_timer != null ) 
               msg += m_serialization_timer.getElapsedTime(", serializationTime=");
	
	    if ( m_parquet_reader_timer != null ) 
               msg += m_parquet_reader_timer.getElapsedTime(", readerTime=");
	
	    if ( m_parquet_initreader_timer != null ) 
              msg += m_parquet_initreader_timer.getElapsedTime(", initReaderTime=");
	
	    if ( m_process_block_timer != null ) 
              msg += m_process_block_timer.getElapsedTime(", blockTime=");

	    if ( m_bf_l_timer != null ) 
              msg += m_bf_l_timer.getElapsedTime(", (long)BFTime=");

	    if ( m_bf_i_timer != null ) 
             msg += m_bf_i_timer.getElapsedTime(", (int)BFtime=");
	  }
       }
	
	if (m_request_submitted) {
	    m_future.cancel(true);
	    m_request_submitted = false;
	}

        if ( sv_scan_timer_on ) {
          m_scan_timer.start2();
        }
        
	if (reader != null) {
	    reader.close();
	    reader = null;
	}

        if ( sv_scan_timer_on ) {
           m_scan_timer.stop2();
           msg += m_scan_timer.getElapsedTime(", totalElapsedEime=");
        }

	if ( logger.isDebugEnabled() ) 
          logger.info(msg);

	return null;
    }
    
    private long getRowCount() throws IOException
    {
	if (logger.isDebugEnabled()) logger.debug("Enter getRowCount(), file: " + ((filePath == null)?"null":filePath));
	inputFileStatus = filePath.getFileSystem(conf).globStatus(filePath);
	if (logger.isTraceEnabled()) logger.trace("fileStatusList: " + inputFileStatus);
	long rowCount = 0;

	for (FileStatus fs : inputFileStatus){
	    if (logger.isTraceEnabled()) logger.trace("fileStatus: " + fs);
            if (!(fs.isFile() && accept(fs.getPath()) && fs.getLen() > 0))
                continue;
	    for (Footer f : ParquetFileReader.readFooters(conf, fs, false)){
		if (logger.isTraceEnabled()) logger.trace("footer: " + f);
		for (BlockMetaData b : f.getParquetMetadata().getBlocks()){
		    if (logger.isTraceEnabled()) logger.trace("block metainfo: " 
							      + " blockName: " + b.getPath()
							      + " compressedSize: " + b.getCompressedSize()
							      + " totalSize: " + b.getTotalByteSize()
							      + " rowCount: " + b.getRowCount()
							      + " startingPos: " + b.getStartingPos()
							      );
		    rowCount += b.getRowCount();
		} // for BlockMetadata
	    } // for Footer
	} // for FileStatus

	if (logger.isTraceEnabled()) logger.trace("getRowCount() - Exit, rowCount: " + rowCount);
	return rowCount;
    }

    private org.apache.parquet.column.statistics.Statistics getColStatsForFile(int colNum, String colName, int colType) 
	throws IOException, java.lang.NoSuchMethodException, java.lang.IllegalAccessException
           ,java.lang.reflect.InvocationTargetException
    {
	if (logger.isDebugEnabled()) logger.debug("Enter getColStatsForFile()"
						  + " colNum: " + colNum
						  + ", colName: " + colName
						  + ", colType: " + colType
						  );

	org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName ptn = null;
	switch (colType) {
	case HIVE_BOOLEAN_TYPE: 
	    ptn = org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
	    break;
	case HIVE_BYTE_TYPE:
	case HIVE_SHORT_TYPE:
	case HIVE_INT_TYPE: 
	    ptn = org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
	    break;
	case HIVE_LONG_TYPE: 
	    ptn = org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
	    break;
	case HIVE_FLOAT_TYPE: 
	    ptn = org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
	    break;
	case HIVE_DOUBLE_TYPE: 
	    ptn = org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
	    break;
	case HIVE_BINARY_TYPE:
	case HIVE_CHAR_TYPE:
	case HIVE_VARCHAR_TYPE:
	case HIVE_STRING_TYPE:
	    ptn = org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
	    break;
	case HIVE_TIMESTAMP_TYPE:
	    ptn = org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
	    break;
	default:
	    ptn = org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
	    break;
	} // switch

	org.apache.parquet.column.statistics.Statistics totalColStats = org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType(ptn);
	org.apache.parquet.column.statistics.Statistics colStats = null;

	inputFileStatus = filePath.getFileSystem(conf).globStatus(filePath);
	for (FileStatus fs : inputFileStatus){
            if (!(fs.isFile() && accept(fs.getPath()) && fs.getLen() > 0))
                continue;
	    for (org.apache.parquet.hadoop.Footer f : org.apache.parquet.hadoop.ParquetFileReader.readFooters(conf, fs, false)){
		for (org.apache.parquet.hadoop.metadata.BlockMetaData b : f.getParquetMetadata().getBlocks()){
		    for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData ccmd : b.getColumns()) {
			if (logger.isDebugEnabled()) logger.debug("ccmd " + ccmd);
			String thisColName = null;

			// Method getPath() in class ColumnChunkMetaData moved
			// to a different location and gives a 'no such method'
			// exception at runtime when running on a system with
			// the older location.
			// Catch that exception and use java Reflection to
			// figure out the right place to call.
			try {
			    thisColName = ccmd.getPath().toDotString();
			} catch (java.lang.NoSuchMethodError er) {
			    if (logger.isTraceEnabled()) logger.trace("NoSuchMethodError: " + er);
			    /*
			      Class cp = 
			      Class.forName("parquet.hadoop.metadata.ColumnChunkMetaData");
			    */
			    Method getPathMethod = 
				ColumnChunkMetaData.class
				.getMethod("getPath");
			    Method toDotStringMethod =  
				getPathMethod
				.getReturnType()
				.getMethod("toDotString");
			    thisColName = 
				(String)toDotStringMethod
				.invoke(getPathMethod.invoke(ccmd));
			} // catch
                        
			if (thisColName.compareTo(colName) == 0) {
			    colStats = ccmd.getStatistics();
			    if (colStats.isEmpty()) {// no stats yet 
				if (logger.isDebugEnabled()) logger.debug("getColStatsForFile, empty col stats for col: " + colName);
				continue;
			    }
			    totalColStats.mergeStatistics(colStats);
			} // if
		    } // for
		} // for BlockMetadata
	    } // for Footer
	} // for FileStatus

	return totalColStats;
    }

    // for input column num, the returned list contains following entries:
    //  (note: if input col num == -1, then only first entry is returned. 
    //         This is used for count(*)  )
    // 
    //     total Num of entries (includes nulls and dups)
    //     type of aggr
    //     not null count
    //     min value
    //     max value
    public Object[] getColStats(int colNum, String colName, int colType) 
	throws IOException, java.lang.NoSuchMethodException, java.lang.IllegalAccessException
           ,java.lang.reflect.InvocationTargetException
    {
	if (logger.isTraceEnabled()) logger.trace("Enter getColStats");

	ArrayList<byte[]> retColStats = new ArrayList();

	// total number of vals (includes null and dups)
	long numVals = getRowCount();
	byte[] bytes = 
	    ByteBuffer.allocate(8) //Long.BYTES)
	    .order(ByteOrder.LITTLE_ENDIAN)
	    .putLong(numVals).array();
	retColStats.add(bytes);
        
	if (colNum == -1) { // file row count
	    return retColStats.toArray();
	}

	org.apache.parquet.column.statistics.Statistics columnStatistics = 
	    getColStatsForFile(colNum, colName, colType);

	if (logger.isDebugEnabled()) logger.debug("getColStats: back from getColStatsForFile"
						  + ", colstats for col: " + colName
						  + ", stats: " + columnStatistics
						  );
	// aggr col type
	int ctInt = colType;
	bytes = 
	    ByteBuffer.allocate(4) //Integer.BYTES)
	    .order(ByteOrder.LITTLE_ENDIAN)
	    .putInt(ctInt).array();
	retColStats.add(bytes);

	// num of not null values (does not include nulls)
	long numNotNullVals = numVals - columnStatistics.getNumNulls();
	bytes = 
	    ByteBuffer.allocate(8) //Long.BYTES)
	    .order(ByteOrder.LITTLE_ENDIAN)
	    .putLong(numNotNullVals).array();
	retColStats.add(bytes);

	switch (colType) 
	    {
	    case HIVE_BOOLEAN_TYPE:
		{
		    org.apache.parquet.column.statistics.BooleanStatistics bcs = 
			(org.apache.parquet.column.statistics.BooleanStatistics)columnStatistics;
		    byte min = (byte)((bcs.getMin() == false) ? 0 : 1);
		    byte max = (byte)((bcs.getMax() == true) ? 1 : 0);

		    bytes = 
			ByteBuffer.allocate(1)
			.put(min).array();
		    retColStats.add(bytes);

		    bytes = 
			ByteBuffer.allocate(1)
			.put(max).array();
		    retColStats.add(bytes);
		}
		break;
                
	    case HIVE_BYTE_TYPE:
	    case HIVE_SHORT_TYPE:
	    case HIVE_INT_TYPE:
		{
		    org.apache.parquet.column.statistics.IntStatistics ics = 
			(org.apache.parquet.column.statistics.IntStatistics)columnStatistics;
		    long min = ics.getMin();
		    long max = ics.getMax();
		    long sum = -1;

		    bytes = 
			ByteBuffer.allocate(8) //Long.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putLong(min).array();

		    retColStats.add(bytes);

		    bytes = 
			ByteBuffer.allocate(8) //Long.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putLong(max).array();

		    retColStats.add(bytes);

		    bytes = 
			ByteBuffer.allocate(8) //Long.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putLong(sum).array();

		    retColStats.add(bytes);
		}
		break;

	    case HIVE_LONG_TYPE:
		{
		    org.apache.parquet.column.statistics.LongStatistics ics = 
			(org.apache.parquet.column.statistics.LongStatistics)columnStatistics;
		    long min = ics.getMin();
		    long max = ics.getMax();
		    long sum = -1;

		    bytes = 
			ByteBuffer.allocate(8) //Long.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putLong(min).array();

		    retColStats.add(bytes);

		    bytes = 
			ByteBuffer.allocate(8) //Long.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putLong(max).array();

		    retColStats.add(bytes);

		    bytes = 
			ByteBuffer.allocate(8) //Long.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putLong(sum).array();

		    retColStats.add(bytes);
		}
		break;

		/*                
				  case DECIMAL:
				  {
				  DecimalColumnStatistics dcs = 
				  (DecimalColumnStatistics)columnStatistics;

				  HiveDecimal min = dcs.getMinimum();
				  HiveDecimal max = dcs.getMaximum();
				  HiveDecimal sum = dcs.getSum();

				  String min_l = min.toString();
				  bytes = min_l.getBytes();
				  retColStats.add(bytes);

				  String max_l = max.toString();
				  bytes = max_l.getBytes();
				  retColStats.add(bytes);

				  String sum_l = sum.toString();
				  bytes = sum_l.getBytes();
				  retColStats.add(bytes);
				  }
				  break;
		*/

	    case HIVE_FLOAT_TYPE:
		{
		    org.apache.parquet.column.statistics.FloatStatistics dcs = 
			(org.apache.parquet.column.statistics.FloatStatistics)columnStatistics;
		    double min = dcs.getMin();
		    bytes = 
			ByteBuffer.allocate(8) //Double.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putDouble(min).array();
		    retColStats.add(bytes);

		    double max = dcs.getMax();
		    bytes = 
			ByteBuffer.allocate(8) //Double.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putDouble(max).array();
		    retColStats.add(bytes);

		    double sum = -1;
		    bytes = Bytes.toBytes(sum);
		    bytes = 
			ByteBuffer.allocate(8) //Double.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putDouble(sum).array();
		    retColStats.add(bytes);
		}
		break;

	    case HIVE_DOUBLE_TYPE:
		{
		    org.apache.parquet.column.statistics.DoubleStatistics dcs = 
			(org.apache.parquet.column.statistics.DoubleStatistics)columnStatistics;
		    double min = dcs.getMin();
		    bytes = 
			ByteBuffer.allocate(8) //Double.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putDouble(min).array();
		    retColStats.add(bytes);

		    double max = dcs.getMax();
		    bytes = 
			ByteBuffer.allocate(8) //Double.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putDouble(max).array();
		    retColStats.add(bytes);

		    double sum = -1;
		    bytes = Bytes.toBytes(sum);
		    bytes = 
			ByteBuffer.allocate(8) //Double.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putDouble(sum).array();
		    retColStats.add(bytes);
		}
		break;

	    case HIVE_BINARY_TYPE:
	    case HIVE_CHAR_TYPE:
	    case HIVE_VARCHAR_TYPE:
	    case HIVE_STRING_TYPE:
	    case HIVE_DECIMAL_TYPE:
		{
		    org.apache.parquet.column.statistics.BinaryStatistics scs = 
			(org.apache.parquet.column.statistics.BinaryStatistics)columnStatistics;
		    org.apache.parquet.io.api.Binary min = scs.getMin();
		    bytes = min.getBytes();
		    retColStats.add(bytes);

		    org.apache.parquet.io.api.Binary max = scs.getMax();
		    bytes = max.getBytes();
		    retColStats.add(bytes);

		    long sum = -1;
		    bytes = 
			ByteBuffer.allocate(8) //Long.BYTES)
			.order(ByteOrder.LITTLE_ENDIAN)
			.putLong(sum).array();
		    retColStats.add(bytes);              
		}
		break;
                
		/*            case DATE:
			      {
			      DateColumnStatistics scs = 
			      (DateColumnStatistics)columnStatistics;
			      String min = scs.getMinimum().toString();
			      bytes = min.getBytes();
			      retColStats.add(bytes);

			      String max = scs.getMaximum().toString();
			      bytes = max.getBytes();
			      retColStats.add(bytes);
			      }
			      break;
		*/

	    case HIVE_TIMESTAMP_TYPE:
		{
		    org.apache.parquet.column.statistics.BinaryStatistics scs = 
			(org.apache.parquet.column.statistics.BinaryStatistics)columnStatistics;

		    org.apache.parquet.io.api.Binary min = scs.getMin();
		    int[] dtf = getDatetimeFields(min);
		    String minStr = String.format
			("%04d-%02d-%02d %02d:%02d:%02d.%06d",
			 dtf[0], dtf[1], dtf[2], dtf[3], dtf[4], dtf[5], dtf[6]);
		    bytes = minStr.getBytes();
		    retColStats.add(bytes);

		    org.apache.parquet.io.api.Binary max = scs.getMax();
		    dtf = getDatetimeFields(max);
		    String maxStr = String.format
			("%04d-%02d-%02d %02d:%02d:%02d.%06d",
			 dtf[0], dtf[1], dtf[2], dtf[3], dtf[4], dtf[5], dtf[6]);

		    bytes = maxStr.getBytes();
		    retColStats.add(bytes);

		}
		break;
                
	    default:
		{
		    retColStats = null;
		}
		break;
	    }

	return retColStats.toArray();
    }

    public byte[][]  getFileStats(String rootDir) 
	throws IOException 
    {

	if (logger.isDebugEnabled()) logger.debug("Enter getFileStats, file: " + rootDir);

	List<byte[]> fileStatusList = new LinkedList<byte[]>();

	Path fp = new Path(rootDir);
	FileStatus[] ifs = fp.getFileSystem(conf).listStatus(fp);
	
	int fileNum = 1;
	int blockNum = 0;
	int blocks = 0;
	boolean lv_blocksize_checked = false;
	boolean lv_concat_blocks = false;

	for (FileStatus fs : ifs){
            if (!(fs.isFile() && accept(fs.getPath()) && fs.getLen() > 0))
                continue;
	    blockNum = 1;
	    for (Footer f : ParquetFileReader.readFooters(conf, fs, false)){ 
		if (logger.isTraceEnabled()) 
		    logger.trace("loop - readFooters"
				 + ", file: " + fs
				 + ", schema:" + f.getParquetMetadata().getFileMetaData().getSchema()
				 );

		String filePath     = null;
		String fileName     = null;
		String blockName    = null;
		long   compressedSize = 0;
		long   totalSize      = 0;
		long   rowCount       = 0;
		long   startingPos    = 0;

		long    lv_block_count = 0;
		boolean lv_continuation = false;
		long    lv_blocks_in_file = f.getParquetMetadata().getBlocks().size();
		long    lv_current_block = 0;

		for (BlockMetaData b : f.getParquetMetadata().getBlocks()) {

		    if (!lv_continuation) {
			filePath     = f.getFile().getParent().toUri().getPath();
			fileName     = f.getFile().getName();
			blockName    = b.getPath();
			compressedSize = b.getCompressedSize();
			totalSize      = b.getTotalByteSize();
			rowCount       = b.getRowCount();
			startingPos    = b.getStartingPos();
		    }
		    else {
			compressedSize += b.getCompressedSize();
			totalSize      += b.getTotalByteSize();
			rowCount       += b.getRowCount();
		    }

		    if (! lv_blocksize_checked) {
			lv_blocksize_checked = true;
			if (sv_concat_blocks_enabled) {
			    if ( totalSize < ( sv_concat_blocksize_in_mb * 1024 * 1024 ) ) {
				lv_concat_blocks = true;  
			    }
			}
			if (logger.isDebugEnabled()) logger.debug("File: " + rootDir
								  + ", file: " + fileName
								  + ", concat blocks for this file: " + lv_concat_blocks
								  + ", concat blocks enabled: " + sv_concat_blocks_enabled
								  + ", concat blocks size(mb): " + sv_concat_blocksize_in_mb
								  + ", concat blocks max: " + sv_concat_blocks_max
								  );
		    }

		    if (lv_concat_blocks) {
			++ lv_block_count;
			lv_continuation = true;
			if (lv_block_count >= sv_concat_blocks_max) {
			    lv_continuation = false;
			    lv_block_count = 0;
			}
		       
			lv_current_block++;
			if (lv_current_block >= lv_blocks_in_file) {
			    lv_continuation = false;
			}
		    }

		    if (logger.isDebugEnabled()) logger.debug("getFileStats, file: " + rootDir
							      + ", file: " + fileName
							      + ", compressedSize: " + compressedSize
							      + ", totalSize: " + totalSize
							      + ", rowCount: " + rowCount
							      + ", startingPos: " + startingPos
							      + ", continuation: " + lv_continuation
							      + ", blockCountInSet: " + lv_block_count
							      + ", blockCountInFile: " + lv_current_block
							      + ", #blocksInFile: " + lv_blocks_in_file
							      );

		    if (!lv_continuation) {
			StringBuilder oneBlock = new StringBuilder();
			oneBlock.append(filePath);
			oneBlock.append("|");
			oneBlock.append(fileName);
			oneBlock.append("|");
			oneBlock.append(blockName);
			oneBlock.append("|");

			oneBlock.append(String.valueOf(fileNum));
			oneBlock.append("|");
			oneBlock.append(String.valueOf(blockNum));
			oneBlock.append("|");
			oneBlock.append(String.valueOf(compressedSize));
			oneBlock.append("|");
			oneBlock.append(String.valueOf(totalSize));
			oneBlock.append("|");
			oneBlock.append(String.valueOf(rowCount));
			oneBlock.append("|");
			oneBlock.append(String.valueOf(startingPos));
			oneBlock.append("|");
                    
			fileStatusList.add(oneBlock.toString().getBytes());
			blockNum++;
			blocks ++;
		    }
		} // for BlockMetadata
	    } // for Footer
	    fileNum++;
	}

	byte[][] fileStatsInfo = null;
        
	if (fileStatusList.size() > 0)
	    {
		fileStatsInfo = new byte[fileStatusList.size()][];
		for (int i = 0; i < fileStatusList.size(); i++) {
		    fileStatsInfo[i] = fileStatusList.get(i);
		}
	    }
	else
	    {
		fileStatsInfo = new byte[0][];
	    }

	//byte[][] fslA = fsl.toArray(new byte[fsl.size()]);
        
	return fileStatsInfo;
    }

    public byte[][] byteList2ByteArray(List<byte[]> list) 
    {
	byte[][] byteArray = null;
        
	if (list.size() > 0)
	    {
		byteArray = new byte[list.size()][];
		for (int i = 0; i < list.size(); i++) {
		    byteArray[i] = list.get(i);
		}
	    }
	else
	    {
		byteArray = new byte[0][];
	    }

	return byteArray;
    }

    // convert a tree map into a byte array.
    public byte[][] treemap2ByteArray(TreeMap<String, String> tmap) 
    {
	byte[][] byteArray = null;
        
	if (tmap.size() > 0) {
	    byteArray = new byte[tmap.size()][];
	    int i=0;
	    for(Map.Entry<String,String> entry : tmap.entrySet()) {
		String key = entry.getKey();
		String value = entry.getValue();

		//System.out.println("key=" + key + ", value=" + value);
		if (logger.isDebugEnabled()) {
		    logger.debug("key=" + key + ", value=" + value);
		}

		byteArray[i++] = value.getBytes();
	    }
	} else
	    {
		byteArray = new byte[0][];
	    }

	return byteArray;
    }

    // convert a time stamp in millisecond to seconds
    public long truncateToSeconds(long timestamp)
    {
	return (timestamp + 500)/1000; // to seconds
    }
    
    private String base64Encode(String str)
	throws UnsupportedEncodingException
    {
	return Base64.getEncoder().encodeToString(str.getBytes());
    }
    
    // generate a stats string for a directory path.
    public String genDirectoryStats(FileSystem fs, Path path)
	throws IOException
    {
	String value = "DirectoryStats|";
	value += base64Encode(path.toString()) + "|";

	long modTime = fs.getFileStatus(path).getModificationTime();
	modTime = truncateToSeconds(modTime);

	value += modTime + "|";

	return value;
    } 
                
    long computeSumOfLengthForStringColumns(BlockMetaData b)
    {
	long sum = 0;

	for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData ccmd : b.getColumns()) 
	    {
		//System.out.println("ColType=" + ccmd.getType() );

		switch ( ccmd.getType() )
		    {
		    case BINARY:
			break;

		    default:
			continue;
		    }

		//org.apache.parquet.column.statistics.Statistics colStats = null;
		//colStats = ccmd.getStatistics();
                            
		//if (colStats.isEmpty()) // no stats yet 
		//  continue;
                            
		sum += ccmd.getTotalSize();
	    }
	return sum;
    }

    // From parquet-hadoop/src/main/java/org/apache/parquet/hadoop/util/HiddenFileFilter.java
    // to ensure compatibility on which files are skipped by Parquet reader
    public boolean accept(Path p){
        String name = p.getName(); 
        return !name.startsWith("_") && !name.startsWith("."); 
    }

    //
    // Return a list of entries in bytes[] form. Each entry is either
    // a DirectoryStats or a FileStats.
    //
    // An example.
    //
    //key=hdfs://localhost:29000/user/trafodion/hive/tpcds/store_sales_parquet/000001_0
    //value=FileStats|hdfs://localhost:29000/user/trafodion/hive/tpcds/store_sales_parquet|000001_0|91575835|1509197432|1509025835|134217728|1|1|adev04.esgyn.com|16|5880479|5880479|92690|4|5881118|5881118|92660|5880483|5884379|5884379|92614|11761601|5882571|5882571|92700|17645980|5882578|5882578|92615|23528551|5882491|5882491|92625|29411129|5881296|5881296|92637|35293620|5881275|5881275|92634|41174916|5884099|5884099|92666|47056191|5883296|5883296|92649|52940290|5884571|5884571|92669|58823586|5880227|5880227|92704|64708157|5880128|5880128|92684|70588384|5880470|5880470|92689|76468512|5881425|5881425|92628|82348982|3319722|3319722|50355|88230407|

    public 
	byte[][] getFileAndBlockAttributes(String rootDir) 
	throws FileNotFoundException, IOException
    {
	timer tm = new timer();

	if (logger.isDebugEnabled()) {
	    tm.start();
	}

	Path path = new Path(rootDir);

	FileSystem fs = path.getFileSystem(conf);

	List<FileStatus> listOfFileStatus = new ArrayList<FileStatus>();

	TreeMap<String, String> tmap = new TreeMap<String, String>();

	// insert a direcotry stats for rootDir.
	tmap.put(rootDir, genDirectoryStats(fs, path));

	org.apache.hadoop.fs.RemoteIterator<LocatedFileStatus> 
	    //fileStatusItr = fs.listLocatedStatus(path);
	    fileStatusItr = fs.listFiles(path, true);

	boolean lv_blocksize_checked = false;
	boolean lv_concat_blocks = false;

	while (fileStatusItr.hasNext()) {
	    LocatedFileStatus f = fileStatusItr.next();

	    if (f.isFile() && accept(f.getPath()) && f.getLen() > 0) {
		Path filePath = f.getPath();
		Path parentPath  = filePath.getParent();

		// first check if the parent directory's stats
		// is in the map table. If not, construct the stats
		// first and then add it to the map table.
		String parent = parentPath.toString();
		if ( tmap.get(parent) == null ) 
		    {
			tmap.put(parent, genDirectoryStats(fs, parentPath));
		    }

		listOfFileStatus.add(f);

		// accumulate file attributes from f

		// To maintain a precision in seconds so that the batch reading 
		// interface and the libhdfs return the same time stamp values.
		//
		// Example: modTime = 1508345634992 (from f)
		//          modTime = 1508345634    (the libhdfs API)
		long modTime = truncateToSeconds(f.getModificationTime());

		String fileName = filePath.getName();
		long length = f.getLen();
		long accessTime = truncateToSeconds(f.getAccessTime());
		long blockSize = f.getBlockSize();
		short repFactor = f.getReplication();

		StringBuilder value = new StringBuilder();
		value.append("FileStats|");

		// base64 encode the path and the file name to prevent
		// the delimit char '|' being part of these names.
		value.append(base64Encode(parent));
		value.append("|");
		value.append(base64Encode(fileName));
		value.append("|");

		value.append(length);
		value.append("|");
		value.append(accessTime);
		value.append("|");
		value.append(modTime);
		value.append("|");
		value.append(blockSize);
		value.append("|");

		BlockLocation[] locations = f.getBlockLocations();
    
		value.append(repFactor).append('|')         // replication factor
		     .append(locations.length).append('|'); // # of blocks
		
		for (int b=0; b<locations.length; b++ )  {         
			String[] hosts = locations[b].getHosts();
			for (short i=0; i<repFactor; i++ ) 
				value.append(i<hosts.length ? hosts[i] : " ").append('|');            
		}

                locations = null;

		if (logger.isDebugEnabled()) {
			logger.debug("getFileAndBlockAttributes, value: " + value.toString() + " \n");
		}
		tmap.put(filePath.toString(), value.toString());

      }//end if 
    }//end while
    fileStatusItr = null;

	if (logger.isDebugEnabled()) {
	    tm.stop();
	    logger.debug(tm.getElapsedTime("ET to get all file attributes="));

	    tm.start();
	}

	int footers= 0;
	long sumOfStringLength = 0;

	for (Footer f : ParquetFileReader.readAllFootersInParallel(
								   conf, listOfFileStatus, false)) {
	    footers++;

	    List<BlockMetaData> blockMetaData = f.getParquetMetadata().getBlocks();

	    StringBuilder strBlockMetaData = new StringBuilder();

	    long compressedSize = 0;
	    long totalSize      = 0;
	    long rowCount       = 0;
	    long startingPos    = 0;

	    long    lv_block_count = 0;
	    boolean lv_continuation = false;
	    long    lv_blocks_in_file = blockMetaData.size();
	    long    lv_current_block = 0;
	    int     lv_block_sets = 0;

	    for (BlockMetaData b : blockMetaData){

		sumOfStringLength += computeSumOfLengthForStringColumns(b);

		if (!lv_continuation) {
		    compressedSize = b.getCompressedSize();
		    totalSize      = b.getTotalByteSize();
		    rowCount       = b.getRowCount();
		    startingPos    = b.getStartingPos();
		}
		else {
		    compressedSize += b.getCompressedSize();
		    totalSize      += b.getTotalByteSize();
		    rowCount       += b.getRowCount();
		}

		if (! lv_blocksize_checked) {
		    lv_blocksize_checked = true;
		    if (sv_concat_blocks_enabled) {
			if ( totalSize < ( sv_concat_blocksize_in_mb * 1024 * 1024 ) ) {
			    lv_concat_blocks = true;  
			}
		    }

		    if (logger.isDebugEnabled()) logger.debug("getFileAndBlockAttributes: " + rootDir
							      + ", file: " + f.getFile().toString()
							      + ", concat blocks for this file: " + lv_concat_blocks
							      + ", concat blocks enabled: " + sv_concat_blocks_enabled
							      + ", concat blocks size(mb): " + sv_concat_blocksize_in_mb
							      + ", concat blocks max: " + sv_concat_blocks_max
							      );
		}

		if (lv_concat_blocks) {
		    ++ lv_block_count;
		    lv_continuation = true;
		    if (lv_block_count >= sv_concat_blocks_max) {
			lv_continuation = false;
			lv_block_count = 0;
		    }
		    
		    lv_current_block++;
		    if (lv_current_block >= lv_blocks_in_file) {
			lv_continuation = false;
		    }
		}

		if (!lv_continuation) {
		    lv_block_sets++;
		    strBlockMetaData.append(String.valueOf(compressedSize));
		    strBlockMetaData.append("|");
		    strBlockMetaData.append(String.valueOf(totalSize));
		    strBlockMetaData.append("|");
		    strBlockMetaData.append(String.valueOf(rowCount));
		    strBlockMetaData.append("|");
		    strBlockMetaData.append(String.valueOf(startingPos));
		    strBlockMetaData.append("|");
		}

		if (logger.isTraceEnabled()) logger.trace("getFileAndBlockAttributes: " + rootDir
							  + ", file: " + f.getFile()
							  + ", compressedSize: " + compressedSize
							  + ", totalSize: " + totalSize
							  + ", rowCount: " + rowCount
							  + ", startingPos: " + startingPos
							  + ", continuation: " + lv_continuation
							  + ", blockCountInSet: " + lv_block_count
							  + ", blockCountInFile: " + lv_current_block
							  + ", #blockSets: " + lv_block_sets
							  + ", #blocksInFile: " + lv_blocks_in_file
							  );

                    
	    } // for BlockMetadata

	    blockMetaData = null;

	    String fullFilePath = f.getFile().toString();
	    StringBuilder newValue = new StringBuilder();
	    newValue.append(tmap.get(fullFilePath));
	    newValue.append(String.valueOf(lv_block_sets));
	    newValue.append("|");
	    newValue.append(strBlockMetaData.toString());
	    newValue.append("|");
	    newValue.append(String.valueOf(sumOfStringLength));

	    tmap.put(fullFilePath, newValue.toString());

	    newValue = null;
	    strBlockMetaData = null;
	} // for Footer

	if (logger.isDebugEnabled()) {
	    tm.stop();
	    logger.debug(tm.getElapsedTime("ET to get all block attributes="));
	}

	byte[][] byteArray = treemap2ByteArray(tmap);

	return byteArray;
    }

    public String[] getFileSchema(String tableName,
				  String rootDir,
				  Object[] col_name_vec,
				  Object[] col_type_vec,
				  int flags)
	throws IOException {

	if (logger.isDebugEnabled()) logger.debug("Enter getFileSchema"
						  + ", table: " + tableName
						  + ", rootDir: " + rootDir
						  );

	setFlags(flags);

	Path fp = new Path(rootDir);
	FileStatus[] ifs = fp.getFileSystem(conf).listStatus(fp);

	String readSchema = null;
	String writeSchema = null;

	/*  
	// get read schema
	for (FileStatus fs : ifs){
            if (!(fs.isFile() && accept(fs.getPath()) && fs.getLen() > 0))
                continue;
	    readSupport = new TrafReadSupport(); 
	    pb = TrafParquetReader.builder(readSupport, fs.getPath());
	    pb.withConf(conf);
	    reader = pb.build();

	    if (reader != null) {
		Group group = reader.read();
		if (group != null) {
		    GroupType groupType = group.getType();
		    readSchema = groupType.toString();
		}
	    }

	    reader.close();
	    break;
	} // for
        */

	// get write schema
	if (col_name_vec != null) {
	    setupColInfo(-1, null, col_name_vec, col_type_vec);
            
	    schema = generateParquetSchema(tableName,
					   listOfReturnedColName,
					   listOfReturnedColType,
					   listOfReturnedColPrec,
					   listOfReturnedColScale);
	    if (schema != null) {
		writeSchema = schema.toString();
	    }
	}

	String[] schemas = new String[2];
	schemas[0] = readSchema;
	schemas[1] = writeSchema;
        
	return schemas;
    }

    static Scanner s_scanner = null;
    
    public static void waitForKeyboard(String p_msg)
    {
	if (s_scanner == null) { s_scanner = new Scanner(System.in); }
	    System.out.print(p_msg);
	    String userdata = s_scanner.next();
    }

    public void experimentalRead1(String pv_in_file) throws IOException
    {
	open("store_sales_parquet",
	     pv_in_file,
	     0L,
	     1000000,
	     -1,
	     null,
	     1000,
	     2000,
	     5,
	     null,
	     null,
	     null,
	     null,
             null,
	     0);

	if (sv_timer_on) m_process_block_timer.start2();
	allocateMemory();
	
	boolean lv_done = false;
	Group lv_g = null;
	long lv_number_rows = 0;

	while (!lv_done) {
	    lv_g = reader.read();
	    if (lv_g == null) {
		lv_done = true;
	    }
	    ++lv_number_rows;
	}

	if (sv_timer_on) m_process_block_timer.stop2();

        close();

	System.out.println("#rows: " + lv_number_rows);
    }

    public void experimentalRead(String pv_in_file) throws IOException
    {
	open("store_sales_parquet",
	     pv_in_file,
	     0L,
	     1000000,
	     -1,
	     null,
	     1000,
	     2000,
	     5,
	     null,
	     null,
	     null,
	     null,
             null,
	     0);

	if (sv_timer_on) m_process_block_timer.start2();
	allocateMemory();
	
	boolean lv_done = false;
	Group lv_g = null;
	long lv_number_rows = reader.read2();

	if (sv_timer_on) m_process_block_timer.stop2();

        close();

	System.out.println("#rows: " + lv_number_rows);
    }

    public static void main(String[] args) throws Exception
    {

	System.out.println("#args: " + args.length);
	String lv_in_file = "/user/trafodion/hive/tpcds/store_sales_parquet";
	int lv_repeat_count = 1;
        if (args.length > 0) {
	    lv_in_file = args[0];
            if (args.length > 1) {
		lv_repeat_count = Integer.parseInt(args[1]);
	    }
	}	

	System.out.println("lv_in_file: " + lv_in_file + ", repeat count: " + lv_repeat_count);

	TrafParquetFileReader reader = new TrafParquetFileReader();
	for (int i = 0; i < lv_repeat_count; i++) {
	    TrafParquetFileReader lv_reader = new TrafParquetFileReader();
	    reader.experimentalRead(lv_in_file);
	}

	int lv_index = 0;
	for (String lv_arg : args) {
	    lv_index++;
	    if (lv_arg.compareTo("-getFileAndBlockAttributes") == 0) {
		String path;
		if (args.length == 1 )
		    path = "/user/trafodion/hive/tpcds/store_sales_parquet";
		else
		    path = args[lv_index];

		reader.getFileAndBlockAttributes(path);
		System.exit(0);
		break;
	    } else if (lv_arg.compareTo("-testHash") == 0) {

		    String lv_file = null;

		    if (args.length > 1) {
		       lv_file = args[1];
		    }

		    System.out.println("file name: " + lv_file);

		    TrafParquetFileReader.testHash(lv_file);


		    System.exit(0);
	    } else if (lv_arg.compareTo("-testMultiHash") == 0) {

		    String lv_file = null;
		    String datatype = null;
		    int funcs = 0;
		    int bytes = 0;

		    if (args.length > 4) {
		       datatype = args[1];
		       lv_file = args[2];
		       funcs = Integer.parseInt(args[3]);
		       bytes = Integer.parseInt(args[4]);
		    }

		    System.out.print("file name=" + lv_file);
		    System.out.println(", datatype=" + datatype);
		    System.out.println(", funcs=" + funcs);
		    System.out.println(", bytes=" + bytes);

                    if ( datatype.compareTo("int") == 0 )
		       TrafParquetFileReader.testMultiHashInt(lv_file, funcs, bytes);
                    else 
                    if ( datatype.compareTo("timestamp") == 0 )
		       TrafParquetFileReader.testMultiHashTimestamp(lv_file, funcs, bytes);
                    else {
                       System.out.println(datatype + " not handled yet.");
                    }

		    System.exit(0);
	    } else
		if (lv_arg.compareTo("-getFileStats") == 0) {
		    String lv_file = "hdfs://localhost:25400/user/hive/warehouse/tparquet10";
		    if (args.length > 1) {
			lv_file = args[1];
		    }
		    System.out.println("file name: " + lv_file);
		    reader.getFileStats(lv_file);
		    waitForKeyboard("Enter any key to continue: ");
		    System.exit(0);
		} else  {
		    System.out.println("TrafParquetFileReader: unknown options");
		    System.out.println("    known options:");
		    System.out.println("       -getFileAndBlockAttributes or");
		    System.out.println("       -getFilestats. ");
		    System.out.println("       -testHash. ");
		    System.out.println("       -testMultiHash. ");
		}
	}
    }
}
