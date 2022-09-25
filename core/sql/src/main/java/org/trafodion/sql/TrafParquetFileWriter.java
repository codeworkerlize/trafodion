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

import java.io.IOException;
import java.util.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ByteOrder;

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.conf.Configuration;
import org.trafodion.sql.TrafConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import java.lang.Integer;
import java.lang.Long;
import java.sql.Timestamp;
import java.sql.Date;
import java.math.BigInteger;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.column.ParquetProperties;

import org.apache.parquet.Log;
import org.apache.parquet.hadoop.example.*;
import org.apache.parquet.example.*;
import org.apache.parquet.example.data.*;
import org.apache.parquet.example.data.simple.*;

import org.apache.parquet.schema.*;
import org.apache.parquet.io.api.Binary;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.log4j.MDC;

public class TrafParquetFileWriter extends TrafExtStorageUtils {

    private class Builder extends ParquetWriter.Builder<Group, Builder> {
        private Builder(Path file) {
            super(file);
        }
        
        @Override
            protected Builder self() {
            return this;
        }
        
        @Override
            protected WriteSupport<Group> getWriteSupport(Configuration conf) {
            return writeSupport;
        }
        
    }        
    
    static Logger logger = Logger.getLogger(TrafParquetFileWriter.class.getName());;
    static Configuration conf = null;

    Builder builder                 = null;
    ParquetWriter<Group> writer     = null;

    GroupWriteSupport writeSupport  = null;
    Path filePath                   = null;

    MessageType schema              = null;
    SimpleGroupFactory groupFact    = null;
    
    List<String> listOfColName;  
    int[] listOfColType;
    int[] listOfColPrecision;
    int[] listOfColScale;

    NullWritable nullValue;

    static {
	conf = TrafConfiguration.create(TrafConfiguration.HDFS_CONF);
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        MDC.put("PID", pid);
	System.setProperty("hostName", System.getenv("HOSTNAME"));
    }
 
    public String open(String tableName,
                       String fileName,
                       Object[] colNameList,
                       Object[] colTypeInfoList,
                       long inBlockSize,
                       long inPageSize,
                       long inDictionaryPageSize,
                       long inWriterMaxPadding,
                       String inCompression,
                       String parqSchStr,
                       int flags) throws IOException {

	if (logger.isDebugEnabled()) 
            logger.debug("Enter open()," 
                         + " file name: " + fileName
                         + " #colNameList: " + (colNameList == null ? "null" : colNameList.length)
                         + " #colTypeInfoList: " + (colTypeInfoList == null ? "null" : colTypeInfoList.length)
			 + " flags: " + flags
                         );

        setFlags(flags);

        listOfColName = new ArrayList<String>(colNameList.length);
        for (int i = 0; i < colNameList.length; i++) {
            listOfColName.add((String)colNameList[i]);
        }

        listOfColType = new int[colTypeInfoList.length];
        listOfColPrecision = new int[colTypeInfoList.length];
        listOfColScale = new int[colTypeInfoList.length];

        for (int i = 0; i < colTypeInfoList.length; i++) {
            ByteBuffer bb = ByteBuffer.wrap((byte[])colTypeInfoList[i]);
            bb.order(ByteOrder.LITTLE_ENDIAN);

            int type      = bb.getInt();
            int length    = bb.getInt();
            int precision = bb.getInt();
            int scale     = bb.getInt();

            listOfColType[i]=type;
            listOfColPrecision[i]=precision;
            listOfColScale[i]=scale;
        }

        /*
        schema = generateParquetSchema(tableName,
                                       listOfColName,
                                       listOfColType,
                                       listOfColPrecision,
                                       listOfColScale);
        */
        schema = generateParquetSchema(parqSchStr);

	if (logger.isDebugEnabled()) logger.debug("generated schema "
						  + ", schema: " + schema
						  );

        groupFact = new SimpleGroupFactory(schema);

        filePath = new Path(fileName); 
        writeSupport = new GroupWriteSupport(); 
        writeSupport.setSchema(schema, conf);

        builder = new Builder(filePath);
        writer =
            builder.withConf(conf)
            .withWriteMode(ParquetFileWriter.Mode.CREATE)
            .withCompressionCodec(CompressionCodecName.fromConf(inCompression))
            .withRowGroupSize((int)inBlockSize)
            .withPageSize((int)inPageSize)
            .withDictionaryPageSize((int)inDictionaryPageSize)
            .withDictionaryEncoding(isParquetEnableDictionary())
            .withMaxPaddingSize((int)inWriterMaxPadding)
            .withWriterVersion(ParquetWriter.DEFAULT_WRITER_VERSION)
            //.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
            .build();

        return null;
    }

    public String close() throws IOException {
	if (logger.isDebugEnabled()) 
            logger.debug("Enter close() " 
                         );
        
        writer.close();

        return null;
    }

    public String insertRows(Object buffer, int bufMaxLen, 
                             int numRows, int bufCurrLen) throws IOException {

	if (logger.isDebugEnabled()) 
            logger.debug("Enter insertRows()," 
                         + " bufMaxLen: " + bufMaxLen
                         + " bufCurrLen: " + bufCurrLen
                         + " numRows: " + numRows
                         );

        ByteBuffer bb = (ByteBuffer)buffer;

        bb.order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < numRows; i++) {

            Group group = groupFact.newGroup();
            GroupType groupType = group.getType();
            for (int j = 0; j < listOfColType.length; j++) {

                int type = listOfColType[j];
                int scale = listOfColScale[j];
                
                short nullVal = bb.getShort();
                int length  = 0;
                if (nullVal == 0)
                    {
                        length = bb.getInt();
                    }

                if (nullVal != 0) { // null value
                    // skip writing this value.
                }
                else if ((nullVal == 0) && (length >= 0)) { // non-nullable val
                    byte[] colVal = new byte[length];
                    bb.get(colVal, 0, length);
                    
                    switch (type) {
                    case HIVE_BOOLEAN_TYPE:
                        String s = Bytes.toString(colVal, 0, length);
                        boolean bv = (s.equals("TRUE") ? true : false);
                        group.add(j, bv);
                        break;
                        
                    case HIVE_BYTE_TYPE:
                        int hb = (Integer.parseInt(Bytes.toString(colVal, 0, length)));
                        
                        group.add(j, hb);
                        break;
                        
                    case HIVE_SHORT_TYPE:
                        {
                            int hs = (Integer.parseInt(Bytes.toString(colVal, 0, length)));
                            
                            group.add(j, hs);
                        }
                        break;

                    case HIVE_INT_TYPE:
                        {
                            int hi = (Integer.parseInt(Bytes.toString(colVal, 0, length)));
                            group.add(j, hi);
                        }
                        break;

                    case HIVE_LONG_TYPE:
                        {
                            long hl = Long.parseLong(Bytes.toString(colVal, 0, length));
                            group.add(j, hl);
                        }                
                        break;
                        
                    case HIVE_FLOAT_TYPE:
                        {
                            float hf =
                                Float.parseFloat(Bytes.toString(colVal, 0, length));
                            group.add(j, hf);
                        }
                        break;

                    case HIVE_DOUBLE_TYPE:
                        {
                            double hd =
                                Double.parseDouble(Bytes.toString(colVal, 0, length));
                            group.add(j, hd);
                        }
                        break;

                     case HIVE_DECIMAL_TYPE:
                         {
                             Type parqType = groupType.getType(j);
                             OriginalType origType = parqType.getOriginalType();

			     if (logger.isDebugEnabled()) logger.debug("Hive decimal type"
								       + ", colName: " + listOfColName.get(j)
								       + ", colVal: " + new String(colVal)
								       + ", type:"  + type
								       + ", precision: " + listOfColPrecision[j]
								       + ", scale: " + scale
								       + ", parqType: " + parqType
								       + ", origType: " + origType
								       );
			     

                             int flbaSize = 0;
                             if (parqType.isPrimitive()) {

                                 String decStr = Bytes.toString(colVal, 0, length);
                                 HiveDecimal hiveDec = HiveDecimal.create(decStr);
                                 int hiveScale = hiveDec.scale();
                                 BigInteger decBi = hiveDec.unscaledValue();
                                 if (scale > hiveScale) {
                                     BigInteger powBi = BigInteger.valueOf(10);
                                     powBi = powBi.pow(scale-hiveScale);
                                     decBi = decBi.multiply(powBi);
                                 }
                                 
                                 PrimitiveType primType = parqType.asPrimitiveType();
                                 PrimitiveType.PrimitiveTypeName primTypeName = primType.getPrimitiveTypeName();
                                     
                                 if (primTypeName == PrimitiveType.PrimitiveTypeName.INT32)
                                     {
					 if (logger.isDebugEnabled())
					     logger.debug("ParquetWriter insertRows"
							  + ", primitive type int32"
							  );
					 
                                         int lv_i = decBi.intValue();
                                         group.add(j, lv_i);
                                     }
                                 else if (primTypeName == PrimitiveType.PrimitiveTypeName.INT64)
                                     {
					 if (logger.isDebugEnabled())
					     logger.debug("ParquetWriter insertRows"
							  + ", primitive type int64"
							  );
					 long lv_l = decBi.longValue();
					 group.add(j, lv_l);
                                     }
                                 else if (primTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                     {
                                         flbaSize = primType.getTypeLength();
                                         byte[] decBiBytes = decBi.toByteArray();
					 byte[] decBiAllBytes = null;
                                         int ii = 0;
					 if (decBiBytes.length < flbaSize) {
					     int lv_sign = decBi.compareTo(java.math.BigInteger.ZERO);
					     byte lv_init = 0;
					     if (lv_sign < 0) {
						 lv_init = -1;
					     }
					     
					     decBiAllBytes = new byte[flbaSize];
					     for (ii = 0; ii < (flbaSize - decBiBytes.length); ii++) {
						 decBiAllBytes[ii] = lv_init;
					     }
					     int lv_src_offset = 0;
					     for (ii = (flbaSize - decBiBytes.length); ii < decBiAllBytes.length; ii++) {
						 decBiAllBytes[ii] = decBiBytes[lv_src_offset++];
					     }
					 }
					 else {
					     decBiAllBytes = decBiBytes;
					 }

                                         Binary decBin = Binary.fromByteArray(decBiAllBytes);
					 if (logger.isDebugEnabled())
					     logger.debug("ParquetWriter insertRows"
							  + ", primitive type fixed len byte array"
							  + ", decStr: " + decStr
							  + ", hiveDec: " + hiveDec
							  + ", hiveScale: " + hiveScale
							  + ", decBi: " + decBi
							  + ", type length: " + flbaSize
							  + ", len byte array(i): " + decBiBytes.length
							  + ", value byte array(i): " + Hex.encodeHexString(decBiBytes)
							  + ", len byte array: " + decBiAllBytes.length
							  + ", value byte array: " + Hex.encodeHexString(decBiAllBytes)
							  + ", value Binary: " + decBin
							  );
                                         group.add(j, decBin);
                                     }
                                 
                             } else {
                                 throw new IOException("TrafParquetFileWriter.insertRows: DECIMAL datatype not yet supported for insert");
                             }
                         }
                         break;

                    case HIVE_CHAR_TYPE:
                    case HIVE_VARCHAR_TYPE:
                    case HIVE_STRING_TYPE:
                        {
                            String hs = Bytes.toString(colVal, 0, length);
                            group.add(j, hs);
                        }
                        break;

                    case HIVE_BINARY_TYPE:
                        {
                            Binary bin = Binary.fromConstantByteArray(colVal, 0, length);
                            group.add(j, bin);
                        }
                        break;

                    case HIVE_DATE_TYPE:
                        {
                            if (true)
                                throw new IOException("TrafParquetFileWriter.insertRows: DATE datatype not yet supported for insert");

                            /*                        
                               colVals[j] = new DateWritable();
                               ((DateWritable)colVals[j]).set
                               (Date.valueOf(Bytes.toString(colVal, 0, length)));
                            */
                        
                        }
                        break;

                    case HIVE_TIMESTAMP_TYPE:
                        {
                            Type parqType = groupType.getType(j);
                            Timestamp ts = 
                                Timestamp.valueOf(Bytes.toString(colVal, 0, length));

                            if (! (((parqType.isPrimitive()) &&
                                    ((parqType.asPrimitiveType().getPrimitiveTypeName() == 
                                      PrimitiveType.PrimitiveTypeName.INT96) ||
                                     (parqType.asPrimitiveType().getPrimitiveTypeName() == 
                                      PrimitiveType.PrimitiveTypeName.INT64)))))
                                {
                                    throw new IOException("TrafParquetFileWriter.insertRows: Unsupported datatype for insert");
                                }

                            if (parqType.asPrimitiveType().getPrimitiveTypeName() == 
                                PrimitiveType.PrimitiveTypeName.INT96)
                                {
				    // The following creates a NanoTime that factors in the timezone
				    // Assumption: timetamp data in Parquet is in GMT
				    // note: default for the 2nd parameter to getNanoTime is false: do not skip time zone conversion
				    NanoTime nt = 
					NanoTimeUtils.getNanoTime(ts,
								  sv_parquet_timestamp_skip_conversion 
								  );
                                    group.add(j, nt);
                                }
                            else // INT64
                                {
                                    // Timestamp "ts" returns millis values since java epoch
                                    //  ('1970-01-01 00:00:00'). Could be after or before.
                                    // 
                                    // Write timestamp value in microseconds since epoch.
                                    // -ve value is microsecs before epoch, +ve is after.
                                    long tsVal = Math.abs((ts.getTime()/1000) * 1000000) +
                                        Math.abs(ts.getNanos()/1000);
                                    if (ts.getTime() < 0)
                                        tsVal = -tsVal;

                                    group.add(j, tsVal);
                                }
                        }
                        break;

                    default:
                        {
                            if (true)
                                throw new IOException("TrafParquetFileWriter.insertRows: Unsupported datatype for insert");
                        }
                        break;
                        
                    } // switch
                    
                } // else non-nullable val

            } // for

            writer.write(group);
        } // for numRows

        return null;
    }
    
    TrafParquetFileWriter() {
	if (logger.isTraceEnabled()) logger.trace("Enter TrafParquetFileWriter()");
    }

    public static void main(String[] args) throws IOException,
                                                InterruptedException,
                                                ClassNotFoundException {

  }
}




// Do not remove the next commented out section on HIVE_DECIMAL_TYPE. It is WIP.
// DO NOT REMOVE: START
/*
  case HIVE_DECIMAL_TYPE:
  {

  // String decStr = Bytes.toString(colVal, 0, length);
  //HiveDecimal hiveDec = HiveDecimal.create(decStr);
  //BigInteger decBi = hiveDec.unscaledValue();

  //byte[] decBiBytes = decBi.toByteArray();
  //int lenDecBiBytes = decBiBytes.length;
  //Binary decBin = Binary.fromByteArray(decBiBytes, 0, lenDecBiBytes);
  //group.add(j, decBin);



  //System.out.println("lenDecBiBytes = " + lenDecBiBytes);
  //int iv = decBi.intValue();
  //System.out.println("iv = " + iv);

  //group.add(j, iv);

  //throw new IOException("TrafParquetFileWriter.insertRows: DECIMAL datatype not yet supported for insert");

  Type parqType = groupType.getType(j);
  OriginalType origType = parqType.getOriginalType();

  int flbaSize = 0;
  if (parqType.isPrimitive())
  {
  String decStr = Bytes.toString(colVal, 0, length);
  HiveDecimal hiveDec = HiveDecimal.create(decStr);
  //System.out.println("hiveDec1 = " + hiveDec.toString());                                     
                                     
  int hiveScale = hiveDec.scale();
  BigInteger decBi = hiveDec.unscaledValue();
  //System.out.println("decBi1 = " + decBi.toString());
  if (scale > hiveScale)
  {
  decBi = decBi.multiply(BigInteger.valueOf(10));

  //                                             System.out.println("hiveDec2 = " + hiveDec.toString());

  //hiveDec = hiveDec.scaleByPowerOfTen(scale-hiveScale);

  //System.out.println("hiveDec3 = " + hiveDec.toString());
  }
                                     
  //                                     BigInteger decBi2 = hiveDec.unscaledValue();
  //                                     System.out.println("decBi2 = " + decBi2.toString());

  //System.out.println("decBi2 = " + decBi.toString());

  PrimitiveType primType = parqType.asPrimitiveType();
  PrimitiveType.PrimitiveTypeName primTypeName = primType.getPrimitiveTypeName();
                                     
  if (primTypeName == PrimitiveType.PrimitiveTypeName.INT32)
  {
  //System.out.println("scale = " + scale + " hiveScale = " + hiveScale + " decStr = " + decStr + " decBi = " + decBi.toString());
  int lv_i = decBi.intValue();
  //System.out.println("lv_i = " + lv_i);
  group.add(j, lv_i);
  }
  else if (primTypeName == PrimitiveType.PrimitiveTypeName.INT64)
  {
  long lv_l = decBi.longValue();
  group.add(j, lv_l);
  }
  else if (primTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
  {
  flbaSize = primType.getTypeLength();
  System.out.println("flbaSize = " + flbaSize);
  byte[] decBiBytes = decBi.toByteArray();
  Binary decBin = Binary.fromByteArray(decBiBytes);
  group.add(j, decBin);
  }

  }
  else
  {
  throw new IOException("TrafParquetFileWriter.insertRows: DECIMAL datatype not yet supported for insert");
  }
*/
/*
  String decStr = Bytes.toString(colVal, 0, length);
  //System.out.println("decStr = "+ decStr);
  HiveDecimal hiveDec = HiveDecimal.create(decStr);

  //String hiveDecStr = hiveDec.toString();
  //System.out.println("hiveDecStr = " + hiveDecStr);

  int hiveScale = hiveDec.scale();
  //System.out.println("hiveScale = " + hiveScale);

  if (scale > hiveScale)
  {
  hiveDec = hiveDec.scaleByPowerOfTen(scale-hiveScale);
  }

  BigInteger decBi = hiveDec.unscaledValue();
  System.out.println("decBi " + decBi.toString());

  byte[] decBiBytes = decBi.toByteArray();

  ByteBuffer b = ByteBuffer.allocate(flbaSize);
  b.put(decBiBytes);
  //                             byte[] decBiNewBytes = b.
  int lenDecBiBytes = decBiBytes.length;
  Binary decBin = Binary.fromByteArray(decBiBytes, 0, lenDecBiBytes);
  System.out.println("lenDecBiBytes = " + lenDecBiBytes);


  //                             byte[] decBiBytes2 = new byte[3];
                             
  group.add(j, decBin);
*/

/*
  System.out.println("hiveDecStr2 = " + hiveDec.toString());
                             
  Buffer buf = AvroSerdeUtils.getBufferFromDecimal(hiveDec, 0);
  //                             if (buf.isDirect())
  {
  byte[] b = AvroSerdeUtils.getBytesFromByteBuffer((ByteBuffer)buf);
  Binary decBin = Binary.fromByteArray(b, 0, b.length);                                     
  group.add(j, decBin);
  }
*/

//                             group.add(j, b);

/*     
       HiveDecimalWritable hdw = new HiveDecimalWritable(hiveDec);
                             
       byte[] his = hdw.getInternalStorage();
       Binary decBin = Binary.fromByteArray(his, 0, his.length);
       group.add(j, decBin);
*/

/*
//BigInteger decBi = new BigInteger(hiveDec.unscaledValue());
BigInteger decBi = hiveDec.unscaledValue();
                             
System.out.println("decBi " + decBi.toString());

byte[] decBiBytes = decBi.toByteArray();
int lenDecBiBytes = decBiBytes.length;
Binary decBin = Binary.fromByteArray(decBiBytes, 0, decBiBytes.length);

System.out.println("lenDecBiBytes = " + lenDecBiBytes);
//String ds = Bytes.toString(decBiBytes, 0, decBiBytes.length);
//System.out.println("ds = "+ ds);

group.add(j, decBin);
*/

/*
  BigInteger decBi = new BigInteger(decStr);
  System.out.println("here");
  int iv = decBi.intValue();
  System.out.println("iv = " + iv);
  byte[] decBiBytes = decBi.toByteArray();
  String ds = Bytes.toString(decBiBytes, 0, decBiBytes.length);
  System.out.println("ds = "+ ds);
  group.add(j, ds);
*/

// DO NOT REMOVE: END
