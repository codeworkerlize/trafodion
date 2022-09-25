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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;
import org.trafodion.sql.TrafConfiguration;

import java.math.BigInteger;
import org.apache.hadoop.hive.common.type.HiveDecimal;

import org.apache.orc.OrcFile;
import org.apache.orc.Writer;
import org.apache.orc.TypeDescription;
import org.apache.orc.CompressionKind;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.log4j.MDC;

public class OrcFileVectorWriter extends TrafExtStorageUtils {

    static Logger logger = Logger.getLogger(OrcFileVectorWriter.class.getName());;
    static Configuration conf;
    private Path           filePath;
    private OrcFile.WriterOptions options;
    private Writer         writer;
    private String[]       colNames;
    private int[]          colTypes;
    private int[]          precisions;
    private int[]          scales;
    private int            rowIndexStride;
    private long           stripeSize;
    private String         compression;
    private boolean        blockPadding;
    private int            bufferSize;
    private TypeDescription schema;
    private int            currRowIndex;
    private VectorizedRowBatch writerBatch;

    //NullWritable nullValue;

    static {
	conf = TrafConfiguration.create(TrafConfiguration.HDFS_CONF);
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        MDC.put("PID", pid);
	System.setProperty("hostName", System.getenv("HOSTNAME"));
    }


    public void open(String fileName,
                       Object[] colNameList,
                       Object[] colTypeInfoList,
                       boolean inBlockPadding,
                       long inStripeSize,
                       int inBufferSize,
                       int inRowIndexStride,
                       String inCompression) throws IOException {

       if (logger.isDebugEnabled()) 
            logger.debug("Enter open()," 
                         + " file name: " + fileName
                         + " colNameList: " + colNameList
                         + " colTypeInfoList: " + colTypeInfoList
			 + " , inBlockPadding: " + inBlockPadding
			 + " , inStripeSize: " + inStripeSize
			 + " , inBufferSize: " + inBufferSize
			 + " , inRowIndexStride: " + inRowIndexStride
			 + " , inCompression: " + inCompression
                         );
          
       filePath = new Path(fileName);
       colNames = (String[])colNameList;
       colTypes = new int[colTypeInfoList.length];
       precisions = new int[colTypeInfoList.length];
       scales = new int[colTypeInfoList.length];
       if (inRowIndexStride != 0)
          rowIndexStride = inRowIndexStride;
       else
          rowIndexStride = VectorizedRowBatch.DEFAULT_SIZE;
       stripeSize = inStripeSize;
       compression = inCompression;
       blockPadding = inBlockPadding;
       bufferSize = inBufferSize;
       int length;
       for (int i = 0; i < colTypeInfoList.length; i++) {
           ByteBuffer bb = ByteBuffer.wrap((byte[])colTypeInfoList[i]);
           bb.order(ByteOrder.LITTLE_ENDIAN);
           colTypes[i] = bb.getInt();
           length = bb.getInt();
           precisions[i] = bb.getInt();
           scales[i] = bb.getInt();
       }
       options = OrcFile.writerOptions(conf)
                      .blockPadding(blockPadding)
                      .rowIndexStride(rowIndexStride)
                      .stripeSize(stripeSize)
                      .bufferSize(bufferSize)
                      .compress(CompressionKind.valueOf(inCompression))
                      .useUTCTimestamp(true);
       schema = TypeDescription.createStruct();
       for (int i = 0; i < colTypes.length; i++) {
          switch (colTypes[i]) {
             case HIVE_BOOLEAN_TYPE:
                schema.addField(colNames[i], TypeDescription.createBoolean());
                break;
             case HIVE_BYTE_TYPE:
                schema.addField(colNames[i], TypeDescription.createByte());
                break;
             case HIVE_SHORT_TYPE:
                schema.addField(colNames[i], TypeDescription.createShort());
                break;
             case HIVE_INT_TYPE:
                schema = schema.addField(colNames[i], TypeDescription.createInt());
                break;
             case HIVE_LONG_TYPE:
                schema.addField(colNames[i], TypeDescription.createLong());
                break;
             case HIVE_FLOAT_TYPE:
                schema.addField(colNames[i], TypeDescription.createFloat());
                break;
             case HIVE_DOUBLE_TYPE:
                schema.addField(colNames[i], TypeDescription.createDouble());
                break;
             case HIVE_DECIMAL_TYPE:
                schema.addField(colNames[i], TypeDescription.createDecimal());
                break;
             case HIVE_CHAR_TYPE:
             case HIVE_VARCHAR_TYPE:
             case HIVE_STRING_TYPE:
                schema.addField(colNames[i], TypeDescription.createString());
                break;
             case HIVE_BINARY_TYPE:
                schema.addField(colNames[i], TypeDescription.createBinary());
                break;
             case HIVE_DATE_TYPE:
                schema.addField(colNames[i], TypeDescription.createDate());
                break;
             case HIVE_TIMESTAMP_TYPE:
                schema.addField(colNames[i], TypeDescription.createTimestamp());
                break;
             default:
                throw new IOException("Column of type " + colTypes[i] + " not supported.");
          } // switch
       }
       options.setSchema(schema);
       writer = OrcFile.createWriter(filePath, options); 
       writerBatch = schema.createRowBatch(rowIndexStride);
       currRowIndex = 0;
       return;
    }

    public void close() throws IOException 
    {
	if (logger.isDebugEnabled()) 
            logger.debug("Enter close() " 
                         );
        if (writer == null)
           return;
        if (currRowIndex > 0) {
           writer.addRowBatch(writerBatch);
           currRowIndex = 0;
           writerBatch.reset();
        }
        writer.close();
        writer = null;
        return;
    }

    public void insertRows(Object buffer, int bufMaxLen, 
                             int numRows, int bufCurrLen) throws IOException 
   {
       if (logger.isDebugEnabled()) 
            logger.debug("Enter insertRows()," 
                         + " bufMaxLen: " + bufMaxLen
                         + " bufCurrLen: " + bufCurrLen
                         + " numRows: " + numRows
                         );

       ByteBuffer bb = (ByteBuffer)buffer;
       bb.order(ByteOrder.LITTLE_ENDIAN);
       short nullVal;
       int length;
       byte[] bytesVal;
       short shortVal;
       int intVal;
       long longVal;
       byte  byteVal;
       String strVal;
       double doubleVal;
       float floatVal;
       for (int rowNo = 0; rowNo < numRows; rowNo++) {
          for (int  colNo = 0; colNo < colTypes.length; colNo++) {
             nullVal = bb.getShort();
             if (nullVal != 0) {
                writerBatch.cols[colNo].isNull[currRowIndex] = true; 
                writerBatch.cols[colNo].noNulls = false;
                continue;
             } else 
                length = bb.getInt();
             switch (colTypes[colNo]) {
                case HIVE_BOOLEAN_TYPE:
                case HIVE_BYTE_TYPE:
                case HIVE_SHORT_TYPE:
                case HIVE_INT_TYPE:
                case HIVE_LONG_TYPE:
                   switch (length) {
                      case 1:
                         byteVal = bb.get();
                         ((LongColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = byteVal;
                         break;
                      case 2: 
                         shortVal = bb.getShort();
                         ((LongColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = shortVal;
                         break;
                      case 4: 
                         intVal = bb.getInt();
                         ((LongColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = intVal;
                         break;
                      case 8: 
                         longVal = bb.getLong();
                         ((LongColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = longVal;
                         break;
                      default:
                         bytesVal = new byte[length];
                         bb.get(bytesVal, 0, length);
                         throw new IOException("Invalid value of length " + length + " for column type " + colTypes[colNo] + " of column no " + colNo);
                      }
                   break;
                case HIVE_CHAR_TYPE:
                case HIVE_VARCHAR_TYPE:
                case HIVE_STRING_TYPE:
                case HIVE_BINARY_TYPE:
                   bytesVal = new byte[length];
                   bb.get(bytesVal, 0, length);
                   ((BytesColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = bytesVal;
                   ((BytesColumnVector)writerBatch.cols[colNo]).length[currRowIndex] = length;
                   ((BytesColumnVector)writerBatch.cols[colNo]).start[currRowIndex] = 0;
                   break;
                case HIVE_FLOAT_TYPE:
                case HIVE_DOUBLE_TYPE:
                   if (length == -4) {
                      floatVal = bb.getFloat(); 
                      ((DoubleColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = floatVal;
                      break;
                   }
                   if (length == -8) {
                      doubleVal = bb.getDouble(); 
                      ((DoubleColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = doubleVal;
                      break;
                   }
                   bytesVal = new byte[length];
                   bb.get(bytesVal, 0, length);
                   strVal = Bytes.toString(bytesVal, 0, length);
                   ((DoubleColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = Double.valueOf(strVal);
                   break;
                case HIVE_DECIMAL_TYPE:
                   ((DecimalColumnVector)writerBatch.cols[colNo]).scale = (short)scales[colNo];
                   ((DecimalColumnVector)writerBatch.cols[colNo]).precision = (short)precisions[colNo];
                   if (precisions[colNo] < 19) {
                      switch (length) {
                         case 2: 
                            shortVal = bb.getShort();
                            longVal = shortVal;
                            break;
                         case 4: 
                            intVal = bb.getInt();
                            longVal = intVal;
                            break;
                         case 8: 
                            longVal = bb.getLong();
                            break;
                         default:
                            bytesVal = new byte[length];
                            bb.get(bytesVal, 0, length);
                            throw new IOException("Invalid value of length " + length + " for column type " + colTypes[colNo] + " of column no " + colNo);
                      }                     
                      HiveDecimalWritable hdw = new HiveDecimalWritable(HiveDecimal.create(BigInteger.valueOf(longVal), scales[colNo]));
                      ((DecimalColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = hdw;
                   }
                   else { 
                      bytesVal = new byte[length];
                      bb.get(bytesVal, 0, length);
                      strVal = new String(bytesVal);
                      ((DecimalColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = new HiveDecimalWritable(strVal);
                   }
                   break;
                case HIVE_DATE_TYPE:
                   longVal = bb.getLong();
                   ((LongColumnVector)writerBatch.cols[colNo]).vector[currRowIndex] = longVal;
                   break;
                case HIVE_TIMESTAMP_TYPE:
                   longVal = bb.getLong();
                   intVal = bb.getInt();
                   ((TimestampColumnVector)writerBatch.cols[colNo]).time[currRowIndex] = longVal;
                   ((TimestampColumnVector)writerBatch.cols[colNo]).nanos[currRowIndex] = intVal;
                   break;
                default:
                   bytesVal = new byte[length];
                   bb.get(bytesVal, 0, length);
                   throw new IOException("Unsupported column type " + colTypes[colNo] + " of column no " + colNo);
             }
          } // for colNo
          writerBatch.size++;
          currRowIndex++;
          if (currRowIndex == rowIndexStride) {
             writer.addRowBatch(writerBatch);          
             currRowIndex = 0;
             writerBatch.reset();
          }
          
       } // for numRows
       return;
    }
 
    
    OrcFileVectorWriter() 
    {
    }
}
