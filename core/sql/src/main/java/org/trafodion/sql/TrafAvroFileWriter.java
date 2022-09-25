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

import java.io.*;
import java.util.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ByteOrder;

import java.text.SimpleDateFormat;

import parquet.io.api.Binary;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.trafodion.sql.TrafConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import java.math.BigInteger;
import org.apache.hadoop.hbase.util.Bytes;
import java.sql.Timestamp;
import java.sql.Date;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.file.DataFileStream;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.log4j.MDC;

public class TrafAvroFileWriter extends TrafExtStorageUtils {

    static Logger logger = Logger.getLogger(TrafAvroFileWriter.class.getName());;
    static Configuration conf = null;
    
    // variable used to access avro file system.
    Schema schema      = null;
    File file          = null;
    GenericDatumWriter<GenericData.Record> datum = null;
    DataFileWriter<GenericData.Record> writer    = null;
    GenericData.Record record                    = null;
    Path filePath                   = null;
    FileStatus[] inputFileStatus;

    List<String> listOfColName;  
    int[] listOfColType;
    int[] listOfColLength;
    int[] listOfColPrecision;
    int[] listOfColScale;
    
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
                       int flags) throws IOException {

	if (logger.isDebugEnabled()) 
            logger.debug("Enter open()," 
                         + " file name: " + fileName
                         + " colNameList: " + colNameList
                         + " colTypeInfoList: " + colTypeInfoList
                         );

        setFlags(flags);

        listOfColName = new ArrayList<String>(colNameList.length);
        for (int i = 0; i < colNameList.length; i++) {
            listOfColName.add((String)colNameList[i]);
        }

        listOfColType = new int[colTypeInfoList.length];
        listOfColLength = new int[colTypeInfoList.length];
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
            listOfColLength[i]=length;
            listOfColPrecision[i]=precision;
            listOfColScale[i]=scale;
        }
        try
        {
        schema = generateAvroSchema(tableName,
                                    listOfColName,
                                    listOfColType,
                                    listOfColLength,
                                    listOfColPrecision,
                                    listOfColScale);
        } catch (Exception e)
        { throw new IOException(e); }

        filePath = new Path(fileName); 
        FSDataOutputStream dos = filePath.getFileSystem(conf).create(filePath);

        datum = new GenericDatumWriter<GenericData.Record>(schema);
        writer = new DataFileWriter<GenericData.Record>(datum);
        writer.create(schema, dos);

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

        GenericData.Record record = null; 
        ByteBuffer bb = (ByteBuffer)buffer;

        bb.order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < numRows; i++) {
            record = new GenericData.Record(schema);
            for (int j = 0; j < listOfColType.length; j++) {

                String name = listOfColName.get(j);
                int type = listOfColType[j];
                int scale = listOfColScale[j];
                
                short nullVal = bb.getShort();
                int length  = 0;
                if (nullVal == 0) {
                    length = bb.getInt();
                }
                
                if (nullVal != 0) { // null value
                    record.put(name, null);
                }
                else if ((nullVal == 0) && (length >= 0)) { // non-nullable val
                    byte[] colVal = new byte[length];
                    bb.get(colVal, 0, length);
                    
                    switch (type) {
                    case HIVE_BOOLEAN_TYPE: {
                        String s = Bytes.toString(colVal, 0, length);
                        boolean bv = (s.equals("TRUE") ? true : false);
                        record.put(name, bv);

                    }
                    break;
                        
                    case HIVE_BYTE_TYPE:
                    case HIVE_SHORT_TYPE:
                    case HIVE_INT_TYPE: {
                        int hi = 
                            (Integer.parseInt(Bytes.toString(colVal, 0, length)));
                        
                        record.put(name, hi);
                    }
                    break;

                    case HIVE_LONG_TYPE: {
                        long hl = 
                            (Long.parseLong(Bytes.toString(colVal, 0, length)));
                        
                        record.put(name, hl);
                    }
                    break;

                    case HIVE_FLOAT_TYPE: {
                        float hf =
                            Float.parseFloat(Bytes.toString(colVal, 0, length));
                        record.put(name, hf);
                    }
                    break;

                    case HIVE_DOUBLE_TYPE: {
                        double hd =
                            Double.parseDouble(Bytes.toString(colVal, 0, length));
                        record.put(name, hd);
                    }
                    break;

                    case HIVE_CHAR_TYPE:
                    case HIVE_VARCHAR_TYPE:
                    case HIVE_STRING_TYPE: {
                        String hs = Bytes.toString(colVal, 0, length);
                        record.put(name, hs);
                    }
                    break;

                    case HIVE_BINARY_TYPE: {

                        ByteBuffer byteBuf = ByteBuffer.wrap(colVal);
                        byteBuf.rewind();

                        record.put(name, byteBuf);
                    }
                    break;

                    case HIVE_DECIMAL_TYPE: {
                        String decStr = Bytes.toString(colVal, 0, length);
                        HiveDecimal hiveDec = HiveDecimal.create(decStr);
                        int hiveScale = hiveDec.scale();
                        BigInteger decBi = hiveDec.unscaledValue();
                        if (scale > hiveScale) {
                            BigInteger powBi = BigInteger.valueOf(10);
                            powBi = powBi.pow(scale-hiveScale);
                            decBi = decBi.multiply(powBi);
                        }

                        byte[] decByteArr = decBi.toByteArray();
                        ByteBuffer byteBuf = ByteBuffer.wrap(decByteArr);
                        byteBuf.rewind();

                        record.put(name, byteBuf);
                    }
                    break;

                    case HIVE_DATE_TYPE: {
                        Date d = Date.valueOf(Bytes.toString(colVal, 0, length));

                        int days = DateWritable.dateToDays(d);
                        record.put(name, days);
                    }
                    break;

                    case HIVE_TIMESTAMP_TYPE:
                        {
                            Timestamp ts = 
                                Timestamp.valueOf(Bytes.toString(colVal, 0, length));

                            // write timestamp value in microseconds
                            long tsVal = (ts.getTime()/1000)*1000000 
                                + (ts.getNanos());
                            record.put(name, tsVal);
                        }
                        break;

                    default:
                        {
                            if (true)
                                throw new IOException("TrafAvroFileWriter.insertRows: Unsupported datatype for insert");
                        }
                        break;
                        
                    } // switch
                    
                } // else non-nullable val

            } // for j

            writer.append(record);
        } // for numRows

        return null;
    }
    
    TrafAvroFileWriter() {
	if (logger.isTraceEnabled()) logger.trace("Enter TrafAvroFileWriter()");
    }

    public static void main(String[] args) throws IOException,
                                                InterruptedException,
                                                ClassNotFoundException {

  }
}



