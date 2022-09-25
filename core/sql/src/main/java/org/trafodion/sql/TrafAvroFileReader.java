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
import java.net.MalformedURLException;
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

import java.math.BigInteger;
import org.apache.hadoop.hbase.util.Bytes;
import java.sql.Timestamp;
import java.time.*;

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
import org.apache.avro.JsonProperties;
import org.codehaus.jackson.JsonNode;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.log4j.MDC;

public class TrafAvroFileReader extends TrafExtStorageUtils
{
    static Logger logger = Logger.getLogger(TrafAvroFileReader.class.getName());;
    static Configuration conf = null;

    // variable used to access avro file system.
    Schema schema      = null;
    File file          = null;
    GenericDatumReader<GenericData.Record> datum = null;
    DataFileReader<GenericData.Record> reader    = null;
    GenericData.Record record                    = null;
    Path filePath                   = null;
    FileStatus[] inputFileStatus;

    static {
	conf = TrafConfiguration.create(TrafConfiguration.HDFS_CONF);
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        MDC.put("PID", pid);
	System.setProperty("hostName", System.getenv("HOSTNAME"));
    }

    TrafAvroFileReader() throws IOException {
	if (logger.isTraceEnabled()) logger.trace("Enter TrafAvroFileReader()");
    }

    // tableName: name of avro table
    // fileName: path to the file that need to be read.
    // offset: not used for avro
    // pv_num_cols_to_project: number of columns to project and return
    // pv_which_cols: array containing index of columns that are to be
    //         returned. Index points to entries in col_name_vec. Index
    //         is 1-based.
    // length: not used for avro
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
                       int    flags) throws IOException {
 
	if (logger.isTraceEnabled()) logger.trace("Enter open()");

        setFlags(flags);

        setupBufferAllocVars(expected_row_size, max_rows_to_fill,
                             max_allocation_in_mb);
        
        setupColInfo(pv_num_cols_to_project, pv_which_cols, 
                     col_name_vec, col_type_vec);

        filePath = new Path(fileName);
        final FsInput fsi = new FsInput(filePath, conf);
        datum = new GenericDatumReader<GenericData.Record>();
        reader = new DataFileReader<GenericData.Record>(fsi, datum);
        record = new GenericData.Record(reader.getSchema());

        return null;
    }

    public byte[] fetchNextRow() throws IOException 
    {
        return null;
    }

    public ByteBuffer fetchNextBlock() throws IOException 
    {
	if (logger.isTraceEnabled()) logger.trace("Enter fetchNextBlock()");

	int lv_num_rows = 0;
	int lv_row_offset = 8; // Initial offset to store the number of orc rows
	int lv_filled_bytes = 0;
	boolean lv_done = false;

	allocateMemory();

        while ((lv_num_rows < m_max_rows_to_fill_in_block) &&
	       (m_allocation_size - lv_row_offset >= m_max_row_size_seen) &&
	       (!lv_done)) {
	    if (logger.isTraceEnabled()) 
                logger.trace("fetchNextBlock (in the loop):");

            if (! reader.hasNext())
                {
                    lv_done = true;
                    break;
                }

            reader.next(record);
	    lv_filled_bytes  = fillNextRow(m_block_bb,
					   lv_row_offset,
					   record);
	    
	    if (lv_filled_bytes > 0) {
		lv_row_offset += lv_filled_bytes;
		lv_num_rows++;
		if (lv_filled_bytes > m_max_row_size_seen) {
		    m_max_row_size_seen = lv_filled_bytes;
		}
	    }
	    else {
		lv_done = true;
	    }
	}
	
	if (logger.isTraceEnabled()) 
            logger.trace("fetchNextBlock (out of the loop):" );

	// Set the number of rows in the block header
	m_block_bb.putInt(0, lv_num_rows);
	m_block_bb.putInt(4, lv_row_offset);

	return m_block_bb;
    }

    private Schema avroColSch = null;
    private JsonNode avroJsonNode = null;
    private int avroColMaxLength = 0;
    private int avroColPrecision = 0;
    private int avroColScale = 0;
    void validateAvroColSchema(String colName, int colType)
        throws IOException 
    {
        Schema.Type neededType = null;
        switch (colType) {
        case HIVE_BOOLEAN_TYPE: 
            neededType = Schema.Type.BOOLEAN; 
            break;
        case HIVE_BYTE_TYPE:
        case HIVE_SHORT_TYPE:
        case HIVE_INT_TYPE:
            neededType = Schema.Type.INT;
            break;
        case HIVE_LONG_TYPE:
            neededType = Schema.Type.LONG;
            break;
        case HIVE_FLOAT_TYPE:
            neededType = Schema.Type.FLOAT;
            break;
        case HIVE_DOUBLE_TYPE:
            neededType = Schema.Type.DOUBLE;
            break;
        case HIVE_CHAR_TYPE:
        case HIVE_VARCHAR_TYPE:
        case HIVE_STRING_TYPE:
            neededType = Schema.Type.STRING;
            break;
        case HIVE_DECIMAL_TYPE:
            neededType = Schema.Type.BYTES;
            break;
        case HIVE_DATE_TYPE:
            neededType = Schema.Type.INT;
            break;
        case HIVE_TIMESTAMP_TYPE:
            neededType = Schema.Type.LONG;
            break;
        case HIVE_BINARY_TYPE:
            neededType = Schema.Type.BYTES;
            break;
        default:
            if (true)
                throw new IOException("TrafAvroFileReader.fillNextRow: datatype '" + colType + "' not yet supported");
            break;
        } // switch

        if (avroColSch.getType() != neededType)
            throw new IOException("Invalid type '" + avroColSch.getName() + "' for field '" + colName + "'. Expected type '" + neededType +"'.");

        String logPropVal = null;
        if ((colType == HIVE_DECIMAL_TYPE) ||
            (colType == HIVE_DATE_TYPE) ||
            (colType == HIVE_TIMESTAMP_TYPE)) {

            avroJsonNode = avroColSch.getJsonProp(AVRO_PROP_LOGICAL_TYPE);
            if ((avroJsonNode != null) && (avroJsonNode.isTextual())) {
                logPropVal = avroJsonNode.getTextValue();
            }
        }

        switch (colType) {
        case HIVE_DECIMAL_TYPE: {
            
            if (! (logPropVal.equals(AVRO_DECIMAL))) {
                throw new IOException("'logicalType' attribute must be specified for 'decimal' field '" + colName + "'");
            }

            avroJsonNode = avroColSch.getJsonProp(AVRO_PROP_PRECISION);
            if ((avroJsonNode == null) || (!avroJsonNode.isInt())) {
                throw new IOException("'precision' attribute must be specified for 'decimal' field '" + colName + "'");
            }
            avroColPrecision = avroJsonNode.getValueAsInt();
            
            avroColScale = 0;
            avroJsonNode = avroColSch.getJsonProp(AVRO_PROP_SCALE);
            if ((avroJsonNode != null) && (avroJsonNode.isInt())) {
                avroColScale = avroJsonNode.getValueAsInt();
            }
        } // HIVE_DECIMAL_TYPE
        break;

        case HIVE_DATE_TYPE: {
            if (! (logPropVal.equals(AVRO_DATE))) {
                throw new IOException("'logicalType' attribute value " + AVRO_DATE + " must be specified for " + colType + " field '" + colName + "'");
            }
        } // HIVE_DATE_TYPE
        break;

        case HIVE_TIMESTAMP_TYPE: {
            if (! (logPropVal.equals(AVRO_TIMESTAMP))) {
                throw new IOException("'logicalType' attribute value " + AVRO_TIMESTAMP + " must be specified for " + colType + " field '" + colName + "'");
            }
        } // HIVE_TIMESTAMP_TYPE
        break;

        case HIVE_CHAR_TYPE:
        case HIVE_VARCHAR_TYPE: {
            avroJsonNode = avroColSch.getJsonProp(AVRO_PROP_MAX_LENGTH);
            if ((avroJsonNode == null) || (!avroJsonNode.isInt())) {
                throw new IOException("'maxLength' attribute must be specified for 'char/varchar' field '" + colName + "'");
            }

            avroColMaxLength = avroJsonNode.getValueAsInt();
         }
        break;

        default:
        break;
        } // switch

        return;
    }

    // fills the row in the given ByteBuffer
    public int fillNextRow(ByteBuffer p_row_bb, 
			   int       p_offset,
                           GenericData.Record record
			   ) throws IOException 
    {
	if (logger.isTraceEnabled()) logger.trace("Enter fillNextRow(),"
						  + " offset: " 
						  + p_offset
						  + " length of array: " 
						  + (p_row_bb == null ? 0 : p_row_bb.capacity())
						  );

	Object lv_field_val = null;

	p_row_bb.position(p_offset);

	p_row_bb.putInt(p_offset);
	p_row_bb.putInt(m_col_count);

        //	p_row_bb.putLong(m_rr.getRowNumber());
	p_row_bb.putLong(1);

	if (logger.isTraceEnabled()) 
            logger.trace("Bytebuffer length1: " + p_row_bb.position());

        Schema recordSchema = record.getSchema();
        for (int i = 0; i < listOfReturnedColName.size(); i++)
            {
                int colType = listOfReturnedColType[i];
                String colName = listOfReturnedColName.get(i);
                
                Object avroVal = record.get(colName);
                if (avroVal == null) { 
                    // missing value, return null
                    p_row_bb.putInt(-1);
                    continue;
                }

                Field field = recordSchema.getField(colName);
                if (field == null) {
                    // missing col, return null.
                    // TBD: get and return default value
                    p_row_bb.putInt(-1);
                    continue;
                 }

                avroColSch = field.schema();
                if (avroColSch == null) {
                    throw new IOException("TrafAvroFileReader.fillNextRow: null schema for field " + colName);
                }

                // nullable types are union of col schema and null schema.
                // Get the col schema that corresponds to "avroVal".
                if ((avroColSch.getTypes().size() == 2) &&
                    (avroColSch.getType().equals(Type.UNION))) {
                    int tag = GenericData.get().resolveUnion(avroColSch, avroVal);
                    avroColSch = avroColSch.getTypes().get(tag);

                    if (avroColSch.getType().equals(Type.NULL)) {
                        // should not happen.
                        throw new IOException("TrafAvroFileReader.fillNextRow: invalid schema for field " + colName);
                    }
                }

                // validate column if it has not yet been validated.
                if (listOfFirstColRef[i] == true) {
                    validateAvroColSchema(colName, colType); 
                    listOfFirstColRef[i] = false;
                }

                switch (colType)
                    {
                    case HIVE_BOOLEAN_TYPE:
                        {
                            byte lv_byte = 
                                (byte)(avroVal.toString() == "true" ? 1 : 0);

                            p_row_bb.putInt(1);
                            p_row_bb.put(lv_byte);
                        }
                        break;

                    case HIVE_BYTE_TYPE:
                    case HIVE_SHORT_TYPE:
                        {
                            int lv_i = (Integer)avroVal;

                            p_row_bb.putInt(2);
                            p_row_bb.putShort((short)lv_i);
                        }
                        break;

                    case HIVE_INT_TYPE:
                        {
                            int lv_i = (Integer)avroVal;

                            p_row_bb.putInt(4);
                            p_row_bb.putInt(lv_i);
                        }
                        break;

                    case HIVE_LONG_TYPE:
                        {
                            long lv_l = (Long)avroVal;
                            
                            p_row_bb.putInt(8);
                            p_row_bb.putLong(lv_l);
                        }
                        break;

                   case HIVE_FLOAT_TYPE:
                        {
                            float lv_f = (Float)avroVal;

                            p_row_bb.putInt(4);
                            p_row_bb.putFloat(lv_f);
                        }
                        break;

                   case HIVE_DOUBLE_TYPE:
                        {
                            double lv_d = (Double)avroVal;

                            p_row_bb.putInt(8);
                            p_row_bb.putDouble(lv_d);
                        }
                        break;

                    case HIVE_STRING_TYPE:
                        {
                            String avroValStr = avroVal.toString();

                            p_row_bb.putInt(avroValStr.getBytes().length);
                            p_row_bb.put(avroValStr.getBytes());
                        }
                        break;

                    case HIVE_CHAR_TYPE:
                    case HIVE_VARCHAR_TYPE:
                        {
                            String avroValStr = avroVal.toString();

                            p_row_bb.putInt(avroValStr.getBytes().length);
                            p_row_bb.put(avroValStr.getBytes());
                        }
                        break;

                    case HIVE_DECIMAL_TYPE:
                        {
                            ByteBuffer lv_bb = (ByteBuffer)avroVal;
                            byte[] arr = 
                                Arrays.copyOf(lv_bb.array(), lv_bb.limit());
                            BigInteger bi = new BigInteger(arr);


                            avroJsonNode = avroColSch.getJsonProp(AVRO_PROP_PRECISION);
                            if ((avroJsonNode == null) || (!avroJsonNode.isInt())) {
                                throw new IOException("'precision' attribute must be specified for 'decimal' field '" + colName + "'");
                            }
                            avroColPrecision = avroJsonNode.getValueAsInt();
                            
                            avroColScale = 0;
                            avroJsonNode = avroColSch.getJsonProp(AVRO_PROP_SCALE);
                            if ((avroJsonNode != null) && (avroJsonNode.isInt())) {
                                avroColScale = avroJsonNode.getValueAsInt();
                            }
                            
                            if ((avroColPrecision >= 1) && (avroColPrecision <= 18))
                                {
                                    // return data as Int64
                                    long lv_l = bi.longValue();
                                    
                                    p_row_bb.putInt(2+2+8); 
                                    p_row_bb.putShort((short)avroColPrecision);
                                    p_row_bb.putShort((short)avroColScale);
                                    
                                    p_row_bb.putLong(lv_l);
                                }
                            else
                                {
                                    // return data as string
                                    String lv_string = bi.toString();
                                    
                                    // insert a decimal point for scale.
                                    lv_string = 
                                        new StringBuilder(lv_string)
                                        .insert(lv_string.length()-avroColScale,
                                                ".")
                                        .toString();
                                    
                                    // 2 bytes precision & scale, 8 bytes unscaled value
                                    p_row_bb.putInt(2+2+lv_string.getBytes().length); 
                                    
                                    p_row_bb.putShort((short)avroColPrecision);
                                    p_row_bb.putShort((short)avroColScale);
                                    
                                    p_row_bb.put(lv_string.getBytes());
                                    
                                }
                        }
                        break;

                    case HIVE_DATE_TYPE:
                        {
                            int lv_i = (Integer)avroVal;

                            Date date = new Date(DateWritable.daysToMillis(lv_i));
                            
                            Calendar c = Calendar.getInstance();
                            c.setTime(date);
                            
                            p_row_bb.putInt(4);
                            
                            // year is 2 bytes
                            p_row_bb.putShort((short)c.get(Calendar.YEAR));

                            // month is 1 byte.
                            // Calendar returns 0-based month. Add 1 to it.
                            p_row_bb.put((byte)(c.get(Calendar.MONTH)+1));

                            // day is 1 byte
                            p_row_bb.put((byte)c.get(Calendar.DATE));
                        }
                        break;

                    case HIVE_TIMESTAMP_TYPE: 
                        {
                            long lv_l = (Long)avroVal;

                            int[] dtFields = getDatetimeFields(lv_l);

                            p_row_bb.putInt(11);

                            // year
                            p_row_bb.putShort((short)dtFields[0]); 
                            
                            // month
                            p_row_bb.put((byte)dtFields[1]);

                            // day
                            p_row_bb.put((byte)dtFields[2]);

                            // hour
                            p_row_bb.put((byte)dtFields[3]);

                            // min
                            p_row_bb.put((byte)dtFields[4]);

                            // second
                            p_row_bb.put((byte)dtFields[5]);
                            
                            // microsec
                            p_row_bb.putInt(dtFields[6]);
                        }
                        break;

                    case HIVE_BINARY_TYPE:
                        {
                            ByteBuffer lv_bb = (ByteBuffer)avroVal;
                            byte[] arr = 
                                Arrays.copyOf(lv_bb.array(), lv_bb.limit());
                            p_row_bb.putInt(arr.length);
                            p_row_bb.put(arr);
                         }
                        break;

                    default:
                        {
                            if (true)
                                throw new IOException("TrafAvroFileReader.fillNextRow: datatype '" + colType + "' not yet supported");
                        }
                        break;

                    } // switch
            } // for

        int lv_filled_bytes = p_row_bb.position() - p_offset;
	p_row_bb.putInt(p_offset, lv_filled_bytes - 16);

	return (lv_filled_bytes);
    }
	
    public String close() throws IOException {

	if (logger.isTraceEnabled()) logger.trace("Enter close()");
        
        if (reader != null) {
            reader.close();
            reader = null;
        }

	return null;
    }
    
    private long getRowCount() throws IOException, URISyntaxException
    {
        inputFileStatus = filePath.getFileSystem(conf).listStatus(filePath);

        DataFileReader<Object> reader1 = null;
        GenericDatumReader<Object> datum1 = new GenericDatumReader<Object>();

        long rowCount = 0L;
        long numBlocks = 0L;
            
        for (FileStatus fs : inputFileStatus){
            Path fsPath = fs.getPath();
            final FsInput fsi = new FsInput(filePath, conf);
            reader1 = new DataFileReader<Object>(fsi, datum1);

            while (reader1.hasNext()) {
                reader1.nextBlock();
                rowCount += reader1.getBlockCount();
                numBlocks++;
            }

            reader1.close();
        } // for

        return rowCount;
    }

    public Object[] getColStats(int colNum, String colName, int colType) 
        throws IOException, URISyntaxException
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
        
        return null;
     }

    public byte[][]  getFileStats(String rootDir) 
        throws IOException {

	if (logger.isTraceEnabled()) logger.trace("Enter getFileStats");

        List<byte[]> fileStatsList = new LinkedList<byte[]>();

        Path rootDirPath = new Path(rootDir);
        FileStatus[] ifs = 
            rootDirPath.getFileSystem(conf).listStatus(rootDirPath);

        DataFileReader<Object> reader1 = null;
        GenericDatumReader<Object> datum1 = new GenericDatumReader<Object>();

        int fileNum = 1;

        for (FileStatus fs : ifs){
               
            long rowCount = 0L;
            long numBlocks = 0L;
    
            Path fsPath = fs.getPath();
            final FsInput fsi = new FsInput(fsPath, conf);
            reader1 = new DataFileReader<Object>(fsi, datum1);
            while (reader1.hasNext()) {
                reader1.nextBlock();
                rowCount += reader1.getBlockCount();
                numBlocks++;

                // getBlockSize available in avro 1.8
                // long bs = reader1.getBlockSize();
            }

            reader1.close();

            String filePath     = fsPath.getParent().toUri().getPath();
            String fileName     = fsPath.getName();
            long fileSize       = fs.getLen();
            long fileMaxSize    = fs.getBlockSize();

            // millisecs since epoch
            long modTime        = fs.getModificationTime(); 
            Instant instant = Instant.ofEpochMilli(modTime);
            String modTimeStr = instant.toString();
            //   Instant has format: yyyy-mm-ddThh:mm:ss.mmmZ
            // Replace 'T' and 'Z' with blanks.
            modTimeStr = modTimeStr.replace('Z', ' ');
            modTimeStr = modTimeStr.replace('T', ' ');

            String oneBlock;
            oneBlock  = filePath + "|";
            oneBlock += fileName + "|";
            oneBlock += String.valueOf(fileNum++) + "|";
            oneBlock += String.valueOf(fileSize) + "|";
            oneBlock += String.valueOf(fileMaxSize) + "|";
            oneBlock += String.valueOf(numBlocks) + "|";
            oneBlock += String.valueOf(rowCount) + "|";
            oneBlock += modTimeStr + "|";
            
            fileStatsList.add(oneBlock.getBytes());
        } // for fs

        byte[][] fileStatsInfo = null;
        
        if (fileStatsList.size() > 0) {
            fileStatsInfo = new byte[fileStatsList.size()][];
            for (int i = 0; i < fileStatsList.size(); i++) {
                fileStatsInfo[i] = fileStatsList.get(i);
            }
        }
        else {
            fileStatsInfo = new byte[0][];
        }

        return fileStatsInfo;
    }

    public String[] getFileSchema(String tableName,
                                  String rootDir,
                                  Object[] col_name_vec,
                                  Object[] col_type_vec,
                                  int flags)
        throws IOException {
        
	if (logger.isTraceEnabled()) logger.trace("Enter getFileSchema");

        setFlags(flags);

        DataFileReader<Object> reader1 = null;
        GenericDatumReader<Object> datum1 = new GenericDatumReader<Object>();

        Path fp = new Path(rootDir);
        FileStatus[] ifs = fp.getFileSystem(conf).listStatus(fp);
        
        String readSchema = null;
        String writeSchema = null;

        // get read schema
        for (FileStatus fs : ifs){
            
            Path fsPath = fs.getPath();
            final FsInput fsi = new FsInput(fsPath, conf);
            reader1 = new DataFileReader<Object>(fsi, datum1);
            
            if ((reader1 != null) &&
                (reader1.hasNext())) {
                GenericData.Record record1 = 
                    new GenericData.Record(reader1.getSchema());
                
                reader1.next(record1);
                Schema recordSchema1 = record1.getSchema();
                
                if (recordSchema1 != null) {
                    String schStr = recordSchema1.toString(true);
                    readSchema = schStr.replace("\\u0000", "");
                }
            }

            reader1.close();
            break;
        } // for

        // get write schema
        if (col_name_vec != null) {
            setupColInfo(-1, null, col_name_vec, col_type_vec);
            try {
            Schema schema = generateAvroSchema(tableName,
                                               listOfReturnedColName,
                                               listOfReturnedColType,
                                               listOfReturnedColLen,
                                               listOfReturnedColPrec,
                                               listOfReturnedColScale);
            if (schema != null) {
                writeSchema = schema.toString(true);
            }
            } catch (Exception e)
            {
               throw new IOException(e);
            }
        }
        
        String[] schemas = new String[2];
        schemas[0] = readSchema;
        schemas[1] = writeSchema;
        
        return schemas;
    }
   
    public static void main(String[] args) throws IOException,

                                                  InterruptedException,
                                                  ClassNotFoundException {
        
    }
    
}
