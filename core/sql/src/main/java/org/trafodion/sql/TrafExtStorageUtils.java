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
import java.nio.*;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.io.api.Binary;
import java.math.BigInteger;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.schema.*;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.*;
import org.apache.avro.Schema.*;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import org.trafodion.sql.NanoTimeUtils;

import java.net.MalformedURLException; 
import java.io.UnsupportedEncodingException; 

public class TrafExtStorageUtils {
    // this set of constants MUST be kept in sync with 
    // enum HiveProtoTypeKind in common/ComSmallDefs.h 
    public static final int HIVE_UNKNOWN_TYPE   = 0;
    public static final int HIVE_BOOLEAN_TYPE   = 1;
    public static final int HIVE_BYTE_TYPE      = 2;
    public static final int HIVE_SHORT_TYPE     = 3;
    public static final int HIVE_INT_TYPE       = 4;
    public static final int HIVE_LONG_TYPE      = 5;
    public static final int HIVE_FLOAT_TYPE     = 6;
    public static final int HIVE_DOUBLE_TYPE    = 7;
    public static final int HIVE_DECIMAL_TYPE   = 8;
    public static final int HIVE_CHAR_TYPE      = 9;
    public static final int HIVE_VARCHAR_TYPE   = 10;
    public static final int HIVE_STRING_TYPE    = 11;
    public static final int HIVE_BINARY_TYPE    = 12;
    public static final int HIVE_DATE_TYPE      = 13;
    public static final int HIVE_TIMESTAMP_TYPE = 14;
    public static final int HIVE_ARRAY_TYPE     = 15;
    public static final int HIVE_STRUCT_TYPE    = 16;

    // this set of constants MUST be kept in sync with 
    // enum OrcPushdownOperatorType in common/ComSmallDefs.h 
    public static final int UNKNOWN_OPER = 0;
    public static final int STARTAND = 1;
    public static final int STARTOR = 2;
    public static final int STARTNOT = 3;
    public static final int END = 4;
    public static final int EQUALS = 5;
    public static final int LESSTHAN = 6;
    public static final int LESSTHANEQUALS = 7;
    public static final int ISNULL = 8;
    public static final int IN = 9;
    public static final int BF = 10; // bloom filter

    // Gregorian Calendar adopted Oct. 15, 1582 (2299161)
    private static final int JGREG= 15 + 31*(10+12*1582);
    private static final double HALFSECOND = 0.5;

    // whether to skip timezone conversion
    public static boolean sv_parquet_timestamp_skip_conversion = false;

    static {

	/* 
	   The following code takes care of initializing log4j for the org.trafodion.sql package 
	   (in case of an ESP, e.g.) when the class:org.trafodion.sql.HBaseClient (which initializes log4j 
           for the org.trafodion.sql package) hasn't been loaded.
	*/
	System.setProperty("hostName", System.getenv("HOSTNAME"));
    	String confFile = System.getProperty("trafodion.log4j.configFile");
    	if (confFile == null) {
    		System.setProperty("trafodion.sql.log", System.getenv("TRAF_LOG") + "/trafodion.sql.java.${hostName}.log");
    		confFile = System.getenv("TRAF_CONF") + "/log4j.sql.config";
    	}

    	PropertyConfigurator.configure(confFile);

    }

    static Logger logger = Logger.getLogger(TrafExtStorageUtils.class.getName());;

    static {
	
	Configuration my_conf = TrafConfiguration.create(TrafConfiguration.HBASE_CONF);
        sv_parquet_timestamp_skip_conversion = my_conf.getBoolean("org.trafodion.sql.parquet.timestamp.skip.conversion", 
                                                               sv_parquet_timestamp_skip_conversion);
        if (logger.isDebugEnabled()) 
	    logger.debug("TrafExtStorageUtils"
			 + ", org.trafodion.sql.parquet.timestamp.skip.conversion: " + sv_parquet_timestamp_skip_conversion
			 );
    }

    public static int[] fromJulian(double injulian) {
        int jalpha,ja,jb,jc,jd,je,year,month,day;
        
        double julian = injulian + HALFSECOND / 86400.0;
        ja = (int) julian;
        if (ja>= JGREG) {
            jalpha = (int) (((ja - 1867216) - 0.25) / 36524.25);
            ja = ja + 1 + jalpha - jalpha / 4;
        }
        
        jb = ja + 1524;
        jc = (int) (6680.0 + ((jb - 2439870) - 122.1) / 365.25);
        jd = 365 * jc + jc / 4;
        je = (int) ((jb - jd) / 30.6001);
        day = jb - jd - (int) (30.6001 * je);
        month = je - 1;
        if (month > 12) month = month - 12;
        year = jc - 4715;
        if (month > 2) year--;
        if (year <= 0) year--;
        
        return new int[] {year, month, day};
    }

    public static int[] getDatetimeFields(Binary pv_binary_ts) {

        NanoTime nt = NanoTime.fromBinary(pv_binary_ts);
	java.sql.Timestamp ts = NanoTimeUtils.getTimestamp(nt,
							   sv_parquet_timestamp_skip_conversion// default is false: do not skip GMT conversion
							   );

	if (logger.isTraceEnabled()) 
	    logger.trace("getDateTimeFields,"
			 + ts.getYear() 
			 + " " + ts.getMonth()
			 + " " + ts.getDate() 
			 + " " + ts.getHours()
			 + " " + ts.getMinutes() 
			 + " " + ts.getSeconds()
			 + " " + ts.getNanos()
			 );

        return new int[] {ts.getYear() + 1900, 
			  ts.getMonth() + 1, 
			  ts.getDate(), 
			  ts.getHours(), 
			  ts.getMinutes(), 
			  ts.getSeconds(), 
			  ts.getNanos()};
    }

    public static int[] getDatetimeFields(parquet.io.api.Binary ts) {
        
        org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime nt = org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime.fromBinary(ts);
        int jd = nt.getJulianDay();
        int[] ymd = fromJulian(jd);
        long ns = nt.getTimeOfDayNanos();
        ns = ns / 1000;
        
        // micro secs
        int ms = (int)(ns % 1000000);
        ns = ns / 1000000;
        
        // second
        int sec = (int)(ns % 60);
        ns = ns / 60;
        
        // min
        int min = (int)(ns % 60);
        ns = ns / 60;
        
        // hour
        int hour = (int)(ns % 24);
        ns = ns / 24;
        
        return new int[] {ymd[0], ymd[1], ymd[2], hour, min, sec, ms};
    }

    // input ts is time in microseconds since epoch.
    // could be +ve or -ve.
    public int[] getDatetimeFields(long ts) {
        
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(ts/1000);
        int year = c.get(Calendar.YEAR);
        int month = c.get(Calendar.MONTH) + 1;
        int day = c.get(Calendar.DATE);
        int hour = c.get(Calendar.HOUR);
        int min = c.get(Calendar.MINUTE);
        int second = c.get(Calendar.SECOND);

        // get microsecs from "ts"
        int micros = (int)(Math.abs(ts) - (Math.abs(ts)/1000000)*1000000);
        return new int[] {year, month, day, hour, min, second, micros};
    }

    // input ts is time in microseconds
    public String getFormattedDatetime(long ts) {
        int dtFields[] = getDatetimeFields(ts);
        
        String fd = dtFields[0] + "-" + dtFields[1] + "-" + dtFields[2];

        return fd;
    }

    public double toJulian(int[] ymd) {
        int year=ymd[0];
        int month=ymd[1]; // jan=1, feb=2,...
        int day=ymd[2];
        int julianYear = year;
        if (year < 0) julianYear++;
        int julianMonth = month;
        if (month > 2) {
            julianMonth++;
        }
        else {
            julianYear--;
            julianMonth += 13;
        }
        
        double julian = (java.lang.Math.floor(365.25 * julianYear)
                         + java.lang.Math.floor(30.6001*julianMonth) + day + 1720995.0);
        if (day + 31 * (month + 12 * year) >= JGREG) {
            // change over to Gregorian calendar
            int ja = (int)(0.01 * julianYear);
            julian += 2 - ja + (0.25 * ja);
        }
        return java.lang.Math.floor(julian);
    }  

    public int computeMinBytesForPrecision(int precision) {

        int numBytes = 1;
        while (java.lang.Math.pow(2.0, 8 * numBytes - 1) < java.lang.Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    // variable used for buffer/memory allocation and usage
    private boolean             m_memory_allocated = false;
    public ByteOrder            m_byteorder = ByteOrder.LITTLE_ENDIAN;
    public int                  m_max_row_size_seen = 0;
    private int                 m_max_allocation_in_mb = 0;
    public ByteBuffer           m_block_bb;
    public ByteBuffer           m_block_bb1;
    public ByteBuffer           m_block_bb2;
    public int                  m_which_buffer = 0;
    public int                  m_allocation_size = 0;
    public int                  m_max_rows_to_fill_in_block = 0;

    void setupBufferAllocVars(
                              int    expected_row_size,
                              int    max_rows_to_fill,
                              int    max_allocation_in_mb
                              )
    {
	m_max_row_size_seen = expected_row_size;
	m_max_rows_to_fill_in_block = max_rows_to_fill;
	m_max_allocation_in_mb = max_allocation_in_mb;

        //System.out.println("m_max_allocation_in_mb = " + m_max_allocation_in_mb);
        //System.out.println("m_max_rows_to_fill_in_block = " + m_max_rows_to_fill_in_block);
        //System.out.println("m_max_row_size_seen = " + m_max_row_size_seen);
        //System.out.println(" ");
    }

    // list of name/datatype of all columns that will be used. Includes
    // returned columns and columns used in filter predicates. 
    public ArrayList<String>  listOfAllColName     = null;  
    public ArrayList<Integer> listOfAllColType     = null;  

    // list of name/datatype info for columns that will be fetched from avro
    // and returned to traf.
    public ArrayList<String> listOfReturnedColName  = null;  

    public int[] listOfReturnedColType      ;  
    public int m_returned_col_type_length = 0;
    public int[] listOfReturnedColLen;
    public int[] listOfReturnedColPrec;
    public int[] listOfReturnedColScale;

    // set to true if the list of columns has a composite(struct,array) column.
    // Currently used to skip some of the optimizations until support for
    // composite cols is added at that layer.
    // See file TrafParquetFileReader.java, method open().
    public boolean hasCompositeCol = false;

    // for parquet: to store precision, scale as obtained from the parquet file meta
    // (filled lazily at the point of reading the column value)
    public int[] listOfFileColPrec;
    public int[] listOfFileColScale;

    public boolean[] listOfFirstColRef;
    int     m_col_count;
    private boolean m_include_cols[] = null;

    void setupColInfo(
                      int    pv_num_cols_to_project,
                      int[]  pv_which_cols,
                      Object[] col_name_vec,
                      Object[] col_type_vec) throws IOException {
	m_col_count = pv_num_cols_to_project;

	int lv_num_cols_in_table = 0;
        if (col_type_vec != null)
            lv_num_cols_in_table = col_type_vec.length;

        // Set up m_include_cols array to indicate the columns to be
        // retrieved.
        m_include_cols = new boolean[lv_num_cols_in_table + 1];
        boolean lv_include_col = false;
        if (pv_num_cols_to_project == -1) {
            lv_include_col = true;
            m_col_count = lv_num_cols_in_table;
        }
        
	// Initialize m_include_cols
	for (int i = 0; i < lv_num_cols_in_table+1; i++) {
	    m_include_cols[i] = lv_include_col;
	}
        
	// Set m_include_cols as per the passed in parameters
	if ((pv_num_cols_to_project > 0) &&
	    (pv_which_cols != null)) {
	    for (int lv_curr_index : pv_which_cols) {
		if ((lv_curr_index >= 1) &&
		    (lv_curr_index <= lv_num_cols_in_table)) {
		    m_include_cols[lv_curr_index] = true;
		}
	    }
	}

        String message = null;
        if (lv_num_cols_in_table > 0) {

            if (col_name_vec.length != col_type_vec.length) {
                throw new IOException("TrafAvroFileReader.setupColInfo: col_name_vec.length != col_type_vec.length");
            }

            listOfAllColName = new ArrayList<String>(col_name_vec.length);
            listOfReturnedColName = new ArrayList<String>(col_name_vec.length);
            for (int i = 0; i < col_name_vec.length; i++) {

                listOfAllColName.add((String)col_name_vec[i]);

                // skip if this col is not to be returned
                if (! m_include_cols[i+1]) {
                    continue;
                }

                listOfReturnedColName.add((String)col_name_vec[i]);
            }
            
            listOfAllColType      = new ArrayList<Integer>(col_type_vec.length);

            listOfReturnedColType = new int[col_type_vec.length];
            listOfReturnedColLen  = new int[col_type_vec.length];
            listOfReturnedColPrec = new int[col_type_vec.length];
            listOfReturnedColScale= new int[col_type_vec.length];
            listOfFileColPrec     = new int[col_type_vec.length];
            listOfFileColScale    = new int[col_type_vec.length];
            listOfFirstColRef     = new boolean[col_type_vec.length];
	    m_returned_col_type_length = 0;
            for (int i = 0; i < col_type_vec.length; i++) {
                ByteBuffer bb = ByteBuffer.wrap((byte[])col_type_vec[i]);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                
                int type      = bb.getInt();
                int len       = bb.getInt();
                int precision = bb.getInt();
                int scale = bb.getInt();
                
                listOfAllColType.add(type);

                if ((type == HIVE_STRUCT_TYPE) ||
                    (type == HIVE_ARRAY_TYPE))
                    hasCompositeCol = true;

                if (! m_include_cols[i+1]) 
                    continue;

                listOfReturnedColType[m_returned_col_type_length]=type;
                listOfReturnedColLen[m_returned_col_type_length]=len;
                listOfReturnedColPrec[m_returned_col_type_length]=precision;
                listOfReturnedColScale[m_returned_col_type_length]=scale;

		// initialize it to -1 and set it the first time when data is read from the file
                listOfFileColPrec[m_returned_col_type_length]=-1;
                listOfFileColScale[m_returned_col_type_length]=-1;

                listOfFirstColRef[m_returned_col_type_length++]=true;
            }
        }
        
    }

    void allocateMemory() 
    {
	if (m_memory_allocated) {

            m_block_bb.clear();
            m_block_bb.order(m_byteorder);

	    return;
	}
        
	long lv_desired_allocation_size = 
            m_max_rows_to_fill_in_block * m_max_row_size_seen + 8;

	if (logger.isDebugEnabled()) logger.debug("allocateMemory(before)"
						  + ", max rows to fill in block: " + m_max_rows_to_fill_in_block
						  + ", max row size seen: " + m_max_row_size_seen
						  + ", desired allocation size: " + lv_desired_allocation_size
						  );

	while ((m_max_rows_to_fill_in_block > 1) && 
               (lv_desired_allocation_size > m_max_allocation_in_mb * 1024 * 1024)) {
            m_max_rows_to_fill_in_block /= 2;
            lv_desired_allocation_size = 
                m_max_rows_to_fill_in_block * m_max_row_size_seen + 8;
	}

	if (lv_desired_allocation_size <= 0) {
	    lv_desired_allocation_size = m_max_allocation_in_mb * 1024 * 1024;
	}

	if (logger.isDebugEnabled()) logger.debug("allocateMemory(after)"
						  + ", max rows to fill in block: " + m_max_rows_to_fill_in_block
						  + ", max row size seen: " + m_max_row_size_seen
						  + ", desired allocation size: " + lv_desired_allocation_size
						  );

	m_allocation_size = (int)lv_desired_allocation_size;
        
	m_block_bb = ByteBuffer.allocateDirect(m_allocation_size);
        
	m_memory_allocated = true;

	m_block_bb.clear();
	m_block_bb.order(m_byteorder);
    }

    void allocateMemoryDoubleBuffer() 
    {
	if (m_memory_allocated) {

	    if (m_which_buffer == 0) {
		m_block_bb = m_block_bb1;
		m_which_buffer = 1;
	    }
	    else {
		m_block_bb = m_block_bb2;
		m_which_buffer = 0;
	    }

            m_block_bb.clear();
            m_block_bb.order(m_byteorder);

	    return;
	}
        
	long lv_desired_allocation_size = 
            m_max_rows_to_fill_in_block * m_max_row_size_seen + 8;

	if (logger.isDebugEnabled()) logger.debug("allocateMemory(before)"
						  + ", max rows to fill in block: " + m_max_rows_to_fill_in_block
						  + ", max row size seen: " + m_max_row_size_seen
						  + ", desired allocation size: " + lv_desired_allocation_size
						  );

	while ((m_max_rows_to_fill_in_block > 1) && 
               (lv_desired_allocation_size > m_max_allocation_in_mb * 1024 * 1024)) {
            m_max_rows_to_fill_in_block /= 2;
            lv_desired_allocation_size = 
                m_max_rows_to_fill_in_block * m_max_row_size_seen + 8;
	}

	if (logger.isDebugEnabled()) logger.debug("allocateMemory(after)"
						  + ", max rows to fill in block: " + m_max_rows_to_fill_in_block
						  + ", max row size seen: " + m_max_row_size_seen
						  + ", desired allocation size: " + lv_desired_allocation_size
						  );

	m_allocation_size = (int)lv_desired_allocation_size;
        
	m_block_bb1 = ByteBuffer.allocateDirect(m_allocation_size);
	m_block_bb2 = ByteBuffer.allocateDirect(m_allocation_size);
        
	m_memory_allocated = true;
	
	allocateMemoryDoubleBuffer();
	
    }

    public MessageType generateParquetSchema(String parqSchStr)
        throws IOException
    {
        MessageType schema = null;
        if (parqSchStr != null)
            schema = MessageTypeParser.parseMessageType(parqSchStr);
        return schema;
    }

    public MessageType generateParquetSchema(String tableName,
                                             List<String> listOfColName,
                                             int[] listOfColType,
                                             int[] listOfColPrecision,
                                             int[] listOfColScale)
        throws IOException
    {
        if (listOfColName == null)
            return null;

        String message = new String();
	String msg_epilogue = null;

        message = " message "+ tableName + " { ";
        for (int i = 0; i < listOfColName.size(); i++) {
            int type = listOfColType[i];
            int precision = listOfColPrecision[i];
            int scale = listOfColScale[i];

            message += " optional ";
	    msg_epilogue = null;

            switch (type)
                {
                case HIVE_BOOLEAN_TYPE:
                    {
                        message += " boolean ";
                    }
                    break;

                case HIVE_BYTE_TYPE:
                    {
                        message += " int32 ";
                    }
                    break;

                case HIVE_SHORT_TYPE:
                    {
                        message += " int32 ";
                    }
                    break;

                case HIVE_INT_TYPE:
                    {
                        message += " int32 ";
                    }
                    break;

                case HIVE_LONG_TYPE:
                    {
                        message += " int64 ";
                    }
                    break;

                case HIVE_FLOAT_TYPE:
                    {
                        message += " float ";
                    }
                    break;

                case HIVE_DOUBLE_TYPE:
                    {
                        message += " double ";
                    }
                    break;

                case HIVE_CHAR_TYPE:
                case HIVE_VARCHAR_TYPE:
                case HIVE_STRING_TYPE:
                case HIVE_BINARY_TYPE:
                    {
                        message += " binary ";
			msg_epilogue = " (UTF8)";
                    }
                    break;

                case HIVE_DECIMAL_TYPE:
                    {
                        int numBytes = computeMinBytesForPrecision(precision);
			
			if (logger.isDebugEnabled()) logger.debug("Hive decimal type"
								 + ", colName: " + listOfColName.get(i)
								 + ", type:"  + type
								 + ", precision: " + precision
								 + ", scale: " + scale
								 + ", numBytes: " + numBytes
								 );

                        message += " fixed_len_byte_array(" + numBytes + ") ";
			msg_epilogue = " (DECIMAL(" + precision + "," + scale + "))";
                    }
                    break;
                    
                case HIVE_TIMESTAMP_TYPE:
                    {
                        if (isParquetLegacyTS())
                            message += " int96 ";
                        else
                            message += " int64 ";
                    }
                    break;

                case HIVE_ARRAY_TYPE:
                case HIVE_STRUCT_TYPE:
                default:
                    {
                        if (true)
                            throw new IOException("generateParquetSchema: datatype '" + type + "' not yet supported");
                    }
                    break;

                } // switch

            message += listOfColName.get(i);
            if (msg_epilogue != null) {
                message += msg_epilogue;
            }
            message += "; ";

        } // for

        message += " } ";


        MessageType schema = generateParquetSchema(message);
        //MessageType schema              = null;
        //schema = MessageTypeParser.parseMessageType(message);
        return schema;
    }

    public static final String AVRO_PROP_LOGICAL_TYPE = "logicalType";
    public static final String AVRO_PROP_MAX_LENGTH   = "maxLength";
    public static final String AVRO_PROP_PRECISION    = "precision";
    public static final String AVRO_PROP_SCALE        = "scale";
    public static final String AVRO_CHAR              = "char";
    public static final String AVRO_VARCHAR           = "varchar";
    public static final String AVRO_DATE              = "date";
    public static final String AVRO_DECIMAL           = "decimal";
    public static final String AVRO_TIMESTAMP         = "timestamp-micros";

    public Schema generateAvroSchema(String tableName,
                                     List<String> listOfColName,
                                     int[] listOfColType,
                                     int[] listOfColLength,
                                     int[] listOfColPrecision,
                                     int[] listOfColScale) 
        throws Exception {
        List<Schema.Field> fields = new ArrayList<Field>();
        final ObjectMapper mapper = new ObjectMapper();
        JsonNode jn = null;
        Schema nullSchema = Schema.create(Schema.Type.NULL);
        for (int i = 0; i < listOfColName.size(); i++) {

            int type = listOfColType[i];
            int length = listOfColLength[i];
            int precision = listOfColPrecision[i];
            int scale = listOfColScale[i];

            String colName = listOfColName.get(i);
            Schema colSchema = null;
            switch (type)
                {
                case HIVE_BOOLEAN_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.BOOLEAN);
                    }
                    break;

                case HIVE_BYTE_TYPE:
                case HIVE_SHORT_TYPE:
                case HIVE_INT_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.INT);
                    }
                    break;

                case HIVE_LONG_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.LONG);
                    }
                    break;

                case HIVE_FLOAT_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.FLOAT);
                    }
                    break;

                case HIVE_DOUBLE_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.DOUBLE);
                    }
                    break;

                case HIVE_CHAR_TYPE:
                case HIVE_VARCHAR_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.STRING);
                        colSchema.addProp(AVRO_PROP_LOGICAL_TYPE, AVRO_CHAR);

                        jn = mapper.convertValue(length/4, JsonNode.class);
                        colSchema.addProp(AVRO_PROP_MAX_LENGTH, jn);
                    }
                    break;

                case HIVE_STRING_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.STRING);
                    }
                    break;

                case HIVE_BINARY_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.BYTES);
                    }
                    break;

                case HIVE_DECIMAL_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.BYTES);
                        colSchema.addProp(AVRO_PROP_LOGICAL_TYPE, AVRO_DECIMAL);

                        jn = mapper.convertValue(precision, JsonNode.class);
                        colSchema.addProp(AVRO_PROP_PRECISION, jn);

                        jn = mapper.convertValue(scale, JsonNode.class);
                        colSchema.addProp(AVRO_PROP_SCALE, jn);
                    }
                    break;
                    
                case HIVE_DATE_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.INT);
                        colSchema.addProp(AVRO_PROP_LOGICAL_TYPE, AVRO_DATE);
                    }
                    break;

                case HIVE_TIMESTAMP_TYPE:
                    {
                        colSchema = Schema.create(Schema.Type.LONG);
                        colSchema.addProp(AVRO_PROP_LOGICAL_TYPE, AVRO_TIMESTAMP);
                    }
                    break;

                default:
                    {
                        if (true)
                            throw new IOException("generateAvroSchema: datatype '" + type + "' not yet supported");
                    }
                    break;

                } // switch

            List<Schema> ls = new ArrayList<Schema>();
            ls.add(nullSchema);
            ls.add(colSchema);
            Schema unionSch = Schema.createUnion(ls);

            Schema.Field field = new Schema.Field(colName, unionSch, null, null);

            fields.add(field);
        } // for

        Schema schema = Schema.createRecord(tableName, null, null, false);
        if (schema != null)
            schema.setFields(fields);

        return(schema);
    }

    // this set of flags constants must remain in sync with
    // enum ExtStorageAccessFlags defined in common/ComSmallDefs.h
    private final int PARQUET_LEGACY_TS           = 0x0001;
    private final int PARQUET_ENABLE_DICTIONARY   = 0x0002;
    private final int PARQUET_SIGNED_MIN_MAX      = 0x0004;

    private int myFlags = 0;

    public void setFlags(int inFlags) {
        myFlags = inFlags;
    }

    private boolean isFlagBitSet(int flagbit) {
        return isFlagBitSet(myFlags, flagbit);
    }

    static private boolean isFlagBitSet(int flags, int flagbit) {
        return ((flags & flagbit) != 0);
    }

    public boolean isParquetLegacyTS() {
        return isFlagBitSet(PARQUET_LEGACY_TS);
    }

    public boolean isParquetEnableDictionary() {
        return isFlagBitSet(PARQUET_ENABLE_DICTIONARY);
    }

    public boolean isParquetSignedMinMax() {
        return isFlagBitSet(PARQUET_SIGNED_MIN_MAX);
    }

    // flags passed in to various methods.
    // These flag bits need to remain in sync with flags defined
    // in file executor/HBaseClient_JNI.h.
    private static final int SYNC_REPL          = 0x0001;
    private static final int INCR_BACKUP        = 0x0002;
    private static final int ASYNC_OPER         = 0x0004;
    private static final int USE_REGION_XN      = 0x0008;
    private static final int USE_TREX           = 0x0010;
    private static final int NO_CONFLICT_CHECK  = 0x0020;
    private static final int PUT_IS_UPSERT   = 0x0040;

    static public boolean useTRex(int flags) {
        return isFlagBitSet(flags, USE_TREX);
    }

    static public boolean syncRepl(int flags) {
        return isFlagBitSet(flags, SYNC_REPL);
    }

    static public boolean incrementalBackup(int flags) {
        return isFlagBitSet(flags, INCR_BACKUP);
    }

    static public boolean useRegionXn(int flags) {
        return isFlagBitSet(flags, USE_REGION_XN);
    }

    static public boolean asyncOper(int flags) {
        return isFlagBitSet(flags, ASYNC_OPER);
    }

    static public boolean noConflictCheck(int flags) {
        return isFlagBitSet(flags, NO_CONFLICT_CHECK);
    }

    static public boolean isUpsert(int flags) {
        return isFlagBitSet(flags, PUT_IS_UPSERT);
    }

    static public int setFlagBit(int flags, int flagbit) {
        flags |= flagbit;
        return flags;
    }

    static public int setUseTRex(int flags, boolean v) {
        if (v)
            flags = setFlagBit(flags, USE_TREX);
        else
            flags &= ~USE_TREX;

        return flags;
    }

    static public int setAsyncOper(int flags, boolean v) {
        if (v)
            flags = setFlagBit(flags, ASYNC_OPER);
        else
            flags &= ~ASYNC_OPER;

        return flags;
    }

    static public int setUseRegionXn(int flags, boolean v) {
        if (v)
            flags = setFlagBit(flags, USE_REGION_XN);
        else
            flags &= ~USE_REGION_XN;

        return flags;
    }

    static public int setIncrementalBackup(int flags, boolean v) {
        if (v)
            flags = setFlagBit(flags, INCR_BACKUP);
        else
            flags &= ~INCR_BACKUP;

        return flags;
    }

    public String initStrawScan(String webServers, String queryId, int explainNodeId, boolean isFactTable, Object[] entries)
            throws MalformedURLException,IOException,UnsupportedEncodingException, StrawScanException
    {
       StrawScanUtils.initStrawScan(webServers, queryId, explainNodeId, isFactTable, entries);
       return null;
    }

    public int getNextRangeNumStrawScan(String webServers, String queryId, int explainNodeId, int sequenceNb, long executionCount, 
            int espNb, int nodeId, boolean isFactTable) throws IOException
    {
       return StrawScanUtils.getNextRangeNumStrawScan(webServers, queryId, explainNodeId, sequenceNb, executionCount, espNb, nodeId, isFactTable);
    }
    
    public String freeStrawScan(String queryId, int explainNodeId) throws IOException
    {
       StrawScanUtils.freeStrawScan(queryId, explainNodeId);
       return null;
    }
}
