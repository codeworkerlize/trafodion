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
import java.net.MalformedURLException; 
import java.io.UnsupportedEncodingException; 
import java.util.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.conf.Configuration;
import org.trafodion.sql.TrafConfiguration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.HiveDecimal;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type.Kind;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.Integer;
import java.lang.Long;
import java.sql.Timestamp;
import java.sql.Date;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.log4j.MDC;

public class OrcFileReader extends TrafExtStorageUtils
{
    class MyLinkedHashMap<K,V> extends java.util.LinkedHashMap<K,V> {

        private static final int MAX_ENTRIES = 64;
        private static final int MIN_FREE_MEMORY_MB = 75;
        private static final int MEGA_BYTE = 1024 * 1024;

        private Runtime m_runtime = null;

        public MyLinkedHashMap(int initialCapacity,
                               float loadFactor,
                               boolean accessOrder)  {
            super(initialCapacity, loadFactor, accessOrder);
            m_runtime = Runtime.getRuntime();
        }

        protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
            if (logger.isTraceEnabled()) logger.trace("removeEldestEntry Enter");
            boolean lv_remove = size() > MAX_ENTRIES;

            if (lv_remove) {
                if (logger.isDebugEnabled()) logger.debug("size: " + size()
                                                          + " of the map greater than configured: " + MAX_ENTRIES);
            }

            if (! lv_remove) {
                long lv_free_memory = m_runtime.freeMemory() / MEGA_BYTE;
                if (lv_free_memory < MIN_FREE_MEMORY_MB) {
                    if (logger.isDebugEnabled()) logger.debug("current available memory(in MB): "
                                                              + lv_free_memory
                                                              + " is less than the minimum free threshold(in MB): "
                                                              + MIN_FREE_MEMORY_MB
                                                              );
                    lv_remove = true;
                }
            }
            return lv_remove;
        }

    };

    static Logger logger = Logger.getLogger(OrcFileReader.class.getName());;
    static Configuration               m_conf;
    
    static {
	m_conf = TrafConfiguration.create(TrafConfiguration.HDFS_CONF);
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName(); 
        MDC.put("PID", pid);
	System.setProperty("hostName", System.getenv("HOSTNAME"));
    }
    
    Path                        m_file_path;
    
    Reader                      m_reader;

    Map<String, Reader>         m_map_readers = new MyLinkedHashMap<String, Reader>(16,
										    (float)0.75,
										    true);

    List<OrcProto.Type>         m_types;
    StructObjectInspector       m_oi;
    List<? extends StructField> m_fields;

    /* Only to be used for testing - when called by this file's main() */
    static void setupLog4j() {
       System.out.println("In setupLog4J");
       String confFile = System.getenv("TRAF_CONF")
	   + "/log4j.hdfs.config";
       System.setProperty("trafodion.hdfs.log", System.getenv("PWD") + "/org_trafodion_sql_OrcFileReader_main.log");
       PropertyConfigurator.configure(confFile);
    }

    OrcFileReader() 
    {
    }

    public String open(String pv_file_name, 
		       int    pv_num_cols_to_project,
		       int[]  pv_which_cols,
		       int    pv_expected_row_size,
		       int    pv_max_rows_to_fill,
		       int    pv_max_allocation_in_mb) throws IOException 
    {
        return open(pv_file_name, 0L, Long.MAX_VALUE, pv_num_cols_to_project, pv_which_cols,
                    pv_expected_row_size, pv_max_rows_to_fill, pv_max_allocation_in_mb, null, null, 0);
    }

    
    public String open(String pv_file_name, 
		       long   offset, 
		       long   length,
		       int    pv_num_cols_to_project,
		       int[]  pv_which_cols,
		       int    expected_row_size,
		       int    max_rows_to_fill,
		       int    max_allocation_in_mb,
                       Object[] ppi_vec,
                       Object[] ppi_all_cols,
                       int    flags) throws IOException 
    {

        //System.out.println("open(): pv_filename = " + pv_file_name);

	if (logger.isDebugEnabled()) logger.debug("Enter open()," 
						  + ", file: " + pv_file_name
						  + ", offset: " + offset
						  + ", length: " + length
						  + ", num_cols_to_project: " + pv_num_cols_to_project
						  + ", #which_cols_array: " + (pv_which_cols == null ? "(null)" : pv_which_cols.length)
						  + ", expected_row_size: " + expected_row_size
 						  + ", max_rows_to_fill: " + max_rows_to_fill
 						  + ", max_allocation_in_mb: " + max_allocation_in_mb
						  + ", #ppi_vec: " + (ppi_vec == null ? "(null)":ppi_vec.length)
						  + ", #ppi_cols: " + (ppi_all_cols == null ? "(null)":ppi_all_cols.length)
						  );

	m_file_path = new Path(pv_file_name);
	
	m_reader = m_map_readers.get(pv_file_name);
	if (m_reader == null) {
	    boolean retryCreate = false;
	    if (logger.isDebugEnabled()) logger.debug("open() - creating a reader" 
						      + ", file: " + pv_file_name
						      + ", offset: " + offset
						      );
	    try{
		m_reader = OrcFile.createReader(m_file_path, OrcFile.readerOptions(m_conf));
	    } 
	    catch (javax.security.sasl.SaslException e2) {retryCreate = true;}
            
            // get this exception for a zero-byte file when the OrcFile#createReader is
            // trying to read the footer data from a non-existent ByteBuffer location.
	    catch (java.lang.IndexOutOfBoundsException e3) {
                m_reader = null; 
                logger.error("Exception while reading the file: " + m_file_path + " ", e3); 
                return null; 
            }
           
	    // Retry the operation once for GSS (Kerberos) exceptions)
	    if (retryCreate) {
	      try{
	         // sleep for a minute before retrying
	         Thread.sleep(60000); 
	      } catch (InterruptedException e) {
	      }
	      try{
	         if (logger.isDebugEnabled()) logger.debug ("open() - retrying call to createReader fo GSS exception"
	                                                    + ", file: " + pv_file_name
	                                                    );
	         m_reader = OrcFile.createReader(m_file_path, OrcFile.readerOptions(m_conf));
	      } catch (javax.security.sasl.SaslException e2) {
                  throw e2;
	         //logger.error("GSS exception: " + pv_file_name);
	         //return "GSS exception";
	      }
	    }

	    m_map_readers.put(pv_file_name, m_reader);
	    if (logger.isDebugEnabled()) logger.debug("open() - put reader to map" 
						      + ", file: " + pv_file_name
						      + ", offset: " + offset
						      + ", map size: " + m_map_readers.size()
						      );

	}
	else {

	    if (logger.isDebugEnabled()) logger.debug("open() - found reader in the map" 
						      + ", file: " + pv_file_name
						      + ", offset: " + offset
						      + ", map size: " + m_map_readers.size()
						      );

	    
	}

	if (m_reader == null) {
	    if (logger.isTraceEnabled()) logger.trace("Error: open failed, createReader returned a null object");
	    return "open failed!";
	}

	m_types = m_reader.getTypes();
	m_oi = (StructObjectInspector) m_reader.getObjectInspector();
	m_fields = m_oi.getAllStructFieldRefs();
	if (logger.isDebugEnabled()) logger.debug("open() - reader created and got MD types" 
						  + ", file: " + pv_file_name
						  + ", offset: " + offset
						  + ", numTypes: " + (m_types == null ? "(null)":m_types.size())
						  + ", numFields: " + (m_fields == null ? "(null)":m_fields.size())
						  );
	if (logger.isDebugEnabled()) logger.debug("Exit open(), got a record reader" 
						  + ", file: " + pv_file_name
						  + ", offset: " + offset
						  );

	return null;
    }
    public void close() throws IOException
    {
	if (logger.isTraceEnabled()) logger.trace("Enter close()");
	m_reader = null;
	m_file_path = null;            
	return;
    }

    public void printFileInfo() throws IOException 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter printFileInfo()");
	
	if (m_reader == null) {
	    if (logger.isTraceEnabled()) logger.trace("OrcFileReader.printFileInfo: Error: reader object is null. Exitting");
	    return;
	}

	System.out.println("Reader: " + m_reader);

	System.out.println("# Rows: " + m_reader.getNumberOfRows());
	System.out.println("Size of the file (bytes): " + m_reader.getContentLength());
	System.out.println("# Types in the file: " + m_types.size());
	for (int i=0; i < m_types.size(); i++) {
	    System.out.println("Type " + i + ": " + m_types.get(i).getKind());
	}

	System.out.println("Compression: " + m_reader.getCompression());
	if (m_reader.getCompression() != CompressionKind.NONE) {
	    System.out.println("Compression size: " + m_reader.getCompressionSize());
	}

	m_oi = (StructObjectInspector) m_reader.getObjectInspector();
	
	System.out.println("object inspector type category: " + m_oi.getCategory());
	System.out.println("object inspector type name    : " + m_oi.getTypeName());

	System.out.println("Number of columns in the table: " + m_fields.size());

	// Print the type info:
	for (int i = 0; i < m_fields.size(); i++) {
	    System.out.println("Column " + i + " name: " + m_fields.get(i).getFieldName());
	    ObjectInspector lv_foi = m_fields.get(i).getFieldObjectInspector();
	    System.out.println("Column " + i + " type category: " + lv_foi.getCategory());
	    System.out.println("Column " + i + " type name: " + lv_foi.getTypeName());
	}

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
    //     sum value (for numeric datatypes) or sum string lengths (for strings)
    public Object[] getColStats(int colNum) throws IOException 
     {
	if (logger.isTraceEnabled())
	    logger.trace("Enter getColStats"
			 + ", ORC file: " + m_file_path
			 + ", colNum: " + colNum
			 );

        if (m_reader == null) {
            ArrayList<byte[]> lv_emptyColStats = new ArrayList();
            lv_emptyColStats.add(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0).array());
            return lv_emptyColStats.toArray();
        }

        ArrayList<byte[]> retColStats = new ArrayList();

        // total number of vals (includes null and dups)
        long numVals = m_reader.getNumberOfRows();
	if (logger.isTraceEnabled()) 
	    logger.trace("getColStats"
			 + ", ORC file: " + m_file_path
			 + ", colNum" + colNum
			 + ", numRows: " + numVals
			 );
	
	ColumnStatistics[] lv_cs = m_reader.getStatistics();

	if (lv_cs == null) {
	    logger.error("getColStats"
			 + ", ORC file: " + m_file_path
			 + ", colNum: " + colNum
			 + ", reader.getStatistics returned null"
			 );
	    numVals = 0;
	    colNum = -1;
	}

	if (lv_cs.length <= colNum) {
	    if (logger.isDebugEnabled()) 
		logger.debug("getColStats"
			     + ", ORC file: " + m_file_path
			     + ", colNum: " + colNum
			     + ", ColumnStatistics[] length: " + lv_cs.length
			     );
	    numVals = 0;
	    colNum = -1;
	}

        byte[] bytes = 
            ByteBuffer.allocate(8) //Long.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putLong(numVals).array();
        retColStats.add(bytes);

        if (colNum == -1)
            {
                return retColStats.toArray();
            }

        ColumnStatistics columnStatistics = lv_cs[colNum];

        // type of aggr
        List<Type> types = m_reader.getTypes();
        Type[] arrayTypes = types.toArray(new Type[0]);
        Type columnType = arrayTypes[colNum];
        int ctInt = columnType.getKind().getNumber();
        bytes = 
            ByteBuffer.allocate(4) //Integer.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(ctInt).array();
        retColStats.add(bytes);

        // num of not null values (does not include nulls)
        long numNotNullVals = columnStatistics.getNumberOfValues();
        bytes = 
            ByteBuffer.allocate(8) //Long.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putLong(numNotNullVals).array();
        retColStats.add(bytes);

	if (logger.isTraceEnabled()) logger.trace("getColStats - before switch"
						  + ", columnType kind: " + columnType.getKind()
						  );
        switch (columnType.getKind())
            {
            case BOOLEAN:
                {
                    BooleanColumnStatistics bcs = 
                        (BooleanColumnStatistics)columnStatistics;
                    long fc = bcs.getFalseCount();
                    long tc = bcs.getTrueCount();
                    byte min = (byte)(fc > 0 ? 0 : 1);
                    byte max = (byte)(tc > 0 ? 1 : 0);

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
                
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                {
                    IntegerColumnStatistics ics = 
                        (IntegerColumnStatistics)columnStatistics;
                    long min = ics.getMinimum();
                    long max = ics.getMaximum();
                    long sum = (ics.isSumDefined() ? ics.getSum() : -1);

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

            case FLOAT:
            case DOUBLE:
                {
                    DoubleColumnStatistics dcs = 
                        (DoubleColumnStatistics)columnStatistics;
                    double min = dcs.getMinimum();
                    bytes = 
                        ByteBuffer.allocate(8) //Double.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putDouble(min).array();
                    retColStats.add(bytes);

                    double max = dcs.getMaximum();
                    bytes = 
                        ByteBuffer.allocate(8) //Double.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putDouble(max).array();
                    retColStats.add(bytes);

                    double sum = dcs.getSum();
                    bytes = Bytes.toBytes(sum);
                    bytes = 
                        ByteBuffer.allocate(8) //Double.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putDouble(sum).array();
                    retColStats.add(bytes);
                }
                break;

            case CHAR:
            case VARCHAR:
            case STRING:
                {
                    StringColumnStatistics scs = 
                        (StringColumnStatistics)columnStatistics;
                    String min = scs.getMinimum();
                    bytes = min.getBytes();
                    retColStats.add(bytes);

                    String max = scs.getMaximum();
                    bytes = max.getBytes();
                    retColStats.add(bytes);

                    long sum = scs.getSum();
                    bytes = 
                        ByteBuffer.allocate(8) //Long.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(sum).array();
                    retColStats.add(bytes);              
                }
                break;
                
            case DATE:
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
                
            case TIMESTAMP:
                {
                    TimestampColumnStatistics scs = 
                        (TimestampColumnStatistics)columnStatistics;

                    String min = scs.getMinimum().toString();
                    bytes = min.getBytes();
                    retColStats.add(bytes);

                    String max = scs.getMaximum().toString();
                    bytes = max.getBytes();
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
 
    public long getSumStringLengths() throws IOException
    {
	if (logger.isTraceEnabled()) logger.trace("Enter getSumStringLengths");

        long result = 0;

        if (m_reader == null) {
            return 0;
        }

        long numVals = m_reader.getNumberOfRows();

        // oddly, the indexing into m_reader.getTypes() and m_reader.getStatistics()
        // seems to be 1-based, though I can't find documentation to this effect;
        // yet m_fields.get() seems to be 0-based

        for (int colNum = 1; colNum <= m_fields.size(); colNum++) {
            List<Type> types = m_reader.getTypes();
            Type[] arrayTypes = types.toArray(new Type[0]);
            Type columnType = arrayTypes[colNum];
         
            if (columnType.getKind() == Kind.STRING) {
 
                ColumnStatistics columnStatistics = 
                     m_reader.getStatistics()[colNum];
                StringColumnStatistics scs = 
                     (StringColumnStatistics)columnStatistics;

                result += scs.getSum();       
            }
        }

	return result;
    }

    // On return, 
    //    object[0]: # of total rows in the OCR file (as long)
    //    object[1]: # of total rows in the OCR file (as long[])
    //    object[2]: offset of each strip (as long[])
    //    object[3]: total length of each strip (as long[])
    long[] getStripeOffsets() throws IOException {

       if ( m_reader == null )
          return null;

       Iterable<StripeInformation> siItor = m_reader.getStripes();
       List<Long> offsets  = new ArrayList<Long>();

       for (StripeInformation si : siItor) {
          offsets.add(si.getOffset());
       }

       //return offsets.toArray(new Long[offsets.size()]);
       long[] x = new long[offsets.size()];
       for (int i=0; i<offsets.size(); i++)
         x[i] = offsets.get(i);

       return x;
    }

    long[] getStripeLengths() throws IOException {

       if ( m_reader == null )
          return null;

       Iterable<StripeInformation> siItor = m_reader.getStripes();
       List<Long> lengths  = new ArrayList<Long>();

       for (StripeInformation si : siItor) {
          lengths.add(si.getIndexLength() + si.getDataLength() + si.getFooterLength());
       }

       //return lengths.toArray(new Long[lengths.size()]);
       long[] x = new long[lengths.size()];
       for (int i=0; i<lengths.size(); i++)
         x[i] = lengths.get(i);

       return x;
    }

    long[] getStripeNumRows() throws IOException {

        if ( m_reader == null ) {
            return null;
        }

       Iterable<StripeInformation> siItor = m_reader.getStripes();
       ArrayList<Long> numRows = new ArrayList<Long>();

       for (StripeInformation si : siItor) {
          numRows.add(si.getNumberOfRows());
       }

       //return numRows.toArray(new long[numRows.size()]);
       long[] x = new long[numRows.size()];
       for (int i=0; i<numRows.size(); i++)
         x[i] = numRows.get(i);

       return x;
    }

    void printStripeInfo() throws IOException {

       long[] offsets = getStripeOffsets();
       long[] lengths= getStripeLengths();
       long [] numRows = getStripeNumRows();

       if ( offsets == null || lengths == null || numRows == null ) {
          System.out.println("Getting stripe info failed");
          return;
       }

       //long dim1[] = (long[])(numRows);
       //Long dim2[] = (Long[])(offsets);
       //Long dim3[] = (Long[])(lengths);

       for (int i=0; i<offsets.length; i++ ) {
         System.out.println(i + ":" + numRows[i] + "," + offsets[i] + "," + lengths[i]);
       }
    }

    public static void main(String[] args) throws Exception
    {
	boolean lv_print_info = false;
	boolean lv_done = false;

	int     lv_column_number = -1;
	String  lv_column_value = null;
	String  lv_file_name = null;

	int     lv_expected_row_size = 1000;
	int     lv_max_rows_to_fill = 128;
	int     lv_max_allocation_in_mb = 32;

	int lv_count = 0;

	setupLog4j();

	int lv_index = 0;
	for (String lv_arg : args) {
	    lv_index++;
	    if (lv_arg.compareTo("-i") == 0) {
		lv_print_info = true;
	    }
	    if (lv_arg.compareTo("-f") == 0) {
		lv_file_name = args[lv_index];
		System.out.println("Input file: " + lv_file_name);
	    }
	}

	System.out.println("OrcFile Reader main");

	OrcFileReader lv_this = new OrcFileReader();

	lv_this.open(lv_file_name, 0, null, 0, 0,0);

	if (lv_print_info) {
	    System.out.println("================= Begin File Info:" + 
			       args[0]);
	
	    lv_this.printFileInfo();

	    System.out.println("================= End File Info:" + 
			       args[0]);
	}
    }
}
