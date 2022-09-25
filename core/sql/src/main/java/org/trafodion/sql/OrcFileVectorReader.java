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

import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;

import org.apache.hadoop.hbase.util.Bytes;

import java.lang.Integer;
import java.lang.Long;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.log4j.MDC;

public class OrcFileVectorReader extends TrafExtStorageUtils
{

    static Logger logger = Logger.getLogger(OrcFileVectorReader.class.getName());;
    static Configuration m_conf;
    static ExecutorService m_executorService;
    static boolean  m_preFetch;

    static {
       m_conf = TrafConfiguration.create(TrafConfiguration.HDFS_CONF);
       m_executorService = Executors.newCachedThreadPool();
       m_preFetch = m_conf.getBoolean("orc.prefetch", true);
       RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
       String pid = rt.getName();
       MDC.put("PID", pid);
       System.setProperty("hostName", System.getenv("HOSTNAME"));
    }

    
    private Path                        m_file_path;
    private VectorizedRowBatch          m_batch = null;
    private Reader.Options              m_options;
    private boolean                     m_include_cols[];
    private int                         m_col_count;
    private int                         m_proj_col_count;
    private Reader                      m_reader;
    private List<TypeDescription>       m_types;
    private RecordReader                m_rr;
    private long                        m_jni_object;
    private int                         m_vector_size;
    private Future                      m_future;
    private FetchBatchHelper            m_fetchBatchHelper;
    private boolean                     m_useUTCTimestamp;
    // Variables used in the next batch
    private int colCount;
    private int vectorSize;
    private int[] columnIds;
    private int[] columnTypeOrdinal;
    private Object[] columnValueArrays;
    private Object[] columnValueExtArrays1;
    private Object[] columnValueExtArrays2;
    private Object[] nullValueArrays;
    private boolean [] noNulls;
    private boolean [] isRepeating;

    class FetchBatchHelper implements Callable 
    {
       public Boolean call() throws IOException {
          boolean nextBatchAvailable;
          nextBatchAvailable = m_rr.nextBatch(m_batch); 
          return nextBatchAvailable;
       } 
    }

    OrcFileVectorReader() 
    {
    }

    Object getOperandObject(int operandType, TypeDescription.Category dataType, byte[] operator_ba) throws IOException
    {
       Object operandObject = null;
       if (operandType == 1) {
          operandObject = java.sql.Timestamp.valueOf(Bytes.toString(operator_ba));
          return operandObject;
       }
       switch (dataType) {
          case SHORT:
          case INT:
          case LONG:
          case BYTE:
             operandObject = Long.valueOf(new String(operator_ba));
             break;
          case FLOAT:
          case DOUBLE:
             operandObject = Double.valueOf(new String(operator_ba));
             break;
          case TIMESTAMP:
             operandObject = java.sql.Timestamp.valueOf(Bytes.toString(operator_ba));
             break;
          case DATE:
             operandObject = java.sql.Date.valueOf(Bytes.toString(operator_ba));
             break;
          case STRING:
          case VARCHAR:
          case CHAR:
             String strVal = Bytes.toString(operator_ba);
             // trim trailing spaces
             operandObject = strVal.replaceAll("\\s*$", "");
             break;
          case DECIMAL:
             operandObject = new HiveDecimalWritable(Bytes.toString(operator_ba));
             break;
          case BINARY:
          default:
             throw new IOException("Push down for column type " + dataType.getName() + " not supported");
        }        
        return operandObject; 
    }

    Object[] constructObjectArray(TypeDescription.Category  dataType, int operandType, ByteBuffer bb) throws IOException
    {
       int entries = bb.getInt();
       if (logger.isInfoEnabled()) 
           logger.info("constructObjectArray()"
             + ", dataType: " + dataType
             + ", operandType: " + operandType
             + ", entries: " + entries
             );

       Object[] objArray = new Object[entries];

       for (int i=0; i<entries; i++ ) {
      
          int operandLen = bb.getInt();
          byte[] operator_ba = null;

          if (logger.isDebugEnabled()) 
             logger.debug("operand length= " + operandLen);
  
          if (operandLen > 0) {
             operator_ba = new byte[operandLen];
             bb.get(operator_ba, 0, operandLen);
          } else {
              throw new IOException("Operand length can't be zero in in-list predicate");
          }
  
          Object operandObject = getOperandObject(operandType, dataType, operator_ba);
          if (logger.isDebugEnabled()) 
             logger.debug("operand = " + operandObject);
          objArray[i] = operandObject;
       }
       return objArray;
    }

    private PredicateLeaf.Type getPredicateType(TypeDescription.Category columnType) 
    {
       PredicateLeaf.Type predicateType = null;
       switch (columnType) {
          case SHORT:
          case INT:
          case LONG:
          case BYTE:
             predicateType = PredicateLeaf.Type.valueOf("LONG");
             break;
          case FLOAT:
          case DOUBLE:
             predicateType = PredicateLeaf.Type.valueOf("FLOAT");
             break;
          case DATE:
             predicateType = PredicateLeaf.Type.valueOf("DATE");
             break;
          case TIMESTAMP:
             predicateType = PredicateLeaf.Type.valueOf("TIMESTAMP");
             break;
          case STRING:
          case VARCHAR:
          case CHAR:
             predicateType = PredicateLeaf.Type.valueOf("STRING");
             break;
          case DECIMAL:
             predicateType = PredicateLeaf.Type.valueOf("DECIMAL");
             break;
       }
       return predicateType;
    }

    private SearchArgument buildSARG(Object[] ppi_vec, String[] pp_col_names) throws IOException
    {
        if (logger.isDebugEnabled()) logger.debug("buildSARG"
                    + ", #ppi_vec: " + (ppi_vec != null ? ppi_vec.length:0)
                    + ", #col names: " + (pp_col_names != null ? pp_col_names.length: 0)
                    );

        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        PredicateLeaf.Type predicateType = null;
       
        int operatorType;
        int colNameLen;
        byte[] colName = null;
        int colIndex;
        int operandType;
        int operandLen;
        byte[] operator_ba = null;
        Object operandObject = null;
        List<String> fieldNames = m_reader.getSchema().getFieldNames();
        String fieldName = null;
        boolean pushdownProblem = false;
        for (int i = 0; i < ppi_vec.length; i++) {
           ByteBuffer bb = ByteBuffer.wrap((byte[])ppi_vec[i]);
           bb.order(m_byteorder);
           operatorType = bb.getInt();
           colNameLen = bb.getInt();
           colIndex = -1;
           TypeDescription.Category  columnType = null;
           if (colNameLen > 0) {
              colName = new byte[colNameLen];
              bb.get(colName, 0, colNameLen);
              colIndex = Arrays.asList(pp_col_names).indexOf(Bytes.toString(colName));
              if ((m_types == null) || (colIndex == -1) || ((colIndex) >= m_types.size())) { 
                 // The colIndex >= m_types.size() case happens on an added column that is missing
                 // from this file.
                 logger.info("Predicate push down not done for " + colName + ", file: " + m_file_path + " ColIndex " + colIndex );
                 pushdownProblem = true; 
                 continue;
              }
              columnType = m_types.get(colIndex).getCategory();
              predicateType = getPredicateType(columnType);
              fieldName = fieldNames.get(colIndex); 
          }
          operandType = bb.getInt();
      
          if (operatorType == IN)  {
              Object[] objectArray = constructObjectArray(columnType, operandType, bb);
              builder.in(Bytes.toString(colName), predicateType, objectArray);
          } else {
             operandLen = bb.getInt();

             if (operandLen > 0) {
                operator_ba = new byte[operandLen];
                bb.get(operator_ba, 0, operandLen);
                operandObject = getOperandObject(operandType, columnType, operator_ba);
             }
             else
                operandObject = null;

             switch (operatorType) {
                case UNKNOWN_OPER:
                   logger.info("Operator: UNKNOWN_OPER for " + fieldName + " value " + operandObject);
                   break;
                case STARTAND:
                   builder.startAnd();
                   break;
                case STARTOR:
                   builder.startOr();
                   break;
                case STARTNOT:
                   builder.startNot();
                   break;
                case END:
                   // sadly, builder throws an exception in the case nothing is pushed down
                   // so we have to avoid calling end() in that case
                   if (!pushdownProblem) {
                     builder.end();
                   }
                   break;
                case EQUALS:
                   builder.equals(fieldName, predicateType, operandObject);
                   break;
                case LESSTHAN:
                   builder.lessThan(fieldName, predicateType, operandObject);
                   break;
                case LESSTHANEQUALS:
                   builder.lessThanEquals(fieldName, predicateType, operandObject);
                   break;
                case ISNULL:
                   builder.isNull(fieldName, predicateType); 
                   break;
             }
          } // operatorType != IN
       }

       // TODO: For now, if there was a predicate we could not push down, we'll avoid
       // pushing anything down. The reason is that we discover that we can't push
       // something down too late. We may have already added a STARTAND to the builder
       // for example, and if we don't give that enough predicates, the builder will
       // throw an exception. Worse, the builder might succeed but build a SARG that is
       // not the equivalent of the original expressions. A better solution would
       // rewrite the predicate stream, for example, removing any STARTNOT that is just
       // before a predicate that can't be pushed down. The general algorithm would
       // be something like: replace any predicate that can't be pushed down with "TRUE"
       // then rewrite the predicate tree via constant folding.
       SearchArgument sarg = pushdownProblem ? null : builder.build();
       return sarg;
    } 
    
    public long open(long jniObject, String pv_file_name, 
             long   offset, 
             long   length,
             int    vector_size,
             int    pv_num_cols_to_project,
             int[]  pv_which_cols,
             Object[] ppi_vec,
             Object[] ppi_all_cols) throws IOException 
    {
       if (logger.isDebugEnabled()) logger.debug("Enter open()," 
                    + ", file: " + pv_file_name
                    + ", offset: " + offset
                    + ", length: " + length
                    + ", num_cols_to_project: " + pv_num_cols_to_project
                    + ", #which_cols_array: " + (pv_which_cols == null ? "(null)" : pv_which_cols.length)
                    + ", #ppi_vec: " + (ppi_vec == null ? "(null)":ppi_vec.length)
                    + ", #ppi_cols: " + (ppi_all_cols == null ? "(null)":ppi_all_cols.length)
                    );

       m_file_path = new Path(pv_file_name);
       m_jni_object = jniObject;
       m_vector_size = vector_size; 
       m_useUTCTimestamp = m_conf.getBoolean("orc.useutctimestamp", true);
       boolean retryCreate = false;
       if (logger.isDebugEnabled()) 
          logger.debug("open() - creating a reader" + ", file: " + pv_file_name + ", offset: " + offset);
       try {
           m_reader = OrcFile.createReader(m_file_path, OrcFile.readerOptions(m_conf).useUTCTimestamp(m_useUTCTimestamp));
       } 
       catch (javax.security.sasl.SaslException e2) {
          retryCreate = true;
       }
       // get this exception for a zero-byte file when the OrcFile#createReader is
       // trying to read the footer data from a non-existent ByteBuffer location.
       catch (java.lang.IndexOutOfBoundsException e3) {
          m_reader = null; 
          logger.error("Exception while reading the file: " + m_file_path + " ", e3); 
          return -1; 
       }
           
       // Retry the operation once for GSS (Kerberos) exceptions)
       if (retryCreate) {
          try {
             // sleep for a minute before retrying
             Thread.sleep(60000); 
           } catch (InterruptedException e) {}
           if (logger.isDebugEnabled()) 
              logger.debug ("open() - retrying call to createReader fo GSS exception" + ", file: " + pv_file_name);
           m_reader = OrcFile.createReader(m_file_path, OrcFile.readerOptions(m_conf).useUTCTimestamp(m_useUTCTimestamp));
       }
       if (logger.isDebugEnabled()) 
          logger.debug("open() - put reader to map" + ", file: " + pv_file_name + ", offset: " + offset);
       m_types = m_reader.getSchema().getChildren();

       int lv_num_cols_in_table = m_types.size();
       m_include_cols = new boolean[lv_num_cols_in_table+1];

       boolean lv_include_col = false;
       m_col_count = m_types.size();
       if (pv_num_cols_to_project == -1) {
           lv_include_col = true;
          m_proj_col_count = m_col_count;
       }
       else
          m_proj_col_count = 0;
   
       // Initialize m_include_cols
       for (int i = 0; i < lv_num_cols_in_table; i++) {
           m_include_cols[i] = lv_include_col;
       }
        // Set m_include_cols as per the passed in parameters
       if ((pv_num_cols_to_project > 0) && (pv_which_cols != null)) {
       	  for (int lv_curr_index : pv_which_cols) {
              if ((lv_curr_index >= 1) && (lv_curr_index <= lv_num_cols_in_table)) {
                 m_include_cols[lv_curr_index] = true;
                 m_proj_col_count++;
              }
          }
       }
       m_options = new Reader.Options()
            .range(offset, length)
            .include(m_include_cols);

       if (ppi_vec != null) {

          SearchArgument sarg = buildSARG(ppi_vec, (String[])ppi_all_cols);

          if (sarg != null) {
             if (logger.isDebugEnabled()) {
                logger.debug("sarg for predicate pushdown created" + ", file: " + pv_file_name
                      + ", offset: " + offset);
             }
             // Second parameter is not used at all in ORC 1.4.4 reader
             m_options = m_options.searchArgument(sarg, (String[])ppi_all_cols);
          }
       }

       m_rr = m_reader.rows(m_options);
       //m_batch shows the projected columns as the number of columns in the table
       //, but the columns that are included in m_include_cols have NullColumnVector(0)
       m_batch = m_reader.getSchema().createRowBatch(m_vector_size);
       if (m_preFetch) {
          m_fetchBatchHelper = new FetchBatchHelper();
          m_future  =  m_executorService.submit(m_fetchBatchHelper);
       }
       // Supposedly to be returning count of qualifying rows, but it seems to be not reliable
       return m_batch.count(); 
    }

    public void close() throws IOException
    {
       if (logger.isTraceEnabled()) logger.trace("Enter close()");
       // Complete the pending IO
       if (m_future != null) {
          try {
             m_future.get(30, TimeUnit.SECONDS);
          } catch(TimeoutException e) {
             logger.error("Asynchronous Thread is Cancelled (timeout), " + e);
             m_future.cancel(true); // Interrupt the thread
          } catch(InterruptedException e) {
             logger.error("Asynchronous Thread is Cancelled (interrupt), " + e);
             m_future.cancel(true); // Interrupt the thread
          } catch (ExecutionException ee)
          {}
       }
       m_batch = null;
       if (m_rr != null) {
          m_rr.close(); 
          m_rr = null;
       }
       m_reader = null;
       m_file_path = null;            
       return;
    }

    public boolean fetchNextBatch() throws IOException, InterruptedException, ExecutionException
    {
       boolean result;
       if (m_preFetch) {
          result = (Boolean)m_future.get();           
          if (result) {  
             result = pushBatchToJni();
             //m_fetchBatchHelper = new FetchBatchHelper();
             m_future  =  m_executorService.submit(m_fetchBatchHelper);
          }
          else {
             vectorSize = m_batch.size;  
             // Pass zero rows to JNI to indicate EOR is reached
             setResultInfo(m_jni_object, m_useUTCTimestamp, colCount, null, null, vectorSize, null, null, 
                  null, null, null, null);
             m_future = null;
          }
       } else {
          result = m_rr.nextBatch(m_batch); 
          if (result)
             result = pushBatchToJni();
          else {
             vectorSize = m_batch.size;  
             // Pass zero rows to JNI to indicate EOR is reached
             setResultInfo(m_jni_object, m_useUTCTimestamp, colCount, null, null, vectorSize, null, null, 
                  null, null, null, null);
          }
       } 
       return (!result); 
    }

    private byte[][] copyBytesColumnVector(byte[][] vector) 
    {
       byte[][] copyVector = new byte[vector.length][];
       System.arraycopy(vector, 0, copyVector, 0, vector.length);
       return copyVector;
    } 
      //public boolean fetchNextBatch() throws IOException
    private boolean pushBatchToJni() throws IOException
    {
       // m_proj_col_count can vary from file to file because of ALTER TABLE ADD COLUMN,
       // so we need to reallocate the arrays if we encounter such variation
       if ((colCount == 0)   // if first time through or no columns
          || (colCount != m_proj_col_count)) {  // or variation between files
          colCount = m_proj_col_count;
          // No columns are projected
          if (colCount == 0) {
             vectorSize = m_batch.size;  
             setResultInfo(m_jni_object, m_useUTCTimestamp, colCount, null, null, vectorSize, null, null, 
                  null, null, null, null);
             return (! m_batch.endOfFile);
          }
          columnValueArrays = new Object[colCount];
          nullValueArrays = new Object[colCount];
          columnTypeOrdinal = new int[colCount];
          columnValueExtArrays1 = new Object[colCount];
          columnValueExtArrays2 = new Object[colCount]; 
          noNulls = new boolean[colCount];
          isRepeating = new boolean[colCount];
          columnIds = new int[colCount];
       }
       ColumnVector[] columnVec = m_batch.cols;
       TypeDescription.Category columnType;
       HiveDecimalWritable[] hdArray;
       HiveDecimal hd;
       long[] hdLongArray = null;
       String[] hdStringArray = null;
       short precision;
       short scale;
       for (int colIndex = 0, j = 0; colIndex < m_batch.projectionSize ; colIndex++) {
           if (m_include_cols[colIndex+1]) {
              columnIds[j] = m_types.get(colIndex).getId();
              columnType = m_types.get(colIndex).getCategory();
              columnTypeOrdinal[j] = columnType.ordinal();
              noNulls[j] = columnVec[colIndex].noNulls;
              isRepeating[j] = columnVec[colIndex].isRepeating;
              if (noNulls[j]) 
                  nullValueArrays[j] = null;
              else
                  nullValueArrays[j] = columnVec[colIndex].isNull;
              switch (columnType) {
                 case SHORT:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_SHORT_TYPE;
                    columnValueArrays[j] = ((LongColumnVector) columnVec[colIndex]).vector;
                    break;
                 case INT:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_INT_TYPE;
                    columnValueArrays[j] = ((LongColumnVector) columnVec[colIndex]).vector;
                    break;
                 case LONG:          
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_LONG_TYPE;
                    columnValueArrays[j] = ((LongColumnVector) columnVec[colIndex]).vector;
                    break;
                 case BOOLEAN:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_BOOLEAN_TYPE;
                    columnValueArrays[j] = ((LongColumnVector) columnVec[colIndex]).vector;
                    break;
                 case FLOAT:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_FLOAT_TYPE;
                    columnValueArrays[j] = ((DoubleColumnVector) columnVec[colIndex]).vector;
                    break;
                 case DOUBLE:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_DOUBLE_TYPE;
                    columnValueArrays[j] = ((DoubleColumnVector) columnVec[colIndex]).vector;
                    break;
                 case TIMESTAMP:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_TIMESTAMP_TYPE;
                    columnValueArrays[j] = ((TimestampColumnVector) columnVec[colIndex]).time;
                    columnValueExtArrays1[j] = ((TimestampColumnVector) columnVec[colIndex]).nanos;
                    break;
                 case DATE:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_DATE_TYPE;
                    columnValueArrays[j] = ((LongColumnVector) columnVec[colIndex]).vector;
                    break;
                 case STRING:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_STRING_TYPE;
                    columnValueArrays[j] = copyBytesColumnVector(((BytesColumnVector) columnVec[colIndex]).vector);
                    columnValueExtArrays1[j] = ((BytesColumnVector) columnVec[colIndex]).start;
                    columnValueExtArrays2[j] = ((BytesColumnVector) columnVec[colIndex]).length;
                    break;
                 case VARCHAR:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_VARCHAR_TYPE;
                    columnValueArrays[j] = copyBytesColumnVector(((BytesColumnVector) columnVec[colIndex]).vector);
                    columnValueExtArrays1[j] = ((BytesColumnVector) columnVec[colIndex]).start;
                    columnValueExtArrays2[j] = ((BytesColumnVector) columnVec[colIndex]).length;
                    break;
                 case CHAR:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_CHAR_TYPE;
                    columnValueArrays[j] = copyBytesColumnVector(((BytesColumnVector) columnVec[colIndex]).vector);
                    columnValueExtArrays1[j] = ((BytesColumnVector) columnVec[colIndex]).start;
                    columnValueExtArrays2[j] = ((BytesColumnVector) columnVec[colIndex]).length;
                    break;
                 case BYTE:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_BYTE_TYPE;
                    columnValueArrays[j] = ((LongColumnVector) columnVec[colIndex]).vector;
                    break;
                 case BINARY:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_BINARY_TYPE;
                    columnValueArrays[j] = copyBytesColumnVector(((BytesColumnVector) columnVec[colIndex]).vector);
                    columnValueExtArrays1[j] = ((BytesColumnVector) columnVec[colIndex]).start;
                    columnValueExtArrays2[j] = ((BytesColumnVector) columnVec[colIndex]).length;
                    break;
                 case DECIMAL:
                    columnTypeOrdinal[j] = TrafExtStorageUtils.HIVE_DECIMAL_TYPE;
                    hdArray = ((DecimalColumnVector) columnVec[colIndex]).vector;
                    precision = ((DecimalColumnVector) columnVec[colIndex]).precision;
                    scale = ((DecimalColumnVector) columnVec[colIndex]).scale;
                    // Convert the HiveDecimal into long if the precision is < 19 and
                    // adjust the decimal value by multiplying the scale power of 10.
                    // If the precision is 19 or greater convert it into a String
                    if (precision < 19) 
                       hdLongArray = new long[hdArray.length];
                    else
                       hdStringArray = new String[hdArray.length]; 
                    for (int i = 0; i < hdArray.length; i++) {
                        hd = hdArray[i].getHiveDecimal();
                        if (hd != null) {
                           if (precision < 19)
                              hdLongArray[i] = (hd.scaleByPowerOfTen(scale)).longValue();
                           else
                              hdStringArray[i] = hd.toString();
                        }
                        // Value is repeating, break early after populating the 0th value in the batch 
                        if (isRepeating[j])
                           break;
                    }
                    if (precision < 19) 
                       columnValueArrays[j] = hdLongArray;
                    else
                       columnValueArrays[j] = hdStringArray;
                    int precisionArray[] = new int[1];
                    precisionArray[0] = precision; 
                    columnValueExtArrays1[j] = precisionArray;
                    break;
                 default:
                    throw new IOException("Column Type " + columnType + " is not supported"); 
              }
              j++;
           }
        } // for loop
        vectorSize = m_batch.size;  
        setResultInfo(m_jni_object, m_useUTCTimestamp, colCount, columnIds, columnTypeOrdinal, vectorSize, columnValueArrays, columnValueExtArrays1, 
                  columnValueExtArrays2, noNulls, nullValueArrays, isRepeating);
        return (! m_batch.endOfFile);
    }
 
    private native void setResultInfo(long jniObject, boolean useUTCTimestamp,
                                  int colCount,
                                  int[] columnIds,
                                  int[] columnTypeOrdinal,
                                  int vectorSize,
                                  Object[] columnValueArray,
                                  Object[] columnValueExtArrays1,
                                  Object[] columnValueExtArrays2,
                                  boolean[] noNulls,
                                  Object[] nullValueArray,
                                  boolean[] isRepeating);
}
