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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.serde2.objectinspector.*;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

public class TestOrcFileReader
{

    static Logger logger = Logger.getLogger(TestOrcFileReader.class.getName());;

    static OrcFileReader sv_ofr = null;
    static String        sv_filename = null;

    /* Only to be used for testing - when called by this file's main() */
    static void setupLog4j() {
       System.out.println("In setupLog4J");
       String confFile = System.getenv("TRAF_CONF")
	   + "/log4j.sql.config";
       System.setProperty("trafodion.sql.log", System.getenv("PWD") + "/org_trafodion_sql_TestOrcFileReader_main.log");
       System.setProperty("hostName", System.getenv("HOSTNAME");
       PropertyConfigurator.configure(confFile);
    }

    static void printElapsedTime(long pv_ts1, long pv_ts2) 
    {
	System.out.println("Start time: " 
			   + pv_ts1
			   + " End time: " 
			   + pv_ts2
			   + " Diff: " 
			   + (pv_ts2 - pv_ts1)
			   + " In ms: "
			   + ((double)(pv_ts2 -pv_ts1)/1000000)
			   );
    }

    public static void mainOld(String[] args) throws Exception
    {
	boolean lv_print_info = false;
	boolean lv_perform_scans = false;
        boolean lv_perform_selective_scans = false;
        boolean lv_perform_vectorized_scans = false;

	setupLog4j();

	for (String lv_arg : args) {
	    if (lv_arg.compareTo("-i") == 0) {
		lv_print_info = true;
	    }
	    if (lv_arg.compareTo("-s") == 0) {
		lv_perform_scans = true;
	    }
	    if (lv_arg.compareTo("-ss") == 0) {
		lv_perform_selective_scans = true;
	    }
	    if (lv_arg.compareTo("-vs") == 0) {
		lv_perform_vectorized_scans = true;
	    }
	}

	System.out.println("OrcFile Reader main");

	OrcFileReader sv_ofr = new OrcFileReader();

	int lv_include_cols [] = new int[4];
	lv_include_cols[0]=1;
	lv_include_cols[1]=2;
	lv_include_cols[2]=6;
	lv_include_cols[3]=7;
	sv_ofr.open(sv_filename, 4, lv_include_cols, 2000, 2000, 4);

	if (lv_print_info) {
	    System.out.println("================= Begin File Info:" + 
			       sv_filename);
	
	    sv_ofr.printFileInfo();

	    System.out.println("================= End File Info:" + 
			       sv_filename);
	}

	if (lv_perform_scans) {
	    System.out.println("================= Begin: readFile_ByteBuffer");
	    sv_ofr.readFile_ByteBuffer();
	    System.out.println("================= End: readFile_ByteBuffer");

	    System.out.println("================= Begin: readFile_String");
	    sv_ofr.readFile_String();
	    System.out.println("================= End: readFile_String");

	    System.out.println("================= Begin: seeknSync(8)");
	    boolean lv_done = false;
	    if (sv_ofr.seeknSync(8) == null) {
		System.out.println("================= Begin: fetchNextRow()");
		while (! lv_done) {
		    System.out.println("Next row #: " + sv_ofr.getPosition());
		    byte[] lv_row_bb = sv_ofr.fetchNextRow();
		    if (lv_row_bb != null) {
			System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_row_bb, 0, 100));
			System.out.println("Length lv_row_bb: " + lv_row_bb.length);
		    }
		    else {
			lv_done = true;
		    }
		}
		System.out.println("================= End: fetchNextRow()");
	    }

	    lv_done = false;
	    String lv_row_string;
	    if (sv_ofr.seeknSync(11) == null) {
		System.out.println("================= Begin: After seeknSync(11)... will do fetchNextRow('|')");
		while (! lv_done) {
		    lv_row_string = sv_ofr.fetchNextRow('|');
		    if (lv_row_string != null) {
			System.out.println(lv_row_string);
		    }
		    else {
			lv_done = true;
		    }
		}
		System.out.println("================= End: After seeknSync(11)... will do fetchNextRow('|')");
	    }

	    sv_ofr.close();
	    sv_ofr.open(sv_filename, -1, null, 2000, 2000, 4);

	    lv_done = false;
	    if (sv_ofr.seeknSync(8) == null) {
		System.out.println("================= Begin: fetchNextRow()");
		while (! lv_done) {
		    System.out.println("Next row #: " + sv_ofr.getPosition());
		    byte[] lv_row_bb = sv_ofr.fetchNextRow();
		    if (lv_row_bb != null) {
			System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_row_bb, 0, 100));
			System.out.println("Length lv_row_bb: " + lv_row_bb.length);
		    }
		    else {
			lv_done = true;
		    }
		}
		System.out.println("================= End: fetchNextRow()");
	    }

	    lv_done = false;
	    if (sv_ofr.seeknSync(0) == null) {
		System.out.println("================= Begin: fetchNextBlock()");
		System.out.println("Next row #: " + sv_ofr.getPosition());
		ByteBuffer lv_block_bb = sv_ofr.fetchNextBlock();
		if (lv_block_bb != null) {
		    int lv_num_rows = lv_block_bb.getInt();
		    System.out.println("Length lv_block_bb: " + lv_block_bb.capacity());
		    System.out.println("Number of rows in the block: " + lv_num_rows);
		}

		System.out.println("================= End: fetchNextBlock()");
	    }

	}

    }

    public static void PerformScan() throws Exception
    {
	int lv_count = 0;
	boolean lv_done = false;

	String lv_row_string;
	sv_ofr.setVectorMode(false);
	if (sv_ofr.seeknSync(1) == null) {
	    logger.trace("================= Begin: After seeknSync(1)... will do fetchNextRow('|')");
	    while (! lv_done) {
		lv_row_string = sv_ofr.fetchNextRow('|');
		if (lv_row_string != null) {
		    System.out.println(lv_row_string);
		}
		else {
		    lv_done = true;
		}
	    }
	    logger.trace("================= End: After seeknSync(1)... will do fetchNextRow('|')");
	}

	sv_ofr.close();
	sv_ofr.open(sv_filename, -1, null, 2000, 2000, 4);

	lv_done = false;
	if (sv_ofr.seeknSync(8) == null) {
	    logger.trace("================= Begin: After seeknSync(8)...fetchNextRowFromVector('|')");
	    while (! lv_done) {
		lv_row_string = sv_ofr.fetchNextRow('|');
		if (lv_row_string != null) {
		    System.out.println(lv_row_string);
		}
		else {
		    lv_done = true;
		}
	    }
	}

	sv_ofr.close();
	sv_ofr.open(sv_filename, -1, null, 2000, 2000, 4);

	lv_done = false;
	if (sv_ofr.seeknSync(8) == null) {
	    logger.trace("================= Begin: fetchNextRow()");
	    while (! lv_done) {
		System.out.println("Next row #: " + sv_ofr.getPosition());
		byte[] lv_row_ba = sv_ofr.fetchNextRow();
		if (lv_row_ba != null) {
		    ByteBuffer lv_row_bb = ByteBuffer.wrap(lv_row_ba);
		    System.out.println("Length of the returned byte array: " + lv_row_ba.length 
				       + " data[] len: " + lv_row_bb.getInt()
				       + " col count: " + lv_row_bb.getInt()
				       + " row number: " + lv_row_bb.getLong());
		    System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_row_ba, 0, 100));
		}
		else {
		    lv_done = true;
		}
	    }
	    logger.trace("================= End: byte[] fetchNextRow()");
	}

	lv_done = false;
	if (sv_ofr.seeknSync(0) == null) {
	    logger.trace("================= Begin: fetchNextBlock()");
	    System.out.println("Next row #: " + sv_ofr.getPosition());
	    ByteBuffer lv_block_bb = sv_ofr.fetchNextBlock();
	    if (lv_block_bb != null) {
		System.out.println("Length of the returned byte array: " + lv_block_bb.capacity() 
				   + " #rows in the block: " + lv_block_bb.getInt()
				   + " data[] len: " + lv_block_bb.getInt()
				   + " col count: " + lv_block_bb.getInt()
				   + " row number: " + lv_block_bb.getLong()
				   + " 1st col len: " + lv_block_bb.getInt());
	    }

	    logger.trace("================= End: fetchNextBlock()");
	}

    }

    public static void PerformVectorizedScan() throws Exception
    {
	boolean lv_done = false;
	int     lv_count = 0;

	String lv_row_string;
	sv_ofr.setVectorMode(true);
	if (sv_ofr.seeknSync(1) == null) {
	    logger.trace("================= Begin: After seeknSync(1)... will do fetchNextRowFromVector('|')");
	    lv_count = 0;
	    while (! lv_done) {
		lv_row_string = sv_ofr.fetchNextRowFromVector('|');
		if (lv_row_string != null) {
		    System.out.println(lv_row_string);
		    if (++lv_count > 5) {
			lv_done = true;
		    }
		}
		else {
		    lv_done = true;
		}
	    }
	    logger.trace("================= End: After seeknSync(1)... will do fetchNextRowFromVector('|')");
	}

	sv_ofr.close();
	sv_ofr.open(sv_filename, -1, null, 2000, 2000, 4);

	lv_done = false;
	if (sv_ofr.seeknSync(8) == null) {
	    logger.trace("================= Begin: After seeknSync(8)...fetchNextRowFromVector('|')");
	    lv_count = 0;
	    while (! lv_done) {
		lv_row_string = sv_ofr.fetchNextRowFromVector('|');
		if (lv_row_string != null) {
		    System.out.println(lv_row_string);
		    if (++lv_count > 5) {
			lv_done = true;
		    }
		}
		else {
		    lv_done = true;
		}
	    }

	    lv_done = false;
	    while (! lv_done) {
		System.out.println("Next row #: " + sv_ofr.getPosition());

		lv_row_string = sv_ofr.fetchNextRowFromVector('|');
		if (lv_row_string != null) {
		    System.out.println(lv_row_string);
		    if (++lv_count > 5) {
			lv_done = true;
		    }
		}
		else {
		    lv_done = true;
		}

	    }
	    logger.trace("================= End: fetchNextRowFromVector('|')");
	}

	lv_done = false;
	if (sv_ofr.seeknSync(8) == null) {
	    logger.trace("================= Begin: byte[] fetchNextRowFromVector()");
	    lv_count = 0;
	    while (! lv_done) {
		System.out.println("Next row #: " + sv_ofr.getPosition());
		byte[] lv_row_ba = sv_ofr.fetchNextRowFromVector();
		if (lv_row_ba != null) {
		    ByteBuffer lv_row_bb = ByteBuffer.wrap(lv_row_ba);
		    System.out.println("Length of the returned byte array: " + lv_row_ba.length 
				       + " data[] len: " + lv_row_bb.getInt()
				       + " col count: " + lv_row_bb.getInt()
				       + " row number: " + lv_row_bb.getLong());
		    System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_row_ba, 0, 100));
		    if (++lv_count > 5) {
			lv_done = true;
		    }
		}
		else {
		    lv_done = true;
		}
	    }
	    logger.trace("================= End: byte[] fetchNextRowFromVector()");
	}
	    
	lv_done = false;
	if (sv_ofr.seeknSync(0) == null) {
	    logger.trace("================= Begin: fetchNextBlockFromVector()");
	    while (! lv_done) {
		System.out.println("Next row #: " + sv_ofr.getPosition());
		ByteBuffer lv_block_bb = sv_ofr.fetchNextBlockFromVector();
		if (lv_block_bb != null) {
		    System.out.println("Length of the returned byte array: " + lv_block_bb.capacity() 
				       + " #rows in the block: " + lv_block_bb.getInt()
				       + " data[] len: " + lv_block_bb.getInt()
				       + " col count: " + lv_block_bb.getInt()
				       + " row number: " + lv_block_bb.getLong()
				       + " 1st col len: " + lv_block_bb.getInt());
		}
		else {
		    lv_done = true;
		}
	    }

	    logger.trace("================= End: fetchNextBlockFromVector()");
	}
    }

    public static void MeasureScan() throws Exception
    {
	boolean lv_done = false;

	lv_done = false;
	String lv_row_string;

	lv_done = false;
	long ts1 = System.nanoTime();
	if (sv_ofr.seeknSync(0) == null) {
	    if (logger.isTraceEnabled()) logger.trace("================= Begin: fetchNextBlock()");
	    System.out.println("Next row #: " 
			       + sv_ofr.getPosition()
			       + " Table EOF?: " 
			       + sv_ofr.isEOF()
			       );
	    while (! lv_done) {
		ByteBuffer lv_block_bb = sv_ofr.fetchNextBlock();
		if (lv_block_bb != null) {
		    if (logger.isTraceEnabled()) logger.trace("Length of the returned byte array: " + lv_block_bb.capacity() 
							      + " #rows in the block: " + lv_block_bb.getInt()
							      + " data[] len: " + lv_block_bb.getInt()
							      + " col count: " + lv_block_bb.getInt()
							      + " row number: " + lv_block_bb.getLong()
							      + " 1st col len: " + lv_block_bb.getInt());
		    
		}
		else {
		    System.out.println("Next row #: " 
				       + sv_ofr.getPosition()
				       + " Table EOF?: " 
				       + sv_ofr.isEOF()
				       );
		    lv_done = true;
		}
	    }

	    if (logger.isTraceEnabled()) logger.trace("================= End: fetchNextBlock()");
	}
	long ts2 = System.nanoTime();
	printElapsedTime(ts1, ts2);

    }

    // Just doing a getNext() till we get to EOF
    public static void MeasurePureScan() throws Exception
    {
	boolean lv_done = false;

	long ts1 = System.nanoTime();
	if (sv_ofr.seeknSync(0) == null) {
	    if (logger.isTraceEnabled()) logger.trace("================= Begin: fetchNextBlock()");
	    System.out.println("Next row #: " 
			       + sv_ofr.getPosition()
			       + " Table EOF?: " 
			       + sv_ofr.isEOF()
			       );
	    while (! lv_done) {
		if (! sv_ofr.isEOF()) {
		    sv_ofr.getNext();
		}
		else {
		    System.out.println("Next row #: " 
				       + sv_ofr.getPosition()
				       + " Table EOF?: " 
				       + sv_ofr.isEOF()
				       );
		    lv_done = true;
		}
	    }

	    if (logger.isTraceEnabled()) logger.trace("================= End: fetchNextBlock()");
	}
	long ts2 = System.nanoTime();
	printElapsedTime(ts1, ts2);

    }

    public static void MeasureVectorizedScan() throws Exception
    {
	boolean lv_done = false;

	lv_done = false;
	sv_ofr.setVectorMode(true);

	lv_done = false;
	long ts1 = System.nanoTime();
	if (sv_ofr.seeknSync(0) == null) {
	    if (logger.isTraceEnabled()) logger.trace("================= Begin: fetchNextBlockFromVector()");
	    System.out.println("Next row #: " 
			       + sv_ofr.getPosition()
			       + " Table EOF?: " 
			       + sv_ofr.isEOF()
			       );
	    while (! lv_done) {
		ByteBuffer lv_block_bb = sv_ofr.fetchNextBlockFromVector();
		if (lv_block_bb != null) {
		    if (logger.isTraceEnabled()) logger.info("Length of the returned byte array: " + lv_block_bb.capacity() 
							     + " #rows in the block: " + lv_block_bb.getInt()
							     + " data[] len: " + lv_block_bb.getInt()
							     + " col count: " + lv_block_bb.getInt()
							     + " row number: " + lv_block_bb.getLong()
							     + " 1st col len: " + lv_block_bb.getInt());
		    
		}
		else {
		    System.out.println("Next row #: " 
				       + sv_ofr.getPosition()
				       + " Table EOF?: " 
				       + sv_ofr.isEOF()
				       );
		    lv_done = true;
		}
	    }

	    if (logger.isTraceEnabled()) logger.trace("================= End: fetchNextBlockFromVector()");
	}

	long ts2 = System.nanoTime();
	printElapsedTime(ts1, ts2);

    }

    // just the getNextBatch (not the time to fill it up in rows to be returned to SQL)
    public static void MeasurePureVectorizedScan() throws Exception
    {
	boolean lv_done = false;

	sv_ofr.setVectorMode(true);

	lv_done = false;
	long ts1 = System.nanoTime();
	if (sv_ofr.seeknSync(0) == null) {
	    if (logger.isTraceEnabled()) logger.trace("================= Begin: fetchNextBlockFromVector()");
	    System.out.println("Next row #: " 
			       + sv_ofr.getPosition()
			       + " Table EOF?: " 
			       + sv_ofr.isEOF()
			       );
	    while (! lv_done) {
		sv_ofr.getNextBatch();
		if (sv_ofr.isEOF()) {
		    System.out.println("Next row #: " 
				       + sv_ofr.getPosition()
				       + " Table EOF?: " 
				       + sv_ofr.isEOF()
				       );
		    lv_done = true;
		}
	    }
	    if (logger.isTraceEnabled()) logger.trace("================= End: fetchNextBlockFromVector()");
	}

	long ts2 = System.nanoTime();
	printElapsedTime(ts1, ts2);

    }

    static void open_read_1st_col()  throws Exception
    {
	int lv_include_cols [] = new int[4];
	lv_include_cols[0]=1;
	
	System.out.println("Opening " + sv_filename + ", reading only the first column");
	if (sv_ofr != null) {
	    sv_ofr.close();
	}

	sv_ofr.open(sv_filename, 1, lv_include_cols, 2000, 2000, 4);
    }

    static void open_read_8th_col()  throws Exception
    {
	int lv_include_cols [] = new int[4];
	lv_include_cols[0]=8;
	
	System.out.println("Opening " + sv_filename + ", reading only the eigth column");
	if (sv_ofr != null) {
	    sv_ofr.close();
	}

	sv_ofr.open(sv_filename, 1, lv_include_cols, 2000, 2000, 4);
    }

    static void open_read_2cols()  throws Exception
    {
	int lv_include_cols [] = new int[4];
	lv_include_cols[0]=1;
	lv_include_cols[1]=2;
	
	System.out.println("Opening " + sv_filename + ", reading the first 2 columns");
	if (sv_ofr != null) {
	    sv_ofr.close();
	}

	sv_ofr.open(sv_filename, 1, lv_include_cols, 2000, 2000, 4);
    }

    static void open_read_3cols()  throws Exception
    {
	int lv_include_cols [] = new int[100];
	lv_include_cols[0]=1;
	lv_include_cols[1]=2;
	lv_include_cols[2]=3;
	
	System.out.println("Opening " + sv_filename + ", reading the first 3 columns");
	if (sv_ofr != null) {
	    sv_ofr.close();
	}

	sv_ofr.open(sv_filename, 1, lv_include_cols, 2000, 2000, 4);
    }

    static void open_read_all_cols()  throws Exception
    {
	System.out.println("Opening " + sv_filename + ", reading all columns");
	if (sv_ofr != null) {
	    sv_ofr.close();
	}
	sv_ofr.open(sv_filename, -1, null, 2000, 2000, 4);
    }

    // test push-down with ORC
    public static String selectiveScanTest(String pv_file_name) throws IOException {

	Configuration m_conf = new Configuration();
	Path m_file_path = new Path(pv_file_name);

	Reader m_reader;
	try{
          m_reader = OrcFile.createReader(m_file_path, OrcFile.readerOptions(m_conf));
	} catch (java.io.FileNotFoundException e1) {
          return "file not found";
	}
	if (m_reader == null)
		return "open failed!";
	List<OrcProto.Type> m_types = m_reader.getTypes();
	StructObjectInspector m_oi = (StructObjectInspector) m_reader.getObjectInspector();
	List<? extends StructField> m_fields = m_oi.getAllStructFieldRefs();

        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();

        // other possible predicates
//.startAnd().equals("_col2", "2").end()
//.startOr().equals("_col2", 2).equals("_col1", 3).end()
//.startNot().equals("_col2", "2").end()
//.startAnd().between("_col2", 2, 3).end()
//.startAnd().in("_col2", 1, 3, 4).end()
        SearchArgument sarg = builder
	    .startAnd()
	    .equals("_col3", 6928)
	    .end()
        .build();
	
        // test 
	//boolean include[] = {true, true, true};
        //String colNames[] = {null, "_col0", "_col1"};
        //
	boolean[] include = new boolean[1+m_fields.size()];
	String[] colNames = new String[1+m_fields.size()];

        colNames[0] = null;
	include[0] = true;

        for (int i = 1; i <= m_fields.size(); i++) {
	   include[i] = true;
	   colNames[i] = new String("_col" + i);
        }

	RecordReader m_rr;
	try{
	   m_rr = m_reader.rowsOptions(new Reader.Options()
                    //.range(0L, Long.MAX_VALUE)
                    .range(0L, Long.MAX_VALUE)
                    .include(include)
                    .searchArgument(sarg, colNames));

	} catch (java.io.IOException e1) {
           return (e1.getMessage());
	}
	
        long startRowNum = m_rr.getRowNumber();
	System.out.println("<<<<<< RecorderReader: " + m_rr + ", startRowNum=" + startRowNum);

	if (m_rr == null)
           return "open:RecordReader is null";

        Object field_val = null;
        StringBuilder row_string = new StringBuilder(1024);
        int ct = 0;
        OrcStruct row = null;

	long lv_count = 0;
	while (m_rr.hasNext() ) {
	    lv_count++;
            row = (OrcStruct) m_rr.next(row);
	}
	System.out.println("Number of rows: " + lv_count);
	/*        while (m_rr.hasNext() ) {
            startRowNum = m_rr.getRowNumber();
            //System.out.println("To read a row with rowNum=" + startRowNum);
            ct++;
            row = (OrcStruct) m_rr.next(row);
            row_string.setLength(0);
            for (int i = 0; i < m_fields.size(); i++) {
                field_val = m_oi.getStructFieldData(row, m_fields.get(i));
                if (field_val != null) {
                    row_string.append(field_val);
                }
                row_string.append('|');
            }
            System.out.println(row_string);
	    }*/

	return null;
    }

    public static void main(String[] args) throws Exception
    {
	boolean lv_print_info = false;
	boolean lv_perform_scans = false;
	boolean lv_measure_scans = false;
	boolean lv_measure_pure_scans = false;
        boolean lv_perform_selective_scans = false;
        boolean lv_perform_vectorized_scans = false;
        boolean lv_measure_vectorized_scans = false;
        boolean lv_measure_pure_vectorized_scans = false;
	boolean lv_done = false;

	int lv_count = 0;

	setupLog4j();

	sv_filename = args[0];

	for (String lv_arg : args) {
	    if (lv_arg.compareTo("-i") == 0) {
		lv_print_info = true;
	    }
	    if (lv_arg.compareTo("-s") == 0) {
		lv_perform_scans = true;
	    }
	    if (lv_arg.compareTo("-ms") == 0) {
		lv_measure_scans = true;
	    }
	    if (lv_arg.compareTo("-mps") == 0) {
		lv_measure_pure_scans = true;
	    }
	    if (lv_arg.compareTo("-ss") == 0) {
		lv_perform_selective_scans = true;
	    }
	    if (lv_arg.compareTo("-vs") == 0) {
		lv_perform_vectorized_scans = true;
	    }
	    if (lv_arg.compareTo("-mvs") == 0) {
		lv_measure_vectorized_scans = true;
	    }
	    if (lv_arg.compareTo("-mpvs") == 0) {
		lv_measure_pure_vectorized_scans = true;
	    }
	}

	System.out.println("OrcFile Reader main");

	sv_ofr = new OrcFileReader();
	sv_ofr.setByteOrder(ByteOrder.BIG_ENDIAN);

	open_read_8th_col();

	if (lv_print_info) {
	    System.out.println("================= Begin File Info:" + 
			       sv_filename);
	
	    sv_ofr.printFileInfo();

	    System.out.println("================= End File Info:" + 
			       sv_filename);
	}

	if (lv_perform_scans) {
	    PerformScan();
	}
	else if (lv_measure_scans) {
	    MeasureScan();
	    open_read_2cols();
	    MeasureScan();
	    open_read_3cols();
	    MeasureScan();
	    open_read_all_cols();
	    MeasureScan();
	}
	else if (lv_measure_pure_scans) {
	    MeasurePureScan();
	    open_read_2cols();
	    MeasurePureScan();
	    open_read_3cols();
	    MeasurePureScan();
	    open_read_all_cols();
	    MeasurePureScan();
	}
	else if (lv_perform_vectorized_scans) {
	    PerformVectorizedScan();
	}
	else if (lv_measure_vectorized_scans) {
	    MeasureVectorizedScan();
	    open_read_2cols();
	    MeasureVectorizedScan();
	    open_read_3cols();
	    MeasureVectorizedScan();
	    open_read_all_cols();
	    MeasureVectorizedScan();
	}
	else if (lv_measure_pure_vectorized_scans) {
	    MeasurePureVectorizedScan();
	    open_read_2cols();
	    MeasurePureVectorizedScan();
	    open_read_3cols();
	    MeasurePureVectorizedScan();
	    open_read_all_cols();
	    MeasurePureVectorizedScan();
	}
        else if ( lv_perform_selective_scans ) {
	    System.out.println(selectiveScanTest(sv_filename));
        }
    }

}
