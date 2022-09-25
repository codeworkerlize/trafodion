package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;
import io.esgyn.ipc.AmpoolIPC;
import io.esgyn.utils.*;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

public class RowGet {

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.GetTransactional;

	int lv_curr_arg = 0;

	ParseArgs lv_pa = new ParseArgs("RowGet.log");
	int lv_retcode = lv_pa.parseArgs(args);
	if (lv_retcode < 0) {
	    System.exit(lv_retcode);
	}

	MClientCache clientCache = CPConnection.createClientCache(lv_pa);
	if (clientCache == null) {
	    System.out.println("Problem connecting to Monarch cache");
	    System.exit(1);
	}

        if (lv_pa.m_verbose) System.out.println("Connection to monarch distributed system is successfully done!");

        long rows = 0L;
        try {

	    String lv_name = "name" + lv_pa.m_rk;
	    Get myget1 = new Get(lv_pa.m_rk_bytes);
	    myget1.addColumn("NAME");
	    myget1.addColumn("ID");
	    myget1.addColumn("AGE");
	    myget1.addColumn("SALARY");

	    if (lv_pa.m_verbose) System.out.println("myget1.toString(): " + myget1);

	    if (lv_pa.m_verbose) System.out.println("Going to perform Get on the table: " + lv_pa.m_table_name);

	    MTable mtable = clientCache.getTable(lv_pa.m_table_name);
	    if (mtable == null) {
		System.err.println("Table: " + lv_pa.m_table_name + " does not exist. Exitting...");
		System.exit(1);
	    }

	    if (lv_pa.m_verbose) {
		AmpoolUtils.printBytes(lv_pa.m_rk_bytes);
	    }
	    
            Row lv_result = mtable.get(myget1);
	    
	    System.out.println("m_result.rowKey"
			       + "(table: " + lv_pa.m_table_name
			       + "): " + new String(lv_result.getRowId()));

	    List<io.ampool.monarch.table.Cell> lv_cells = lv_result.getCells();
	    System.out.println("Number of cells in the result: " + lv_cells.size());

	    if (lv_cells.size() > 0) {
		ListIterator<io.ampool.monarch.table.Cell> lv_ci;
		for (lv_ci = lv_cells.listIterator();
		     lv_ci.hasNext();) {
		    io.ampool.monarch.table.Cell lv_cell = lv_ci.next();
		    System.out.println("col name: " + new String(lv_cell.getColumnName())
				       + ", col value: " + (new String((byte[])lv_cell.getColumnValue())));
		}
	    }
        }
	catch (Exception all_e) {
	    System.out.println("Exception: " + all_e);
	    all_e.printStackTrace();
	}
    }

}
