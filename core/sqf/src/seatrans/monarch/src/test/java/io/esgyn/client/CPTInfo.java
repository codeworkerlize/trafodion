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

public class CPTInfo {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ESGYN_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.GetTransactional;

	ParseArgs lv_pa = new ParseArgs("CPTInfo.log");
	lv_pa.setValidate(false);
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

	    if (lv_pa.m_verbose) System.out.println("Going to Get Location Info of the table: " + lv_pa.m_table_name);

	    MTable mtable = clientCache.getTable(lv_pa.m_table_name);
	    if (mtable == null) {
		System.err.println("Table: " + lv_pa.m_table_name + " does not exist. Exitting...");
		System.exit(1);
	    }

	    AmpoolUtils.printMTableLocation(mtable);
	    
	    System.out.println("table descriptor: " + mtable.getTableDescriptor());

        }
	catch (MCoprocessorException cce) {
	    System.out.println("MCoprocessorException: " + cce);
	    cce.printStackTrace();
        }
	catch (Exception all_e) {
	    System.out.println("Exception: " + all_e);
	    all_e.printStackTrace();
	}
    }

}
