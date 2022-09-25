package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.esgyn.ipc.AmpoolIPC;

public class RowDelete {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ESGYN_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.DeleteTransactional;

	ParseArgs lv_pa = new ParseArgs("RowDelete.log");
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
	    Delete mydelete1 = new Delete(lv_pa.m_rk_bytes);

	    if (lv_pa.m_verbose) System.out.println("Going to call delete on the table: " + lv_pa.m_table_name);

	    MTable mtable = clientCache.getTable(lv_pa.m_table_name);
	    mtable.delete(mydelete1);
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
