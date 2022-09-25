package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import java.util.List;
import java.util.Map;

import io.esgyn.ipc.AmpoolIPC;

public class CPNumCalls {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ROW_COUNT_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.NumCPCalls;

	ParseArgs lv_pa = new ParseArgs("CPNumCalls.log");
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
            MExecutionRequest request = new MExecutionRequest();
	    request.setArguments(lv_sr);

	    MTable mtable = clientCache.getTable(lv_pa.m_table_name);
	    if (mtable != null) {
		if (lv_pa.m_verbose) System.out.println("Table Name: " + mtable.getName());
	    }
            List<Object> collector = mtable
		.coprocessorService(ROW_COUNT_COPROCESSOR_CLASS, 
				    "cpcall", 
				    Bytes.toBytes("rowkey-0"),
				    request);

	    if (collector != null) {
		if (lv_pa.m_verbose) System.out.println("Result size: " + collector.size());
		if (collector.size() > 0) {
		    Object lv_rp_obj = collector.get(0);
		    System.out.println("Number of Coprocessor calls: " + (long) lv_rp_obj);
		}
	    }
            if (lv_pa.m_verbose) System.out.println("Number of entries in the result collector: " + collector.size());

        } catch (MCoprocessorException cce) {
	    System.out.println("MCoprocessorException: " + cce);
	    cce.printStackTrace();
        }
	catch (Exception lv_exc_exception) {
	    System.out.println("Exception: " + lv_exc_exception);
	    lv_exc_exception.printStackTrace();
	    System.exit(1);
        }
    }


}
