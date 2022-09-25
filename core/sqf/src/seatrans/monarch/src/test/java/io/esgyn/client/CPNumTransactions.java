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

public class CPNumTransactions {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ESGYN_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.GetTransactionCount;

	ParseArgs lv_pa = new ParseArgs("CPNumTransactions.log");
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
	    if (lv_pa.m_verbose) System.out.println("Counting the number of active transactions on the table: " + lv_pa.m_table_name);
	    java.util.Map<java.lang.Integer,java.util.List<java.lang.Object>> collector = mtable
		.coprocessorService(ESGYN_COPROCESSOR_CLASS, 
				    "cpcall", 
				    null,
				    null,
				    request);
	    if (lv_pa.m_verbose) System.out.println("Back from the coprocessor");
	    if (collector != null) {
		if (lv_pa.m_verbose) System.out.println("Result size: " + collector.size());
		for (Entry<java.lang.Integer,java.util.List<java.lang.Object>> lv_e : collector.entrySet()) {
		    for (java.lang.Object lv_value : lv_e.getValue()) {
			AmpoolIPC.GetTransactionCountResponse lv_rp = (AmpoolIPC.GetTransactionCountResponse) lv_value;
		    
			System.out.println("Entry: " + lv_e.getKey()
					   + ", status: " + lv_rp.m_status
					   + ", has_exception: " + lv_rp.m_has_exception
					   + ", value: " + lv_rp.m_num_transactions);

			if (lv_rp.m_has_exception && (lv_rp.m_exception != null)) {
			    System.out.println("GetTransactionList response exception: "
					       + lv_rp.m_exception);
			    lv_rp.m_exception.printStackTrace();
			}
		    }
		}
	    }
	}
	catch (MCoprocessorException cce) {
	    System.out.println("MCoprocessorException : " + cce);
	    cce.printStackTrace();
        }
	catch (Exception exc_generic) {
	    System.out.println("Exception : " + exc_generic);
	    exc_generic.printStackTrace();
        }
    }


}
