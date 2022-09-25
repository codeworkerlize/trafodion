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

public class CPTransactionList {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ESGYN_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.GetTransactionList;

	ParseArgs lv_pa = new ParseArgs("CPTransactionList.log");
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
	    if (mtable == null) {
		System.err.println("Table: " + lv_pa.m_table_name + " does not exist. Exitting...");
		System.exit(1);
	    }

	    if (lv_pa.m_verbose) System.out.println("Counting the number of active transactions on the table: " + lv_pa.m_table_name);
	    java.util.Map<java.lang.Integer,java.util.List<java.lang.Object>> collector = mtable
		.coprocessorService(ESGYN_COPROCESSOR_CLASS, 
				    "cpcall", 
				    null,
				    null,
				    request);
	    if (lv_pa.m_verbose) System.out.println("Back from the coprocessor!");
	    if (collector != null) {
		if (lv_pa.m_verbose) System.out.println("Result size: " + collector.size());
		for (Entry<java.lang.Integer,java.util.List<java.lang.Object>> lv_e : collector.entrySet()) {
		    if (lv_pa.m_verbose) System.out.println("Key: " + lv_e.getKey()
							    + ", Value: " + lv_e.getValue());
		    for (java.lang.Object lv_value : lv_e.getValue()) {
			AmpoolIPC.GetTransactionListResponse lv_rp = (AmpoolIPC.GetTransactionListResponse) lv_value;
			
			if (lv_rp.m_num_transactions <= 0) continue;
		    
			System.out.println("Entry: " + lv_e.getKey()
					   + ", status: " + lv_rp.m_status
					   + ", has_exception: " + lv_rp.m_has_exception
					   + ", num_transactions: " + lv_rp.m_num_transactions
					   + ", num_entries in the list: " + (lv_rp.m_txid_list != null ? lv_rp.m_txid_list.size() : 0)
					   );

			if (lv_rp.m_has_exception && (lv_rp.m_exception != null)) {
			    System.out.println("GetTransactionList response exception: "
					       + lv_rp.m_exception);
			    lv_rp.m_exception.printStackTrace();
			    continue;
			}

			if (lv_rp.m_txid_list == null) continue;
 
			for (int i = 0 ; i < lv_rp.m_txid_list.size(); i++) {
			    System.out.println("txid["+ i + "]=" + lv_rp.m_txid_list.get(i));
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
	catch (Throwable exc_throwable) {
	    System.out.println("Throwable : " + exc_throwable);
	    exc_throwable.printStackTrace();
        }
    }


}
