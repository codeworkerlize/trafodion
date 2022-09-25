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
import io.esgyn.utils.AmpoolUtils;

public class CPCommit {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ESGYN_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.CommitTransaction;

	ParseArgs lv_pa = new ParseArgs("CPCommit.log");
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
	    AmpoolIPC.CommitRequest lv_cpr = new AmpoolIPC.CommitRequest();
	    lv_cpr.m_transaction_id = lv_pa.m_xid;
	    lv_cpr.m_participant_num = 1;
	    lv_sr.m_request = lv_cpr;

	    if (lv_pa.m_verbose) System.out.println("Going to Commit Transaction"
			       + " table: " + lv_pa.m_table_name
			       + ", txid: " + lv_cpr.m_transaction_id);
            MExecutionRequest request = new MExecutionRequest();
	    request.setArguments(lv_sr);

	    MTable mtable = clientCache.getTable(lv_pa.m_table_name);
	    if (mtable == null) {
		System.err.println("Table: " + lv_pa.m_table_name + " does not exist. Exitting...");
		System.exit(1);
	    }

	    if (lv_pa.m_rk == null) {
		lv_pa.m_rk_bytes = AmpoolUtils.getStartRowKey(mtable, lv_pa.m_bucket_id);
	    }
	    
	    if (lv_pa.m_verbose) {
		AmpoolUtils.printBytes(lv_pa.m_rk_bytes);
	    }
	    
            List<Object> collector = mtable
                    .coprocessorService(ESGYN_COPROCESSOR_CLASS, 
					"cpcall", 
					lv_pa.m_rk_bytes,
					request);
	    if (lv_pa.m_verbose) System.out.println("Back from the coprocessor");

	    if (collector != null) {
		if (lv_pa.m_verbose) System.out.println("Result size: " + collector.size());
		if (collector.size() > 0) {
		    Object lv_rp_obj = collector.get(0);
		    if (lv_pa.m_verbose) System.out.println("CP response object: " + lv_rp_obj);
		    if (lv_rp_obj instanceof AmpoolIPC.CommitResponse) {
			AmpoolIPC.CommitResponse lv_rp_cpr = (AmpoolIPC.CommitResponse) lv_rp_obj;
			System.out.println("Commit response: " 
					   + " status: " + lv_rp_cpr.m_status
					   + ", has_exception: " + lv_rp_cpr.m_has_exception
					   );
			if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
			    System.out.println("Commit response exception: "
					       + lv_rp_cpr.m_exception);
			    lv_rp_cpr.m_exception.printStackTrace();
			}
			else {
			    System.out.println("Commit request status code: " + lv_rp_cpr.m_status_code
					       );
			}
		    }
		}
	    }

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
