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

public class CPDelete {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ESGYN_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.DeleteTransactional;

	ParseArgs lv_pa = new ParseArgs("CPDelete.log");
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
	    AmpoolIPC.DeleteTransactionalRequest lv_cpr = new AmpoolIPC.DeleteTransactionalRequest();
	    String lv_name = "name" + lv_pa.m_rk;
	    //TBD: MDelete is not 'serializable' right now so a workaround for now
	    //     (Not using MDelete or EsgynMDelete
	    byte[] mydelete1 = lv_pa.m_rk_bytes;

	    lv_cpr.m_delete = mydelete1; 
	    lv_cpr.m_transaction_id = lv_pa.m_xid;
	    lv_sr.m_request = lv_cpr;

	    if (lv_pa.m_verbose) System.out.println("Going to Delete on the table: " + lv_pa.m_table_name);
            MExecutionRequest request = new MExecutionRequest();
	    request.setArguments(lv_sr);

	    MTable mtable = clientCache.getTable(lv_pa.m_table_name);
            List<Object> collector = mtable
                    .coprocessorService(ESGYN_COPROCESSOR_CLASS, 
					"cpcall", 
					mydelete1,
					request);
	    if (lv_pa.m_verbose) System.out.println("Back from the coprocessor");

	    if (collector != null) {
		System.out.println("Result size: " + collector.size());
		if (collector.size() > 0) {
		    Object lv_rp_obj = collector.get(0);
		    if (lv_pa.m_verbose) System.out.println("CP response object: " + lv_rp_obj);
		    if (lv_rp_obj instanceof AmpoolIPC.DeleteTransactionalResponse) {
			AmpoolIPC.DeleteTransactionalResponse lv_rp_cpr = (AmpoolIPC.DeleteTransactionalResponse) lv_rp_obj;
			System.out.println("Delete response: " 
					   + " status: " + lv_rp_cpr.m_status
					   + ", has_exception: " + lv_rp_cpr.m_has_exception
					   );
			if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
			    System.out.println("Delete response exception: "
					       + lv_rp_cpr.m_exception);
			    lv_rp_cpr.m_exception.printStackTrace();
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
