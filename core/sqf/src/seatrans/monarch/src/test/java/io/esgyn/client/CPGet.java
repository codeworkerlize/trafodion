package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import io.esgyn.ipc.AmpoolIPC;

public class CPGet {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ESGYN_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.GetTransactional;

	int lv_curr_arg = 0;

	ParseArgs lv_pa = new ParseArgs("CPGet.log");
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
	    AmpoolIPC.GetTransactionalRequest lv_cpr = new AmpoolIPC.GetTransactionalRequest();
	    String lv_name = "name" + lv_pa.m_rk;
	    EsgynMGet myget1 = new EsgynMGet(lv_pa.m_rk_bytes);
	    myget1.addColumn("NAME");
	    myget1.addColumn("ID");
	    myget1.addColumn("AGE");
	    myget1.addColumn("SALARY");
	    List<byte[]> lv_cnm = myget1.getColumnNameList();
	    ListIterator<byte[]> lv_it = null;
	    if (lv_pa.m_verbose) System.out.println("lv_cnm size: " + lv_cnm.size());
	    for (lv_it = lv_cnm.listIterator();
		 lv_it.hasNext();) {
		byte[] lv_column_name = lv_it.next();
		if (lv_pa.m_verbose) System.out.println("lv_cnm key: " + new String(lv_column_name));
	    }

	    if (lv_pa.m_verbose) System.out.println("myget1.toString(): " + myget1);

	    lv_cpr.m_get = myget1; 
	    lv_cpr.m_transaction_id = lv_pa.m_xid;
	    lv_sr.m_request = lv_cpr;

	    if (lv_pa.m_verbose) System.out.println("Going to Get on the table: " + lv_pa.m_table_name);
            MExecutionRequest request = new MExecutionRequest();
	    request.setArguments(lv_sr);

	    MTable mtable = clientCache.getTable(lv_pa.m_table_name);
            List<Object> collector = mtable
                    .coprocessorService(ESGYN_COPROCESSOR_CLASS, 
					"cpcall", 
					myget1.getRowKey(),
					request);
	    if (lv_pa.m_verbose) System.out.println("Back from the coprocessor");

	    if (collector != null) {
		System.out.println("Result size: " + collector.size());
		if (collector.size() > 0) {
		    Object lv_rp_obj = collector.get(0);
		    if (lv_pa.m_verbose) System.out.println("CP response object: " + lv_rp_obj);
		    if (lv_rp_obj instanceof AmpoolIPC.GetTransactionalResponse) {
			AmpoolIPC.GetTransactionalResponse lv_rp_cpr = (AmpoolIPC.GetTransactionalResponse) lv_rp_obj;
			System.out.println("Get response: " 
					   + " status: " + lv_rp_cpr.m_status
					   + ", has_exception: " + lv_rp_cpr.m_has_exception
					   + ", m_result: " + lv_rp_cpr.m_result
					   );
			if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
			    System.out.println("Get response exception: "
					       + lv_rp_cpr.m_exception);
			    lv_rp_cpr.m_exception.printStackTrace();
			}
			else {
			    System.out.println("m_result.rowKey: " + new String(lv_rp_cpr.m_result.getRowId()));
			    List<io.ampool.monarch.table.Cell> lv_cells = lv_rp_cpr.m_result.getCells();
			    ListIterator<io.ampool.monarch.table.Cell> lv_ci;
			    for (lv_ci = lv_cells.listIterator();
				 lv_ci.hasNext();) {
				io.ampool.monarch.table.Cell lv_cell = lv_ci.next();
				System.out.println("col name: " + new String(lv_cell.getColumnName())
						   + ", col value: " + (new String((byte[])lv_cell.getColumnValue())));
			    }
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
