package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;
import io.esgyn.utils.AmpoolUtils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.esgyn.ipc.AmpoolIPC;

public class CPCheckAndPut {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ESGYN_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.CheckAndPut;

	ParseArgs lv_pa = new ParseArgs("CPCheckAndPut.log");
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
	    AmpoolIPC.CheckAndPutRequest lv_cpr = new AmpoolIPC.CheckAndPutRequest();
	    String lv_name = "Name-" + lv_pa.m_entered_key;
	    EsgynMPut myput1 = new EsgynMPut(lv_pa.m_rk_bytes);
	    myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes(lv_name));
	    myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(new String("Id-" + lv_pa.m_entered_key)));
	    myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(new String("Age-" + lv_pa.m_entered_key)));
	    myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(new String("Salary-" + lv_pa.m_entered_key)));
	    Map<io.ampool.monarch.table.internal.ByteArrayKey,java.lang.Object> lv_cvm = myput1.getColumnValueMap();
	    for (Entry<io.ampool.monarch.table.internal.ByteArrayKey, java.lang.Object> lv_e : lv_cvm.entrySet()) {
		io.ampool.monarch.table.internal.ByteArrayKey lv_e_k = lv_e.getKey();
		if (lv_pa.m_verbose) System.out.println("lv_cfm key: " + new String(lv_e_k.getByteArray())
						   + " lv_cfm val: " + new String((byte[]) lv_e.getValue())
						   + " lv_cfm type of val: " + lv_e.getValue().getClass().getName());
	    }
	    if (lv_pa.m_verbose) System.out.println("lv_cvm size: " + lv_cvm.size());
	    lv_cpr.m_put = myput1; 
	    lv_cpr.m_transaction_id = lv_pa.m_xid;
	    lv_cpr.m_column = Bytes.toBytes("ID");
	    lv_cpr.m_value =  null;
	    lv_cpr.m_row_key = lv_pa.m_rk_bytes;
	    lv_sr.m_request = lv_cpr;

	    if (lv_pa.m_verbose) System.out.println("Going to checkAnddPut on the table: " + lv_pa.m_table_name);
            MExecutionRequest request = new MExecutionRequest();
	    request.setArguments(lv_sr);

	    MTable mtable = clientCache.getTable(lv_pa.m_table_name);
            List<Object> collector = mtable
                    .coprocessorService(ESGYN_COPROCESSOR_CLASS, 
					"cpcall", 
					myput1.getRowKey(),
					request);
	    if (lv_pa.m_verbose) System.out.println("Back from the coprocessor");

	    if (collector != null) {
		System.out.println("Result size: " + collector.size());
		if (collector.size() > 0) {
		    Object lv_rp_obj = collector.get(0);
		    if (lv_pa.m_verbose) System.out.println("CP response object: " + lv_rp_obj);
		    if (lv_rp_obj instanceof AmpoolIPC.CheckAndPutResponse) {
			AmpoolIPC.CheckAndPutResponse lv_rp_cpr = (AmpoolIPC.CheckAndPutResponse) lv_rp_obj;
			System.out.println("CheckAndPut response: " 
					   + " status: " + lv_rp_cpr.m_status
					   + ", has_exception: " + lv_rp_cpr.m_has_exception
					   );
			if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
			    System.out.println("CheckAndPut response exception: "
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
