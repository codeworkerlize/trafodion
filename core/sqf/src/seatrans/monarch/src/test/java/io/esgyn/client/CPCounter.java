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

public class CPCounter {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String ROW_COUNT_COPROCESSOR_CLASS = "io.esgyn.coprocessor.AmpoolTransactions";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.Counter;

	ParseArgs lv_pa = new ParseArgs("CPCounter.log");
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

	if (lv_pa.m_verbose) System.out.println("lv_sr.m_request_type: " + lv_sr.m_request_type);
	AmpoolIPC.CounterRequest lv_cr = new AmpoolIPC.CounterRequest();
	lv_cr.m_transaction_id = 0;
	lv_sr.m_request = lv_cr;


        long rows = 0L;
        try {
            //execute coprocessor to get row count
            Scan scan = new Scan();
	    /** If you want to count a range of rows
		String startKey = "rowkey-0";
		String stopKey = "rowkey-" + 2000;
		scan.setStartRow(Bytes.toBytes(startKey));
		scan.setStopRow(Bytes.toBytes(stopKey));
	    **/
            MExecutionRequest request = new MExecutionRequest();
            request.setScanner(scan);
	    request.setArguments(lv_sr);

	    if (! clientCache.getAdmin().tableExists(lv_pa.m_table_name)) {
		System.out.println("Table: " + lv_pa.m_table_name + " does not exist");
		String[] lv_table_names = clientCache.getAdmin().listTableNames();
		System.out.println("List of tables");
		for (String table_name: lv_table_names) {
		    System.out.println("Table: " + table_name);
		}		    
		System.exit(1);
	    }
	    MTable mtable = clientCache.getTable(lv_pa.m_table_name);

	    if (lv_pa.m_verbose) System.out.println("Counting the number of rows in the table: " + lv_pa.m_table_name);
	    if (lv_pa.m_verbose) System.out.println("Going to call the coprocessor");
            Map<Integer, List<Object>> collector = mtable
		/* if counting the number of rows withing a start/stop key pair
		   .coprocessorService(ROW_COUNT_COPROCESSOR_CLASS, "cpcall", scan.getStartRow(), scan.getStopRow(), request);
		*/
		.coprocessorService(ROW_COUNT_COPROCESSOR_CLASS, 
				    "cpcall", 
				    null,
				    null,
				    request);

	    if (lv_pa.m_verbose) System.out.println("Back from the coprocessor");

            rows = collector.values()
		.stream()
		.mapToLong(value -> value
			   .stream()
			   .map(val -> (Long) val)
			   .reduce(0L, (prev, current) -> prev + current))
		.sum();

            System.out.println("Row count: " + rows);

        } 
	catch (MCoprocessorException cce) {
	    System.out.println("MCoprocessorException: " + cce);
	    cce.printStackTrace();
        }
	catch (Exception all_e) {
	    System.out.println("Exception: " + all_e);
	    all_e.printStackTrace();
	}
	if (lv_pa.m_verbose) System.out.println("Before exitting");
    }


}
