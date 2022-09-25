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

public class CPCounter_saved {

    private static final String TABLE_NAME = "EmployeeTable";
    private static final String COL1 = "NAME";
    private static final String COL2 = "ID";
    private static final String COL3 = "AGE";
    private static final String COL4 = "SALARY";

    final static int numBuckets = 113;
    final static int numOfEntries = 1000;

    private static String ROW_COUNT_COPROCESSOR_CLASS = "io.ampool.quickstart.SampleRowCountCoprocessor";

    public static void main(String args[]) {

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.Counter;

	int lv_curr_arg = 0;
	
	System.out.println("lv_sr.m__request_type: " + lv_sr.m_request_type);
	AmpoolIPC.CounterRequest lv_cr = new AmpoolIPC.CounterRequest();
	lv_cr.m_transaction_id = 0;
	lv_sr.m_request = lv_cr;

	String locator_host = "localhost";
        int locator_port = 25510;
        if(args.length > (lv_curr_arg + 2)){
            locator_host = args[lv_curr_arg++];
            locator_port = Integer.parseInt(args[lv_curr_arg++]);
        }

        //Step:1 create a connection with monarch distributed system (DS).
        //create a configuration and connect to monarch locator.
        MConfiguration mconf = MConfiguration.create();
        mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, locator_host);
        mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, locator_port);
        mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "CPCounter_saved.log");
        MClientCache clientCache = new MClientCacheFactory().create(mconf);
        System.out.println("Connection to monarch distributed system is successfully done!");

        long rows = 0L;
        try {
            //execute coprocessor to get row count
            Scan scan = new Scan();
            String startKey = "rowkey-0";
            String stopKey = "rowkey-" + (numOfEntries - 1);
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(stopKey));
            MExecutionRequest request = new MExecutionRequest();
            request.setScanner(scan);
	    request.setArguments(lv_sr);

	    if (! clientCache.getAdmin().tableExists(TABLE_NAME)) {
		System.out.println("Table: " + TABLE_NAME + " does not exist");
		String[] lv_table_names = clientCache.getAdmin().listTableNames();
		System.out.println("List of tables");
		for (String lv_table_name: lv_table_names) {
		    System.out.println("Table: " + lv_table_name);
		}		    
		System.exit(1);
	    }
	    MTable mtable = clientCache.getTable(TABLE_NAME);

	    System.out.println("Before the call to the coprocessor");
            Map<Integer, List<Object>> collector = mtable
                    .coprocessorService(ROW_COUNT_COPROCESSOR_CLASS, "rowCount", scan.getStartRow(), scan.getStopRow(), request);
	    System.out.println("After the call to the coprocessor");

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
	System.out.println("Before exitting");
    }


}
