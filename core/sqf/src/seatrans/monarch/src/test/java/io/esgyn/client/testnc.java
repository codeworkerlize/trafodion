package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import java.util.List;
import java.util.Map;

public class testnc {

    private static final String TABLE_NAME = "EmployeeTable";

    private static String COPROCESSOR_CLASS = "io.esgyn.coprocessor.NumCallsCoprocessor";

    public static void main(String args[]) {

	String lv_table_name = TABLE_NAME;

	if (args.length > 0) {
	    lv_table_name = TABLE_NAME + "2";
	}

	String locator_host = "localhost";
        int locator_port = 25510;

        //Step:1 create a connection with monarch distributed system (DS).
        //create a configuration and connect to monarch locator.
        MConfiguration mconf = MConfiguration.create();
        mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, locator_host);
        mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, locator_port);
        mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "testnc.log");
        MClientCache clientCache = new MClientCacheFactory().create(mconf);
        System.out.println("Connection to monarch distributed system is successfully done!");

        try {
            MExecutionRequest request = new MExecutionRequest();

	    MTable mtable = clientCache.getTable(lv_table_name);
	    if (mtable != null) {
		System.out.println("Table Name: " + mtable.getName());
	    }

            List<Object> collector = mtable
		.coprocessorService(COPROCESSOR_CLASS, 
				    "numCalls", 
				    Bytes.toBytes("rowkey-0"),
				    request);

	    if (collector != null) {
		System.out.println("Number of entries in the result collector: " + collector.size());
		for (Object lv_num_calls : collector) {
		    System.out.println("NumCalls: " + (long) lv_num_calls);
		}
	    }
	}
	catch (MCoprocessorException cce) {
	    System.out.println("Coprocessor Exception: " + cce);
	    cce.printStackTrace();
        }
    }


}
