package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import java.util.List;
import java.util.Map;

public class MTableDelete {

    private static final String TABLE_NAME = "EmployeeTableaaa";

    private static MConfiguration sv_mconf = null;
    private static MClientCache sv_client_cache = null;

    public static void main(String args[]) {

	int lv_curr_arg = 0;
	String lv_table_name = TABLE_NAME;
	if (args.length > lv_curr_arg) {
	    if (args[lv_curr_arg].compareTo("-t") == 0) {
		lv_table_name = args[++lv_curr_arg];
		System.out.println("Arg[" + lv_curr_arg + "]=" + lv_table_name);
	    }
	}

	String locator_host = "localhost";
        int locator_port = 25510;

        if(args.length > (lv_curr_arg + 2)){
            locator_host = args[lv_curr_arg++];
            locator_port = Integer.parseInt(args[lv_curr_arg++]);
        }

        //Step:1 create a connection with monarch distributed system (DS).
        //create a configuration and connect to monarch locator.
        sv_mconf = MConfiguration.create();
        sv_mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, locator_host);
        sv_mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, locator_port);
        sv_mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "MTableDelete.log");
        sv_client_cache = new MClientCacheFactory().create(sv_mconf);

        System.out.println("Connection to monarch distributed system is successfully done!");

        long rows = 0L;
        try {
            deleteTable(lv_table_name);
	    System.out.println("back from deleteTable()");
        } catch (Exception lv_exc_exception) {
	    System.out.println("Exception: " + lv_exc_exception);
	    lv_exc_exception.printStackTrace();
	    System.exit(1);
        }
    }

    private static void deleteTable(String tableName) {

	System.out.println("Enter deleteTable: " + tableName);

        Admin admin = sv_client_cache.getAdmin();
        admin.deleteTable(tableName);

	System.out.println("Deleted table: " + tableName);
	
	return;
    }

}
