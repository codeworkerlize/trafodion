package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import java.util.List;
import java.util.Map;


/**
 * Created by nilkanth on 2/2/16.
 */
public class MTableCPSetup2 {

    private static final String TABLE_NAME = "EmployeeTable2";
    private static final String COL1 = "NAME";
    private static final String COL2 = "ID";
    private static final String COL3 = "AGE";
    private static final String COL4 = "SALARY";

    final static int numBuckets = 113;
    final static int numOfEntries = 10;

    public static void main(String args[]) {

	int lv_curr_arg = 0;
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
        mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "MTableCPSetup2.log");
        MClientCache clientCache = new MClientCacheFactory().create(mconf);
        System.out.println("Connection to monarch distributed system is successfully done!");

        long rows = 0L;
        try {
            MTable mtable = createTable(TABLE_NAME);
	    System.out.println("Done creating table:" + TABLE_NAME);
            insertRows(mtable);
	    System.out.println("Done insertRows table:" + TABLE_NAME);
        }
	catch (Exception lv_exc_exception) {
	    System.out.println("Exception: " + lv_exc_exception);
	    lv_exc_exception.printStackTrace();
	    System.exit(1);
        }
    }

    private static MTable createTable(String tableName) {
        MTableDescriptor tableDescriptor = new MTableDescriptor();

        tableDescriptor.addColumn(Bytes.toBytes(COL1)).
                addColumn(Bytes.toBytes(COL2)).
                addColumn(Bytes.toBytes(COL3)).
                addColumn(Bytes.toBytes(COL4));

        tableDescriptor.setRedundantCopies(1);
        tableDescriptor.setTotalNumOfSplits(numBuckets);

        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        Admin admin = clientCache.getAdmin();
        MTable mtable = admin.createTable(tableName, tableDescriptor);
        //assertEquals(mtable.getName(), tableName);
        return mtable;
    }

    private static void insertRows(MTable mtable) {
        for (int keyIndex = 0; keyIndex < numOfEntries; keyIndex++) {
            Put myput1 = new Put(Bytes.toBytes("rowkey-" + keyIndex));
            myput1.addColumn(Bytes.toBytes(COL1), Bytes.toBytes("col" + keyIndex));
            myput1.addColumn(Bytes.toBytes(COL2), Bytes.toBytes(keyIndex + 10));
            myput1.addColumn(Bytes.toBytes(COL3), Bytes.toBytes(keyIndex + 10));
            myput1.addColumn(Bytes.toBytes(COL4), Bytes.toBytes(keyIndex + 10));
            mtable.put(myput1);
        }
    }

}
