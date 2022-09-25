package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.exceptions.MTableExistsException;

import java.util.List;
import java.util.Map;

public class MTableCPSetup {

    private static final String TABLE_NAME = "EmployeeTable";
    private static final String COL1 = "NAME";
    private static final String COL2 = "ID";
    private static final String COL3 = "AGE";
    private static final String COL4 = "SALARY";

    final static int numBuckets = 8;

    public static void main(String args[]) {

	ParseArgs lv_pa = new ParseArgs("MTableCPSetup.log");
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

        System.out.println("Connection to monarch distributed system is successfully done!");

        long rows = 0L;
        try {
            MTable mtable = createTable(lv_pa.m_table_name);
	    System.out.println("Done creating table:" + lv_pa.m_table_name);
	    if (lv_pa.m_load) {
		insertRows(mtable, lv_pa);
		System.out.println("Done insertRows table:" + lv_pa.m_table_name);
	    }
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
	MTable mtable = null;
	try {
	    mtable = admin.createTable(tableName, tableDescriptor);
	}
	catch (MTableExistsException e_tee) {
	    System.out.println("Table: " + tableName + " already exists");
	    mtable = clientCache.getTable(tableName);
	}
					      
        //assertEquals(mtable.getName(), tableName);
        return mtable;
    }

    private static void insertRows(MTable mtable, ParseArgs pv_pa) {
        for (int keyIndex = 0; keyIndex < pv_pa.m_num_entries; keyIndex++) {
            Put myput1 = new Put(Bytes.toBytes(pv_pa.m_prefix + keyIndex));
            myput1.addColumn(Bytes.toBytes(COL1), Bytes.toBytes("Name-" + keyIndex));
            myput1.addColumn(Bytes.toBytes(COL2), Bytes.toBytes(new String("Id-" + keyIndex)));
            myput1.addColumn(Bytes.toBytes(COL3), Bytes.toBytes(new String("Age-" + keyIndex)));
            myput1.addColumn(Bytes.toBytes(COL4), Bytes.toBytes(new String("Salary-" + keyIndex)));
            mtable.put(myput1);
        }
    }

}
