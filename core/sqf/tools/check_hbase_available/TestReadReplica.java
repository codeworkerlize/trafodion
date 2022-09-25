/**
* @@@ START COPYRIGHT @@@
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
**/

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class TestReadReplica {
    public static void main(String[] args) throws IOException {

	int lv_replica_id = 0;
        System.out.println("#args: " + args.length);
        
        if (args.length > 0) {
            lv_replica_id = Integer.parseInt(args[0]);
            System.out.println("replica id to read from: " + lv_replica_id);
        }
            
        /* The HBaseConfiguration object holds information on where this
       client is going to connect to. It reads configuration 
        file hbase-site.xml. Those files 
	 have to be kept in the classpath (see setenv.bat)
	*/
	
	Configuration config = HBaseConfiguration.create();

        System.out.println("opening the table");
        long ts1 = System.nanoTime();
	HTable table = new HTable(config, "replicated_table");
        long ts2 = System.nanoTime();
        System.out.println("Time(ns) to open table: " + (ts2-ts1));
        System.out.println("Getting the table descriptor");
        ts1 = System.nanoTime();
        HTableDescriptor desc = table.getTableDescriptor();
        ts2 = System.nanoTime();
        System.out.println("Time(ns) to get table descriptor: " + (ts2-ts1));
        int configured_region_replication = desc.getRegionReplication();
        System.out.println("Configured region replication: " + configured_region_replication);

	/* Now retrieve the information stored, using a Get 
       object instance
	*/
    
	Get g = new Get(Bytes.toBytes("rowA1"));
        g.setConsistency(Consistency.TIMELINE);
        if (lv_replica_id >= configured_region_replication) {
            System.out.println("Provided " 
                               + "replica id: " + lv_replica_id 
                               + " is out of the range, 0:"+ (configured_region_replication - 1)
                               + ". Going to use: " + (configured_region_replication - 1)
                               );
            lv_replica_id = configured_region_replication - 1;
        }
        g.setReplicaId(lv_replica_id);

	Result r = table.get(g);
	byte [] value = r.getValue(Bytes.toBytes("cf1"),
				   Bytes.toBytes("colA1"));
	String valueStr = Bytes.toString(value);
	System.out.println("GET: " + valueStr);

	/* Leverage the Scan type to scan the table */

	Scan s = new Scan();
	s.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("colA1"));
	ResultScanner scanner = table.getScanner(s);
	try {
	    for (Result rr : scanner) 
		{
		    System.out.println("Found row: " + rr);
		}

	} finally {
	    scanner.close();
	}
    }
}