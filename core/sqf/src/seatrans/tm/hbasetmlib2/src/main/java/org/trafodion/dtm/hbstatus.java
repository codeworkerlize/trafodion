// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Hewlett-Packard Development Company, L.P.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// @@@ END COPYRIGHT @@@

package org.trafodion.dtm;

import java.io.IOException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.hbase.master.RegionState;

import org.apache.log4j.PropertyConfigurator;

/* 
 Checks the status of the HBase environment

 Usage: $JAVA_HOME/bin/java hbstatus [table name pattern]

 Checks the status of HMaster.
 Gets the number (and info) of the Region Servers.
 Gets the status of the tables (based on the pattern that is passed as a parameter).
 By default, it checks for the status of the Trafodion meta tables [TRAFODION._META_.*]
 
*/

public class hbstatus {

    static {
	System.setProperty("hostName", System.getenv("HOSTNAME"));
    	String confFile = System.getProperty("trafodion.log4j.configFile");
    	if (confFile == null) {
    		System.setProperty("trafodion.tools.log", System.getenv("TRAF_LOG") + "/trafodion.tools.java.${hostName}.log");
    		confFile = System.getenv("TRAF_CONF") + "/log4j.tools.config";
    	}
    	PropertyConfigurator.configure(confFile);
    }

    Admin         m_Admin;
    Configuration m_Config;
    boolean       m_Verbose;
    Connection    m_Connection;
    Configuration adminConf;
    Connection adminConnection;

    public hbstatus(Configuration config) throws Exception {

	this.m_Config = config;
	m_Config.setInt("hbase.client.retries.number", 2);
	m_Config.setInt("hbase.client.pause", 1000);
	m_Config.setInt("zookeeper.session.timeout", 5000);
	m_Config.setInt("zookeeper.recovery.retry", 1);
	m_Config.set("hbase.root.logger","ERROR,console");
	System.out.print("ZooKeeper Quorum: " + m_Config.get("hbase.zookeeper.quorum"));
	System.out.println(", ZooKeeper Port  : " + m_Config.get("hbase.zookeeper.property.clientPort"));

	m_Connection = ConnectionFactory.createConnection(m_Config);

        String rpcTimeout = System.getenv("HAX_ADMIN_RPC_TIMEOUT");
        if (rpcTimeout != null){
           adminConf = new Configuration(m_Config);
           int rpcTimeoutInt = Integer.parseInt(rpcTimeout.trim());
           String value = adminConf.getTrimmed("hbase.rpc.timeout");
           adminConf.set("hbase.rpc.timeout", Integer.toString(rpcTimeoutInt));
           String value2 = adminConf.getTrimmed("hbase.rpc.timeout");
           System.out.println("HAX: ADMIN RPC Timeout, revise hbase.rpc.timeout from " + value + " to " + value2);
           adminConnection = ConnectionFactory.createConnection(adminConf);
        }
        else {
           adminConnection = m_Connection;
        }

	m_Admin = m_Connection.getAdmin();

	m_Verbose = false;
    }

    public void setVerbose(boolean pv_verbose) {
	m_Verbose = pv_verbose;
    }

    public boolean CheckStatus() throws Exception {

	ClusterStatus lv_cs;
	try {
	    lv_cs = m_Admin.getClusterStatus();
	}
    catch (Exception e) {
        System.out.println("Caught an exception trying to check the status of the HBase cluster: " + e);
	    System.out.println("HBase is not available");
        return false;
    }

    System.out.println("\nHBase is available!");
    System.out.println("\nHBase version: " + lv_cs.getHBaseVersion());

	System.out.println("HMaster: " + lv_cs.getMaster());

	Collection<ServerName> lv_csn = lv_cs.getServers();
	System.out.println("\nNumber of RegionServers available:" + lv_csn.size());
	if (lv_csn.size() <=0 ) {
	    System.out.println("Not a single RegionServer is available at the moment. Exitting...");
	    return false;
	}
	int lv_rs_count=0;
	for (ServerName lv_sn : lv_csn) {
	    System.out.println("RegionServer #" + ++lv_rs_count + ": " + lv_sn);
	}

	Collection<ServerName> lv_dsn = lv_cs.getDeadServerNames();
	System.out.println("\nNumber of Dead RegionServers:" + lv_dsn.size());
	lv_rs_count=0;
	for (ServerName lv_sn : lv_dsn) {
	    System.out.println("Dead RegionServer #" + ++lv_rs_count + ": " + lv_sn);
	}

	System.out.println("Number of regions: " + lv_cs.getRegionsCount());
	Map<String, RegionState> lv_map_rit = lv_cs.getRegionsInTransition();
	System.out.println("Number of regions in transition: " + lv_map_rit.size());
	for (Map.Entry<String, RegionState> me: lv_map_rit.entrySet()) {
	    System.out.println("RegionInTransition: " + me.getValue().toDescriptiveString());
	}

	System.out.println("Average load: " + lv_cs.getAverageLoad());

	System.out.println();

	return true;
    }

    public boolean CheckTable(HTableDescriptor pv_htd) throws Exception {

        TableName lv_tblname = pv_htd.getTableName();
        RegionLocator lv_region_locator = m_Connection.getRegionLocator(lv_tblname);
        List<HRegionLocation> lv_list_rl = lv_region_locator.getAllRegionLocations();
        
        System.out.println("========================================================");
        System.out.println("Table:" + pv_htd
                           + "\n#Regions:" + lv_list_rl.size()
                           + ":" + (m_Admin.isTableAvailable(lv_tblname) ? "Available":"Not Available")
                           + ":" + (m_Admin.isTableDisabled(lv_tblname) ? "Disabled":"Enabled")
                           );


        if (!m_Verbose) {
            return true;
        }

        int lv_region_count=0;
	for (HRegionLocation lv_rl: lv_list_rl) {
            System.out.println("----");
            HRegionInfo lv_hri = lv_rl.getRegionInfo();
            System.out.println("Region#" 
                               + ++lv_region_count 
                               + ":" + (lv_hri.isOffline() ? "Offline":"Online")
                               + ":" + lv_rl.getServerName()
                               + "\n" + lv_hri
                               );
	}

	return true;
    }

    public boolean CheckTable(TableName pv_table_name) throws Exception {

        Table lv_table = m_Connection.getTable(pv_table_name);
        HTableDescriptor lv_htd = lv_table.getTableDescriptor();

        return CheckTable(lv_htd);
    }

    public boolean CheckTablePattern(String pv_pattern) throws Exception {

	HTableDescriptor[] la_table_descriptors = m_Admin.listTables(pv_pattern);
	System.out.println("Number of user tables matching the pattern: "
			   + pv_pattern
			   + ":" + la_table_descriptors.length);

	for (HTableDescriptor lv_td: la_table_descriptors) {
	    CheckTable(lv_td);
	}

	return true;
    }

    public boolean CheckMetadata() throws Exception {

	boolean lv_metadata_tables_fine = true;

	System.out.println("Checking the status of Trafodion metadata tables...");
	HTableDescriptor[] la_tables = m_Admin.listTables("TRAF_RSRVD_1.*");
	System.out.println("Number of Trafodion metadata tables: " + la_tables.length);

	for (HTableDescriptor lv_table: la_tables) {
	    boolean lv_table_available = m_Admin.isTableAvailable(lv_table.getTableName());
	    boolean lv_table_disabled = m_Admin.isTableDisabled(lv_table.getTableName());
	    if ( (!lv_table_available) || lv_table_disabled || m_Verbose) {
		if ( (!lv_table_available) || lv_table_disabled ) {
		    lv_metadata_tables_fine = false;
		}
		System.out.println("Table :" + lv_table 
				   + ":" + (lv_table_available ? "Available":"Not Available") 
				   + ":" + (lv_table_disabled ? "Disabled":"Enabled"));
		System.out.println("----");
				       
	    }

	    List<HRegionInfo> lv_lhri = m_Admin.getTableRegions(lv_table.getTableName());
	    for (HRegionInfo lv_hri: lv_lhri) {
		if (lv_hri.isOffline()) {
		    lv_metadata_tables_fine = false;
		    System.out.println( "Table :" + lv_table 
					+ ": Region: " + lv_hri + " is Offline");
		}
	    }
	}

	if (la_tables.length > 0) {
	    System.out.print("Trafodion Metadata Table status:");
	    if (!lv_metadata_tables_fine) {
		System.out.println("Not Good");
		System.out.println("Not all the Trafodion Metadata Tables are online / available / enabled");
	    }
	    else {
		System.out.println("Good");
		System.out.println("All the Trafodion Metadata Tables are online / available / enabled");
	    }
	}

	return lv_metadata_tables_fine;
    }

    static void Usage() {
	
	System.out.println("Usage:");
	System.out.println("hbstatus [ -g <reg expr> | -h | -m | -p <peer id> | -t <table name>] | -v ]");
	System.out.println("-g <reg expr>   : Check the status of the table(s) (if any) that match the provided <reg expr>.");
	System.out.println("-h              : Help (this output).");
	System.out.println("-m              : Check the status of Trafodion metadata tables.");
	System.out.println("-p <peer id>    : Connect to the HBase instance of the peer <peer id> cluster. Defaults to the local HBase instance.");
	System.out.println("-t <table name> : Check the status of the provided table.");
	System.out.println("-v              : Verbose output.");
	System.out.println("                  For the -t, -g, -m options, shows detailed region information (server location,start/end keys).");

    }

    public static void main(String[] Args) {

	String  lv_pattern = new String("");
	String  lv_table   = new String("");
	boolean lv_retcode = true;
	boolean lv_verbose = false;

	boolean lv_checkmetadata = false;
	boolean lv_checkpattern  = false;
	boolean lv_checktable    = false;

	hbstatus lv_hbstatus;

	int     lv_peer_id = 0;
	int     lv_num_params = Args.length;

	int lv_index = 0;
	for (String lv_arg : Args) {
	    lv_index++;
	    if (lv_arg.compareTo("-h") == 0) {
		Usage();
		System.exit(0);
	    }
	    if (lv_arg.compareTo("-v") == 0) {
		lv_verbose = true;
	    }
	    else if (lv_arg.compareTo("-m") == 0) {
		System.out.println("Check metadata");
		lv_checkmetadata = true;
	    }
	    else if (lv_arg.compareTo("-g") == 0) {
		lv_checkpattern = true;
		if (lv_index >= lv_num_params) {
		    System.out.println("Table pattern parameter not provided");
		    hbstatus.Usage();
		    System.exit(1);
		}
		lv_pattern = Args[lv_index];
		System.out.println("Check for the table pattern: " + lv_pattern);
	    }
	    else if (lv_arg.compareTo("-t") == 0) {
		lv_checktable = true;
		if (lv_index >= lv_num_params) {
		    System.out.println("Table name parameter not provided");
		    hbstatus.Usage();
		    System.exit(1);
		}
		lv_table = Args[lv_index];
		System.out.println("Check for the table: " + lv_pattern);
	    }
	    else if (lv_arg.compareTo("-p") == 0) {
		lv_peer_id = Integer.parseInt(Args[lv_index]);
		System.out.println("Check Peer ID: " + lv_peer_id);
	    }

	}

	STRConfig pSTRConfig = null;
	Configuration lv_config = HBaseConfiguration.create();
	if (lv_peer_id > 0) {
	    try {
		pSTRConfig = STRConfig.getInstance(lv_config);
		lv_config = pSTRConfig.getPeerConfiguration(lv_peer_id, false);
		if (lv_config == null) {
		    System.out.println("Peer ID: " + lv_peer_id + " does not exist OR it has not been configured for synchronization.");
		    System.exit(1);
		}
	    } catch (IOException ioe) {
		System.out.println("IO Exception trying to get STRConfig instance: " + ioe);
		System.exit(1);
	    }
	}

	try {
	    lv_hbstatus = new hbstatus(lv_config);
	    lv_hbstatus.setVerbose(lv_verbose);
	    lv_retcode = lv_hbstatus.CheckStatus();
	    if (! lv_retcode) {
		System.exit(1);
	    }
	    if (lv_checkmetadata) {
		System.out.println("Checking metadata...");
		lv_retcode = lv_hbstatus.CheckMetadata();
	    }
	    if (lv_checktable) {
		System.out.println("Checking table: " + lv_table);
		lv_retcode = lv_hbstatus.CheckTable(TableName.valueOf(lv_table));
	    }
	    if (lv_checkpattern) {
		System.out.println("Checking for tables with the pattern: " + lv_pattern);
		lv_retcode = lv_hbstatus.CheckTablePattern(lv_pattern);
	    }
	}
	catch (Exception e)
	    {
		System.out.println("exception: " + e);
		System.exit(1);
	    }

	if (! lv_retcode) {
	    System.exit(1);
	}

	System.exit(0);
    }

}
