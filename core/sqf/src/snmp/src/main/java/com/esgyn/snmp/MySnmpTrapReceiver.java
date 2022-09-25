package com.esgyn.snmp;

// RI import
//
/*
import com.sun.management.snmp.SnmpPduTrap;

// jdmk import
//
import com.sun.management.snmp.agent.SnmpTrap;
        import com.sun.jdmk.internal.ClassLogger;

// java import
//

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.lang.NumberFormatException;

import java.net.SocketException;

        import java.sql.Timestamp;

import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
        import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.zookeeper.KeeperException;


public class MySnmpTrapReceiver extends com.sun.management.snmp.agent.SnmpTrapReceiver {

    static Map<String,String> sv_ipmi_host_map;

    static Set<Integer> sv_specific_code_in_trap_to_cause_rs_deletion = new HashSet<Integer>();

    ZKClient m_zkc;
    DFSAdminClient dfs_admin_client;

    boolean m_any_IPMI_managing_an_RS;
    boolean m_are_all_IPMIs_managing_an_RS;

    // Debug Tag.
    private final static String dbgTag = "MySnmpTrapReceiver";

    public static void populateSpecificCode() 
    {
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Enter populateSpecificCode."
                           );
        
        String lv_trap_specific_code_file_name = ZKClient.m_env_traf_conf + "/" + "specific_codes";
        File lv_file = new File(lv_trap_specific_code_file_name); 
        BufferedReader lv_br = null;
  
        try {
            lv_br = new BufferedReader(new FileReader(lv_file)); 
        }
        catch (FileNotFoundException fnfe) {
            System.err.println("File: " + lv_trap_specific_code_file_name
                               + " not found."
                               + " This file contains the specific codes in SNMP traps that should cause the deletion of the HBase RS ZK Node."
                               + " Please provide the file and start the process again."
                               + " Exiting..."
                               );
            System.exit(1);
        }
  
        String lv_line; 
        int lv_specific_code;
        try {
            while ((lv_line = lv_br.readLine()) != null) {

                if (lv_line.startsWith("#")) {
                    // Comment line - let's continue
                    continue;
                }

                try {
                    lv_specific_code = Integer.parseInt(lv_line);
                    sv_specific_code_in_trap_to_cause_rs_deletion.add(lv_specific_code);
                }
                catch (NumberFormatException nfe) {
                    System.err.println(new Timestamp(System.currentTimeMillis()) 
                                       + ": NumberFormatException while converting: " 
                                       + lv_line
                                       + " to an integer."
                                       );
                }
            }
        }
        catch (IOException ioe) {
            System.err.println(new Timestamp(System.currentTimeMillis()) 
                               + ": IO Exception: " + ioe 
                               + " , while reading the file: " + lv_trap_specific_code_file_name
                               );
        }
        
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Number of entries in the specific_code set: " 
                           + sv_specific_code_in_trap_to_cause_rs_deletion.size()
                           );

        if (sv_specific_code_in_trap_to_cause_rs_deletion.size() < 1) {
            System.err.println(new Timestamp(System.currentTimeMillis()) 
                               + ": File: " + lv_trap_specific_code_file_name
                               + " does not have any specific code."
                               + " This file contains the specific codes in SNMP traps that should cause the deletion of the HBase RS ZK Node."
                               + " Please provide specific codes in the file and start the process again."
                               + " Exiting..."
                               );
            System.exit(1);
        }            
        
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Specific Code Set that should result in HBase RS ZKNode deletion: " 
                           + sv_specific_code_in_trap_to_cause_rs_deletion
                           );
        
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Exit populateSpecificCode."
                           );
        
    }

    public static void read_IPMIMapFile() throws Exception
    {
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Enter read_IPMIMapFile"
                           );

        if (sv_ipmi_host_map == null) {
            sv_ipmi_host_map = new HashMap<String,String>();
        }

        sv_ipmi_host_map.clear();
        
        String lv_ipmi_map_file_name = ZKClient.m_env_traf_conf + "/" + "ipmi_host_map";
        File lv_file = new File(lv_ipmi_map_file_name); 
        BufferedReader lv_br = null;
  
        try {
            lv_br = new BufferedReader(new FileReader(lv_file)); 
        }
        catch (FileNotFoundException fnfe) {
            System.err.println(new Timestamp(System.currentTimeMillis()) 
                               + ": File: " + lv_ipmi_map_file_name
                               + " not found."
                               + " This file contains the mapping between the IPMI IP address and the Linux Host IP address."
                               + " Please provide the file and start the process again."
                               + " Exiting..."
                               );
            System.exit(1);
        }

  
        String lv_line; 
        while ((lv_line = lv_br.readLine()) != null) {
            if (lv_line.startsWith("#")) {
                // Comment line - let's continue
                continue;
            }
            String[] lv_split = lv_line.split(" ");
            int lv_split_entries = lv_split.length;
            if (lv_split_entries == 2) {
                String lv_key = lv_split[0];
                String lv_value = lv_split[1];
                sv_ipmi_host_map.put(lv_key, lv_value);
            }
        }

        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Number of entries in the ipmi to host map: " + sv_ipmi_host_map.size()
                           );

        if (sv_ipmi_host_map.size() < 1) {
            System.err.println(new Timestamp(System.currentTimeMillis()) 
                               + ": File: " + lv_ipmi_map_file_name
                               + " does not have any mapping."
                               + " This file contains the mapping between the IPMI IP address and the Linux Host IP address."
                               + " Please provide the file and start the process again."
                               + " Exiting..."
                               );
            System.exit(1);
        }

        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": IPMI IP to Host IP map: " 
                           + sv_ipmi_host_map
                           );

        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Exit read_IPMIMapFile"
                           );
        
    } 
    
    public MySnmpTrapReceiver(int port, ZKClient p_zkc) throws KeeperException {
       super(null,null,port,null);
       //       receiveAsGeneric(true); (whether to receive SNMP traps as generic?)

       System.out.println(new Timestamp(System.currentTimeMillis()) 
                          + ": MySnmpTrapReceiver ctor - Enter"
                          );

       populateSpecificCode();

       m_zkc = p_zkc;
       if (m_zkc == null) {
           System.err.println(new Timestamp(System.currentTimeMillis()) 
                              + ": MySnmpTrapReceiver - ERROR - the passed in parameter to talk to ZK is null. Exiting..."
                              );
           System.exit(1);
       }

       Map<String,String> lv_rs_map_zk = m_zkc.mapRSNodesFromZK();

       m_any_IPMI_managing_an_RS = Is_Any_IPMI_Managing_An_RS(lv_rs_map_zk);

       m_are_all_IPMIs_managing_an_RS = Are_All_IPMIs_Managing_An_RS(lv_rs_map_zk);

       System.out.println(new Timestamp(System.currentTimeMillis()) 
                          + ": MySnmpTrapReceiver ctor - Exit"
                          );
    }
    public MySnmpTrapReceiver(int port, ZKClient p_zkc, DFSAdminClient dfs_admin) throws KeeperException {
        super(null, null, port, null);
        //       receiveAsGeneric(true); (whether to receive SNMP traps as generic?)

        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": MySnmpTrapReceiver ctor - Enter"
        );

        populateSpecificCode();

        m_zkc = p_zkc;
        this.dfs_admin_client = dfs_admin;
        if (this.dfs_admin_client == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": MySnmpTrapReceiver - ERROR - the passed remove datanode failure..."
            );
            System.exit(1);
        }
        if (m_zkc == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": MySnmpTrapReceiver - ERROR - the passed in parameter to talk to ZK is null. Exiting..."
            );
            System.exit(1);
        }

        Map<String, String> lv_rs_map_zk = m_zkc.mapRSNodesFromZK();

        m_any_IPMI_managing_an_RS = Is_Any_IPMI_Managing_An_RS(lv_rs_map_zk);

        m_are_all_IPMIs_managing_an_RS = Are_All_IPMIs_Managing_An_RS(lv_rs_map_zk);

        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": MySnmpTrapReceiver ctor - Exit"
        );
    }

    boolean Is_Any_IPMI_Managing_An_RS(Map<String,String> p_rs_map) {

        if (p_rs_map == null) {
            System.err.println(new Timestamp(System.currentTimeMillis()) 
                               + ": Is_Any_IPMI_Managing_An_RS - ERROR: input parameter is null. Returning."
                               );
            return false;
        }
            
        boolean lv_exists = false;
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Enter - Any IPMI Managing an RS?"
                           );

        for (Entry<String,String> lv_entry : sv_ipmi_host_map.entrySet()) {
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                               + ": IPMI IP Address: " + lv_entry.getKey()
                               + ", Linux Host IP Address: " + lv_entry.getValue()
                               );
            lv_exists = p_rs_map.containsKey(lv_entry.getValue());
            if (lv_exists) {
                break;
            }
        }
        
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Exit - Any IPMI Managing an RS?, return value: " + lv_exists
                           );
        return lv_exists;
    }

    boolean Are_All_IPMIs_Managing_An_RS(Map<String,String> p_rs_map) {
        boolean lv_exists = false;

        if (p_rs_map == null) {
            System.err.println(new Timestamp(System.currentTimeMillis()) 
                               + ": Are_All_IPMIs_Managing_An_RS - ERROR: input parameter is null. Returning."
                               );
            return false;
        }
            
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Enter - Are All IPMIs Managing an RS?"
                           );

        for (Entry<String,String> lv_entry : sv_ipmi_host_map.entrySet()) {
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                               + ": IPMI IP Address: " + lv_entry.getKey()
                               + ", Linux Host IP Address: " + lv_entry.getValue()
                               );
            lv_exists = p_rs_map.containsKey(lv_entry.getValue());
            if ( ! lv_exists) {
                break;
            }
        }
        
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Exit - Are All IPMIs Managing an RS?, return value: " + lv_exists
                           );
        return lv_exists;

    }

    protected synchronized void receivedTrap(SnmpTrap trap) {
        logger.info("receivedTrap", "Received a generic trap: " + trap);
    }

    protected synchronized void logReceivedV1Trap(SnmpPduTrap trap) {
        String lv_agentAddr = trap.agentAddr.toString();
        logger.info("receivedV1Trap", "Received a V1 trap"
                    + ", from: " + lv_agentAddr
                    + ", generic code: " + trap.genericTrap
                    + ", specific code: " + trap.specificTrap
                    + ", timestamp: " + trap.timeStamp
                    );

        for(int i = 0; i < trap.varBindList.length; i++)
            logger.info("logReceivedV1Trap", "oid: " + 
                        trap.varBindList[i].getOid() + ", val: " + 
                        trap.varBindList[i].getSnmpValue() + "\n");


    }

    boolean isThisTheTrapToCauseTheDeletionOfHBaseZKNode(SnmpPduTrap pv_trap) {

        int lv_specific_trap = pv_trap.specificTrap;
        if (sv_specific_code_in_trap_to_cause_rs_deletion.contains(lv_specific_trap)) {
            return true;
        }

        return false;
    }

    protected synchronized void receivedV1Trap(SnmpPduTrap trap) {
        String lv_agentAddr = trap.agentAddr.toString();

        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": receivedV1Trap"
                           + ", Agent: " + lv_agentAddr
                           + ", generic code: " + trap.genericTrap
                           + ", specific code: " + trap.specificTrap
                           + ", timestamp: " + trap.timeStamp
                           );

        for(int i = 0; i < trap.varBindList.length; i++)
            System.out.println("oid: " + trap.varBindList[i].getOid() 
                               + ", val: " + trap.varBindList[i].getSnmpValue()
                               );

        String lv_host = sv_ipmi_host_map.get(lv_agentAddr);
        
        if (lv_host == null) {
            System.err.println(new Timestamp(System.currentTimeMillis()) 
                               + ": receivedV1Trap - ERROR - Could not find the host machine associated with the IPMI agent: " 
                               + lv_agentAddr
                               );
            return;
        }

        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": receivedV1Trap"
                           + ", Agent: " + lv_agentAddr 
                           + ", Linux Host: " + lv_host
                           );

        if (isThisTheTrapToCauseTheDeletionOfHBaseZKNode(trap)) {
            // shutdown datanode
            System.out.println(new Timestamp(System.currentTimeMillis())
                    + ": Going to delete the DataNode on the host: " + lv_host
            );
            boolean sucess = dfs_admin_client.removeBadNode(lv_host);
            if (sucess) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + ": Going to delete the DataNode on the host: " + lv_host + " sucess"
                );
            } else {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + ": Going to delete the DataNode on the host: " + lv_host + " failure"
                );
            }
            // delete Primary Meta in zk
            if (m_zkc.isDeleteMeta) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + ": Going to delete the HBase Meta ZKNode for the RS on the host: " + lv_host
                );
                m_zkc.deleteMetaLocation(lv_host,HRegionInfo.DEFAULT_REPLICA_ID);
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + "delete the HBase Meta ZKNode compelete."
                );
            } else {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + ": Not Execute the HBase Meta ZKNode for the RS on the host: " + lv_host
                );
            }
            System.out.println( new Timestamp(System.currentTimeMillis()) 
                                + ": Going to delete the HBase ZKNode for the RS on the host: " + lv_host
                                );
            m_zkc.deleteZKNode(lv_host);
            System.out.println( new Timestamp(System.currentTimeMillis()) 
                                + ": Let's print the ZK Nodes for the HBase RS."
                                );
            m_zkc.printRSNodes();
        }
        else {
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                               + ": receivedV1Trap"
                               + ", No further action for the trap with the specific code: " + trap.specificTrap
                               );
        }
        
        System.out.println( new Timestamp(System.currentTimeMillis()) 
                            + ": receivedV1Trap Exit."
                            );
        return;
    }

    public static void main(String[] Args) throws Exception 
    {
        System.out.println("==========================================================");
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Starting the SNMP Trap Receiver"
                           );
        System.out.println("==========================================================");

        int lv_snmp_trap_receiver_port = 162;
        boolean startuserOnrack = false;
        boolean startMeta = true;
        if (Args.length > 0) {
            lv_snmp_trap_receiver_port = Integer.parseInt(Args[0]);
            for (int i = 1; i < Args.length; i++) {
                if (DFSAdminClient.PARAMETER.equals(Args[i])) {
                    System.out.println("start only remove racks");
                    startuserOnrack = true;
                }
                if (ZKClient.startMeta.equals(Args[i])) {
                    startMeta = false;
                }
            }
        }


	Configuration sv_config = HBaseConfiguration.create();

        ZKClient lv_zkc = new ZKClient(sv_config);
        lv_zkc.isDeleteMeta = startMeta;
        DFSAdminClient dfsAdminClient = new DFSAdminClient(sv_config);
        dfsAdminClient.setStartOnlyRack(startuserOnrack);
        lv_zkc.printRSNodes();

        read_IPMIMapFile();

        if (Args.length > 0) {
            lv_snmp_trap_receiver_port = Integer.parseInt(Args[0]);
        }

        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": port to listen for SNMP traps: " + lv_snmp_trap_receiver_port
                           );

        MySnmpTrapReceiver lv_tr = new MySnmpTrapReceiver(lv_snmp_trap_receiver_port, lv_zkc, dfsAdminClient);
        
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                           + ": Waiting for SNMP traps"
                           );

        try {
            lv_tr.start();
        }
        catch (SocketException my_se) {
            System.out.println("Exception in start() in MySnmpTrapReceiver " + my_se);
        }
    }

    private static final ClassLogger logger = 
	new ClassLogger(ClassLogger.LOGGER_SNMP,dbgTag);

}
*/