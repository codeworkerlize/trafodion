package com.esgyn.snmp;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.util.Tool;
import org.apache.zookeeper.KeeperException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;
import java.sql.Timestamp;
import java.util.Set;


public class PingReceiver {

    ZKClient m_zkc;
    DFSAdminClient dfs_admin_client;
    int retcode;
    String badName;

    public PingReceiver(ZKClient p_zkc, DFSAdminClient dfs_admin) throws KeeperException {
        //       receiveAsGeneric(true); (whether to receive SNMP traps as generic?)

        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": MySnmpTrapReceiver ctor - Enter"
        );
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
        m_zkc.mapRSNodesFromZK();
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": MySnmpTrapReceiver ctor - Exit"
        );
    }

    public int getRetCode() {
        return retcode;
    }

    private void start(String lv_host) {
        convertToHostName(lv_host);
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": Start downNode: " + lv_host + ",hostname:" + this.badName
        );
        if (lv_host == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": receivedV1Trap - ERROR - Could not find the host machine "
            );
            return;
        }

        // shutdown datanode
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": Going to delete the DataNode on : " + lv_host
        );
        boolean sucess = dfs_admin_client.removeBadNode(badName);
        if (sucess) {
            System.out.println(new Timestamp(System.currentTimeMillis())
                    + ": Going to delete the DataNode on  " + lv_host + " sucess"
            );
        } else {
            System.out.println(new Timestamp(System.currentTimeMillis())
                    + ": Going to delete the DataNode on : " + lv_host + " failure"
            );
        }
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": Going to delete the HBase ZKNode for the RS on : " + lv_host
        );
        m_zkc.deleteZKNode(badName);
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": Let's print the ZK Nodes for the HBase RS."
        );

        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": ping  Exit."
        );
    }

    public void convertToHostName(String hostname) {
        Set<String> keys = m_zkc.getLv_map().keySet();
        System.out.println("zk exit Host="+keys+",bad host="+hostname);
        for (String key : keys) {
            if (key.startsWith(hostname)) {
                this.badName = key;
                System.out.println("Bad hostName="+this.badName);
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String badnode = null;
        String conf = null;
        System.out.println("==========================================================");
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": Starting the Ping Receiver"
        );
        System.out.println("==========================================================");
        Options options = new Options();
        options.addOption("h", "Help", false, "Help");
        options.addOption("n", "node", true, "Set badNode");
        options.addOption("c", "conf", true, "Set conf");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (cmd.hasOption('h')) {
            System.out.println("Help Message");
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("Command ", options);
            System.exit(0);
        }
        if (cmd.hasOption("n")) {
            badnode = cmd.getOptionValue("n");
            System.out.println(new Timestamp(System.currentTimeMillis())
                    +": pase="+badnode);
        }
        if (cmd.hasOption("c")) {
            conf = cmd.getOptionValue("c");
            System.out.println("conf="+conf);
        }

        Configuration sv_config = HBaseConfiguration.create();
        String hbase_site = conf+"/"+"hbase-site.xml";
        String core_site =  conf+"/"+"core-site.xml";
        String hdfs_site =  conf+"/"+"hdfs-site.xml";
        File hbase_file= new File(hbase_site);
        File core_file=  new File(core_site);
        File hdfs_file=  new File(hdfs_site);
        if(hbase_file.exists()) sv_config.addResource(new FileInputStream(hbase_file));
        if(core_file.exists())  sv_config.addResource(new FileInputStream(core_file));
        if(hdfs_file.exists()) sv_config.addResource(new FileInputStream(hdfs_file));

        //zk is on shutdown node
        sv_config.setInt("ipc.client.connect.timeout",
                1000);
        sv_config.setInt(
                "ipc.client.connect.max.retries.on.timeouts",
                1);
//    int code =ToolRunner.run(new HadoopPingTool(sv_config),args);
        start(args,sv_config,badnode);
    }

    static class HadoopPingTool extends Configured implements Tool {
        private String badNode;
        HadoopPingTool(Configuration conf,String badNode) {
            super(conf);
            this.badNode=badNode;
        }
        public int run(String[] args) throws Exception {
            getConf();
            start(args,getConf(),badNode);
            return 0;
        }
    }

    public static void start(String[] args, Configuration conf,String badnode){
        boolean startuserOnrack = false;
        boolean startMeta = true;
        System.out.println(new Timestamp(System.currentTimeMillis())+" Start HadoopPingTool");
        if (args.length > 0) {
            for (int i = 1; i < args.length; i++) {
                if (DFSAdminClient.PARAMETER.equals(args[i])) {
                    System.out.println("start only remove racks");
                    startuserOnrack = true;
                }
            }
        }
        ZKClient lv_zkc = null;
        try {
            lv_zkc = new ZKClient(conf);
            DFSAdminClient dfsAdminClient = new DFSAdminClient(conf);
            dfsAdminClient.setStartOnlyRack(startuserOnrack);
            lv_zkc.printRSNodes();
            PingReceiver lv_tr = new PingReceiver(lv_zkc, dfsAdminClient);
            lv_tr.start(badnode);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }
}
