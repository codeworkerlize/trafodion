package com.esgyn.snmp;

import org.apache.commons.cli.*;

import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.hbase.Abortable;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HRegionInfo;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;

import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import org.apache.hadoop.util.Tool;

import org.apache.zookeeper.KeeperException;

import org.apache.zookeeper.WatchedEvent;

import org.apache.zookeeper.Watcher;

import org.apache.zookeeper.ZooKeeper;



import java.io.FileInputStream;

import java.io.FileNotFoundException;

import java.io.IOException;

import java.io.File;

import java.sql.Timestamp;

import java.util.Arrays;

import java.util.List;

import java.util.Set;

import java.util.concurrent.ExecutorService;

import java.util.concurrent.Executors;

import java.util.concurrent.Future;

import java.util.concurrent.TimeUnit;

import java.lang.management.MemoryMXBean;

import java.lang.management.ManagementFactory;

import java.util.regex.Matcher;

import java.util.regex.Pattern;

public class CleanZNode {

    static  final Log LOG = LogFactory.getLog(CleanZNode.class);

    public  static final String ZNODE_SEPARATOR = ",";

    public  static final String PATH_SEPARATOR = "/";

    public  static String JUTE_MAXBUFFER = "31457280";

    public  static long ZNODE_CLEAN_TIMEOUT = 900;

    public  static int MAX_THREAD = 500;

    public static String OP_TYPE = "all";

    public static String TAG_TIMESTAMP = "";

    public static String TAG_PREFIX = "TRAF_RSRVD_3:TRAFODION._BACKUP";    
    
//    private ExecutorService executorService = Executors.newCachedThreadPool();

    private static ExecutorService executorService;  

    static ZooKeeperWatcher zkw = null;



    public static void main(String[] args) throws Exception {

        String znode = "/trafodion/recovery,/trafodion/splitbalance,/trafodion/traflock";

        String conf = null;

        LOG.info("==========================================================");

        LOG.info(new Timestamp(System.currentTimeMillis())

                + ": Starting The Clean ZNODE"

        );

        LOG.info("==========================================================");

        Options options = new Options();

        options.addOption("h", "Help", false, "Help");

        options.addOption("n", "node", true, "Set ZNode");

        options.addOption("c", "conf", true, "Set conf");

        options.addOption("t", "timeout", true, "Set zktimeout");

        options.addOption("b", "bufer", true, "Set maxbuffer");

        options.addOption("p", "maxthread", true, "Set pool");

        options.addOption("op", "opType", true, "Set opType");

        options.addOption("ts", "timestmap", true, "Set timestmap");

        CommandLineParser parser = new PosixParser();

        CommandLine cmd = null;

        try {

            cmd = parser.parse(options, args);

        } catch (ParseException e) {

            e.printStackTrace();

        }

        if (cmd.hasOption('h')) {

            LOG.info("Help Message");

            HelpFormatter hf = new HelpFormatter();

            hf.printHelp("Command ", options);

            System.exit(0);

        }

        if (cmd.hasOption("n")) {

            znode = cmd.getOptionValue("n");

            LOG.info("PATH=" + znode);

        }

        if (cmd.hasOption("c")) {

            conf = cmd.getOptionValue("c");

            LOG.info("CONF=" + conf);

        }



        if (cmd.hasOption("t")) {

            ZNODE_CLEAN_TIMEOUT = Long.valueOf(cmd.getOptionValue("t"));

            LOG.info("ZNODE_CLEAN_TIMEOUT=" + ZNODE_CLEAN_TIMEOUT);

        }

        if (cmd.hasOption("b")) {

            JUTE_MAXBUFFER  = cmd.getOptionValue("b");

            LOG.info("JUTE_MAXBUFFER=" + JUTE_MAXBUFFER);

        }

        if (cmd.hasOption("p")) {

            MAX_THREAD = Integer.parseInt(cmd.getOptionValue("p"));

            LOG.info("MAX_THREAD=" + MAX_THREAD);

        }

        if (cmd.hasOption("op")) {

            OP_TYPE = cmd.getOptionValue("op");
            LOG.info("op=" + OP_TYPE);
        }

        if (cmd.hasOption("ts")) {

            TAG_TIMESTAMP = cmd.getOptionValue("ts");
            LOG.info("TAG_TIMESTAMP=" + TAG_TIMESTAMP);
        }
        Configuration sv_config = HBaseConfiguration.create();

        String hbase_site = conf + "/" + "hbase-site.xml";

        String core_site = conf + "/" + "core-site.xml";

        String hdfs_site = conf + "/" + "hdfs-site.xml";

        setConf(sv_config, hbase_site);

        setConf(sv_config, core_site);

        setConf(sv_config, hdfs_site);

        //

        MemoryMXBean mxb = ManagementFactory.getMemoryMXBean();

        LOG.info("Max:" + mxb.getHeapMemoryUsage().getMax() / 1024 / 1024 + "MB");    //Max:1776MB

        LOG.info("Init:" + mxb.getHeapMemoryUsage().getInit() / 1024 / 1024 + "MB");  //Init:126MB

        LOG.info("Committed:" + mxb.getHeapMemoryUsage().getCommitted() / 1024 / 1024 + "MB");   //Committed:121MB

        LOG.info("Used:" + mxb.getHeapMemoryUsage().getUsed() / 1024 / 1024 + "MB");  //Used:7MB

        LOG.info(mxb.getHeapMemoryUsage().toString()); 

        //

        String property = System.setProperty("jute.maxbuffer",JUTE_MAXBUFFER);

        executorService = Executors.newFixedThreadPool(MAX_THREAD);

        //

        zkw = new ZooKeeperWatcher(sv_config, "CleanZnode", new Abortable() {

            @Override

            public void abort(String why, Throwable e) {



            }



            @Override

            public boolean isAborted() {

                return false;

            }

        }, false);

        CleanZNode cleanZNode = new CleanZNode();

        //parse ZNODE

        String[] paths = znode.split(ZNODE_SEPARATOR);

        long s=System.currentTimeMillis();

        for (String path : paths) {

            cleanZNode.deleteZnode(path);

        }

        cleanZNode.executorService.shutdown();

        boolean isTimeOut = cleanZNode.executorService.awaitTermination(ZNODE_CLEAN_TIMEOUT, TimeUnit.SECONDS);

        long end = System.currentTimeMillis() - s;

        if(!isTimeOut && (end>= ZNODE_CLEAN_TIMEOUT*1000 )){

            cleanZNode.executorService.shutdownNow();

            LOG.info("Delete Path " + Arrays.asList(paths) + " Timeout " + ZNODE_CLEAN_TIMEOUT+" s, Task MayBe Not Be Compeleted");

            return;

        }

        LOG.info("Delete "+Arrays.asList(paths)+" cost "+ end+" ms");

    }



    public static void setConf(Configuration sv_config, String file) throws FileNotFoundException {

        File hbase_file = new File(file);

        if (hbase_file.exists()) sv_config.addResource(new FileInputStream(hbase_file));

    }



    public void deleteZnode(String path) {

        final List<String> children;

        try {

            children = ZKUtil.listChildrenNoWatch(zkw, path);

            if(children==null || children.size()<=0){

                LOG.info("There is no Znode which be Deleted Path "+path);

                return;

            }

            for (int i = 0; i < children.size(); i++) {

                boolean isRemove = checkTag(children.get(i));

                if(isRemove){

                final int finalI = i;

                final String  fpath = path;

                executorService.submit(new Runnable() {

                    @Override

                    public void run() {

                        try {

                            org.apache.hadoop.hbase.zookeeper.ZKUtil.deleteNodeRecursively(zkw, fpath + PATH_SEPARATOR + children.get(finalI));
                             
                            LOG.info("Delete Path " + children.get(finalI));

                        } catch (KeeperException e) {

                            LOG.info("Delete Path " +fpath + " Generate " + e.getMessage());

                            e.printStackTrace();

                        }

                    }

                });
              }
            }

        } catch (KeeperException e) {

            e.printStackTrace();

        }



    }


    boolean removeZnodeByTime(String currenttime) {
        if (currenttime == null || currenttime.equals("")) {
            return false;
        }
        return currenttime.compareTo(TAG_TIMESTAMP) < 0;
    }

   public String parseZnodeForTime(String tablename) {
        String timestamp = "";
        String regEx = "(\\d{20,})";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(tablename);
        while (m.find()) {
            timestamp = m.group(0);
            LOG.debug("tablename time =" + timestamp);
        }
        return timestamp;
    }


  public boolean checkTag(String tablename) {
        boolean isRemove = true;
        switch (OP_TYPE.toLowerCase()) {
            case "checktag":
                String currentTime = parseZnodeForTime(tablename);
                if (!(tablename.startsWith(TAG_PREFIX) && removeZnodeByTime(currentTime))) {
                    isRemove = false;
                }
                break;
            default:
        }
        return isRemove;
    }
}

