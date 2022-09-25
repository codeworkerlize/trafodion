/**
 * @@@ START COPYRIGHT @@@
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @@@ END COPYRIGHT @@@
 */
package org.trafodion.dcs.zookeeper;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.sql.Date;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.util.DcsConfiguration;

/**
 * Tool for reading ZooKeeper servers from dcs XML configuration and producing a line-by-line list
 * for use by bash scripts.
 */
public class ZKShellTool {
    private static final Configuration CONF = DcsConfiguration.create();
    private static final String PARENT_DCS_ZNODE = //"/trafodion/1";
                CONF.get(Constants.ZOOKEEPER_ZNODE_PARENT, Constants.DEFAULT_ZOOKEEPER_ZNODE_DCS_PARENT);
    private static final String REGISTERED_PATH =
            PARENT_DCS_ZNODE + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED;
    private static final String RUNNING_PATH = PARENT_DCS_ZNODE + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING;
    private static final String MASTER_PATH = PARENT_DCS_ZNODE + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER;
    private static final String LEADER_PATH = PARENT_DCS_ZNODE + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER_LEADER;

    private static final String SPLIT = ":";
    private static final String STARTING = "STARTING";
    private static final String AVAILABLE = "AVAILABLE";
    private static final String CONNECTED = "CONNECTED";
    private static final String CONNECTING = "CONNECTING";
    private static final String DISABLE = "DISABLE";
    private static final String ENABLE = "ENABLE";
    private static final String RESTART = "RESTART";
    private static final String REBALANCE = "REBALANCE";
    private static final String SUSPEND = "SUSPEND";
    private static final String EXIT = "EXIT";
    private static final String EACHNODEMXOS = "allNodeMxos";

    enum MxosrvrStat {
        AVAILABLE("AVAILABLE"), DISABLE("DISABLE"), CONNECTING("CONNECTING"), CONNECTED("CONNECTED"), SUSPEND("SUSPEND"), RESTART(
                "RESTART"), REBALANCE("REBALANCE");
        private String name;

        private MxosrvrStat(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

    }

    private ZkClient zkc = null;

    // used for scroll restart
    private final Map<String, String> servers = new ConcurrentHashMap<String, String>();
    private final Map<String, String> connServers = new ConcurrentHashMap<String, String>();
    private final Map<String, String> availServers = new ConcurrentHashMap<String, String>();
    private final Map<String, String> realDataMap = new ConcurrentHashMap<String, String>();
    private final Map<String, Integer> nodeStatusNum = new ConcurrentHashMap<String, Integer>();
    private long currentTime = 0L;
    private long oneDayTime = 86400000L;
    private int totalRestart = 0;
    private int availMxoCount = 0;
    private int connMxoCount = 0;
    private CountDownLatch latch = new CountDownLatch(1);
    private CountDownLatch availLatch = new CountDownLatch(1);
    private static String statDataPath;
    private final static String statDataNameFile = "SRscript.RW";
    private final static String srConnCountFile = "SRconnNum.RW";
    private final StringBuffer mxoStatBuffer = new StringBuffer();
    private final int DEFAULT_SHOWDETAILSIZE = 80;

    private ZKShellTool() {
        zkc = new ZkClient();
        statDataPath = System.getenv("TRAF_VAR") + "/";
        try {
            zkc.connect();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        currentTime = System.currentTimeMillis();
    }

    /**
     * Run the tool.
     * 
     * @param args Command line arguments.
     */
    public static void main(String[] args) throws Exception{
//        args = new String[] {"-S a"};
        CommandLine cmd = initCMD(args);

        ZKShellTool zkshelltool = new ZKShellTool();

        if (cmd.hasOption("I")) {
            zkshelltool.outputDCSInfo(cmd.getOptionValues("I"));
        } else if (cmd.hasOption("S")) {
            zkshelltool.toScrollRestart(cmd.getOptionValues("S"));
        } else if (cmd.hasOption("B")) {
            zkshelltool.toBalance();
        } else if (cmd.hasOption("E")) {
            zkshelltool.toEnable(cmd.getOptionValues("E"));
        } else if (cmd.hasOption("D")) {
            zkshelltool.toDisable(cmd.getOptionValues("D"));
        } else if (cmd.hasOption("GS")) {
            zkshelltool.getStartedConnNum(cmd.getOptionValues("GS"));
        }else if (cmd.hasOption("G")) {
            zkshelltool.getConnNum(cmd.getOptionValues("G"));
        }else if (cmd.hasOption("R")) {
            zkshelltool.toRestore();
        }
    }

    private void toBalance() {
        System.out.println("balance Start working : ");
        Map<String, List<String>> nodesCount = new ConcurrentHashMap<String, List<String>>();
        int nodeCount = 0;
        String serverCount = null;
        try {
            synchronized (lock) {
                List<String> children = zkc.getChildren(REGISTERED_PATH, new ChildrenWatcher());
                servers.clear();
                nodesCount.clear();
                if (!children.isEmpty()) {
                    Stat stat = null;
                    String data = null;
                    for (String child : children) {
                        stat = zkc.exists(REGISTERED_PATH + "/" + child, false);
                        if (stat != null) {
                            data = new String(zkc.getData(REGISTERED_PATH + "/" + child, new DataWatcher(), stat));
                            if (data.startsWith(CONNECTED)) {
                                servers.put(child, data);
                                mxosToNode(child, nodesCount);
                            }else {
                                checkPreOperation(data);
                            }
                        }
                    }
                }

                try {
                    serverCount = getServerCount(new String[] {"sh", "-c","sqshell -c node info | grep -w Any| grep -cw Up"}, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (!serverCount.matches("\\d*")) {
                    String message = "there are 0 dcsServer are runing, please do sqcheck or dcscheck to verify";
                    printErrorMsg("ERROR: ", message);
                    System.exit(1);
                }
                nodeCount = Integer.parseInt(serverCount);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

        totalRestart = servers.size();
        if (totalRestart == 0) {
            return;
        }
        int expectedNum = (totalRestart / nodeCount) + 1;
        if (nodeCount == 1) {
            String message = "Single node is not required reBalanced";
            printErrorMsg("WARN : ", message);
            return;
        }
        List<String> serversToRebalance = new ArrayList<String>();
        for (Entry<String, List<String>> entry : nodesCount.entrySet()) {
            List<String> servers = entry.getValue();

            int rebalanceNum = servers.size() - expectedNum;

            if (rebalanceNum <= 0) {
                continue;
            }

            // rebalance number means the number need to do rebalance on each node
            while (rebalanceNum-- > 0) {
                serversToRebalance.add(servers.remove(0));
            }
        }
        System.out.println("Load finish. <" + serversToRebalance.size() + "> mxosrvrs will be balanced.");
        for (String server : serversToRebalance) {
            String data = servers.get(server);
            String[] arrData = data.split(SPLIT, 2);
            try {
                zkc.setData(REGISTERED_PATH + "/" + server, (REBALANCE + SPLIT + arrData[1]).getBytes(), -1);
                mxoStatBuffer.append(server).append(";");
                connToNode(server, nodeStatusNum, 1);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                continue;
            }
        }
        try {
            String serverData = getServerStatus(nodeStatusNum, serversToRebalance.size());
            mkDirectory(srConnCountFile);
            storeMxoStats(serverData, srConnCountFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            mkDirectory(statDataNameFile);
            storeMxoStats(mxoStatBuffer.toString(), statDataNameFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void toEnable (String[] option) {
        System.out.println("toEnable Start working : ");
        String[] nodesIndex = sortNodeList(option[1].split(","));

        try {
            synchronized (lock) {
                // enable nodes
                List<String> nodes = zkc.getChildren(RUNNING_PATH, new ChildrenWatcher());
                for (String node : nodes) {
                    if (checkDisable(nodesIndex, node)) {
                        Stat stat = zkc.exists(RUNNING_PATH + "/" + node, false);
                        String data = new String(zkc.getData(RUNNING_PATH + "/" + node, false, stat));
                        if (data.startsWith(DISABLE)){
                            data = data.replace(DISABLE, ENABLE);
                            zkc.setData(RUNNING_PATH + "/" + node, data.getBytes(), -1);
                        }
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    private void toDisable (String[] option) {
        System.out.println("toDisable Start working : ");
        Map<String, List<String>> metaCount = new ConcurrentHashMap<String, List<String>>();
        Map<String, List<String>> nodesCount = new ConcurrentHashMap<String, List<String>>();
        String[] nodesIndex = sortNodeList(option[1].split(","));
        boolean isFull = true;
        int tdCount = 0;
        try {
            synchronized (lock) {
                // disable nodes
                List<String> nodes = zkc.getChildren(RUNNING_PATH, new ChildrenWatcher());
                for (String node : nodes)
                {
                    if (checkDisable(nodesIndex, node))
                    {
                        Stat stat = zkc.exists(RUNNING_PATH + "/" + node, false);
                        String data = new String(zkc.getData(RUNNING_PATH + "/" + node, false, stat));
                        if (data.startsWith(ENABLE))
                        {
                            data = data.replace(ENABLE, DISABLE);
                            zkc.setData(RUNNING_PATH + "/" + node, data.getBytes(), -1);
                        }
                    }
                }

                // restore mxosrvrs
                List<String> children = zkc.getChildren(REGISTERED_PATH, new ChildrenWatcher());
                servers.clear();
                availServers.clear();
                connServers.clear();
                metaCount.clear();
                nodesCount.clear();
                while (isFull && !children.isEmpty()) {
                    Stat stat = null;
                    String data = null;
                    for (String child : children) {
                        stat = zkc.exists(REGISTERED_PATH + "/" + child, false);
                        data = new String(zkc.getData(REGISTERED_PATH + "/" + child,
                                new DataWatcher(), stat));
                        checkPreOperation(data);
                        if (stat != null && checkDisable(nodesIndex, child)) {
                            mxosToNode(child, metaCount);
                            if (data.startsWith(AVAILABLE) || data.startsWith(CONNECTED)
                                    || data.startsWith(DISABLE)) {
                                if (data.startsWith(CONNECTED)) {
                                    if (!connServers.containsKey(child)) {
                                        connServers.put(child, data);
                                    }
                                }
                                if (!servers.containsKey(child)) {
                                    servers.put(child, data);
                                }
                                mxosToNode(child, nodesCount);
                            }
                        }else if(data.startsWith(AVAILABLE)) {
                            if (!availServers.containsKey(child)) {
                                availServers.put(child, data);
                            }
                        }
                    }
                    availMxoCount = availServers.size();
                    connMxoCount = connServers.size();
                    if (availMxoCount < connMxoCount) {
                        String message =
                            "there are not enough available mxos toDisable, disable mxos = "
                                + connMxoCount + ", but available mxos = " + availMxoCount;
                        printErrorMsg("ERROR: ", message);
                        System.exit(1);
                    }
                    for (Entry<String, List<String>> entry : metaCount.entrySet()) {
                        String key = entry.getKey();
                        List<String> metaMxos = entry.getValue();
                        List<String> tdMxos = nodesCount.get(key);
                        if (tdMxos != null && metaMxos.size() == tdMxos.size()) {
                            //set mxos state to disable
                            setDisable(tdMxos);
                            tdCount += tdMxos.size();
                            //remove this node
                            String serverNode = key.substring(key.indexOf(":") + 1);
                            int pos = Arrays.binarySearch(nodesIndex, serverNode);
                            nodesIndex = (String[]) ArrayUtils.remove(nodesIndex, pos);
                            //remove these mxos
                            metaCount.remove(key);
                            nodesCount.remove(key);
                        }
                    }
                    try {
                        String serverData = getServerStatus(nodeStatusNum, tdCount);
                        mkDirectory(srConnCountFile);
                        storeMxoStats(serverData, srConnCountFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if (servers.size() == tdCount) {
                        isFull = false;
                    }
                    children = zkc.getChildren(REGISTERED_PATH, new ChildrenWatcher());
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        try {
            mkDirectory(statDataNameFile);
            storeMxoStats(mxoStatBuffer.toString(), statDataNameFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
    private void toRestore() {
        System.out.println("toRestore Start working : ");
        int rsCount  = 0;
        try {
            String realData = getMxoStats(statDataNameFile);
            String[] rsData = realData.split(";");
            Stat stat = null;
            String data = null;

            // restore mxosrvrs
            for (String path : rsData) {
                stat = zkc.exists(REGISTERED_PATH + "/" + path, false);
                if (stat == null) {
                    continue;
                }
                data = new String(
                        zkc.getData(REGISTERED_PATH + "/" + path, new DataWatcher(), stat));
                System.out.println(data);
                if (data.startsWith(RESTART) || data.startsWith(REBALANCE)) {
                    rsCount++;
                    String[] mxoData = data.split(SPLIT, 2);
                    String tmpData = CONNECTED + SPLIT + mxoData[1];
                    zkc.setData(REGISTERED_PATH + "/" + path, tmpData.getBytes(), -1);
                    System.out.println("restart or rebalance be restored");
                } else if (data.startsWith(DISABLE)) {
                    String[] tmpArr = data.split(SPLIT);
                    if (!tmpArr[2].equals("")) {
                        rsCount++;
                        String[] mxoData = data.split(SPLIT, 2);
                        String tmpData = CONNECTED + SPLIT + mxoData[1];
                        zkc.setData(REGISTERED_PATH + "/" + path, tmpData.getBytes(), -1);
                        System.out.println("disable be restored");
                    }
                }
            }
            
            if (rsCount == 0) {
                String msg = "there are 0 connection be restored";
                printErrorMsg("WARN", msg);
                System.exit(0);
            }else {
                System.out.println("torestore success!");
            }
            deleteFile(statDataNameFile);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        
    }
    
    private void mxosToNode (String child, Map<String, List<String>> nodeMap) {
        String node = child.substring(0, child.lastIndexOf(":"));
        if (!nodeMap.containsKey(node)) {
            List<String> servers = new ArrayList<String>();
            servers.add(child);
            nodeMap.put(node, servers);
        } else {
            List<String> servers = nodeMap.get(node);
            if (!servers.contains(child)) {
                servers.add(child);
                nodeMap.put(node, servers);
            }
        }
    }
    
    private void connToNode (String child, Map<String, Integer> nodeMap, int num) {
        String node = child.substring(0, child.indexOf(":"));
        if (!nodeMap.containsKey(node)) {
            int tmp = num;
            nodeMap.put(node, tmp);
        } else {
            int tmp = nodeMap.get(node);
            nodeMap.put(node, tmp + num);
        }
    }
    
    private void setDisable (List<String> tdMxos) {
        for (int k = 0; k < tdMxos.size(); k++) {
            String path = tdMxos.get(k);
            String data = servers.get(path);
            String[] metaData = data.split(SPLIT,2);
            if (DISABLE.equals(metaData[0])) {
                continue;
            }
            String keepData = null;
            if (data.startsWith(CONNECTED)) {
                try {
                    Thread.sleep(100); // slow disable
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }

                keepData = DISABLE + SPLIT + metaData[1];
                mxoStatBuffer.append(path).append(";"); 
            }else {
                String[] arrData = data.split(SPLIT);
                keepData = DISABLE + SPLIT + arrData[1] + SPLIT + SPLIT + arrData[3] + SPLIT
                        + arrData[4] + SPLIT + arrData[5] + SPLIT + arrData[6] + SPLIT + arrData[7]
                        + SPLIT + SPLIT + SPLIT + SPLIT + SPLIT + SPLIT + SPLIT + SPLIT + SPLIT
                        + SPLIT + SPLIT + SPLIT;
            }
            try {
                zkc.setData(REGISTERED_PATH + "/" + path, keepData.getBytes(),
                        -1);
                connToNode(path, nodeStatusNum, 1);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                continue;
            }
        }
    }
    
    private String getServerCount(String[] strings, File dir) throws Exception {
        StringBuilder result = new StringBuilder();
 
        Process process = null;
        BufferedReader bufrIn = null;
        BufferedReader bufrError = null;

        try {
            process = Runtime.getRuntime().exec(strings, null, dir);
            process.waitFor();
            bufrIn = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
            bufrError = new BufferedReader(new InputStreamReader(process.getErrorStream(), "UTF-8"));
            String line = null;
            while ((line = bufrIn.readLine()) != null) {
                result.append(line);
            }
            while ((line = bufrError.readLine()) != null) {
                result.append(line).append('\n');
            }
        } finally {
            closeStream(bufrIn);
            closeStream(bufrError);
            if (process != null) {
                process.destroy();
            }
        }
        return result.toString();
    }
    private void closeStream(Closeable stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (Exception e) {
            }
        }
    }
    
    private void getConnNum (String[] option) {
        System.out.println("getConnNum Start working : ");
        Map<String, Integer> connNum = new ConcurrentHashMap<String, Integer>();
        String[] nodesIndex = option[1].split(",");
        try {
            synchronized (lock) {
                List<String> children = zkc.getChildren(REGISTERED_PATH, new ChildrenWatcher());
                Stat stat = null;
                String data = null;
                for (String child : children) {
                    stat = zkc.exists(REGISTERED_PATH + "/" + child, false);
                    data = new String(
                            zkc.getData(REGISTERED_PATH + "/" + child, new DataWatcher(), stat));
                    if (stat != null && checkDisable(nodesIndex, child)) {
                        connToNode(child, connNum, 0);
                        if (data.startsWith(RESTART) || data.startsWith(CONNECTED)
                                || data.startsWith(REBALANCE)) {
                            connToNode(child, connNum, 1);
                        } else if (data.startsWith(DISABLE)) {
                            String[] tmpArr = data.split(SPLIT);
                            if (!tmpArr[2].equals("")) {
                                connToNode(child, connNum, 1);
                            }
                        }
                    }
                }

            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        for (Entry<String, Integer> entry : connNum.entrySet()) {
            System.out.println(entry.getKey()+SPLIT+entry.getValue());
        }
        
    }

    private void getStartedConnNum (String[] option) {
        System.out.println("getStartedConnNum Start working : ");
        Map<String, Integer> connNum = new ConcurrentHashMap<String, Integer>();
        String[] nodesIndex = option[1].split(",");
        for (String node : nodesIndex)
            connNum.put(node,0);

        try {
            List<String> children = zkc.getChildren(REGISTERED_PATH, new ChildrenWatcher());
            Stat stat = null;
            String data = null;
            String nodeIndexOfZnode = null;
            Integer tmp = 0;
            for (String child : children) {
                nodeIndexOfZnode = child.substring(child.indexOf(":") + 1, child.lastIndexOf(":"));
                if (connNum.containsKey(nodeIndexOfZnode))
                {
                    stat = zkc.exists(REGISTERED_PATH + "/" + child, false);
                    data = new String(zkc.getData(REGISTERED_PATH + "/" + child, false, stat));
                    if (!data.startsWith(STARTING)) {
                        tmp = connNum.get(nodeIndexOfZnode);
                        tmp++;
                        connNum.put(nodeIndexOfZnode, tmp);
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

        for (Entry<String, Integer> entry : connNum.entrySet()) {
            System.out.println(entry.getKey()+SPLIT+entry.getValue());
        }
    }

    enum RestartType {
        A, D, S, M, B, N
    }

    private void toScrollRestart(String[] option) {
        System.out.println("ScrollRestart start working : ");
        String[] kv = checkOptionValue(option);
        RestartType type = RestartType.valueOf(kv[0].toUpperCase());
        boolean restartAll = type == RestartType.A;
        boolean restartAvailable = type == RestartType.N;

        try {
            synchronized (lock) {
                // restart mxosrvr
                List<String> children = zkc.getChildren(REGISTERED_PATH, new ChildrenWatcher());
                servers.clear();
                availServers.clear();
                connServers.clear();
                if (!children.isEmpty()) {
                    Stat stat = null;
                    String data = null;
                    for (String child : children) {
                        stat = zkc.exists(REGISTERED_PATH + "/" + child, false);
                        data = new String(zkc.getData(REGISTERED_PATH + "/" + child, new DataWatcher(), stat));
                        checkPreOperation(data);
                        if ((stat != null && restartAll) || (stat != null && checkRestart(type, kv, child, stat, data))) {
                            if (data.startsWith(AVAILABLE) || data.startsWith(DISABLE)) {
                                availServers.put(child, data);
                            }else {
                                connServers.put(child, data);
                            }
                        }else if(data.startsWith(AVAILABLE)) {
                            servers.put(child, data);
                        }
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        connMxoCount = connServers.size();
        availMxoCount = availServers.size();
        if (availMxoCount == 0) {
            availLatch.countDown();
        }
        int mxosCount = servers.size() + availMxoCount;
        System.out.println("Load finish. <" + (connMxoCount+availMxoCount) + "> mxosrvrs will scroll restart.");

        if (!restartAvailable){
            if (connMxoCount > mxosCount) {
                String message = "there are not enough available mxos toScrollRestart, restart mxos = "
                        + connMxoCount + ", but available mxos = " + mxosCount;
                printErrorMsg("ERROR: ", message);
                System.exit(1);
            }
        }
        for (Entry<String, String> entry : availServers.entrySet()) {
            String path = entry.getKey();
            String data = entry.getValue();
            String[] arrData = data.split(SPLIT, 2);
            try {
                zkc.setData(REGISTERED_PATH + "/" + path, (EXIT + SPLIT + arrData[1]).getBytes(),
                        -1);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                continue;
            }
        }
        try {
            availLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (!restartAvailable) {
            if (connMxoCount == 0) {
                latch.countDown();
            }
            for (Entry<String, String> entry : connServers.entrySet()) {
                String path = entry.getKey();
                String data = entry.getValue();
                String[] arrData = data.split(SPLIT, 2);
                try {
                    zkc.setData(REGISTERED_PATH + "/" + path, (RESTART + SPLIT + arrData[1]).getBytes(),
                            -1);
                    mxoStatBuffer.append(path).append(";");
                    connToNode(path, nodeStatusNum, 1);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
            }
            try {
                String serverData = getServerStatus(nodeStatusNum, connMxoCount);
                mkDirectory(srConnCountFile);
                storeMxoStats(serverData, srConnCountFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                mkDirectory(statDataNameFile);
                storeMxoStats(mxoStatBuffer.toString(), statDataNameFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private String[] checkOptionValue(String[] option) {
        for (int i = 0; i < option.length; i++) {
            option[i] = option[i].trim().toUpperCase();
        }

        RestartType type = null;
        try {
            type = RestartType.valueOf(option[0]);
        } catch (Exception e) {
            printErrorMsg("Error format", option[0]);
            System.exit(1);
        }

        switch (type) {
            case A:
                break;
            case D:
                try {
                    if (option.length < 2) {
                        printErrorMsg("Error value", "d");
                        System.exit(1);
                    }
                    Integer.parseInt(option[1]);
                } catch (NumberFormatException e) {
                    printErrorMsg("Error value", "d");
                    System.exit(1);
                }
                System.out.println("Restart mxosrvrs start " + option[1] + " days ago");
                break;
            case S:
                if (option.length < 2) {
                    printErrorMsg("Error value", "s");
                    System.exit(1);
                }
                System.out.println("Restart mxosrvrs running on " + option[1]);
                break;
            case M:
                String[] mxosrvrInfo = option[1].split(":");
                if (mxosrvrInfo.length != 2) {
                    printErrorMsg("Error value", "m");
                    System.exit(1);
                }
                try {
                    if (Integer.parseInt(mxosrvrInfo[0]) <= 0 || Integer.parseInt(mxosrvrInfo[1]) <= 0)
                        throw new NumberFormatException();
                } catch (NumberFormatException e) {
                    printErrorMsg("Error value", "m");
                    System.exit(1);
                }
                System.out.println("Restart specified mxosrvrs " + option[1]);
                option = new String[] {option[0], mxosrvrInfo[0], mxosrvrInfo[1]};
                break;
            case N:
                String[] availableNodes = option[1].split(",");
                int optLen = availableNodes.length;
                if (optLen == 0) {
                    printErrorMsg("Null value", "n");
                    System.exit(1);
                }else {
                    String temp = option[0];
                    option = new String[optLen + 1];
                    System.arraycopy(availableNodes, 0, option, 1, optLen);
                    option[0] = temp;
                }
                break;
            default:
                System.out.println("Unknown option " + option[0] + ", Please use -h for help");
                System.exit(0);
                break;
        }
        return option;
    }
    
    private void printPreOperationError() {
        String message = "The previous operation is not completed, Please check it";
        printErrorMsg("ERROR: ", message);
        System.exit(1);
    }

    private void printErrorMsg(String type, String option) {
        System.out.println(type + " for " + option + ". Please use -h for help");
    }

    private boolean checkRestart(RestartType type, String[] kv, String znode, Stat stat, String data) {
        switch (type) {
            case D:
                return stat.getCtime() + Integer.parseInt(kv[1]) * oneDayTime < currentTime;
            case S:
                znode = znode.substring(0, znode.indexOf(":"));
                return znode.equalsIgnoreCase(kv[1]);
            case M:
                String[] dataTmp = data.split(":");
                String serverIndex = znode.substring(znode.indexOf(":") + 1, znode.lastIndexOf(":"));
                String pid = dataTmp[4];
                return serverIndex.equalsIgnoreCase(kv[1]) && pid.equalsIgnoreCase(kv[2]);
            case N:
                if (kv[1].equals("ALL") || checkDisable(kv, znode)) {
                    return true;
                }else {
                    return false;
                }
            default:
                return false;
        }
    }
    
    private boolean checkDisable(String[] nodesIndex, String znode) {
        Scanner scn = new Scanner(znode);
        scn.useDelimiter(":");
        scn.next(); // hostname
        String serverIndex = scn.next();
        scn.close();
        return Arrays.asList(nodesIndex).contains(serverIndex);
    }

    private void checkPreOperation(String data) {
        if (data == null) {
            return;
        }
        if(data.startsWith(RESTART) || data.startsWith(EXIT) || data.startsWith(REBALANCE)) {
            printPreOperationError();
        }else if(data.startsWith(DISABLE)) {
            String[] tmpArr = data.split(SPLIT);
            if (!tmpArr[2].equals("")) {
                printPreOperationError();
            }
        }
    }

    private Object lock = new Object();

    private class ChildrenWatcher implements Watcher {
        public void process(WatchedEvent event) {
            switch (event.getType()) {
                case NodeChildrenChanged:
                    try {
                        zkc.getChildren(event.getPath(), new ChildrenWatcher());
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    System.out.println("trigger : " + event.toString());
                    break;
            }
        }
    }
    private class DataWatcher implements Watcher {
        public void process(WatchedEvent event) {
            System.out.println(event);
            String path = event.getPath();
            path = path.substring(path.lastIndexOf("/") + 1);
            switch (event.getType()) {
                case NodeCreated:
                    System.out.println("<" + path + "> finish restart.");
                    if (availServers.containsKey(path)) {
                        availMxoCount--;
                        if (availMxoCount == 0) {
                            System.out.println("available mxosrvrs finish restart, exit.");
                            availLatch.countDown();
                        }
                    }
                    if (connServers.containsKey(path)) {
                        connMxoCount--;
                        if (connMxoCount == 0) {
                            System.out.println("connected mxosrvrs finish restart, exit.");
                            latch.countDown();
                        }
                    }
                    break;
                case NodeDeleted:
                    try {
                        zkc.exists(event.getPath(), new DataWatcher());
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                case NodeDataChanged:
                    try {
                        String data = new String(zkc.getData(event.getPath(), new DataWatcher(), null));
                        if (data.startsWith(RESTART)) {
                            System.out.println("<" + path + "> ready to restart.");
                        }
                        if (data.startsWith(DISABLE)) {
                            System.out.println("Disable <" + path + ">. NodeDataChanged");
                        }
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    System.out.println("Unknown trigger for : " + event);
                    break;
            }
        }
    }
    
    private String getServerStatus(Map<String, Integer> serverMap, int allMxos) {
        
        StringBuffer serverStatusBuffer = new StringBuffer();
        if (serverMap.isEmpty() && serverMap.size() == 0 && allMxos == 0) {
            allMxos = 1;
        }else {
            for (Entry<String, Integer> entry : serverMap.entrySet()) {
                serverStatusBuffer.append(entry.getKey()).append("=").append(entry.getValue()).append(";");
            }
        }
        serverStatusBuffer.append(EACHNODEMXOS).append("=").append(allMxos);
        return serverStatusBuffer.toString();
    }
    
    private Integer initSrConnCountFile(String[] option) {
        int detailNum = 0;
        if (option[0].equals("d")) {
            detailNum = DEFAULT_SHOWDETAILSIZE;
        }else {

            String temp = option[0].toString().toUpperCase();
            if (temp.equals("TRUE")){
                detailNum = 1;
            }else if (temp.equals("FALSE")){
                detailNum = 100;
            }else{
                String msg = "mobility flag must be true or false";
                printErrorMsg("ERROR", msg);
                System.exit(1);
            }
        }
        
        String tempData = getMxoStats(srConnCountFile);
        String realDate[] = tempData.split(";");
        for (String str : realDate) {
            String realKeyAndValue[] = str.split("=");
            nodeStatusNum.put(realKeyAndValue[0], Integer.parseInt(realKeyAndValue[1]));
        }
        return detailNum;
    }

    private void outputDCSInfo(String[] option) throws IOException, KeeperException, InterruptedException {
        List<String[]> serversConf = readServersConf();
        List<String> mastersConf = readMastersConf();
        List<String[]> zkLeaders = readZKLeaders();
        List<String[]> zkMasters = readZKMasters();
        List<String[]> zkServers = readZKServers();
        List<String[]> zkMxosrvrs = readZKMxosrvrs();
        Map<String, List<String[]>> serverMxosrvrMap = new HashMap<String, List<String[]>>();
        Map<String, List<String[]>> mxoMsgsMap = new HashMap<String, List<String[]>>();
        
        int showDetailsNum = initSrConnCountFile(option);
        NumberFormat numberFormat = NumberFormat.getInstance(); 
        numberFormat.setMaximumFractionDigits(2);
        String mobility;
        int currentNodeMxoCount = 0;
        int allDisablingCount = 0;
        String oriLine = "------------------------------------------------------------------------------------------------------------------------";

        String[] zkMasterInfo = zkMasters.get(0);
        int mxosrvrs = 0;
        for (String[] server : serversConf) {
            mxosrvrs += Integer.parseInt(server[2]);
            if (serverMxosrvrMap.get(server[0]) == null) {
                serverMxosrvrMap.put(server[0], new ArrayList<String[]>());
            }
        }

        int availableCount = 0;
        int connectingCount = 0;
        int connectedCount = 0;
        int restartCount = 0;
        int rebalanceCount = 0;
        int disableCount = 0;
        int suspendCount = 0;
        /*
         * mxo[]
         * esgyn-dim.novalocal:1:1
         * esgyn-dim.novalocal:1:1
         * CONNECTED:212453701055347105:506923851:0:12972:$Z0000000DND:192.168.0.59:23405:esgyn-dim.novalocal:192.168.0.59:60878:TrafCI:defaultSLA:defaultProfile:defaultProfile:1586849825217:db__root::0:
         */
        for (String[] mxosrvr : zkMxosrvrs) {
            if (mxosrvr.length == 25) {
                serverMxosrvrMap.get(mxosrvr[1])
                        .add(new String[] {mxosrvr[3], mxosrvr[4], mxosrvr[6], mxosrvr[0],
                                mxosrvr[10], mxosrvr[8], mxosrvr[11], mxosrvr[9], mxosrvr[23],
                                mxosrvr[24]});
            }else if(mxosrvr.length == 14) {
                serverMxosrvrMap.get(mxosrvr[1])
                .add(new String[] {mxosrvr[3], mxosrvr[4], mxosrvr[6], mxosrvr[0],
                        mxosrvr[10], mxosrvr[8], mxosrvr[11], mxosrvr[9], mxosrvr[12],
                        mxosrvr[13]});
            }
            MxosrvrStat stat = MxosrvrStat.valueOf(MxosrvrStat.class, mxosrvr[4]);
            switch (stat) {
                case AVAILABLE:
                    availableCount++;
                    break;
                case CONNECTING:
                    connectingCount++;
                    break;
                case CONNECTED:
                    connectedCount++;
                    break;
                case RESTART:
                    restartCount++;
                    break;
                case REBALANCE:
                    rebalanceCount++;
                    break;
                case DISABLE:
                    disableCount++;
                    break;
                case SUSPEND:
                    suspendCount++;
                    break;
                default:
                    break;
            }
        }
        // output conf & actual info
        //  Configured DcsMaster(s)   : GMnode03.esgyn.cn GMnode04.esgyn.cn GMnode05.esgyn.cn
        //  Active DcsMaster(s)       : gmnode05.esgyn.cn
        //  DcsMaster listen port     : 23400
        //
        //  Process         Configured      Actual          Down
        //  ---------       ----------      ------          ----
        //  DcsMaster       3               3
        //  DcsServer       6               6
        //  mxosrvr         1080            1080

        StringBuilder output = new StringBuilder(1024 * 2);
        output.append("Configured DcsMaster(s)")
                .append(fillBlanksByGivenLenForGivenStrLen(26, "Configured DcsMaster(s)")).append(" : ")
                .append(mastersConf);
        output.append(System.lineSeparator());
        output.append("Active DcsMaster(s)").append(fillBlanksByGivenLenForGivenStrLen(26, "Active DcsMaster(s)"))
                .append(" : ").append(zkMasterInfo[1]);
        output.append(System.lineSeparator());
        output.append("DcsMaster listen port").append(fillBlanksByGivenLenForGivenStrLen(26, "DcsMaster listen port"))
                .append(" : ").append(zkMasterInfo[2]);
        output.append(System.lineSeparator());
        output.append(System.lineSeparator());

        output.append("Process").append(fillBlanksByDefaultLenForGivenStrLen("Process")).append("Configured")
                .append(fillBlanksByDefaultLenForGivenStrLen("Configured")).append("Actual")
                .append(fillBlanksByDefaultLenForGivenStrLen("Actual")).append("Down");
        output.append(System.lineSeparator());
        output.append(oriLine.subSequence(0, 60));
        output.append(System.lineSeparator());
        output.append("DcsMaster").append(fillBlanksByDefaultLenForGivenStrLen("DcsMaster")).append(mastersConf.size())
                .append(fillBlanksByDefaultLenForGivenStrLen(mastersConf.size() + "")).append(zkLeaders.size())
                .append(fillBlanksByDefaultLenForGivenStrLen(zkLeaders.size() + ""));
        if (mastersConf.size() - zkLeaders.size() > 0) {
            output.append(mastersConf.size() - zkLeaders.size())
                    .append(fillBlanksByDefaultLenForGivenStrLen((mastersConf.size() - zkLeaders.size()) + ""));
        }
        output.append(System.lineSeparator());
        output.append("DcsServer").append(fillBlanksByDefaultLenForGivenStrLen("DcsServer")).append(serversConf.size())
                .append(fillBlanksByDefaultLenForGivenStrLen(serversConf.size() + "")).append(zkServers.size())
                .append(fillBlanksByDefaultLenForGivenStrLen(zkServers.size() + ""));
        if (serversConf.size() - zkServers.size() > 0) {
            output.append(serversConf.size() - zkServers.size())
                    .append(fillBlanksByDefaultLenForGivenStrLen((serversConf.size() - zkServers.size()) + ""));
        }
        output.append(System.lineSeparator());
        output.append("mxosrvr").append(fillBlanksByDefaultLenForGivenStrLen("mxosrvr")).append(mxosrvrs)
                .append(fillBlanksByDefaultLenForGivenStrLen(mxosrvrs + "")).append(zkMxosrvrs.size())
                .append(fillBlanksByDefaultLenForGivenStrLen(zkMxosrvrs.size() + ""));
        if (mxosrvrs - zkMxosrvrs.size() > 0) {
            output.append(mxosrvrs - zkMxosrvrs.size())
                    .append(fillBlanksByDefaultLenForGivenStrLen((mxosrvrs - zkMxosrvrs.size()) + ""));
        }
        output.append(System.lineSeparator());
        System.out.println(output.toString());

        // output actual stat info
        // Nodes\State| AVAILABLE CONNECTING CONNECTED
        // -----------|------------------------------------
        // NodeName   | 1        1            2
        output.delete(0, output.length());
        output.append("Nodes").append(" \\ ").append("State").append(fillBlanksByGivenLenForGivenStrLen(16,"State"))
                .append("|").append(fillBlanksByGivenLenForGivenStrLen(2, "|")).append(AVAILABLE)
                .append(fillBlanksByDefaultLenForGivenStrLen(AVAILABLE)).append(CONNECTING)
                .append(fillBlanksByDefaultLenForGivenStrLen(CONNECTING)).append(CONNECTED)
                .append(fillBlanksByDefaultLenForGivenStrLen(CONNECTED)).append(RESTART)
                .append(fillBlanksByDefaultLenForGivenStrLen(RESTART)).append(REBALANCE)
                .append(fillBlanksByDefaultLenForGivenStrLen(REBALANCE)).append(DISABLE)
                .append(fillBlanksByDefaultLenForGivenStrLen(DISABLE)).append(SUSPEND)
                .append(fillBlanksByDefaultLenForGivenStrLen(SUSPEND)).append("NODE TOTAL")
                .append(fillBlanksByDefaultLenForGivenStrLen("NODE TOTAL")).append("MIGRATION RATE");
        output.append(System.lineSeparator());
        output.append(oriLine.substring(0, 24)).append("|")
                .append(oriLine);
        output.append(System.lineSeparator());


        for (String[] server : serversConf) {
            output.append(server[0]).append(fillBlanksByGivenLenForGivenStrLen(24, server[0])).append("|")
                    .append(fillBlanksByGivenLenForGivenStrLen(4, "|"));

            int nodeAvailableCount = 0;
            int nodeConnectingCount = 0;
            int nodeConnectedCount = 0;
            int nodeRestartCount = 0;
            int nodeRebalanceCount = 0;
            int nodeDisableCount = 0;
            int nodeDisablingCount = 0;
            int nodeSuspendCount = 0;
            int nodeTotal = 0;
            for (String[] mxosrvr : serverMxosrvrMap.get(server[0])) {
                nodeTotal++;
                MxosrvrStat stat = MxosrvrStat.valueOf(MxosrvrStat.class, mxosrvr[1]);
                switch (stat) {
                    case AVAILABLE:
                        nodeAvailableCount++;
                        break;
                    case CONNECTING:
                        nodeConnectingCount++;
                        break;
                    case RESTART:
                        nodeRestartCount++;
                        storeSRingMsgs(mxoMsgsMap, mxosrvr);
                        break;
                    case REBALANCE:
                        nodeRebalanceCount++;
                        storeSRingMsgs(mxoMsgsMap, mxosrvr);
                        break;
                    case DISABLE:
                        if (!mxosrvr[2].equals("")) {
                            nodeDisablingCount++;
                            storeSRingMsgs(mxoMsgsMap, mxosrvr);
                        }
                        nodeDisableCount++;
                        break;
                    case CONNECTED:
                        nodeConnectedCount++;
                        break;
                    case SUSPEND:
                        nodeSuspendCount++;
                        break;
                    default:
                        break;
                }
            }
            allDisablingCount += nodeDisablingCount;
            int nodeNotTransferNum = nodeRestartCount > 0 ? nodeRestartCount
                    : (nodeRebalanceCount > 0 ? nodeRebalanceCount
                            : (nodeDisablingCount > 0 ? nodeDisablingCount : 0));
            if (nodeStatusNum.containsKey(server[0])) {
                currentNodeMxoCount = nodeStatusNum.get(server[0]);
            }else {
                currentNodeMxoCount = nodeTotal;
            }
            mobility = numberFormat.format((float)(currentNodeMxoCount - nodeNotTransferNum)/(float)currentNodeMxoCount * 100);
            output.append(nodeAvailableCount).append(fillBlanksByDefaultLenForGivenStrLen(nodeAvailableCount + ""))
                    .append(nodeConnectingCount).append(fillBlanksByDefaultLenForGivenStrLen(nodeConnectingCount + ""))
                    .append(nodeConnectedCount).append(fillBlanksByDefaultLenForGivenStrLen(nodeConnectedCount + ""))
                    .append(nodeRestartCount).append(fillBlanksByDefaultLenForGivenStrLen(nodeRestartCount + ""))
                    .append(nodeRebalanceCount).append(fillBlanksByDefaultLenForGivenStrLen(nodeRebalanceCount + ""))
                    .append(nodeDisablingCount+"/"+nodeDisableCount).append(fillBlanksByDefaultLenForGivenStrLen(nodeDisablingCount+"/"+nodeDisableCount))
                    .append(nodeSuspendCount).append(fillBlanksByDefaultLenForGivenStrLen(nodeSuspendCount + ""))
                    .append(nodeTotal).append(fillBlanksByDefaultLenForGivenStrLen(nodeTotal + ""))
                    .append(mobility + "%");
            output.append(System.lineSeparator());
        }
        output.append(oriLine.substring(0, 24)).append("|")
                .append(oriLine);
        output.append(System.lineSeparator());
        int notTransferNum = restartCount > 0 ? restartCount
                : (rebalanceCount > 0 ? rebalanceCount : (allDisablingCount > 0 ? allDisablingCount : 0));
        currentNodeMxoCount = nodeStatusNum.get(EACHNODEMXOS);
        mobility = numberFormat.format((float)(currentNodeMxoCount - notTransferNum)/(float)currentNodeMxoCount * 100);

        output.append("TOTAL").append(fillBlanksByGivenLenForGivenStrLen(24, "TOTAL")).append("|")
                .append(fillBlanksByGivenLenForGivenStrLen(4, "|")).append(availableCount)
                .append(fillBlanksByDefaultLenForGivenStrLen(availableCount + "")).append(connectingCount)
                .append(fillBlanksByDefaultLenForGivenStrLen(connectingCount + "")).append(connectedCount)
                .append(fillBlanksByDefaultLenForGivenStrLen(connectedCount + "")).append(restartCount)
                .append(fillBlanksByDefaultLenForGivenStrLen(restartCount + "")).append(rebalanceCount)
                .append(fillBlanksByDefaultLenForGivenStrLen(rebalanceCount + "")).append(allDisablingCount+"/"+disableCount)
                .append(fillBlanksByDefaultLenForGivenStrLen(allDisablingCount+"/"+disableCount)).append(suspendCount)
                .append(fillBlanksByDefaultLenForGivenStrLen(suspendCount + "")).append(zkMxosrvrs.size())
                .append(fillBlanksByDefaultLenForGivenStrLen(zkMxosrvrs.size() + "")).append(mobility + "%");
        output.append(System.lineSeparator());
        System.out.println(output.toString());
        
        if (notTransferNum == 0) {
            String success = "The last operation is complete";
            System.out.println("SUCCESS : " + success);
        }else if (showDetailsNum == 1 || Double.parseDouble(mobility) >= showDetailsNum){
            output.delete(0, output.length());
            output.append("connMxos").append(" \\ ").append("Msgs")
                    .append(fillBlanksByGivenLenForGivenStrLen(13,"Msgs")).append("|")
                    .append(fillBlanksByGivenLenForGivenStrLen(5, "|")).append("IP")
                     .append(fillBlanksByDefaultLenForGivenStrLen("IP")).append("PID")
                     .append(fillBlanksByDefaultLenForGivenStrLen("PID")).append("PORT")
                     .append(fillBlanksByDefaultLenForGivenStrLen("PORT")).append("PNAME")
                    .append(fillBlanksByGivenLenForGivenStrLen(22, "PNAME")).append("CTIME")
                    .append(fillBlanksByGivenLenForGivenStrLen(22, "CTIME")).append("MTIME");
            output.append(System.lineSeparator());
            output.append(oriLine.substring(0, 24)).append("|").append(oriLine.substring(0, 110));
            output.append(System.lineSeparator());
            for (Entry<String, List<String[]>> entry : mxoMsgsMap.entrySet()) {
                List<String[]> msgsTmp = entry.getValue();
                for (String[] mxosrvr : msgsTmp) {
    
                    output.append(mxosrvr[0]).append(fillBlanksByGivenLenForGivenStrLen(24, mxosrvr[0])).append("|")
                            .append(fillBlanksByGivenLenForGivenStrLen(2, "|"));
    
                    long t1 = Long.parseLong(mxosrvr[5]);
                    long t2 = Long.parseLong(mxosrvr[6]);
                    Date d1 = new Date(t1);
                    Date d2 = new Date(t2);
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String cTime = dateFormat.format(d1);
                    String mTime = dateFormat.format(d2);
    
                    output.append(mxosrvr[1])
                            .append(fillBlanksByGivenLenForGivenStrLen(13, mxosrvr[1]))
                            .append(mxosrvr[2])
                            .append(fillBlanksByDefaultLenForGivenStrLen(mxosrvr[2]))
                            .append(mxosrvr[3])
                            .append(fillBlanksByDefaultLenForGivenStrLen(mxosrvr[3]))
                            .append(mxosrvr[4])
                            .append(fillBlanksByGivenLenForGivenStrLen(14, mxosrvr[4])).append(cTime)
                            .append(fillBlanksByGivenLenForGivenStrLen(24, cTime)).append(mTime);
                    output.append(System.lineSeparator());
                }
            }
            
            System.out.println(output.toString());
        }

    }

    private String fillBlanksByGivenLenForGivenStrLen(int len, String str) {
        // total len is 12 default
        byte[] b = new byte[len - str.length()];
        Arrays.fill(b, (byte) 0x20);
        return new String(b);
    }

    private String fillBlanksByDefaultLenForGivenStrLen(String str) {
        // default total len is 12 default
        return fillBlanksByGivenLenForGivenStrLen(12, str);
    }

    /**
     * Get the list of the registered mxosrvrs in zk. Should have the same number as configure.
     *
     * @return The return content will be : <b>znodeName</b>, <b>hostName</b>, <b>serverIndex</b>,
     *         <b>mxosrvrIndex</b>, <b>znodeContent</b>, <b>nodeStartTime</b>, <b>nodeEndTime</b>
     * @throws KeeperException
     * @throws InterruptedException
     */
    private List<String[]> readZKMxosrvrs() throws KeeperException, InterruptedException {
        return readZkDatas(REGISTERED_PATH);
    }

    /**
     * Get the list of the running servers in zk. Should have the same number as configure.
     *
     * @return The return content will be : <b>znodeName</b>, <b>hostName</b>, <b>serverIndex</b>, ,
     *         <b>serverPort</b>, <b>znodeContent</b>, <b>nodeStartTime</b>, <b>nodeEndTime</b>
     * @throws KeeperException
     * @throws InterruptedException
     */
    private List<String[]> readZKServers() throws KeeperException, InterruptedException {
        return readZkDatas(RUNNING_PATH);
    }

    private List<String[]> readZkDatas(String path) throws KeeperException, InterruptedException {
        List<String[]> zkServers = new ArrayList<String[]>();
        try {
            List<String> children = zkc.getChildren(path, null);

            String fullPath = null;
            Stat stat = null;
            String strData = null;
            String[] childInfo = null;
            List<String> list = new ArrayList<String>();
            for (String child : children) {
                list.clear();
                list.add(child);
                childInfo = child.split(SPLIT);
                Collections.addAll(list, childInfo);

                fullPath = path + "/" + child;
                stat = zkc.exists(fullPath, false);
                if (stat == null) {
                    continue;
                }
                strData = new String(zkc.getData(fullPath, false, stat));

                childInfo = strData.split(SPLIT);
                Collections.addAll(list, childInfo);
                list.add(stat.getCtime() + "");
                list.add(stat.getMtime() + "");
                zkServers.add(list.toArray(new String[list.size()]));
            }
        } finally {
        }
        return zkServers;
    }

    /**
     * Get the list of the masters in zk. Actually it has only one master in one time.
     * 
     * @return The return content will be : <b>znodeName:port::</b>, <b>znodeContent</b>,
     *         <b>nodeStartTime</b>, <b>nodeEndTime</b> <br>
     * @throws KeeperException
     * @throws InterruptedException
     */
    private List<String[]> readZKMasters() throws KeeperException, InterruptedException {
        return readZkDatas(MASTER_PATH);
    }

    /**
     * Get the list of the elect masters in zk.
     * 
     * @return The return content will be : <b>znodeName</b>, <b>znodeContent</b>, <b>nodeStartTime</b>,
     *         <b>nodeEndTime</b> <br>
     * @throws KeeperException
     * @throws InterruptedException
     */
    private List<String[]> readZKLeaders() throws KeeperException, InterruptedException {
        return readZkDatas(LEADER_PATH);
    }

    /**
     * Read the <b>servers</b> file, which store the configuration of dcsservers
     * 
     * @return The return content will be : <b>hostName</b>, <b>serverIndex</b>, <b>mxosrvrCount</b>
     *         <br>
     *         <b>hostName</b> is in little case.<br>
     *         <b>serverIndex</b> start with 1.<br>
     *         <b>mxosrvrCount</b> means the total mxosrvrs number of the node.<br>
     * @throws IOException
     */
    private List<String[]> readServersConf() throws IOException {
        List<String[]> servers = new ArrayList<String[]>();

        InputStream is = this.getClass().getResourceAsStream("/servers");
        if (is == null)
            throw new IOException("Cannot find servers file.");

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;
        int lineNum = 1;
        while ((line = br.readLine()) != null) {
            Scanner scn = new Scanner(line);
            scn.useDelimiter("\\s+");
            String hostName = null;
            String serverCount = null;
            if (scn.hasNext())
                hostName = scn.next();// host name
            else
                hostName = new String("localhost");
            if (scn.hasNext())
                serverCount = scn.next();// optional
            else
                serverCount = "1";
            scn.close();
            if (hostName.equalsIgnoreCase("localhost")) {
                hostName = InetAddress.getLocalHost().getHostName();
            }
            servers.add(new String[] {hostName.toLowerCase(), lineNum + "", serverCount});
            lineNum++;
        }

        if (servers.size() < 1)
            throw new IOException("No entries found in servers file");
        return servers;
    }

    /**
     * Read the <b>masters</b> file, which store the configuration of dcsmasters
     * 
     * @return The return content will be : <b>hostName</b> <br>
     *         <b>hostName</b> is in little case.<br>
     * @throws IOException
     */
    private List<String> readMastersConf() throws IOException {
        List<String> masters = new ArrayList<String>();

        InputStream is = this.getClass().getResourceAsStream("/masters");
        if (is == null)
            throw new IOException("Cannot find servers file.");

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = br.readLine()) != null) {
            Scanner scn = new Scanner(line);
            scn.useDelimiter("\\s+");
            String hostName = null;
            if (scn.hasNext())
                hostName = scn.next();// host name
            else
                hostName = new String("localhost");
            scn.close();
            if (hostName.equalsIgnoreCase("localhost")) {
                hostName = InetAddress.getLocalHost().getHostName();
            }
            masters.add(hostName.toLowerCase());
        }
        if (masters.size() < 1)
            throw new IOException("No entries found in masters file");
        return masters;
    }


    private static CommandLine initCMD(String[] args) {
        // create Options object
        Options options = new Options();

        Option option = OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator()
                .withDescription("Lightweight Scroll restart. " + System.lineSeparator() + "a : restart all."
                        + System.lineSeparator() + "d=<int> : restart mxosrvrs which start more than <int> days."
                        + System.lineSeparator() + "s=<serverName> : restart mxosrvrs running on <serverName>."
                        + System.lineSeparator()
                        + "m=<nodeIndex:moxpid> : restart the specified mxosrvr."
                        + System.lineSeparator()
                        + "n=<node1,node2,node3...> : restart specified node or nodes available state mxosrvr.")
                .create("S");
        options.addOption(option);
        
        Option opt_te = OptionBuilder.withArgName("node=nodeIndex").hasArgs(2).withValueSeparator()
                .withDescription("use Enable to open specified node or nodes"
                        + System.lineSeparator() + "usage: n=1,2,3...")
                .create("E");
        options.addOption(opt_te);

        Option opt_td = OptionBuilder.withArgName("node=nodeIndex").hasArgs(2).withValueSeparator()
                .withDescription("use toDisable to close specified node or nodes"
                        + System.lineSeparator() + "usage: n=1,2,3...")
                .create("D");
        options.addOption(opt_td);
        
        Option opt_gcn = OptionBuilder.withArgName("node=nodeIndex").hasArgs(2).withValueSeparator()
                .withDescription("use getnum to get specified node or nodes' connected numbles"
                        + System.lineSeparator() + "usage: n=1,2,3...")
                .create("G");
        options.addOption(opt_gcn);
        
        Option opt_gs = OptionBuilder.withArgName("node=nodeIndex").hasArgs(2).withValueSeparator()
                .withDescription("use getstartednum to get specified node or nodes' started numbers"
                        + System.lineSeparator() + "usage: n=1,2,3...")
                .create("GS");
        options.addOption(opt_gs);

        Option opt_info = OptionBuilder.withArgName("ismobility").hasArgs(1).withValueSeparator()
                .withDescription("use get status when last operation have not finished, show some mxo's details if ismobility  = true"
                        + System.lineSeparator() + "usage: true or false")
                .create("I");
        options.addOption(opt_info);

        options.addOption("R", "restore", false, "Load restore for connection which is not work for a long time.");
        options.addOption("B", "rebalance", false, "Load balance for all client connection.");
        options.addOption("H", "help", false, "print help msg");

        CommandLine cmd = null;
        try {
            cmd = new GnuParser().parse(options, args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(100);
            formatter.printHelp("ZK Shell Tool", options);
            System.exit(0);
        }

        if (cmd.hasOption("H")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ZK Shell Tool", options);
            System.exit(0);
        }

        return cmd;
    }
    
    private void storeSRingMsgs(Map<String, List<String[]>> mxoMsgsMap, String[] mxosrvr) {
        if (!mxoMsgsMap.containsKey(mxosrvr[1])) {
            List<String[]> oneMxoMsgs = new ArrayList<String[]>();
            String[] strTmp = new String[] {mxosrvr[3], mxosrvr[4], mxosrvr[5],
                    mxosrvr[6], mxosrvr[7], mxosrvr[8], mxosrvr[9]};
            oneMxoMsgs.add(strTmp);
            mxoMsgsMap.put(mxosrvr[1], oneMxoMsgs);
        } else {
            List<String[]> oneMxoMsgs = mxoMsgsMap.get(mxosrvr[1]);
            String[] strTmp = new String[] {mxosrvr[3], mxosrvr[4], mxosrvr[5],
                    mxosrvr[6], mxosrvr[7], mxosrvr[8], mxosrvr[9]};
            if (Collections.binarySearch(oneMxoMsgs, strTmp,
                    new Comparator<String[]>() {
                        public int compare(String[] o1, String[] o2) {
                            int result = 1;
                            if (Arrays.equals(o1, o2)) {
                                result = 0;
                            }
                            return result;
                        }
                    }) != 0) {
                oneMxoMsgs.add(strTmp);
                mxoMsgsMap.put(mxosrvr[1], oneMxoMsgs);
            }
        }
    }
    
    private void storeMxoStats(String realData, String realFile) throws IOException {

        OutputStreamWriter osw =
                new OutputStreamWriter(new FileOutputStream(statDataPath + realFile));
        osw.write(realData, 0, realData.length());
        osw.flush();

        osw.close();
    }

    private void mkDirectory(String realFile) {
        File file = null;
        file = new File(statDataPath);
        if (!file.exists()) {
            String message = "Please check if your environment is running";
            printErrorMsg("ERROR:", message);
            System.exit(1);
        }
        File writeFile = new File(statDataPath, realFile);
        try {
            if (!writeFile.exists()) {
                writeFile.createNewFile();
            } else {
                deleteFile(realFile);
                writeFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getMxoStats(String realFile) {

        String data = null;
        try {
            File delFile = new File(statDataPath + realFile);
            if (!delFile.isFile() && !delFile.exists()) {
                String msg = "you have not execute scrollRestart script, please do it first";
                printErrorMsg("WARN", msg);
                System.exit(0);
            }
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(statDataPath + realFile)));
            data = br.readLine();
            if (data == null) {
                String msg = "All connections are transferred ";
                printErrorMsg("WARN", msg);
                deleteFile(realFile);
                System.exit(0);
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    private void deleteFile(String realFile) {
        try {
            File delFile = new File(statDataPath + realFile);
            if (delFile.isFile() && delFile.exists()) {
                delFile.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String[] sortNodeList(String[] nodeList) {
        int[] arr = Arrays.stream(nodeList).mapToInt(Integer::parseInt).sorted().toArray();
        String[] tmp = new String[arr.length];
        for (int k = 0; k < arr.length; k++) {
            tmp[k] = String.valueOf(arr[k]);
        }
        return tmp;
    }
}
