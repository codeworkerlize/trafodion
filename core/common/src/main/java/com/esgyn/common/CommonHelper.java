// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Scanner;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.common.zookeeper.ZkClient;


/**
 * Common Helper class that has methods used by other clasess
 *
 */

public class CommonHelper {
    private static final Logger _LOG = LoggerFactory.getLogger(CommonHelper.class);
    
    /**
     * Returns the EsgynDB Root Znode
     * @return
     */
    public static String getTrafRootZNode() {
        String trafRootZnode = Constants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;
        try {
            trafRootZnode = System.getenv("TRAF_ROOT_ZNODE");
            if(trafRootZnode == null || trafRootZnode.length() < 1) {
                trafRootZnode = Constants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;
            }
        } catch (Exception e) {
        }
        return trafRootZnode;
    }
    
    /**
     * Returns the current active DCS Master host name
     * @return
     */
    public static String getActiveDcsMasterHost() {
        ZkClient zkClient = new ZkClient();
        ZooKeeper zk = null;
        String activeDcsMasterHost = "";
        
        try {
            zk = zkClient.connect();
            String instanceId = System.getenv("TRAF_INSTANCE_ID");
            if(instanceId == null)
            	instanceId = "1";
            
            String path = getTrafRootZNode() + "/" + instanceId + "/dcs/master";
            _LOG.info("Looking up active dcs master using znode : " + path);
            List<String> dcsMasters = zk.getChildren(path, false);
            if(dcsMasters.size() > 0) {
                String[] attributes = dcsMasters.get(0).split(":"); 
                activeDcsMasterHost = attributes[0];
            }
        }catch(Exception ex) {
            _LOG.error("Failed to get active dcs master : " + ex.getMessage());
        }
        return activeDcsMasterHost;
    }
    
    /**
     * Get the node name this process is running on
     * @return
     */
    public static String getLocalNodeName() {
        String hostName = System.getenv("HOSTNAME");
        if(hostName == null) {
            try (Scanner s = new Scanner(Runtime.getRuntime().exec("hostname").getInputStream()).useDelimiter("\\A")) {
                hostName= s.hasNext() ? s.next() : "";
            }
            catch (IOException e) {
                
            }           
        }
        return hostName;
    }
    
    /**
     * Checks if the current node is the active dcs master node
     * @return
     */
    public static boolean isLocalNodeActiveDcsMasterNode() {
        String localHostName = getLocalNodeName();
        if(localHostName != null && localHostName.indexOf(".") >=0) {
            localHostName = localHostName.substring(0, localHostName.indexOf("."));
        }
        String activeDcsMasterHost = getActiveDcsMasterHost();
        if(activeDcsMasterHost != null && activeDcsMasterHost.indexOf(".") >=0) {
            activeDcsMasterHost = activeDcsMasterHost.substring(0, activeDcsMasterHost.indexOf("."));
        }
        return localHostName != null && activeDcsMasterHost != null && localHostName.equalsIgnoreCase(activeDcsMasterHost);
    }
    
    /**
     * Run shell command on the cluster
     * @param args
     * @return
     * @throws Exception
     */
     static String runShellCommand(List<String> args) throws Exception {
        String result = "";
        ProcessBuilder pb = new ProcessBuilder(args);
        Process process = pb.start();
        
        String stdOut = null;
        StringBuilder sbo = new StringBuilder();
        BufferedReader bro = new BufferedReader(new InputStreamReader(process.getInputStream(), Charset.defaultCharset()));
        try {
            while((stdOut = bro.readLine()) != null){
                sbo.append(stdOut +  System.getProperty("line.separator"));
            }
        } finally {
            if(bro!=null){
                bro.close();
            }
        }
        String stdErr = null;
        BufferedReader bre = new BufferedReader(new InputStreamReader(process.getErrorStream(), Charset.defaultCharset()));
        StringBuilder sbe = new StringBuilder();
        try {
            while((stdErr = bre.readLine()) != null){
                sbe.append(stdErr +  System.getProperty("line.separator"));
            }
        } finally {
            if(bre !=null){
                bre.close();
            }
        }
        
        int errCode = process.waitFor();
        if(errCode == 0) {
            result = sbo.toString();
        }else{
            throw new Exception(sbe.toString());
        }        
        return result;
    }
    
    public static void main(String[] args) {
        System.out.println("Active Dcs Master Host Name : " + getActiveDcsMasterHost());
        System.out.println("Local Host Name : " + getLocalNodeName());
        System.out.println("Running on active dcs master node : " + isLocalNodeActiveDcsMasterNode());
    }
}
