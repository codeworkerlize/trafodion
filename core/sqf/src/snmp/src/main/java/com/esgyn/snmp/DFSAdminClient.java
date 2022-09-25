package com.esgyn.snmp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.tools.DFSAdmin;

import java.sql.Timestamp;

public class DFSAdminClient {

    public static final String COMMAND = "-shutdownDatanodeByNamenode";
    public static final String PARAMETER = "onlyracks";
    public static final String PARMTERROR = "Usage: hdfs dfsadmin"
            + " [-shutdownDatanodeByNamenode <datanode_host> [onlyracks]]";
    private Configuration conf;
    private DFSAdmin dfsAdmin;
    private boolean isStartOnlyRack = false;
    private String[] parameters;
    private FileSystem fs;

    protected DFSAdminClient() {
    }

    public DFSAdminClient(Configuration conf) {
        this.conf = conf;
        this.dfsAdmin = new DFSAdmin(conf);
    }

    public DFSAdminClient(Configuration conf, DFSAdmin dfsAdmin) {
        this.conf = conf;
        this.dfsAdmin = dfsAdmin;
    }

    private boolean removeBadNodes(String... parameters) {
        boolean removesuccess = false;
        int exitCode = -1;
        if (parameters == null) {
            System.out.println(PARMTERROR);
            return removesuccess;
        }

        try {
            exitCode = dfsAdmin.run(parameters);
            if (exitCode == 0) {
                removesuccess = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            removesuccess = false;
        }

        return removesuccess;
    }

    public boolean removeBadNode(String hostname) {
        if (hostname == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": Enter deleteData - ERROR: input parameter is null. Returning."
            );
            return false;
        }
        if (isStartOnlyRack) {
            return removeBadNodes(COMMAND, hostname, PARAMETER);
        }
        return removeBadNodes(COMMAND, hostname);
    }



    public void setStartOnlyRack(boolean startOnlyRack) {
        isStartOnlyRack = startOnlyRack;
    }
}