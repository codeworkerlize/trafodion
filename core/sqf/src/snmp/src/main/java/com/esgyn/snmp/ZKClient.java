package com.esgyn.snmp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import java.io.*;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZKClient implements Abortable {
    public static final String startMeta = "stopmeta";
    public static final int WAIT_META = 200;
    public static final int TIME_OUT_META = 1000;
    public boolean isDeleteMeta = true;
    public Configuration m_config;
    public ZooKeeperWatcher m_zkw;
    Map<String, String> lv_map;
    public String m_hbase_root_zknode = "/hbase";
    public String m_trafodion_root_zknode = "/trafodion";
    public static String m_env_traf_log;
    public static String m_env_traf_conf;

    static {
        long start=System.currentTimeMillis();
        m_env_traf_log = System.getenv("TRAF_LOG");
        if (m_env_traf_log == null) {
            m_env_traf_log = "";
        }
        System.out.println("env variable TRAF_LOG: " + m_env_traf_log);

        m_env_traf_conf = System.getenv("TRAF_CONF");
        if (m_env_traf_conf == null) {
            m_env_traf_conf = "";
        }
        System.out.println("env variable TRAF_CONF: " + m_env_traf_conf+",cost="+(System.currentTimeMillis()-start));
    }

    public ZKClient(Configuration p_config) throws IOException {
        m_config = p_config;
        m_zkw = new ZooKeeperWatcher(m_config, "Receiver", this, true);
    }

    /**
     * @return
     * @throws KeeperException
     */
    public List<String> getChildren(String p_zkNode) throws KeeperException {
        return ZKUtil.listChildrenNoWatch(m_zkw, p_zkNode);
    }

    /**
     * @return void
     * @throws KeeperException
     */
    public boolean deleteZKNode(String p_node_name) {

        if (p_node_name == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": Enter deleteZKNode - ERROR: input parameter is null. Returning."
            );
            return false;
        }
        if ((lv_map == null) ||
                (lv_map.size() < 1)) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": deleteZKNode"
                    + ", input node name: " + p_node_name
                    + ", ERROR: Could not get the mapping to help find the HBase ZK Node to delete. Returning."
            );
            return false;
        }

        String lv_rs_ZKNode = lv_map.get(p_node_name);
        if (lv_rs_ZKNode == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": deleteZKNode"
                    + ", input node name: " + p_node_name
                    + ", ERROR: Could not find the HBase ZK Node to delete. Returning."
            );
            return false;
        }

        String lv_zk_node_to_delete = m_hbase_root_zknode + "/rs" + "/" + lv_rs_ZKNode;
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": Going to delete the ZK Node: " + lv_zk_node_to_delete
        );

        try {
            ZKUtil.deleteNodeRecursively(m_zkw,
                    lv_zk_node_to_delete
            );
        } catch (KeeperException ke) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": deleteZKNode - ERROR - KeeperException while deleting the ZK node: " + lv_zk_node_to_delete
            );
            return false;
        }

        return true;
    }
    /**
    * @return void
    * @throws KeeperException
    */
    public void deleteMasterZKNode(String p_node_name) {
        if (p_node_name == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": Enter deleteZKNode - ERROR: input parameter is null. Returning."
            );
	    return;
        }
        boolean isMaster = deleteIfEquals(m_zkw, p_node_name);
        if(isMaster){
            System.out.println(new Timestamp(System.currentTimeMillis())
                    + ": Going to delete the Master ZK Node: " + p_node_name
            );
        }else{
            System.out.println(new Timestamp(System.currentTimeMillis())
                    + p_node_name +" is not the Master ,So not been deleted "
            );
        }

    }

    /**
     * delete the master znode if its content is same as the parameter
     * @param zkw must not be null
     * @param content must not be null
    */
    public static boolean deleteIfEquals(ZooKeeperWatcher zkw, final String content) {
        if (content == null){
            throw new IllegalArgumentException("Content must not be null");
        }

        try {
            Stat stat = new Stat();
            byte[] data = ZKUtil.getDataNoWatch(zkw, zkw.getMasterAddressZNode(),stat);
            ServerName sn = ServerName.parseFrom(data);
            System.out.println(new Timestamp(System.currentTimeMillis())+
                    " master znode is "+(sn==null?null:sn.toString())
           );
            if (sn != null && sn.toString().contains(content)) {
                return (ZKUtil.deleteNode(zkw, zkw.getMasterAddressZNode(), stat.getVersion()));
            }
        } catch (KeeperException e) {
            System.out.println(new Timestamp(System.currentTimeMillis())+
                    "Can't get or delete the master znode"+e
            );
        } catch (DeserializationException e) {
            System.out.println(new Timestamp(System.currentTimeMillis())+
                    "Can't get or delete the master znode"+e
            );
        }

        return false;
    }

    public boolean deleteRunningZKNode(String p_node_name) {

        if (p_node_name == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": Enter deleteRunningZKNode - ERROR: input parameter is null. Returning."
            );
            return false;
        }

        String lv_zk_node_to_delete = m_trafodion_root_zknode+"/"+"1/cluster/running"+"/"+ p_node_name;

        try {
            if(-1==ZKUtil.checkExists(m_zkw,lv_zk_node_to_delete)) {
                System.err.println(new Timestamp(System.currentTimeMillis())
                        + ": Enter deleteRunningZKNode is not exist:"+lv_zk_node_to_delete
                );
                return false;
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": Going to delete the ZK Node: " + lv_zk_node_to_delete
        );

        try {
            ZKUtil.deleteNodeRecursively(m_zkw,
                    lv_zk_node_to_delete
            );
        } catch (KeeperException ke) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": deleteZKNode - ERROR - KeeperException while deleting the ZK node: " + lv_zk_node_to_delete
            );
            return false;
        }

        return true;
    }
    public boolean deleteMetaLocation(String p_node_name, int replicaId) {
        boolean success = true;
        String metaNode = m_zkw.getZNodeForReplica(replicaId);

        if (p_node_name == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": Enter deleteMeta - ERROR: input parameter is null. Returning."
            );
            return false;
        }

        //Check the bad node in zk against the desired hostname from ping detection
        if (!matchMetaZnode(p_node_name, replicaId)) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": Meta has not been on down node . Returning."
            );
            return false;
        }
        if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
            System.out.println(new Timestamp(System.currentTimeMillis())
                    + ": Start Deleting hbase:meta region location in ZooKeeper [" + metaNode + "]");

        } else {
            // This feature is not currently available
            System.out.println(new Timestamp(System.currentTimeMillis())
                    + ": Deleting hbase:meta for " + replicaId + " region location in ZooKeeper [" + metaNode + "]");
        }

        // Just delete the node.  Don't need any watches.
        try {
            ZKUtil.deleteNode(m_zkw, metaNode);
        } catch (KeeperException e) {
            success = false;
            switch (e.code()) {
                case NONODE:
                    System.err.println(new Timestamp(System.currentTimeMillis()) + " Node " + metaNode + " already deleted");
                    break;
                default:
                    System.err.println(new Timestamp(System.currentTimeMillis()) + " deleteZKNode - ERROR - KeeperException while deleting the ZK node: " + metaNode);
            }
        }
        return success;
    }

    public boolean matchMetaZnode(String p_node_name, int replicaId) {
        boolean match = false;
        ServerName metaRegionLocation = recordMetaLocation(replicaId, TIME_OUT_META);
        if (metaRegionLocation == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": could not find  Meta info on ZKNode"
            );
            return match;
        }

        String hostname = metaRegionLocation.getHostname();
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": hbase:meta on Node From ZK " + "[" + hostname + "]");

        if (hostname == null) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ", Meat on ZKNode " + p_node_name
                    + ", ERROR: Could not find the HBase Meta ZK Node to delete. Returning."
            );
            return match;
        }
        if (hostname.toLowerCase().equals(p_node_name)) {
            System.out.println(new Timestamp(System.currentTimeMillis())
                    + ": hbase:meta on Down Node From ZK " + "[" + hostname + "]");
            match = true;
            return match;
        }

        return match;
    }


    public ServerName recordMetaLocation(int replicaId, int timeout) {

        if (timeout < 0) throw new IllegalArgumentException();
        MetaTableLocator metaTableLocator = new MetaTableLocator();
        long startTime = System.currentTimeMillis();
        ServerName sn = null;
        while (true) {
            sn = metaTableLocator.getMetaRegionLocation(m_zkw, replicaId);
            if (sn != null || (System.currentTimeMillis() - startTime)
                    > timeout - WAIT_META) {
                break;
            }
            try {
                Thread.sleep(WAIT_META);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return sn;
    }

    public void printRSNodes() {
        System.out.println(new Timestamp(System.currentTimeMillis()) + ": Enter printRSNodes");

        String lv_rs_zknode = m_hbase_root_zknode + "/rs";

        List<String> lv_children = null;
        try {
            lv_children = getChildren(lv_rs_zknode);
        } catch (KeeperException ke) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": MyZKClient.printRSNodes - ERROR - KeeperException while getting the children of the ZK node: "
                    + lv_rs_zknode
            );
        }
        System.out.println(new Timestamp(System.currentTimeMillis())
                + ": printRSNodes, Number of ZK Nodes under " + lv_rs_zknode
                + ": " + ((lv_children == null) ? "0" : lv_children.size())
        );

        for (String lv_child : lv_children) {
            System.out.println(lv_child);
        }

        System.out.println(new Timestamp(System.currentTimeMillis()) + ": Exit printRSNodes");
    }

    public Map<String, String> getLv_map() {
        return lv_map;
    }

    public Map<String, String> mapRSNodesFromZK() {
        System.out.println(new Timestamp(System.currentTimeMillis()) + ": Enter mapRSNodesFromZK.");

        String lv_rs_zknode = m_hbase_root_zknode + "/rs";

        List<String> lv_children = null;
        try {
            lv_children = getChildren(lv_rs_zknode);
        } catch (KeeperException ke) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": MyZKClient.mapRSNodesFromZK - ERROR - KeeperException while getting the children of the ZK node: "
                    + lv_rs_zknode
            );
            return null;
        }

        lv_map = new HashMap<String, String>();

        for (String lv_child : lv_children) {
            String[] lv_split = lv_child.split(",");
            if (lv_split[0] != null) {
                lv_map.put(lv_split[0], lv_child);
            }
        }

        System.out.println(new Timestamp(System.currentTimeMillis()) + ": Exit mapRSNodesFromZK."
                + ", map size: " + lv_map.size()
        );
        return lv_map;
    }

   /*
    public Map<String,String> mapRSNodesFromFile()
    {
        System.out.println(new Timestamp(System.currentTimeMillis()) + ": Enter mapRSNodesFromFile.");

        Map<String,String> lv_map = new HashMap<String,String>();

        String lv_rs_zknode_file = ZKClient.m_env_traf_log + "/" + "hbase_rs_zk_list";
        File lv_file = new File(lv_rs_zknode_file);
        BufferedReader lv_br = null;

        try {
            lv_br = new BufferedReader(new FileReader(lv_file));
        }
        catch (FileNotFoundException fnfe) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": File not found: " + lv_rs_zknode_file
            );
            return null;
        }

        String lv_line;
        try {
            while ((lv_line = lv_br.readLine()) != null) {
                String[] lv_split = lv_line.split(",");
                if (lv_split[0] != null) {
                    lv_map.put(lv_split[0], lv_line);
                }
            }
        }
        catch (IOException ioe) {
            System.err.println(new Timestamp(System.currentTimeMillis())
                    + ": IO Exception: " + ioe
                    + " , while reading the file: " + lv_rs_zknode_file
            );
        }

        System.out.println(new Timestamp(System.currentTimeMillis()) + ": Exit mapRSNodesFromFile."
                + ", map size: " + lv_map.size()
        );
        return lv_map;
    }
*/

    public void abort(String arg0, Throwable arg1) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.Abortable#isAborted()
     */

    public boolean isAborted() {
        // TODO Auto-generated method stub
        return false;
    }

}
