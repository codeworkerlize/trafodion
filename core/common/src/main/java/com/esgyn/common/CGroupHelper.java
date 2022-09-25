// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.common.zookeeper.DistributedZkLock;
import com.esgyn.common.zookeeper.ZkClient;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class CGroupHelper {
    
    public static enum ActionType {
        ADD, ALTER, DELETE, CHECK;
    }
    
    private static final Logger _LOG = LoggerFactory.getLogger(CGroupHelper.class);
    public static final String ESGYNDB_PARENT_CGROUP_NAME = "ESGYNDB";
    public static final String ESGYN_SYSTEM_TENANT = "ESGYNDB";
    public static final long BYTES_PER_GB = 1024 * 1024 * 1024L;
    private static final Object lock = new Object();
    public static final String ESGYN_CGP_CPU     = "ESGYN_CGP_CPU";
    public static final String ESGYN_CGP_CPUACCT = "ESGYN_CGP_CPUACCT";
    public static final String ESGYN_CGP_MEM     = "ESGYN_CGP_MEM";
    
    //The percent of real tenant memory used in cgroup creation. The balance is left for infrastructure processes.
    //If tenant gets 1 compute unit of 32GB, the cgroup is created with limit 0.95 * 32GB.
    private static final double REAL_TENANT_MEMORY_PERCENT = 95.0;
    
    // Several lists are maintained here as global variables.
    // These are listed in the order in which they are created.
    // Since other processes may have changed tenants, they need to
    // be refreshed, when needed, by calling the refresh method.

    // list of node names of the cluster, computed by getEsgynNodeStatus()
    private static LinkedHashMap<String, Integer> esgynNodes = new LinkedHashMap<String, Integer>();
    private static LinkedHashMap<Integer, String> esgynNodesById = new LinkedHashMap<Integer, String>();
    private static int CLUSTER_SIZE = -1;

    // List of the total number of cores in esgyn nodes, computed by getNumberCoresInEachNode()
    private static HashMap<Integer, Integer> numCoresInNodeList = new HashMap<Integer, Integer>();

    // List of the total max memory in esgyn nodes, computed by getTotalMemoryInNodes()
    private static HashMap<Integer, Long>totalMemoryInNodeList = new HashMap<Integer, Long>();
    
    // cgroup information for each node, computed by loadEsgynCGroupInfo()
    private static HashMap<Integer, Integer> totalCpuSharesPerNodeList = new HashMap<Integer, Integer>();
    private static HashMap<Integer, Integer> esgynCpuSharesPerNodeList = new HashMap<Integer, Integer>();
    private static HashMap<Integer, Long>esgynMemoryBytesPerNodeList = new HashMap<Integer, Long>();

    // allocated and available slices for each node in the cluster
    // limits are computed by getMaxSlicesFromCGroups()
    // maintained by the addToTenantNodeAllocation() and removeFromTenantNode() methods
    private static NodeUnitCounters existingTenants = null;

    // list of allowed tenant sizes for an Adaptive Segmentation tenant,
    // based on the size of the cluster, computed lazily by getComputeFactors()
    private static ArrayList<Integer> COMPUTE_FACTORS = new ArrayList<Integer>();

    // all tenants and their node allocations, computed by refresh() and checkAndCreateLocalTenantCGroups()
    // maintained by addToTenantNodeAllocation() and removeFromTenantNodeAllocation()
    private static HashMap<String, NamedTenantNodes> tenantNodeAllocations = new HashMap<String, NamedTenantNodes>();

    // definitions of a tenant unit or slice, note that the
    // same definitions also exist in file core/sql/common/NAWNodeSet.h
    private static int CORES_PER_SLICE = 4;
    private static int MEMORY_GB_PER_SLICE = 32;
    private static int MIN_SLICES_PER_TENANT = 4;
  // private static int TOTAL_CPU_SHARES = 1024;
    
    static {
    	try {
    		//Load EsgynDB cluster information
    		loadEsgynClusterInfo();
    	} catch (Exception e) {
    		_LOG.error("Failed to cluster information",e.getMessage());
    	}
    }

    public static int getClusterSize() {
    	return loadEsgynClusterInfo();
    }

    /** Does this system use virtual nodes
     *
     * At this time, virtual nodes are only used in development
     * systems and all nodes are virtual in that case. Since
     * the shell commands used return duplicate node names in such
     * environments, we need to make the node ids unique in some
     * other way.
     * @return
     */
    public static int getNumVirtualNodes() {
        String virtualNodes = System.getenv("SQ_VIRTUAL_NODES");
        int numVirtualNodes = 0;

        try {
            if (virtualNodes != null)
                numVirtualNodes = Integer.parseInt(virtualNodes);
        }
        catch (Exception e) {
            numVirtualNodes = 0;
        }

        return numVirtualNodes;
    }
    
    /** Get a list of Esgyn node names
     * @return
     */
    public static ArrayList<String> getEsgynNodeNames(){
    	return new ArrayList<String>(esgynNodes.keySet());
    }
    /**
     * Get the number of cores in a given node
     * @return
     */
    public static int getNumberOfCores(int nodeID) {
    	if (numCoresInNodeList.size() < 1) {
    		getNumberCoresInEachNode();
    	}
    	return numCoresInNodeList.containsKey(nodeID) ? numCoresInNodeList.get(nodeID) : 32;
    }

    /**
     * Given a node name, get its node id
     * @param nodeName
     * @return
     */
    public static int getNodeIDForNodeName(String nodeName) {
        String adjustedNodeName = nodeName;
        if (nodeName.contains(".")) {
          int periodIndex = adjustedNodeName.indexOf(".");
          if (periodIndex > 0)
            adjustedNodeName = nodeName.substring(0, periodIndex);
        }

    	Integer v = esgynNodes.get(adjustedNodeName);

        if (v != null)
            return v;
    	return -1;
    }
    
    /**
     * Given a node ID, get its node name
     * @param nodeID
     * @return
     */
    public static String getNodeNameForNodeID(int nodeID) {
        return esgynNodesById.get(nodeID);
    }
   
    /** Get the number of cores in the current node */
    private static void getNumberCoresInEachNode() {
        synchronized(lock) {
            try {
        		numCoresInNodeList = new HashMap<Integer, Integer>();
        		List<String> args = new ArrayList<String>();
        		args.addAll(Arrays.asList("edb_pdsh", "-a", "nproc", "--all"));
    
        		String output = CommonHelper.runShellCommand(args);
        		String[] lines = output.split(System.getProperty("line.separator"));
                int numVirtualNodes = getNumVirtualNodes();
    
        		for (String line : lines) {
        		    String[] tokens = line.split(":");
        		    if (tokens.length > 1) {
        		        int ni = getNodeIDForNodeName(tokens[0].trim());
                            if (numVirtualNodes > 1)
                                ni = numCoresInNodeList.size();
        		        if (ni >= 0) {
        		            numCoresInNodeList.put(ni, Integer.parseInt(tokens[1].trim()));
        		        }
        		    }
        		}
        	} catch (Exception ex) {
                    //System.out.println(String.format("Failed to determine the number of cores : %1$s", ex.getMessage()));
        		_LOG.error(String.format("Failed to determine the number of cores : %1$s", ex.getMessage()));
        	}
        }
    }

    /**
     * Find the total max memory in each of the Esgyn nodes
     */
    private static void getTotalMemoryInNodes() {
    	if (totalMemoryInNodeList.size() != 0) return;

        synchronized(lock) {
            totalMemoryInNodeList = new HashMap<Integer, Long>();
        	try {
        		List<String> args = new ArrayList<String>();
        		args.addAll(Arrays.asList("edb_pdsh", "-a", "grep", "MemTotal", "/proc/meminfo"));
                        int numVirtualNodes = getNumVirtualNodes();
        		String output = CommonHelper.runShellCommand(args);
        		String[] lines = output.split(System.getProperty("line.separator"));
        		for (String line : lines) {
        			String[] tokens = line.split(":");
        			if (tokens.length > 1) {
        			    int ni = getNodeIDForNodeName(tokens[0].trim());
                                    if (numVirtualNodes > 1)
                                            ni = totalMemoryInNodeList.size();
        			    if (ni >= 0) {
        			        String memoryValue = tokens[2].trim();
                                        int spaceIndex = memoryValue.indexOf(" ");
        			        if (spaceIndex > 0)
                                            memoryValue = memoryValue.substring(0, spaceIndex);
        			        totalMemoryInNodeList.put(ni, 1024 * Long.parseLong(memoryValue.trim()));
        			    }
        			}
        		}
        	} catch (Exception ex) {
        		_LOG.error(String.format("Failed to determine the total memory : %1$s", ex.getMessage()));
        	}
        }
    }

    /**
     * Load the esgyn cluster info
     * @return number of nodes in the cluster
     */
    public static int loadEsgynClusterInfo() {
    	if (CLUSTER_SIZE < 0) {
    		try {
    			loadEsgynNodeList();
    		} catch (Exception e) {
    			_LOG.error(e.getMessage());
    		}
    	}
    	return CLUSTER_SIZE;
    }

    /**
     * Convert a dense node id into an actual node id
     * @param position a number between 0 and getClusterSize() - 1
     *        to identify a dense node id
     * @return the node id (same as position if the node ids are contiguous)
     */
    public static int getNodeId(int position) {
        return existingTenants.id(position);
    }

    /**
     * Refresh all the internal variables
     *
     * This method clears all the internal variables that cache the
     * state of the cluster and brings them in sync with zookeeper.
     * Since other processes may create/drop/alter tenants and since
     * nodes can go up or down or be added or dropped, this method
     * should be called before relying on the cached state.
     */
    public static void refresh() throws Exception {
        refresh(true);
    }

    /**
     * Refresh all the internal variables
     *
     * This method clears all the internal variables that cache the
     * state of the cluster and brings them in sync with zookeeper or
     * with the metadata. Since other processes may create/drop/alter
     * tenants and since nodes can go up or down or be added or dropped,
     * this method should be called before relying on the cached state.
     *
     * @param useZK load the state from zookeeper (true) or from the
     *        metadata tables (false)
     */
    public static void refresh(boolean useZK) throws Exception {
        synchronized(lock) {
            // refresh CLUSTER_SIZE, create existingTenants
            // with just the list of nodes, no tenants added yet
        	loadEsgynNodeList();
            loadEsgynCGroupInfo();
            getMaxSlicesFromCGroups();
            tenantNodeAllocations.clear();
            COMPUTE_FACTORS.clear();
    
            ArrayList<NamedTenantNodes> tenants = fetchTenantsFromZK(false);
    
            for(NamedTenantNodes tenant : tenants)
                if(!tenant.getName().equals(CGroupHelper.ESGYN_SYSTEM_TENANT))
                    addToTenantNodeAllocation(tenant, null);
        }
    }

    public static NodeUnitCounters getTenantUtilization(boolean refreshData)
        throws Exception {
        if (refreshData)
            refresh();
        return existingTenants;
    }
    
    /**
     * Load the list of Esgyn node names and node id's.
     * @return
     */
    public static void fetchEsgynNodeNames() {
    	if (CLUSTER_SIZE < 0) {
    		try {
    			loadEsgynNodeList();
    		} catch (Exception e) {
    			_LOG.error(e.getMessage());
    		}
    	}
    }
    /**
     * Fetch the list of Esgyn nodes and their status
     * @return
     * @throws Exception
     */
/*    private static LinkedHashMap<String, String> getEsgynNodeStatus() throws Exception{
        LinkedHashMap<String, String> nodeStatus = new LinkedHashMap<String, String>();
        synchronized(lock) {
            try{
                List<String> args = new ArrayList<String>();
                args.addAll(Arrays.asList("trafnodestatus","-j"));
                String result = CommonHelper.runShellCommand(args);
                JsonFactory factory = new JsonFactory();
                ObjectMapper mapper = new ObjectMapper(factory);
                JsonNode jnode = mapper.readTree(result);
                if(jnode instanceof ArrayNode) {
                    ArrayNode nodes = (ArrayNode)jnode;
                    esgynNodes = new LinkedHashMap<String, Integer>();
                    esgynNodesById = new LinkedHashMap<Integer, String>();
                    CLUSTER_SIZE = nodes.size();
                    int [] nodeIds = new int[CLUSTER_SIZE];
                    int n = 0;
                    for(JsonNode node : nodes) {
                        nodeStatus.put(node.get("NODE_NAME").textValue(), node.get("STATUS").textValue());
                        String nodeName = node.get("NODE_NAME").textValue();
                        if (nodeName.contains(".")) {
                            int periodIndex = nodeName.indexOf(".");
                            if (periodIndex > 0)
                              nodeName = nodeName.substring(0, periodIndex);
                        }
                        int nodeId = node.get("NODE_ID").asInt();
                        esgynNodes.put(nodeName, nodeId);
                        esgynNodesById.put(nodeId, nodeName);
                        nodeIds[n++] = nodeId;
                    }
                    // the limits are computed later by getMaxSlicesFromCGroups()
                    existingTenants = new NodeUnitCounters(nodeIds, 0);
                }
                
            }catch (Exception ex) {
                //System.out.println("exception: " + ex.getMessage());
                throw new Exception (String.format("Failed to fetch node status : %1$s", ex.getMessage()));
            }
        }
        return nodeStatus;            
    }
 */
    
    /**
     * Fetch the list of Esgyn nodes
     * @return
     * @throws Exception
     */
    private static void loadEsgynNodeList() throws Exception{
        synchronized(lock) {
            try{
                List<String> args = new ArrayList<String>();
                args.addAll(Arrays.asList("trafconf","-node"));
                String result = CommonHelper.runShellCommand(args);
        		String[] lines = result.trim().split(System.getProperty("line.separator"));
                CLUSTER_SIZE = lines.length;
                esgynNodes = new LinkedHashMap<String, Integer>();
                esgynNodesById = new LinkedHashMap<Integer, String>();
                int [] nodeIds = new int[CLUSTER_SIZE];
                int n = 0;
        		
        		for(String line : lines) {
        			String[] tokens = line.split("[=;]+");
                    String nodeName = tokens[3];
                    if (nodeName.contains(".")) {
                        int periodIndex = nodeName.indexOf(".");
                        if (periodIndex > 0)
                          nodeName = nodeName.substring(0, periodIndex);
                    }
                    int nodeId = Integer.parseInt(tokens[1]);
                    esgynNodes.put(nodeName, nodeId);
                    esgynNodesById.put(nodeId, nodeName);
                    nodeIds[n++] = nodeId;
                }
                // the limits are computed later by getMaxSlicesFromCGroups()
                existingTenants = new NodeUnitCounters(nodeIds, 0);
                
            }catch (Exception ex) {
                throw new Exception (String.format("Failed to fetch nodes list : %1$s", ex.getMessage()));
            }
        }
    }    
    
    public static void setCoresPerSlice(int cores) {
        CORES_PER_SLICE = cores;
    }
    
    public static void setMemoryGBPerSlice(int memGB) {
        MEMORY_GB_PER_SLICE = memGB;
    }
    
    public static void setMinSlicesPerTenant(int slices) {
        MIN_SLICES_PER_TENANT = slices;
    }
    
    /**
     * Node Allocation for all tenants
     * @return
     */
    public static HashMap<String, NamedTenantNodes> getTenantNodeAllocations(){
        return tenantNodeAllocations;
    }
    
    /**
     * Node Allocation for a given tenant as a list of node names
     * @return
     */
    public static ArrayList<String> getNodeNamesForTenant(String tenantName){
        NamedTenantNodes ntn = getTenantNodesByTenantName(tenantName);
        ArrayList<String> result = new ArrayList<String>();

        if (ntn != null) {
                TenantNodes tn = ntn.getTenantNodes();
                int nn = tn.getNumNodes();

                for (int n=0; n<nn; n++) {
                	String nodeName = getNodeNameForNodeID(tn.getDistinctNodeId(n));
                	if(nodeName != null && nodeName.length() > 0) {
                		result.add(nodeName);
                	}
                }
            }
        
        return result;
    }
    
    /**
     * Create a Parent cgroup
     * @param parentCGroupName
     * @param cpuPercent A percent value of Esgyns CPU shares
     * @param memPercent A percent value of Esgyns memory capacity
     * @param nodes empty for local node, otherwise -a or -w options to
     *        specify a set of nodes where this command should be executed
     *        (note that -w expects a comma-separated list of nodes)
     * @return
     * @throws Exception
     */
    public static boolean createCGroup(String parentCGroupName,
                                       int cpuPercent,
                                       int memPercent,
                                       String nodes) throws Exception{
        return createCGroup(parentCGroupName, "", cpuPercent, memPercent, nodes);
    }
    /**
     * Create a child cgroup
     * @param parentCGroupName Parent cgroup name
     * @param cGroupName Child cgroup name
     * @param cpuPercent A percent value of Esgyns CPU shares
     * @param memPercent A percent value of Esgyns memory capacity
     * @param nodes empty for local node, otherwise -a or -w options to
     *        specify a set of nodes where this command should be executed
     *        (note that -w expects a comma-separated list of nodes)
     * @return
     * @throws Exception
     */
    public static boolean createCGroup(String parentCGroupName,
                                       String cGroupName,
                                       int cpuPercent,
                                       int memPercent,
                                       String nodes) throws Exception{
        return createCGroup(parentCGroupName, cGroupName, cpuPercent, memPercent, "", "", nodes);
    }

    /***
     * Create a child cgroup
     * @param parentCGroupName Parent cgroup name
     * @param cGroupName Child cgroup name
     * @param cpuPercent A percent value of Esgyns CPU shares
     * @param memPercent A percent value of Esgyns memory capacity
     * @param userName Username who will own the child cgroup
     * @param groupName Group owernship for the child group
     * @param nodes empty for local node, otherwise -a or -w options to
     *        specify a set of nodes where this command should be executed
     *        (note that -w expects a comma-separated list of nodes)
     * @return
     * @throws Exception
     */
    public static boolean createCGroup(String parentCGroupName,
                                       String cGroupName,
                                       int cpuPercent,
                                       int memPercent,
                                       String userName,
                                       String groupName,
                                       String nodes) throws Exception{
        try{
            List<String> args = new ArrayList<String>();

            if (nodes.isEmpty())
                // single node
                args.add("edb_cgroup_cmd");
            else {
                // some or all nodes
                args.add("edb_cgroup");
                args.addAll(Arrays.asList(nodes.split(" ")));
            }
            args.addAll(Arrays.asList(
                    "--add", "--pcgrp", parentCGroupName,  
                    "--cpu-pct", String.valueOf(cpuPercent), 
                    "--mem-pct", String.valueOf(memPercent)));
            if(cGroupName != null && cGroupName.length() > 0){
                args.addAll(Arrays.asList("--ccgrp", cGroupName));
            }
            if(userName != null && userName.length() > 0){
                args.addAll(Arrays.asList("--user", userName));
            }
            if(groupName != null && groupName.length() > 0){
                args.addAll(Arrays.asList("--group", groupName));
            }
            ProcessBuilder pb = new ProcessBuilder(args);
            Process process = pb.start();
            
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
            if(errCode != 0) {
                throw new Exception(sbe.toString());
            }
        }catch (Exception ex) {
            throw new Exception (String.format("Failed to create cgroup %1$s : %2$s", cGroupName, ex.getMessage()));
        }
        return true;
    }
    
    /**
     * Create tenant cgroup on the local node
     *
     * @param tenantName tenant name
     * @throws Exception
     */
    public static void createNodeLocalTenantCGroup( String tenantName,
            boolean printDiagnostics) throws Exception {
        DistributedZkLock dl = null;
        ZooKeeper zk = null;

        try {
            // acquire a distributed lock for this tenant on this node, so that only one
            // process creates the cgroup
            ZkClient zkClient = new ZkClient();
            String zkLockDir =
                TenantHelper.TENANT_ZNODE_PARENT + "/" + tenantName + TenantHelper.TENANT_ZNODE_LOCKS;
            int localNodeNum = getLocalNodeNum();
            String lockName = "cgroup_node_" + Integer.toString(localNodeNum);

            zk = zkClient.connect();
            dl = new DistributedZkLock(zk, zkLockDir, lockName, true);

            if (dl.lock(480000) != 0) // 8 minutes timeout
                throw new Exception("Unable to obtain lock to create the cgroup for tenant " +
                        tenantName + " on node " + Integer.toString(localNodeNum));

            // check whether the cgroup exists now, another process may have created
            // it in the meantime
            File cgroupFileCPU     = new File(System.getenv(ESGYN_CGP_CPU) + "/" + tenantName);
            File cgroupFileCPUAcct = new File(System.getenv(ESGYN_CGP_CPUACCT) + "/" + tenantName);
            File cgroupFileMem     = new File(System.getenv(ESGYN_CGP_MEM) + "/" + tenantName);

            if (!cgroupFileCPU.exists() ||
                !cgroupFileCPUAcct.exists() ||
                !cgroupFileMem.exists()) {

                // for this short method, we only need a subset of the global variables
                // this could be optimized later
                refresh();

                NamedTenantNodes tenant = tenantNodeAllocations.get(tenantName);

                if (tenant == null)
                    throw new Exception("Tenant " + tenantName + " not found in zookeeper");

                TenantNodes tn = tenant.getTenantNodes();
                int computeUnits = tn.getSize();
                int tenantNumNodes = tn.getNumNodes();
                int limit = Math.max(existingTenants.getDenseLimit(localNodeNum), 1);
                int percentage = 0;

                // find the number of units on the local node, if any
                for (int i=0; i<tenantNumNodes; i++) {
                    if (tn.getDistinctNodeId(i) == localNodeNum) {
                        percentage = (100 * tn.getDistinctWeight(i)) / limit;
                        percentage = Math.min(Math.max(percentage, 1), 100);
                        percentage = (int)Math.round((REAL_TENANT_MEMORY_PERCENT * percentage)/100);
                        if (printDiagnostics)
                            System.out.format(
                                    "Tenant %s is assigned to this node (%d) and gets %d units, equal to %d percent.\n",
                                    tenantName,
                                    localNodeNum,
                                    tn.getDistinctWeight(i),
                                    percentage);
                        break;
                    }
                }

                if (percentage <= 0) {
                    // tenant is not usually assigned to this node
                    percentage = calculateAvgPercentage(computeUnits,
                                                        limit);
                    if (printDiagnostics)
                        System.out.format(
                                "Tenant %s is not assigned to this node (%d) and gets %d percent.\n",
                                tenantName,
                                localNodeNum,
                                percentage);
                }

                CGroupHelper.createCGroup(ESGYNDB_PARENT_CGROUP_NAME,
                                          tenant.getName(),
                                          percentage,
                                          percentage,
                                          "");
            } // cgroup does not yet exist
            else if (printDiagnostics) {
                System.out.format("The cgroup for tenant %s already exists.\n",tenantName);
            }
        } catch (Exception e) {
    		String errorPrefix = "Failed to create local tenant cgroup for " + tenantName + ", reason: ";
    		Exception ee = new Exception (errorPrefix + e.getMessage());
    		_LOG.error(ee.getMessage());
    		if (printDiagnostics) {
    		    System.out.println(e.getMessage());
    		    e.printStackTrace();
    		}
    		throw ee;
    	}
        finally {
            try {
                if (dl != null)
                    dl.unlock();
                if (zk != null)
                    zk.close();
            }
            catch (Exception e) {
            }
        }
    }
    
    /**
     * Read tenant information from Zookeeper and create the tenant cgroup on the local node
     * Each node can call this to recreate the tenant cgroups at start up time.
     * @param printDiagnostics
     * @throws Exception
     */
    public static void createNodeLocalTenantCGroups(
            boolean printDiagnostics) throws Exception {
			
    	loadEsgynNodeList();
    	ArrayList<NamedTenantNodes> tenants = fetchTenantsFromZK(true);
    	loadLocalEsgynCGroupInfo();
    	
    	String tenantName = "";
    	for(NamedTenantNodes tenant : tenants) {
	        try {
	        	tenantName = tenant.getName();
	            // check whether the cgroup exists now, another process may have created
	            // it in the meantime
	            File cgroupFileCPU     = new File(System.getenv(ESGYN_CGP_CPU) + "/" + tenantName);
	            File cgroupFileCPUAcct = new File(System.getenv(ESGYN_CGP_CPUACCT) + "/" + tenantName);
	            File cgroupFileMem     = new File(System.getenv(ESGYN_CGP_MEM) + "/" + tenantName);
	            int localNodeNum = getLocalNodeNum();
	            
	            if (!cgroupFileCPU.exists() ||
	                !cgroupFileCPUAcct.exists() ||
	                !cgroupFileMem.exists()) {
	
	                TenantNodes tn = tenant.getTenantNodes();
	                int tenantNumNodes = tn.getNumNodes();
	                int percentage = 0;
	
	                // find the number of units on the local node, if any
	                for (int i=0; i<tenantNumNodes; i++) {
	                	int nodeNum = tn.getDistinctNodeId(i);
	                    if (nodeNum == localNodeNum) {
	    	                int weight = tn.getDistinctWeight(i);
	    	                if(esgynMemoryBytesPerNodeList.containsKey(nodeNum)) {
	                            long esgynMemory = esgynMemoryBytesPerNodeList.get(nodeNum);
	                            long tenantMemory = weight * MEMORY_GB_PER_SLICE * BYTES_PER_GB;
		                        percentage = (int)Math.round((REAL_TENANT_MEMORY_PERCENT * tenantMemory) / esgynMemory);
		                        if(percentage > 0 && percentage <= 100 ) {
			                        if (printDiagnostics)
			                            System.out.format(
			                                    "Tenant %s is assigned to this node (%d) and gets %d units, equal to %d percent.\n",
			                                    tenantName,
			                                    localNodeNum,
			                                    tn.getDistinctWeight(i),
			                                    percentage);
			    	                
			                    	
			    	                CGroupHelper.createCGroup(ESGYNDB_PARENT_CGROUP_NAME,
			    	                                         tenant.getName(),
			    	                                         percentage,
			    	                                         percentage,
			    	                                         "");			                        
			                        
		    	                }else {
		    	                	if(percentage <= 0)
		    	                		_LOG.error("ERROR : Computed Cgroup allocation percent is less than 0% for tenant "+ tenantName);
		    	                	if(percentage > 100)
		    	                		_LOG.error("ERROR : Computed Cgroup allocation percent " + percentage + "% is greater than 100% for tenant "+ tenantName);
		    	                				
		    	                }
		                        break;
	    	                }else {
	    	                	_LOG.warn("WARN : Esgyn root memory cgroup not available in the current host");
	    	                }
	                    }
	                }
	                

	            } // cgroup does not yet exist
	            else if (printDiagnostics) {
	                System.out.format("The cgroup for tenant %s already exists.\n",tenantName);
	            }
	        } catch (Exception e) {
	    		String errorPrefix = "Failed to create local tenant cgroup for " + tenantName + ", reason: ";
	    		Exception ee = new Exception (errorPrefix + e.getMessage());
	    		_LOG.error(ee.getMessage());
	    		if (printDiagnostics) {
	    		    System.out.println(e.getMessage());
	    		    e.printStackTrace();
	    		}
	    		throw ee;
	    	}
    	}
    }
    /**
     * Modify a Parent cgroup
     * @param parentCGroupName
     * @param cpuPercent A percent value of Esgyns CPU shares
     * @param memPercent A percent value of Esgyns memory capacity
     * @param nodes empty for local node, otherwise -a or -w options to
     *        specify a set of nodes where this command should be executed
     *        (note that -w expects a comma-separated list of nodes)
     * @return
     * @throws Exception
     */
    public static boolean modifyCGroup(String parentCGroupName,
                                       int cpuPercent,
                                       int memPercent,
                                       String nodes) throws Exception{
        return modifyCGroup(parentCGroupName, "", cpuPercent, memPercent, nodes);
    }
    
    /**
     * Modify a child cgroup
     * @param parentCGroupName Parent cgroup name
     * @param cGroupName Child cgroup name
     * @param cpuPercent A percent value of Esgyns CPU shares
     * @param memPercent A percent value of Esgyns memory capacity
     * @param nodes empty for local node, otherwise -a or -w options to
     *        specify a set of nodes where this command should be executed
     *        (note that -w expects a comma-separated list of nodes)
     * @return
     * @throws Exception
     */
    public static boolean modifyCGroup(String parentCGroupName,
                                       String cGroupName,
                                       int cpuPercent,
                                       int memPercent,
                                       String nodes) throws Exception{
        return modifyCGroup(parentCGroupName, cGroupName, cpuPercent, memPercent, "", "", nodes);
    }
    /***
     * Modify a child cgroup
     * @param parentCGroupName Parent cgroup name
     * @param cGroupName Child cgroup name
     * @param cpuPercent A percent value of Esgyns CPU shares
     * @param memPercent A percent value of Esgyns memory capacity
     * @param userName Username who will own the child cgroup
     * @param groupName Group owernship for the child group
     * @param nodes empty for local node, otherwise -a or -w options to
     *        specify a set of nodes where this command should be executed
     *        (note that -w expects a comma-separated list of nodes)
     * @return
     * @throws Exception
     */    
    public static boolean modifyCGroup(String parentCGroupName, String cGroupName, int cpuPercent, int memPercent,
            String userName, String groupName, String nodes) throws Exception{
        try{
            List<String> args = new ArrayList<String>();

            if (nodes.isEmpty())
                // single node
                args.add("edb_cgroup_cmd");
            else {
                // some or all nodes
                args.add("edb_cgroup");
                args.addAll(Arrays.asList(nodes.split(" ")));
            }
            args.addAll(Arrays.asList(
                    "--modify", "--pcgrp", parentCGroupName,  
                    "--cpu-pct", String.valueOf(cpuPercent), 
                    "--mem-pct", String.valueOf(memPercent)));
            if(cGroupName != null && cGroupName.length() > 0){
                args.addAll(Arrays.asList("--ccgrp", cGroupName));
            }
            if(userName != null && userName.length() > 0){
                args.addAll(Arrays.asList("--user", userName));
            }
            if(groupName != null && groupName.length() > 0){
                args.addAll(Arrays.asList("--group", groupName));
            }
            
            ProcessBuilder pb = new ProcessBuilder(args);
            Process process = pb.start();
            
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
            if(errCode != 0) {
                throw new Exception(sbe.toString());
            }
        }catch (Exception ex) {
            throw new Exception (String.format("Failed to modify cgroup %1$s : %2$s", cGroupName, ex.getMessage()));
        }
        return true;
    }
    
    /**
     * Delete a parent cgroup on all nodes
     * @param parentCGroupName
     * @return
     * @throws Exception
     */
    public static boolean deleteCGroup(String parentCGroupName) throws Exception{
        return deleteCGroup(parentCGroupName, "");
    }
    
    /***
     * Delete a child cgroup on all nodes
     * @param parentCGroupName
     * @param cGroupName
     * @return
     * @throws Exception
     */
    public static boolean deleteCGroup(String parentCGroupName, String cGroupName) throws Exception{
        try{
            List<String> args = new ArrayList<String>();
            args.addAll(Arrays.asList("edb_cgroup","--delete", "-a", "--pcgrp", parentCGroupName));
            if(cGroupName != null && cGroupName.length() > 0){
                args.addAll(Arrays.asList("--ccgrp", cGroupName));
            }
            ProcessBuilder pb = new ProcessBuilder(args);
            Process process = pb.start();
            
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
            if(errCode != 0) {
                throw new Exception(sbe.toString());
            }
        }catch (Exception ex) {
            throw new Exception (String.format("Failed to delete cgroup %1$s : %2$s", cGroupName, ex.getMessage()));
        }
        return true;
    }
    
    /**
     * Get CGroup information on local node as json string
     * @param parentCGroupName
     * @param cGroupName
     * @param recursive
     * @return
     * @throws Exception
     */
    public static String getLocalCGroup(String parentCGroupName, String cGroupName, boolean recursive) throws Exception{
        String result = "";
        try{
            List<String> args = new ArrayList<String>();
            String edbCommand = recursive ? "edb_cgroup" : "edb_cgroup_cmd";
            args.addAll(Arrays.asList(edbCommand,"--get", "--pcgrp", parentCGroupName));
            if(cGroupName != null && cGroupName.length() > 0){
                args.addAll(Arrays.asList("--ccgrp", cGroupName));
            }
            result = CommonHelper.runShellCommand(args);

        }catch (Exception ex) {
            throw new Exception (String.format("Failed to display cgroup %1$s : %2$s", cGroupName, ex.getMessage()));
        }
        return result;        
    }

    
    /**
     * Get CGroup information as json string
     * @param parentCGroupName
     * @param cGroupName
     * @param allNodes
     * @return
     * @throws Exception
     */
    public static String getCGroup(String parentCGroupName, String cGroupName, boolean allNodes) throws Exception{
        String result = "";
        try{
            List<String> args = new ArrayList<String>();
            String edbCommand = allNodes ? "edb_cgroup" : "edb_cgroup_cmd";
            args.addAll(Arrays.asList(edbCommand,"--get", "--pcgrp", parentCGroupName));
            if (allNodes)
                args.add("-a");
            if(cGroupName != null && cGroupName.length() > 0){
                args.addAll(Arrays.asList("--ccgrp", cGroupName));
            }
            result = CommonHelper.runShellCommand(args);

        }catch (Exception ex) {
            throw new Exception (String.format("Failed to display cgroup %1$s : %2$s", cGroupName, ex.getMessage()));
        }
        return result;        
    }
    
    /**
     * Get the cgroup information recursively including child cgroups
     * @param parentCGroupName
     * @param cGroupName
     * @param allNodes
     * @return
     * @throws Exception
     */
    public static String getCGroupRecursive(String parentCGroupName, String cGroupName, boolean allNodes) throws Exception{
        String result = "";
        try{
            List<String> args = new ArrayList<String>();
            String edbCommand = allNodes ? "edb_cgroup" : "edb_cgroup_cmd";
            args.addAll(Arrays.asList(edbCommand,"--getr", "--pcgrp", parentCGroupName));
            if (allNodes)
                args.add("-a");
            if(cGroupName != null && cGroupName.length() > 0){
                args.addAll(Arrays.asList("--ccgrp", cGroupName));
            }
            result = CommonHelper.runShellCommand(args);

        }catch (Exception ex) {
            throw new Exception (String.format("Failed to display cgroup %1$s : %2$s", cGroupName, ex.getMessage()));
        }
        return result;        
    }
    /**
     * Get resource capacity of each node in the cluster
     * @param allNodes
     * @return
     * @throws Exception
     */
    public static String getClusterResources(boolean allNodes) throws Exception{
        String result = "";
        try{
            List<String> args = new ArrayList<String>();
            String edbCommand = allNodes ? "edb_cgroup" : "edb_cgroup_cmd";
            args.addAll(Arrays.asList(edbCommand,"--si"));
            if (allNodes)
                args.add("-a");
            result = CommonHelper.runShellCommand(args);
        }catch (Exception ex) {
            throw new Exception (String.format("Failed to fetch cluser resource : %1$s", ex.getMessage()));
        }
        return result;        
    }
    

    
    /**
     * Fetch the Esgyn Service level CGroup information in the current node
     * The CPU and memory limits for Esgyn Service 
     */
    private static void loadEsgynCGroupInfo(){
        synchronized(lock) {
            JsonFactory factory = new JsonFactory();
            ObjectMapper mapper = new ObjectMapper(factory);
            int numVirtualNodes = getNumVirtualNodes();
            totalCpuSharesPerNodeList = new HashMap<Integer, Integer>();
            esgynCpuSharesPerNodeList = new HashMap<Integer, Integer>();
            esgynMemoryBytesPerNodeList = new HashMap<Integer, Long>();
            try {
                String esgynCGroupAllNodes =
                        CGroupHelper.getCGroup(CGroupHelper.ESGYNDB_PARENT_CGROUP_NAME, "", true);
                String[] esgynCGroups = esgynCGroupAllNodes.split(System.getProperty("line.separator"));
                if(esgynCGroups.length < 1) {
                	_LOG.error("ERROR : Failed to find Esgyn root cgroup information");
                }
                for (String line : esgynCGroups) {
                    try {
                        int sep = line.indexOf(":");
                        String[] tokens = new String[2];
                        tokens[0] = line.substring(0, sep);
                        tokens[1] = line.substring(sep + 1);
                        if (tokens.length > 1) {
                            int ni = getNodeIDForNodeName(tokens[0].trim());
                            if (numVirtualNodes > 1)
                                ni = esgynCpuSharesPerNodeList.size();
                            if(ni >=0 ) {
                                JsonNode node = mapper.readTree(tokens[1].trim());
                                if(node instanceof ArrayNode){
                                    JsonNode firstNode = ((ArrayNode)node).get(0);
                                    totalCpuSharesPerNodeList.put(ni, firstNode.get("total.cpu.shares").intValue());
                                    esgynCpuSharesPerNodeList.put(ni, firstNode.get("cpu.shares").intValue());
                                    esgynMemoryBytesPerNodeList.put(ni, firstNode.get("memory.limit_in_bytes").longValue());
                                }else{
                                    totalCpuSharesPerNodeList.put(ni, node.get("total.cpu.shares").intValue());
                                    esgynCpuSharesPerNodeList.put(ni, node.get("cpu.shares").intValue());
                                    esgynMemoryBytesPerNodeList.put(ni, node.get("memory.limit_in_bytes").longValue());
                                }
                            }
                        }
                    } catch (Exception ex1) {
                        _LOG.error("Error reading Esgyn cgroup information for node" +
                                line + ": " + ex1.getMessage());      
                    }
                }

                CGroupHelper.getNumberCoresInEachNode();
                CGroupHelper.getTotalMemoryInNodes();
            } catch (Exception ex2) {
                _LOG.error("Error reading Esgyn cgroup information : " + ex2.getMessage());
            }
        }
    }
    
    /**
     * Fetch the Esgyn Service level CGroup information on the current node
     * The CPU and memory limits for Esgyn Service 
     */
    private static void loadLocalEsgynCGroupInfo(){
        synchronized(lock) {
            JsonFactory factory = new JsonFactory();
            ObjectMapper mapper = new ObjectMapper(factory);
            int numVirtualNodes = getNumVirtualNodes();
            try {
            	String[] esgynCGroups = new String[0];
        		String hostName = CommonHelper.getLocalNodeName();
        		esgynCGroups = new String[] { CGroupHelper.getLocalCGroup(CGroupHelper.ESGYNDB_PARENT_CGROUP_NAME, "", false)};
        		if(esgynCGroups.length < 1) {
                	_LOG.error("ERROR : Failed to find Esgyn root cgroup information");
                }
                for (String line : esgynCGroups) {
                    try {
                        int ni = getNodeIDForNodeName(hostName);
                        if (numVirtualNodes > 1)
                            ni = esgynCpuSharesPerNodeList.size();
                        if(ni >=0 ) {
                            JsonNode node = mapper.readTree(line);
                            if(node instanceof ArrayNode){
                                JsonNode firstNode = ((ArrayNode)node).get(0);
                                esgynCpuSharesPerNodeList.put(ni, firstNode.get("cpu.shares").intValue());
                                esgynMemoryBytesPerNodeList.put(ni, firstNode.get("memory.limit_in_bytes").longValue());
                            }else{
                                esgynCpuSharesPerNodeList.put(ni, node.get("cpu.shares").intValue());
                                esgynMemoryBytesPerNodeList.put(ni, node.get("memory.limit_in_bytes").longValue());
                            }
                        }else {
                        	_LOG.error("Error resolving node name to node id");
                        }
                    } catch (Exception ex1) {
                        _LOG.error("Error reading Esgyn cgroup information for node" +
                                line + ": " + ex1.getMessage());      
                    }
                }
            } catch (Exception ex2) {
                _LOG.error("Error reading Esgyn cgroup information : " + ex2.getMessage());
            }
        }
    }
    
    public static int getTotalCPUSharesInNode(int nodeID) {
        Integer result = totalCpuSharesPerNodeList.get(nodeID);

        if (result != null)
            return result;

        // not found, assume 1024 total shares
        return 1024;
    }

    /**
     * Compute the max number of slices available in each node,
     * based on number of cores, cpu shares and memory
     */
    public static void getMaxSlicesFromCGroups() {
        synchronized(lock) {
            for (int d=0; d<CLUSTER_SIZE; d++) {
                int unitLimit = 0;
                // convert the index into a node id
                int nodeId = getNodeId(d);

                try {
                    //Compute the number of cpu share slices for each node.
                    int totalCPUShares   = getTotalCPUSharesInNode(nodeId);
                    int cpuSharePerCore  = totalCPUShares / getNumberOfCores(nodeId);
                    int cpuSlicesPerNode = esgynCpuSharesPerNodeList.get(nodeId) /
                        Math.max(cpuSharePerCore * CORES_PER_SLICE, 1);

                    // Get max memory assigned for Esgyn per node.
                    // Allow for a small percentage loss, since the total memory
                    // in /proc/meminfo is usually slightly below the physical
                    // memory installed.
                    float memBytes = MEMORY_GB_PER_SLICE * 0.98f * BYTES_PER_GB;

                    Long availableMemoryCG = esgynMemoryBytesPerNodeList.get(nodeId);
                    Long availableMemoryOS = totalMemoryInNodeList.get(nodeId);
                    long availableMemory = 0;
                    int memSlicesPerNode = 1;

                    if (availableMemoryCG != null || availableMemoryOS != null)
                        if (availableMemoryCG != null && availableMemoryOS != null)
                            availableMemory = Math.min(availableMemoryCG.longValue(),
                                                       availableMemoryOS.longValue());
                        else
                            availableMemory = (availableMemoryCG != null
                                               ? availableMemoryCG
                                               : availableMemoryOS);
                        
                    //Compute the number of memory slices for each node.
                    memSlicesPerNode = (int) (availableMemory / memBytes);

                    unitLimit = Math.max(Math.min(cpuSlicesPerNode, memSlicesPerNode), 1);

                }
                catch (NullPointerException e) {
                    // If some nodes are down, we will throw an exception.
                    // Set the limit for those nodes to zero and therefore
                    // exclude them from the choice. When the nodes come up,
                    // it might make sense to rebalance some of the tenants.
                    // Note that this mainly affects RG tenants.
                }
                existingTenants.setDenseLimit(nodeId, unitLimit);
            }
        }
    }

    /**
     * Finds the max possible resource slice per node based on the Esgyn system level allocation
     *
     * @param nodeId id of the node to check
     * @return max number of slices (aka cubes, units) that node nodeId can hold,
     *         ignoring any already allocated tenants
     */
    public static int getMaxSlicesPerNode(int nodeId) {

        return existingTenants.getLimitById(nodeId);
    }

    /**
     * Return total number of slices available on the cluster
     * @return Number of slices available on all the nodes of
     *         the cluster
     */
    public static int getMaxSlicesPerCluster() {
        return existingTenants.getSumOfLimits();
    }

    /**
     * Determine the compute factors based on the cluster size
     * @return An array of possible compute sizes
     * @throws Exception
     */
    public static ArrayList<Integer> getComputeFactors() throws Exception {
        if (CLUSTER_SIZE < 0) {
            try {
            	loadEsgynNodeList();
            }catch (Exception ex) {
            }
        }
        if(COMPUTE_FACTORS.isEmpty() && CLUSTER_SIZE > 0) {
            int totalSlices =  CLUSTER_SIZE;
            // get factors up to CLUSTER_SIZE
            for (int i = 1; i < totalSlices; i++) {
                i = ASNodes.getNextPossibleTenantSize(i, totalSlices);
                if(i >= MIN_SLICES_PER_TENANT)
                    COMPUTE_FACTORS.add(i);
            }
            // assume a homogeneous cluster
            int maxWeight = getMaxSlicesPerNode(getNodeId(0));
            // factors > CLUSTER_SIZE and <= maxWeight
            for (int i=2; i <= maxWeight; i++){
                COMPUTE_FACTORS.add(CLUSTER_SIZE * i);
            }
        }
        return COMPUTE_FACTORS;
    }

    /**
     * Determine the tenant node affinity
     * @param computeUnits
     * @param overrideWarning
     * @return affinity
     * @throws Exception
     */
    public static int getNodeAffinity(int computeUnits, boolean overrideWarning) throws Exception{
        int affinity = 0;

        ASNodes asn = new ASNodes(computeUnits,existingTenants.length(), -1);

        //Use Adaptive segmentation logic to determine the node affinity
        boolean exceedsLimit = asn.placeTenant(existingTenants, false);
        affinity = asn.getAffinity();

        //Check if the new allocation causes the Esgyn level cgroup threshold to go over
        if (!overrideWarning && exceedsLimit){
            String errorMsg = String.format("NODE_QUOTA_EXCEEDED: Compute allocation of %1$d units with affinity %2$d will exceed Esgyn quota. Max allowed units per node: %3$d.",
                    computeUnits, affinity, getMaxSlicesPerNode(getNodeId(0)));

            _LOG.error(errorMsg);
            //If over threshold, throw a warning to caller. They can resubmit request with override if they want to proceed.
            throw new Exception(errorMsg);
        }

        return affinity;
    }
    
    /**
     * Update tenant affinity for the new compute units requested
     *
     * Note: This method does not actually update the resources of the tenant,
     * it just computes an affinity. The caller will need to call
     * removeFromTenantNodeAllocation() and addToTenantNodeAllocation() to do
     * the actual update. If oldTenant already has an affinity assigned, that
     * value will be returned.
     *
     * @param oldTenant existing tenant, already recorded in existingTenants
     * @param newTenant parameters to which we want to update the existing tenant
     * @return affinity of the updated tenant (also set int newTenant) or -1
     *         if the tenant is not based on an affinity
     * @throws Exception
     */
    public static int updateTenantAffinity(NamedTenantNodes oldTenant,
                                           TenantNodes newTenant,
                                           boolean overrideWarning) throws Exception{
        int affinity = -1;
        boolean exceedsLimit = false;

        if (existingTenants.length() < 1) {
            throw new Exception("CGroupHelper.updateTenantAllocation called when existingTenants array is empty.");
        }

        if (newTenant instanceof ASNodes) {
            ASNodes newASNodes = (ASNodes) newTenant;

            affinity = newASNodes.getAffinity();

            if (affinity < 0) {

                //Remove the current allocation so we don't double
                //count old and new
                existingTenants.subtractTenant(oldTenant.getTenantNodes());

                try {
                    //Use Adaptive segmentation logic to determine the
                    //node affinity. Don't update the counters, the
                    //caller will do that by calling
                    //removeFromTenantNodeAllocation() and
                    //addToTenantNodeAllocation()
                    exceedsLimit = newASNodes.placeTenant(existingTenants, false);
                    affinity = newASNodes.getAffinity();
                } finally {
                    // undo our operation
                    existingTenants.addTenant(oldTenant.getTenantNodes());
                }

                //Check if the new allocation causes the Esgyn level
                //cgroup threshold to go over
                if (!overrideWarning && exceedsLimit){
                    String errorMsg = String.format(
                        "NODE_QUOTA_EXCEEDED: Compute allocation of %1$d units with affinity %2$d will exceed Esgyn quota. Max allowed units per node: %3$d.",
                        newTenant.getSize(), affinity, getMaxSlicesPerNode(getNodeId(0)));

                    _LOG.info(errorMsg);
                    //If over threshold, throw a warning to
                    //caller. They can resubmit request with override
                    //if they want to proceed.
                    throw new Exception(errorMsg);
                }
            }
        }

        return affinity;    
    }

    /**
     * Compute and Store tenant's node allocation in local cache, this includes
     * updating the list of nodes and weights used as well as storing a list
     * of node names
     * @param tenant a description of the tenant and its assigned nodes
     * @return true if adding the tenant caused oversubscription, false otherwise
     */
    public static boolean addToTenantNodeAllocation(NamedTenantNodes tenant,
                                                    OversubscriptionInfo osi){
        boolean result = false;

        synchronized(lock) {
            result = existingTenants.addTenant(tenant.getTenantNodes());
            _LOG.info(String.format("Tenant %1$s allocation array (id:cnt:limit,...) : %2$s", tenant.getName(), existingTenants.toString()));
            tenantNodeAllocations.put(tenant.getName(), tenant);
            if (osi != null)
                if (result)
                    existingTenants.getOversubscriptionInfo(osi, tenant.getTenantNodes());
                else
                    osi.init();
        }

        return result;
    }
    
    /**
     * Remove the tenant's allocation from the in-memory per node allocation objects
     * @param oldTenant a description of the tenant and its assigned nodes
     */
    public static void removeFromTenantNodeAllocation(NamedTenantNodes oldTenant) throws Exception {
        String tenantName = oldTenant.getName();

        if (existingTenants.length() < 1) {
            throw new Exception("CGroupHelper.removeTenantAllocation called when existingTenants array is empty.");
        }
        synchronized(lock) {
            existingTenants.subtractTenant(oldTenant.getTenantNodes());
            _LOG.info(String.format("Tenant allocation array reset : %1$s", existingTenants.toString()));
            if(tenantNodeAllocations.containsKey(tenantName)){
                _LOG.info(String.format("Removing tenant node allocation for %1$s", tenantName));
                tenantNodeAllocations.remove(tenantName);
            }
        }
    }
    
    /**
     * Get list of nodes allocated for tenant
     * @param tenantName
     * @return
     */
    public static NamedTenantNodes getTenantNodesByTenantName(String tenantName){
        synchronized(lock) {
           if(!tenantNodeAllocations.containsKey(tenantName))
                _LOG.debug("No tenant node allocation found for " + tenantName);
            return tenantNodeAllocations.get(tenantName);
        }
    }

    /**
     * Get the node number this process is running on
     *
     * @return local node number
     */
    public static int getLocalNodeNum() {
        String hostName = CommonHelper.getLocalNodeName();
        if(hostName != null) {
        	hostName = hostName.trim();
	        int dotIndex = hostName.indexOf(".");
	        if (dotIndex > 0)
	            hostName = hostName.substring(0, dotIndex);
	        Integer result = esgynNodes.get(hostName);
	
	        if (result != null)
	            return result;
        }
        return -1;
    }
    

    
    /**
     * Create/Alter the tenant level cgroup
     * @param action ADD, ALTER or CHECK
     * @param tenant tenant name and its allocations
     * @param allNodes local node only or entire cluster
     * @param currentState for CHECK, the current values
     *        of the cgroup as returned by getCGroupRecursive(),
     *        use this only for CHECK and for local node
     * @throws Exception
     */
    public static void addAlterTenantCGroup(ActionType action,
                                            NamedTenantNodes tenant,
                                            boolean allNodes,
                                            ArrayNode currentState) throws Exception{
        TenantNodes tn = tenant.getTenantNodes();
    	int computeUnits = tn.getSize();
    	int tenantNumNodes = tn.getNumNodes();
        int percentages[] = new int[CLUSTER_SIZE];
        boolean samePercentageOnAllNodes = true;
        int commonPercentage = -1;
        int localNodeNum = getLocalNodeNum();
        for (int p=0; p<CLUSTER_SIZE; p++)
            percentages[p] = -1;

        // ---- phase 1 ----
        // loop over the nodes assigned to the tenant and calculate the
        // percentage of resources it gets on each assigned node
        for (int i=0; i<tenantNumNodes; i++) {
            int nodeIx          = tn.getDistinctNodeId(i);
            int numSlicesOnNode = tn.getDistinctWeight(i);

            // for the purpose of this exercise, we use dense ids, so
            // we can maintain a dense array "percentages"
            if (!tn.usesDenseIds())
                nodeIx = existingTenants.indexOf(nodeIx);

            // this is the percentage we should assign to this tenant
            // on node nodeId, the number of our slices on the node
            // divided by the total number of Esgyn slices on that node,
            // limited to a range of [1, 100]
            int percentageOnNode = 0;
            int limit = existingTenants.getDenseLimit(nodeIx);

            if (limit > 0)
                percentageOnNode = (100 * numSlicesOnNode) / limit;

            percentageOnNode = Math.min(Math.max(percentageOnNode,1),100);
            percentages[nodeIx] = percentageOnNode;

            // are the percentages still all the same?
            if (samePercentageOnAllNodes)
                if (commonPercentage == -1)
                    commonPercentage = percentageOnNode;
                else if (commonPercentage != percentageOnNode)
                    samePercentageOnAllNodes = false;
        }

        // ---- phase 2 ----
        // If not all nodes are assigned to the tenant and we don't
        // have the same cgroup size for all nodes, determine the size
        // of the cgroup for these non-assigned nodes. When we execute
        // a query and some of the nodes assigned to the tenant fail,
        // the executor will use nodes that are not assigned to the
        // tenant. Therefore we will need to have a cgroup on
        // unassigned nodes as well.
        if (tenantNumNodes < CLUSTER_SIZE && !samePercentageOnAllNodes) {
            for (int u=0; u<CLUSTER_SIZE; u++)
                if (percentages[u] == -1)
                    percentages[u] = calculateAvgPercentage(computeUnits,
                                                            existingTenants.getDenseLimit(u));
        }

        // ---- phase 3 ----
        // If this is a check, compare the percentage calculated in
        // phases 1 and 2 with the current percentage. Return if they
        // match, otherwise continue with phase 4.
        if (action == ActionType.CHECK) {
            // check to see whether the cgroup already exists and has
            // the correct size
            if (currentState == null || allNodes)
                throw new IllegalArgumentException(
                    "For check, pass in currentState and check local node only");

            action = ActionType.ADD;
            for (JsonNode node : currentState) {
                String cGroupName = node.get("name").textValue();
                if (tenant.getName().equalsIgnoreCase(cGroupName)) {
                    // cgroup exists, now check the size, calculate
                    // the current percentage used by the cgroup.
                    // For now, we check only the CPU shares, assuming
                    // that the hardware configuration did not change.
                    int cpuShares = node.get("cpu.shares").asInt();
                    long memoryLimit = node.get("memory.limit_in_bytes").asLong();
                    Integer esgynShares = esgynCpuSharesPerNodeList.get(localNodeNum);
                    Long esgynMemory = esgynMemoryBytesPerNodeList.get(localNodeNum);
                    int currCpuSharesPct = 0;
                    int currMemoryPct = 0;

                    if (esgynShares != null && esgynShares > 0)
                        currCpuSharesPct = 100 * cpuShares / esgynShares;
                    else
                        currCpuSharesPct = 0;

                    if (esgynMemory != null && esgynMemory > 0)
                        currMemoryPct = (int) (100 * memoryLimit / esgynMemory);

                    if (currCpuSharesPct == percentages[localNodeNum] &&
                        currMemoryPct == percentages[localNodeNum]) {
                        // return, we checked and the cgroup exists and
                        // has the correct size, nothing to do
                        _LOG.debug(String.format("Tenant %s CGroup exists on node %d",
                                                 tenant.getName(),
                                                 localNodeNum));
                        return;
                    }

                    // the check indicated that we need to update the
                    // cgroup, continue below
                    action = ActionType.ALTER;
                    _LOG.debug(String.format("Tenant %s CGroup needs to be resized from %d percent (cpu) and %d percent (memory) to %d percent for both cpu and memory on node %d",
                                             tenant.getName(),
                                             currCpuSharesPct,
                                             currMemoryPct,
                                             percentages[localNodeNum],
                                             localNodeNum));
                    break;
                }
            }
            if (action == ActionType.ADD)
                _LOG.debug(String.format("Tenant %s CGroup needs to be created on node %d",
                                         tenant.getName(),
                                         localNodeNum));
        }

        //---- phase 4 ----
    	//Create or modify the tenant cgroup
    	try {
            for (int c=0; c<CLUSTER_SIZE; c++)
                if (percentages[c] > 0) {
                    // We found a node with an assigned percentage.
                    // Now find all nodes with the same percentage and
                    // create the cgroups on that node set in parallel.
                    StringBuilder nodes = new StringBuilder();
                    int percentage = percentages[c];

                    if (!allNodes) {
                        if (c == localNodeNum) {
                            // make sure we only create the cgroup for this one node
                            nodes.append("-w ");
                            nodes.append(getNodeNameForNodeID(getNodeId(c)));
                            c = CLUSTER_SIZE;
                        }
                        else continue;
                    } else if (samePercentageOnAllNodes) {
                        // change it all in one shot, use -a instead of enumerating all nodes
                        nodes.append("-a");
                        c = CLUSTER_SIZE;
                    }
                    else {
                        // make a comma-separated list of all the
                        // nodes that have the same percentage
                        for (int d=c; d<CLUSTER_SIZE; d++)
                            if (percentages[d] == percentage) {
                                if (nodes.length() == 0)
                                    nodes.append("-w ");
                                else
                                    nodes.append(",");
                                nodes.append(getNodeNameForNodeID(getNodeId(d)));
                                percentages[d] = -1; // mark as already processed
                            }
                    }

                    String nodeString = nodes.toString();

                    if (action == ActionType.ADD) {
    			_LOG.info(String.format("Creating tenant cgroup %1$s percentage=%2$s on nodes %3$s",
                                                tenant.getName(), percentage, nodeString));
    			CGroupHelper.createCGroup(ESGYNDB_PARENT_CGROUP_NAME,
                                                  tenant.getName(),
                                                  percentage,
                                                  percentage,
                                                  nodeString);
                    } else if (action == ActionType.ALTER) {
    			_LOG.info(String.format("Updating tenant cgroup %1$s percentage=%2$s on nodes %3$s",
                                                tenant.getName(), percentage, nodeString));
    			CGroupHelper.modifyCGroup(ESGYNDB_PARENT_CGROUP_NAME,
                                                  tenant.getName(),
                                                  percentage,
                                                  percentage,
                                                  nodeString);
                    }
                }
                else {
                    // This node is probably down, skip it. The cgroups
                    // will have to be created on-demand at runtime
                }

    	} catch (Exception e) {
    		String errorPrefix = "Failed to create tenant cgroup";
    		if(action == ActionType.ALTER) {
    			errorPrefix = "Failed to update tenant cgroup";
    		}
    		Exception ee = new Exception (errorPrefix + " : " + e.getMessage());
    		_LOG.error(e.getMessage());
    		throw ee;
    	}
    }

    private static int calculateAvgPercentage(int computeUnits, int limit)
    {
        if (limit <= 0)
            return 0;

        // This node is not assigned to the tenant, we are probably
        // using this node because some of the tenant's own nodes are
        // down.  Assign the average percentage, but don't go below
        // one unit, to avoid situations where we have a very small
        // cgroup on a node that causes queries to run out of memory
        // or run very slowly.
        int avgPercent = (100 * computeUnits) / getMaxSlicesPerCluster();
        int singleUnitPct = 100 / limit;

        // limit to interval [1, 100]
        avgPercent = Math.min(Math.max(avgPercent,1),100);

        return Math.max(avgPercent, singleUnitPct);
    }

    /**
     * Recreate the tenant level cgroups as needed by reading the tenant metadata
     * @throws Exception
     */
    public static void checkAndCreateLocalTenantCGroups(boolean useZK) throws Exception{
        try {
            refresh(useZK);
            Collection<NamedTenantNodes> tenants = getTenantNodeAllocations().values();
            String cGroupJSON = CGroupHelper.getCGroupRecursive(ESGYNDB_PARENT_CGROUP_NAME, "", false);
            JsonFactory factory = new JsonFactory();
            ObjectMapper mapper = new ObjectMapper(factory);
            JsonNode cGroupsNode = mapper.readTree(cGroupJSON);
            if(cGroupsNode instanceof ArrayNode){
                ArrayNode arrayNode = (ArrayNode)cGroupsNode;
    
                for(NamedTenantNodes tenant : tenants){
                    if(tenant.getName().equals(CGroupHelper.ESGYN_SYSTEM_TENANT))
                        continue;

                    addAlterTenantCGroup(ActionType.CHECK, tenant, false, arrayNode);
                }    
            }
        } catch (Exception ex) {
            Exception ee = new Exception(String.format("Failed to create local cgroups for all tenants : " + ex.getMessage()));
            _LOG.error(ee.getMessage());
            throw ee;
        }
    }
    
    /**
     * Return a list of tenants and its attributes read from Esgyn MD tables
     * @return
     * @throws Exception
     */
    public static ArrayList<NamedTenantNodes> fetchTenantsFromMD() throws Exception{
        ArrayList<NamedTenantNodes> tenants = new ArrayList<NamedTenantNodes>();
        PreparedStatement pstmt = null;
        Connection connection = null;
        ResultSet rs = null;
        try {
            Class.forName("org.apache.trafodion.jdbc.t2.T2Driver");
            _LOG.debug("T2 driver loaded...");

            connection = DriverManager.getConnection("jdbc:t2jdbc:");
            _LOG.debug("Reading tenant details from metadata");
            
            // The following SQL query returns a set of rows ordered by the 
            // tenant name followed by the node name.  Multiple rows per tenant 
            // are returned one for each node assigned weights, for example:
            //     TENANT1     NODE1  4 (units) 4 (total units)
            //     TENANT2     NODE2  2 (units) 8 (total units)
            //     TENANT2     NODE3  4 (units) 8 (total units)
            //     TENANT2     NODE5  2 (units) 8 (total units)
            //     . . .
            String queryText = "SELECT auth_db_name, resource_name, " +
                   "usage_value, affinity, cluster_size, tenant_size, " +
                   "o.catalog_name, o.schema_name " +
                   "FROM  TRAFODION.\"_TENANT_MD_\".tenants t " +
                   " JOIN TRAFODION.\"_MD_\".auths a " +
                   " ON t.tenant_id = a.auth_id " +
                   " LEFT JOIN TRAFODION.\"_MD_\".objects o " +
                   " on t.default_schema_uid = o.object_uid " +
                   " LEFT JOIN TRAFODION.\"_TENANT_MD_\".resource_usage u " +
                   " ON t.tenant_id = u.usage_uid AND u.usage_type = 'T' " +
                   " AND u.usage_value > 0 " +
                   "ORDER BY auth_db_name, resource_name, usage_value";
            _LOG.debug(queryText);

            pstmt = connection.prepareStatement(queryText);
            rs = pstmt.executeQuery();

            // define attributes to save current tenant information when the 
            // next tenant is accessed from the result set
            String lastTenantName = null;
            int lastAffinity = -1;
            int lastClusterSize = -1;
            int lastTenantSize = -1;
            String lastDefaultSchema = "";
            boolean lastTenantIsRGTenant = false;
            HashMap<Integer, Integer> nodeAllocations = new LinkedHashMap<Integer, Integer>();

            // Simulate up to 20 nodes as a testing tool on development work 
            // stations 
            boolean simulateMode = false;
            String clusterName = System.getenv("CLUSTERNAME");
            if (clusterName == null)
                simulateMode = true;
            int simulatedNodeIds = 20;
            
            // variables to control the while loop below
            // moreData: We have another row in the result set
            boolean moreData = rs.next();
            // processLastTenant: We have exhausted the result
            // set, process the node allocation of the last tenant
            boolean processLastTenant = false;

            String tenantName = null;
            int nodeWeight = -1;
            int affinity = -1;
            int clusterSize = -1;
            int tenantSize = -1;
            int nodeId = -1;
            String defaultCatalog = "";
            String defaultSchema = "";
            boolean isRGTenant = false;
            TenantNodes tenantNodes = null;

            // we currently don't store the session limit in the metadata, so try to
            // salvage that information from existing zookeeper data, if possible
            ArrayList<NamedTenantNodes> zkTenants = null;

            try {
                zkTenants = fetchTenantsFromZK(false);
            }
            catch (Exception e) {
                // ignore any exceptions from zookeeper
            }

            // The while loop processes each returned row. For each tenant, a 
            // map<key, value> (nodeAllocations) is used to store node/weight 
            // allocations.  The key is the node ID and the value is the weight.
            // Once all the rows for a particular tenant are processed, an 
            // ASNode (for AS tenants) or a WeightedNode (for RG tenants) is 
            // allocated. The ASNode or WeightedNode is added to the list of 
            // NamedTenantNodes and returned to the caller.
            while(moreData || processLastTenant){
                if (moreData) {
                    tenantName = rs.getString(1);
                    String nodeName = rs.getString(2);
                    nodeWeight = rs.getInt(3);
                    affinity = rs.getInt(4);
                    clusterSize = rs.getInt(5);
                    tenantSize = rs.getInt(6);
                    defaultCatalog = rs.getString(7);
                    defaultSchema = rs.getString(8);
                    isRGTenant = (affinity < 0 && tenantSize > 0);

                    if (defaultCatalog == null)
                        defaultCatalog = "";
                    if (defaultSchema == null)
                        defaultSchema = "";
                    if (!defaultCatalog.isEmpty())
                        defaultSchema = defaultCatalog + "." + defaultSchema;

                    if (isRGTenant) {
                        // RG tenant
                        nodeId = getNodeIDForNodeName(nodeName);
                        if (simulateMode && nodeId == -1) {
                            nodeId = simulatedNodeIds--;
                        }
                        if (nodeId == -1) {
                            throw new Exception(
                                    "node name " + nodeName + " is not part of the cluster");
                        }
                        String sqlRow = String.format("SQL row returned -> tenant: " + 
                                tenantName + " nodeID: " + Integer.toString(nodeId) + 
                                " node: " + nodeName + " tenantSize: " + 
                                Integer.toString(tenantSize));
                        _LOG.debug(sqlRow);
                    }
                    else
                        // AS tenant
                        nodeId = -1;
                }

                // First time through the while loop lastTenantName is null.
                if (processLastTenant ||
                    ((lastTenantName != null) && (!lastTenantName.equals(tenantName)))) {
                    if (lastTenantIsRGTenant) {
                            
                        // resource group tenant 
                        // setup nodeIds and their weights in a form required
                        // by WeightedNodes.
                        // (May want to add a new constructor to WeightedNodes
                        //  to avoid this copy)
                        int[] ids = new int[nodeAllocations.size()];
                        int[] weights = new int[nodeAllocations.size()];

                        int count = 0;
                        for (Map.Entry<Integer, Integer> entry : nodeAllocations.entrySet()) {
                            ids[count] = entry.getKey();
                            weights[count++] = entry.getValue();
                        }

                        String tenantAllocated = String.format ("%1$s%2$s%3$s%4$d%5$s%6$d%7$s%8$s",
                                                 "Processed resource group tenant: ", lastTenantName,
                                                 ", number nodes: ", nodeAllocations.size(),
                                                 ", total tenant weight: ", lastTenantSize,
                                                 ", default schema: ", defaultSchema);
                        //System.out.println(tenantAllocated);
                        _LOG.info(tenantAllocated);
                        
                        for (int i = 0; i < nodeAllocations.size(); i++) {
                            String nodeWeights = String.format ("nodeID: " + ids[i] +
                                                 ", weight: " + weights[i]);
                            //System.out.println(nodeWeights);
                            _LOG.debug (nodeWeights);
                        }

                        nodeAllocations.clear();

                        // add entry for tenant
                        tenantNodes = new WeightedNodeList(ids, weights);
                    }
                    else {
                        // Adaptive segmentation, add entry for tenant
                        String tenantAllocated = String.format("%1$s%2$s%3$s%4$d", 
                                      "Processed adaptive segmentation tenant: ", lastTenantName,
                                      ", tenant size:", lastTenantSize);
                        //System.out.println(tenantAllocated);
                        _LOG.info(tenantAllocated);
                        tenantNodes = new ASNodes(lastTenantSize, lastClusterSize, lastAffinity);
                    }

                    // look up session limit in zookeeper, if possible, see Mantis-8185,
                    String sessionLimit = "";
                    if (zkTenants != null) {
                        for (NamedTenantNodes zk : zkTenants)
                            if (zk.getName().equals(lastTenantName)) {
                                sessionLimit = zk.getSessionLimit();
                                break;
                            }
                    }

                    NamedTenantNodes tenant = new NamedTenantNodes(lastTenantName,
                                                                   tenantNodes,
                                                                   lastDefaultSchema,
                                                                   sessionLimit);
                    tenants.add(tenant);
                }
 
                // save nodeId and its weight
                if (isRGTenant) {
                    nodeAllocations.put(nodeId, nodeWeight);
                }

                // save current tenant attributes
                lastTenantName = tenantName;
                lastAffinity = affinity;
                lastClusterSize = clusterSize;
                lastTenantSize = tenantSize;
                lastDefaultSchema = defaultSchema;
                lastTenantIsRGTenant = isRGTenant;

                // prepare for the next row in the result set (or final processing)
                if (processLastTenant)
                    processLastTenant = false; // exit the loop
                else {
                    moreData = rs.next();
                    if (!moreData)
                        // go through the loop one final time
                        processLastTenant = true;
                }
            }
            rs.close();

        } catch (Exception ex) {
            //System.out.println("exception: " + ex.getMessage());
            Exception ee = new Exception(String.format("Failed to fetch list of tenants : " + ex.getMessage()));
            _LOG.error(ee.getMessage());
            throw ee;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {

                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {

                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ex) {

                }
            }
        }    
        return tenants;
    }
    
    /**
     * Return a list of tenants and its attributes read from Zookeeper nodes
     * Tenant ZNodes from /trafodion/wms/tenants
     * @param ignoreSystemTenant
     * @return
     * @throws Exception
     */
    public static ArrayList<NamedTenantNodes> fetchTenantsFromZK(boolean ignoreSystemTenant) throws Exception{
        ArrayList<NamedTenantNodes> tenants = new ArrayList<NamedTenantNodes> ();
        ZkClient zkClient = new ZkClient();
        ZooKeeper zk = null;
        try {
            zk = zkClient.connect();
            List<String> tenantZNodes = zk.getChildren(TenantHelper.TENANT_ZNODE_PARENT, true);
            for(String tenantName : tenantZNodes) {
                //System.out.println("Processing node: " + tenantName);
            	if (ignoreSystemTenant && tenantName.equalsIgnoreCase(CGroupHelper.ESGYN_SYSTEM_TENANT))
            		continue;
                String path = TenantHelper.TENANT_ZNODE_PARENT + "/" + tenantName;
                Stat stat = zk.exists(path, false);
                byte[] data = zk.getData(path, false, stat);
                String dataString = new String(data, StandardCharsets.UTF_8);
                //System.out.println("Tenant details: " + dataString);
                NamedTenantNodes tenant = null;
                TenantNodes nodes = null;
                // for older style ZK nodes, phase out
                int affinity = -1;
                int tenantSize = -1;
                int maxNodes = -1;
                String defaultSchema = "";
                String sessionLimit = "";
                
                String[] attributes = dataString.split("[=:]");
                for (int i = 0; i < attributes.length; i = i + 2) {
                    if(attributes[i+1] != null && attributes[i+1].trim().length()>0 ) {
                        switch(attributes[i]) {
                        // old attribute, phase out
                        case TenantHelper.AFFINITY:
                            try {
                                affinity = Integer.parseInt(attributes[i+1]);
                            }catch(Exception e) {
                                
                            }
                            break;
                        // old attribute, phase out
                        case TenantHelper.COMPUTE_UNITS:
                            try {
                                tenantSize = Integer.parseInt(attributes[i+1]);
                            }catch(Exception e) {
                                
                            }
                            break;
                        // old attribute, phase out
                        case TenantHelper.MAX_NODES:
                            try {
                                maxNodes = Integer.parseInt(attributes[i+1]);
                            }catch(Exception e) {
                                
                            }
                            break;
                        case TenantHelper.NODES:
                            try {
                                nodes = TenantNodes.fromSerializedForm(attributes[i+1]);
                            }catch(Exception e) {
                                
                            }
                            break;
                        case TenantHelper.DEFAULT_SCHEMA:
                            try {
                                defaultSchema = attributes[i+1];
                            }catch(Exception e) {
                                
                            }
                            break;
                        case TenantHelper.SESSION_LIMIT:
                            try {
                                sessionLimit = attributes[i+1];
                            }catch(Exception e) {
                                
                            }
                            break;
                        default:
                            
                        }
                    }
                 }
                // phase this out
                if (nodes == null) {
                    if (affinity >= 0)
                        nodes = new ASNodes(tenantSize, maxNodes, affinity);
                    else
                        nodes = new WeightedNodeList(tenantSize);
                }
                tenant = new NamedTenantNodes(tenantName, nodes, defaultSchema, sessionLimit);
                tenants.add(tenant);
            }

        } catch (Exception ex) {
            Exception ee = new Exception(String.format("Failed to fetch list of tenants : " + ex.getMessage()));
            _LOG.error(ee.getMessage());
            throw ee;
        }
        finally {
            try {
                zk.close();
            }catch(Exception e) {
                
            }
        }
        return tenants;        
    }
    
    public static String rebuildTenantInZKAndCgroup(NamedTenantNodes t) {
        String result = "";
        try {
            TenantHelper.upsertTenantZnode(t);
            deleteCGroup(
                    CGroupHelper.ESGYNDB_PARENT_CGROUP_NAME,
                    t.getName());
            addAlterTenantCGroup(
                    ActionType.ADD,
                    t,
                    true,
                    null);
        }
        catch (Exception e) {
            result = e.getMessage();
            e.printStackTrace();
        }
        return result;
    }

    public static void checkAndCompare(boolean rebuild)
    {
        int missing = 0;
        int systemTenants = 0;

        try {
            refresh(true);
            HashMap<String, NamedTenantNodes> zkList = getTenantNodeAllocations();
            ArrayList<NamedTenantNodes>       mdList = fetchTenantsFromMD();
            int numTenantsInMD = mdList.size();

            if (rebuild) {
                System.out.println("Rebuilding tenant data in zookeeper from the metadata");
                System.out.println("-----------------------------------------------------");
            }
            else {
                System.out.println("Comparing tenant data in zookeeper with the metadata");
                System.out.println("----------------------------------------------------");
            }

            for (int t = 0; t<numTenantsInMD; t++) {
                NamedTenantNodes md = mdList.get(t);

                if (!md.getName().equals(CGroupHelper.ESGYN_SYSTEM_TENANT)) {
                    NamedTenantNodes zk = zkList.get(md.getName());
                    String errMsg = "";
                    String rebuildMsg = "OK";

                    if (zk == null) {
                        errMsg = "Exists in MD, missing in ZK";
                        missing++;
                        if (rebuild)
                            rebuildMsg = rebuildTenantInZKAndCgroup(md);
                    }
                    else if (!zk.equals(md)) {
                        errMsg = String.format("Definition is different, zookeeper=(nodes:%s,schema:%s,sessions:%s), metadata=(nodes:%s,schema:%s,sessions:%s)",
                                               zk.getTenantNodes().toSerializedForm(),
                                               zk.getDefaultSchema(),
                                               zk.getSessionLimit(),
                                               md.getTenantNodes().toSerializedForm(),
                                               md.getDefaultSchema(),
                                               md.getSessionLimit());
                        if (rebuild) {
                            rebuildMsg = rebuildTenantInZKAndCgroup(md);
                        }
                    }
                    if (errMsg.isEmpty())
                        System.out.format("%s: OK (size:%d,nodes:%s,schema:%s,sessions:%s)\n",
                                          md.getName(),
                                          md.getTenantNodes().getSize(),
                                          md.getTenantNodes().toSerializedForm(),
                                          md.getDefaultSchema(),
                                          md.getSessionLimit());
                    else {
                        System.out.format("%s: ***INCONSISTENT*** %s\n",
                                          md.getName(),
                                          errMsg);
                        if (rebuild)
                            System.out.format("%s rebuild: %s\n",
                                              md.getName(),
                                              rebuildMsg.isEmpty() ? "OK" : rebuildMsg);
                    }
                }
                else
                    systemTenants++;
            }

            if (mdList.size() - systemTenants - missing < zkList.size()) {
                // find extra items in zookeeper that are not in the metadata
                int mdSize = mdList.size();

                for (NamedTenantNodes zk : zkList.values()) {
                    boolean found = false;

                    for (int i=0; i<mdSize; i++)
                        if (mdList.get(i).getName().equals(zk.getName())) {
                            found = true;
                            break;
                        }

                    if (!found) {
                        System.out.format("%s: ***INCONSISTENT*** is only in zookeeper: %s\n",
                                          zk.getName(),
                                          zk.getTenantNodes().toSerializedForm());
                        if (rebuild) {
                            try {
                                TenantHelper.deleteTenantZnode(zk.getName());
                                deleteCGroup(
                                    CGroupHelper.ESGYNDB_PARENT_CGROUP_NAME,
                                    zk.getName());
                                System.out.format("%s removed from zookeeper and cgroups\n", zk.getName());
                            }
                            catch (Exception e) {
                                System.out.format("%s rebuild: %s\n",
                                        zk.getName(),
                                        e.getMessage());
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }

            if (systemTenants == 1)
                System.out.format("%s: OK (system tenant, all nodes of the cluster)\n",
                                  CGroupHelper.ESGYN_SYSTEM_TENANT);
            else
                System.out.format("%s: ***INCONSISTENT*** system tenant appears %d times in the metadata\n",
                                  systemTenants);   
        }
        catch (Exception e) {
            System.out.println("Exception during check: " + e.getMessage());
            e.printStackTrace();
        }

    }

    // main method, can also be invoked through the edb_cgroup_util script
    public static void main(String[] args) {
        boolean testMode = false;
        boolean initializeCGroups = false;
        boolean useZK = true;
        boolean checkAndCompare = false;
        boolean rebuild = false;
        boolean localCGroup = false;
        boolean printHelp = false;
        String tenantName = "";
        
        if(args.length == 0) {
            printHelp = true;
        } else {
            if(args[0].equalsIgnoreCase("initZK") || args[0].equalsIgnoreCase("initMD")) {
                useZK = args[0].equalsIgnoreCase("initZK");
                initializeCGroups = true;
            }
            else if (args[0].equalsIgnoreCase("check")) {
                checkAndCompare = true;
                rebuild = false;
            }
            else if (args[0].equalsIgnoreCase("rebuild")) {
                checkAndCompare = true;
                rebuild = true;
            }
            else if (args[0].equalsIgnoreCase("lcgroup")) {
                localCGroup = true;
                if (args.length == 2)
                    tenantName = args[1];
                else
                    printHelp = true;
            }
            else if (args[0].equalsIgnoreCase("help")) {
                printHelp = true;
            }
            else if (args[0].equalsIgnoreCase("test")) {
                testMode = true;
            }
            else {
                printHelp = true;
            }
        }

        if (printHelp) {
            System.out.println("Valid options:");
            System.out.println("  initZK : Recreate cgroups on local node for all tenants from zookeeper data");
            System.out.println("  initMD : Recreate cgroups on local node for all tenants from metadata");
            System.out.println("  check  : Compare zookeeper and metadata and print result");
            System.out.println("  rebuild: Bring zookeeper data in sync with metadata (if check showed issues)");
            System.out.println("  lcgroup tenant: create cgroup for tenant on local node");
            System.out.println("  test   : Print cgroups and resources of all nodes");
            System.out.println("  help   : Print this message");
            return;
        }

        try {

            if(initializeCGroups)
                CGroupHelper.createNodeLocalTenantCGroups(useZK);

            if (checkAndCompare)
                checkAndCompare(rebuild);

            if (localCGroup)
                createNodeLocalTenantCGroup(tenantName, true);

            if(testMode) {
                String parentCGroup = CGroupHelper.ESGYNDB_PARENT_CGROUP_NAME;

                System.out.println(CGroupHelper.getCGroupRecursive(parentCGroup, "", true));
                System.out.println(CGroupHelper.getClusterResources(true));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

}
