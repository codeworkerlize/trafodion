// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.common;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.common.zookeeper.DistributedZkLock;
import com.esgyn.common.zookeeper.ZkClient;

public class TenantHelper {
    
    private static final Logger _LOG = LoggerFactory.getLogger(TenantHelper.class);
    public static String TENANT_ZNODE_PARENT = "/trafodion/wms/tenants";
    // old attributes, for AS only, should be phased out
    public static final String COMPUTE_UNITS = "computeUnits";
    public static final String AFFINITY = "affinity";
    public static final String MAX_NODES = "maxNodes";
    // new attribute, serialized nodes, use instead of the above three
    public static final String NODES = "nodes";
    public static final String DEFAULT_SCHEMA = "defaultSchema";
    public static final String SESSION_LIMIT = "sessionLimit";
    // used for distributed locks
    public static final String TENANT_ZNODE_LOCKS = "/locks";
    
    static {
    	try {
        	String trafRootZnode = CommonHelper.getTrafRootZNode();
            TENANT_ZNODE_PARENT = trafRootZnode + "/wms/tenants";
    	} catch (Exception e) {
    		_LOG.error("Failed to get tenant root znode", e.getMessage());
    	}
    }

    public TenantHelper() {}

    /**
     * Interface to registerTenant using a serialized tenant
     *
     * @param tenantName
     * @param serializedNodes serialized nodes assigned to the tenant,
     *        both incomplete and complete forms are allowed
     * @param sessionLimit
     * @param defaultSchema
     * @param overrideWarning true allows oversubscription, false throws
     *        an exception if the cluster is oversubscribed
     * @param oversubsCounters returns information about oversubscription,
     *        if applicable. See class OversubscriptionInfo for details.
     * @return serialized form of the assigned nodes (same as
     *         serializedNodes if that was complete)
     * @throws Exception
     */
    static public String registerTenant(String tenantName,
                                        String serializedNodes,
                                        String sessionLimit,
                                        String defaultSchema,
                                        boolean overrideWarning,
                                        int[] oversubsCounters) throws Exception {

        NamedTenantNodes tenant = new NamedTenantNodes(
            tenantName,
            TenantNodes.fromSerializedForm(serializedNodes),
            defaultSchema,
            sessionLimit);
        OversubscriptionInfo osi = new OversubscriptionInfo();

        try {
            registerTenant(tenant,
                           overrideWarning,
                           osi);
        }
        finally {
            osi.copyMeasures(oversubsCounters);
        }

        return tenant.getTenantNodes().toSerializedForm();
    }

    /**
     * @deprecated Register a tenant that uses AS (Adaptive Segmentation)
     *
     * Call the method with a NamedTenantNodes object instead.
     *
     * @throws Exception
     */
    @Deprecated
    static public void registerTenant(String tenantName,
                                      int computeUnits,
                                      int clusterSize,
                                      int affinity,
                                      String sessionLimit,
                                      String defaultSchema) throws Exception {
        
        NamedTenantNodes tenant = new NamedTenantNodes(tenantName,
                                                       new ASNodes(computeUnits,
                                                                   clusterSize,
                                                                   affinity),
                                                       defaultSchema,
                                                       sessionLimit);

        registerTenant(tenant, false, null);
    }

    /**
     * Register a new tenant
     *
     * @param tenant name and incomplete or complete
     *        description of the assigned nodes,
     *        nodes will be assigned to an incomplete tenant
     * @param overrideWarning allow oversubscription, if true,
     *        throw exception if false and if oversubscribed
     * @throws Exception
     */
    static public void registerTenant(NamedTenantNodes tenant,
                                      boolean overrideWarning,
                                      OversubscriptionInfo osi) throws Exception {
        int phase = 1;

        try {
            // refresh node allocation for all tenants in local cache
            NodeUnitCounters currentTenantAllocation = CGroupHelper.getTenantUtilization(true);

            if (!tenant.getTenantNodes().isComplete()) {
                // nodes not yet assigned, do that now, without updating global counters
                tenant.getTenantNodes().placeTenant(currentTenantAllocation, false);
            }

            phase = 2;
            //create the tenant cgroup
            CGroupHelper.addAlterTenantCGroup(CGroupHelper.ActionType.ADD,
                                              tenant,
                                              true,
                                              null);
            _LOG.info("REGISTER_TENANT : Tenant " + tenant.getName() + " CGroup created");
    
            phase = 3;
            boolean oversubscribed = CGroupHelper.addToTenantNodeAllocation(tenant, osi);
            _LOG.info("REGISTER_TENANT : Tenant " + tenant.getName() + " allocation added to cache");
    
            phase = 4;
            if (!overrideWarning && oversubscribed) {
                _LOG.error("REGISTER_TENANT failure: Oversubscription in " + Integer.toString(osi.getMeasure(OversubscriptionInfo.NUM_OVERSUBSCRIBED_NODES_IX)) + " nodes");
                throw new Exception("Unable to register tenant, not enough resources available in the cluster");
            }

            //create the tenant znode
            phase = 5;
            if(tenant.getSessionLimit() != null && tenant.getSessionLimit().trim().equalsIgnoreCase("-2")) {
                tenant.setSessionLimit("-1");
            }            
            TenantHelper.upsertTenantZnode(tenant);
            _LOG.info("REGISTER_TENANT : Tenant " + tenant.getName() + " znode created");
        }catch(Exception ex) {

            _LOG.error("REGISTER_TENANT_FAILURE in phase " + Integer.toString(phase) + ": " +
                       ex.getMessage());
            if (phase > 3) {
                _LOG.info("Deleting cgroup for tenant " + tenant.getName());
                CGroupHelper.deleteCGroup(CGroupHelper.ESGYNDB_PARENT_CGROUP_NAME,
                                          tenant.getName());
                _LOG.info("Removing tenant " + tenant.getName() + " allocation from cache");
                CGroupHelper.removeFromTenantNodeAllocation(tenant);
            }
            throw ex;
        }
    }
    
    /**
     * Interface to alterTenant using a serialized tenant
     *
     * @param tenantName
     * @param serializedOldTenant serialized form of the nodes
     *        currently assigned to the tenant
     * @param serializedNewTenant serialized nodes to be assigned to the
     *        tenant, both incomplete and complete forms are allowed
     * @param sessionLimit
     * @param defaultSchema
     * @param overrideWarning true allows oversubscription, false throws
     *        an exception if the cluster is oversubscribed
     * @param oversubsCounters returns information about oversubscription,
     *        if applicable. See class OversubscriptionInfo for details.
     * @return serialized nodes of the altered tenant, now complete
     * @throws Exception
     */
    public static String alterTenant(String tenantName,
                                     String serializedOldTenant,
                                     String serializedNewTenant,
                                     String sessionLimit,
                                     String defaultSchema,
                                     boolean overrideWarning,
                                     int[] oversubsCounters) throws Exception {
        TenantNodes oldTenant = TenantNodes.fromSerializedForm(serializedOldTenant);
        TenantNodes newTenant = TenantNodes.fromSerializedForm(serializedNewTenant);
        OversubscriptionInfo osi = new OversubscriptionInfo();

        try {
            alterTenant(new NamedTenantNodes(tenantName,
                                             oldTenant,
                                             "",
                                             ""),
                        new NamedTenantNodes(tenantName,
                                             newTenant,
                                             defaultSchema,
                                             sessionLimit),
                        overrideWarning,
                        osi);
        }
        finally {
            osi.copyMeasures(oversubsCounters);
        }
        return newTenant.toSerializedForm();
    }

    /**
     * @deprecated Alter a tenant that uses AS (Adaptive Segmentation)
     *
     * Call the method with a NamedTenantNodes object instead.
     *
     * @throws Exception
     */
    @Deprecated
    public static void alterTenant(String tenantName,
                                   int oldAffinity,
                                   int oldComputeUnits,
                                   int newComputeUnits, 
                                   int oldClusterSize,
                                   int newClusterSize,
                                   String sessionLimit,
                                   String defaultSchema,
                                   boolean overrideWarning) throws Exception {
        NamedTenantNodes oldNamedTenant =
            new NamedTenantNodes(tenantName,
                                 new ASNodes(oldComputeUnits,
                                             oldClusterSize,
                                             oldAffinity),
                                 "",
                                 "");
        NamedTenantNodes newNamedTenant =
            new NamedTenantNodes(tenantName,
                                 new ASNodes(newComputeUnits,
                                             newClusterSize,
                                             -1),
                                 defaultSchema,
                                 sessionLimit);

        alterTenant(oldNamedTenant,
                    newNamedTenant,
                    overrideWarning,
                    null);
    }
        
    /**
     * Alter attributes of a tenant
     *
     * @param oldNamedTenant name an existing node allocation of a tenant
     * @param newTenant new nodes to be assigned to the tenant
     * @param overrideWarning true allows oversubscription, false throws
     *        an exception if the cluster is oversubscribed
     * @param osi returns information about oversubscription,
     *        if applicable
     * @throws Exception
     */
    public static void alterTenant(NamedTenantNodes oldNamedTenant,
                                   NamedTenantNodes newNamedTenant,
                                   boolean overrideWarning,
                                   OversubscriptionInfo osi) throws Exception {

        if (!oldNamedTenant.getName().equals(newNamedTenant.getName()))
            throw new Exception("Invalid arguments, old and new tenant names must match");

        int phase = 1;

        try {
            // refresh node allocation for all tenants in local cache
            NodeUnitCounters currentTenantAllocation = CGroupHelper.getTenantUtilization(true);

            if (!oldNamedTenant.getTenantNodes().isComplete()) {
                String savedTenantName = oldNamedTenant.getName();
                oldNamedTenant = CGroupHelper.getTenantNodesByTenantName(oldNamedTenant.getName());

                if (oldNamedTenant == null) 
                    throw new Exception("Tenant " + savedTenantName + " is no longer defined in Zookeeper.");
            }
            phase = 2;
            if(! oldNamedTenant.getTenantNodes().equals(newNamedTenant.getTenantNodes())) {

                CGroupHelper.removeFromTenantNodeAllocation(oldNamedTenant);

                if (!newNamedTenant.getTenantNodes().isComplete()) {
                    phase = 3;

                    // nodes not yet assigned, do that now, without updating global counters
                    newNamedTenant.getTenantNodes().placeTenant(currentTenantAllocation, false);
                }

                phase = 4;
                CGroupHelper.addAlterTenantCGroup(CGroupHelper.ActionType.ALTER,
                                                  newNamedTenant,
                                                  true,
                                                  null);
                _LOG.info("ALTER_TENANT : Tenant CGroup updated");

                phase = 5;
                boolean oversubscribed = CGroupHelper.addToTenantNodeAllocation(newNamedTenant, osi);
                _LOG.info("ALTER_TENANT : Tenant allocation updated in cache");

                phase = 6;
                if (!overrideWarning && oversubscribed) {
                    _LOG.error("ALTER_TENANT failure: Oversubscription in " + Integer.toString(osi != null ? osi.getMeasure(OversubscriptionInfo.NUM_OVERSUBSCRIBED_NODES_IX) : -1) + " nodes");
                    throw new Exception("Unable to alter tenant, not enough resources available in the cluster");
                }
            }

            phase = 7;
            //update the tenant znode
            if(newNamedTenant.getSessionLimit() != null && newNamedTenant.getSessionLimit().trim().equalsIgnoreCase("-2")) {
                HashMap<String, String> tenantZNodeValues = getTenantZNodeValues(newNamedTenant.getName());
                if(tenantZNodeValues.containsKey(SESSION_LIMIT)) {
                    newNamedTenant.setSessionLimit(tenantZNodeValues.get(SESSION_LIMIT));
                }
            }
            
            TenantHelper.upsertTenantZnode(newNamedTenant);
            _LOG.info("ALTER_TENANT : Tenant znode update");

        } catch(Exception ex) {

            _LOG.error("ALTER_TENANT_FAILURE in phase " + Integer.toString(phase) + ": " +
                       ex.getMessage());
            if (phase > 5) {
                _LOG.error("Removing new tenant " + newNamedTenant.getName() + " allocation from cache");
                CGroupHelper.removeFromTenantNodeAllocation(newNamedTenant);
            }
            if (phase > 2) {
                _LOG.error("Adding old tenant " + oldNamedTenant.getName() + " allocation back to cache");
                CGroupHelper.addToTenantNodeAllocation(oldNamedTenant, null);
            }
            throw ex;
        }
    }
    
    /**
     * Interface to unRegisterTenant using a tenant name
     *
     * @param tenantName
     * @throws Exception
     */
    public static void unRegisterTenant(String tenantName) throws Exception {
        try{
            // refresh node allocation for all tenants in local cache
            CGroupHelper.getTenantUtilization(true);
        }catch(Exception ex){
            _LOG.error("Failed to initialize data from zookeeper " + ex.getMessage());
            throw ex;
        }

        NamedTenantNodes tenantNodes = CGroupHelper.getTenantNodesByTenantName(tenantName);
        if (tenantNodes == null)
            throw new Exception("Tenant " + tenantName + " is no longer defined in Zookeeper.");
        unRegisterTenant(tenantNodes);
    }
        
    /**
     * @deprecated unregister a tenant that uses AS (Adaptive Segmentation)
     *
     * Call the method with a NamedTenantNodes object instead.
     *
     * @throws Exception
     */
    @Deprecated
    public static void unRegisterTenant(String tenantName,
                                        int computeUnits,
                                        int clusterSize,
                                        int affinity) throws Exception {

        NamedTenantNodes tenant = new NamedTenantNodes(tenantName,
                                                       new ASNodes(computeUnits,
                                                                   clusterSize,
                                                                   affinity),
                                                       "",
                                                       "");
        unRegisterTenant(tenant);
    }

    /**
     * Unregister a tenant
     *
     * @param tenant name and assigned nodes for the tenant
     * @throws Exception
     */
    public static void unRegisterTenant(NamedTenantNodes tenant) throws Exception {
        Exception firstEx = null;  // we'll execute all three try blocks then throw the first exception encountered

        try{
            _LOG.info("Deleting znode for tenant " + tenant.getName());
            TenantHelper.deleteTenantZnode(tenant.getName());
        }catch(Exception ex){
            _LOG.error("Failed to delete tenant znode " + ex.getMessage());
            firstEx = ex;
        }
        
        try{
            _LOG.info("Deleting cgroup for tenant " + tenant.getName());
            CGroupHelper.deleteCGroup(CGroupHelper.ESGYNDB_PARENT_CGROUP_NAME,
                                      tenant.getName());
        }catch(Exception ex){
            _LOG.error("Failed to delete tenant cgroup " + ex.getMessage());
            if (firstEx == null)
                firstEx = ex;
        }
        
        try{
            _LOG.info("Reset in-memory tenant allocation map");
            CGroupHelper.removeFromTenantNodeAllocation(tenant);
        }catch(Exception ex){
            _LOG.error("Failed to remove tenant allocation " + ex.getMessage());
            if (firstEx == null)
                firstEx = ex;
		}    

        if (firstEx != null)	
            throw firstEx;
    }

    /**
     * Create a cgroup for a given tenant on the local node
     *
     * Use this at runtime, if a cgroup does not yet exists.
     * This method is invoked from C++ through JNI. Since multiple
     * processes could call this at the same time, we synchronize
     * the method through zookeeper, such that only one process
     * will do the actual cgroup creation, the others will wait.
     *
     * The only reason why this cgroup-related method is in this
     * class is because this is the class used by JNI.
     *
     * @param tenantName tenant name
     * @throws Exception
     */
    public static void createLocalCGroup(String tenantName) throws Exception {
        // call the appropriate CGroupHelper method
        CGroupHelper.createNodeLocalTenantCGroup(tenantName, false);
    }
    
    /**
     * Creates/Modifies a tenant ZNode with tenant configuration details
     * @param name
     * @param computeUnits
     * @param affinity
     * @param clusterSize
     * @param schemaName
     * @param sessionLimit
     * @return
     * @throws Exception
     */
    public synchronized static boolean upsertTenantZnode(NamedTenantNodes tenant) throws Exception {
        String name = tenant.getName();
        boolean znodeUpdated = false;
        Stat stat = null;
        byte[] data = null;
        
        if(name.equals(CGroupHelper.ESGYN_SYSTEM_TENANT)== false){
            ZkClient zkClient = new ZkClient();
            ZooKeeper zkc = zkClient.connect();
            try {
                String serializedNodes = tenant.getTenantNodes().toSerializedForm();
                String tdata = String.format("%1$s=%2$s:isDefault=%3$s:defaultSchema=%4$s:sessionLimit=%5$s:lastUpdate=%6$s",
                                             NODES, serializedNodes, "no", tenant.getDefaultSchema(),
                                             tenant.getSessionLimit(), Long.toString(System.currentTimeMillis()));
                data = tdata.getBytes(StandardCharsets.UTF_8);
                stat = zkc.exists(TENANT_ZNODE_PARENT + "/" + name,false);
                if (stat == null) {
                    zkc.create(TENANT_ZNODE_PARENT + "/" + name,
                            data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } else {
                    zkc.setData(TENANT_ZNODE_PARENT + "/" + name, data, -1);
                }
                znodeUpdated = true;
            }
            finally {
                try {
                    zkc.close();
                }catch(Exception e) {
                    
                }
            }
        }
        return znodeUpdated;
    
    }

    /**
     * Deletes a tenant Znode
     * @param tenantName
     * @return
     * @throws Exception
     */
    public synchronized static boolean deleteTenantZnode(String tenantName) throws Exception{
        Stat stat = null;
        byte[] data = null;
        boolean znodeDeleted = false;
        String tenantZNode = TENANT_ZNODE_PARENT + "/" + tenantName;
        _LOG.debug("Tenant Name :" + tenantName);
        if(tenantName.equals(CGroupHelper.ESGYN_SYSTEM_TENANT)== false){
            ZkClient zkClient = new ZkClient();
            ZooKeeper zkc = zkClient.connect();
            try {
            stat = zkc.exists(tenantZNode, false);
            if(stat != null){
                data = zkc.getData(tenantZNode, false, stat);
                if(data != null ){
                    String[] sData = (new String(data, StandardCharsets.UTF_8)).split(":");
                    for (int index =0; index < sData.length; index++){
                        String element = sData[index];
                        if(element.equals("isDefault=no")) {
                            // delete any lock files/dirs under the tenant node
                            DistributedZkLock dl = new DistributedZkLock(zkc, tenantZNode + TENANT_ZNODE_LOCKS, "dummy", false);
                            
                            dl.deleteBaseDir(300000);
                            // now delete the tenant node itself
                            zkc.delete(tenantZNode, -1);
                            znodeDeleted = true;
                            break;
                        }
                    }
                }
            }
        }
        finally {
            if(zkc !=null) {
                try {
                    zkc.close();
                }catch(Exception e) {
                    
                }
            }
        }

        }
        return znodeDeleted;
    }
    
    public synchronized static HashMap<String, String> getTenantZNodeValues(String tenantName) throws Exception {
    	Stat stat = null;
    	byte[] data = null;
    	HashMap<String, String> tenantZNodeValues = new HashMap<String, String>();

    	ZkClient zkClient = new ZkClient();
    	ZooKeeper zkc = zkClient.connect();
    	try {
    		String path = TenantHelper.TENANT_ZNODE_PARENT + "/" + tenantName;
    		stat = zkc.exists(path, false);
    		if(stat != null) {
    			data = zkc.getData(path, false, stat);
    			String dataString = new String(data, StandardCharsets.UTF_8);

    			String[] attributes = dataString.split("[=:]");
    			for (int i = 0; i < attributes.length; i = i + 2) {
    				tenantZNodeValues.put(attributes[i], attributes[i+1]);
    			}
    		}
    	}
    	finally {
    		try {
    			zkc.close();
    		}catch(Exception e) {

    		}
    	}
    	return tenantZNodeValues;

    }

    // run this main program with the following commands:
    // export CLASSPATH=esgyn-common-*.jar:$CLASSPATH
    // java com.esgyn.common.TenantHelper
    public static void main(String[] args)
    {
        // define the size and node ids of the cluster
        final int nodeIds[] = new int[] { 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13 };
        final int numNodes = nodeIds.length;
        final int commonLimit = 4;

        // other local variables
        int prevCounters[] = new int[numNodes];
 
         // test getNextPossibleTenantSize() method
        System.out.format("Possible tenant sizes for a cluster with %d nodes:%n", numNodes);
        for (int i=1; i<numNodes; i++)
            {
                i = ASNodes.getNextPossibleTenantSize(i, numNodes);
                System.out.format("Next possible size: %d%n", i);
            }

        // test placeTenant() method
        System.out.format("%n%nSome examples of adding tenants to this cluster of %d nodes%n",
                          numNodes);

        NodeUnitCounters nodeCounters = new NodeUnitCounters(nodeIds, commonLimit);

        // data for the 10 experiments
        int newTenantSizes[] = new int[]{ 3, 3, 3, 2, 12, 12, 1, 6, 2, 1 };
        int newAffinity = -1;

        // now admit 10 different tenants
        for (int e=0; e<newTenantSizes.length; e++)
            {
                ASNodes asn = new ASNodes(newTenantSizes[e], numNodes, -1);

                System.out.format("Adding a new tenant with size %d units%n",
                                                     newTenantSizes[e]);
                for (int p=0; p<numNodes; p++)
                    prevCounters[p] = nodeCounters.getDenseCounter(p);

                boolean limitExceeded = asn.placeTenant(nodeCounters, true);
                newAffinity = asn.getAffinity();

                System.out.format("Chosen affinity: %d%n", newAffinity);
                System.out.format("Limit exceeded: %s%n", limitExceeded);
                System.out.format("Now the cluster looks like the following (absolute and delta):%n");
                for (int x=0; x<numNodes; x++)
                    System.out.format("%3d ", nodeCounters.getDenseCounter(x));
                System.out.format("%n");
                for (int y=0; y<numNodes; y++)
                    System.out.format("%3d ", nodeCounters.getDenseCounter(y) - prevCounters[y]);
                System.out.format("%n%n");
            }

        for (int ee=0; ee<newTenantSizes.length; ee++) {
            if (ee == 0) {
                System.out.format("Adding tenants defined by a resource group for the first half of the nodes\n");
                for (int aa=numNodes/2; aa<numNodes; aa++)
                    nodeCounters.setLimitById(nodeCounters.id(aa), 0);
            }
            else if (ee == newTenantSizes.length/2) {
                System.out.format("Adding tenants defined by a resource group for the second half of the nodes\n");
                nodeCounters.setAllLimits(commonLimit);
                for (int aa=0; aa<numNodes/2; aa++)
                    nodeCounters.setLimitById(nodeCounters.id(aa), 0);                
            }
            WeightedNodeList wnl = new WeightedNodeList(newTenantSizes[ee]);

            System.out.format("Adding a new tenant with size %d units\n",
                                                 newTenantSizes[ee]);
            for (int p=0; p<numNodes; p++)
                prevCounters[p] = nodeCounters.getDenseCounter(p);

            boolean limitExceeded = wnl.placeTenant(nodeCounters, true);

            System.out.format("Limit exceeded: %s\n", limitExceeded);
            System.out.format("Now the cluster looks like the following (absolute and delta):\n");
            for (int x=0; x<numNodes; x++)
                System.out.format("%3d ", nodeCounters.getDenseCounter(x));
            System.out.format("\n");
            for (int y=0; y<numNodes; y++)
                System.out.format("%3d ", nodeCounters.getDenseCounter(y) - prevCounters[y]);
            System.out.format("%n%n");
        }

        // test placeConnection() method
        int existingConnections[] = new int[numNodes];
        int limits[] = new int[numNodes];

        for (int i=0; i<numNodes; i++)
            {
                existingConnections[i] = 0;
                limits[i] = 20;
            }

        NodeUnitCounters connCounters = new NodeUnitCounters(nodeIds, existingConnections, limits);

        // mark one node as down
        connCounters.setLimitById(5, 0);
        System.out.format("Marking node 5 as down%n");

        // data for the 20 experiments
        int tenantAffinities[]   = new int[]{ 2, 2, 2, 2, 6, 7, 5, 3, 2, 1, 3, 3, 3, 2, 6, 2, 2, 3, 2, 1 };
        int tenantSizes[]        = new int[]{ 12,1, 3, 4, 6, 12,2, 2, 1, 4, 4, 3, 3, 2, 6, 1, 1, 3, 2, 1 };
        int assignedNode = -1;

        // now admit 20 connections for various tenants
        for (int c=0; c<20; c++)
            {
                ASNodes asn = new ASNodes(tenantSizes[c], numNodes, tenantAffinities[c]);
                                               
                System.out.format("Adding a new connection for a tenant with affinity %d and size %d%n",
                                  tenantAffinities[c], tenantSizes[c]);

                assignedNode = asn.placeConnection(connCounters,
                                                   false);

                System.out.format("Chosen node: %d%n", assignedNode);
                System.out.format("Now the cluster looks like the following:%n");
                for (int y=0; y<numNodes; y++)
                    System.out.format("%3d ", connCounters.getCounterById(nodeIds[y]));
                System.out.format("%n%n");
            }

    }
}
