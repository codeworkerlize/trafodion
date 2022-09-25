// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common;

/**
 * This abstract class represents the nodes assigned to
 * a tenant and it defines how the compute units assigned
 * to the tenant are distributed over the assigned nodes.
 * 
 * The "size" of this object is the number of compute
 * units assigned to nodes.
 * 
 * There are currently two subclasses defined:
 * 
 * class ASNodes assigned nodes using Adaptive Segmentation
 * and it places some restrictions on the size, which must
 * be a factor or a multiple of the cluster size
 * 
 * class WeightedNodeList has no such restrictions, it is
 * just a list of nodes, each associated with a weight
 * (the number of units assigned to this node)
 * 
 * The methods that return node ids may either return
 * "dense", logical ids numbered 0 ... n-1 or actual node
 * ids that may contain holes. Method usesDenseIds()
 * indicates which kind of ids will be returned.
 *
 */
public abstract class TenantNodes {

    protected TenantNodes() {}

    /**
     * Get the total size in "units"
     * 
     * @return sum of the weights of all nodes
     */
    // tenants and must be maintained by derived classes
    public abstract int getSize();
    
    /**
     * Get the number of unique nodes this tenant is allowed
     * to use. Note that this number is less or equal to getSize().
     * 
     * @return number of unique nodes used by this tenant
     */
    public abstract int getNumNodes();

    /**
     * Return whether this object is completely defined, meaning
     * that specific node ids have been assigned. An incomplete
     * object contains the size but not yet the specific nodes
     * to be used.
     *
     * For an incomplete object, only call methods getSize(),
     * isComplete(), equals(), placeTenant() and the serialization
     * and deserialization methods.
     * 
     * @return true if nodes are completely defined
     */
    public abstract boolean isComplete();

    /**
     * Return whether the node ids used by this tenant are
     * dense, "logical" ids numbered 0 ... n-1 for a cluster
     * with n nodes, or whether they are potentially sparse
     * nids assigned by the monitor.
     * 
     * @return true if node ids used are dense
     */
    public abstract boolean usesDenseIds();

    /**
     * Iterate over the nodes of a tenant
     * 
     * Each returned node id is for a particular "unit" of the tenant,
     * with units being numbered 0 ... getSize() - 1. Multiple units
     * may be placed on the same node, so some node ids may be returned
     * more than once. Example code:
     *
     * <pre>
     * for (int i=0; i &#60 getSize(); i++)
     *   int nodeIdToUse = getNodeNumber(i);
     * </pre>
     *
     * @param ix specifies which unit of the tenant is to be returned.
     *        ix must have a non-negative value less than getSize()
     * @return node id where unit number ix of the tenant is placed
     * @see #getSize()
     */
    public abstract int getNodeNumber(int ix);

    /**
     * Get a distinct list of nodes used by this tenant, ordered by node id
     *
     * Unlike the getNodeNumber() method, which should be used when
     * actually assigning resources to a query, this method is used
     * when we want to record usage information as a list of distinct
     * node ids, each associate with the number of units allocated
     * on the node.  Example code:
     *
     * <pre>
     * for (int i=0; i &#60 getNumNodes(); i++) {
     *   int allocatedNodeId    = getDistinctNodeId(i);
     *   int numUnitsOnThisNode = getDistinctWeight(i);
     * }
     * </pre>
     *
     * @param ix Index between 0 and getNumNodes() into the distinct
     *        list of nodes used by this tenant.
     * @return distinct node id number ix used by this tenant
     * @see #getNodeNumber(int)
     * @see #getNumNodes()
     **/
    public abstract int getDistinctNodeId(int ix);

    /**
     * Get the number of units associated with a node id
     *
     * This method it used together with the getDistinctNodeId()
     * to create a list of nodes an assigned units for each node
     *
     * @param ix Index between 0 and getNumNodes() into the distinct
     *        list of nodes used by this tenant.
     * @return number of units assigned to node ix
     * @see #getNodeNumber(int)
     * @see #getNumNodes()
     **/
    public abstract int getDistinctWeight(int ix);

    /**
     * Determine whether this tenant has the same weight on all of its
     * nodes (or on all nodes of the cluster if overflow is set to false).
     * Such tenants can be managed more efficiently, since they allow
     * the same setup on every node of the cluster.
     * 
     * @param overflow indicates whether the method should consider
     *                 overflow nodes (nodes not belonging to the tenant
     *                 that may nevertheless be used when nodes are down)
     * @param numNodesInCluster total number of nodes in the cluster
     *
     * @return the common weight for this tenant on all nodes or -1
     *         if there is no such common weight.
     */
    public abstract int getCommonWeightForAllNodes(boolean overflow,
                                                   int numNodesInCluster);

    /**
     * Get the total weight assigned for a particular node number
     * (a dense or sparse node number as returned by getNodeNumber()).
     * This total weight represents the amount of resources that
     * need to be allocated on a given node.
     * 
     * @param nodeNum dense or sparse node number.
     * @param overflow when set to true, the method will return a non-zero
     *                 weight for nodes that are not assigned to the
     *                 tenant. This allows to allocate resource on other
     *                 nodes, in case we need to overflow the tenant, due
     *                 to unavailable nodes.
     * @return total weight or units allocated for this tenant on
     *         node nodeNum.
     */
    public abstract int getWeightForNodeNumber(int nodeNum, boolean overflow);

    /**
     * Assign nodes to a tenant
     * 
     * This method takes a partially specified tenant (e.g. its size, but
     * not its exact list of nodes) and assigns a list of nodes from the
     * provided array, using heuristics to achieve a good placement.
     * 
     * @param nc An object that describes the current cluster and the
     *           already assigned tenant units on each node. The object
     *           may also specify limits for units to place on the node.
     *           Limits greater than 0 are guidelines that may be exceeded
     *           (the method will return true if placing the tenant
     *           exceeded a limit, but not if the limits were already
     *           exceeded). Limits that are set to 0, however, indicate
     *           nodes that are not available for placement.
     * @param updateCounters indicates whether the call should update the
     *           node counters nc to include the new tenant. If this is set
     *           to false, the caller needs to update the counters later
     *           by calling the NodeUnitCounters.addTenant() method.
     * @return Returns false if placing the new tenant did not cause any
     *         node to exceed its limit, true otherwise (true means at
     *         least one of the nodes on which we placed the tenant now
     *         exceeds its limit).
     */
    public abstract boolean placeTenant(NodeUnitCounters nc, boolean updateCounters);

    /**
     *  Determine the best node for a new connection
     *
     *  @param nc An object that describes the current cluster and the
     *            already assigned connections on each node. The object
     *            must also specify limits for the number of connections
     *            on the node.
     *  @param assignProportionally identifies how to assign new
     *                        connections. When set to false,
     *                        the method tries to assign equal
     *                        numbers of connections to each node.
     *                        When set to true, the method tries
     *                        to balance the connections
     *                        proportionally to the limits.
     *  @return node id of the node to use for the new connection
     *          or -1 if no more connections are available, based
     *          on the limits that were passed in
     */
    public abstract int placeConnection(NodeUnitCounters nc,
                                        boolean assignProportionally);

    /**
     *  Place a connection when we have no available servers for a tenant
     *
     *  This method will use nodes other than those assigned to
     *  the tenant to find a node with an available server. This is
     *  used only when there are no servers running on any of the nodes
     *  of the tenant.
     *
     *  @param nc An object that describes the current cluster and the
     *            already assigned connections on each node. The object
     *            must also specify limits for the number of connections
     *            on the node.
     *  @param assignProportionally identifes how to assign new
     *                        connections. When set to false,
     *                        the method tries to assign equal
     *                        numbers of connections to each node.
     *                        When set to true, the method tries
     *                        to balance the connections
     *                        proportionally to the limits.
     *  @param nextCandidate a parameter used to randomize assignment
     *         to avoid a hot spot on specific nodes 
     *  @return node id of the node to use for the new connection
     *          or -1 if no more connections are available, based
     *          on the limits that were passed in
     */
    public static int placeOverflowConnection(
            NodeUnitCounters nc,
            boolean assignProportionally,
            int nextCandidate) {
        float lowWaterMark = -1;
        int resultIx = -1;
        int numNodes = nc.length();

        // loop over all counters and find the one with the
        // least utilization (absolute or relative to the limit)
        for (int i=0; i < numNodes; i++) {
            int ix = (nextCandidate + i) % numNodes;
            int lim = nc.getDenseLimit(ix);
            int cnt = nc.getDenseCounter(ix);
            float ratio = cnt;

            // skip nodes that are already maxed out
            if (lim > cnt) {
                if (assignProportionally)
                    ratio = ratio/lim;

                if (ratio < lowWaterMark || lowWaterMark < 0.0) {
                    lowWaterMark = ratio;
                    resultIx = ix;
                }
            }
        }

        if (resultIx >= 0) {
            nc.addToDenseCounter(resultIx, 1);
            return nc.id(resultIx);
        }

        return -1;
    }

    /**
     * Deserialize an object derived from class TenantNodes
     * 
     * Note that this is not the standard Java serialization interface
     * and that this can be used to pass objects between C++ and Java
     * 
     * @param serializedNodes object in serialized string form
     * @return an object of a class derived from TenantNodes or null,
     *         if the serialized string is empty or invalid
     */
    static public TenantNodes fromSerializedForm(String serializedNodes) {
        String trimmedString = serializedNodes.trim();
        
        if (trimmedString.length() == 0)
            return null;
        
        if (trimmedString.charAt(0) == TenantNodesAS)
            return ASNodes.ASNodesFromSerializedString(trimmedString);
        else if (trimmedString.charAt(0) == TenantNodeSetList)
            return WeightedNodeList.WeightedNodeListFromSerializedString(trimmedString);

    	return null;
    }

    /**
     * Serialize the object into a string
     * 
     * Note that this is not the standard Java serialization interface
     * and that this can be used to pass objects between C++ and Java
     * 
     * @return object in serialized form
     */
    public abstract String toSerializedForm();

    // Identifying characters and delimiters used in the serialized forms,
    // these are also defined in the C++ code in
    // file core/sql/common/NAWNodeSet.cpp. Make sure these don't conflict
    // with the delimiters used in zookeeper nodes that store tenant
    // information (currently they use '=' and ':', see TenantHelper class).
    protected static final char TenantNodesAS = 'A';
    protected static final char TenantNodeSetList = 'L';
    protected static final char TenantNodesDelim = '|';
    protected static final char TenantWeightedNodelistDelim = '*';

}
