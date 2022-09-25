// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.common;

/**
 * This class represents the nodes of a cluster,
 * it's node ids, and a number of weights or units
 * allocated on each node. The class also allows
 * limits on every node. Here are two use cases:
 * 
 * The distribution of tenant "units" across a
 * cluster. The limits represent the size of
 * each node, measured in units.
 * 
 * The distribution of ODBC/JDBC connections
 * across the nodes of a cluster. The limits
 * represent the number of mxosrvrs on each
 * node.
 */
public class NodeUnitCounters {

    public NodeUnitCounters(int ids[], int commonLimit)
    {
        ids_ = ids;
        counters_ = new int[ids.length];
        limits_ = new int[ids.length];
        for (int i=0; i<ids.length; i++)
        {
            counters_[i] = 0;
            limits_[i] = commonLimit;
        }
        checkConsistency();
    }
    
    // Constructor with a list of node ids of the cluster,
    // in ascending order
    public NodeUnitCounters(int ids[])
    {
        this(ids, Integer.MAX_VALUE);
    }
    
    // Constructor with a list of node ids of the cluster, in
    // ascending order, a list of counters (e.g. active connections)
    // and a list of limits (e.g. max. number of connections). Note
    // that all three arrays must have the same length.
    public NodeUnitCounters(int ids[], int counters[], int limits[])
    {
        if (ids.length != counters.length ||
            ids.length != limits.length)
            throw new IllegalArgumentException(
                    "The lengths of the ids, counters and limits arrays must match");
        ids_ = ids;
        counters_ = counters;
        limits_ = limits;
        checkConsistency();
    }
    
    /**
     * Return the length of the counter array, that is the number
     * of nodes in the cluster
     * 
     * @return the number of counters in the object
     */
    public int length() {
        return (ids_ == null ? 0 : ids_.length);
    }
    
    /**
     * Return the node id in position ix of the counter array
     * 
     * @param ix position in the counter array, must be between
     *        0 and length() - 1
     * @return node id for the ixth counter in the object
     */
    public int id(int ix) {
        return ids_[ix];
    }
    
    /**
     * Get the counter for a given node id
     * 
     * @param id node id for which to return the counter
     * @return counter value for node id "id"
     */
    public int getCounterById(int id) {
        return counters_[idx(id)];
    }
    
    /**
     * Get the counter for a given index
     * 
     * @param ix index in the counter array for which to return the counter
     * @return counter value for index "ix"
     */
    public int getDenseCounter(int ix) {
        return counters_[ix];
    }
    
    /**
     * Get the limit for a given node id
     * 
     * @param id node id for which to return the limit
     * @return limit for node id "id"
     */
    public int getLimitById(int id) {
        return limits_[idx(id)];
    }

    /**
     * Get the total limit of all nodes
     *
     * @return sum of all limits or Integer.MAX_VALUE if
     *         there are no limits
     */
    public int getSumOfLimits() {
        int result = 0;

        for (int i : limits_)
            if (i == Integer.MAX_VALUE)
                return Integer.MAX_VALUE;
            else
                result += i;

        return result;
    }

    /**
     * Get the limit for a given node index
     * 
     * @param ix index in the counter array for which to return the limit
     * @return limit for index "ix"
     */
    public int getDenseLimit(int ix) {
        return limits_[ix];
    }

    /**
     * Set the value of a counter for a given id
     * 
     * @param id node id of the counter to change
     * @param val new value of the counter
     */
    public void setCounterById(int ix, int val) {
        if (val < 0)
            throw new IllegalArgumentException(
                    "Counter becomes negative");
        counters_[idx(ix)] = val;
    }

    /**
     * Set the value of a counter at a given index
     * 
     * @param ix index of the counter to change
     * @param al new value of the counter
     */
    public void setDenseCounter(int ix, int val) {
        if (val < 0)
            throw new IllegalArgumentException(
                    "Counter becomes negative");
        counters_[ix] = val;
    }

    /**
     * Add a value to a counter for a given id
     * 
     * @param id node id of the counter to change
     * @param delta delta value to add (positive or negative)
     */
    public void addToCounterById(int id, int delta) {
        int check = counters_[idx(id)] += delta;
        
        if (check < 0)
            throw new IllegalArgumentException(
                    "Counter becomes negative");
    }

    /**
     * Add a value to a counter at a given index
     * 
     * @param ix index of the counter to change
     * @param delta delta value to add (positive or negative)
     */
    public void addToDenseCounter(int ix, int delta) {
        int check = (counters_[ix] += delta);
        
        if (check < 0)
            throw new IllegalArgumentException(
                    "Counter becomes negative");
    }

    /**
     * set all limits to a specified value
     * 
     * @param limit new limit for all nodes
     */
    public void setAllLimits(int limit)
    {
        for (int i=0; i<limits_.length; i++)
            limits_[i] = limit;
    }
    
    /**
     * set the limit for a given id (not index) to
     * a new value
     * 
     * @param id     node id
     * @param limit  new limit for node with the specified id
     */
    public void setLimitById(int id, int limit)
    {
        limits_[idx(id)] = limit;
    }
    
    /**
     * set the limit for a given index to a new value
     * 
     * @param id     dense node id (index into ids)
     * @param limit  new limit for node with this index
     */
    public void setDenseLimit(int id, int limit)
    {
        limits_[id] = limit;
    }
    
    /**
     * Add all the units (on various nodes) of tenant t to
     * the counters.
     * 
     * @param t tenant nodes to add
     * @return true if adding this tenant exceeded at
     *              least one of the specified limits,
     *              false otherwise
     */
    public boolean addTenant(TenantNodes t)
    {
        return addOrSubtractTenant(t, 1);
    }
    
    /**
     * Remove all the units (on various nodes) of tenant t
     * from the counters.
     * 
     * @param t tenant nodes to remove
     */
    public void subtractTenant(TenantNodes t)
    {
        addOrSubtractTenant(t, -1);
    }

    public int indexOf(int i)
    {
        return idx(i);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(100);

        for (int i=0; i<ids_.length; i++) {
            if (i > 0)
                sb.append(",");
            sb.append(ids_[i]);
            sb.append(":");
            sb.append(counters_[i]);
            sb.append(":");
            sb.append(limits_[i]);
        }

        return sb.toString();
    }

    public void getOversubscriptionInfo(OversubscriptionInfo osi,
                                        TenantNodes t) {
        double mostOversubscribed = 0.0;
        int[] tempLimits = limits_;

        if (t != null)
            {
                // if a TenantNodes object is passed in, we
                // restrict the oversubscription info to the
                // nodes contained in that object by setting
                // the limits of all the other nodes to 0
                int tenantNumNodes = t.getNumNodes();

                tempLimits = new int[limits_.length];

                for (int i=0; i<tempLimits.length; i++)
                    tempLimits[i] = 0;

                for (int n=0; n<tenantNumNodes; n++)
                    {
                        int ix = idx(t.getDistinctNodeId(n));

                        if (ix >= 0)
                            tempLimits[ix] = limits_[ix];
                    }
            }

        osi.init();
        osi.setMeasure(OversubscriptionInfo.NUM_AVAILABLE_NODES_IX, ids_.length);

        for (int ix=0; ix<ids_.length; ix++)
            {
                // set some measures for the cluster that don't depend on oversubscription
                osi.addToMeasure(OversubscriptionInfo.NUM_AVAILABLE_UNITS_IX, limits_[ix]);
                osi.addToMeasure(OversubscriptionInfo.NUM_ALLOCATED_UNITS_IX, counters_[ix]);

                // check for oversubscription on node ix
                if (tempLimits[ix] > 0 && counters_[ix] > tempLimits[ix])
                    {
                        double ratio = ((double) counters_[ix]) / tempLimits[ix];
                        // this node is oversubscribed
                        osi.addToMeasure(OversubscriptionInfo.NUM_OVERSUBSCRIBED_NODES_IX, 1);

                        if (ratio > mostOversubscribed)
                            {
                                // we found the most oversubscribed node so far
                                osi.setMeasure(OversubscriptionInfo.MOST_OVERSUBSCRIBED_NODE_ID_IX, ix);
                                osi.setMeasure(OversubscriptionInfo.MOST_OVERSUBSCRIBED_AVAIL_IX, tempLimits[ix]);
                                osi.setMeasure(OversubscriptionInfo.MOST_OVERSUBSCRIBED_ALLOC_IX, counters_[ix]);
                                mostOversubscribed = ratio;
                            }
                    }
            }
    }
    
    private boolean addOrSubtractTenant(TenantNodes t, int delta)
    {
        boolean result = false;
        int s = t.getSize();
        boolean dense = t.usesDenseIds();
        
        for (int i=0; i<s; i++)
        {
            int nn = t.getNodeNumber(i);
            
            if (!dense)
                nn = idx(nn);

            counters_[nn] += delta;
            if (delta > 0 &&
                counters_[nn] > limits_[nn])
                // we are exceeding the limit on a node for this tenant
                result = true;
        }
        
        return result;
    }
    
    private void checkConsistency()
    {
        if (ids_.length != counters_.length ||
            ids_.length != limits_.length)
            throw new IllegalArgumentException(
                    "ids, counters and limits arrays must have the same length");
        
        int prevId = -1;
        
        // check that ids are listed in ascending order and also check whether
        // they are dense, meaning numbered 0 ... n-1 without any missing ids
        for (int i=0; i<ids_.length; i++)
            {
                if (prevId >= 0) {
                    if (ids_[i] <= prevId)
                        throw new IllegalArgumentException(
                                "ids are not in ascending order");
                }
                else
                    prevId = ids_[i];
            }
    }
    
    private int idx(int id)
    {
        // start with id as a likely result value (no holes in the ids)
        int result = id;

        if (result >= ids_.length)
            result = ids_.length - 1;

        // since the ids are sorted, non-negative, distinct integers,
        // it is sufficient to look in index "result" and lower
        // indexes only
        while (result >= 0)
            if (id == ids_[result])
                return result;
            else
                result--;

        return -1;
    }
    
    private int ids_[];
    private int counters_[];
    private int limits_[];
}
