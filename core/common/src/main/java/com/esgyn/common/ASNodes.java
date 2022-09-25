// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common;

import java.util.StringTokenizer;

// ---------------------------------------------------
// NOTE: There is a corresponding C++ class hierarchy
//       in core/sql/common/NAWNodeSet.h with similar
//       logic. Please keep this class ASNodes and
//       C++ class NAASNodes in sync.
// ---------------------------------------------------

/**
 * This class allows to assign "size" compute units
 * to the nodes of a cluster with "clusterSize" nodes.
 * The assignment is defined by just three numbers,
 * the parameters of the constructor. However, this
 * class requires that size is a factor or a multiple
 * of clusterSize. The methods used here are part of
 * the Adaptive Segmentation (AS) feature.
 */
public class ASNodes extends TenantNodes {

    /**
     * Constructor for the system tenant with no limits
     **/
    public ASNodes() {
        size_ = -1;
        clusterSize_ = -1;
        affinity_ = -1;
    }

    /**
     * Constructor for a tenant using Adaptive Segmentation
     *
     * @param size        size of the tenant (must be a factor or
     *                    a multiple of clusterSize). This is
     *                    measured in "cubes", which are currently
     *                    hardware resources equivalent to 4 cores
     *                    and 32 GB of memory.
     * @param clusterSize size of the cluster to assume for this
     *                    tenant, should usually match the actual
     *                    cluster size, except when resizing a
     *                    cluster
     * @param affinity    the affinity selects a specific set of
     *                    nodes from the cluster. One of the nodes
     *                    of the tenant is affinity % clusterSize.
     *                    This value can be left unspecified (-1)
     *                    and can be set with the placeTenant
     *                    method later.
     */
    public ASNodes(int size, int clusterSize, int affinity)
    {
        if (size < 1)
            if (size == -1 && clusterSize == -1 && affinity == -1) {
                // create a node set that stretches across the entire cluster,
                // with as many units/slices as the cluster provides
                clusterSize = CGroupHelper.getClusterSize();
                size = CGroupHelper.getMaxSlicesPerCluster();
                // size should be a factor or a multiple of clusterSize, but
                // just in case (e.g. if a node is down)
                if (size > clusterSize)
                    // round down to the next multiple of clusterSize
                    size -= size % clusterSize;
                else
                    // round up to clusterSize
                    size = clusterSize;
                affinity = 0; // ideally, to randomize, use local node id
            }
            else
                throw new IllegalArgumentException("size must be a positive integer");

        if (clusterSize < 1)
            throw new IllegalArgumentException("clusterSize must be a positive integer");

        if (clusterSize % size != 0 && size % clusterSize != 0)
            throw new IllegalArgumentException("size must be a factor or a multiple of clusterSize");

        size_ = size;
        clusterSize_ = clusterSize;
        affinity_ = affinity;
    }

    public int getClusterSize() {
        return clusterSize_;
    }

    public int getAffinity() {
        return affinity_;
    }

    public void setClusterSize(int clusterSize) {
        clusterSize_ = clusterSize;
    }

    public void setAffinity(int affinity) {
        affinity_ = affinity;
    }

    /**
     *  Compute the "stride", the distance between nodes for this tenant
     *
     *  <p>For tenants smaller than the cluster size, we use every nth node
     *     and this method computes the n
     *
     *  @return the distance between nodes for this tenant, a positive number
     */
    public int getNodeDistanceInSegment() {
        return clusterSize_ / getNumNodes();
    }

    @Override
    public boolean isComplete() {
        return (affinity_ >= 0);
    }

    /**
     *  Return possible sizes (in number of nodes) of tenants
     *
     *  <p>This is used to offer the user a choice of valid tenant sizes.
     *  Note that this method does not return multiples of numNodes as
     *  valid tenant sizes, even though they are valid.
     *
     *  @param startHere smallest size to return, a value between 1 and numNodes
     *  @param numNodes  number of nodes in the cluster, a positive number
     *  @return smallest possible size of a tenant that is greater
     *          than or equal to startHere and does not exceed numNodes
     */
    static public int getNextPossibleTenantSize(int startHere, int numNodes)
        throws IllegalArgumentException
    {
        if (numNodes < 1)
            throw new IllegalArgumentException("numNodes must be a positive number");

        if (startHere > numNodes)
            throw new IllegalArgumentException("Maximum tenant size is numNodes");

        if (startHere < 1)
            throw new IllegalArgumentException("Minimum value for startHere is 1");

        // skip all numbers that are not a factor of numNodes
        while (numNodes % startHere != 0)
            startHere++;

        return startHere;
    }


    @Override
    public int getSize() {
        return size_;
    }

    @Override
    public int getNumNodes() {
        return (size_ < clusterSize_) ? size_ : clusterSize_;
    }

    @Override
    public boolean usesDenseIds() {
        return true;
    }

    @Override
    public int getNodeNumber(int ix) {
        return (int) ((long) affinity_ + ix * getNodeDistanceInSegment()) %
            clusterSize_;
    }

    @Override
    public int getDistinctNodeId(int ix) {
        int stride = getNodeDistanceInSegment();

        // This is similar to getNodeNumber, which also returns
        // a list of distinct <getNumNodes()> node numbers as the
        // first elements, but here we use affinity_ % stride,
        // to make the returned node ids ascending. Taking the
        // modulo clusterSize_ isn't really necessary.
        return ((affinity_ % stride) + ix * stride) % clusterSize_;
    }

    @Override
    public int getDistinctWeight(int ix) {
        return (size_ <= clusterSize_) ? 1 : size_ / clusterSize_;
    }

    @Override
    public int getCommonWeightForAllNodes(boolean overflow,
                                          int numNodesInCluster) {
        if (!overflow && size_ < clusterSize_)
            // tenant does not use all nodes and we don't
            // consider overflow nodes
            return -1;
        
        // all nodes of the adaptive segment have the same weight
        // and any overflow nodes (if the tenant size is smaller
        // than the cluster) also should use the same weight
        return (size_ <= clusterSize_) ? 1 : size_ / clusterSize_;
    }

    @Override
    public int getWeightForNodeNumber(int nodeNum, boolean overflow) {
        int result = (size_ <= clusterSize_) ? 1 : size_ / clusterSize_;

        if (!overflow &&
            size_ < clusterSize_ &&
            nodeNum % (clusterSize_ / size_) != affinity_ % (clusterSize_ / size_)) {
            // nodeNum is not part of the adaptive segment
            result = 0;
        }

        return result;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject instanceof ASNodes) {
            ASNodes other = (ASNodes) anObject;

            return (other.size_ == size_ &&
                    other.clusterSize_ == clusterSize_ &&
                    other.affinity_ == affinity_);
        }

        return false;
    }

    // support for the equals() method
    @Override
    public int hashCode() {
        return (10000 * size_ + 100 * clusterSize_) ^ affinity_;
    }

    // For ASNodes, placeTenant() computes the affinity, based on
    // the overall load of the system. The object must have a valid
    // size_ and clusterSize_, affinity can be any value and will be
    // overwritten.
    @Override
    public boolean placeTenant(NodeUnitCounters nc, boolean updateCounters) {
        boolean result = false;
        int tenantWeight = 1;

        // do some basic argument checks
        if (nc.length() < clusterSize_)
            throw new IllegalArgumentException(
                "existingTenants must be an array with at least as many entries as the assume cluster size");

        if (size_ > clusterSize_)
            {
                // for the purpose of this method, limit the number of
                // tenant nodes to the number of nodes in the cluster
                // and adjust the weights instead
                tenantWeight *= (size_ / clusterSize_);
            }

        // the stride is the spacing between nodes for this tenant
        int stride = getNodeDistanceInSegment();
        int lowestTotalWeight = -1;
        int lowestMaxWeight = -1;
        int bestAffinity = -1;
        int highestBuddyTotalWeight = 0;
        int highestBuddyMaxWeight = -1;

        // there are "stride" possibilities to place the tenant;
        // evaluate them all
        for (int affinity=0; affinity<stride; affinity++)
            {
                int totalWeight = 0;
                int maxWeight = -1;
                int buddyTotalWeightForAffinity = 0;
                int buddyMaxWeightForAffinity = -1;
                boolean isBetter = false;

                // find total and max weights for this set of nodes
                for (int node=affinity; node<clusterSize_; node += stride)
                    {
                        int cnt = nc.getDenseCounter(node);

                        totalWeight += cnt;
                        if (maxWeight < cnt)
                            maxWeight = cnt;
                    }

                // Look at my "buddy" node sets and see whether they
                // are already occupied. Arrange the affinities for
                // this tenant size (between 0 and "stride") in a
                // circle. Then look at all factors "f" of "stride"
                // other than 1 and "stride", in descending order.
                // Affinities "b" that satisfy the equation
                // ("affinity" + "f") % "stride" == "b" are my
                // buddies.  My best buddies come first, then come the
                // less close buddies when we look at smaller factors.

                // While we want the lowest utilized nodes for
                // ourselves, we want the highest utilized buddies.
                // Doing so leave the greatest degree of freedom for
                // future, larger, tenants.
                for (int f=stride-1; f>1; f--)
                    if (stride % f == 0)
                        {
                            // look at factor f of stride; enumerate
                            // all other affinities b that are a
                            // multiple of f away from me in the
                            // circle
                            for (int b = (affinity + f) % stride;
                                 b != affinity;
                                 b = (b + f) % stride)
                                {
                                    int bn = b;

                                    int buddyTotalWeight = 0;
                                    int buddyMaxWeight = -1;

                                    // look at all the nodes bn of by buddy
                                    // tenant with affinity b
                                    do
                                        {
                                            int cnt = nc.getDenseCounter(bn);
                                            
                                            buddyTotalWeight += cnt;
                                            if (buddyMaxWeight < cnt)
                                                buddyMaxWeight = cnt;
                                            bn = (bn + stride) % clusterSize_;
                                        } while (bn != b);

                                    if (buddyMaxWeight > buddyMaxWeightForAffinity)
                                        {
                                            // note this will give preference to the second
                                            // affinity we look at if all the existing tenant
                                            // weights are the same, because we reach this line
                                            // when highestBuddyMaxWeight == -1
                                            buddyMaxWeightForAffinity = buddyMaxWeight;
                                            buddyTotalWeightForAffinity = 0;
                                        }
                                    else if (buddyMaxWeight == buddyMaxWeightForAffinity &&
                                             buddyTotalWeight > buddyTotalWeightForAffinity)
                                        {
                                            buddyTotalWeightForAffinity = buddyTotalWeight;
                                        }

                                } // loop over buddy affinities
                        } // loop over factors of stride

                // Decide whether this set of nodes, identified by
                // "affinity" is better than what we have so far.
                // We have three priorities when assigning tenants
                // to nodes:
                // First priority:  Minimizing the highest load put
                //                  on any single node
                // Second priority: Spreading the load out as evenly
                //                  as possible
                // Third priority:  Leaving room for larger tenants
                //                  by placing smaller tenants such
                //                  that they are not in the way of
                //                  bigger tenants. We do this by
                //                  preferring positions where the
                //                  complementary "buddy" positions
                //                  are already filled.

                if (lowestMaxWeight < 0 || maxWeight < lowestMaxWeight)
                    // first priority
                    isBetter = true;
                else if (maxWeight == lowestMaxWeight)
                    if (totalWeight < lowestTotalWeight)
                        // second priority
                        isBetter = true;
                    else if (totalWeight == lowestTotalWeight)
                        if (buddyMaxWeightForAffinity > highestBuddyMaxWeight)
                            // third priority, based on max
                            isBetter = true;
                        else if (buddyMaxWeightForAffinity == highestBuddyMaxWeight &&
                                 buddyTotalWeightForAffinity > highestBuddyTotalWeight)
                            // third priority, based on total
                            isBetter = true;

                if (isBetter)
                    {
                        // yes, we prefer this affinity
                        lowestTotalWeight = totalWeight;
                        lowestMaxWeight = maxWeight;
                        highestBuddyTotalWeight = buddyTotalWeightForAffinity;
                        highestBuddyMaxWeight = buddyMaxWeightForAffinity;
                        bestAffinity = affinity;
                    }

            }

        // add the weight per node for this new tenant to the weights array
        for (int n=bestAffinity; n<clusterSize_; n += stride)
            {
                if (nc.getDenseCounter(n) + tenantWeight > nc.getDenseLimit(n))
                    result = true;
                if (updateCounters)
                    nc.addToDenseCounter(n, tenantWeight);
            }
        
        affinity_ = bestAffinity;

        return result;
    }

    @Override
    public int placeConnection(NodeUnitCounters nc,
                               boolean assignProportionally) {
        float bestScore = -1;
        int result = -1;
        boolean connectionsAvailable = false;

        // the stride is the spacing between nodes for this tenant, it is also a factor
        // of numNodes
        int stride = getNodeDistanceInSegment();

        // the nodes "n" usable by this tenant all satisfy this condition:
        // n % stride = affinity_ % stride
        int affinity = affinity_ % stride;
        int clusterSize = (clusterSize_ > 0 ? clusterSize_ : nc.length());

        // start looking at the second possible node of the cluster
        // (if any) to avoid a bias towards lower node numbers
        int startNode = (affinity + stride) % clusterSize;
        int n = startNode;

        // find the node with the lowest utilization and return its number
        do
            {
                int cnt = nc.getDenseCounter(n);
                int limit = nc.getDenseLimit(n);

                if (limit > cnt)
                    {
                        // the default score is the number of existing
                        // connections
                        float currScore = (float) cnt;

                        if (assignProportionally && limit > 0)
                            // express the score as how much of the
                            // max. allowed connections we have used
                            currScore = currScore / limit;

                        if (bestScore < 0.0 || currScore < bestScore)
                            {
                                // of the usable nodes, this one is the most
                                // suitable so far, based on the comparison
                                // method we chose
                                bestScore = currScore;
                                result = n;
                            }
                    }
                else if (limit > 0)
                    connectionsAvailable = true;

                n = (n + stride) % clusterSize;
            }
        while (n != startNode && bestScore != 0.0);

        if (result >= 0)
            // update the connections array
            nc.addToDenseCounter(result, 1);
        else if (!connectionsAvailable)
            return placeOverflowConnection(nc, assignProportionally, startNode);
        
        return result;
    }

    static ASNodes ASNodesFromSerializedString(String serializedString) {
        // see toSerializedForm() method below for a description of the format
        StringTokenizer tokens = new StringTokenizer(serializedString,
                                                     Character.toString(TenantNodesDelim));
        int numTokens = tokens.countTokens();
        String tok;
        int totalSize = -1;
        int clusterSize = -1;
        int affinity = -1;

        if (numTokens < 4)
            // we need a string in the form A|sz|cs|aff
            return null;
        
        tok = tokens.nextToken().trim();
        if (tok.length() != 1 || tok.charAt(0) != TenantNodesAS)
            // missing "A"
            return null;

        tok = tokens.nextToken();
        totalSize = Integer.parseInt(tok);

        tok = tokens.nextToken();
        clusterSize = Integer.parseInt(tok);

        tok = tokens.nextToken();
        affinity = Integer.parseInt(tok);

        if (totalSize <= 0 || clusterSize <= 0)
            return null;

        return new ASNodes(totalSize, clusterSize, affinity);
    }

    @Override
    public String toSerializedForm() {
        // the serialized string of an ASNodes object looks like this:
        // "A|s|c|a" where A is the capital letter NAWNodeSetAS
        // and s, c and a are decimal integers representing size,
        // cluster size and affinity
        StringBuilder sb = new StringBuilder(20);
        sb.append(TenantNodesAS);
        sb.append(TenantNodesDelim);
        sb.append(size_);
        sb.append(TenantNodesDelim);
        sb.append(clusterSize_);
        sb.append(TenantNodesDelim);
        sb.append(affinity_);
        return sb.toString();
    }

    // private data members
    private int size_;
    private int clusterSize_;
    private int affinity_;
}
