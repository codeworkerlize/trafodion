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
//       logic. Please keep this class WeightedNodeList
//       and C++ class NAWNodeSet and derived classes
//       in sync.
// ---------------------------------------------------

/**
 * This class allows a flexible assignment of compute
 * units to nodes of a cluster by just providing a list
 * of node ids and weights.
 */
public class WeightedNodeList extends TenantNodes {

    /**
     * Constructor for a tenant defined by a size only; populate list
     * of nodes by calling placeTenant
     */
    public WeightedNodeList(int totalSize)
    {
        anticipatedSize_ = totalSize;
    }

    /**
     * Constructor for a tenant defined by a list
     * of node ids and weights per node
     */
    public WeightedNodeList(int[] nodeIds, int[] weights)
    {
        nodeIds_ = nodeIds;
        weights_ = weights;
        checkOrder();
        recomputeSize();
        anticipatedSize_ = 0;
    }    

    /**
     * Constructor for a tenant defined by a size and by a list
     * of node ids to use
     */
    public WeightedNodeList(int totalSize, int[] nodeIds)
    {
        nodeIds_ = nodeIds;
        anticipatedSize_ = totalSize;
        checkOrder();
    }    

    @Override
    public int getSize() {
        return (cachedSize_ > 0 ? cachedSize_ : anticipatedSize_);
    }

    @Override
    public int getNumNodes() {
        if (nodeIds_ != null)
            return nodeIds_.length;
        return 0;
    }

    @Override
    public boolean isComplete() {
        return (anticipatedSize_ == 0);
    }

    @Override
    public boolean usesDenseIds()
    {
        return false;
    }

    @Override
    public int getNodeNumber(int ix) {
        int totalWeightSoFar = 0;
        int nodeIx = 0;
        
        if (ix < 0 || ix >= cachedSize_)
            throw new IllegalArgumentException("ix must be >= 0 and < cachedSize_ (" + cachedSize_ + "), ix is: " + ix);
        
        // skip over nodes that contain units numbered lower than ix
        while (totalWeightSoFar + weights_[nodeIx] <= ix)
        {
            totalWeightSoFar += weights_[nodeIx];
            nodeIx++;
        }
        
        // now we are at a node that contains unit number ix
        return nodeIds_[nodeIx];
    }

    @Override
    public int getDistinctNodeId(int ix) {
        return nodeIds_[ix];
    }

    @Override
    public int getDistinctWeight(int ix) {
        return weights_[ix];
    }

    @Override
    public int getCommonWeightForAllNodes(boolean overflow,
                                          int numNodesInCluster) {
        if (cachedSize_ <= 0)
            return -1;

        int result = weights_[0];

        // check whether all weights are the same
        for (int i=0; i<nodeIds_.length; i++) {
            if (weights_[i] != result)
                return -1;
        }

        if (!overflow && numNodesInCluster < getNumNodes())
            // the tenant does not use all nodes and we
            // excluded overflow nodes, so not all nodes
            // have the same weight
            return -1;

        return result;
    }

    @Override
    public int getWeightForNodeNumber(int nodeNum, boolean overflow) {
        for (int i=0; i<nodeIds_.length; i++) {
            if (nodeIds_[i] == nodeNum)
                return weights_[i];
        }

        // if we reach here, nodeNum is not assigned to this tenant
        if (overflow && cachedSize_ > 0) {
            // we need to make a heuristic decision in this case,
            // for now let's return the average weight, rounded up
            return (int) Math.ceil(cachedSize_ / (float) nodeIds_.length);
        }
    
        return 0;
    }
    
    @Override
    public boolean placeTenant(NodeUnitCounters nc, boolean updateCounters) {
        // Imagine the counters as a landscape of mountains
        // and valleys. To place a tenant of size s, imagine
        // pouring s units of water into that landscape such
        // that the water occupies the lowest-lying spots.
        boolean result = false;
        float lowWaterRatio = -1;
        float epsilon = (float) 0.001;
        int numNodesInCluster = nc.length();
        int totalSize = anticipatedSize_;
        int numNodes = 0;
        int[] distrib = new int[numNodesInCluster];
        boolean[] subset = new boolean[numNodesInCluster];

        for (int i=0; i<numNodesInCluster; i++) {
            distrib[i] = 0;
            subset[i] = (nodeIds_ == null);
        }

        if (anticipatedSize_ <= 0)
            throw new IllegalArgumentException(
                    "Expecting a tenant that only has its size specified");

        if (nodeIds_ != null) {
            // this incomplete object provides a list of node ids to use,
            // restrict our choice to these nodes and store their dense ids
            // as "true" values in the subset array
            for (int nx : nodeIds_) {
                // convert the node id into a "dense" id
                int ix = nc.indexOf(nx);

                if (ix >= 0)
                    subset[ix] = true;
            }                
        }

        while (anticipatedSize_ > 0) {
            lowWaterRatio = -1;

            // loop over all counters and find the lowest
            // one (relative to its limit)
            for (int i=0; i < numNodesInCluster; i++) {
                int ix = (nextCandidate_ + i) % numNodesInCluster;
                int lim = nc.getDenseLimit(ix);
                int cnt = nc.getDenseCounter(ix) + distrib[ix];

                if (lim > 0 && subset[ix]) {
                    float ratio = ((float) cnt)/lim;

                    if (ratio < lowWaterRatio || lowWaterRatio < 0) {
                        lowWaterRatio = ratio;
                    }
                }
            }

            // avoid any (unlikely) rounding errors
            lowWaterRatio += epsilon;

            if (lowWaterRatio < 0)
                // While we are ok with exceeding the provided limits, if
                // necessary, that applies only to limits > 0. Limits equal
                // to zero are interpreted as nodes that cannot be used.
                throw new IllegalArgumentException(
                        "Unable to find available node(s) to place compute units for tenant.");

            // now fill in the lowest points of the valleys
            for (int j=0; j < numNodesInCluster && anticipatedSize_ > 0; j++) {
                int ix = (nextCandidate_ + j) % numNodesInCluster;
                int lim = nc.getDenseLimit(ix);
                int cnt = nc.getDenseCounter(ix) + distrib[ix];

                if (lim > 0 && subset[ix] && ((float) cnt)/lim <= lowWaterRatio) {
                    distrib[ix] += 1;
                    anticipatedSize_--;
                    if (lim <= cnt)
                        // we are exceeding the limit
                        result = true;
                }
            }
        }
        
        // now construct the list of nodes and weights for this tenant
        for (int k=0; k < numNodesInCluster; k++)
            if (distrib[k] > 0)
                numNodes++;
        nodeIds_ = new int[numNodes];
        weights_ = new int[numNodes];
        
        int tgtIx = 0;
        
        for (int l=0; l < numNodesInCluster; l++)
            if (distrib[l] > 0) {
                nodeIds_[tgtIx] = nc.id(l);
                weights_[tgtIx] = distrib[l];
                // make sure the node ids are in ascending order
                if (tgtIx > 0 && nodeIds_[tgtIx-1] >= nodeIds_[tgtIx])
                    throw new IllegalArgumentException(
                            "NodeUnitCounter object has out of order node ids");
                if (updateCounters)
                    nc.addToDenseCounter(l,distrib[l]);
                tgtIx++;
            }
            
        recomputeSize();
        if (totalSize != getSize())
            throw new IllegalArgumentException(
                    "Internal error, did not allocate the correct number of units");

        nextCandidate_ = (nextCandidate_ + 1) % 65536;
        return result;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject instanceof WeightedNodeList) {
            WeightedNodeList other = (WeightedNodeList) anObject;

            if (other.cachedSize_ != cachedSize_)
                return false;

            if (other.anticipatedSize_ != anticipatedSize_)
                return false;

            // lengths must match
            if ((other.nodeIds_ != null ? other.nodeIds_.length : 0) !=
                (nodeIds_ != null ? nodeIds_.length : 0))
                return false;

            // this assumes that the list is ordered by node id
            if (nodeIds_ != null)
                for (int i=0; i < nodeIds_.length; i++)
                    if (other.nodeIds_[i] != nodeIds_[i] ||
                        (other.weights_ != null ? other.weights_[i] : 0) !=
                        (weights_ != null ? weights_[i] : 0))
                        return false;

            return true;
        }

        return false;
    }

    // support for the equals() method
    @Override
    public int hashCode() {
        return getSize() + 10 * getNumNodes();
    }

    @Override
    public int placeConnection(NodeUnitCounters nc,
                               boolean assignProportionally) {
        float lowWaterMark = -1;
        int resultIx = -1;
        int numNodes = getNumNodes();
        boolean connectionsAvailable = false;

        if (anticipatedSize_ > 0)
            throw new IllegalArgumentException(
                    "Expecting a fully specified list of nodes and weights");

        // loop over all counters and find the one with the
        // least utilization (absolute or relative to the limit)
        for (int i=0; i < numNodes; i++) {
            int ix = (nextCandidate_ + i) % numNodes;
            int denseIx = nc.indexOf(nodeIds_[ix]);
            if (denseIx >= 0 && weights_[ix] > 0) {
                int lim = nc.getDenseLimit(denseIx);
                int cnt = nc.getDenseCounter(denseIx);
                float ratio = cnt;

                // skip nodes that are already maxed out
                if (lim > cnt) {
                    if (assignProportionally)
                        ratio = ratio / (weights_[ix] * lim);

                    if (ratio < lowWaterMark || lowWaterMark < 0.0) {
                        lowWaterMark = ratio;
                        resultIx = denseIx;
                        // to even out the distribution somewhat,
                        // start the search with the next node
                        // next time
                        nextCandidate_ = ix + 1;
                    }
                }
                else if (lim > 0)
                    connectionsAvailable = true;
            }
        }

        if (resultIx >= 0) {
            nc.addToDenseCounter(resultIx, 1);

            // return a real node id, not a dense one
            return nc.id(resultIx);
        }

        // there are no nodes with available servers for any of our
        // nodes, overflow to a node not assigned to this tenant
        if (!connectionsAvailable) {
            nextCandidate_ = (nextCandidate_ + 1) % nc.length();
            return placeOverflowConnection(
                    nc,
                    assignProportionally,
                    nextCandidate_);
        }

        return -1;
    }

    static WeightedNodeList WeightedNodeListFromSerializedString(String serializedString) {
        // see toSerializedForm() method below for a description of the format
        StringTokenizer tokens = new StringTokenizer(serializedString,
                                                     Character.toString(TenantNodesDelim));
        int numTokens = tokens.countTokens();
        String tok;
        int totalSize = -1;
        int numNodes = -1;

        if (numTokens < 3)
            // we need at least the L|s|n
            return null;

        tok = tokens.nextToken().trim();
        if (tok.length() != 1 || tok.charAt(0) != TenantNodeSetList)
            // missing "L"
            return null;

        tok = tokens.nextToken();
        totalSize = Integer.parseInt(tok);

        tok = tokens.nextToken();
        numNodes = Integer.parseInt(tok);

        if (totalSize <= 0 || numNodes != numTokens - 3)
            // an empty tenant or an inconsistency between
            // the summary information and the list
            return null;
        
        if (numNodes == 0)
            // an incomplete tenant with no node list,
            // just the size is specified
            return new WeightedNodeList(totalSize);
        
        int nodeIds[] = new int[numNodes];
        int weights[] = new int[numNodes];
        int prevNodeId = -1;
        int prevWeight = -2;
        int checkWeight = 0;
        boolean isComplete = true;

        for (int n = 0; n < numNodes; n++) {
            tok = tokens.nextToken();
            StringTokenizer nums = new StringTokenizer(tok,
                    Character.toString(TenantWeightedNodelistDelim));
            int numElems = nums.countTokens();
            int nodeId = -1;
            int weight = -1;

            if (numElems < 1 || numElems > 2 ||
                prevWeight < -1 && numElems < 2)
                // incorrect list element
                return null;

            nodeId = Integer.parseInt(nums.nextToken());
            if (numElems > 1) {
                weight = Integer.parseInt(nums.nextToken());
                prevWeight = weight;
                if (n == 0)
                    isComplete = (weight != -1);
                else if (isComplete != (weight != -1))
                    // a mix of -1 and other weights is not allowed
                    return null;
            }
            else
                weight = prevWeight;

            checkWeight += weight;

            if (nodeId < 0 || weight < -1 || weight == 0 || nodeId <= prevNodeId)
                // invalid node id or weight or node ids not
                // in ascending order
                return null;

            nodeIds[n] = nodeId;
            weights[n] = weight;
        }

        if (isComplete) {
            if (totalSize != checkWeight)
                // total size and sum of individual weights are inconsistent
                return null;

            return new WeightedNodeList(nodeIds, weights);
        }

        // an incomplete list with a total size and a list of nodes
        return new WeightedNodeList(totalSize, nodeIds);
    }

    @Override
    public String toSerializedForm() {
        // The serialized form of a WeightedNodeList has the following form:
        //
        //  L|s|n [ |n1*w1 [ |nn[*wn] ... ]]
        //
        // The first part is the letter L (TenantNodeSetList), followed
        // by the size (in units) of the list, followed by the number of
        // nodes involved.
        //
        // The rest is a list of 0 or more fields of two decimal numbers
        // separated by a star. The first number is a node id, the second
        // number is the weight. For subsequent fields, the weight can be
        // omitted if it is the same as the previous weight.
        //
        // For an incompletely specified tenant, the number of nodes is 0
        // or, if nodes are specified, all weights are -1. In the latter
        // case, the tenant allocation is to be restricted to the specified
        // nodes.
        //
        StringBuilder sb = new StringBuilder(100);
        int size = getSize();
        int numNodes = getNumNodes();
        int prevWeight = -2;
        
        if (size == 0)
            return "";

        // add the L|s|n
        sb.append(TenantNodeSetList);
        sb.append(TenantNodesDelim);
        sb.append(size);
        sb.append(TenantNodesDelim);
        sb.append(numNodes);

        if (numNodes > 0) {
            // add the list
            for (int i=0; i<nodeIds_.length; i++) {
                int weight = -1;

                if (weights_ != null)
                    weight = weights_[i];

                sb.append(TenantNodesDelim);
                sb.append(nodeIds_[i]);
                if (weight != prevWeight) {
                    sb.append(TenantWeightedNodelistDelim);
                    sb.append(weight);
                    prevWeight = weight;
                }
            }
        }

        return sb.toString();
    }

    private void recomputeSize()
    {
        cachedSize_ = 0;
        if (weights_ != null)
            for (int i=0; i<weights_.length; i++)
                cachedSize_ += weights_[i];
    }

    private void checkOrder()
    {
        if (nodeIds_ != null)
            {
                int prevId = -1;

                for (int i : nodeIds_)
                    if ((prevId >= 0 && i <= prevId) || i < 0)
                        throw new IllegalArgumentException(
                            "node ids are not in ascending order or are negative");
                    else
                        prevId = i;
            }
        if (weights_ != null && weights_.length != nodeIds_.length)
            throw new IllegalArgumentException(
                "Weights don't have same length as node ids");
    }

    // We may have an object for which we only know
    // the size so far, not the node list. For such
    // objects we store the anticipated size here.
    private int anticipatedSize_;
    private int cachedSize_;
    // a list of nodes for this tenant
    private int[] nodeIds_;
    private int[] weights_;

    // start the search at some more - or less - random place
    private static int nextCandidate_ = 3;
}
