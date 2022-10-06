// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
#ifndef NAWNODESET_H
#define NAWNODESET_H

#include "common/Collections.h"
#include "common/NABitVector.h"
#include "common/NAString.h"
#include "export/ComDiags.h"
#include "export/NABasicObject.h"

// forward declarations
class NAClusterInfo;

// Contents of this file
class NAWNodeSet;
class NAASNodes;
class NAWSetOfNodeIds;

// Definitions of a tenant unit or slice, these values
// should match those in class com.esgyn.common.CGroupHelper
#define NAWNODESET_CORES_PER_SLICE       4
#define NAWNODESET_MEMORY_GB_PER_SLICE   32
#define NAWNODESET_MIN_SLICES_PER_TENANT 4

// ------------------------------------------------------------------
// Classes to represent a weighted set of nodes in an EsgynDB cluster.
// Such weighted node sets are currently used for the following
// purposes:
//
// - Define the nodes of a tenant. The weights define the amount
//   of resources (measured in "units" or "cubes") assigned to
//   the tenant on each node.
// - Define the nodes to be used for a query, without tenant support.
//   The weights define how many ESPs to run on a particular node.
// - Define the nodes to be used for a query within a tenant.
//   The weights define how many ESPs to run on a particular node.
//
// NAWNodeSet is the base class of a class hierarchy:
//
// - NAASNodes is a set of nodes defined by Adaptive Segmentation
// - NAWSetOfNodeIds is a set of node ids with a weight assigned
//   to each of the node ids
//
// NOTE: There are Java classes equivalent to these C++ classes:
//
//       C++               Java
//       ----------------  -------------------------------------
//       NAWNodeSet        com.esgyn.common.TenantNodes
//       NAASNodes         com.esgyn.common.ASNodes
//       NAWSetOfNodeIds   com.esgyn.common.WeightedNodeList
//       ----------------  -------------------------------------
//
//       These classes must be kept in sync, since their
//       serialized form is exchanged via JNI
//
// An NANodeSet can be serialized into a string and deserialized
// from a string, through a static method.
// ------------------------------------------------------------------

/* Here are some code examples:

   // Assign nodes for a parallel query with n parallel instances:

   int delta = <random, non-negative offset>;
   for (int i=delta; i<n+delta; i++)
     printf("Assign node %d\n", getNodeNumber(i));

   // List the units assigned to the tenant, in the order
   // in which ESPs are assigned

   for (int i=0; i<getTotalWeight(); i++)
     printf("Unit %d is allocated on node %d\n",
            i,
            getNodeNumber(i));

   // List the nodes assigned to the tenant in ascending
   // order and with the number of units assigned to each
   // node
   for (int i=0; i<getNumNodes(); i++)
     printf("Node %d has %d units\n",
            getDistinctNodeId(i),
            getDistinctWeight(i));

   Note that if the object uses "dense" node ids, the ids
   must be converted back to regular node ids.
 */

class NAWNodeSet : public NABasicObject {
 public:
  // create the node set for the current tenant, from
  // information stored in the CLI
  static int getNumNodesOfCluster();
  static NAWNodeSet *getDefaultNodes(CollHeap *outHeap);

  // make a deep copy of the same derived class
  virtual NAWNodeSet *copy(CollHeap *outHeap) const = 0;

  // the total number of nodes in the set
  virtual int getNumNodes() const = 0;

  // the total weight of all the nodes in the set
  virtual int getTotalWeight() const = 0;

  // for a tenant, the weights indicate the size in "units" or "cubes"
  int getTenantSize() const { return getTotalWeight(); }

  // Get the node id with index ix
  // (ix ranges between 0 and getTotalWeight() - 1).
  // This could be a "dense" or a real node id, check
  // method usesDenseNodeIds().
  virtual int getNodeNumber(int ix) const = 0;

  // downcast methods
  virtual const NAASNodes *castToNAASNodes() const;
  virtual NAASNodes *castToNAASNodes();
  virtual const NAWSetOfNodeIds *castToNAWSetOfNodeIds() const;
  virtual NAWSetOfNodeIds *castToNAWSetOfNodeIds();

  // can this tenant use all nodes (not necesarily all of the
  // resources) of the cluster?
  virtual bool canUseAllNodes(NAClusterInfo *nacl) const;

  // can this tenant use all the cores and all the memory available
  // to Trafodion on this cluster?
  bool canUseAllAvailableUnits(NAClusterInfo *nacl) const;

  // compare two sets (just the nodes, no other attributes)
  virtual bool usesTheSameNodes(const NAWNodeSet *other, NAClusterInfo *nacl) const;

  // adjust a proposed size so it is a legal size (for a tenant of a
  // DoP, for example), optionally specify a subset size into which
  // the new size must fit
  virtual int makeSizeFeasible(int proposedSize, int subsetSize = -1, int round = 0) const = 0;

  // is this object completely specified or do we still need to
  // assign specific nodes to it?
  virtual bool isComplete() const = 0;

  // does this node set use "dense" node ids that need
  // to be translated back into actual nids, using
  // NAClusterInfo::mapLogicalToPhysicalNodeId(), for
  // example?
  virtual bool usesDenseNodeIds() const = 0;

  // get the weight for a given node id
  // (e.g. to determine the size of the cgroup to create on that node)
  virtual int getWeightForNodeId(int nodeId) const = 0;

  // methods to get a list of <getNumNodes()> distinct node
  // ids, ordered by node id, and their associated total weights,
  // this is for storing info in the metadata and for displaying
  // the node allocation for the tenant
  virtual int getDistinctNodeId(int ix) const = 0;
  virtual int getDistinctWeight(int ix) const = 0;

  // check for a valid definition
  virtual const char *isInvalid(NAClusterInfo *nacl) const = 0;

  virtual bool tryToMakeValid(NAClusterInfo *nacl, bool updateCliContext) = 0;

  // serialize this object into a string
  virtual void serialize(NAText &output) const = 0;

  // create a new object from a serialized string
  static NAWNodeSet *deserialize(const char *serializedString, CollHeap *heap);

  virtual void display() const = 0;

  virtual bool describeTheCluster() = 0;
};

// NAASNodes: A class to determine the nodes on which to run a query
// with Adaptive Segmentation (AS).
//
// This object is used to represent the nodes used for both tenants
// and queries.  Adaptive Segmentation assumes that we have a cluster
// with <clusterSize> nodes. Queries and tenants have a <size> that
// must be a factor or a multiple of <clusterSize>.  <size> is
// expressed in "cubes", currently those are units of 4 cores and 32
// GB of memory. Each physical node of the cluster may encompass
// multiple such "cubes". The nodes and cubes assigned to a tenant or
// query can represented by an NAASNodes object (or by another object
// in the NAWNodeSet class hierarchy).
//
// An NAASNodes object identifies
//
//    <numNodes> = min(<size>, <clusterSize>)
//
// nodes. These nodes are called a "segment". Adaptive Segmentation
// places limits on what nodes can form a segment. Other classes
// derived from NAWNodeSet avoid these limitations, but they lose
// the load balancing advantages of Adaptive Segmentation.
//
// Elasticity
// ----------
//
// Because an EsgynDB cluster can be resized dynamically, we associate
// a fixed <clusterSize> with an NAASNodes object, typically the
// number of nodes in the cluster when the object was created. This
// matters mostly for tenants, which are long-lived objects. The
// <clusterSize> associated with an NAASNodes object should generally
// not exceed the number of nodes in the cluster.
//
// Elastic resize operations may leave holes in the normally
// contiguous node ids of a cluster. The formulas used for AS
// don't work with such non-contiguous numbers, so we need to
// use contiguous numbers for these calculations and translate
// them into actual node ids at the end.
//
// Affinity
// --------
//
// We assign an <affinity> to an NAASNodes object (a tenant or a
// query). The affinity is a non-negative integer. It determines which
// nodes we assign to the tenant or query. <size>, <affinity> and
// <clusterSize> together define a virtual "segment" of the cluster,
// with <size> nodes.
//
// A cluster has <numSegs> different segments for a given <size>, with
//
// <numSegs> = <clusterSize> / <size>.
//
// The segment identified by <size>, <affinity>, <clusterSize>, is the
// set of nodes that satisfies this equation for the node number n:
//
// n mod <numSegs> = <affinity> mod <numSegs>
//
// For any segment with <affinity> a, node a mod <clusterSize> is
// part of that segment.
//
// Examples.
//
//  When <clusterSize> = 8 and <size> = 4
//
//  <numSegs> = 8 / 4 = 2
//
//  For <affinity> = 0, the node set = {0, 2, 4, 6} defines segment 1
//  For <affinity> = 1, the node set = {1, 3, 5, 7} defines segment 2
//  For <affinity> = 2, the node set = {2, 4, 6, 0} defines segment 1
//  For <affinity> = 3, the node set = {3, 5, 7, 1} defines segment 2
//  ... ... ...
//
//
//  When <clusterSize> = 12 and <size> = 4
//  <numSegs> = 12 / 4 = 3
//
//  For <affinity> = 0, the node set = {0, 3, 6, 9} defines segment 1
//  For <affinity> = 1, the node set = {1, 4, 7, 10} defines segment 2
//  For <affinity> = 2, the node set = {2, 5, 8, 11} defines segment 3
//  For <affinity> = 3, the node set = {3, 6, 9, 9} defines segment 1
//  For <affinity> = 4, the node set = {4, 7, 19, 1} defines segment 2
//  ... ... ...
//
//
// Queries running within tenants, subsets
// ---------------------------------------
//
// Now we want to run a query with degree of parallelism (DoP) <qsize>
// inside a tenant of size <tsize> and affinity <taffinity>. In this
// case, <qsize> must be a factor or a multiple of <tsize>. This is
// because the segment formed by the query must be a subset of the
// tenant's segment. In other words, the nodes assigned to the query
// must be a subset of the nodes assigned to the tenant.  This is
// achieved by making the query object a subset of the tenant object.
// Right now this is handled entirely in Generator::remapESPAllocationAS().
//
// Note that although there are some restrictions for the
// relationships between <size> and <clusterSize>, there is no
// requirement for these numbers to be powers of two.
//
// Example 1
// ---------
//
//   clusterSize    = 36  (2*2*3*3)
//   tenant size    =  9
//   query dop      =  3
//   affinity       = 10
//
// The affinity identifies node 10 (10 mod 36) as the starting node.
// This identifies tenant segment 2 (0-based), with node numbers
// { 2, 6, 10, 14, 18, 22, 26, 30, 34 }.
// To run a query with DoP 3 in this teant, we use every 3rd node of
// the tenant segment, starting with node 10 and wrapping around, if
// necessary:
// { 10, 22, 34 }
//
// Example 2
// ---------
//
// Here are all possible segments for a query with DoP Q running inside
// a tenant of size T, assuming a <clusterSize> of 6.
// The list excludes values Q > T and T > 6. The excluded values have
// the same segments as those of their largest factors shown below.
//
// Note that each line involves two NAASNodes objects, one for the
// tenant and one for the query. Both of these objects share the same
// <affinity> and <clusterSize> = 6, the only difference is their <size>s
// T and Q:
//
//                  nodes
//  T   Q  affinity 012345
// --- --- -------- ------
//  1   1  0        0
//  1   1  1         1
//  1   1  2          2
//  1   1  3           3
//  1   1  4            4
//  1   1  5             5
//
//  2   1  0        0
//  2   1  1         1
//  2   1  2          2
//  2   1  3           3
//  2   1  4            4
//  2   1  5             5
//
//  2   2  0,3      0  3
//  2   2  1,4       1  4
//  2   2  2,5        2  5
//
//  3   1  0        0
//  3   1  1         1
//  3   1  2          2
//  3   1  3           3
//  3   1  4            4
//  3   1  5             5
//
//  3   3  0,2,4    0 2 4
//  3   3  1,3,5     1 3 5
//
//  6   1  0        0
//  6   1  1         1
//  6   1  2          2
//  6   1  3           3
//  6   1  4            4
//  6   1  5             5
//
//  6   2  0,3      0  3
//  6   2  1,4       1  4
//  6   2  2,5        2  5
//
//  6   3  0,2,4    0 2 4
//  6   3  1,3,5     1 3 5
//
//  6   6  0-5      012345
//

class NAASNodes : public NAWNodeSet {
 public:
  // Constructor. Specifying -1 for all three values creates an
  // NAASNodes object that spans the entire (homegeneous)
  // cluster. Specifying -1 for the affinity creates an incompletely
  // specified object that can be completed with a call to
  // TenantHelper_JNI methods.
  NAASNodes(int totalWeight, int clusterSize, int affinity);

  // virtual methods

  virtual NAWNodeSet *copy(CollHeap *outHeap) const;
  virtual int getNumNodes() const;
  virtual int getTotalWeight() const;
  virtual int getNodeNumber(int ix) const;
  virtual const NAASNodes *castToNAASNodes() const;
  virtual NAASNodes *castToNAASNodes();
  virtual bool canUseAllNodes(NAClusterInfo *nacl) const;
  virtual bool usesTheSameNodes(const NAWNodeSet *other, NAClusterInfo *nacl) const;
  virtual int makeSizeFeasible(int proposedSize, int subsetSize = -1, int round = 0) const;
  virtual bool isComplete() const;
  virtual bool usesDenseNodeIds() const;
  virtual int getWeightForNodeId(int nodeId) const;
  virtual int getDistinctNodeId(int ix) const;
  virtual int getDistinctWeight(int ix) const;
  virtual const char *isInvalid(NAClusterInfo *nacl) const;
  virtual bool tryToMakeValid(NAClusterInfo *nacl, bool updateCliContext);
  virtual void serialize(NAText &output) const;

  // create a new object from a serialized string
  static NAASNodes *deserialize(const char *serializedString, CollHeap *heap);

  // methods specific to this class

  int getAffinity() const { return affinity_; }
  int getClusterSize() const { return clusterSize_; }

  // distance between nodes in the segment
  int getNodeDistanceInSegment() const { return clusterSize_ / getNumNodes(); }

  // identify which tenant segment among those with the same size we are using
  int getIdOfTenantSegment() const { return (affinity_ % (clusterSize_ / getNumNodes())); }

  // generate a random affinity value with the same node set
  int randomizeAffinity(int randomAffinity) const;

  void display() const;

  bool describeTheCluster();

 private:
  int size_;
  int affinity_;
  int clusterSize_;
};

class NAWSetOfNodeIds : public NAWNodeSet {
 public:
  // There are two ways to build this object:
  // 1. Pass in a positive anticipated total weight, then
  //    call addNode() zero or more times with weight -1.
  //    This creates an incompletely specified object that
  //    is restricted to a set of nodes, or unrestricted,
  //    if no nodes were added at all.
  // 2. Pass in an anticipated total weight of 0, then call
  //    addNode() one or more times with a positive weight.
  //    This creates a completely specified object.
  // Both methods can be used when calling TenantHelper_JNI
  // methods.
  NAWSetOfNodeIds(CollHeap *heap, int anticipatedTotalWeight)
      : anticipatedTotalWeight_(anticipatedTotalWeight),
        cachedTotalWeight_(0),
        commonWeight_(-2),
        nodeIds_(heap),
        individualWeights_(NULL),
        heap_(heap) {}

  // virtual methods
  virtual NAWNodeSet *copy(CollHeap *outHeap) const;
  virtual int getNumNodes() const;
  virtual int getTotalWeight() const;
  virtual int getNodeNumber(int ix) const;
  virtual const NAWSetOfNodeIds *castToNAWSetOfNodeIds() const;
  virtual NAWSetOfNodeIds *castToNAWSetOfNodeIds();
  virtual bool usesTheSameNodes(const NAWNodeSet *other, NAClusterInfo *nacl) const;
  virtual int makeSizeFeasible(int proposedSize, int subsetSize = -1, int round = 0) const;
  virtual bool isComplete() const;
  virtual bool usesDenseNodeIds() const;
  virtual int getWeightForNodeId(int nodeId) const;
  virtual int getDistinctNodeId(int ix) const;
  virtual int getDistinctWeight(int ix) const;
  virtual const char *isInvalid(NAClusterInfo *nacl) const;
  virtual bool tryToMakeValid(NAClusterInfo *nacl, bool updateCliContext);
  virtual void serialize(NAText &output) const;

  // create a new object from a serialized string
  static NAWSetOfNodeIds *deserialize(const char *serializedString, CollHeap *heap);

  // methods specific to this class

  // add a node (must be done once per node, in ascending order by id)
  void addNode(int nodeId, int weight = -1);

  // undo placement, return object to incomplete state, leaving
  // the node list in place
  void makeIncomplete();

  bool describeTheCluster();

  const ARRAY(short) & getNodeIdArray() const { return nodeIds_; }

  void display() const;

 private:
  // We may have an object for which we only know
  // the size so far, not the node list. For such
  // objects we store the anticipated size here
  // and all other fields are empty.
  int anticipatedTotalWeight_;

  // a cached sum of the weights of all nodes
  int cachedTotalWeight_;

  // if all nodes have the same weight, we store it here
  // and individualWeights_ is NULL
  int commonWeight_;

  // an array storing the node ids of all nodes in the set,
  // in ascending order
  ARRAY(short) nodeIds_;

  // an optional array assigning an individual weight to
  // each node id in nodeIds_, used only if nodes don't
  // all have the same weight
  ARRAY(short) * individualWeights_;

  CollHeap *heap_;
};

// NAWNodeSet comes in two flavors: real IDs and "dense IDs".
// Some callers only care about real IDs. Rather than force
// these callers to know about both flavors, and force them
// to have the NAClusterInfo object available in order to
// convert from dense to real, we instead provide a wrapper
// object to hide this detail from them. As this object today
// is always transient and built as a stack variable, it does
// not need to inherit from NABasicObject.

class NAWNodeSetWrapper {
 public:
  NAWNodeSetWrapper(NAWNodeSet *nodeSet, NAClusterInfo *clusterInfo) : nodeSet_(nodeSet), clusterInfo_(clusterInfo){};

  ~NAWNodeSetWrapper(){/* nothing to do */};

  // returns true if there is an NAWNodeSet wrapped here
  bool hasNodeSet() { return nodeSet_ != NULL; };

  // find a node different than the one passed in; if there
  // is none available, return -1. Always deals with "real" node
  // id; none of this "dense" node id business
  int findAlternativeNode(int node);

 private:
  NAWNodeSet *nodeSet_;
  NAClusterInfo *clusterInfo_;
};

#endif
