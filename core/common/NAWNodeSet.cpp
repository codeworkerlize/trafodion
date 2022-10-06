// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
#include "common/NAWNodeSet.h"

#include <cstdlib>

#include "cli/Context.h"
#include "cli/Globals.h"
#include "common/NAClusterInfo.h"

// delimiter used in serialized strings, avoid commas, since the
// serialized string will be sent as part of a comma-separated list to
// the compiler. The same delimiters also need to be defined on the
// Java side, see class com/esgyn/common/TenantNodes.
const char NAWNodeSetDelim = '|';
const char *NAWNodeSetDelimStr = "|";

// delimiter between node and weight in NAWSetOfNodeIds
const char NAWSetOfNodeIdsDelim = '*';

// the first field in a serialized node set is a single character,
// indicating the derived type
const char NAWNodeSetAS = 'A';
const char NAWNodeSetList = 'L';

// some helper methods for serialization and deserialization
static void addDelimiter(NAText &s) { s.append(NAWNodeSetDelimStr); }

static void addInt(int v, NAText &s) {
  char buf[20];

  snprintf(buf, sizeof(buf), "%d", v);
  s.append(buf);
}

static void addIntField(int v, NAText &s) {
  addInt(v, s);
  addDelimiter(s);
}

static void addString(const char *v, NAText &s) { s.append(v); }

static void addStringField(const char *v, NAText &s) {
  s.append(v);
  addDelimiter(s);
}

static bool isEmpty(const char *s) { return (s == NULL || s[0] == 0); }

static const char *skipWhiteSpace(const char *s) {
  while (*s == ' ' || *s == '\t') s++;

  return s;
}

static const char *skipToNextField(const char *s) {
  while (*s != 0 && *s != NAWNodeSetDelim) s++;

  if (*s == NAWNodeSetDelim) return skipWhiteSpace(++s);

  return NULL;
}

// get an integer that may be and entire or a partial field
static bool getInt(const char *&s, int &result) {
  long r = strtol(s, const_cast<char **>(&s), 10);

  if (result > INT_MAX || result < INT_MIN) return false;

  result = (int)r;
  return true;
}

// get a field (or rest of a field) consisting of only an integer
static bool getIntField(const char *&s, int &result) {
  bool retval = getInt(s, result);

  s = skipWhiteSpace(s);

  // if there is anything left other than the integer,
  // that's an error
  if (s && *s != NAWNodeSetDelim) return false;

  // skip over the delimiter
  s = skipToNextField(s);

  return true;
}

static bool getStringField(const char *&s, NAText &result) {
  const char *start = skipWhiteSpace(s);
  s = skipToNextField(start);

  if (s)
    result.assign(start, s - start);
  else
    result.assign(start, strlen(start));

  return true;
}

// ----------------------------------------------------
// Methods for class NAWNodeSet
// ----------------------------------------------------

const NAASNodes *NAWNodeSet::castToNAASNodes() const { return NULL; }

NAASNodes *NAWNodeSet::castToNAASNodes() { return NULL; }

const NAWSetOfNodeIds *NAWNodeSet::castToNAWSetOfNodeIds() const { return NULL; }

NAWSetOfNodeIds *NAWNodeSet::castToNAWSetOfNodeIds() { return NULL; }

bool NAWNodeSet::canUseAllNodes(NAClusterInfo *nacl) const { return (getNumNodes() >= nacl->getTotalNumberOfCPUs()); }

bool NAWNodeSet::canUseAllAvailableUnits(NAClusterInfo *nacl) const {
  int w = getTotalWeight();

  return w < 0 || w >= nacl->numberOfTenantUnitsInTheCluster();
}

bool NAWNodeSet::usesTheSameNodes(const NAWNodeSet *other, NAClusterInfo *nacl) const {
  // This base class can be called by the derived method to
  // compare objects of different classes. This class should
  // NOT be called to compare two objects of the same derived
  // class, because it does not do a full comparison!!

  // for now we only do this very simple check
  return (canUseAllNodes(nacl) && other->canUseAllNodes(nacl));
}

NAWNodeSet *NAWNodeSet::deserialize(const char *serializedString, CollHeap *heap) {
  const char *s = serializedString;

  if (isEmpty(s)) return NULL;

  s = skipWhiteSpace(s);
  if (isEmpty(s))
    // string with only white space
    return NULL;

  // the first character determines the node type
  if (serializedString[0] == NAWNodeSetAS)
    return NAASNodes::deserialize(serializedString, heap);
  else if (serializedString[0] == NAWNodeSetList)
    return NAWSetOfNodeIds::deserialize(serializedString, heap);
  else
    return NULL;
}

// ----------------------------------------------------
// Methods for class NAASNodes
// ----------------------------------------------------

NAASNodes::NAASNodes(int totalWeight, int clusterSize, int affinity)
    : size_(totalWeight), clusterSize_(clusterSize), affinity_(affinity) {
  assert(size_ >= -1 && clusterSize_ >= -1 && affinity_ >= -1 && size_ != 0 && clusterSize_ != 0);
}

NAWNodeSet *NAASNodes::copy(CollHeap *outHeap) const { return new (outHeap) NAASNodes(size_, clusterSize_, affinity_); }

int NAASNodes::getNumNodes() const {
  // size_ is a factor or a multiple of clusterSize_.
  // If the size is a factor (including the cluster size itself), then
  // we can use size_ nodes. Otherwise, we can use clusterSize_ nodes,
  // since that's the maximum number of nodes we have available.
  return (size_ <= clusterSize_ ? size_ : clusterSize_);
}

int NAASNodes::getTotalWeight() const {
  // the total weight of an Adaptive Segmentation query or tenant is
  // its size
  return size_;
}

int NAASNodes::getNodeNumber(int ix) const {
  return (int)((long)affinity_ + ix * getNodeDistanceInSegment()) % clusterSize_;
}

const NAASNodes *NAASNodes::castToNAASNodes() const { return this; }

NAASNodes *NAASNodes::castToNAASNodes() { return this; }

bool NAASNodes::canUseAllNodes(NAClusterInfo *nacl) const {
  return (size_ == -1 && affinity_ == -1 && clusterSize_ == -1 || NAWNodeSet::canUseAllNodes(nacl));
}

bool NAASNodes::usesTheSameNodes(const NAWNodeSet *other, NAClusterInfo *nacl) const {
  if (getNumNodes() != other->getNumNodes()) return false;

  const NAASNodes *oth = other->castToNAASNodes();

  if (oth == NULL) return NAWNodeSet::usesTheSameNodes(other, nacl);

  return (getClusterSize() == oth->getClusterSize() && getIdOfTenantSegment() == oth->getIdOfTenantSegment());
}

int NAASNodes::makeSizeFeasible(int proposedSize, int subsetSize, int round) const {
  if (subsetSize <= 0) subsetSize = clusterSize_;

  if (subsetSize % proposedSize == 0 || proposedSize % subsetSize == 0)
    // proposed number is a factor or a multiple
    return proposedSize;

  int result = proposedSize;

  if (proposedSize < subsetSize) {
    if (round <= 0)
      // find a factor < proposedSize
      while ((subsetSize % (--result)) != 0)
        ;

    if (round >= 0) {
      int result2 = proposedSize;

      // find a factor > proposedSize
      while ((subsetSize % (++result2)) != 0)
        ;

      if (round > 0 || (result2 - proposedSize) <= (proposedSize - result)) result = result2;
    }
    return result;
  } else {
    // find a multiple
    int result = proposedSize - (proposedSize % subsetSize);

    if (round < 0 || (round == 0 && (proposedSize - result) < subsetSize / 2))
      // next lower multiple of subsetSize
      return result;
    else
      // next higher multiple of subsetSize
      return result + subsetSize;
  }
}

bool NAASNodes::isComplete() const { return affinity_ >= 0; }

bool NAASNodes::usesDenseNodeIds() const {
  // Adaptive Segmentation works only on dense ids
  return true;
}

int NAASNodes::getWeightForNodeId(int /*nodeId*/) const { return (size_ < clusterSize_ ? 1 : size_ / clusterSize_); }

int NAASNodes::getDistinctNodeId(int ix) const {
  int stride = getNodeDistanceInSegment();

  // This is similar to getNodeNumber, which also returns
  // a list of distinct <getNumNodes()> node numbers as the
  // first elements, but here we use affinity_ % stride,
  // to make the returned node ids ascending. Taking the
  // modulo clusterSize_ isn't really necessary.
  return ((affinity_ % stride) + ix * stride) % clusterSize_;
}

int NAASNodes::getDistinctWeight(int ix) const {
  // the weights are all the same
  return getWeightForNodeId(-1);
}

const char *NAASNodes::isInvalid(NAClusterInfo *nacl) const {
  if (size_ == -1 && clusterSize_ == -1 && affinity_ == -1) return NULL;
  if (clusterSize_ <= 0) return "Size must be a positive integer";
  if (affinity_ < -1) return "Affinity must be greater or equal to -1";
  if (clusterSize_ > nacl->getTotalNumberOfCPUs())
    return "Number of nodes is greater than the number of nodes in the cluster";

  if (size_ <= 0 || size_ != makeSizeFeasible(size_))
    return "Size must be a factor or a multiple of the number of nodes";

  return NULL;  // NULL means a valid object
}

bool NAASNodes::tryToMakeValid(NAClusterInfo *nacl, bool updateCliContext) {
  if (clusterSize_ <= 0) clusterSize_ = 1;
  if (size_ <= 0) size_ = 1;

  if (clusterSize_ > nacl->getTotalNumberOfCPUs()) {
    // The object was defined with a cluster size greater than our
    // current cluster. This is not recommended, tenants should be
    // resized to a smaller number of nodes before resizing the
    // physical cluster. Ok, someone did not heed that
    // recommendation... So, we resize the object here to a smaller
    // number of nodes that allows us to retain a similar size.
    int numPhysNodes = nacl->getTotalNumberOfCPUs();

    // start with a resize operation that's brutal but that always
    // works: size 1
    int newClusterSize = numPhysNodes;
    int newSize = 1;

    // Now try different numbers of nodes and tenant sizes that come
    // close to the original. Example: The tenant is defined for a
    // cluster with 8 nodes, but we only have 7 at this time. This
    // would give us tenant sizes 1 and 7. We may be better off to
    // define the tenant for a cluster with 6 nodes and have choices
    // of tenant sizes 1, 2, 3 and 6. Note that we only do a limited
    // number of tries, up to 4 right now.

    for (int t = 0; t < 4 && t < numPhysNodes; t++) {
      // determine a new tenant size that is feasible for
      // a cluster with numPhysCPUs - t nodes
      int feasibleSize = makeSizeFeasible(size_, numPhysNodes - t, -1);

      if (feasibleSize > newSize) {
        // this is a better combination of number of nodes and
        // tenant size, getting closer to what we originally had
        newSize = feasibleSize;
        newClusterSize = numPhysNodes - t;
      }
    }
    size_ = newSize;
    clusterSize_ = newClusterSize;
  }  // clusterSize is defined for a larger cluster

  size_ = makeSizeFeasible(size_, -1, -1);

  if (isInvalid(nacl)) return false;

  if (updateCliContext) {
    // update the CLI context, so we don't get repeated log messages
    NAWNodeSet *availableNodes = GetCliGlobals()->currContext()->getAvailableNodes();

    if (availableNodes) {
      NAASNodes *asNodes = availableNodes->castToNAASNodes();

      if (asNodes) {
        asNodes->size_ = size_;
        asNodes->clusterSize_ = clusterSize_;
      }
    }
  }

  return true;
}

void NAASNodes::serialize(NAText &output) const {
  // the serialized string of an NAASNodes looks like this:
  // "A|s|c|a" where A is the capital letter NAWNodeSetAS
  // and s, c and a are decimal integers representing size,
  // cluster size and affinity
  output.append(1, NAWNodeSetAS);
  addDelimiter(output);
  addIntField(size_, output);
  addIntField(clusterSize_, output);
  addInt(affinity_, output);
}

// create a new object from a serialized string
NAASNodes *NAASNodes::deserialize(const char *serializedString, CollHeap *heap) {
  const char *s = serializedString;
  int size = -1, clusterSize = -1, affinity = -1;

  if (s[0] != NAWNodeSetAS)
    // first letter indicating class type does not match
    return NULL;

  s = skipToNextField(s);
  if (isEmpty(s))
    // missing second and subsequent fields
    return NULL;

  if (!getIntField(s, size)) return NULL;

  if (!getIntField(s, clusterSize)) return NULL;

  if (!getInt(s, affinity)) return NULL;

  if (!isEmpty(s)) s = skipWhiteSpace(s);

  if (!isEmpty(s))
    // extra information after last field
    return NULL;

  return new (heap) NAASNodes(size, clusterSize, affinity);
}

int NAASNodes::randomizeAffinity(int randomAffinity) const {
  assert(affinity_ >= 0 && randomAffinity >= 0);
  // Given a non-negative random number, choose a new affinity such
  // that we produce the same set of node ids as with the existing
  // affinity value, affinity_. The purpose of this is to randomize
  // the starting node id produced.
  int nd = getNodeDistanceInSegment();
  // the return value r is relatively close to randomAffinity and it
  // satisfies the condition
  // r % nd == affinity_ % nd
  return randomAffinity - (randomAffinity % nd) + (affinity_ % nd);
}

// ----------------------------------------------------
// Methods for class NAWSetOfNodeIds
// ----------------------------------------------------

NAWNodeSet *NAWSetOfNodeIds::copy(CollHeap *outHeap) const {
  NAWSetOfNodeIds *result = new (outHeap) NAWSetOfNodeIds(outHeap, anticipatedTotalWeight_);

  for (CollIndex ix = 0; ix < nodeIds_.entries(); ix++) result->addNode(nodeIds_[ix], getDistinctWeight(ix));

  return result;
}

int NAWSetOfNodeIds::getNumNodes() const { return nodeIds_.entries(); }

int NAWSetOfNodeIds::getTotalWeight() const {
  return (anticipatedTotalWeight_ > 0 ? anticipatedTotalWeight_ : cachedTotalWeight_);
}

int NAWSetOfNodeIds::getNodeNumber(int ix) const {
  // we have two ways of returning nodes:
  // a) when we have a common weight for all nodes, we
  //    just go round-robin in the list of node ids
  // b) when we have different weights, we return each
  //    node id w times, where w is its weight

  if (commonWeight_ > 0)
    // a)
    return nodeIds_[ix % nodeIds_.entries()];
  else {
    // b)
    int resultIx = 0;
    // the results of this method are the same mod cachedTotalWeight_
    int unitsToSkip = ix % cachedTotalWeight_;

    // skip over nodes with a total weight of "unitsToSkip" units
    while ((unitsToSkip -= getDistinctWeight(resultIx)) >= 0) resultIx++;

    return nodeIds_[resultIx];
  }
}

const NAWSetOfNodeIds *NAWSetOfNodeIds::castToNAWSetOfNodeIds() const { return this; }

NAWSetOfNodeIds *NAWSetOfNodeIds::castToNAWSetOfNodeIds() { return this; }

bool NAWSetOfNodeIds::usesTheSameNodes(const NAWNodeSet *other, NAClusterInfo *nacl) const {
  if (getNumNodes() != other->getNumNodes()) return false;

  const NAWSetOfNodeIds *oth = other->castToNAWSetOfNodeIds();

  if (oth == NULL) return NAWNodeSet::usesTheSameNodes(other, nacl);

  // compare the lists of node ids
  if (nodeIds_.entries() != oth->nodeIds_.entries()) return false;

  // we asserted elsewhere that the lists are sorted
  for (CollIndex i = 0; i < nodeIds_.entries(); i++)
    if (nodeIds_[i] != oth->nodeIds_[i]) return false;

  return true;
}

int NAWSetOfNodeIds::makeSizeFeasible(int proposedSize, int subsetSize, int round) const { return proposedSize; }

bool NAWSetOfNodeIds::isComplete() const { return anticipatedTotalWeight_ == 0; }

bool NAWSetOfNodeIds::usesDenseNodeIds() const { return false; }

int NAWSetOfNodeIds::getWeightForNodeId(int nodeId) const {
  CollIndex ix = nodeIds_.find(nodeId);

  if (ix == NULL_COLL_INDEX) return 0;

  return getDistinctWeight(ix);
}

const char *NAWSetOfNodeIds::isInvalid(NAClusterInfo *nacl) const {
  if (anticipatedTotalWeight_ < 0) return "Specified size must be a positive value";
  // assume it is always valid, we assert if we see basic
  // mistakes, like negative node ids or weights <= 0
  return NULL;
}

bool NAWSetOfNodeIds::tryToMakeValid(NAClusterInfo *nacl, bool updateCliContext) { return true; }

// The serialized form of an NAWSetOfNodeIds has the
// following form:
//
//  L|s|n [ |n1*w1 [ |nn[*wn] ... ]]
//
// This is the letter L (NAWNodeSetList), followed
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
void NAWSetOfNodeIds::serialize(NAText &output) const {
  int prevWeight = -2;

  // assert(getNumNodes() > 0);
  output.append(1, NAWNodeSetList);
  addDelimiter(output);
  addInt(getTotalWeight(), output);
  addDelimiter(output);
  addInt(getNumNodes(), output);

  for (CollIndex ix = 0; ix < nodeIds_.entries(); ix++) {
    int weight = getDistinctWeight(ix);

    // add delimiter and node id, the "|n" in "|n[*w]"
    addDelimiter(output);
    addInt(nodeIds_[ix], output);

    if (weight != prevWeight) {
      // add the "*w"
      output.append(1, NAWSetOfNodeIdsDelim);
      addInt(weight, output);
      prevWeight = weight;
    }
  }
}

NAWSetOfNodeIds *NAWSetOfNodeIds::deserialize(const char *serializedString, CollHeap *heap) {
  const char *s = serializedString;
  int size = 0;
  int numNodes = 0;

  // parse the L|s|n

  if (s[0] != NAWNodeSetList)
    // first letter indicating class type does not match
    return NULL;

  s = skipToNextField(s);
  if (isEmpty(s))
    // missing second and subsequent fields
    return NULL;
  if (!getInt(s, size) || size <= 0) return NULL;

  s = skipToNextField(s);
  if (isEmpty(s)) return NULL;
  if (!getInt(s, numNodes) || numNodes <= 0) return NULL;

  s = skipToNextField(s);
  if (isEmpty(s)) return NULL;

  // parse the list
  NAWSetOfNodeIds *result = new (heap) NAWSetOfNodeIds(heap, 0);
  int prevWeight = -2;
  int prevNodeNum = -1;
  int nodeNum = -1;
  int weight = -1;

  do {
    // read the mandatory "n"
    if (!getInt(s, nodeNum)) goto error;

    if (s[0] == NAWSetOfNodeIdsDelim) {
      // read the optional ":w"
      s++;
      if (!getInt(s, weight)) goto error;
      prevWeight = weight;
    } else
      // no weight specified, use previous weight
      weight = prevWeight;

    if (nodeNum <= prevNodeNum || weight < -1 || weight == 0)
      // received junk values for node id or weight,
      // node ids must be in ascending order
      goto error;

    // now use the regular addNode method
    result->addNode(nodeNum, weight);
    prevNodeNum = nodeNum;

    s = skipToNextField(s);
  } while (!isEmpty(s));

  if (result->getTotalWeight() != size || result->getNumNodes() != numNodes) return NULL;  // inconsistency

  return result;

error:
  delete result;
  return NULL;
}

void NAWSetOfNodeIds::addNode(int nodeId, int weight) {
  assert(nodeId >= 0 && (weight > 0 || weight == -1));
  CollIndex ix = nodeIds_.entries();

  // assert that we insert the node ids in ascending order,
  // sorry, no built-in sort
  assert(ix == 0 || nodeIds_[ix - 1] < nodeId);
  nodeIds_.insertAt(ix, nodeId);

  if (weight > 0) cachedTotalWeight_ += weight;

  // now adjust common and individual weights, if needed
  if (weight != commonWeight_) {
    if (nodeIds_.entries() == 1)
      // set the common weight with the weight of the first entry
      // we add
      commonWeight_ = weight;
    else {
      // this is a bit more tricky, we have a weight that is
      // different from other weights already in the cluster, we
      // have to update and maybe even create the array of
      // individual weights
      if (!individualWeights_) {
        individualWeights_ = new (heap_) ARRAY(short)(heap_);
        for (int i = 0; i < nodeIds_.entries() - 1; i++) individualWeights_->insertAt(i, commonWeight_);
        commonWeight_ = -2;
      }

      // -1 weights and positive weights don't mix
      assert(weight > 0);
      individualWeights_->insertAt(ix, weight);
    }
  }
}

int NAWSetOfNodeIds::getDistinctNodeId(int ix) const { return nodeIds_[ix]; }

int NAWSetOfNodeIds::getDistinctWeight(int ix) const {
  if (!nodeIds_.used(ix)) return 0;

  if (individualWeights_ == NULL)
    return commonWeight_;
  else
    return (*individualWeights_)[ix];
}

void NAWSetOfNodeIds::makeIncomplete() {
  if (anticipatedTotalWeight_ <= 0) {
    anticipatedTotalWeight_ = cachedTotalWeight_;
    cachedTotalWeight_ = 0;
    commonWeight_ = -2;
    if (individualWeights_) {
      delete individualWeights_;
      individualWeights_ = NULL;
    }
  }
}

bool NAASNodes::describeTheCluster() {
  if (size_ != clusterSize_) return false;

  NAClusterInfo *nac = CmpCommon::context()->getClusterInfo();

  return (nac && nac->getTotalNumberOfCPUs() == size_);
}

// describe the cluster self. That is, total weight == # of nodes
// and each node has the same weight.
bool NAWSetOfNodeIds::describeTheCluster() {
  int n = getNumNodes();

  if (getTotalWeight() != n) return false;

  NAClusterInfo *nac = CmpCommon::context()->getClusterInfo();

  if (!nac || nac->getTotalNumberOfCPUs() != n) return false;

  int weight = -1;

  for (int i = 0; i < n; i++) {
    if (weight < 0) weight = getWeightForNodeId(i);

    if (weight != getWeightForNodeId(i)) return false;
  }

  return true;
}

void NAWSetOfNodeIds::display() const {
  cout << "NAWSetOfNodeIds::display():"
       << ", anticipatedTotalWeight_=" << anticipatedTotalWeight_ << ", cachedTotalWeight_=" << cachedTotalWeight_
       << ", commonWeight_=" << commonWeight_ << endl;
}

void NAASNodes::display() const {
  cout << "NAASNodes::display():"
       << ", size_=" << size_ << ", affinity_=" << affinity_ << ", clusterSize_=" << clusterSize_ << endl;
}

// ----------------------------------------------------
// Methods for class NAWNodeSetWrapper
// ----------------------------------------------------

int NAWNodeSetWrapper::findAlternativeNode(int node) {
  // return a "real" node id different than the input value; return input
  // value if no others are available

  // try to pick the node as a function of the input value so we don't return the
  // same node for all input values on a non-trivial cluster

  int returnedNode = node;
  if (nodeSet_)  // if the set of nodes is a subset of the cluster (e.g. multi-tenancy)
  {
    if (nodeSet_->usesDenseNodeIds()) {
      int totalWeight = nodeSet_->getTotalWeight();
      if (clusterInfo_ && (totalWeight > 0)) {
        int startPoint = (node + 1) % totalWeight;
        int logicalNode = nodeSet_->getNodeNumber(startPoint);
        returnedNode = clusterInfo_->mapLogicalToPhysicalNodeId(logicalNode);
      }
    } else {
      int numNodes = nodeSet_->getNumNodes();
      if (numNodes > 1)  // if numNodes == 1, we'd loop infinitely below
      {
        int startPoint = (node + 1) % numNodes;
        returnedNode = nodeSet_->getNodeNumber(startPoint);
        // we might get back the same physical node ID we had before,
        // so keep incrementing until we get a new one
        while (returnedNode == node) {
          startPoint = (startPoint + 1) % numNodes;
          returnedNode = nodeSet_->getNodeNumber(startPoint);
        }
      }
    }
  } else if (clusterInfo_) {
    // we are free to pick any node in the cluster
    int numberOfNodes = clusterInfo_->getTotalNumberOfCPUs();
    int logicalNode = clusterInfo_->mapPhysicalToLogicalNodeId(node);
    logicalNode = (logicalNode + 1) % numberOfNodes;
    returnedNode = clusterInfo_->mapLogicalToPhysicalNodeId(logicalNode);
  }

  return returnedNode;
}
