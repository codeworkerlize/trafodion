
//==============================================================================
// Implementation for classes NodeMapEntry and Nodemap.
//==============================================================================

#include "optimizer/NodeMap.h"

#include <stdio.h>
#include <string.h>

#include <queue>

#include "cli/CliSemaphore.h"
#include "optimizer/PartFunc.h"
#include "arkcmp/CompException.h"
#include "comexe/FragDir.h"
#include "common/cextdecs.h"
#include "common/trafconfig.h"
#include "exp/exp_function.h"
#include "generator/Generator.h"
#include "optimizer/Analyzer.h"
#include "optimizer/OptimizerSimulator.h"
#include "optimizer/SchemaDB.h"
#include "optimizer/opt.h"
#include "sqlcomp/NADefaults.h"

static const int nodeNameLen = TC_PROCESSOR_NAME_MAX;  // defined in trafconf/trafconfig.h
//<pb>
//==============================================================================
//  Helper functions called only by NodeMap member functions.
//==============================================================================

//==============================================================================
//  Determine whether or not a specified DP2 volume name exists in a specified
// container collection of DP2 volume names.
//
//  Note: this routine insists upon a non-null DP2 volume name.
//
// Input:
//  container  -- specified container of DP2 volume names.
//  volumeName -- specified DP2 volume name.
//
// Output:
//  none
//
// Return:
//  TRUE if volume name found in collection; FALSE otherwise.
//
//==============================================================================
static NABoolean volumeFound(const DP2VolumeNamesContainer &container, const char &volumeName) {
  //-------------------------------------
  //  Ensure DP2 volume name is not null.
  //-------------------------------------
  CMPASSERT(volumeName != 0);

  //-----------------------------------------------
  //  Loop over all entries in collection searching
  // for specified DP2 volume name.
  //-----------------------------------------------
  for (CollIndex idx = 0; idx < container.entries(); idx++) {
    if (container[idx] && strcmp(container[idx], &volumeName) == 0) {
      return TRUE;
    }
  }

  return FALSE;

}  // volumeFound()
//<pb>
//==============================================================================
// Methods for class NodeMapEntry; it associates a disk process name with
// an SMP node and a cluster.
//==============================================================================

//==============================================================================
//  NodeMapEntry constructor.
//
// Input:
//  fullName  --  fully qualified, null terminated partition name which
//                  includes cluster name.
//  heap      --  heap in which to allocate disk process name for this node map
//                  entry.
//  tableIdent  -- unique identifier assigned by NATable.cpp for each table
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
NodeMapEntry::NodeMapEntry(char *fullName, char *givenName, CollHeap *heap, int tableIdent, NABoolean noService)
    : heap_(heap), partitionState_(ACTIVE) {
  int error;

  CMPASSERT(fullName);
  short nameBufferSize = strlen(fullName) + 1;
  char *name = new (heap_) char[nameBufferSize];
  short length = 4;

  //----------------------------------------------------------
  // Save the original, full partition-name.
  //----------------------------------------------------------
  partitionName_ = new (heap_) char[strlen(fullName) + 1];
  strcpy(partitionName_, fullName);

  //----------------------------------------------------------
  // Extract cluster name from fully qualified partition name.
  //----------------------------------------------------------
  NABoolean NO_SERVICES = noService;
#ifndef NDEBUG
  if (getenv("NO_SERVICES")) NO_SERVICES = TRUE;
#endif

  if (NO_SERVICES)                    // KSKSKS
  {                                   // KSKSKS
    length = 6;                       // KSKSKS
    strcpy(name, "\\NOSER.$SYSTEM");  // KSKSKS
  }                                   // KSKSKS

  //------------------------------------------
  // Put trailing null at end of cluster name.
  //------------------------------------------
  name[length] = 0;
  //------------------------------------------------------------------
  // Allocate space for disk process name (including trailing null) in
  // specified heap.
  //------------------------------------------------------------------
  dp2Name_ = new (heap_) char[length + 1];
  strcpy(dp2Name_, name);
  NADELETEBASIC(name, heap_);
  //--------------------------------------------------------
  // Convert cluster name to cluster number.
  //
  // Note that node in NSK-lite is a cluster in NT parlance.
  //--------------------------------------------------------
  if (NO_SERVICES)  // KSKSKS
  {                 // KSKSKS
    clusterNumber_ = 3;
    nodeNumber_ = 1;  // KSKSKS
  }                   // KSKSKS

  //------------------------------------------------------------------------------------------
  // Set the given name for the partition (the ansi identifier associated with the partition)
  //------------------------------------------------------------------------------------------
  if (givenName) {
    givenName_ = new (heap_) char[strlen(givenName) + 1];
    strcpy(givenName_, givenName);
  } else {
    givenName_ = 0;
  }

}  // NodeMapEntry constructor

//<pb>
//==============================================================================
//  NodeMapEntry copy constructor.
//
// Input:
//  other  --  other NodeMapEntry from which to copy.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
NodeMapEntry::NodeMapEntry(const NodeMapEntry &other, CollHeap *heap)
    : heap_((heap == 0) ? other.heap_ : heap),
      // we prefer non-NULL CollHeap*'s
      nodeNumber_(other.nodeNumber_),
      clusterNumber_(other.clusterNumber_),
      partitionState_(other.partitionState_) {
  //---------------------------------------------
  //  See if other entry has a disk process name.
  //---------------------------------------------
  if (other.dp2Name_ != 0) {
    //------------------------------------------------------------------
    // Allocate space for disk process name (including trailing null) in
    // in other object's heap.
    //------------------------------------------------------------------
    dp2Name_ = new (heap_) char[strlen(other.dp2Name_) + 1];
    strcpy(dp2Name_, other.dp2Name_);

    if (other.partitionName_) {
      partitionName_ = new (heap_) char[strlen(other.partitionName_) + 1];
      strcpy(partitionName_, other.partitionName_);
      if (other.givenName_) {
        givenName_ = new (heap_) char[strlen(other.givenName_) + 1];
        strcpy(givenName_, other.givenName_);
      } else {
        givenName_ = 0;
      }

    } else {
      partitionName_ = givenName_ = 0;
    }
  } else {
    dp2Name_ = partitionName_ = givenName_ = 0;
  }

}  // NodeMapEntry copy constructor
//<pb>

//==============================================================================
//  Set this node map entry to have a new disk process name.
//
// Input:
//  dp2Name  --  Newly specified, null terminated disk process name.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
// The function is not called anywhere
void NodeMapEntry::setDp2Name(char *dp2Name) {
  // Delete old full name; it's no longer valid.
  NADELETEBASIC(partitionName_, heap_);
  partitionName_ = 0;
  NADELETEBASIC(givenName_, heap_);
  givenName_ = 0;

  // Delete old disk process name and allocate space for newly specified name.
  NADELETEBASIC(dp2Name_, heap_);
  dp2Name_ = new (heap_) char[strlen(dp2Name) + 1];
  strcpy(dp2Name_, dp2Name);

}  // NodeMapEntry::setDp2Name()

//<pb>
//==============================================================================
//  Overloaded assignment operator for NodeMapEntry.
//
// Input:
//  other  --  other NodeMapEntry from which to copy.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
NodeMapEntry &NodeMapEntry::operator=(const NodeMapEntry &other) {
  //--------------------------------------
  //  Nothing to do when copying yourself.
  //--------------------------------------
  if (this == &other) {
    return *this;
  }

  //------------------------------
  // Delete old disk process name.
  //------------------------------
  NADELETEBASIC(dp2Name_, heap_);
  NADELETEBASIC(partitionName_, heap_);
  NADELETEBASIC(givenName_, heap_);

  NodeMapEntry(other, heap_);

  /* //  See if other DP2 name exists.
   if (other.dp2Name_ != 0)
     {

       //-----------------------------------------
       //  Allocate space for newly specified name
       // including space for trailing null.
       //-----------------------------------------
       dp2Name_ = new (heap_) char[strlen(other.dp2Name_) + 1];
       strcpy(dp2Name_,other.dp2Name_);

       if (other.partitionName_)
         {
           partitionName_ = new (heap_) char[strlen(other.partitionName_) + 1];
           strcpy(partitionName_,other.partitionName_);
           if (other.givenName_)
           {
             givenName_ = new (heap_) char[strlen(other.givenName_) + 1];
             strcpy(givenName_,other.givenName_);
           }
           else
           {
             givenName_ = 0;
           }
         }
       else
         {
           partitionName_ = givenName_ = 0;
         }
     }
   else
     {
       dp2Name_ = partitionName_ = givenName_ = 0;
     }

   nodeNumber_     = other.nodeNumber_;
   clusterNumber_  = other.clusterNumber_;
   partitionState_ = other.partitionState_; */

  return *this;

}  // NodeMapEntry::operator=
//<pb>

//=======================================================

// to be called from the debugger
void NodeMapEntry::display() const { NodeMapEntry::print(); }

//=======================================================

HBaseNodeMapEntry::HBaseNodeMapEntry(const HBaseNodeMapEntry &other, CollHeap *heap) : NodeMapEntry(other, heap) {}

HBaseNodeMapEntry &HBaseNodeMapEntry::operator=(const HBaseNodeMapEntry &other) {
  NodeMapEntry::operator=(other);
  return *this;
}

//=======================================================

//==============================================================================
//  Print out the contents of this node map entry to a specified file.
//
// Input:
//  ofd    -- pointer to file descriptor of specified file.
//  indent -- string of spaces for indentation.
//  title  -- title associated with this node map display.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
void NodeMapEntry::print(FILE *ofd, const char *indent, const char *title) const {
  BUMP_INDENT(indent);

  fprintf(ofd, "%s %s (%-8s %s %2d, %s %2d, %s %2d)\n", NEW_INDENT, title, dp2Name_, "Node:", nodeNumber_,
          "Cluster: ", clusterNumber_, "State: ", partitionState_);

}  // NodeMapEntry::print()

//<pb>
//============================================================================
// Methods for class NodeMap; it encapsulates an ordered collection of node
// map entries.
//============================================================================

//==============================================================================
//  NodeMap constructor indicating the number of node map entries and their
// initial state (default to ACTIVE).
//
// Input:
//  heap       -- heap for newly allocated node map.
//  numEntries -- number of entries in newly allocated node map.
//  state      -- initial partition state for each node map entry.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
NodeMap::NodeMap(CollHeap *heap, const CollIndex numEntries, const NodeMapEntry::PartitionState state, NodeMapType type)
    : map_(heap, numEntries),
      numActivePartitions_(-1),
      numEstActivePartitionsAtRuntime_(-1),
      numOfDP2Volumes_(-1),
      numOfActiveDP2Volumes_(-1),
      tableIdent_(-1),
      usesLocality_(FALSE),
      heap_(heap),
      type_(type) {
  //-------------------------------------------------------
  //  Allocate all node map entries with a specified state.
  //-------------------------------------------------------
  for (CollIndex idx = 0; idx < numEntries; idx++) {
    NodeMapEntry *entryCopy;

    switch (type_) {
      case NodeMap::HBASE:
        entryCopy = new (heap) HBaseNodeMapEntry(state, heap);
        break;

      default:
        entryCopy = new (heap) NodeMapEntry(state);
    }

    map_.insertAt(idx, entryCopy);
  }

}  // NodeMap Constructor with number of entries and a specified state.

NodeMap::NodeMap(CollHeap *heap, const CollIndex numEntries, const ARRAY(short) & activeNodeIds, NodeMapType type)
    : map_(heap, numEntries),
      numActivePartitions_(-1),
      numEstActivePartitionsAtRuntime_(-1),
      numOfDP2Volumes_(-1),
      numOfActiveDP2Volumes_(-1),
      tableIdent_(-1),
      usesLocality_(FALSE),
      heap_(heap),
      type_(type) {
  NodeMapEntry::PartitionState state = NodeMapEntry::IDLE;
  for (CollIndex i = 0; i < numEntries; i++) {
    NodeMapEntry *entry = NULL;

    switch (type_) {
      case NodeMap::HBASE:
        entry = new (heap) HBaseNodeMapEntry(state, heap);
        break;

      default:
        entry = new (heap) NodeMapEntry(state);
    }

    map_.insertAt(i, entry);
  }

  for (CollIndex i = 0; i < activeNodeIds.entries(); i++) {
    short nodeId = activeNodeIds[i];
    NodeMapEntry *entry = getNodeMapEntry(nodeId);
    entry->setPartitionState(NodeMapEntry::ACTIVE);
  }
}  // NodeMap Constructor with number of entries and a nodeId array.

//<pb>
//==============================================================================
//  NodeMap copy constructor.
//
// Input:
//  other -- other NodeMap object from which to copy.
//  heap  -- heap for newly allocated node map.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
NodeMap::NodeMap(const NodeMap &other, CollHeap *heap)
    : map_(heap, other.map_.entries()),
      heap_((heap == 0) ? other.heap_ : heap),  // we prefer non-NULL CollHeap*'s
      type_(other.type_) {
  //------------------------------
  //  Deep copy of node map array.
  //------------------------------
  for (CollIndex idx = 0; idx < other.map_.entries(); idx++) {
    //----------------------------------------------------------------
    //  Delete and remove entry from this node map if it is allocated.
    //----------------------------------------------------------------
    if (map_.getUsage(idx) != UNUSED_COLL_ENTRY) {
      delete map_[idx];
      map_.remove(idx);
    }

    //------------------------------------------
    //  Copy node map entry from other node map.
    //------------------------------------------
    if (other.map_.getUsage(idx) != UNUSED_COLL_ENTRY) {
      NodeMapEntry *entryCopy = NULL;
      switch (type_) {
        case SQ:
          entryCopy = new (heap_) NodeMapEntry(*other.map_[idx], heap_);
          break;

        case HBASE:
          entryCopy = new (heap_) HBaseNodeMapEntry(*(HBaseNodeMapEntry *)other.map_[idx], heap_);
          break;

        default:
          CMPASSERT("Unknown NodeMap type");
      }

      map_.insertAt(idx, entryCopy);
    }
  }

  //------------------------------------------
  //  Now, copy rest of node map members:
  //------------------------------------------
  numActivePartitions_ = other.numActivePartitions_;
  numEstActivePartitionsAtRuntime_ = other.numEstActivePartitionsAtRuntime_;
  numOfDP2Volumes_ = other.numOfDP2Volumes_;
  numOfActiveDP2Volumes_ = other.numOfActiveDP2Volumes_;
  tableIdent_ = other.tableIdent_;
  usesLocality_ = other.usesLocality_;

}  // NodeMap copy constructor.
//<pb>
//==============================================================================
//  NodeMap destructor.
//
// Input:
//  none
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
// NodeMaps are deleted with the statement heap.
NodeMap::~NodeMap() {
  if (collHeap() && CmpCommon::getDefault(COMP_BOOL_98) == DF_OFF) return;

  if (CURRSTMT_OPTDEFAULTS->compileTimeMonitor() && QueryAnalysis::Instance())
    QueryAnalysis::Instance()->tempMonitor().enter();

  //-------------------------------
  //  Delete each used array entry.
  //-------------------------------
  for (CollIndex idx = 0; idx < map_.entries(); idx++) {
    if (map_.getUsage(idx) != UNUSED_COLL_ENTRY) {
      delete map_[idx];
    }
  }

  //----------------------
  //  Make node map empty.
  //----------------------
  map_.clear();

  if (CURRSTMT_OPTDEFAULTS->compileTimeMonitor() && QueryAnalysis::Instance())
    QueryAnalysis::Instance()->tempMonitor().exit();

}  // NodeMap destructor.
//<pb>
//==============================================================================
//  Make a copy of this NodeMap object in a specified heap.
//
// Input:
//  heap  -- heap for copy of this NodeMap object.
//
// Output:
//  none
//
// Return:
//  pointer to copy of NodeMap object.
//
//==============================================================================
NodeMap *NodeMap::copy(CollHeap *heap) const { return new (heap) NodeMap(*this, heap); }  // NodeMap::copy()

NodeMap *NodeMap::copy() const { return new (heap_) NodeMap(*this, heap_); }  // NodeMap::copy()

//==============================================================================
//  Produce a logical node map with a specified number of entries using the
// partition grouping algorithm of SQL/MX. Attempt to co-locate ESPs with their
// respective DP2 partitions.
//
// Input:
//  logicalNumEntries -- specified number of entries for logical node map.  This
//                        is equivalent to the number of ESPs.
//  forESP -- true if nodemap is for ESP, false if nodemap is for DP2
//
// Output:
//  none
//
// Return:
//  pointer to synthesized logical node map.
//
//==============================================================================
NodeMap *NodeMap::synthesizeLogicalMap(const CollIndex logicalNumEntries, NABoolean forESP) {
  // Time monitor this function
  if (CURRSTMT_OPTDEFAULTS->compileTimeMonitor() && QueryAnalysis::Instance())
    QueryAnalysis::Instance()->tempMonitor().enter();

  //---------------------------------------------------------------------
  //  Extract number of CPUs per node and number of nodes in cluster from
  // defaults table.
  //---------------------------------------------------------------------
  NADefaults &defs = ActiveSchemaDB()->getDefaults();

  CollIndex totalESPs = CURRSTMT_OPTDEFAULTS->getTotalNumberOfCPUs();

  //-----------------------------------------------------------------------
  //  Ensure that number of logical entries is positive and does not exceed
  // number of CPUs in cluster.
  //-----------------------------------------------------------------------
#ifdef _DEBUG
  if ((CmpCommon::getDefault(NSK_DBG) == DF_ON) && (CmpCommon::getDefault(NSK_DBG_GENERIC) == DF_ON)) {
    CURRCONTEXT_OPTDEBUG->stream() << "NodeMap::" << endl
                                   << "logicalNumEntries = " << logicalNumEntries << endl
                                   << "totalESPs= " << totalESPs << endl;
  }
#endif

  int requiredESPs = CURRSTMT_OPTDEFAULTS->getRequiredESPs();

  if (requiredESPs == -1 && forESP && type() == NodeMap::SQ)
    CMPASSERT(logicalNumEntries >= 1 && logicalNumEntries <= totalESPs);

  // If requiredESPs is not -1, it means we are dealing with IUDs that require
  // a particular partition number on the layer of ESPa feeding rows to
  // the IUDs on the right child of a tuple flow operator. Since this method
  // is called on behave of synthesiztion physical property for ESP operator,
  // we can not assert that logicalNumEntries == requiredESPs, because
  // they may not be the same (see SQ case 3010). We will rely on the physical
  // property satisfication verification method to verify if this ESP can
  // satisfy the physical requirement, in particular the partition count.
  // Note that it probably is a bad idea to fabricate a partition funtion by
  // using the particular partition number from the IDU node itself in the
  // RequireApproximatelyNPartitions::realize() function called above because
  // that function handles any ESPs, whether it is the one interfacing with
  // the tuple flow, or any ones located below.
  /*
       CMPASSERT(!forESP ||
                 (logicalNumEntries == 1 OR logicalNumEntries == requiredESPs));
  */

  // Reuse the nodemap if already computed for similar parallel degree
  if (logicalNumEntries > 1 AND CmpCommon::getDefault(COMP_BOOL_99) ==
      DF_OFF AND QueryAnalysis::Instance() AND QueryAnalysis::Instance()->getNodeMap(logicalNumEntries)) {
    if (CURRSTMT_OPTDEFAULTS->compileTimeMonitor()) QueryAnalysis::Instance()->tempMonitor().exit();

    return QueryAnalysis::Instance()->getNodeMap(logicalNumEntries);
  }

  //------------------------------------------------------------------
  //  Create a node map with the specified number of entries initially
  // in an NOT_ACTIVE state.
  //------------------------------------------------------------------
  NodeMap *logicalMap = new HEAP NodeMap(heap_, logicalNumEntries, NodeMapEntry::NOT_ACTIVE, type());

  // get a list of the nodes in the cluster
  const NAArray<CollIndex> &cpuArray(CURRCONTEXT_CLUSTERINFO->getCPUArray());
  int cpuCount = cpuArray.entries();
  int affinityDef = ActiveSchemaDB()->getDefaults().getAsLong(AS_AFFINITY_VALUE);

  // "float" single ESP
  if (logicalNumEntries == 1 && forESP) {
    // node is chosen at runtime
    // ESP exchange is active
    logicalMap->setPartitionState(0, NodeMapEntry::ACTIVE);
  } else if ((CmpCommon::getDefault(COMP_BOOL_82) == DF_ON &&
              (logicalNumEntries >= (CollIndex)cpuCount ||  // caller wants all cpus.
                                                            // prime the nodemap cache with an entry that has all cpus.
               logicalNumEntries > getNumEntries()))
             // more consumers than producers. So, grouping does not apply, ie,
             // no consumer-to-cpu assignment can co-locate each consumer with its
             // producer(s). Yes, a consumer probably reads from multiple producers.
             // We want to assign consumers to producers' cpus and then to other cpus.
             // But, that requires bookkeeping. For now, we simply assign consumers
             // to available segments' cpus, round-robin.
             || affinityDef == -2 ||
             // do the same if adaptive segmentation's nodemap remapping is active
             // because any ESP nodemap computed here will be tossed later when
             // the generator remaps ESP nodemaps for adaptive load balancing.
             (affinityDef >= -1 || affinityDef == -3 || affinityDef == -4)) {
    if (logicalNumEntries == 1) {
      // adaptive segmentation is active and forDP2 replicate broadcast.
      // we have to float this ESP (node chosen at runtime)
      // NB: trying to float the segment may seem like a good idea here.
      // But, don't do it. It will cause some queries (see
      // regress/opt/optdml04,05) to go serial towards the top!
    } else {
      // ESP parallelism >= 1
      CollIndex espX, clusX = 0, cpuX = 0;
      for (espX = 0; espX < logicalNumEntries; espX++) {
        // each ESP in an ESP exchange is active
        logicalMap->setPartitionState(espX, NodeMapEntry::ACTIVE);
        // assign ESP consumers to segments/cpus, round-robin
        logicalMap->setNodeNumber(espX, cpuArray[cpuX]);
        cpuX++;  // advance to next cpu
        if (cpuX >= cpuArray.entries()) {
          cpuX = 0;
        }
      }
    }
  }
  //--------------------------------------
  //  Return synthesized logical node map.
  //--------------------------------------
#ifndef NDEBUG
  if (getenv("debug_MNO")) {
    FILE *f = fopen("superNodeMap", "ac");
    this->print(f, DEFAULT_INDENT, "child");
    logicalMap->print(f, DEFAULT_INDENT, "parent");
    fclose(f);
  }
#endif

  // Save this nodemap in the global array
  if (logicalNumEntries > 1 AND QueryAnalysis::Instance()) {
    QueryAnalysis::Instance()->setNodeMap(logicalMap, logicalNumEntries);
  }

  // exit monitor
  if (CURRSTMT_OPTDEFAULTS->compileTimeMonitor() && QueryAnalysis::Instance())
    QueryAnalysis::Instance()->tempMonitor().exit();

  return logicalMap;

}  // NodeMap::synthesizeLogicalMap()

//<pb>
//==============================================================================
//  Determine how to group the entries in this node map based on the default
// value for BASE_NUM_PAS_ON_ACTIVE_PARTS.  If this default is turned off, each
// group should contain contiguous physical partitions; otherwise each group
// should contain consecutive active partitions.
//
//  NOTE: the group start and group size arrays returned by this routine must
//       be deleted by the caller.
//
//  * * * * * * * * * * * * *  I M P O R T A N T  * * * * * * * * * * * * * * *
//  *                                                                         *
//  *  THIS MEMBER FUNCTION USES THE SAME GROUPING ALGORITHM AS MEMBER        *
//  * FUNCTION RangePartitionBoundaries::scaleNumberOfPartitions().  ANY      *
//  * CHANGES TO THIS MEMBER FUNCTION WOULD NECESSITATE CHANGES TO MEMBER     *
//  * FUNCTION RangePartitionBoundaries::scaleNumberOfPartitions() AS WELL.   *
//  *                                                                         *
//  * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
// Input:
//  numGroups       -- desired number of groups.
//
// Output:
//  groupStart      -- array of starting locations for each group.
//  groupSize       -- array of sizes for each group.
//
// Return:
//  none
//
//==============================================================================
// the method is not used under SQ, except in OSM mode simulating NSK
void NodeMap::deriveGrouping(const CollIndex numGroups, CollIndexPointer &groupStart, CollIndexPointer &groupSize) {
  //--------------------------------------------------------------------------
  //  Allocate group start array and group size array with specified number of
  // groupings.
  //--------------------------------------------------------------------------
  groupStart = new HEAP CollIndex[numGroups];
  groupSize = new HEAP CollIndex[numGroups];

  //-------------------------------------------------------------------------
  //  Size of smallest group and number of remaining partitions if each group
  // had this size.
  //-------------------------------------------------------------------------
  CollIndex size;
  CollIndex remainder;

  //----------------------------------
  //  Index to current node map entry.
  //----------------------------------
  CollIndex mapIdx = 0;

  if (CmpCommon::getDefault(BASE_NUM_PAS_ON_ACTIVE_PARTS) == DF_OFF) {
    //---------------------------------------------------------------------
    // In this case, group size is based on allocating contiguous
    // physical partitions among the groups.  Handle both cases: (1)
    // the number of physical partitions exceeds or equals the
    // number of desired groups. (2) the number of physical
    // partitions is less than the number of desired groups.
    //---------------------------------------------------------------------

    CollIndex numParts = getNumEntries();

    if (numGroups <= numParts) {
      // (1) the number of physical partitions exceeds or equals the
      // number of desired groups.

      size = numParts / numGroups;
      remainder = numParts % numGroups;

      for (CollIndex groupIdx = 0; groupIdx < numGroups; groupIdx++) {
        //-----------------------------------------
        //  Group starts at current node map entry.
        //-----------------------------------------
        groupStart[groupIdx] = mapIdx;

        //---------------------------------------------------------
        //  Determine size of current group.
        //
        //  Note that by convention groups numbered from 0 to
        // (logicalNumEntries - remainder -1) contain size entries.
        // The groups numbered from (logicalNumEntries - remainder)
        // to (logicalNumEntries - 1) contain (size + 1) entries.
        //---------------------------------------------------------
        if (groupIdx < numGroups - remainder) {
          groupSize[groupIdx] = size;
        } else {
          groupSize[groupIdx] = size + 1;
        }

        //------------------------------------
        //  Set node map index for next group.
        //------------------------------------
        mapIdx = groupStart[groupIdx] + groupSize[groupIdx];
      }

    } else {
      // (2) the number of physical partitions is less than the
      // number of desired groups.

      size = numGroups / numParts;
      remainder = numGroups % numParts;
      CollIndex transPoint = numParts - remainder;

      CollIndex groupIdx = 0;
      for (mapIdx = 0; mapIdx < numParts; mapIdx++) {
        for (CollIndex i = 0; i < size; i++) {
          groupStart[groupIdx] = mapIdx;
          groupSize[groupIdx] = 1;
          groupIdx++;
        }

        if (mapIdx >= transPoint) {
          groupStart[groupIdx] = mapIdx;
          groupSize[groupIdx] = 1;
          groupIdx++;
        }
      }
      CMPASSERT(groupIdx == numGroups);
    }
    return;

  } else {
    // (getDefault(BASE_NUM_PAS_ON_ACTIVE_PARTS) != DF_OFF)

    //---------------------------------------------------------------------
    // In this case, group size is based on allocating active partitions
    // among the groups.  Handle both cases: (1) the number of active
    // partitions exceeds or equals the number of desired groups. (2)
    // the number of active partitions is less than the number of
    // desired groups.
    //---------------------------------------------------------------------

    CollIndex numParts = getNumActivePartitions();

    CMPASSERT(numParts);

    if (numGroups <= numParts) {
      // (1) the number of active partitions exceeds or equals the
      // number of desired groups.

      size = numParts / numGroups;
      remainder = numParts % numGroups;

      for (CollIndex groupIdx = 0; groupIdx < numGroups; groupIdx++) {
        //-----------------------------------------
        //  Group starts at current node map entry.
        //-----------------------------------------
        groupStart[groupIdx] = mapIdx;

        //---------------------------------------------------------
        //  Determine size of current group.
        //
        //  Note that by convention groups numbered from 0 to
        // (logicalNumEntries - remainder -1) contain size entries.
        // The groups numbered from (logicalNumEntries - remainder)
        // to (logicalNumEntries - 1) contain (size + 1) entries.
        //---------------------------------------------------------
        CollIndex currentGroupSize;
        if (groupIdx < numGroups - remainder) {
          currentGroupSize = size;
        } else {
          currentGroupSize = size + 1;
        }

        //--------------------------------------------------------------------
        //  Continue looping until current group has required number of active
        // partitions.
        //--------------------------------------------------------------------
        CollIndex numActive = 0;
        while (numActive < currentGroupSize) {
          if (isActive(mapIdx)) {
            numActive++;
          }

          mapIdx++;
        }

        //-----------------------------------------------------------------------
        //  Set actual group size to include both active and inactive partitions.
        //-----------------------------------------------------------------------
        groupSize[groupIdx] = mapIdx - groupStart[groupIdx];
      }
    } else {
      // (2) the number of active partitions is less than the number of
      // desired groups.

      size = numGroups / numParts;
      remainder = numGroups % numParts;
      CollIndex transPoint = numParts - remainder;

      mapIdx = 0;
      CollIndex groupIdx = 0;
      for (CollIndex partIdx = 0; partIdx < numParts; groupIdx++) {
        // Find next active mapIdx
        //
        while (!isActive(mapIdx)) {
          mapIdx++;
        }

        // Assign this active partition to multiple groups.
        //
        for (CollIndex i = 0; i < size; i++) {
          groupStart[groupIdx] = mapIdx;

          // Each group is associated with only one active partition.
          //
          groupSize[groupIdx] = 1;
          groupIdx++;
        }

        // Trailing active partitions are assigned to an extra group.
        //
        if (partIdx >= transPoint) {
          groupStart[groupIdx] = mapIdx;
          groupSize[groupIdx] = 1;
          groupIdx++;
        }

        mapIdx++;
      }
      CMPASSERT(groupIdx == numGroups);
    }
  }
}  // NodeMap::deriveGrouping()
//<pb>
//==============================================================================
//  Determine if node map has a node specification for all entries.
//
// Input:
//  none
//
// Output:
//  none
//
// Return:
//  TRUE if all node map entries have a node specification; FALSE otherwise.
//
//==============================================================================
NABoolean NodeMap::allNodesSpecified(void) const {
  for (CollIndex mapIdx = 0; mapIdx < getNumEntries(); mapIdx++) {
    if (getNodeNumber(mapIdx) == IPC_CPU_DONT_CARE) {
      return FALSE;
    }
  }

  return TRUE;
}

NABoolean NodeMap::allNodesAreWildcards() const {
  for (CollIndex mapIdx = 0; mapIdx < getNumEntries(); mapIdx++) {
    if (map_[mapIdx]->getDP2Name() != 0) {
      return FALSE;
    }
  }

  return TRUE;
}

//<pb>
//==============================================================================
//  Determine if node map entry at a specified position within the node map is
// active.
//
// Input:
//  position  --  Specified position of desired node map entry.
//
// Output:
//  none
//
// Return:
//  TRUE if entry is active; false otherwise.
//
//==============================================================================
NABoolean NodeMap::isActive(const CollIndex position) const {
  CMPASSERT(map_.getUsage(position) != UNUSED_COLL_ENTRY);
  return map_[position]->isPartitionActive();

}  // NodeMap::isActive()

int NodeMap::getPopularNodeNumber(CollIndex beginPos, CollIndex endPos) const {
  int numNodes = CmpCommon::context()->getNumOfSMPs();
  // an array of nodes in the cluster
  long *nodes = new (CmpCommon::statementHeap()) long[numNodes];
  for (int i = 0; i < numNodes; i++) nodes[i] = 0;  // init the array with 0

  for (int index = beginPos; index < endPos; index++) {
    CMPASSERT(map_.getUsage(index) != UNUSED_COLL_ENTRY);
    int currNodeNum = getNodeNumber(index);

    if (currNodeNum != IPC_CPU_DONT_CARE) nodes[currNodeNum] += 1;  // keep count regions node number
  }

  int nodeFrequency = 0;
  int popularNodeNumber = -1;  // first node number is popular to start with
  // introduce a pseudo-random offset to avoid a bias
  // towards particular nodes
  int offset = ExHDPHash::hash((char *)&beginPos, 0, sizeof(beginPos)) % numNodes;
  for (int index = 0; index < numNodes; index++) {
    int offsetIndex = (index + offset) % numNodes;
    if (nodes[offsetIndex] > nodeFrequency) {
      nodeFrequency = nodes[offsetIndex];
      popularNodeNumber = offsetIndex;
    }
  }
  NADELETEBASIC(nodes, CmpCommon::statementHeap());
  return popularNodeNumber;
}  // NodeMap::getNodeNum

// Smooth the node map to reassign entries with identical node Id of higher
// frequency than the rest to some other nodes. Assume there are m total entries in the map, and
// n nodes in the cluster. Assume further there are s entries in the map (out of m) that refer
// to very few nodes.  These s entrres are the subject of smoothing operation.  We do so by
// 1. allocate an array nodeUsageMap[] and the ith entry in it contains all indexes k in the map
//    that points at i (i.e., nodeMap.getNodeId[k] = i)
// 2. Find out s by frequency counting
// 3. Find out m - s
// 4. Find out how many nodes in each of the s entries that should be moved
//
// The nodels accepting the reasssignment will be from the set of the nodes contained in the map.
//
// Example 1.  Assume the original node map with 6 entries as follows:
//
//    NodeMapIndex   0   1   2   3  4  5
//    NodeMapEntry   0   1   1   3  1  5
//
// The nodes accepting the reasssignment will be { 0, 3, 5 }
//
// The subset of entries referring to a few nodes with high frequency: s = { 1, 2, 4 }
// Since f(0)=f(3)=f(5)=1, the average frequency of nodes not in s: (1+1+1)/3 = 1.
// We will allow one (1) assigment in s to remain in node 1 since it is the average frequency, and
// re-assign the rest (marked X) to different nodes via round-robin starting the 1st node in the map.
// The node (1) is excluded from the re-assignment.
//
//    NodeMapIndex   0   1   2   3  4  5
//    NodeMapEntry   0   1   1   3  1  5
//                           X      X
//                           ^      ^
//                           |      |
//                          to 0   to 3
//
// The final smoothed node map:
//
//    NodeMapIndex   0   1   2   3  4  5
//    NodeMapEntry   0   1   0   3  3  5
//
NABoolean NodeMap::smooth(int numNodes) {
  NABoolean smoothed = FALSE;

  typedef ClusteredBitmap *ClusteredBitmapPtr;

  ClusteredBitmap **nodeUsageMap = new (heap_) ClusteredBitmapPtr[numNodes];

  for (int index = 0; index < numNodes; index++) {
    nodeUsageMap[index] = NULL;
  }

  ClusteredBitmap includedNodes(heap_);

  int highestFreq = 0;
  for (int index = 0; index < getNumEntries(); index++) {
    int currNodeNum = getNodeNumber(index);

    if (currNodeNum != IPC_CPU_DONT_CARE) {
      if (nodeUsageMap[currNodeNum] == NULL) nodeUsageMap[currNodeNum] = new (heap_) ClusteredBitmap(heap_);

      nodeUsageMap[currNodeNum]->insert(index);
      includedNodes.insert(currNodeNum);

      int entries = nodeUsageMap[currNodeNum]->entries();
      if (highestFreq < entries) {
        highestFreq = entries;
      }
    }
  }

  // Find how many entries wth the highest frequency, and compute the number of entries with
  // normal frequency (normEntries) and the number of nodes that appear with normal frequency
  // (normalNodesCt).
  int count = 0;
  int normalEntries = 0;
  int normalNodesCt = 0;
  for (int index = 0; index < numNodes; index++) {
    if (!nodeUsageMap[index]) continue;

    if (nodeUsageMap[index]->entries() == highestFreq) {
      count++;
      includedNodes.subtractElement(index);
    } else {
      normalEntries++;
      normalNodesCt += nodeUsageMap[index]->entries();
    }
  }

  if (normalEntries >= 1 && count <= 2 && count < getNumEntries() && count * highestFreq < floor(numNodes * 0.67)) {
    int baseFreq = ceil(normalNodesCt / normalEntries);
    CollIndex availableNode = 0;

    for (int index = 0; index < numNodes; index++) {
      if (nodeUsageMap[index] && nodeUsageMap[index]->entries() == highestFreq) {
        // skip first baseFreq entries and reassign the rest starting at the (baseFreq+1)th entry
        int notTouched = 0;
        NABoolean canAssign = FALSE;
        for (CollIndex j = 0; nodeUsageMap[index]->nextUsed(j); j++) {
          if (canAssign) {
            // round-robin to the next available node. If we exhause all the available
            // nodes, go back to the start
            if (!includedNodes.nextUsed(availableNode)) {
              availableNode = 0;
              includedNodes.nextUsed(availableNode);
            }

            // availableNode++ is part ofhte round-robin scheme, required to
            // iterate over a ClusteredBitmap.
            setNodeNumber(j, availableNode++);

            smoothed = TRUE;
          } else {
            notTouched++;
            if (notTouched >= baseFreq) canAssign = TRUE;
          }
        }
      }
    }
  }

  NADELETEARRAY(nodeUsageMap, numNodes, ClusteredBitmapPtr, heap_);

  return smoothed;
}

//<pb>
//==============================================================================
//  Return node number of node map entry at a specified position within the node
// map.
//
// Input:
//  position  --  Specified position of desired node map entry.
//
// Output:
//  none
//
// Return:
//  Node number of node map entry at a specified position.
//
//==============================================================================
int NodeMap::getNodeNumber(const CollIndex position) const {
  CMPASSERT(map_.getUsage(position) != UNUSED_COLL_ENTRY);
  return map_[position]->getNodeNumber();

}  // NodeMap::getNodeNum

int NodeMap::mapNodeNameToNodeNum(const NAString node) const {
  return CURRCONTEXT_CLUSTERINFO->mapNodeNameToNodeNum(node);
}  // NodeMap::getNodeNmber

//==============================================================================
//  Return cluster number of node map entry at a specified position within the node
// map.
//
// Input:
//  position  --  Specified position of desired node map entry.
//
// Output:
//  none
//
// Return:
//  cluster number of node map entry at a specified position.
//
//==============================================================================
int NodeMap::getClusterNumber(const CollIndex position) const {
  CMPASSERT(map_.getUsage(position) != UNUSED_COLL_ENTRY);
  return map_[position]->getClusterNumber();
}
//<pb>
//==============================================================================
//  Return number of active partitions for this node map.  If it has already
// been calculated, use the cached value; otherwise calculate it from scratch.
//
// Input:
//  none
//
// Output:
//  none
//
// Return:
//  Number of active partitions for this node map.
//
//==============================================================================
CollIndex NodeMap::getNumActivePartitions() {
  //--------------------------------------------------------
  //  This cast is safe because we take care of the negative
  // value case below.
  //--------------------------------------------------------
  CollIndex actPart = CollIndex(numActivePartitions_);

  if (numActivePartitions_ < 0) {
    //--------------------------------------------------------
    //  First time we call this function, so compute number of
    // active partitions.
    //--------------------------------------------------------
    actPart = 0;
    for (CollIndex i = 0; i < getNumEntries(); i++) {
      if (map_[i]->isPartitionActive()) actPart++;
    }
    numActivePartitions_ = actPart;
  }

  //--------------------------------------------------------------
  // actPart is an estimate.  Make sure it is always at least one.
  //--------------------------------------------------------------
  actPart = (actPart == 0 ? 1 : actPart);

  return actPart;

}  // NodeMap::getNumActivePartitions()

//==============================================================================
// Return the estimated number of active partitions at Runtime for this node map.
// This variable is set to the maximum number of partitions that can be active,
// if a query has an equality predicate with a host/parameter variable
// on leading partition column.
// If this is not set, return the active partition determined by
// getNumActivePartition() function.
//
// Input:
//  none
//
// Output:
//  none
//
// Return:
//  Number of effective active partitions for this node map.
//
//==============================================================================

CollIndex NodeMap::getEstNumActivePartitionsAtRuntime() {
  if (numEstActivePartitionsAtRuntime_ < 0)
    return getNumActivePartitions();
  else
    return numEstActivePartitionsAtRuntime_;
}

//<pb>
//==============================================================================
//  Return number of DP2 volumes for this node map.  If it has already
// been calculated, use the cached value; otherwise calculate it from scratch.
//
// Input:
//  none
//
// Output:
//  none
//
// Return:
//  Number of DP2 volumes for this node map.
//
//==============================================================================
CollIndex NodeMap::getNumOfDP2Volumes() {
  //-------------------------------------------------------------
  //  Return immediately if we already have a valid cached value.
  //-------------------------------------------------------------
  if (numOfDP2Volumes_ >= 0) {
    return numOfDP2Volumes_;
  }

  //--------------------------------------------------------------
  //  Set the number of volumes to the number of partitions in the
  // table.  Need this to fake costing in TPCD SF 50.
  //--------------------------------------------------------------
  if (CmpCommon::getDefault(FAKE_VOLUME_ASSIGNMENTS) == DF_ON) {
    numOfDP2Volumes_ = MINOF(getNumEntries(), CmpCommon::getDefaultNumeric(FAKE_VOLUME_NUM_VOLUMES));
    return numOfDP2Volumes_;
  }

  //********************************************************
  //  Actual computation for number of distinct DP2 volumes.
  //********************************************************

  //-----------------------------------------------------------
  //  Use container to keep track of distinct DP2 volume names.
  // Initially we have no volumes.
  //-----------------------------------------------------------
  DP2VolumeNamesContainer container(CmpCommon::statementHeap());
  numOfDP2Volumes_ = 0;
  for (CollIndex nodeIdx = 0; nodeIdx < getNumEntries(); nodeIdx++) {
    //--------------------------------------------------------------
    //  Extract DP2 volume name from current entry.  If name is not
    // null and if it hasn't been seen in an earlier node map entry,
    // we have encountered a new DP2 volume.
    //--------------------------------------------------------------
    const char *volumeName = map_[nodeIdx]->getDP2Name();
    if (volumeName != 0 && NOT volumeFound(container, *volumeName)) {
      numOfDP2Volumes_++;
      container.insert(volumeName);
    }
  }

  return numOfDP2Volumes_;

}  // NodeMap::getNumOfDP2Volumes()

// COLOCATION CHECKING: test whether the locations of partitions match
NABoolean NodeMap::isCoLocated(const NodeMap *rMap) const {
  const NodeMapEntry *n_entry = NULL;
  const NodeMapEntry *r_entry = NULL;
  int numEntries;

  if ((numEntries = getNumEntries()) != rMap->getNumEntries()) return FALSE;

  for (CollIndex i = 0; i < numEntries; i++) {
    n_entry = getNodeMapEntry(i);
    r_entry = rMap->getNodeMapEntry(i);

    const char *n_DP2Name = n_entry->getDP2Name();
    const char *r_DP2Name = r_entry->getDP2Name();

    // if left name is empty, return FALSE. This is because
    // the left name normally is associated with the requirement which
    // is generated from the left-most leaf node. Here we just say if
    // the left-most leaf node does not have a node map, we can not
    // allow the colocation test to pass (i.e., the CS in question can
    // not be pushed).
    if (n_DP2Name == 0) return FALSE;

    // However, if the right name is empty, we simple declare the
    // colocation checking is OK.
    if (r_DP2Name == 0) return TRUE;

    if (_stricmp(r_DP2Name, n_DP2Name) != 0) {
      return FALSE;
    }
  }
  return TRUE;
}

//<pb>
//==============================================================================
//  Return number of distinct DP2 volume names associated with active
// partitions.
//
// Input:
//  none
//
// Output:
//  none
//
// Return:
//  Number of distinct active DP2 volume names associated with active
//   partitions.
//
//==============================================================================
CollIndex NodeMap::getNumActiveDP2Volumes() {
  //-------------------------------------------------------------
  //  Return immediately if we already have a valid cached value.
  //-------------------------------------------------------------
  if (numOfActiveDP2Volumes_ >= 0) {
    return numOfActiveDP2Volumes_;
  }

  //---------------------------------------------------
  //  Directive to associate each partition with a
  // separate volume despite reality, so set number of
  // active DP2 volumes to number of active partitions.
  //---------------------------------------------------
  if (CmpCommon::getDefault(FAKE_VOLUME_ASSIGNMENTS) == DF_ON) {
    numOfActiveDP2Volumes_ = MINOF(getNumActivePartitions(), CmpCommon::getDefaultNumeric(FAKE_VOLUME_NUM_VOLUMES));
    return numOfActiveDP2Volumes_;
  }

  //**************************************************
  //  Actual computation for number of active volumes.
  //**************************************************

  //-----------------------------------------------------------
  //  Use container to keep track of distinct DP2 volume names.
  // Initially we have no active volumes.
  //-----------------------------------------------------------
  DP2VolumeNamesContainer container(CmpCommon::statementHeap());
  numOfActiveDP2Volumes_ = 0;

  //---------------------------------
  //  Loop over all node map entries.
  //---------------------------------
  for (CollIndex nodeIdx = 0; nodeIdx < getNumEntries(); nodeIdx++) {
    //------------------------------------------------
    //  Determine if current node map entry is active.
    //------------------------------------------------
    if (isActive(nodeIdx)) {
      //--------------------------------------------------------------
      //  Extract DP2 volume name from current entry.  If name is not
      // null and if it hasn't been seen in an earlier node map entry,
      // we have a new active DP2 volume.
      //--------------------------------------------------------------
      const char *volumeName = map_[nodeIdx]->getDP2Name();
      if (volumeName != 0 && NOT volumeFound(container, *volumeName)) {
        numOfActiveDP2Volumes_++;
        container.insert(volumeName);
      }
    }
  }

  return numOfActiveDP2Volumes_;

}  // NodeMap::getNumActiveDP2Volumes()
//<pb>
//==============================================================================
//  Determine if a contiguous set of entries includes more than one cluster,
//  optionally disregarding any inactive entries.
//
// Input:
//  start - first entry to consider
//  end   - first entry beyond the last entry to consider
//  activeOnly - indicate whether to exclude inactive entries
//
// Output:
//  none
//
// Return:
//  TRUE is more than one cluster is in set, FALSE otherwise.
//
//==============================================================================
NABoolean NodeMap::isMultiCluster(CollIndex start, CollIndex end, NABoolean activeOnly) const {
  const int ClusterNotSet = -2;
  int clusterOfGroup = ClusterNotSet;
  for (CollIndex ix = start; ix < end; ix++) {
    if (activeOnly) {
      NodeMapEntry::PartitionState partState = getNodeMapEntry(ix)->getPartitionState();
      if ((partState != NodeMapEntry::ACTIVE) && (partState != NodeMapEntry::ACTIVE_NO_DATA))
        continue;  // don't care about inactive partition's cluster.
    }

    if (clusterOfGroup == ClusterNotSet)
      clusterOfGroup = getNodeMapEntry(ix)->getClusterNumber();
    else if (clusterOfGroup != getNodeMapEntry(ix)->getClusterNumber())
      return TRUE;
  }
  return FALSE;
}
//<pb>
//==============================================================================
//  Insert a node map entry at a specified position.
//
// Input:
//  position  --  Specified position of desired node map entry.
//  entry     --  Supplied node map entry from which to create a new entry.
//  heap      --  Heap in which to create new entry.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
void NodeMap::setNodeMapEntry(const CollIndex position, const NodeMapEntry &entry, CollHeap *heap) {
  //---------------------------------------------
  // If another node map entry exists, delete it.
  //---------------------------------------------
  if (map_.getUsage(position) != UNUSED_COLL_ENTRY) {
    delete map_[position];
  }

  map_.insertAt(position, new (heap) NodeMapEntry(entry, heap));

  resetCachedValues();

}  // NodeMap::setNodeMapEntry()
//<pb>
//==============================================================================
// This method asserts if ANY of the full partition-name entries is NULL
// (e.g., after a NodeMapEntry:: copy ctor, assignment op, or setDp2Name() ...).
//
// Either you must not attempt this lookup after one of those is done, or
// you need to maintain the partitionName member in those methods.
//==============================================================================
NABoolean NodeMap::containsPartition(const char *fullName) const {
  if (fullName && fullName[0])  // not NULL or EMPTY
    for (CollIndex idx = 0; idx < map_.entries(); idx++) {
      const char *nodeMapEntryPartName = map_[idx]->getPartitionName();
      CMPASSERT(nodeMapEntryPartName);
      if (strcmp(nodeMapEntryPartName, fullName) == 0) return TRUE;  // found it
    }
  return FALSE;  // didn't find it
}  // NodeMap::containsPartition()
//<pb>
//==============================================================================
//  Set partition state for a node map entry at a specified position within the
// node map.  Update the active partition count to reflect this new state.
//
// Input:
//  position  --  Specified position of desired node map entry.
//  newState  --  New state to store in specified node map entry.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
void NodeMap::setPartitionState(const CollIndex &position, const NodeMapEntry::PartitionState &newState) {
  //---------------------------------------------------------------
  //  Verify that specified position refers to an existing node map
  // entry.
  //---------------------------------------------------------------
  CMPASSERT(position >= 0 && position < map_.entries() && map_.getUsage(position) != UNUSED_COLL_ENTRY);

  //---------------------------------------------
  //  New state equals old state.  Nothing to do.
  //---------------------------------------------
  if (newState == map_[position]->getPartitionState()) {
    return;
  }

  //-----------------------------------------------------------------------
  //  Set new state and reset cached values since they are no longer valid.
  //-----------------------------------------------------------------------
  map_[position]->setPartitionState(newState);
  resetCachedValues();

}  // NodeMap::setPartitionState()
//<pb>
//==============================================================================
//  Change node number of a node map entry at a specified position within the
// node map.
//
// Input:
//  position   --  Specified position of desired node map entry.
//  nodeNumber --  New node number for specified entry.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
void NodeMap::setNodeNumber(const CollIndex position, const int nodeNumber) {
  //---------------------------------------------------------------
  //  Verify that specified position refers to an existing node map
  // entry.
  //---------------------------------------------------------------
  CMPASSERT(position >= 0 && position < map_.entries() && map_.getUsage(position) != UNUSED_COLL_ENTRY);

  map_[position]->setNodeNumber(nodeNumber);

}  // NodeMap::setNodeNumber()

//==============================================================================
//  Change cluster number of a node map entry at a specified position within the
// node map.
//
// Input:
//  position   --  Specified position of desired node map entry.
//  nodeNumber --  New cluster number for specified entry.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
void NodeMap::setClusterNumber(const CollIndex position, const int clusterNumber) {
  //---------------------------------------------------------------
  //  Verify that specified position refers to an existing node map
  // entry.
  //---------------------------------------------------------------
  CMPASSERT(position >= 0 && position < map_.entries() && map_.getUsage(position) != UNUSED_COLL_ENTRY);

  map_[position]->setClusterNumber(clusterNumber);
}
//<pb>

//<pb>
//==============================================================================
//  Generate an ExEspNodeMap object from the node map in a partitioning
//  function (NOTE: this is a static method, it doesn't access "this")
//
// Input:
//  partFunc -- Partitioning function whose node map is to be generated
//  numEsps -- checking only, to verify we agree on the number of ESPs
//  generator -- to get space and set the generated object
//
// Output:
//  none
//
// Return:
//  != 0 if an error occurred
//
//==============================================================================
short NodeMap::codeGen(const PartitioningFunction *partFunc, const int numESPs, Generator *generator) {
  int rc = 0;
  if (partFunc == NULL) {
    generator->setGenObj(NULL, NULL);
    return 0;
  }

  Space *space = generator->getSpace();
  const NodeMap *compNodeMap = partFunc->getNodeMap();
  ExEspNodeMap *exeNodeMap = new (space) ExEspNodeMap;
  ExEspNodeMapEntry *mapEntries = new (space) ExEspNodeMapEntry[numESPs];

  assert(numESPs == compNodeMap->getNumEntries());

  char *clusterName = new (space) char[nodeNameLen];
  strcpy(clusterName, "NODE");

  exeNodeMap->setMapArray(numESPs, mapEntries);
  MS_Mon_Node_Info_Type nodeInfo;
  for (int i = 0; i < numESPs; i++) {
    const NodeMapEntry *ne = compNodeMap->getNodeMapEntry(i);

    // For a virtual node setup that is only good for compilation,
    // we have to use actual node id of 0 as the node id.
    int nodeId = (CURRCONTEXT_CLUSTERINFO->hasVirtualNodes()) ? 0 : ne->getNodeNumber();

    rc = msg_mon_get_node_info_detail(nodeId, &nodeInfo);

    strcpy(clusterName, nodeInfo.node[0].node_name);
    exeNodeMap->setEntry(i, clusterName, nodeId, ne->isPartitionActive(), space);
  }

  generator->setGenObj(NULL, (ComTdb *)exeNodeMap);
  return 0;
}

// Does this node map have any remote partitions
// If all nodes are local, then return FALSE,
// otherwise return TRUE.
//
NABoolean NodeMap::hasRemotePartitions() const {
  short sysNum;
  try {
    sysNum = OSIM_MYSYSTEMNUMBER();
  } catch (OsimLogException &e) {
    OSIM_errorMessage(e.getErrMessage());
    return FALSE;
  }

  for (int i = 0; i < getNumEntries(); i++) {
    const NodeMapEntry *ne = getNodeMapEntry(i);

    // If the system number is different from the local system number,
    // return TRUE (it has remote partitions.).
    // -1 is synonomous with the local node. (NODENUMBER_TO_NODENAME()
    // will map -1 to the name of the local system).
    //
    if (sysNum != ne->getClusterNumber() && ne->getClusterNumber() != -1) {
      return TRUE;
    }
  }
  return FALSE;
}

void NodeMap::printToLog(const char *indent, const char *title) const {
  NAString logFile = ActiveSchemaDB()->getDefaults().getValue(HIVE_HDFS_STATS_LOG_FILE);
  FILE *ofd = NULL;

  if (logFile.length()) {
    ofd = fopen(logFile, "a");
    if (ofd) {
      print(ofd, indent, title);
      fclose(ofd);
    }
  }
}

NABoolean NodeMap::printMsgToLog(const char *indent, const char *msg) const {
  NAString logFile = ActiveSchemaDB()->getDefaults().getValue(HIVE_HDFS_STATS_LOG_FILE);
  FILE *ofd = NULL;

  if (logFile.length()) {
    ofd = fopen(logFile, "a");
    if (ofd) {
      fprintf(ofd, "%s\n", msg);
      fclose(ofd);
      return TRUE;
    }
  }
  return FALSE;
}

// to be called from the debugger
void NodeMap::display() const { NodeMap::print(); }

//==============================================================================
//  Print out the contents of this node map to a specified file.
//
// Input:
//  ofd    -- pointer to file descriptor of specified file.
//  indent -- string of spaces for indentation.
//  title  -- title associated with this node map display.
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
void NodeMap::print(FILE *ofd, const char *indent, const char *title) const {
  BUMP_INDENT(indent);
  char btitle[50];
  char *S = btitle;

  //-------------------------------------------
  //  Print out info about node map as a whole.
  //-------------------------------------------
  fprintf(ofd, "%s %s\n", NEW_INDENT, title);
  fprintf(ofd, "%s %s %3d\n", NEW_INDENT, "Number of NodeMap entries:  ", map_.entries());
  fprintf(ofd, "%s %s %3d\n", NEW_INDENT, "Number of Active Partitions:", numActivePartitions_);
  fprintf(ofd, "%s %s %3d\n", NEW_INDENT,
          "Number of Est Active Partitions At Runtime:", numEstActivePartitionsAtRuntime_);
  fprintf(ofd, "%s %s %3d\n", NEW_INDENT, "Number of DP2 Volumes:      ", numOfDP2Volumes_);

  for (int nodeIdx = 0; nodeIdx < map_.entries(); nodeIdx++) {
    const NodeMapEntry *entry = map_[nodeIdx];

    if (!entry) fprintf(ofd, "%s %s is empty!!!  This is a bug!!!\n", "    ", S);

    {
      sprintf(S, "NodeMapEntry[%3d] :", nodeIdx);
      entry->print(ofd, "    ", S);
    }
  }

  fprintf(ofd, "*********************************************************************\n");

}  // NodeMap::print()

//=======================================================
// Generate a string representation of this NodeMap.
// Result is a comma-separated list of ranges.
// Each range has one of the following three forms:
// a) A single node number:   <num>
// b) Repeated numbers:       <num>(<n> times)
// c) A contiguous range:     <num>-<num>
// d) A range of 3 or more
//    evenly spaced numbers:  <num>+<num>+...+<num>
//
// Called by RelExpr::addExplainInfo()
// Used by the explain function.
//=======================================================
const NAString NodeMap::getText() const {
  int lastNodeNumber = -1;
  int stride = -1;
  int numNodesInRange = 0;
  int numRanges = 0;
  char buffer[100];

  NAString result = "(";

  NAString partitionActiveStr;
  NABoolean inactivePartitionSeen = FALSE;

  for (int nodeIdx = 0; nodeIdx < map_.entries(); nodeIdx++) {
    const NodeMapEntry *entry = map_[nodeIdx];

    int nn = entry->getNodeNumber();
    NABoolean isLast = (nodeIdx == map_.entries() - 1);
    int lastIxInRange = -1;
    int lastInRange = -1;

    // the second number in a range determines the "stride",
    // the (positive) distance between node numbers
    if (numNodesInRange == 1 && nn >= lastNodeNumber) stride = nn - lastNodeNumber;

    if (numNodesInRange > 0 && stride >= 0 && lastNodeNumber + stride == nn) {
      // this could be the middle or the end of range,
      // don't print anything until we know
      numNodesInRange++;
      if (isLast) {
        lastIxInRange = nodeIdx;
        lastInRange = nn;
      }
    } else {
      lastIxInRange = nodeIdx - 1;
      lastInRange = lastNodeNumber;
    }

    if (lastIxInRange >= 0) {
      // end of a range

      // at this point we have the following:
      // lastNodeNumber: node number of entry nodeIdx-1, if nodeIdx > 0
      // nn:             node number of entry nodeIdx
      // lastInRange:    lastNodeNumber or nn

      if (numNodesInRange == 1) {
        // form a), a single node number, already printed as the
        // begin of the range
        buffer[0] = 0;
      } else if (numNodesInRange == 2) {
        // two single numbers, first already printed as begin of
        // range, now add a second form a) for the end
        snprintf(buffer, sizeof(buffer), ",%d", lastInRange);
        numRanges++;
      } else if (stride == 0) {
        // end of a range of form b)
        snprintf(buffer, sizeof(buffer), "(%d times)", numNodesInRange);
      } else if (stride == 1) {
        // end of a range of form c)
        snprintf(buffer, sizeof(buffer), "-%d", lastInRange);
      } else {
        // end of a range of form d)
        snprintf(buffer, sizeof(buffer), "+%d+%s%d",
                 // the second number in the range
                 map_[lastIxInRange - numNodesInRange + 2]->getNodeNumber(),
                 // print the "...+" only for 4 or more numbers
                 (numNodesInRange == 3 ? "" : "...+"), lastInRange);
      }
      result += buffer;
      numNodesInRange = 0;
      numRanges++;
    }

    if (numNodesInRange == 0 && lastIxInRange != nodeIdx) {
      // print the first node number nn in a range, this is the same
      // for all thre forms a) b) and c)
      snprintf(buffer, sizeof(buffer), "%s%d",
               // print a comma-separator for ranges after the first
               (numRanges == 0 ? "" : ","),
               // print the first node number in the range
               nn);
      result += buffer;
      numNodesInRange++;
      lastIxInRange = -1;
      lastInRange = -1;
      stride = -1;
    }

    lastNodeNumber = nn;

    if (entry->isPartitionActive() == FALSE) {
      partitionActiveStr.append('0');
      inactivePartitionSeen = TRUE;
    } else
      partitionActiveStr.append('1');

  }  // loop over node map entries

  result += ")";

  if (inactivePartitionSeen) {
    result += " [activity vector=";
    result += partitionActiveStr;
    result += "]";
  }

  return result;
}

//<pb>
//==============================================================================
//  Reset this node map's cached values indicating they need to be recalculated.
//
// Input:
//  none
//
// Output:
//  none
//
// Return:
//  none
//
//==============================================================================
void NodeMap::resetCachedValues() {
  numActivePartitions_ = -1;
  numEstActivePartitionsAtRuntime_ = -1;
  numOfDP2Volumes_ = -1;
  numOfActiveDP2Volumes_ = -1;
}

int NodeMap::getNumberOfUniqueNodes() const {
  int maxNodeNum = 0;
  for (int i = 0; i < getNumEntries(); i++) {
    int nodeNum = getNodeMapEntry(i)->getNodeNumber();
    if (nodeNum != IPC_CPU_DONT_CARE) {
      if (maxNodeNum < nodeNum) maxNodeNum = nodeNum;
    }
  }

  NAArray<int> na(HEAP, maxNodeNum + 1);  // 0-based

  for (int i = 0; i < maxNodeNum + 1; i++) na.insertAt(i, 0);

  for (int i = 0; i < getNumEntries(); i++) {
    int nodeNum = getNodeMapEntry(i)->getNodeNumber();

    if (nodeNum != IPC_CPU_DONT_CARE) na.insertAt(nodeNum, 1);
  }

  int count = 0;
  for (int i = 0; i < maxNodeNum + 1; i++) count += na.at(i);

  return count;
}
