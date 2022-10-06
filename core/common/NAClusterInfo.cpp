
// Implementation for the classes listed in SystemParameters.h

#include "common/NAClusterInfo.h"

#include <cextdecs/cextdecs.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include <cstdlib>

#include "arkcmp/CompException.h"
#include "common/ComRtUtils.h"
#include "common/NAWNodeSet.h"
#include "nsk/nskport.h"
#include "optimizer/OptimizerSimulator.h"
#include "sqlcomp/NADefaults.h"
#include "utility.h"

static int intHashFunc(const int &Int) { return (int)Int; }

void fillNodeName(char *buf, int len);

//============================================================================
// Methods for class NAClusterInfo; it provides information about the cluster
// in which we are running.
//============================================================================

//============================================================================
//  NAClusterInfo constructor.
//
// Input: heap pointer(should always be context heap
//
//
// Output: Retrieves information for the local cluster. This includes information
// regarding its CPUs as well as for the dp2s. All these information will be cached
// in the appropriate structure.
//
// Return:
//  none
//
//==============================================================================

NAClusterInfo::NAClusterInfo(CollHeap *heap, NABoolean isOsim)
    : heap_(heap), cpuArray_(heap), hasVirtualNodes_(FALSE), localSMP_(-1), numVirtualNodes_(0) {
  if (!isOsim) {
    int dummyClusterNum;

    // Hash Map to store NodeName and NodeIds
    nodeNameToNodeIdMap_ = new (heap) NAHashDictionary<NAString, int>(NAString::hash, 101, TRUE, heap_);
    nodeIdToNodeNameMap_ = new (heap) NAHashDictionary<int, NAString>(&intHashFunc, 101, TRUE, heap);

    NADefaults::getNodeAndClusterNumbers(localSMP_, dummyClusterNum);

    int nodeCount = 0;
    int nodeMax = 0;
    MS_Mon_Node_Info_Entry_Type *nodeInfo = NULL;

    // Get the number of nodes to know how much info space to allocate
    int error = msg_mon_get_node_info(&nodeCount, 0, NULL);
    CMPASSERT(error == 0);
    CMPASSERT(nodeCount > 0);

    // Detect if we want to simulate virtual nodes on a workstation.
    //
    // To the compiler, there are <$doVirtualNodes> nodes,
    // and the ith node is named as vnode<i>.
    //
    // To executor, there is still just one node.
    //
    // The simulation is useful to work on features that require
    // multiple nodes in compiler in particular, such as tenants
    // based on resource groups.
    char *dovNodesStr = getenv("doVirtualNodes");
    numVirtualNodes_ = (dovNodesStr) ? atoi(dovNodesStr) : -1;

    if (numVirtualNodes_ > 0) {
      nodeCount = numVirtualNodes_;
    }

    // Allocate the space for node info entries
    nodeInfo = (MS_Mon_Node_Info_Entry_Type *)new (heap) char[nodeCount * sizeof(MS_Mon_Node_Info_Entry_Type)];
    CMPASSERT(nodeInfo);

    // Get the node info
    memset(nodeInfo, 0, sizeof(nodeInfo));
    nodeMax = nodeCount;

    if (numVirtualNodes_ <= 0)
      error = msg_mon_get_node_info(&nodeCount, nodeMax, nodeInfo);
    else {
      // fill the nodeInfo array with faked node info.
      const char *vNodeNamePrefix = "vnode";
      int vNodeNamePrefixLen = strlen(vNodeNamePrefix);
      char seqIdStr[20];

      for (int i = 0; i < nodeCount; i++) {
        nodeInfo[i].spare_node = false;
        nodeInfo[i].type = MS_Mon_ZoneType_Storage;
        nodeInfo[i].nid = i;

        // fill node_name field with vnode<nid>:<nid>
        str_itoa(i, seqIdStr);

        int seqIdStrLen = strlen(seqIdStr);
        // nodeInfo[i].node_name is char[128];
        int idx = 0;
        strcpy(nodeInfo[i].node_name, vNodeNamePrefix);
        idx += vNodeNamePrefixLen;
        strcpy(nodeInfo[i].node_name + idx, seqIdStr);
        idx += seqIdStrLen;
        strcpy(nodeInfo[i].node_name + idx, ":");
        idx += 1;
        strcpy(nodeInfo[i].node_name + idx, seqIdStr);
      }
    }

    CMPASSERT(error == 0);

    bool isFirst = TRUE;
    activeNodeNamesCommaSeparatedList_ = "";
    for (int i = 0; i < nodeCount; i++) {
      if (nodeInfo[i].spare_node) continue;

      // The zone type must either be an aggregation node or storage node
      // to be included in the list of CPUs.
      if ((nodeInfo[i].type & MS_Mon_ZoneType_Aggregation) != 0 ||
          ((nodeInfo[i].type & MS_Mon_ZoneType_Storage) != 0)) {
        cpuArray_.insertAt(cpuArray_.entries(), nodeInfo[i].nid);

        // store nodeName-nodeId pairs
        NAString *key_nodeName = new (heap_) NAString(nodeInfo[i].node_name, heap_);
        size_t pos = key_nodeName->index('.');
        size_t colon = key_nodeName->index(':');

        if (pos && pos != NA_NPOS) key_nodeName->remove(pos);

        if (colon != NA_NPOS) {
          // The node names for virtual nodes seen with workstations
          // are of format <nodeName>:0, <nodeName>:1 etc. We work
          // with such node names by removing all substrings
          // starting at ':' and inserting the node name into the
          // nodeIdToNodeNameMap_.
          hasVirtualNodes_ = TRUE;
          if (pos == NA_NPOS) key_nodeName->remove(colon);
        }
        if (!isFirst) activeNodeNamesCommaSeparatedList_ += ",";
        isFirst = false;
        activeNodeNamesCommaSeparatedList_ += *key_nodeName;
        int *val_nodeId = new int(nodeInfo[i].nid);
        nodeNameToNodeIdMap_->insert(key_nodeName, val_nodeId);

        // store nodeId->nadeName
        // share the same memory with nodeNameToNodeIdMap_
        nodeIdToNodeNameMap_->insert(val_nodeId, key_nodeName);
      }
    }
    NADELETEBASIC(nodeInfo, heap);

    fillNodeName(localNodeName_, MAX_SEGMENT_NAME_LEN + 1);
  }
}  // NAClusterInfo::NAClusterInfo()

NAClusterInfo::~NAClusterInfo() {
  if (nodeNameToNodeIdMap_) {
    nodeNameToNodeIdMap_->clear();
    delete nodeNameToNodeIdMap_;
  }

  if (nodeIdToNodeNameMap_) {
    nodeIdToNodeNameMap_->clear();
    delete nodeIdToNodeNameMap_;
  }
}

int NAClusterInfo::mapNodeNameToNodeNum(const NAString &keyNodeName) const {
  if (nodeNameToNodeIdMap_->contains(&keyNodeName)) {
    int *nodeValue = nodeNameToNodeIdMap_->getFirstValue(&keyNodeName);
    return *nodeValue;
  } else
    return IPC_CPU_DONT_CARE;

}  // NodeMap::getNodeNmber

const NAString *NAClusterInfo::mapNodeNamesToNodeNums(ConstStringList *nodeNames, ARRAY(short) & resultNodeIds) const {
  if (!nodeNames) return NULL;

  for (CollIndex i = 0; i < nodeNames->entries(); i++) {
    int nodeId = mapNodeNameToNodeNum(*((*nodeNames)[i]));

    resultNodeIds.insertAt(i, nodeId);

    if (nodeId == IPC_CPU_DONT_CARE) return (*nodeNames)[i];
  }

  return NULL;
}

NABoolean NAClusterInfo::mapNodeIdToNodeName(int nodeId, NAString &nodeName) const {
  NAString *value = nodeIdToNodeNameMap_->getFirstValue(&nodeId);

  if (value) {
    nodeName = *value;
    return TRUE;
  } else {
    nodeName.clear();
    return FALSE;
  }
}

int NAClusterInfo::mapLogicalToPhysicalNodeId(int ix) {
  assert(ix >= 0 && ix < cpuArray_.entries());
  return cpuArray_[ix];
}

int NAClusterInfo::mapPhysicalToLogicalNodeId(int ix) {
  // start with ix as a likely result value (no holes in the ids)
  int result = ix;

  if (result >= cpuArray_.entries()) result = cpuArray_.entries() - 1;

  // adjust for non-contiguous ids
  while (result >= 0)
    if (cpuArray_[result] == ix)
      return result;
    else
      result--;

  // did not find node id ix
  return -1;
}

int NAClusterInfo::numberOfTenantUnitsInTheCluster() const {
  size_t numCores = numberOfCpusPerSMP();
  long physMemGB = physicalMemoryAvailableInKB() / (1024 * 1024);

  int result = MAXOF(
      MINOF(numCores / NAWNODESET_CORES_PER_SLICE, ((long)(physMemGB / (0.98 * NAWNODESET_MEMORY_GB_PER_SLICE)))), 1);

  // TODO: Handle heterogeneous clusters
  return result * cpuArray_.entries();
}

#pragma warn(1506)  // warning elimination

// Returns total number of CPUs (including down CPUs)
int NAClusterInfo::getTotalNumberOfCPUs() {
  int cpuCount = cpuArray_.entries();

  return cpuCount;
}

NAClusterInfoLinux *NAClusterInfo::castToNAClusterInfoLinux() { return NULL; }

MS_MON_PROC_STATE NAClusterInfo::getNodeStatus(int nodeId) {
  int error;
  MS_MON_PROC_STATE state = MS_Mon_State_Unknown;
  MS_Mon_Node_Info_Entry_Type *nodeInfo = NULL;
  int nodeCount = getTotalNumberOfCPUs();

  nodeInfo = (MS_Mon_Node_Info_Entry_Type *)new (heap_) char[nodeCount * sizeof(MS_Mon_Node_Info_Entry_Type)];
  memset(nodeInfo, 0, sizeof(nodeInfo));

  error = msg_mon_get_node_info(&nodeCount, nodeCount, nodeInfo);
  if (error != 0) {
    QRLogger::log(CAT_SQL_EXE, LL_ERROR, "NAClusterInfo::getNodeStatus msg_mon_get_node_info return error %d\n", error);
    goto err;
  }

  if (nodeId < nodeCount) {
    state = nodeInfo[nodeId].state;
  }

err:
  if (nodeInfo) NADELETEBASIC(nodeInfo, heap_);
  return state;
}

// ------- Methods for class NAClusterInfoLinux ------

NAClusterInfoLinux::NAClusterInfoLinux(CollHeap *heap, NABoolean isOsim)
    : NAClusterInfo(heap, isOsim), nid_(0), pid_(0) {
  if (!isOsim) determineLinuxSysInfo();
}

NAClusterInfoLinux::~NAClusterInfoLinux() {}

int NAClusterInfoLinux::processorFrequency() const { return frequency_; }

float NAClusterInfoLinux::ioTransferRate() const { return iorate_; }

float NAClusterInfoLinux::seekTime() const { return seekTime_; }

int NAClusterInfoLinux::cpuArchitecture() const { return CPU_ARCH_UNKNOWN; }

size_t NAClusterInfoLinux::numberOfCpusPerSMP() const { return numCPUcoresPerNode_; }

size_t NAClusterInfoLinux::pageSize() const { return pageSize_; }

// Return the physical memory available in kilobytes
long NAClusterInfoLinux::physicalMemoryAvailableInKB() const {
  // NSK returns the total memory available so we do the same thing
  // on Linux.  This allows the plans to stay constant even as
  // the amount of memory fluctuates.
  return totalMemoryAvailableInKB_;
}

long NAClusterInfoLinux::totalMemoryAvailableInKB() const { return totalMemoryAvailableInKB_; }

long NAClusterInfoLinux::virtualMemoryAvailable() {
  // Just return a constant (like NSK does).
  return 256000000 / 1024;
}

#define LINUX_DEFAULT_FREQ 3000
#define LINUX_IO_RATE      75.0
#define LINUX_SEEK_RATE    0.0038

void NAClusterInfoLinux::determineLinuxSysInfo() {
  // Set the page size in killobytes and determine how much memory
  // is available on this node (in kilobytes).
  pageSize_ = (size_t)sysconf(_SC_PAGESIZE) / 1024U;
  totalMemoryAvailableInKB_ = pageSize_ * (size_t)sysconf(_SC_PHYS_PAGES);
  numCPUcoresPerNode_ = sysconf(_SC_NPROCESSORS_ONLN);

  frequency_ = 0.0;

  // Read the CPU frequency from the sysfs filesystem.
  ifstream infoFile("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq");
  if (infoFile.fail()) {
    // This code should log a warning.

    // use /proc/cpuinfo
    char var[256];
    ifstream cpuInfoFile("/proc/cpuinfo");
    const char *freqToken = "cpu MHz";
    int freqTokenLen = strlen(freqToken);
    while (cpuInfoFile.good()) {
      // Read the variable name from the file.
      cpuInfoFile.getline(var, sizeof(var), ':');  // read the token part
      int len = strlen(var);
      if (len >= freqTokenLen && !strncmp(var, freqToken, freqTokenLen)) {
        cpuInfoFile >> frequency_;
        break;
      }
      cpuInfoFile.getline(var, sizeof(var));  // read the value part
    }

    if (frequency_ == 0.0)
      // Use the default frequency
      frequency_ = LINUX_DEFAULT_FREQ;
  } else {
    int freqUlongVal;
    infoFile >> freqUlongVal;
    frequency_ = freqUlongVal / 1000;
    infoFile.close();
  }

  // These should be determined programmatically, but are hard-coded for now.
  iorate_ = LINUX_IO_RATE;
  seekTime_ = LINUX_SEEK_RATE;
}

//============================================================================
// This method writes the information related to the NAClusterInfoLinux class
// to a logfile called "NAClusterInfo.txt".
//============================================================================
void NAClusterInfoLinux::captureOSInfo(ofstream &nacllinuxfile) const {
  nacllinuxfile << "frequency_: " << frequency_ << endl
                << "iorate_: " << iorate_ << endl
                << "seekTime_: " << seekTime_ << endl
                << "pageSize_: " << pageSize_ << endl
                << "totalMemoryAvailable_: " << totalMemoryAvailableInKB_ << endl
                << "numCPUcoresPerNode_: " << numCPUcoresPerNode_ << endl;
}

NAClusterInfoLinux *NAClusterInfoLinux::castToNAClusterInfoLinux() { return this; }
