/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
// This file contains a class, NAClusterInfo, which provides information about
// the cluster in which SQL/MX is running. It assumes that all nodes in
// it are identical (in terms of parameters like the number of CPUs and the
// kind of CPU they are, as well as on the memory available and the page size
// etc.) It also tells which DP2 processs are running in which SMP. It assumes
// that SMPs are numbered 0, 1, 2, ... etc

#ifndef __NA_CLUSTER_INFO_H
#define __NA_CLUSTER_INFO_H

#include "common/Platform.h"
#include "common/Collections.h"
#include "export/NAStringDef.h"
#include "common/NAString.h"
#include "common/ComRtUtils.h"

//-----------------------------
// Classes defined in this file
//-----------------------------

class NAClusterInfo;
class NAClusterInfoLinux;

//----------------------
// Known processor types
//----------------------
enum ProcesorTypes {
  CPU_ARCH_INTEL_80386,
  CPU_ARCH_INTEL_80486,
  CPU_ARCH_PENTIUM,
  CPU_ARCH_PENTIUM_PRO,
  CPU_ARCH_MIPS,
  CPU_ARCH_ALPHA,
  CPU_ARCH_PPC,
  CPU_ARCH_UNKNOWN
};

// --------------------------------------------------------------------------
// Information about the cluster is kept in a global location in the
// CLI.  When we are in the simulation phase of OSIM, however, we
// override this information in the compiler context. In that case, we
// use a different class, derived from NAClusterInfoLinux. Therefore
//
// - Executor code should access this information through the CLI, using
//   CliGlobals::getNAClusterInfo()
//
// - Compiler code should access NAClusterInfo through
//   CmpContext::getNAClusterInfo(), there is a macro
//   CURRCONTEXT_CLUSTERINFO defined for this call.
//
// Cluster information in the CLI is set up once and then used until
// the process terminates. TBD: We may also need to refresh it when we
// dynamically add or remove nodes from the cluster.
// --------------------------------------------------------------------------

class NAClusterInfo : public NABasicObject {
 public:
  NAClusterInfo(CollHeap *heap, NABoolean isOsim);
  ~NAClusterInfo();

  virtual int processorFrequency() const = 0;
  virtual float ioTransferRate() const = 0;
  virtual float seekTime() const = 0;
  virtual int cpuArchitecture() const = 0;

  virtual size_t numberOfCpusPerSMP() const = 0;

  virtual size_t pageSize() const = 0;
  virtual long physicalMemoryAvailableInKB() const = 0;
  virtual long totalMemoryAvailableInKB() const = 0;
  virtual long virtualMemoryAvailable() = 0;

  // This is called by captureNAClusterInfo() to capture the OSIM
  // information that is specific to the operating system. Each new
  // platform must define this.
  virtual void captureOSInfo(ofstream &f) const = 0;

  int getNumActiveCluster() const { return 1; }
  NABoolean smpActive(int smp) const;

  // return total number of CPUs (includes all, that is, even down CPUs)
  int getTotalNumberOfCPUs();
  const NAArray<CollIndex> &getCPUArray() const { return cpuArray_; }

  NABoolean hasVirtualNodes() const { return hasVirtualNodes_; }

  int mapNodeNameToNodeNum(const NAString &node) const;

  const NAString *mapNodeNamesToNodeNums(ConstStringList *nodeNames, ARRAY(short) & resultNodeIds) const;

  NABoolean mapNodeIdToNodeName(int nodeId, NAString &nodeName) const;
  NAString &getActiveNodeNamesAsCommaSeparatedList() { return activeNodeNamesCommaSeparatedList_; }

  // return the physical node id (nid) for a logical id (always
  // numbered 0 ... n-1) the physical id is always >= the logical id
  int mapLogicalToPhysicalNodeId(int ix);
  // and the same way in reverse
  int mapPhysicalToLogicalNodeId(int ix);
  // are logical/physical node ids the same?
  NABoolean nodeIdsAreContiguous() { return cpuArray_[cpuArray_.entries() - 1] == cpuArray_.entries() - 1; }

  // TODO: Handle heterogeneous clusters
  int numberOfTenantUnitsInTheCluster() const;

  // The OSIM uses the following method to capture cluster information
  void captureNAClusterInfo(ofstream &naclfile);

  virtual NAClusterInfoLinux *castToNAClusterInfoLinux();

  short getLocalNodeId() { return localSMP_; }
  const char *getLocalNodeName() { return localNodeName_; }

  int getNumVirtualNodes() { return numVirtualNodes_; }

  MS_MON_PROC_STATE getNodeStatus(int nodeId);

 protected:
  //------------------------------------------------------------------------
  // localSMP_ is the current node ID.
  //------------------------------------------------------------------------
  short localSMP_;

  //------------------------------------------------------------------------
  // heap_ is where this NAClusterInfo was allocated.  This should be the
  // context heap.
  //------------------------------------------------------------------------
  CollHeap *heap_;

  // ------------------------------------------------------------------------
  // A list of node ids of available nodes. Typically, this will be
  // a list of the numbers 0 ... n-1 but in some cases a node in
  // the middle may be removed, so we end up with "holes" in the
  // node ids.
  // ------------------------------------------------------------------------
  NAArray<CollIndex> cpuArray_;

  //------------------------------------------------------------------------
  // hashdictionary used to store the mapping of cluster name to cluster id
  // This structure is stored on the context heap
  // because we don't expect this mapping to change during a session..
  //------------------------------------------------------------------------
  NAHashDictionary<int, NAString> *nodeIdToNodeNameMap_;

  // hashdictionary that maps nodeName to nodeId.
  NAHashDictionary<NAString, int> *nodeNameToNodeIdMap_;

  NABoolean hasVirtualNodes_;

  NAString activeNodeNamesCommaSeparatedList_;

  // The number of vritual nodes in a faked cluster environment.
  // This variable is set to 0 when not in faking mode.
  int numVirtualNodes_;

  // local node name
  char localNodeName_[MAX_SEGMENT_NAME_LEN + 1];
};

class NAClusterInfoLinux : public NAClusterInfo {
 public:
  NAClusterInfoLinux(CollHeap *heap, NABoolean isOsim);
  ~NAClusterInfoLinux();
  int processorFrequency() const;
  float ioTransferRate() const;
  float seekTime() const;
  int cpuArchitecture() const;
  size_t numberOfCpusPerSMP() const;
  size_t pageSize() const;
  long physicalMemoryAvailableInKB() const;
  long totalMemoryAvailableInKB() const;
  long virtualMemoryAvailable();

  void captureOSInfo(ofstream &) const;

  int get_pid() { return pid_; };
  int get_nid() { return nid_; };

  virtual NAClusterInfoLinux *castToNAClusterInfoLinux();

 private:
  void determineLinuxSysInfo();

  int pid_;  // the pid of the current process
  int nid_;  // the nid of the current process

 protected:
  //-------------------------------------------------------------------------
  // Stores the frequency of the SMP, in Megahertz
  //-------------------------------------------------------------------------
  int frequency_;

  //-------------------------------------------------------------------------
  // Stores the IO transfer rate of the disk, in MB/sec
  //-------------------------------------------------------------------------
  float iorate_;

  //-------------------------------------------------------------------------
  // Stores the average seek time of the disk, in ms
  //-------------------------------------------------------------------------
  float seekTime_;

  //-------------------------------------------------------------------------
  // Stores the memory page size, in kilobytes.
  //-------------------------------------------------------------------------
  size_t pageSize_;

  //-------------------------------------------------------------------------
  // Stores the total memory available, in kilobytes.
  //-------------------------------------------------------------------------
  long totalMemoryAvailableInKB_;

  //-------------------------------------------------------------------------
  // Number of CPU cores per Linux node.
  //-------------------------------------------------------------------------
  size_t numCPUcoresPerNode_;
};

#endif  // __NA_CLUSTER_INFO_H
