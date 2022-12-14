
/* -*-C++-*-
****************************************************************************
*
* File:         FragDir.cpp
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

// -----------------------------------------------------------------------

#include "comexe/FragDir.h"

#include "comexe/ComPackDefs.h"
#include "comexe/PartInputDataDesc.h"
#include "common/Ipc.h"
#include "common/trafconfig.h"
static const int nodeNameLen = TC_PROCESSOR_NAME_MAX;  // defined in trafconf/trafconfig.h

ExFragDir::ExFragDir(int entries, Space *space, NABoolean multiFragments, NABoolean fragmentQuotas,
                     UInt16 multiFragmentVm, UInt8 numMultiFragments, ComASNodes *asNodes)
    : NAVersionedObject(-1) {
  numEntries_ = entries;
  scratchFileOptions_ = (ExScratchFileOptionsPtr)NULL;
  nodeMask_ = 0;
  planVersion_ = ComVersion_GetCurrentPlanVersion();

  // allocate an array of entries from "space"
  fragments_ = (ExFragDirEntryPtr *)space->allocateAlignedSpace((size_t)(sizeof(ExFragDirEntryPtr) * numEntries_));

  for (int i = 0; i < numEntries_; i++) fragments_[i] = new (space) ExFragDirEntry();
  flags_ = 0;
  if (multiFragments) flags_ |= MULTI_FRAGMENTS;
  if (fragmentQuotas) flags_ |= FRAGMENT_QUOTAS;
  multiFragmentVm_ = multiFragmentVm;
  numMultiFragments_ = numMultiFragments;
  maxESPsPerNode_ = -1;
  tenantASNodes_ = asNodes;
}

Long ExFragDir::pack(void *space) {
  fragments_.pack(space, numEntries_);
  scratchFileOptions_.pack(space);
  tenantASNodes_.pack(space);
  return NAVersionedObject::pack(space);
}

int ExFragDir::unpack(void *base, void *reallocator) {
  if (fragments_.unpack(base, numEntries_, reallocator)) return -1;
  if (scratchFileOptions_.unpack(base, reallocator)) return -1;
  if (tenantASNodes_.unpack(base, reallocator)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}

int ExFragDir::getExplainFragDirEntry(int &fragOffset, int &fragLen, int &topNodeOffset) {
  // Find the EXPLAIN Fragment
  CollIndex explainFragIndex = NULL_COLL_INDEX;

  // Loop over all the fragments
  for (CollIndex i = 0; i < (CollIndex)getNumEntries(); i++) {
    if (getType(i) == ExFragDir::EXPLAIN) {
      explainFragIndex = i;
      break;
    }
  }

  // There should always be just one EXPLAIN Fragment
  if (explainFragIndex == NULL_COLL_INDEX) return -1;

  fragLen = getFragmentLength(explainFragIndex);
  fragOffset = getGlobalOffset(explainFragIndex);
  topNodeOffset = getTopNodeOffset(explainFragIndex);
  return 0;
}

Long ExFragDirEntry::pack(void *space) {
  partDescriptor_.pack(space);
  espNodeMap_.pack(space);
  return NAVersionedObject::pack(space);
}

int ExFragDirEntry::unpack(void *base, void *reallocator) {
  if (partDescriptor_.unpack(base, reallocator)) return -1;
  if (espNodeMap_.unpack(base, reallocator)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}

Long ExEspNodeMapEntry::pack(void *space) { return clusterName_.pack(space); }

int ExEspNodeMapEntry::unpack(void *base, void *reallocator) { return clusterName_.unpack(base); }

ExEspNodeMap::ExEspNodeMap() : map_(NULL), entries_(0) {}

int ExEspNodeMap::getNodeNumber(int instance) const {
  if (map_ == (ExEspNodeMapEntryPtr)NULL) return IPC_CPU_DONT_CARE;
  return map_[instance].nodeNumber_;
}

const char *ExEspNodeMap::getClusterName(int instance) const {
  if (map_ == (ExEspNodeMapEntryPtr)NULL) return NULL;
  return map_[instance].clusterName_;
}

NABoolean ExEspNodeMap::needToWork(int instance) const {
  if (map_ == (ExEspNodeMapEntryPtr)NULL) return FALSE;
  return map_[instance].needToWork_;
}

void ExEspNodeMap::setEntry(int instance, const char *clusterName, int nodeNumber, NABoolean needToWork, Space *space) {
  if (map_ && entries_ > instance) {
    map_[instance].clusterName_ = space->allocateMemory(nodeNameLen);
    strcpy(map_[instance].clusterName_, clusterName);
    map_[instance].nodeNumber_ = nodeNumber;
    map_[instance].needToWork_ = needToWork;
  }
}

Long ExEspNodeMap::pack(void *space) { return map_.packArray(space, entries_); }

int ExEspNodeMap::unpack(void *base, void *reallocator) { return map_.unpackArray(base, entries_, reallocator); }

int ExEspNodeMap::isNodeNumDuplicate(int numCpus) {
  if (map_ == (ExEspNodeMapEntryPtr)NULL) return 0;

  int nodeEspNum[numCpus];
  for (int j = 0; j < numCpus; ++j) nodeEspNum[j] = 0;
  for (int i = 0; i < entries_; ++i) {
    int nodeNum = getNodeNumber(i);
    nodeEspNum[nodeNum]++;
    if (nodeEspNum[nodeNum] > 1) return 1;  // duplicated
  }
  return 0;
}

void ExEspNodeMap::display(NAText &output) {
  const char delim = '|';
  output.reserve(100);
  char buf[20];
  for (int i = 0; i < entries_; ++i) {
    sprintf(buf, "%d%c", getNodeNumber(i), delim);
    output.append(buf);
  }
}
