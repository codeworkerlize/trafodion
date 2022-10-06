
/* -*-C++-*-
****************************************************************************
*
* File:         FragDir.h
* Description:  Fragment directory in the executor (attached to root
*               tdbs and to split bottom tdbs, used by operators that
*               download fragments to other processes)

* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

#ifndef FRAG_DIR_H
#define FRAG_DIR_H

#include "common/Int64.h"
#include "export/NAVersionedObject.h"
#include "comexe/PartInputDataDesc.h"
#include "comexe/ComResourceInfo.h"
#include "comexe/ComASNodes.h"

// -----------------------------------------------------------------------
// Contents of this file
// -----------------------------------------------------------------------
class ExEspNodeMap;
class ExEspNodeMapEntry;
class ExFragDir;
class ExFragDirEntry;

// -----------------------------------------------------------------------
// Forward references
// -----------------------------------------------------------------------
class ExPartInputDataDesc;
class PartitioningFunction;

// -----------------------------------------------------------------------
// Id of a fragment (actually just an index into the fragment directory
// -----------------------------------------------------------------------
typedef UInt32 ExFragId;

// -----------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExFragDir
// -----------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ExFragDir> ExFragDirPtr;

// -----------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExEspNodeMapEntry and ExFragDir
// -----------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ExEspNodeMapEntry> ExEspNodeMapEntryPtr;
typedef NAVersionedObjectPtrTempl<ExFragDirEntry> ExFragDirEntryPtr;

// -----------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExFragDirPtr
// -----------------------------------------------------------------------
typedef NAVersionedObjectPtrArrayTempl<ExFragDirEntryPtr> ExFragDirEntryPtrArray;

// -----------------------------------------------------------------------
// Each node map entry contains a node number and cluster number
// indicating the location where a fragment instance will be executed.
// -----------------------------------------------------------------------
class ExEspNodeMapEntry : public NAVersionedObject {
  friend class ExEspNodeMap;

 public:
  ExEspNodeMapEntry() : NAVersionedObject(-1) {}

  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  Long pack(void *space);
  int unpack(void *, void *reallocator);

 private:
  NABasicPtr clusterName_;        // EXPAND node name        // 00-07
  int nodeNumber_;              // NSK CPU number          // 08-11
  NABoolean needToWork_;          // When TRUE, the esp needs to work // 12-15
  char fillersExEspNodeMap_[16];  // 16-31
};

// -----------------------------------------------------------------------
// Each fragment entry has an ExEspNodeMap field.
// -----------------------------------------------------------------------
class ExEspNodeMap {
 public:
  ExEspNodeMap();
  // Initialize and allocate map_.
  //
  // No destructor. If destructor is added, compiler will
  // add four hidden bytes in front of array ExEspNodeMap,
  // which causes some inconvenience to deallocate the ExEspNodeMap[]
  // from an NAHeap.  Need to locate the starting byte of array.

  int getNodeNumber(int instance) const;
  const char *getClusterName(int instance) const;
  NABoolean needToWork(int instance) const;

  // Mutator methods (for code generator only)
  void setMapArray(int entries, ExEspNodeMapEntry *me) {
    map_ = me;
    entries_ = entries;
  }
  void setEntry(int instance, const char *clusterName, int nodeNumber, NABoolean needToWork, Space *space);

  int isNodeNumDuplicate(int numCpus);

  // pack and unpack
  Long pack(void *space);
  int unpack(void *base, void *reallocator);

  void display(NAText &output);

 private:
  // An array of node map entries.
  // The number of entries is equal to numESPs.
  ExEspNodeMapEntryPtr map_;      // 00-07
  int entries_;                 // 08-11
  char fillersExEspNodeMap_[20];  // 12-31
};

// -----------------------------------------------------------------------
// An entry of the executor fragment directory. This entry describes
// one fragment of an execution plan. A fragment is a part of a plan
// that has a root tdb, ends in either leaf tdbs (scan, tuple, ...) or
// tdbs that offload work to other processes (part. access, split top).
// A plan fragment is executed in a single process.
// -----------------------------------------------------------------------
class ExFragDirEntry : public NAVersionedObject {
  friend class ExFragDir;

 public:
  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(ExFragDirEntry); }

  ExFragDirEntry() : NAVersionedObject(-1) {}

  virtual Long pack(void *);
  virtual int unpack(void *, void *reallocator);

  NABoolean isNeedsTransaction() { return (flags_ & NEEDS_TRANSACTION); }
  NABoolean isCompressFrag() { return (flags_ & COMPRESS_FRAGMENT); }
  NABoolean isSoloFrag() { return (flags_ & SOLO_FRAGMENT); }
  NABoolean isContainsBMOs() { return (flags_ & CONTAINS_BMO); }

 private:
  enum ExFragEntryFlags {
    NEEDS_TRANSACTION = 0x0001,  // fragment needs TA to execute
    COMPRESS_FRAGMENT = 0x0002,  // should compress this fragment before
                                 // sending it down to ESPs
    SOLO_FRAGMENT = 0x0004,      // must be only fragment in ESP
    CONTAINS_BMO = 0x0008        // fragment contains one or more BMOs
  };

  // type of the entry (from this we can derive the type of the top level
  // node in the fragment)
  int type_;  // 00-03

  // fragment id of the parent fragment.
  UInt32 parentId_;  // 04-07

  // offset of this fragment within buffer that contains all fragments.
  long globalOffset_;  // 08-15

  // length of this fragment (always a multiple of 8)
  long fragmentLength_;  // 16-23

  // offset of the top-level node of the fragment within its buffer
  // (usually this is 0)
  long topNodeOffset_;  // 24-31

  // partitioning information
  ExPartInputDataDescPtr partDescriptor_;  // 32-39

  // flags
  int flags_;  // 40-43

  // fields used only for ESP entries
  // # ESPs assumed by optimizer
  int numESPs_;  // 44-47

  // node map info
  ExEspNodeMap espNodeMap_;  // 48-63

  // level of esp layer relative to root node. First esp layer is 1.
  int espLevel_;  // 64-67

  UInt16 fragmentMemoryQuota_;  // 68-69

  char fillersExFragDirEntry4_[18];  // 70-87
};

// -----------------------------------------------------------------------
// Fragment Directory contains a list of fragments.
// -----------------------------------------------------------------------
class ExFragDir : public NAVersionedObject {
 public:
  enum ExFragDirFlags {
    MULTI_FRAGMENTS = 1,  // multi-fragment ESPs is enabled
    FRAGMENT_QUOTAS = 2   // multi-fragment ESP quotas is enabled
  };

  enum ExFragEntryType {
    MASTER = 1,  // executed in master with an ex_root_tdb on top
    DP2 = 2,     // downloaded to DP2
    ESP = 3,     // executed in an ESP with an ex_split_bottom_tdb on top
    EXPLAIN = 4  // fragment used to hold explain info.
  };

  // allocate a fragment directory with <entries> entries in a particular
  // space (constructor can be used at compile time only)
  //
  ExFragDir(int entries, Space *space, NABoolean multiFragments, NABoolean fragmentQuotas, UInt16 multiFragmentVm,
            UInt8 numMultiFragments, ComASNodes *asNodes);

  ExFragDir() : NAVersionedObject(-1) {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(ExFragDir); }

  // access members that are not part of fragment directory entries
  inline int getNumEntries() const { return numEntries_; }

  inline const ExScratchFileOptions *getScratchFileOptions() const { return scratchFileOptions_; }
  inline void setScratchFileOptions(ExScratchFileOptions *sfo) { scratchFileOptions_ = sfo; }
  inline int getNodeMask() const { return nodeMask_; }
  inline const ComASNodes *getTenantASNodes() const { return tenantASNodes_; }
  inline void setNodeMask(int nm) { nodeMask_ = nm; }
  inline int getMaxESPsPerNode() { return maxESPsPerNode_; }
  inline void setMaxESPsPerNode(int v) { maxESPsPerNode_ = v; }

  // access fragment directory entries
  inline void set(ExFragId index, ExFragEntryType type, ExFragId parentId, int globalOffset, int fragmentLength,
                  int topNodeOffset, ExPartInputDataDesc *partDescriptor, ExEspNodeMap *espNodeMap, int numESPs,
                  int espLevel, int needsTransaction, NABoolean compressFrag = FALSE, NABoolean soloFrag = FALSE,
                  UInt16 fragmentMemoryQuota = 0, NABoolean containsBMOs = FALSE) {
    ExFragDirEntry *entry = fragments_[index];

    entry->type_ = type;
    entry->parentId_ = parentId;
    entry->globalOffset_ = globalOffset;
    entry->fragmentLength_ = fragmentLength;
    entry->topNodeOffset_ = topNodeOffset;
    entry->partDescriptor_ = partDescriptor;
    entry->numESPs_ = numESPs;
    entry->espLevel_ = espLevel;
    if (espNodeMap) entry->espNodeMap_ = *espNodeMap;
    entry->flags_ = 0;
    if (needsTransaction) entry->flags_ |= ExFragDirEntry::NEEDS_TRANSACTION;
    if (compressFrag) entry->flags_ |= ExFragDirEntry::COMPRESS_FRAGMENT;
    if (soloFrag) entry->flags_ |= ExFragDirEntry::SOLO_FRAGMENT;
    entry->fragmentMemoryQuota_ = fragmentMemoryQuota;
    if (containsBMOs) entry->flags_ |= ExFragDirEntry::CONTAINS_BMO;
  }

  inline ExFragEntryType getType(ExFragId ix) const { return (ExFragEntryType)fragments_[ix]->type_; }
  inline ExFragId getParentId(ExFragId ix) const { return fragments_[ix]->parentId_; }
  inline int getGlobalOffset(ExFragId ix) const { return (int)(fragments_[ix]->globalOffset_); }
  inline int getFragmentLength(ExFragId ix) const { return (int)(fragments_[ix]->fragmentLength_); }
  inline int getTopNodeOffset(ExFragId ix) const { return (int)(fragments_[ix]->topNodeOffset_); }
  inline int getNumESPs(ExFragId ix) const { return fragments_[ix]->numESPs_; }
  inline int getEspLevel(ExFragId ix) const { return fragments_[ix]->espLevel_; }
  inline ExPartInputDataDesc *getPartDesc(ExFragId ix) const { return fragments_[ix]->partDescriptor_; }
  inline ExEspNodeMap *getEspNodeMap(ExFragId ix) const { return &fragments_[ix]->espNodeMap_; }
  inline int needsTransaction(ExFragId ix) const { return fragments_[ix]->isNeedsTransaction(); }
  inline int getPlanVersion(void) const { return planVersion_; }
  inline void setPlanVersion(int pv) { planVersion_ = pv; }
  inline NABoolean isCompressFrag(ExFragId ix) const { return fragments_[ix]->isCompressFrag(); }
  inline NABoolean soloFrag(ExFragId ix) const { return fragments_[ix]->isSoloFrag(); }
  inline UInt16 getFragmentMemoryQuota(ExFragId ix) const { return fragments_[ix]->fragmentMemoryQuota_; }
  inline NABoolean containsBMOs(ExFragId ix) const { return fragments_[ix]->isContainsBMOs(); }
  inline NABoolean espMultiFragments() const { return (flags_ & MULTI_FRAGMENTS) != 0; }
  inline NABoolean espFragmentQuotas() const { return (flags_ & FRAGMENT_QUOTAS) != 0; }
  inline UInt16 espMultiFragmentVm() const { return multiFragmentVm_; }
  inline UInt8 espNumFragments() const { return numMultiFragments_; }

  // The fragment directory gets generated by the generator, so we need
  // pack and unpack procedures for it (they handle the entries as well)
  //
  Long pack(void *space);
  int unpack(void *, void *reallocator);

  int getExplainFragDirEntry(int &fragOffset, int &fragLen, int &topNodeOffset);

 private:
  // ptr to an array of <numEntries_> entries
  ExFragDirEntryPtrArray fragments_;  // 00-07
  int numEntries_;                  // 08-11

  // get up to a multiple of 8 bytes
  int nFiller_;  // 12-15

  // resource information for all fragments
  ExScratchFileOptionsPtr scratchFileOptions_;  // 16-23
  UInt32 nodeMask_;                             // 24-27
  UInt32 planVersion_;                          // 28-31
  UInt16 multiFragmentVm_;                      // 32-33
  UInt8 flags_;                                 // 34
  UInt8 numMultiFragments_;                     // 35
  int maxESPsPerNode_;                        // 36-39

  // In multi-tenant configurations, info on where the
  // tenant is located, to make sure we start ESPs only
  // on nodes where the tenant can run
  ComASNodesPtr tenantASNodes_;  // 40-47

  char fillersExFragDir_[16];  // 48-63
};

#endif /* EX_FRAG_DIR_H */
