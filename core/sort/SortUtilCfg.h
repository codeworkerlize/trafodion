
#ifndef SORTUTILCONFIG_H
#define SORTUTILCONFIG_H

/* -*-C++-*-
******************************************************************************
*
* File:         SortUtilConfig.h
* RCS:          $Id: SortUtilCfg.h,v 1.8 1998/07/29 22:02:29  Exp $
*
* Description:  This file contains the definition of the SortUtilConfig class.
*               This class is can be used to configure a sorting session by
*               passing it as a parameter to SortUtil::SortInitialize member
*               function.
*               The main purpose of this class is to group together various
*               configurable parameters related to the sorting session.
*               In SQL/MP, many of these fields used to be separate parameters
*               to sort APIs. This class is needed only to initialize the
*               sort session, after which it can be destructed.
*
* Created:      07/12/96
* Modified:     $ $Date: 1998/07/29 22:02:29 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
******************************************************************************
*/

#include "CommonStructs.h"
#include "Const.h"
#include "comexe/ComResourceInfo.h"
#include "export/NABasicObject.h"

// Forward reference
class ExSubtask;
class IpcEnvironment;
class ex_tcb;

class SortUtilConfig : public NABasicObject {
 public:
  SortUtilConfig(CollHeap *heap);
  SortUtilConfig();  // this constructor should be removed once executor
                     // made the change to use the other constructor
  ~SortUtilConfig();

  NABoolean setSortType(SortType &sorttype);

  void setScratchThreshold(unsigned short scratchThreshold) { scratchThreshold_ = scratchThreshold; }
  unsigned short getScratchThreshold() { return scratchThreshold_; }

  void setMaxNumBuffers(int mbs) { maxNumBuffers_ = mbs; }
  int getMaxNumBuffers() const { return maxNumBuffers_; }

  void setSortMemory(int min = 150000L, int max = 12000000L);

  void setSortMaxMemory(int max = 120000L);
  void getSortMaxMemory(int &max);

  NABoolean setRecSize(int recsize);
  int getRecSize() const;

  NABoolean setKeyInfo(int keysize);

  void setUseBuffered(NABoolean torf);
  NABoolean getUseBuffered();

  void setDisableCmpHintsOverflow(NABoolean torf) { disableCmpHintsOverflow_ = torf; }
  NABoolean getDisableCmpHintsOverflow() { return disableCmpHintsOverflow_; }

  void setLogInfoEvent(NABoolean torf);
  NABoolean logInfoEvent();

  void setLogInfoEventDone() { logDone_ = TRUE; }
  NABoolean logInfoEventDone() { return logDone_; }
  const ExScratchDiskDrive *getScratchDirListSpec() { return scratchDirListSpec_; }

  void setScratchDirListSpec(const ExScratchDiskDrive *scratch_disk_list_spec) {
    scratchDirListSpec_ = scratch_disk_list_spec;
  }

  void setNumDirsSpec(int nd) { numDirsSpec_ = nd; }
  int getNumDirsSpec() { return numDirsSpec_; }

  void setEspInstance(int esp_num) { espInstance_ = esp_num; }
  int getEspInstance() { return espInstance_; }

  void setNumEsps(int numesps) { numEsps_ = numesps; }
  int getNumEsps() { return numEsps_; }
  void setEventHandler(ExSubtask *eh) { ioEventHandler_ = eh; }
  void setIpcEnvironment(IpcEnvironment *ipc) { ipcEnv_ = ipc; }
  void setCallingTcb(ex_tcb *tcb) { callingTcb_ = tcb; }

  inline ex_tcb *getCallingTcb() { return callingTcb_; }
  void setMemoryQuotaMB(Int16 memoryQuotaMB) {
    memoryQuotaMB_ = memoryQuotaMB;
    initialMemoryQuotaMB_ = memoryQuotaMB;
  }
  void setMemoryPressureThreshold(Int16 pressureThreshold) { pressureThreshold_ = pressureThreshold; }

  void setMinimalSortRecs(UInt32 sortRecs) { minimalSortRecs_ = sortRecs; }
  void setPartialSort(NABoolean partialSort) { partialSort_ = partialSort; }

  UInt32 getMinimalSortRecs(void) { return minimalSortRecs_; }

  void setScratchMgmtOption(int scratchMgmtOption) { scratchMgmtOption_ = scratchMgmtOption; }
  void setScratchMaxOpens(int scratchMaxOpens) { scratchMaxOpens_ = scratchMaxOpens; }
  void setPreallocateExtents(NABoolean v) { preAllocateExtents_ = v; }
  void setSortMergeBlocksPerBuffer(int smbb) { sortMergeBlocksPerBuffer_ = smbb; }
  void setScratchDiskLogging(NABoolean v) { scratchDiskLogging_ = v; }
  NABoolean scratchDiskLogging(void) { return scratchDiskLogging_; }

  void setScratchIOBlockSize(int blockSize) { scratchIOBlockSize_ = blockSize; }
  int getScratchIOBlockSize() { return scratchIOBlockSize_; }

  void setScratchIOVectorSize(Int16 vectorSize) { scratchIOVectorSize_ = vectorSize; }
  int getScratchIOVectorSize() { return scratchIOVectorSize_; }

  void setScratchOverflowMode(ScratchOverflowMode ovMode) { ovMode_ = ovMode; }

  ScratchOverflowMode getScratchOverflowMode(void) { return ovMode_; }

  NABoolean resizeCifRecord() const { return resizeCifRecord_; }
  void setResizeCifRecord(NABoolean v) { resizeCifRecord_ = v; }

  NABoolean considerBufferDefrag() const { return considerBufferDefrag_; }
  void setConsiderBufferDefrag(NABoolean v) { considerBufferDefrag_ = v; }
  Int16 numberOfBytesForRecordSize() const {
    if (resizeCifRecord()) {
      return sizeof(UInt32);
    } else {
      return 0;
    }
  }

  void setBmoCitizenshipFactor(Float32 bmoCf) { bmoCitizenshipFactor_ = bmoCf; }
  Float32 getBmoCitizenshipFactor(void) { return bmoCitizenshipFactor_; }

  void setMemoryContingencyMB(int mCMB) { pMemoryContingencyMB_ = mCMB; }
  int getMemoryContingencyMB(void) { return pMemoryContingencyMB_; }

  void setSortMemEstInKBPerNode(Float32 s) { sortMemEstInKBPerNode_ = s; }
  Float32 getSortMemEstInKBPerNode() { return sortMemEstInKBPerNode_; }

  void setEstimateErrorPenalty(Float32 e) { estimateErrorPenalty_ = e; }
  Float32 getEstimateErrorPenalty() { return estimateErrorPenalty_; }

  void setBmoMaxMemThresholdMB(Int16 mm) { bmoMaxMemThresholdMB_ = mm; }
  void setIntermediateScratchCleanup(NABoolean v) { intermediateScratchCleanup_ = v; }
  NABoolean intermediateScratchCleanup() { return intermediateScratchCleanup_; }
  void setTopNSort(NABoolean v) { topNSort_ = v; }

  friend class SortUtil;

  SortType sortType_;  // This structure contains several bit fields used
                       // to specify various options like the algorithm
                       // being used, whether any special settings like
                       // top of  N, Splitting vector,or Remove duplicate
                       // are being used.
 private:
  CollHeap *heapAddr_;  // Pointer to the space object which can be used
                        // to obtain heap memory by calling the member
                        // function :
                        //    Space::allocateAlignedSpace(size_t size)
                        // Executor  implements heap memory as an
                        // object of class Space. Any executor component
                        // can use a member function within this object
                        // to allocate memory. The executor would pass
                        // a pointer to this object during sortInitialize

  int approxNumRecords_;  // Can be used by SortUtil for various
                          // estimates including those related to
                          // memory requirements

  int maxMemRecords_;  // Maximum number of records which
                       // Executor can hold in memory at any
                       // point.

  int recSize_;  // Maximum record size of input.
  int keySize_;  // Length of the key prepended to each records.
                 // This key is already encoded as a string.

  int runSize_;                                   // Using it currently to allow user to specify
  int mergeOrder_;                                // Need to modify this to do automatically.
  int minMem_;                                    // Minimum sort heap  memory
  int maxMem_;                                    // Maximum sort heap memory
  NABoolean topNSort_;                            // TopN sorting enable/disable
  int runSizeIncr_;                               // unused :how much to increment the run size by.
  int maxNumBuffers_;                             // Max buffer space as set by the compiler
  unsigned short scratchThreshold_;               // percent of disk usage after which a disk will be discarded for use
  const ExScratchDiskDrive *scratchDirListSpec_;  // Information about scratchvols to  include or exclude.
  int numDirsSpec_;
  int espInstance_;
  int numEsps_;
  ExSubtask *ioEventHandler_;
  IpcEnvironment *ipcEnv_;
  ex_tcb *callingTcb_;
  NABoolean useBufferedWrites_;
  NABoolean disableCmpHintsOverflow_;
  NABoolean logInfoEvent_;
  NABoolean logDone_;
  NABoolean partialQuotaSystem_;
  NABoolean partialSort_;

  // amount of heap memory quota for this operator determined at generation
  // time taking into other BMO operators. If value is negative, then BMO
  // operation is not enabled.
  Int16 memoryQuotaMB_;
  Int16 initialMemoryQuotaMB_;  // memory quota as set by generator.

  // amount of memory quota consumed in real time
  // in relation to memoryQuotaMB_.
  UInt32 memoryQuotaUsedBytes_;

  // memory threshold after which overflow is triggered.
  // Set by the compiler defaults, applicable in general
  // to all memory quota participants. This field can be
  // removed, once the check for memroy pressure logic is
  // moved to statement globals.
  Int16 pressureThreshold_;

  // Threshold value based on which sort will either choose quick sort
  // or interative heap sort.
  UInt32 minimalSortRecs_;

  // This option refers to SETMODE(141, option).
  // Valid values are 0, 5 and 9.
  // 0 non buffered writes
  // 5 Early reply + striping on primary and mirror
  // 9 Early reply.
  int scratchMgmtOption_;

  // Number of scratch opens per scratch file.
  int scratchMaxOpens_;

  NABoolean preAllocateExtents_;

  // Units of 56kb blocks that logically considered as Io block.
  // This is used for merge reads to reduce disk seek time.
  int sortMergeBlocksPerBuffer_;

  NABoolean scratchDiskLogging_;

  int scratchIOBlockSize_;
  int scratchIOVectorSize_;

  ScratchOverflowMode ovMode_;

  NABoolean resizeCifRecord_;
  NABoolean considerBufferDefrag_;

  Float32 bmoCitizenshipFactor_;

  int pMemoryContingencyMB_;

  Float32 estimateErrorPenalty_;

  Float32 sortMemEstInKBPerNode_;

  UInt16 bmoMaxMemThresholdMB_;

  NABoolean intermediateScratchCleanup_;
};

#endif
