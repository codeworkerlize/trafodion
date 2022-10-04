

#ifndef COM_SPACE_H
#define COM_SPACE_H

#include <stddef.h>
#include "common/CollHeap.h"

class Block;
#include <iosfwd>
using namespace std;

/////////////////////////////////////////////////////
//
// class ComSpace
//
/////////////////////////////////////////////////////
typedef class ComSpace : public CollHeap {
  friend class ExHeapStats;   // Muse uses private data members
  friend class ExSpaceStats;  // Other muse class that needs private access
 public:
  enum SpaceType {
    EXECUTOR_SPACE,   // master executor and ESPs
    GENERATOR_SPACE,  // arkcmp, used to generate module file
    SYSTEM_SPACE,
    SINGLE_BLOCK_SPACE
  };

  Int32 operator==(const ComSpace &other) const { return this == &other; };

 private:
  // recommended/default size for block. Caller can allocate
  // a different size, though, and pass that to
  // the init method.
  SpaceType type_;

  // if this space is an EXECUTOR_SPACE or SYSTEM_SPACE, it is
  // allocated from parent_! If parent_ is NULL, it is allocated from
  // the system heap.

  // mar NAHeap * parent_;
  // mar
  // mar NB: parent_ is always a NAHeap*, but we avoid some really nasty
  // mar multiple-inheritance evilness if we store this data member as
  // mar the parent class (it'll do the right thing and call the proper
  // mar virtual method when needed)
  CollHeap *parent_;

  Block *firstBlock_;
  Block *lastBlock_;
  Block *searchList_;

  int initialSize_;

  NABoolean fillUp_;  // flag to indicate if we ever revisit a block to
                      // look for free space. The default is true. If
                      // fillUp_ is false, we can guarantee that objects
                      // in the space are always allocated in cronological
                      // order. ExStatsArea.formatStatistics() relies on
                      // this.
  // don't call this directly, as allocateAlignedSpace() won't work
  // if some of the requests are for non-aligned space
  char *privateAllocateSpace(ULng32 size, NABoolean failureIsFatal = TRUE);

  // allocate a block of the indicated length (must be a multiple
  // of 8)
  Block *allocateBlock(SpaceType type, int in_block_size = 0, NABoolean firstAllocation = FALSE,
                       char *in_block_addr = NULL, NABoolean failureIsFatal = TRUE);

 public:
  ComSpace(SpaceType type = SYSTEM_SPACE, NABoolean fillUp = TRUE, char *name = NULL);
  void destroy();

  ~ComSpace();

  void freeBlocks(void);

  void setType(SpaceType type, int initialSize = 0);

  void setFirstBlock(char *blockAddr, int blockLen, NABoolean failureIsFatal = TRUE);

  void setParent(CollHeap *parent);

  char *allocateAlignedSpace(size_t size, NABoolean failureIsFatal = TRUE);

  char *allocateAndCopyToAlignedSpace(const char *dp, size_t dlen, size_t countPrefixSize = 0,
                                      NABoolean failureIsFatal = TRUE, NABoolean noSizeAdjustment = FALSE);

  // returns total space allocated size (space allocated by the user)
  inline int getAllocatedSpaceSize() { return allocSize_; }

  void *allocateSpaceMemory(size_t size, NABoolean failureIsFatal = TRUE);

  void deallocateSpaceMemory(void *){/* no op */};

  Long convertToOffset(void *);

  void *convertToPtr(Long offset) const;

  int allocAndCopy(void *, ULng32, NABoolean failureIsFatal = TRUE);

  short isOffset(void *);

  NABoolean isOverlappingMyBlocks(char *buf, ULng32 size);

  // moves all the Blocks into the output contiguous buffer.
  char *makeContiguous(char *out_buf, ULng32 out_buflen);

#if (defined(_DEBUG) || defined(NSK_MEMDEBUG))
  void dumpSpaceInfo(ostream *outstream, int indent);
  void traverseAndCheckHostHeap(const char *msg);
#endif

  static void outputBuffer(ComSpace *space, char *buf, char *newbuf);

  static void display(char *buf, size_t buflen, size_t countPrefixSize, ostream &outstream);

  static int defaultBlockSize(SpaceType type);

 public:
  NABoolean outputbuf_;  // set to false until we use the buffer for output

} Space;

void *operator new(size_t size, ComSpace *s);

/////////////////////////////////////////////
//
// class Block
//
// A block is a contiguous space of bytes.
// The first sizeof(Block) bytes are the
// Block itself. The space following it is
// the dataspace.
// There is no constructor or destructor for
// this class since the caller allocates
// space from dp2 segment, executor segment
// or system memory. See class Space.
/////////////////////////////////////////////
class Block {
  int blockSize_;

  int maxSize_;  // max data space size
  int allocatedSize_;
  int freeSpaceSize_;
  int freeSpaceOffset_;

  char *dataPtr_;

  Block *nextBlock_;
  Block *nextSearchBlock_;  // list of searchable blocks

 public:
  void init(int block_size, int data_size, char *data_ptr);

  // allocate 'size' amount of space in this block
  char *allocateMemory(ULng32 size);

  NABoolean isOverlapping(char *buf, ULng32 size);

  inline int getAllocatedSize() { return allocatedSize_; };

  inline Block *getNext() { return nextBlock_; };

  inline int getFreeSpace() { return freeSpaceSize_; };

  inline int getBlockSize() { return blockSize_; };

  inline Block *getNextSearch() { return nextSearchBlock_; };

  inline void setNext(Block *b) { nextBlock_ = b; };

  inline void setNextSearch(Block *b) { nextSearchBlock_ = b; };

  inline char *getDataPtr() { return dataPtr_; };

  inline int getMaxSize() { return maxSize_; };
};

#endif
