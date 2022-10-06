

#include "common/Platform.h"
#include "DiskPool.h"
#include "common/ComDistribution.h"

#include <assert.h>
#if !defined(FORDEBUG) && !defined(NDEBUG)
#define NDEBUG
#endif

//--------------------------------------------------------------------------
// DiskPool()
//    The constructor.
//--------------------------------------------------------------------------
DiskPool::DiskPool(CollHeap *heap)
    : numberOfDisks_(0),
      numberOfLocalDisks_(0),
      diskTablePtr_(NULL),
      localDisksPtr_(NULL),
      currentDisk_(-1),
      scratchSpace_(NULL),
      heap_(heap) {}

//--------------------------------------------------------------------------
//~DiskPool()
//    The destructor, it will destroy array of DiskDetailsPtr if it
//    is created.
//--------------------------------------------------------------------------
DiskPool::~DiskPool() {
  // This dtor is pure-virtual: The derived classes would do the deletions.
  //  NADELETEARRAY(diskTablePtr_, numberOfDisks_, DiskDetailsPtr, heap_ );
  diskTablePtr_ = NULL;
  localDisksPtr_ = NULL;
}
