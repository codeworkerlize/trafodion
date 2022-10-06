
#ifndef NAHEAP__H
#define NAHEAP__H

#include "common/ComSpace.h"

#include <iosfwd>
using namespace std;


class NASpace : public CollHeap {
 public:
  NASpace(Space::SpaceType t = Space::SYSTEM_SPACE);

  virtual ~NASpace();

  virtual void *allocateMemory(size_t size, NABoolean failureIsFatal = TRUE);

  virtual void deallocateMemory(void *) {
#ifdef NAHEAP__DEBUG
    cout << "NASpace::deallocateMemory()\n";
#endif
  }

#if (defined(_DEBUG) || defined(NSK_MEMDEBUG))
  virtual void dump(ostream *outstream, int indent){};
#else
  inline void dump(void *outstream, int indent) {}
#endif

 private:
  NASpace(const NASpace &);
  NASpace &operator=(const NASpace &);

  Space s_;
};  // end of NASpace

#endif
