// **********************************************************************

// ***********************************************************************
// *
// * File:         ExSimpleSQLBuffer.h
// * RCS:          $Id
// * Description:  ExSimpleSQLBuffer class declaration
// * Created:      6/1/97
// * Modified:     $Author
// * Language:     C++
// * Status:       $State
// *
// *
//
// *******************************************************************
#ifndef __ExSimpleSQLBuffer_h__
#define __ExSimpleSQLBuffer_h__

// Declaration of the ExSimpleSQLBuffer class and its private helper
// class ExSimpleSQLBufferEntry.  The ExSimpleSQLBuffer class is used
// to manage free and referenced tuples.  It is most commonly used as
// a tuple allocator in TCB work procedures.
//

#include "executor/ex_stdh.h"
#include "exp/ExpSqlTupp.h"

// Forward external declarations
//
class NAMemory;
class ExDupSqlBuffer;

// Forward internal declarations
//
class ExSimpleSQLBufferEntry;
class ExSimpleSQLBuffer;

// ExSimpleSQLBufferEntry
//
// This class is used exclusively by ExSimpleSQLBuffer and its derived
// classes.  A more elegant approach would be to define this class as
// a nested class within ExSimpleSQLBuffer, but historically some
// compilers could not handle nested classes at the time this class
// was implemented.  Instead, all members of this class are declared
// private and ExSimpleSQLBuffer is defined as a friend.
//
class ExSimpleSQLBufferEntry {
  friend class ExSimpleSQLBuffer;
  friend class ExDupSqlBuffer;

 private:
  // init - Initializes an entry by setting initializing the tupp
  // descriptor and setting the next field to NULL.
  //
  void init(int size) {
    tuppDesc_.init(size, 0, data_);
    next_ = 0;
  };

  // setNext - Sets the next_ field, which points to the next entry
  // in the linked list.
  //
  void setNext(ExSimpleSQLBufferEntry *next) { next_ = next; };

  // getNext - Returns the next_ field, which points to the next entry
  // in the linked list.
  //
  ExSimpleSQLBufferEntry *getNext() const { return next_; };

  // getDataPtr - Return a pointer to the tuple data at the end of the
  // ExSimpleSQLBufferEntry structure (variable length allocation).
  //
  char *getDataPtr() { return &data_[0]; };

  // getTupDesc - Return a pointer to the tupp_descriptor
  tupp_descriptor *getTupDesc() { return &tuppDesc_; };

 private:
  // tuppDesc_ - The tupp descriptor for this tuple. Is initialized in
  // init.
  //
  tupp_descriptor tuppDesc_;

  // next_ - The next pointer for maintaining linked lists of
  // ExSimpleSQLBufferEntrys.  The constructor sets this to NULL.  Users
  // must explicitly manipulate this field via the setNext() and getNext()
  // methods.
  //
  ExSimpleSQLBufferEntry *next_;

  // data_ - The begining of the tuple data.  When an
  // ExSimpleSQLBufferEntry is allocated, additional space is
  // allocated for the tuple data.  Initialization is done in the
  // ExSimpleSQLBuffer constructor.  The tuple data is accessible
  // through the getDataPtr() method.
  //
  char data_[1];
};

// ExSimpleSQLBuffer
//
class ExSimpleSQLBuffer : public ExGod {
 public:
  // ExSimpleSQLBuffer - constructor
  //
  ExSimpleSQLBuffer(int numTuples, int tupleSize, NAMemory *);

  // ExSimpleSQLBuffer - constructor specially designed for mapping
  // sql_buffer_pool construction calls to ExSimpleSQLBuffer calls.
  //
  ExSimpleSQLBuffer(int numBuffers, int bufferSize, int tupleSize, NAMemory *);

  // ~ExSimpleSQLBuffer - destructor
  //
  ~ExSimpleSQLBuffer();

  // getFreeTuple - sets <tp> to reference a free tuple from the buffer
  //
  int getFreeTuple(tupp &tp);

  // getNumTuples - returns the total number of tupps in the buffer
  //
  int getNumTuples() const { return numberTuples_; };

  // getTupleSize - returns the data size of each tupp in the buffer
  //
  int getTupleSize() const { return tupleSize_; };

  void reinitializeTuples(void);

 protected:
  // Get an entry from the free list.  Reclaim unreferenced
  // tuples if the free list is empty.
  ExSimpleSQLBufferEntry *getFreeEntry(void);

  ExSimpleSQLBufferEntry *getUsedList(void) { return usedList_; }

  void setUsedList(ExSimpleSQLBufferEntry *entry) { usedList_ = entry; }

 private:
  // Common constructor initialization code
  void init(CollHeap *heap);

  // Move unreferenced tuples from the used list to the free list.
  void reclaimTuples(void);

  // numberTuples_ - The total number of tuples in the buffer (free and used).
  // Set in the constructor and accesible using getNumTuples().
  //
  int numberTuples_;

  // tupleSize_ - The data size of each tuple in the buffer. Set in the
  // constructor and accesible using getTupleSize().
  //
  int tupleSize_;

  // allocationSize_ - The allocated size of each ExSimpleSQLBufferEntry.
  // Set in the constructor and never accessed afterwards.
  //
  int allocationSize_;

  // freeList_ - Linked list of free (unreferenced) tuples
  // usedList_ - Linked list of used (referenced) tuples
  //
  // When tuples are allocated they are removed from
  // the freeList_ and added to the usedList_. When the freeList_ is
  // empty, a pass over the usedList_ move all unreferenced tuples from
  // the usedList_ to the freeList_.
  //
  ExSimpleSQLBufferEntry *freeList_;
  ExSimpleSQLBufferEntry *usedList_;

  // data_ - Buffer memory allocated from the heap in the constructor.
  //
  char *data_;
};

#endif
