

#ifndef COMQUEUE_H
#define COMQUEUE_H

#include "export/NABasicObject.h"
#include "export/NAVersionedObject.h"

// ---------------------------------------------------------------------
// The PackQueue macros provide a way to really pack the "whole" queue.
// This includes the backbone of the queue as well as objects referenced
// by the (void *) in the Q_Entry's. The real class identity of those
// objects is passed as a parameter to the macros. Depending on whether
// those objects belong to a derived class of NAVersionedObject or not,
// either drivePack() or pack() will be called. Note that queuePtr has
// to be of type "QueuePtr".
// ---------------------------------------------------------------------

#define PackQueueOfNAVersionedObjects(queuePtr, space, Type)                         \
  if (queuePtr) {                                                                    \
    Type *entry;                                                                     \
    queuePtr->position();                                                            \
    while ((entry = (Type *)(queuePtr->getNext())) != NULL) entry->drivePack(space); \
    queuePtr.pack(space);                                                            \
  }

#define PackQueueOfNonNAVersionedObjects(queuePtr, space, Type)                 \
  if (queuePtr) {                                                               \
    Type *entry;                                                                \
    queuePtr->position();                                                       \
    while ((entry = (Type *)(queuePtr->getNext())) != NULL) entry->pack(space); \
    queuePtr.pack(space);                                                       \
  }

#define UnpackQueueOfNAVersionedObjects(queuePtr, base, Type, Reallocator) \
  if (queuePtr) {                                                          \
    if (queuePtr.unpack(base, reallocator)) return -1;                     \
    Type *entry;                                                           \
    queuePtr->position();                                                  \
    while ((entry = (Type *)(queuePtr->getNext())) != NULL) {              \
      Type obj;                                                            \
      entry->driveUnpack(base, &obj, reallocator);                         \
    }                                                                      \
  }

#define UnpackQueueOfNonNAVersionedObjects(queuePtr, base, Type) \
  if (queuePtr) {                                                \
    if (queuePtr.unpack(base)) return -1;                        \
    Type *entry;                                                 \
    queuePtr->position();                                        \
    while ((entry = (Type *)(queuePtr->getNext())) != NULL) {    \
      entry->fixupVTblPtr();                                     \
      entry->unpack(base);                                       \
    }                                                            \
  }

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for Q_Entry
// ---------------------------------------------------------------------
class Q_Entry;
typedef NAOpenObjectPtrTempl<Q_Entry> Q_EntryPtr;

class Q_Entry {
  friend class Queue;

  NABasicPtr entry;          // 00-07
  Q_EntryPtr prev;           // 08-15
  Q_EntryPtr next;           // 16-23
  UInt32 packedLength_;      // 24-27
  char fillersQ_Entry_[20];  // 28-47

 public:
  Q_Entry(void *entry_, Q_Entry *prev_, Q_Entry *next_);

  ~Q_Entry();

  Long pack(void *space);
  int unpack(void *base);

  int packedLength() { return packedLength_; }
};

class Queue : public NAVersionedObject {
  Q_EntryPtr head;  // 00-07
  Q_EntryPtr tail;  // 08-15
  Q_EntryPtr curr;  // 16-23

  CollHeap *heap_;

  int numEntries_;  // 32-35

  // length of queue in 'packed' (contiguous) form.
  // This field is updated whenever a new queue entry is added (removed).
  UInt32 packedLength_;  // 36-39

  UInt16 flags_;           // 40-41
  char fillersQueue_[14];  // 42-55

  enum {
    // if set, then remove doesn't deletes Q_Entry, only makes the
    // entry pointer NULL.
    // Insert is done at the first empty Q_Entry.
    // remove removes the head entry and moves it to the end
    // of the list.
    DO_SPACE_OPT = 0x0001
  };

 public:
  Queue();

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(Queue); }

  Queue(CollHeap *heap);

  ~Queue();

  // cleanup method; removes all entries
  void cleanup();

  // inserts at tail
  void insert(const void *entry_, int entryPackedLength = 0);

  // inserts at tail, returns addr of Queue entry inserted
  void insert(const void *entry_, int entryPackedLength, void **queueEntry);

  // returns the head entry
  void *get();

  // returns the i'th entry. 0 based. First entry is i = 0
  void *get(int i);

  // returns the head entry
  void *getHead();

  // returns the tail(last) entry
  void *getTail();

  // positions on the head entry
  void position();

  // positions on a specified Queue entry
  void position(void *queueEntry);

  // returns the current entry
  void *getCurr();

  // returns the current entry ptr
  void *getCurrEntryPtr();

  // advances the current entry
  void advance();

  // returns the current entry and advances curr
  void *getNext();

  // returns -1, if all entries have been returned.
  short atEnd();

  // removes the head entry
  void remove();

  // removes the head entry
  void removeHead() { remove(); };

  // removes the tail entry
  void removeTail();

  void removeWithSpaceOpt();

  // removes the 'entry' entry
  NABoolean remove(void *entry);

  // removes all entries starting
  // from 'entry' until the prev of curr.
  NABoolean removeStartingFrom(void *entry);

  virtual Long pack(void *space);

  virtual int unpack(void *base, void *reallocator);

  // returns -1, if queue is empty. Otherwise, returns 0.
  int isEmpty() { return ((numEntries() == 0) ? -1 : 0); }

  int numEntries() { return numEntries_; }

  int entries() { return numEntries(); }

  int packedLength() { return packedLength_; }

  void setDoSpaceOpt(short v) { (v ? flags_ |= DO_SPACE_OPT : flags_ &= ~DO_SPACE_OPT); };
  NABoolean doSpaceOpt() { return (flags_ & DO_SPACE_OPT) != 0; };
};

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for Queue
// ---------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<Queue> QueuePtr;

// A HashQueue is basically a container which is accessed by
// hashing. It is introduced here to speed up the statement
// and descriptor lookup in the sqlcli. The statement- and
// descriptor lists were originally implemented as Queues. Lookup
// was always just a sequential search in this list. Note
// that the following is a quick and dirty implementation
// to eliviate this performance problem. If we would only
// have more time (sigh), a better design would be a template
// based implementation of containers (lists, arrays, queues,
// stacks, you name it) for the executor similar to the collections
// in the compiler.

class HashQueueEntry {
  friend class HashQueue;

 public:
  HashQueueEntry(void *entry, HashQueueEntry *prev, HashQueueEntry *next, int hashValue)
      : entry_(entry), prev_(prev), next_(next), hashValue_(hashValue){};

  ~HashQueueEntry(){};

 private:
  HashQueueEntry *prev_;
  HashQueueEntry *next_;
  void *entry_;
  int hashValue_;
};

class HashQueue : public NABasicObject {
 public:
  HashQueue(CollHeap *heap, int hashTableSize = 513);

  HashQueue(const NABoolean shadowCopy, const HashQueue &other);

  ~HashQueue();

  int entries() { return entries_; }

  int size() { return hashTableSize_; }

  void insert(const char *data, int dataLength, void *entry);

  // position by hashing
  void position(const char *data, int dataLength);

  // position globally
  void position();

  void *getNext();

  // returns the current entry
  void *getCurr();

  // advances the current entry
  void advance();

  void remove(void *entry);

  // To remove the last returned entry via getNext() in case of global scan
  // of hash queue
  void remove();

  // To remove the entry by passing in the hashing fields and the corresponding entry
  void remove(const char *data, int dataLength, void *entry);

  int numEntries() { return (int)entries_; }

  NABoolean sequentialAdd() { return (flags_ & SEQUENTIAL_ADD) != 0; }

  void setSequentialAdd(NABoolean v) { (v ? flags_ |= SEQUENTIAL_ADD : flags_ &= ~SEQUENTIAL_ADD); }
  int isEmpty() { return ((entries_ == 0) ? -1 : 0); }

 private:
  enum Flags {
    // add hash entries in a sequential order (the same order in which
    // they are inserted).
    SEQUENTIAL_ADD = 0x0001
  };

  int entries_;                // number of entries in this HashQueue
  int hashTableSize_;          // size of the hash table
  HashQueueEntry **hashTable_;    // the hash table itself
  HashQueueEntry *lastReturned_;  // the last entry returned by getNext()
  HashQueueEntry *current_;       // points to the current entry
  int currentChain_;           // the chain were current_ is located
  int hashValue_;              // hash value of the last position
  NABoolean globalScan_;          // if true, getNext ignores hashValue
  CollHeap *heap_;                // the heap a HashQueue allocates from
  NABoolean shadowCopy_;          // TRUE if constructed by shadow copy

  int flags_;

  void getHashValue(const char *data, int dataLength);

  void removeLastReturned();
};

class SyncHashQueue : public HashQueue {
 public:
  SyncHashQueue(CollHeap *heap, int hashTableSize = 513);
  void position(const char *data, int dataLength);
  void position();
  void remove(void *entry);
  void remove();
  void remove(const char *data, int dataLength, void *entry);
  void insert(const char *data, int dataLength, void *entry);
};

#endif
