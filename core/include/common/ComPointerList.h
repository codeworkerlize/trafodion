
#ifndef COMPOINTERLIST_H
#define COMPOINTERLIST_H

#include "common/BaseTypes.h"
#include "common/Collections.h"
#include "common/NAMemory.h"

#define ARRAY_INITIAL_SIZE   0
#define ARRAY_INCREMENT_SIZE 16
template <class Elem>
class ComPointerList;
template <class Elem, class Key>
class ComOrderedSet;
template <class Elem, class Key>
class ComOrderedPointerSet;

// -------------------------------------------------------------------------------------
//
// Class ComPointerList
//
template <class Elem>
class ComPointerList {
 public:
  // Default constructor.
  // Methods of this class uses the heap for pointer array allocations only.
  ComPointerList(CollHeap *heap = NULL, const CollIndex initSize = ARRAY_INITIAL_SIZE,
                 const CollIndex increment = ARRAY_INCREMENT_SIZE);
  virtual ~ComPointerList();

  // Accessors
  CollIndex entries(void) const { return inUse_; };

  // Operators
  Elem &operator[](const CollIndex index);
  const Elem &operator[](const CollIndex index) const;

  // Mutators

  // Insert element at specified position.
  virtual void insertAt(const Elem &element, const CollIndex index);  // Insert the element pointer into the array

  // Insert element pointer at the end. Declared as NABoolean so that ComOrderedSet can override.
  // Will unconditionally return TRUE.
  virtual NABoolean insert(const Elem &element, CollIndex &position);  // Return position of inserted element
  virtual NABoolean insert(const Elem &element);

  // Element removal.
  virtual void remove(const CollIndex index);  // Remove a single element pointer
  virtual void clear(void);                    // Remove all element pointers

 protected:
  CollHeap *heap_;   // User defined heap or NULL
  CollIndex inUse_;  // Number of elements in use

 private:
  // Copy constructor, don't implement
  ComPointerList(const ComPointerList<Elem> &rhs);

  Elem **arr_;           // Array of pointers to elements
  CollIndex allocated_;  // Number of allocated elements.
  CollIndex increment_;  // The size of the chunk for allocation

  void abortIfOutOfBounds(const CollIndex index) const;  // Aborts if index is out of bounds
};

// -------------------------------------------------------------------------------------
//
// Class ComOrderedPointerSet
//

template <class Elem, class Key>
class ComOrderedPointerSet : public ComPointerList<Elem> {
 public:
  // Default constructor.
  // Methods of this class don't use the heap.
  ComOrderedPointerSet(CollHeap *heap = NULL, const CollIndex initSize = ARRAY_INITIAL_SIZE,
                       const CollIndex increment = ARRAY_INCREMENT_SIZE)
      : ComPointerList<Elem>(heap, initSize, increment), lastInserted_(0){};

  virtual ~ComOrderedPointerSet(){};

  // Accessors
  CollIndex find(const Key &key) const;  // Find an element that may be in the list

  // Mutators

  // Insert element pointer in the order specified by the element class' comparison operators.
  // Return TRUE if the element was inserted, FALSE if it exists in the set already.
  // Maintains lastInserted_. These two methods are intended for the situation where the element
  // already has been constructed
  virtual NABoolean insert(const Elem &element, CollIndex &position);  // Return position of inserted element
  virtual NABoolean insert(const Elem &element);

  // The next two methods are intended for the situation where the element will not be
  // constructed if it is in the set already. insertAt should never be called without a
  // preceding call to findPositionToInsert.
  NABoolean findPositionToInsert(CollIndex &position,
                                 const Key &key) const;  // Return TRUE if element is there already, FALSE if not.
  virtual void insertAt(const Elem &element,
                        const CollIndex index);  // Insert the element pointer into the array, maintain lastInserted_

  // Insert all elements from another pointer set.
  void merge(const ComOrderedPointerSet<Elem, Key> &sourceList);

  // Element removal.
  virtual void remove(const CollIndex index);  // Remove a single element pointer
  virtual void clear(void);                    // Remove all element pointers

 private:
  // Copy constructor, don't implement
  ComOrderedPointerSet(const ComOrderedPointerSet<Elem, Key> &rhs);

  CollIndex lastInserted_;  // The index of the last inserted element. Ordering optimisation.
};

//--------------------------------------------------------------------
//
// Class ComOrderedSet

template <class Elem, class Key>
class ComOrderedSet : public ComOrderedPointerSet<Elem, Key> {
 public:
  // Default constructor
  ComOrderedSet(CollHeap *heap = NULL, const CollIndex initSize = ARRAY_INITIAL_SIZE,
                const CollIndex increment = ARRAY_INCREMENT_SIZE)
      : ComOrderedPointerSet<Elem, Key>(heap, initSize, increment){};

  virtual ~ComOrderedSet(void);

  // Mutators

  // Element removal.
  virtual void remove(const CollIndex index);  // Remove and deallocate a single entry
  virtual void clear(void);                    // Remove and deallocate all entries

  // Element insertion. Insert a pointer to a copy of the element
  virtual NABoolean insert(const Elem &element, CollIndex &position);  // Return position of inserted element
  virtual NABoolean insert(const Elem &element);

  // Insert a copy of each element from another ordered set.
  inline void merge(const ComOrderedSet<Elem, Key> &sourceList);

 private:
  // Copy constructor, don't implement
  ComOrderedSet(const ComOrderedSet<Elem, Key> &rhs);
};

// -----------------------------------------------------------------------
// This is done similarly to Tools.h++: if we want to instantiate
// templates at compile time, the compiler needs to know the
// implementation of the template functions. Do this by setting the
// preprocessor define NA_COMPILE_INSTANTIATE.
// -----------------------------------------------------------------------
#ifdef NA_COMPILE_INSTANTIATE

// -------------------------------------------------------------------------------------
//
// Class ComPointerList methods
//
template <class Elem>
ComPointerList<Elem>::ComPointerList(CollHeap *heap, const CollIndex initSize, const CollIndex increment)
    : heap_(heap), increment_(increment), allocated_(initSize), inUse_(0), arr_(NULL) {
  if (initSize > 0) arr_ = new (heap_) Elem *[initSize];
}

template <class Elem>
ComPointerList<Elem>::~ComPointerList(void) {
  // It is the responsibility of the instantiator to deallocate
  // what is pointed to.
  if (allocated_ > 0) NADELETEBASIC(arr_, heap_);
}

// Operators
template <class Elem>
Elem &ComPointerList<Elem>::operator[](const CollIndex index) {
  abortIfOutOfBounds(index);
  return *(arr_[index]);
}

template <class Elem>
const Elem &ComPointerList<Elem>::operator[](const CollIndex index) const {
  abortIfOutOfBounds(index);
  return *(arr_[index]);
}

// Insert pointer at specific position
template <class Elem>
void ComPointerList<Elem>::insertAt(const Elem &element  // Insert the pointer
                                    ,
                                    const CollIndex index)  // at the specified position
{
  // Increase inUse_ up front, to prevent an out of bounds condition
  // on insert at the end
  inUse_++;

  abortIfOutOfBounds(index);
  // First, check if we need to allocate more room
  if (inUse_ > allocated_) {
    // all allocated entries (if any) are in use, get some more
    allocated_ += increment_;
    Elem **newArr = new (heap_) Elem *[allocated_];

    // copy existing element pointers to new array
    if (inUse_ > 1) memcpy(newArr, arr_, ((inUse_ - 1) * sizeof(*newArr)));

    // remove old array and store pointer to new array
    if (allocated_ > increment_) NADELETEBASIC(arr_, heap_);
    arr_ = newArr;
  }

  // Move subsequent elements one up
  for (CollIndex i = inUse_ - 1; i > index; i--) arr_[i] = arr_[i - 1];

  // Insert element
  arr_[index] = (Elem *)&element;
}

template <class Elem>
NABoolean ComPointerList<Elem>::insert(const Elem &element, CollIndex &position) {
  // Insert at the end
  position = inUse_;
  insertAt(element, inUse_);
  return TRUE;
}

template <class Elem>
NABoolean ComPointerList<Elem>::insert(const Elem &element) {
  // Insert at the end
  CollIndex dummyPosition;
  return insert(element, dummyPosition);
}

// Removal
template <class Elem>
void ComPointerList<Elem>::remove(const CollIndex index)  // Remove a pointer
{
  abortIfOutOfBounds(index);

  CollIndex i;

  // decrease used space
  inUse_--;

  // Move subsequent entries one down
  for (i = index; i < inUse_; i++) arr_[i] = arr_[i + 1];

  // See if tail chunk needs to be deallocated.
  // We will not deallocate the last chunk. However,
  // the logic does not preserve an initial allocation
  // greater than the increment.
  if ((allocated_ == inUse_ + increment_) && (allocated_ > increment_)) {
    allocated_ -= increment_;
    Elem **newArr = new (heap_) Elem *[allocated_];

    // copy existing element pointers to new array
    for (i = 0; i < inUse_; i++) newArr[i] = arr_[i];

    // remove old array and store pointer to new array
    NADELETEBASIC(arr_, heap_);
    arr_ = newArr;
  }
}

template <class Elem>
void ComPointerList<Elem>::clear(void)  // Remove all pointers
{
  // remove old array
  NADELETEBASIC(arr_, heap_);

  // allocate minimum array
  allocated_ = increment_;
  arr_ = new (heap_) Elem *[allocated_];

  // clear in-use
  inUse_ = 0;
}

template <class Elem>
void ComPointerList<Elem>::abortIfOutOfBounds(const CollIndex index) const {
  if (index >= inUse_)
    // Programming error - eat dirt!
    ABORT("Index exceeds # of entries");
}

// -------------------------------------------------------------------------------------
//
// Class ComOrderedPointerSet methods
//
// Find an element that may be in the list.
template <class Elem, class Key>
CollIndex ComOrderedPointerSet<Elem, Key>::find(const Key &key) const {
  CollIndex index;

  if (findPositionToInsert(index, key))
    // It's there, return its position
    return index;
  else
    // It's not there ...
    return NULL_COLL_INDEX;
}

// Find position to insert element. Return TRUE if element is there already,
// FALSE if not. Requires element classes to define comparison operators.
template <class Elem, class Key>
NABoolean ComOrderedPointerSet<Elem, Key>::findPositionToInsert(CollIndex &position, const Key &key) const {
  position = 0;
  if (this->inUse_) {
    // Only go searching if set contains elements.
    // Optimisation: Start searching from last inserted position.
    // Saves searching if inserts are ordered in advance.
    if (key >= (*this)[lastInserted_].getKey()) position = lastInserted_ + 1;

    while (position < this->inUse_) {
      if (key < (*this)[position].getKey())
        // found position to insert, quit
        break;

      // try next element
      position++;
    }
  }

  if ((position) && (key == (*this)[position - 1].getKey())) {
    // Element is in set already. Return the position of the element.
    position--;
    return TRUE;
  } else {
    return FALSE;
  }
}

// Mutators

// Insert the element pointer at the correct position
template <class Elem, class Key>
NABoolean ComOrderedPointerSet<Elem, Key>::insert(const Elem &element, CollIndex &position) {
  // Return if element is there already
  if (findPositionToInsert(position, element.getKey())) return FALSE;

  // OK, it isn't there - insert it at the found position
  insertAt(element, position);
  return TRUE;
}

template <class Elem, class Key>
NABoolean ComOrderedPointerSet<Elem, Key>::insert(const Elem &element) {
  CollIndex dummyPosition;
  return insert(element, dummyPosition);
}

template <class Elem, class Key>
void ComOrderedPointerSet<Elem, Key>::merge(const ComOrderedPointerSet<Elem, Key> &sourceSet) {
  for (CollIndex i = 0; i < sourceSet.entries(); i++) insert(sourceSet[i]);
}

template <class Elem, class Key>
void ComOrderedPointerSet<Elem, Key>::insertAt(const Elem &element, const CollIndex index) {
  ComPointerList<Elem>::insertAt(element, index);
  lastInserted_ = index;
}

// Removal
template <class Elem, class Key>
void ComOrderedPointerSet<Elem, Key>::remove(const CollIndex index)  // One element
{
  ComPointerList<Elem>::remove(index);

  // Invalidate lastInserted_ if needed
  if (this->inUse_ == 0)
    lastInserted_ = 0;
  else if (lastInserted_ >= this->inUse_)
    lastInserted_ = this->inUse_ - 1;
}

template <class Elem, class Key>
void ComOrderedPointerSet<Elem, Key>::clear(void)  // All elements
{
  ComPointerList<Elem>::clear();
  lastInserted_ = 0;
}

//--------------------------------------------------------------------
//
// Class ComOrderedSet methods

template <class Elem, class Key>
ComOrderedSet<Elem, Key>::~ComOrderedSet(void) {
  // Deallocate all elements
  for (CollIndex index = 0; index < this->inUse_; index++)
    if (this->heap_)
      NADELETE(&((*this)[index]), Elem, this->heap_);
    else
      delete &((*this)[index]);
}

// Insert a pointer to a copy of an element.
template <class Elem, class Key>
NABoolean ComOrderedSet<Elem, Key>::insert(const Elem &element, CollIndex &position) {
  if (ComOrderedPointerSet<Elem, Key>::findPositionToInsert(position, element.getKey()))
    // Element is in set already
    return FALSE;

  const Elem *copyOfElement = new (this->heap_) Elem(element);
  ComPointerList<Elem>::insertAt(*copyOfElement, position);
  return TRUE;
}

template <class Elem, class Key>
NABoolean ComOrderedSet<Elem, Key>::insert(const Elem &element) {
  CollIndex dummyPosition;
  return insert(element, dummyPosition);
}

template <class Elem, class Key>
void ComOrderedSet<Elem, Key>::merge(const ComOrderedSet<Elem, Key> &sourceSet) {
  for (CollIndex i = 0; i < sourceSet.entries(); i++) insert(sourceSet[i]);
}

// Remove an entry, deallocate the element pointed to.
template <class Elem, class Key>
void ComOrderedSet<Elem, Key>::remove(const CollIndex index) {
  Elem &element = (*this)[index];

  // remove and deallocate the element
  ComOrderedPointerSet<Elem, Key>::remove(index);
  if (this->heap_)
    NADELETE(&element, Elem, this->heap_);
  else
    delete &element;
}

// Remove all entries, deallocate all elements.
template <class Elem, class Key>
void ComOrderedSet<Elem, Key>::clear(void) {
  // Deallocate all elements
  for (CollIndex index = 0; index < this->inUse_; index++)
    if (this->heap_)
      NADELETE(&((*this)[index]), Elem, this->heap_);
    else
      delete &((*this)[index]);

  // Clear pointer array
  ComOrderedPointerSet<Elem, Key>::clear();
}

#endif

#endif  // COMPOINTERLIST_H
