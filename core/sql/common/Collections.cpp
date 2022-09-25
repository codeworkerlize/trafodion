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
/* -*-C++-*-
**************************************************************************
*
* File:         Collections.C
* Description:  Implementation for Collection type templates
* Created:      4/27/94
* Language:     C++
*
*
*
**************************************************************************
*/
 
// -----------------------------------------------------------------------
// functions for the NACollection<T> template
// -----------------------------------------------------------------------
#include "Platform.h"

  #include "Collections.h"

#include "fixupVTable.h"


template <class T> NACollection<T>::~NACollection()
{
  deallocate();
}

// For HSC, define these methods as inline methods in the Collections.h header file.

template <class T>
void NACollection<T>::copy(const NACollection<T> &other) 
{
  // allocate the arrays
  // heap_       = other.heap_; //!!!do not change the value of a collection's heap_!!!
  allocate(other.maxLength_);
  usedLength_ = other.usedLength_;
  entries_    = other.entries_;
  failureIsFatal_= other.failureIsFatal_;
  isComplete_= other.isComplete_;
  
  // copy the entries
  for (CollIndex i = FIRST_COLL_INDEX; i < usedLength_; i++)
    {
      arr_[i] = other.arr_[i];
      usages_[i]  = other.usages_[i];
    }
}


template <class T>
void NACollection<T>::insert(CollIndex posToInsert,
			     const T   &newElement,
			     CollIndex newUsage,
                             NABoolean*  inserted)
{
  // is a valid position and usage given?
  assert((posToInsert < MAX_COLL_INDEX) AND
	 (newUsage    != UNUSED_COLL_ENTRY));
  
  // do we need to increase the size?
  if (posToInsert >= maxLength_) {
    CollIndex existingSize = getSize();

    resize(posToInsert + 1);

    CollIndex newSize = getSize();

    if ( existingSize == newSize ) {

      if ( inserted )
        *inserted = FALSE;
      return ;
    }
  }
  
  // is the new position in the unused portion?
  if (posToInsert >= usedLength_)
    {
      // extend the used portion by filling in the usages_
      // entries with UNUSED_COLL_ENTRY values
      for (CollIndex i = usedLength_; i <= posToInsert; i++)
	{
	  usages_[i] = UNUSED_COLL_ENTRY;
	}
      usedLength_ = posToInsert + 1;
    }
  
  // overwrite or insert?
  if (usages_[posToInsert] == UNUSED_COLL_ENTRY)
    entries_++;
  
  // ok, now we're ready to insert the new element
  usages_[posToInsert]  = newUsage;
  arr_[posToInsert] = newElement;
  
  if ( inserted )
    *inserted = TRUE;

  return;
}

template <class T>
CollIndex NACollection<T>::resize(CollIndex newSize)
{
  if (newSize == maxLength_ OR
      newSize < usedLength_)
    // no need to resize or impossible to resize
    return maxLength_;
  else
    {
      
      // increment the size in larger chunks to avoid
      // many expensive resize calls
      if (newSize > maxLength_ AND
	  newSize/3 < (maxLength_+2)/2)
	{
	  newSize = 3 * (maxLength_+2)/2;
	}
  
      // shouldn't even come close to this
      assert (newSize < MAX_COLL_INDEX);

      // use a temp collection with the new size
      NACollection<T> newOne(heap_,newSize, failureIsFatal_);

      if ( newOne.isComplete() ) {
    
         assert(newSize >= usedLength_);
   
         for (CollIndex i = FIRST_COLL_INDEX; i < usedLength_; i++)
   	 {
   	   newOne.usages_[i] = usages_[i];
   	   if (usages_[i] != UNUSED_COLL_ENTRY)
             {
                newOne.arr_[i] = arr_[i];
             }
         }
         
         // now just deallocate the old arrays and take the new
         // arrays and maxLength_
         deallocate();
         usages_        = newOne.usages_;
         arr_           = newOne.arr_;
         maxLength_     = newOne.maxLength_;
         
         // make sure the destructor of newOne won't mess things up
         newOne.usages_     = NULL;
         newOne.arr_        = NULL;
         newOne.usedLength_ = 0;
         newOne.maxLength_  = 0;
         
      }
      // If we reach here because of incompleteness during the construction of
      // newOne, we just return the existing maxLength_. The caller should 
      // make an attempt to verify this fact and act accordingly.
      return maxLength_;
    }
}

#ifdef __GNUC__
#  if __GNUC__ * 100 + __GNUC_MINOR__ >= 404
     // push_options added in 4.4
#pragma GCC push_options
     // GCC prior to 4.4 did not complain about this
     // need to ignore uninitialized for this statement:
     //   T temp;
#    pragma GCC diagnostic ignored "-Wuninitialized"
#  if __GNUC__ * 100 + __GNUC_MINOR__ >= 408
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#  endif
#  endif
#endif
template <class T> void NACollection<T>::allocate(CollIndex initLen)
{
  // NOTE: this assumes that the heap_ data member has been set.
  // No other data members need to be set before calling this.
  assert(initLen < MAX_COLL_INDEX);
 
  maxLength_  = initLen;
  usedLength_ = 0;
  entries_    = 0;
  isComplete_ = TRUE;
  if (maxLength_ > 0)
    {
      // Since new[] and new are different operators, the following
      // can not be combined, even that in new(size_t, NAMemory*)
      // used the ::operator new if NAMemory* is 0.
      if ( heap_ == NABasicObject::systemHeapPtr() )
	{
	  arr_ = new T[maxLength_];
	  usages_ = new CollIndex[maxLength_];
	}
      else
	{	
	  // The method for dynamic allocation should match the one in 
	  // deallocate. When the compiler supports the feature to overload
	  // new[] and delete[], the following two lines should be used
	  // instead.
	  //arr_ = new(heap_) T[maxLength_];
	  //usages_ = new(heap_) CollIndex[maxLength_];

	  arr_ = (T *)heap_->allocateMemory(sizeof(T) * ((size_t) maxLength_), failureIsFatal_);

          if ( !arr_ ) {
            isComplete_ = FALSE;
	     usages_ = NULL;
          } else {
	     usages_ = (CollIndex *) heap_->allocateMemory(
                  sizeof(CollIndex) * ((size_t) maxLength_), failureIsFatal_);

             if ( !usages_ ) {
               isComplete_ = FALSE;
               deallocate();
             }
          }

          // To finish up, we copy uninitialized objects of type T into
          // the newly-alloc'd space, so vtbl-pointers get set properly.
          // (This is not always necessary, but it's a good idea in
          // general!)
          if ( isComplete_ ) {
             T temp; 
             for ( CollIndex i = 0 ; i < maxLength_ ; i++ )
               memcpy ((void*)&arr_[i], (void*)&temp, sizeof(T)) ;
          }
        }
    }
  else
    {
      arr_    = NULL;
      usages_ = NULL;
    }
}

#ifndef __aarch64__
#ifdef __GNUC__
#  if __GNUC__ * 100 + __GNUC_MINOR__ >= 404
     // pop_options added in 4.4
#    pragma GCC pop_options
#  endif
#endif
#endif


template <class T> void NACollection<T>::clearFrom( CollIndex entry )
{
  assert( entry >= FIRST_COLL_INDEX );

  for (CollIndex i = entry; i < usedLength_; i++)
    usages_[i] = UNUSED_COLL_ENTRY;

  entries_    = entry;
  usedLength_ = entry;
}

template <class T> T & NACollection<T>::rawEntry(CollIndex ix)
{
  // resize, if index lies out of the allocated portion
  if (ix >= maxLength_)
    resize(ix + 1);
  
  // adjust used part to contain ix
  if (ix >= usedLength_)
    {
      for (CollIndex i = usedLength_; i <= ix; i++)
	usages_[i] = UNUSED_COLL_ENTRY;
      usedLength_ = ix + 1;
    }
  
  // if ix refers to a new entry, initialize its usage
  if (usages_[ix] == UNUSED_COLL_ENTRY)
    {
      entries_++;
      usages_[ix] = NULL_COLL_INDEX;
    }
  
  return arr_[ix];
}


template <class T>
CollIndex NACollection<T>::findUsage(const CollIndex &toFind)
{
  for (CollIndex i = FIRST_COLL_INDEX; i < usedLength_; i++)
    {
      if (usages_[i] == toFind)
	return i;
    }
  
  // return a "not found" indicator
  return NULL_COLL_INDEX;
}

template <class T>
NABoolean NACollection<T>::sanityCheck()
{
   // The following IF condition checks whether usages_ 
   // is corrupted.
   //
   // In M13883 ([DDL&DML] sometimes drop table return ERROR[8838] and 
   // ERROR [2005],then insert data will core dump and generate 3 
   // core files), usages_ = 0x2 and heap_ = 0x20000120.
   if ( usages_ && (void*)usages_ < (void*)heap_ )
     return FALSE;

   return TRUE;
}

// For HSC, define these methods as inline methods in the Collections.h header file.

// -----------------------------------------------------------------------
// functions for the NASubCollection<T> template
// -----------------------------------------------------------------------

template <class T>
NASubCollection<T>::NASubCollection (NACollection<T> *superset,
                                     NAMemory *heap,
                                     NABoolean failureIsFatal
                                    )
     : builtin_  ( TRUE ),
       entries_  ( 0 ),
       heap_     ( (heap==NULL && superset!=NULL) ? superset->heap_ : heap ),
       maxLength_( BuiltinSubsetWords ),
       superset_ ( superset ),
       wordSize_ ( BuiltinSubsetWords ),
       lastStaleBit_(0),
       failureIsFatal_ (failureIsFatal),
       isComplete_(TRUE)
   {
   CollIndex    i     = BuiltinSubsetWords;
   WordAsBits * pBits = (WordAsBits*)(&sbits_[ 0 ]);

   pBits_ = pBits;

   do
      {
      *pBits++ = 0x0;
      }
   while (--i);
   }

template <class T>
NASubCollection<T>::NASubCollection( const NASubCollection<T> &other,
                                     NAMemory * heap
                                   ) 
     : builtin_  ( other.builtin_ ),
       entries_  ( other.entries_ ),
       heap_     ( (heap==NULL) ? other.heap_ : heap ),
       maxLength_( other.maxLength_ ),
       superset_ ( other.superset_ ),
       wordSize_ ( other.wordSize_ ),
       lastStaleBit_( other.lastStaleBit_ ),
       failureIsFatal_( other.failureIsFatal_ ),
       isComplete_( other.isComplete_ )
   {
   CollIndex    i          = maxLength_;
   WordAsBits * pBits;
   WordAsBits * pOtherBits = other.pBits_;

   if (builtin_)
      pBits_ = (WordAsBits*)(&sbits_[ 0 ]);
   else
      pBits_ = allocateBits( maxLength_ );

   pBits = pBits_;

   do
      {
      *pBits++ = *pOtherBits++;
      }
   while (--i);
   }

template <class T>
NASubCollection<T>::~NASubCollection()
{
  if (!builtin_)
    deallocateBits();
}

template <class T>
void NASubCollection<T>::setHeap(CollHeap *heap)
   {
   assert( builtin_ );
   heap_ = heap;
   }

template <class T>
NABoolean NASubCollection<T>::contains( const NASubCollection<T> & other ) const
   {
   assert( superset_ == other.superset_ );

   if (other.entries_)
      {
      CollIndex    otherWords  = other.wordSize_;
      CollIndex    commonWords = MINOF( wordSize_, otherWords );
      CollIndex    i           = commonWords;
      WordAsBits * pBits       = pBits_;
      WordAsBits * pOtherBits  = other.pBits_;
      WordAsBits   w;

      if (i)
         {
         do
            {
            w = *pBits++;

            // for a word present in both, "other" mustn't have bits on that
            // are off in word(i) (in other words, when ORing the two words,
            // other must not cause any changes by having additional bits set)

            if (w != (w LOR *pOtherBits++))
	              return( FALSE );
            }
         while (--i);
         }

      i = otherWords - commonWords;

      if ((Lng32) i > 0)
         {
         do
            {
            // if "other" has any bits set that "this" doesn't have, return FALSE
            if (*pOtherBits++)
               return( FALSE );
            }
         while (--i);
         }
      }

   return( TRUE );
   }


template <class T>
NABoolean NASubCollection<T>::lastUsed(CollIndex &lastBit) const
  {
  CollIndex lastNonZeroWord = getWordSize() - 1;

  // find last non-zero word -- the > 0 test is there to avoid wrapping
  // in case CollIndex is an unsigned integer
  while ((lastNonZeroWord > 0) && (word(lastNonZeroWord) == 0))
    {
      lastNonZeroWord--;
    }

  // at this point, we either found the last non-zero word or there
  // are none

  if (word(lastNonZeroWord) == 0)
    return FALSE;  // return; there are none

  // we know we have the last non-zero word; find last set bit

  lastBit = 
    (lastNonZeroWord << LogBitsPerWord) + BitsPerWord - 1;

  while (!testBit(lastBit))
    {
      lastBit--;
    }
  
  return TRUE;  // found the last set bit
  }

template <class T>
NABoolean NASubCollection<T>::operator == ( const NASubCollection<T> & other ) const
   {
   assert( superset_ == other.superset_ );

   if (other.entries_ != entries_)
      return( FALSE );
   else
      {
      if (other.entries_ == 0 && entries_ == 0)
         return( TRUE );
      }

   CollIndex    commonWords = MINOF( wordSize_, other.wordSize_ );
   CollIndex    i           = commonWords;
   WordAsBits * pBits       = pBits_;
   WordAsBits * pOtherBits  = other.pBits_;

   if (i)
      {
      //
      // compare the common words.
      //
      do
         {
         if (*pBits++ != *pOtherBits++)
            return( FALSE );
         }
      while (--i);
      }

   // if one of the bitmap arrays is longer, then it must contain zeroes.

   i = wordSize_ - commonWords;

   if ((Lng32) i > 0)
      {
      do
         {
	 if (*pBits++)
	    return( FALSE );
         }
      while (--i);
      }
   else
      {
      i = other.wordSize_ - commonWords;

      if ((Lng32) i > 0)
         {
         do
            {
            if (*pOtherBits++)
               return( FALSE );
            }
         while (--i);
         }
      }

   return( TRUE );
   }

template <class T>
NASubCollection<T> & NASubCollection<T>::addSet( const NASubCollection<T> & other )
   {
   CollIndex maxWords   = other.wordSize_;

   assert( superset_ == other.superset_ );

   if (other.entries_)
      {
      if (wordSize_ < maxWords)
         extendWordSize( maxWords );

      if (maxWords)
         {
         WordAsBits * pBits      = pBits_;
         WordAsBits * pOtherBits = other.pBits_;

         if (entries_ == 0)  // We can skip computing the entries.
            {
            do
               {
               *pBits++ |= *pOtherBits++;
               }
            while (--maxWords);

            entries_ = other.entries_;
            }
         else
            {
            Lng32 entryCount    = 0; 
            Lng32 trailingWords = (Lng32) (wordSize_ - maxWords); 

            do
               {
               *pBits |= *pOtherBits++;

               entryCount += ones( *pBits++ );
               }
            while (--maxWords);

            while (trailingWords-- > 0)
               {
               entryCount += ones( *pBits++ );
               }
 
            entries_ = entryCount;
            }
         }
      }
   if (other.lastStaleBit_ > lastStaleBit_)
     lastStaleBit_ = other.lastStaleBit_;

   return( *this );
   }

template <class T>
NASubCollection<T> & NASubCollection<T>::intersectSet( const NASubCollection<T> & other )
   {
   assert( superset_ == other.superset_ );

   if (entries_)
      {
      CollIndex commonWords = MINOF( wordSize_, other.wordSize_ );
      CollIndex i           = commonWords;
      WordAsBits * pBits    = pBits_;

      if (i)
         {
         WordAsBits * pOtherBits = other.pBits_;

        if (other.entries_)
            {
            Lng32 entryCount = 0;

            do
               {
               *pBits &= *pOtherBits++;

               entryCount += ones( *pBits++ );
               }
            while (--i);

            entries_ = entryCount;
            }
         else
            {
            do
               {
               *pBits++ = 0x0;
               }
            while (--i);

            entries_ = 0;
            }
         }

      i = wordSize_ - commonWords;

      if ((Lng32) i > 0)
         {
         do
            {
            *pBits++ = 0x0;
            }
         while (--i);
         }
      }

   return( *this );
   }


template <class T>
NASubCollection<T> & NASubCollection<T>::subtractSet( const NASubCollection<T> & other )
   {
   assert( superset_ == other.superset_ );

   if (other.entries_ && entries_)
      {
      CollIndex commonWords = MINOF( wordSize_, other.wordSize_ );

      if (commonWords)
         {
         Lng32         entryCount    = 0;
         CollIndex    trailingWords = wordSize_ - commonWords;
         WordAsBits * pBits         = pBits_;
         WordAsBits * pOtherBits    = other.pBits_;

         do
            {
            *pBits &= LNOT *pOtherBits++;

            entryCount += ones( *pBits++ );
            }
         while (--commonWords);

         while (trailingWords-- > 0)
            {
            entryCount += ones( *pBits++ );
            } 
         entries_ = entryCount;
         }
      }

   return( *this );
   }


template <class T>
WordAsBits NASubCollection<T>::hash() const
   {
   CollIndex    i      = wordSize_;
   WordAsBits   result = 0x0;
   WordAsBits * pBits  = pBits_;

   if (i && entries_)
      {
      do
         {
         result ^= *pBits++;
         }
      while (--i);
      }

   return( result );
   }

template <class T>
NASubCollection<T> & NASubCollection<T>::complement()
   {
  // can only take the complement of a subset
  assert(superset_ != NULL);

  CollIndex maxWords;
  CollIndex superSetSize = superset_->getSize();

  resize(superSetSize);
  maxWords = getWordSize();

  // for each used entry in the superset, toggle the corresponding subset bit
  for (Int32 i = 0; i < (Int32) superSetSize; i++)
    {
      if (superset_->getUsage(i) == UNUSED_COLL_ENTRY)
	{
	  // a subset shouldn't have an element that's not part of
	  // the superset
	  assert(NOT testBit(i));
	}
      else
	{
	  // is the element in the subset
	  if (testBit(i))
	    // yes, then delete it
	    subtractElement(i);
	  else
	    // no, then add it
	    addElement(i);
	}
    }
  
  return *this;
  }

// -----------------------------------------------------------------------
// functions for the NASet<T> template
// -----------------------------------------------------------------------

template <class T>
NASet<T>::~NASet()
{}

template <class T>
T & NASet<T>::operator [] (CollIndex i)
{
  CollIndex userIndex;
  CollIndex arrayIndex;

  if (i >= this->entries())
    ABORT("Set index exceeds # of entries");

  if (userIndexCache_ <= i)
    {
      // start with the cached position
      userIndex = userIndexCache_;
      arrayIndex = arrayIndexCache_;
    }
  else
    {
      // start with the first occupied entry
      userIndex = 0;
      arrayIndex = 0;

      // skip over unused entries
      while (this->getUsage(arrayIndex) == UNUSED_COLL_ENTRY AND
	     arrayIndex < this->getSize())
	arrayIndex ++;
    }

  // advance to the desired entry
  while (userIndex < i)
    {
      userIndex++;
      arrayIndex++;
      // skip over unused entries
      while (this->getUsage(arrayIndex) == UNUSED_COLL_ENTRY AND
	     arrayIndex < this->getSize())
	arrayIndex ++;
    }

  // cache the results
  userIndexCache_ = userIndex;
  arrayIndexCache_ = arrayIndex;

  return this->usedEntry(arrayIndex);
}

template <class T>
const T & NASet<T>::operator [] (CollIndex i) const
{
  CollIndex userIndex = 0;
  CollIndex arrayIndex = 0;

  if (i >= this->entries())
    ABORT("Set index exceeds # of entries");

  // skip over unused entries
  while (this->getUsage(arrayIndex) == UNUSED_COLL_ENTRY AND
	 arrayIndex < this->getSize())
    arrayIndex ++;
  
  // advance to the desired entry
  while (userIndex < i)
    {
      userIndex++;
      arrayIndex++;
      // skip over unused entries
      while (this->getUsage(arrayIndex) == UNUSED_COLL_ENTRY AND
	     arrayIndex < this->getSize())
	arrayIndex ++;
    }

  return this->constEntry(arrayIndex);
}

template <class T>
NABoolean  NASet<T>::operator==(const NASet<T> &other) const
{
  CollIndex count = this->entries();

  if (count != other.entries())
    return FALSE;

  for (CollIndex i = 0; i < count; i++)
    {
      if (NOT this->contains(other[i]))
	  return FALSE;
    }

  return TRUE;
}

// -----------------------------------------------------------------------
// functions for the NAList<T> template
// -----------------------------------------------------------------------
        
template <class T>
NAList<T>::NAList(CollHeap * heap,
         CollIndex initLen,
         NABoolean failureIsFatal) : 
   NACollection<T>(heap,initLen, failureIsFatal)
,mutex_(true, false, true)
{ 
   first_ = last_ = userIndexCache_ = arrayIndexCache_ = NULL_COLL_INDEX; 
   invalidValuePtr_ = (failureIsFatal) ? NULL : new (heap) T;
   simualteBadIndex_ = FALSE;
}

template <class T>
NAList<T>::NAList(CollHeap * heap,
         CollIndex initLen,
         NABoolean failureIsFatal,
         NABoolean useMutex) : 
   NACollection<T>(heap,initLen, failureIsFatal)
,mutex_(true, useMutex, true)
{ 
   first_ = last_ = userIndexCache_ = arrayIndexCache_ = NULL_COLL_INDEX; 
   invalidValuePtr_ = (failureIsFatal) ? NULL : new (heap) T;
   simualteBadIndex_ = FALSE;
}

// copy ctor
template <class T>
NAList<T>::NAList(const NAList<T> &other, CollHeap * heap) : 
   NACollection<T>(other, heap)
,mutex_(true, false, true)
{
    first_ = other.first_;
    last_ = other.last_;
    userIndexCache_ = other.userIndexCache_;
    arrayIndexCache_ = other.arrayIndexCache_;
    invalidValuePtr_ = (this->isFailureIsFatal()) ? NULL : new (heap) T;
    simualteBadIndex_ = other.simualteBadIndex_;
}

template <class T>
NAList<T>::~NAList()
{
  //delete invalidValuePtr_;
  NADELETEBASIC(invalidValuePtr_, this->getHeap());
  invalidValuePtr_ = NULL;
}

template <class T>
void NAList<T>::fixupMyVTable(NAList* ptr)
{
   static NAList<T> sourceA(NULL, 0, TRUE);
   if ( ptr )
      memcpy((char*)ptr, (char*)&sourceA, sizeof(void*));
}
  
template <class T>
NAList<T> & NAList<T>::operator=(const NAList<T> &other)
{
  if ( this != &other ) // avoid copy-self!
    {
      this->deallocate();
      NACollection<T>::copy( (const NACollection<T>&) other);
      first_ = other.first_;
      last_ = other.last_;
      userIndexCache_ = other.userIndexCache_;
      arrayIndexCache_ = other.arrayIndexCache_;
      invalidValuePtr_ = (this->isFailureIsFatal()) ? NULL : new (this->getHeap()) T;
      simualteBadIndex_ = other.simualteBadIndex_;
      mutex_ = NAMutex(true,false/*other.mutex_.isEnabled()*/,true);
    }
  return *this;
}

template <class T>
NABoolean NAList<T>::operator== (const NAList<T> &other) const
{
  if (this->entries() != other.entries())
    return FALSE;

  for (CollIndex i = 0; i < this->entries(); i++)
    {
      if (NOT (at(i) == other.at(i)))
	return FALSE;
    }

  return TRUE;
}

  
template <class T>
NABoolean NAList<T>::find(const T &elem, T &returnedElem) const
{
  CollIndex foundIndex;
  
  if ((foundIndex = NACollection<T>::find(elem)) == NULL_COLL_INDEX)
    return FALSE;
  else
    {
      returnedElem = this->constEntry(foundIndex);
      return TRUE;
    }
}

template <class T>
T & NAList<T>::operator [] (CollIndex i)
{
  CollIndex arrayIndex = NULL_COLL_INDEX;
  {

    NAMutexScope mutex(mutex_);
    arrayIndex = findArraysIndex(i);
  }
  if ( arrayIndex == NULL_COLL_INDEX ) {
    assert(invalidValuePtr_);
    return *invalidValuePtr_;
  }

  return this->usedEntry(arrayIndex);
}

template <class T>
const T & NAList<T>::operator [] (CollIndex i) const
{
  CollIndex arrayIndex = NULL_COLL_INDEX;
  {

    NAMutexScope mutex(mutex_);
    arrayIndex = findArraysIndex(i);
  }
  if ( arrayIndex == NULL_COLL_INDEX ) {
    assert(invalidValuePtr_);
    return *invalidValuePtr_;
  }

  return this->constEntry(arrayIndex);
}


template <class T>
CollIndex NAList<T>::removeCounted(const T &elem, const CollIndex desiredCount)
{
  CollIndex actualCount = 0;
  CollIndex curr = first_;             // current entry
  CollIndex pred = NULL_COLL_INDEX;    // predecessor of current entry

  while (curr != NULL_COLL_INDEX) {

    if (this->usedEntry(curr) == elem) {
      // ok, found (first) matching entry, now delete it
      if (pred == NULL_COLL_INDEX)
	// we have a new first element in the list
	first_ = this->getUsage(curr);
      else
	// unlink this element from the list
	this->setUsage(pred,this->getUsage(curr));

      // delete the actual entry
      NACollection<T>::remove(curr);

      // take care of the last_ pointer
      if (last_ == curr) last_ = pred;

      if (++actualCount == desiredCount) break;
      curr = pred;
    }

    // go to the next element
    pred = curr;
    curr = this->getUsage(curr);
  }

  // invalidate the cache if necessary
  if (actualCount) invalidateCache();

  return actualCount;
}

// turn on the following macro to simulate the occurance of a bad index
#define SIMULATE_BAD_INDEX 1

template <class T>
CollIndex NAList<T>::findArraysIndex(CollIndex i)
{
  CollIndex userIndex;
  CollIndex arrayIndex;

  if (userIndexCache_ <= i)
    {
      // start with the cached position
      userIndex = userIndexCache_;
      arrayIndex = arrayIndexCache_;
    }
  else
    {
      // start with the first entry
      userIndex = 0;
      arrayIndex = first_;
    }

  while (userIndex < i)
    {
      userIndex++;
      arrayIndex = this->getUsage(arrayIndex);
      if (arrayIndex == NULL_COLL_INDEX) {
        if ( this->isFailureIsFatal() )
          ABORT("List index exceeds # of entries");
        else {
          this->setIsComplete(FALSE);
	  return NULL_COLL_INDEX;
        }
      }
    }

  userIndexCache_ = userIndex;
  arrayIndexCache_ = arrayIndex;

#ifdef SIMULATE_BAD_INDEX
  if ( simualteBadIndex_ ) {
    this->setIsComplete(FALSE);
    return NULL_COLL_INDEX;
  }
#endif

  return arrayIndex;
}

template <class T>
CollIndex NAList<T>::findArraysIndex(CollIndex i) const
{
  CollIndex userIndex;
  CollIndex arrayIndex;

  if (userIndexCache_ <= i)
    {
      // start with the cached position
      userIndex = userIndexCache_;
      arrayIndex = arrayIndexCache_;
    }
  else
    {
      // start with the first entry
      userIndex = 0;
      arrayIndex = first_;
    }

  // We cast away const-ness from _this_ so that we can update the 2
  // IndexCache data members.  Semantically, this is alright, since
  // changing those 2 data members does not truly "modify" the NAList
  // object.  But we get a big performance benefit from this.
  //
  // Yes, if MSVC++ supported the mutable keyword reliably, this unusual
  // cast would not be necessary.
  NAList<T>* thisPtr = (NAList<T>*) this ;

  while (userIndex < i)
    {
      userIndex++;
      arrayIndex = this->getUsage(arrayIndex);
      if (arrayIndex == NULL_COLL_INDEX) {
        if ( this->isFailureIsFatal() )
          ABORT("List index exceeds # of entries");
        else {
          thisPtr->setIsComplete(FALSE);
	  return NULL_COLL_INDEX;
        }
      }
    }

  thisPtr->userIndexCache_  = userIndex;
  thisPtr->arrayIndexCache_ = arrayIndex;

#ifdef SIMULATE_BAD_INDEX
  if ( simualteBadIndex_ ) {
    thisPtr->setIsComplete(FALSE);
    return NULL_COLL_INDEX;
  }
#endif

  return arrayIndex;
}

// For HSC, define this method as an inline method in the Collections.h header file.

// A dumb but easy implementation for insert one LIST into another
template <class T>
void NAList<T>::insert(const NAList<T> &other, NABoolean* inserted)
{
  CollIndex count = other.entries();
  for (CollIndex i = 0; i < count; i++)
    {
      insert(other[i], inserted);

      if ( inserted && !(*inserted) )
        return;
    } // for loop for iterating over members of the set
} // insert(LIST(T))

// For HSC, define this method as an inline method in the Collections.h header file.

template <class T>
NABoolean NAList<T>::getLast(T &elem)
{
  if (this->entries() > 0)
    {
      // copy last element
      elem = this->usedEntry(last_);

      // remove the last element from the list
      NACollection<T>::remove(last_);

      // fix up the list pointers
      if (first_ == last_)
	first_ = last_ = NULL_COLL_INDEX;
      else
	{
	  last_ = this->findUsage(last_);
	  this->setUsage(last_, NULL_COLL_INDEX);
	}

      // invalidate the cache, if necessary
      if (userIndexCache_ >= this->entries())
	invalidateCache();

      return TRUE;
    }
  else
    return FALSE;
}

// -----------------------------------------------------------------------
// functions for the NAArray<T> template
// -----------------------------------------------------------------------

template <class T>
NABoolean NAArray<T>::operator ==(const NAArray<T> &other) const
{
  if(this->entries() != other.entries())
    return FALSE;

  for(CollIndex i = 0; i < this->entries(); i++)
    {
      if(NOT(at(i) == other.at(i)))
	return FALSE;
    }

  return TRUE;
}
  
template <class T>
void NAArray<T>::fixupMyVTable(NAArray *ptr)
{
   static NAArray<T> sourceA(NULL, NULL);
   if ( ptr )
      memcpy((char*)ptr, (char*)&sourceA, sizeof(void*));
}
  
template <class T>
NABoolean NAArray<T>::intersect(const NAArray<T>& other, NAArray<T>& result) const
{
  NABoolean inserted = TRUE;

  CollIndex k = 0;

  for(CollIndex i = 0; i < this->entries(); i++)
  {
    for(CollIndex j = 0; j < other.entries(); j++)
    {
      if (at(i) == other.at(j)) {
        result.insertAt(k++, other.at(j), &inserted);
 
        if ( !inserted )
          return FALSE;
      }
    }
  }
  return TRUE;
}

// -----------------------------------------------------------------------
// functions for the NASubArray<T> template
// -----------------------------------------------------------------------

template <class T>
NASubArray<T>::~NASubArray()
{}

// ***********************************************************************
// functions for the Hash Dictionary
// ***********************************************************************
//
// first some definitions of macros useful to deal with events of failing
// to access an entry in the list, such as when the list is stored in a 
// shared segment and some process is removing an entry from the list. 
#define NAHB_RESET_IS_COMPLETE(x) \
         NAHashBucket<K,V>* xPtr = (NAHashBucket<K,V>*)(x); \
        (xPtr->bucket_).setIsComplete(TRUE); 

#define NAHB_RETURN_VALUE_IF_NOT_OK(x, y) \
      if ( !(x->bucket_).isComplete() ) { \
        NAHB_RESET_IS_COMPLETE(x) \
        return y; \
      }

#define NAHB_RETURN_NULL_IF_NOT_OK(x) NAHB_RETURN_VALUE_IF_NOT_OK(x, NULL) 
#define NAHB_RETURN_FALSE_IF_NOT_OK(x) NAHB_RETURN_VALUE_IF_NOT_OK(x, FALSE) 

#define NAHB_RETURN_IF_NOT_OK(x) \
      if ( !(x->bucket_).isComplete() ) { \
        NAHB_RESET_IS_COMPLETE(x) \
        return; \
      }

template <class K, class V>
NAHashBucketEntry<K,V>::~NAHashBucketEntry()
{
} //  NAHashBucketEntry<K,V>::~NAHashBucketEntry()
				    
template <class K, class V>
void NAHashBucketEntry<K,V>::clear(NABoolean deleteContents)
{
  if (deleteContents)
    {
      fixupVTable(key_);
      delete key_;

      fixupVTable(value_);
      delete value_;
    }
} //  NAHashBucketEntry<K,V>::clear()

template <class K, class V>
void NAHashBucketEntry<K,V>::fixupMyVTable(NAHashBucketEntry* ptr)
{
   static NAHashBucketEntry<K,V> sourceBE(NULL, NULL);

   if ( ptr ) 
      memcpy((char*)ptr, (char*)&sourceBE, sizeof(void*));
}

template <class K, class V>
NABoolean NAHashBucketEntry<K,V>::matchEntryType(enum iteratorEntryType type) const
{
    if ( type == iteratorEntryType::EVERYTHING )
      return TRUE;

    if ( isEnabled() && type == iteratorEntryType::ENABLED )
      return TRUE;

    if ( !isEnabled() && type == iteratorEntryType::DISABLED)
      return TRUE;

    return FALSE;
}

template <class K, class V>
void NAHashBucketEntry<K,V>::display() const
{
  printf("(%p,%p, %d) ",key_, value_, isEnabled_);

} //  NAHashBucketEntry<K,V>::display()
				    
template <class K, class V>
Int32 NAHashBucketEntry<K,V>::printStatistics(char *buf)
{
  return sprintf(buf, "(0X%X,0X%X) ",key_, value_);
} //  NAHashBucketEntry<K,V>::printStatistics()
				    
// -----------------------------------------------------------------------
// NAHashBucket::NAHashBucket()
// -----------------------------------------------------------------------
template <class K, class V>
NAHashBucket<K,V>::NAHashBucket(const NAHashBucket<K,V> & other, NAMemory * heap)
     : heap_( (heap==NULL) ? other.heap_ : heap )
       ,bucket_(other.bucket_)
       ,failureIsFatal_(other.failureIsFatal_)
{
} // NAHashBucket<K,V>::NAHashBucket()

template <class K, class V>
NAHashBucket<K,V>::~NAHashBucket()
{
  clear(FALSE); // delete all hash bucket entries
} // NAHashBucket<K,V>::~NAHashBucket()

template <class K, class V>
void NAHashBucket<K,V>::fixupMyVTable(NAHashBucket* ptr)
{
   static NAHashBucket<K,V> sourceBucket(NULL /*heap*/, TRUE /*failure is fatal*/);

   if ( ptr ) {
      memcpy((char*)ptr, (char*)&sourceBucket, sizeof(void*));
      NAList<NAHashBucketEntry<K,V>*>::fixupMyVTable(ptr->getBucket());
   }
}

// -----------------------------------------------------------------------
// NAHashBucket::clear()
// -----------------------------------------------------------------------
template <class K, class V>
void NAHashBucket<K,V>::clear(NABoolean deleteContents)
{
  CollIndex ne = bucket_.entries();
  for (CollIndex index = 0; index < ne; index++)
    {
      NAHashBucketEntry<K,V>* bep = bucket_[index];

      NAHB_RETURN_IF_NOT_OK(this);

      bep->clear(deleteContents);

      NAHashBucketEntry<K,V>::fixupMyVTable(bep);

      delete bep;
    }
  bucket_.clear(); // clear list head
} // NAHashBucket<K,V>::clear()

// -----------------------------------------------------------------------
// NAHashBucket::contains()
// Making two separate contains() methods allows a hash dictionary with
// a value type for which we don't have a comparison operator
// -----------------------------------------------------------------------
template <class K, class V>
NABoolean NAHashBucket<K,V>::contains(const K* key, bool checkAll) const 
{
  CollIndex ne = bucket_.entries();
  
  for (CollIndex index = 0; index < ne; index++)
    {
      // ----------------------------------------------------------------
      // Index a hash bucket entry.
      // ----------------------------------------------------------------
      NAHashBucketEntry<K,V>* bep = bucket_[index];

      NAHB_RETURN_FALSE_IF_NOT_OK(this);

      // ----------------------------------------------------------------
      // Compare the stored key value with the given key value.
      // ----------------------------------------------------------------
      // NOTE: uses K::operator==()
      if ((checkAll || (bep->isEnabled())) && *bep->getKey() == * key)
        return true;

    } // end for

  return FALSE;

} // NAHashBucket<K,V>::contains()

template <class K, class V>
NABoolean NAHashBucket<K,V>::contains(const K* key, const V* value) const 
{
  CollIndex ne = bucket_.entries();
  
  for (CollIndex index = 0; index < ne; index++)
    {
      // ----------------------------------------------------------------
      // Index a hash bucket entry.
      // ----------------------------------------------------------------
      NAHashBucketEntry<K,V>* bep = bucket_[index];

      NAHB_RETURN_FALSE_IF_NOT_OK(this);

      // ----------------------------------------------------------------
      // Compare the stored key value with the given key value.
      // ----------------------------------------------------------------
      // NOTE: uses K::operator==()
      if ( bep->isEnabled() && *bep->getKey() == * key )
	{

	  if (value) // a value is also supplied
	    {
	      // --------------------------------------------------------
	      // Compare the stored value with the given value.
	      // --------------------------------------------------------
	      if (*bep->getValue() == * value) // NOTE: uses V::operator==()
		return TRUE;
	    }
	  else           // keys are equal
	    return TRUE;

	} // endif

    } // end for

  return FALSE;

} // NAHashBucket<K,V>::contains()

// -----------------------------------------------------------------------
// NAHashBucket::display()
// -----------------------------------------------------------------------
template <class K, class V>
void NAHashBucket<K,V>::display() const
{
  CollIndex ne = bucket_.entries();
  printf("NAHashBucket<K,V>::display(): entries=%d\n", ne);
  if (ne > 0)
    {
      for (CollIndex index = 0; index < ne; index++)
	{
          NAHashBucketEntry<K,V>* bep = bucket_[index];

          NAHB_RETURN_IF_NOT_OK(this);

          printf("%d: ", index);
	  bep->display();
          printf("\n");
	}

      printf("done!\n");
    }
  else
    printf("*** empty ***\n");
} //  NAHashBucket<K,V>::display()
				    
// -----------------------------------------------------------------------
// NAHashBucket::printStatistics()
// -----------------------------------------------------------------------
template <class K, class V>
Int32 NAHashBucket<K,V>::printStatistics(char *buf)
{
  CollIndex ne = bucket_.entries(); Int32 c = 0;
  if (ne > 0) {
    for (CollIndex index = 0; index < ne; index++) {
          NAHashBucketEntry<K,V>* bep = bucket_[index];

          NAHB_RETURN_VALUE_IF_NOT_OK(this, c);

	  c += bep->printStatistics(buf+c);
	  if (index > 0 AND (index/4* 4 == index)) 
        c += sprintf(buf+c,"\n");
	}
  }
  else {
    c = sprintf(buf,"*** empty ***\n");
  }
  return c;
} //  NAHashBucket<K,V>::printStatistics()
				    
// -----------------------------------------------------------------------
// NAHashBucket::getFirstValue()
// -----------------------------------------------------------------------
template <class K, class V>
V* NAHashBucket<K,V>::getFirstValue(const K* key) const
{
  CollIndex ne = bucket_.entries();
  
  for (CollIndex index = 0; index < ne; index++)
    {
      // ----------------------------------------------------------------
      // Index a hash bucket entry.
      // ----------------------------------------------------------------
      NAHashBucketEntry<K,V>* bep = bucket_[index];

      NAHB_RETURN_NULL_IF_NOT_OK(this);

      // ----------------------------------------------------------------
      // Compare the stored key value with the given key value.
      // ----------------------------------------------------------------
      // NOTE: uses K::operator==()
      if ( bep->isEnabled() && *bep->getKey() == *key )  
	return bep->getValue();
      
    } // end for

  return NULL; // key not found in the hash dictionary
  
} // NAHashBucket<K,V>::getFirstValue()

// -----------------------------------------------------------------------
// NAHashBucket::getFirstValueAll()
//   returns the first value for the key whether enabled or disabled.
// -----------------------------------------------------------------------
template <class K, class V>
V* NAHashBucket<K,V>::getFirstValueAll(const K* key) const
{
  CollIndex ne = bucket_.entries();

  for (CollIndex index = 0; index < ne; index++)
    {
      // ----------------------------------------------------------------
      // Index a hash bucket entry.
      // ----------------------------------------------------------------
      NAHashBucketEntry<K,V>* bep = bucket_[index];

      NAHB_RETURN_NULL_IF_NOT_OK(this);

      // ----------------------------------------------------------------
      // Compare the stored key value with the given key value.
      // ----------------------------------------------------------------
      // NOTE: uses K::operator==()
      if ( *bep->getKey() == *key )
        return bep->getValue();

    } // end for

  return NULL; // key not found in the hash dictionary

} // NAHashBucket<K,V>::getFirstValue()

// -----------------------------------------------------------------------
// NAHashBucket::getKeyValuePair()
// -----------------------------------------------------------------------
template <class K, class V>
void NAHashBucket<K,V>::getKeyValuePair(const K* key, const V* value,
				        NAHashBucket<K,V>& container, 
                                        NABoolean* inserted) const
{
  CollIndex ne = bucket_.entries();
  
  NABoolean isInserted = TRUE;
  if (key)
    { // a key is given
      for (CollIndex index = 0; index < ne; index++)
	{
	  // -------------------------------------------------------------
	  // Index a hash bucket entry.
	  // -------------------------------------------------------------
	  NAHashBucketEntry<K,V>* bep = bucket_[index];

          if ( !bucket_.isComplete() ) {

            if ( inserted )
               *inserted = FALSE;

            NAHB_RESET_IS_COMPLETE(this);

            return;
          }

	  // -------------------------------------------------------------
	  // Compare the stored key value with the given key value.
	  // -------------------------------------------------------------
          // NOTE: uses K::operator==()
	  if ( bep->isEnabled() && *bep->getKey() == * key )  
	    {

	      if (value) // a value is also supplied
		{
		  // -----------------------------------------------------
		  // Compare the stored value with the given value.
		  // -----------------------------------------------------
		  if (*bep->getValue() == * value) // NOTE: uses V::operator==()
		    container.insert(bep->getKey(), bep->getValue(), &isInserted);
		}
	      else      // keys are equal
		container.insert(bep->getKey(), bep->getValue(), &isInserted);

              if ( !isInserted ) {

                 if ( inserted )
                    *inserted = FALSE;

                 return;
              }

	    } // endif stored key == given key

	} // end for

    } // a key is given
  else
    {
      // Return all hash bucket entries
      for (CollIndex index = 0; index < ne; index++)
      {
	 NAHashBucketEntry<K,V>* bep = bucket_[index];

         if ( !bucket_.isComplete() ) {

            if ( inserted )
               *inserted = FALSE;

            NAHB_RESET_IS_COMPLETE(this);

            return;
        }

        if ( bep->isEnabled() ) {
	  container.insert(bep->getKey(), bep->getValue(), &isInserted);
          if ( !isInserted ) {
            if ( inserted )
               *inserted = FALSE;
            return;
          }
        }
      }
    }
  
} // NAHashBucket<K,V>::getKeyValuePair()

// -----------------------------------------------------------------------
// NAHashBucket::remove()
// -----------------------------------------------------------------------
template <class K, class V>
K* NAHashBucket<K,V>::remove(K* key, Lng32& entriesEnabled, NABoolean removeKV)
{
  CollIndex ne = bucket_.entries();
  
  for (CollIndex index = 0; index < ne; index++)
    {
      // ----------------------------------------------------------------
      // Index a hash bucket entry.
      // ----------------------------------------------------------------
      NAHashBucketEntry<K,V>* bep = bucket_[index];

      NAHB_RETURN_NULL_IF_NOT_OK(this);

      // ----------------------------------------------------------------
      // Compare the stored key value with the given key value.
      // ----------------------------------------------------------------
      if (*bep->getKey() == *key)  // NOTE: uses K::operator==()
	{
          if ( bep->isEnabled() )
            entriesEnabled--;

	  bucket_.removeAt(index);

          // remove the key and the value if needed
          if ( removeKV )
             bep->clear(TRUE /*deleteContents*/);

          NAHashBucketEntry<K,V>::fixupMyVTable(bep);
          delete bep;

	  return key;
	}
      
    } // end for

  return NULL; // key not found in the hash dictionary
  
} // NAHashBucket<K,V>::remove()

// -----------------------------------------------------------------------
// NAHashBucket::enable()
// -----------------------------------------------------------------------
template <class K, class V>
K* NAHashBucket<K,V>::enable(K* key, NABoolean enforceUniqueness, Lng32& entriesEnabled)
{
  CollIndex ne = bucket_.entries();

  K* lastKeySeen = NULL;
  
  for (CollIndex index = 0; index < ne; index++)
    {
      // ----------------------------------------------------------------
      // Index a hash bucket entry.
      // ----------------------------------------------------------------
      NAHashBucketEntry<K,V>* bep = bucket_[index];

      NAHB_RETURN_NULL_IF_NOT_OK(this);

      // ----------------------------------------------------------------
      // Compare the stored key value with the given key value.
      // ----------------------------------------------------------------
      if (*bep->getKey() == *key)  // NOTE: uses K::operator==()
	{
	  if ( !(bep->isEnabled()) )
            entriesEnabled++;

	  bep->setIsEnabled(TRUE);

          if ( enforceUniqueness )
	     return key;
          else 
             lastKeySeen = key;
	}
      
    } // end for

  return lastKeySeen; 
  
} // NAHashBucket<K,V>::enable()

// -----------------------------------------------------------------------
// NAHashBucket::disable()
// -----------------------------------------------------------------------
template <class K, class V>
K* NAHashBucket<K,V>::disable(K* key, NABoolean enforceUniqueness, Lng32& entriesEnabled)
{
  CollIndex ne = bucket_.entries();

  K* lastKeySeen = NULL;
  
  for (CollIndex index = 0; index < ne; index++)
    {
      // ----------------------------------------------------------------
      // Index a hash bucket entry.
      // ----------------------------------------------------------------
      NAHashBucketEntry<K,V>* bep = bucket_[index];

      NAHB_RETURN_NULL_IF_NOT_OK(this);

      // ----------------------------------------------------------------
      // Compare the stored key value with the given key value.
      // ----------------------------------------------------------------
      if (*bep->getKey() == *key)  // NOTE: uses K::operator==()
	{
	  if ( bep->isEnabled() )
            entriesEnabled--;

	  bep->setIsEnabled(FALSE);

          if ( enforceUniqueness )
	     return key;
          else 
             lastKeySeen = key;
	}
      
    } // end for

  return lastKeySeen; 
  
} // NAHashBucket<K,V>::disable()


// -----------------------------------------------------------------------
// NAHashBucket::isEnable()
// -----------------------------------------------------------------------
template <class K, class V>
NABoolean NAHashBucket<K,V>::isEnable(K* key, NABoolean enforceUniqueness)
{
  NABoolean result = FALSE;

  CollIndex ne = bucket_.entries();
  for (CollIndex index = 0; index < ne; index++)
    {
      NAHashBucketEntry<K,V>* bep = bucket_[index];
      NAHB_RETURN_NULL_IF_NOT_OK(this);

      if (*bep->getKey() == *key)
        {
          if (enforceUniqueness)
            return bep->isEnabled();
          result = bep->isEnabled();
        }
    
    }
  return result;
}

template <class K, class V>
NABoolean NAHashBucket<K,V>::sanityCheck()
{
  // check bucket_ which is a NAList which is a NACollection.
  return (bucket_.sanityCheck());
}
// ----------------------------------------------------------------------
// NAHashDictionary constructor functions
// ----------------------------------------------------------------------
template <class K, class V>
NAHashDictionary<K,V>::NAHashDictionary(
// see long detailed comment in Collections.h about the hash function param.
			ULng32 (*hashFunction)(const K &), 
			ULng32 hashSize,
			NABoolean enforceUniqueness,
			NAMemory* heap,
                        NABoolean failureIsFatal)
			: 
                       failureIsFatal_(failureIsFatal),
                       heap_(heap),
		       hash_(hashFunction),
		       entries_(0), 
		       entriesEnabled_(0), 
		       isComplete_(TRUE), 
		       enforceUniqueness_(enforceUniqueness)
{
   createHashTable(hashSize);
}

template <class K, class V>
NAHashDictionary<K,V>::NAHashDictionary(
// see long detailed comment in Collections.h about the hash function param.
			ULng32 (*hashFunction)(const K &), 
			ULng32 hashSize,
			NABoolean enforceUniqueness,
			NAMemory* heap,
                        NABoolean failureIsFatal,
                        NABoolean useMutex)
			:
                       failureIsFatal_(failureIsFatal),
                       heap_(heap),
		       hash_(hashFunction),
		       entries_(0), 
		       entriesEnabled_(0), 
		       isComplete_(TRUE), 
		       enforceUniqueness_(enforceUniqueness)
{
   createHashTable(hashSize, useMutex);
}

template <class K, class V>
NAHashDictionary<K,V>::NAHashDictionary (const NAHashDictionary<K,V> & other,
                                         NAMemory * heap) 
     : heap_( (heap==NULL) ? other.heap_ : heap ),
       hash_(other.hash_),
       entries_(other.entries_), 
       entriesEnabled_(other.entriesEnabled_), 
       enforceUniqueness_(other.enforceUniqueness_),
       failureIsFatal_(other.failureIsFatal_)
{
  createHashTable(other.hashSize_);

  if ( !isComplete_ )
    return;

  for (CollIndex index = 0; index < hashSize_; index++)
     (*hashTable_)[index] = (*other.hashTable_)[index];
}

// ----------------------------------------------------------------------
// NAHashDictionary destructor function.
// ----------------------------------------------------------------------
template <class K, class V>
NAHashDictionary<K,V>::~NAHashDictionary() 
{
  for (CollIndex index = 0; index < hashSize_; index++) {
    NAHashBucket<K,V>::fixupMyVTable((*hashTable_)[index]);
    delete (*hashTable_)[index];
  }

  delete hashTable_; 
}

// ----------------------------------------------------------------------
// NAHashDictionary::clear()
// Deletes all entries in the hash buckets.
// Delete any key or value when deleteContents is TRUE.
// Does not delete any hash bucket or the hash table.
// ----------------------------------------------------------------------
template <class K, class V>
void NAHashDictionary<K,V>::clear(NABoolean deleteContents)
{
  for (ULng32 index = 0; index < hashSize_; index++)
    (*hashTable_)[index]->clear(deleteContents);
  entries_ = entriesEnabled_ = 0;
} //  NAHashDictionary<K,V>::clear()


template <class K, class V>
void NAHashDictionary<K,V>::detachHashBuckets()
{
  for (ULng32 index = 0; index < hashSize_; index++)
    (*hashTable_)[index] = NULL;
} 

// ----------------------------------------------------------------------
// NAHashDictionary::createHashTable()
// Helper function for creating a hash table.
// ----------------------------------------------------------------------
template <class K, class V>
void NAHashDictionary<K,V>::createHashTable(ULng32 hashSize)
{
  assert (hashSize > 0);
  hashSize_ = hashSize;
  hashTable_ = new(heap_, failureIsFatal_) NAArray<NAHashBucket<K,V>* >(heap_,hashSize, failureIsFatal_);

  isComplete_ = ( hashTable_ != NULL );

  if ( !isComplete_ )
    return;

  NABoolean inserted = TRUE;
  for (CollIndex index = 0; index < hashSize; index++) {

     NAHashBucket<K,V>* newBucket = 
         new(heap_, failureIsFatal_) NAHashBucket<K,V>(heap_, failureIsFatal_);

     if (!newBucket) {
        isComplete_ = FALSE;
        return;
     }

     hashTable_->insertAt(index, newBucket, &inserted);

     if ( !inserted ) {
        isComplete_ = FALSE;
        return;
     }
  }
} //  NAHashDictionary<K,V>::createHashTable()

template <class K, class V>
void NAHashDictionary<K,V>::createHashTable(ULng32 hashSize, NABoolean useMutex)
{
  assert (hashSize > 0);
  hashSize_ = hashSize;
  hashTable_ = new(heap_, failureIsFatal_) NAArray<NAHashBucket<K,V>* >(heap_,hashSize, failureIsFatal_);

  isComplete_ = ( hashTable_ != NULL );

  if ( !isComplete_ )
    return;

  NABoolean inserted = TRUE;
  for (CollIndex index = 0; index < hashSize; index++) {

     NAHashBucket<K,V>* newBucket = 
         new(heap_, failureIsFatal_) NAHashBucket<K,V>(heap_, failureIsFatal_, useMutex);

     if (!newBucket) {
        isComplete_ = FALSE;
        return;
     }

     hashTable_->insertAt(index, newBucket, &inserted);

     if ( !inserted ) {
        isComplete_ = FALSE;
        return;
     }
  }
} 
// ----------------------------------------------------------------------
template <class K, class V>
void NAHashDictionary<K,V>::destroyHashTable()
{
  if ( !hashTable_ )
     return;

  clearAndDestroy(); // delete all keys and values 

  for (CollIndex index = 0; index < hashSize_; index++) {
   
     NAHashBucket<K,V>::fixupMyVTable((*hashTable_)[index]);
     delete (*hashTable_)[index];

  }

  NAArray<NAHashBucket<K,V>*>::fixupMyVTable(hashTable_);
  delete hashTable_;

  entries_ = entriesEnabled_ = hashSize_ = 0;
  isComplete_ = FALSE;
  hashTable_ = NULL;
} 

// ----------------------------------------------------------------------
// NAHashDictionary::getHashCode()
// Function for generating a hash code
// ----------------------------------------------------------------------
template <class K, class V>
ULng32 NAHashDictionary<K,V>::getHashCode(const K& key, ULng32(*ptr)(const K&)) const
{
  // use the key's hash method to get the hash value
//  unsigned long hashValue = key.hash() % hashSize_;
//#else
  ULng32 hashValue = (ptr) ? ptr(key) % hashSize_ : hash_(key) % hashSize_;
//#endif
  assert(hashValue < hashSize_);
  return hashValue;
} //  NAHashDictionary<K,V>::getHashCode()

// ----------------------------------------------------------------------
// NAHashDictionary::insert()
// ----------------------------------------------------------------------
template <class K, class V>
K* NAHashDictionary<K,V>::insert(K* key, V* value, ULng32(*ptr)(const K&), 
                                 NABoolean* inserted) 
{ 
  if (enforceUniqueness_ AND contains(key, ptr))
    {
      assert(enforceUniqueness_);
      return NULL; // don't insert a duplicate key
    }

  NABoolean isInserted = TRUE;
  (*hashTable_)[getHashCode(*key, ptr)]->insert(key, value, &isInserted); 

  if ( isInserted ) {
    entries_++;
    entriesEnabled_++;
  }

  if ( inserted )
    *inserted = isInserted;

  return key;
} // NAHashDictionary<K,V>::insert()				

template <class K, class V>
NABoolean NAHashDictionary<K,V>::sanityCheck()
{
  if ( !isComplete_ )
     return FALSE;

  for (ULng32 index = 0; index < hashSize_; index++)
  {
    if ( (*hashTable_)[index]->sanityCheck() == FALSE )
      return FALSE;
  }

   return TRUE;
}

template <class K, class V>
void NAHashDictionary<K,V>::fixupMyVTable(NAHashDictionary* ptr)
{
   static NAHashDictionary<K,V> sourceDictionary(NULL /*heap*/, NULL /*failure is fatal*/);

   if ( ptr ) {
      memcpy((char*)ptr, (char*)&sourceDictionary, sizeof(void*));
   }
}

// ----------------------------------------------------------------------
// NAHashDictionaryIterator::ctor()
// ----------------------------------------------------------------------
template <class K, class V>
NAHashDictionaryIterator<K,V>::NAHashDictionaryIterator (const NAHashDictionary<K,V> & dict,
                                                         const K* key, 
                                                         const V* value,
                                                         CollHeap* heap,
                                                         ULng32(*hashFunc)(const K&),
                                                         NABoolean failureIsFatal)
     // use the supplied heap for iterator_ if not null
     : iterator_((heap) ? heap : dict.heap_, failureIsFatal)
{
  NABoolean inserted;
  if (key)
    (*(dict.hashTable_))[dict.getHashCode(*key, hashFunc)]->getKeyValuePair(key,value,iterator_, &inserted);
  else 
    {      
      // iterate over all key value pairs in the hash table
      for (CollIndex index = 0; index < dict.hashSize_; index++)
	(*(dict.hashTable_))[index]->getKeyValuePair(key,value,iterator_, &inserted) ; 
    } 

  // iterator_ now contains all the qualifying key value pairs  
  reset() ; // set the position so we're ready to iterate

  isComplete_ = inserted;

} // NAHashDictionaryIterator<K,V>()

// ----------------------------------------------------------------------
// NAHashDictionaryIterator::copy ctor()
// ----------------------------------------------------------------------
template <class K, class V>
NAHashDictionaryIterator<K,V>::NAHashDictionaryIterator (const NAHashDictionaryIterator<K,V> & other, NAMemory * heap)
     : iterator_(heap, other.failureIsFatal_), // NB: we always put copies on the stmt heap
       isComplete_(other.isComplete_)
{
  iterator_         = other.iterator_ ; 
  iteratorPosition_ = other.iteratorPosition_ ; 
} // NAHashDictionaryIterator<K,V>() copy ctor()

// ----------------------------------------------------------------------
// ~NAHashDictionaryIterator()
// ----------------------------------------------------------------------
template <class K, class V>
NAHashDictionaryIterator<K,V>::~NAHashDictionaryIterator() 
{
  isComplete_ = TRUE;
  iterator_.clear() ; 
  reset() ; 
} // ~NAHashDictionaryIterator<K,V>

// ----------------------------------------------------------------------
// NAHashDictionaryIterator::getNext()
// ----------------------------------------------------------------------
template <class K, class V>
NABoolean NAHashDictionaryIterator<K,V>::getNext(K*& key, V*& value)
{
  if (iteratorPosition_ < iterator_.entries())
    {
      key = iterator_[iteratorPosition_]->getKey();
      value = iterator_[iteratorPosition_]->getValue();
      iteratorPosition_++ ;
      return TRUE;
    }
  else
    {
      // If the application has advanced the iterator beyond the number
      // of entries, signal that there are no more keys and values.
      key = NULL;
      value = NULL;
      return FALSE;
    }
} // NAHashDictionaryIterator<K,V>::getNext()				

template <class K, class V>
void NAHashDictionaryIterator<K,V>::display() const
{
  printf("NAHashDictionaryIterator<K,V>::display(): this=%p\n", this);
  iterator_.display();
} 

// ----------------------------------------------------------------------
// NAHashDictionaryIteratorNoCopy::ctor()
// ----------------------------------------------------------------------
template <class K, class V>
NAHashDictionaryIteratorNoCopy<K,V>::NAHashDictionaryIteratorNoCopy(
                      const NAHashDictionary<K,V> & dict,
                      enum iteratorEntryType type,
                      const K* key, 
                      const V* value,
                      CollHeap* heap,
                      ULng32(*hashFunc)(const K&)) : 
     dict_(dict),
     type_(type)
{
  if (key) {
    beginBucket_ = endBucket_ = dict_.getHashCode(*key, hashFunc);
  } else {      
    beginBucket_ = 0;
    endBucket_ = dict.hashSize_ - 1;
  } 
        
  reset() ; // set the position so we're ready to iterate
} 

template <class K, class V>
NAHashDictionaryIteratorNoCopy<K,V>::NAHashDictionaryIteratorNoCopy(const NAHashDictionaryIteratorNoCopy<K,V> & other, NAMemory * heap)
{
  dict_ = other.dict_; 
  type_ = other.type_; 
  beginBucket_ = other.beginBucket_ ; 
  endBucket_ = other.endBucket_ ; 

  currentBucket_ = other.currentBucket_ ; 
  iteratorPosition_ = other.iteratorPosition_ ; 
} 

template <class K, class V>
NAHashDictionaryIteratorNoCopy<K,V>::~NAHashDictionaryIteratorNoCopy() 
{
  reset() ; 
} 


// advance the current position: (currentBucket_, iteratorPosition_).
template <class K, class V>
void NAHashDictionaryIteratorNoCopy<K,V>::advance()
{
  // If this varialble is set to TRUE, it means we have exhausted
  // all entries in the current bucket and need to move to next
  // non-empty bucket.
  NABoolean newBucket = FALSE;

  for ( ; currentBucket_ <= endBucket_; currentBucket_++ )
  {
     // If we can find a useful element in the current bucket, 
     // bail out.
     if ( advanceInCurrentBucket(newBucket) )
        return;

     // need to try next bucket
     newBucket = TRUE;
  }

  // have exhausted all buckets. 
  iteratorPosition_ = NULL_COLL_INDEX;
}

template <class K, class V>
NABoolean NAHashDictionaryIteratorNoCopy<K,V>::advanceInCurrentBucket(NABoolean newBucket)
{
   NAHashBucket<K,V>* current = currentBucket();

  if ( current->entries() == 0 ) 
    return FALSE;

   int j = (newBucket ) ? FIRST_COLL_INDEX  : iteratorPosition_  + 1;

   for (; j< current->entries(); j++)
     {
        if ( (*current)[j]->matchEntryType(type_) ) {

          iteratorPosition_ = j;  
          return TRUE;
        }

     }

   return FALSE;

}

template <class K, class V>
void NAHashDictionaryIteratorNoCopy<K,V>::reset()
{
  for (currentBucket_ = beginBucket_; currentBucket_ <= endBucket_; currentBucket_++)
  {
     // If we can find a useful entry in the current bucket, bail out.
     if ( advanceInCurrentBucket(TRUE) )
        return;
  }

  // Have exhausted all buckets. 
  iteratorPosition_ = NULL_COLL_INDEX;
}

template <class K, class V>
NABoolean NAHashDictionaryIteratorNoCopy<K,V>::getNext(K*& key, V*& value)
{
  if ( iteratorPosition_ == NULL_COLL_INDEX ) {
    // no more entries to return
    key = NULL;
    value = NULL;
    return FALSE;
  } else {
    // Return the currently enabled entry 
    NAHashBucket<K,V>* current = this->currentBucket();

    key = (*current)[iteratorPosition_]->getKey();
    value = (*current)[iteratorPosition_]->getValue();
    advance(); // get ready for the next call to getNext().
    return TRUE;
  }

  // impossible to reach here.
  return FALSE;
} 

template <class K, class V>
void NAHashDictionaryIteratorNoCopy<K,V>::display(const char* msg) const
{
  if ( msg )
    printf("%s\n", msg);

  printf("NAHashDictionaryIteratorNoCopy<K,V>::display(): this=%p\n", this);

  reset();

  K* key = NULL;
  V* value = NULL;
  while (getNext(key, value))  {
     printf("(%p,%p) ", key, value);
  } 

} 

// ----------------------------------------------------------------------
// NAHashDictionary::remove()
// ----------------------------------------------------------------------
template <class K, class V>
K* NAHashDictionary<K,V>::remove(K* key, ULng32(*hashFunc)(const K&), NABoolean removeKV)
{
  K* removedKey = (*hashTable_)[getHashCode(*key, hashFunc)]->remove(key, entriesEnabled_, removeKV);
  if (removedKey)
    entries_--;
  return removedKey;
} // NAHashDictionary<K,V>::remove()				

template <class K, class V>
K* NAHashDictionary<K,V>::enable(K* key, ULng32(*hashFunc)(const K&))
{
  K* enabledKey = (*hashTable_)[getHashCode(*key, hashFunc)]->enable(key, enforceUniqueness_, entriesEnabled_);
  return enabledKey;
}

template <class K, class V>
K* NAHashDictionary<K,V>::disable(K* key, ULng32(*hashFunc)(const K&))
{
  K* disabledKey = (*hashTable_)[getHashCode(*key, hashFunc)]->disable(key, enforceUniqueness_, entriesEnabled_);
  return disabledKey;
}

template <class K, class V>
NABoolean NAHashDictionary<K,V>::isEnable(K* key, ULng32(*hashFunc)(const K&))
{
  return (*hashTable_)[getHashCode(*key, hashFunc)]->isEnable(key, enforceUniqueness_);
}

// ----------------------------------------------------------------------
// NAHashDictionary::display()
// ----------------------------------------------------------------------
template <class K, class V>
void NAHashDictionary<K,V>::display() const
{
  if (hashSize_ > 0)
    {
      for (CollIndex index = 0; index < hashSize_; index++)
	{
	  printf("\nbucket[%d] : \n",index);
	  (*hashTable_)[index]->display();
	}
    }
  else
    printf("*** hash table is empty ***");
  printf("\n");
} //  NAHashDictionary<K,V>::display()
				    
// ----------------------------------------------------------------------
// NAHashDictionary::printStatistics()
// ----------------------------------------------------------------------
template <class K, class V>
Int32 NAHashDictionary<K,V>::printStatistics(char *buf)
{
  Int32 c = 0;
  if (hashSize_ > 0) {
    for (CollIndex index = 0; index < hashSize_; index++) {
	  c += sprintf(buf+c, "\nbucket[%d] : \n",index);
	  c += (*hashTable_)[index]->printStatistics(buf+c);
	}
  }
  else 
    c = sprintf(buf,"*** hash table is empty ***");
  c += sprintf(buf+c,"\n");
  return c;
} //  NAHashDictionary<K,V>::printStatistics()

static ULng32 hashFunc(const Int32& x)
{
   return (ULng32)x;
}

template <class K, class V>
NABoolean 
NAHashDictionary<K,V>::operator==(const NAHashDictionary<K,V>& other) const
{
   if ( entries() != other.entries() )
     return FALSE;

   NAHashDictionaryIteratorNoCopy<K,V> walkThis(*this);
   NAHashDictionaryIteratorNoCopy<K,V> walkOther(other);

   K* keyThis = NULL;
   V* valueThis = NULL;
   K* keyOther = NULL;
   V* valueOther = NULL;

   for (int i=0; i<entries(); i++ ) {
      if ( !walkThis.getNext(keyThis, valueThis) )
        return FALSE;

      if ( !walkOther.getNext(keyOther, valueOther) )
        return FALSE;

      if ( keyThis != keyOther ||
           valueThis != valueOther) 
        return FALSE;
   }

   return TRUE;
}

template <class T>
void NASimpleArray<T>::fixupMyVTable(NASimpleArray* ptr)
{
  static NASimpleArray<T> simpleArray(NULL);

   if ( ptr ) {
      memcpy((char*)ptr, (char*)&simpleArray, sizeof(void*));
   }
}
