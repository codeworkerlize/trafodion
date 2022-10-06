/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComExtents.h
 * Description:  Provides conversion functions to convert Maxsize and Allocate
 *               attributes to primary-extents, secondary-extents and max-extents
 *
 *
 *
 * Created:      11/28/94
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#ifndef _COM_EXTENTS_H_
#define _COM_EXTENTS_H_

#include "common/ComUnits.h"
#include "common/ComSmallDefs.h"
#include "common/Int64.h"

class ComExtents {
 public:
  // ---------------------------------------------------------------------
  // Constructors & destructor
  // ---------------------------------------------------------------------
  ComExtents(long maxSize, ComUnits units);

  ComExtents(long maxSize);

  ComExtents(const ComExtents &rhs);

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------

  long getMaxSize(void) const;
  ComUnits getMaxSizeUnits(void) const;
  long getSizeInBytes(long sizeToConvert, ComUnits units);

 protected:
 private:
  // ---------------------------------------------------------------------
  // Private data members
  // ---------------------------------------------------------------------
  long maxSize_;
  ComUnits units_;
};

inline long ComExtents::getMaxSize(void) const { return maxSize_; };

inline ComUnits ComExtents::getMaxSizeUnits(void) const { return units_; };

#endif  //_COM_EXTENTS_H_
