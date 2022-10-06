/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComExtents.C
 * Description:  Provides conversion functions to convert Maxsize and Allocate
 *               attributes to primary-extents, secondary-extents and max-extents
 *
 * Created:      11/28/94
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "ComExtents.h"

#include "common/ComASSERT.h"
#include "common/Int64.h"

ComExtents::ComExtents(long maxSize, ComUnits units) : maxSize_(maxSize), units_(units) {
  // ---------------------------------------------------------------------
  // Calculate the extent size:
  //
  // maxFileSize = MAX(maxSizeInBytes, 20MB)
  //
  // maxFileSize = MIN(maxFileSize, 2gbytes) -- make sure this size
  //   does not go over 2gbytes (largest supported by NSK)
  //
  // Extent sizes are no longer calculated because DP2 decides on
  // the extent sizes to use.
  // ---------------------------------------------------------------------

  const long maxSizeInBytes = getSizeInBytes(maxSize_, units_);

  long maxFileSize = maxSizeInBytes;
  // If maxSize_ is too small, set it to the minimum allowed (in bytes).
  if (maxFileSize < COM_MIN_PART_SIZE_IN_BYTES) {
    maxSize_ = COM_MIN_PART_SIZE_IN_BYTES;
    units_ = COM_BYTES;
  }
  // If maxSize_ is too large, set it to the maximum allowed (in bytes).
  else if (maxFileSize > COM_MAX_PART_SIZE_IN_BYTES) {
    maxSize_ = COM_MAX_PART_SIZE_IN_BYTES;
    units_ = COM_BYTES;
  }
  // If maxSize_ is within the allowed range, leave it and units_ unchanged.
};

ComExtents::ComExtents(long maxSize) : maxSize_(maxSize) {
  // Since units_ was unspecified, maxSize is bytes.
  units_ = COM_BYTES;

  // If maxSize_ is too small, set it to the minimum allowed.
  if (maxSize < COM_MIN_PART_SIZE_IN_BYTES) maxSize_ = COM_MIN_PART_SIZE_IN_BYTES;
  // If maxSize_ is too large, set it to the maximum allowed.
  else if (maxSize > COM_MAX_PART_SIZE_IN_BYTES)
    maxSize_ = COM_MAX_PART_SIZE_IN_BYTES;
};

// -----------------------------------------------------------------------
// getSizeInBytes:
//
// This function calculates the size of the input parameter in bytes
// -----------------------------------------------------------------------
long ComExtents::getSizeInBytes(long sizeToConvert, ComUnits units) {
  long convertedSize = 0;

  switch (units) {
    case COM_BYTES:
      convertedSize = (sizeToConvert);
      break;
    case COM_KBYTES:
      convertedSize = (sizeToConvert * 1024);
      break;
    case COM_MBYTES:
      convertedSize = (sizeToConvert * 1024 * 1024);
      break;
    case COM_GBYTES:
      convertedSize = (sizeToConvert * (1024 * 1024 * 1024));
      break;
    default:
      ComASSERT(FALSE);  // raise an exception
      break;
  };
  return convertedSize;
};
