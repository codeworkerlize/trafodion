#ifndef MDAMENUMS_H
#define MDAMENUMS_H

/* -*-C++-*-
********************************************************************************
*
* File:         MdamEnums.h
* Description:  MDAM Enums
*
*
* Created:      11/18/96
* Language:     C++
*
*

*
*
********************************************************************************
*/

// -----------------------------------------------------------------------------

// *****************************************************************************
// MdamEnums is a dummy class that contains Mdam-related enums.
// *****************************************************************************

class MdamEnums {
 public:
  enum MdamOrder { MDAM_LESS = -1, MDAM_EQUAL = 0, MDAM_GREATER = 1 };

  enum MdamInclusion { MDAM_EXCLUDED = 0, MDAM_INCLUDED = 1 };

  enum MdamEndPointType { MDAM_BEGIN = 0, MDAM_END = 1 };

};  // end class MdamEnums

#endif /* MDAMENUMS_H */
