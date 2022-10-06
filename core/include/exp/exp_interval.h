
#ifndef EXP_INTERVAL_H
#define EXP_INTERVAL_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         exp_interval.h
 * Description:  Interval Type
 *
 *
 * Created:      8/21/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/dfs2rec.h"
#include "exp/ExpError.h"
#include "exp/exp_attrs.h"

class ExpInterval : public SimpleType {
 public:
  static short getIntervalStartField(int fsDatatype, rec_datetime_field &startField);

  static short getIntervalEndField(int fsDatatype, rec_datetime_field &endField);

  static int getStorageSize(rec_datetime_field startField, UInt32 leadingPrecision, rec_datetime_field endField,
                            UInt32 fractionPrecision = 0);

  static int getDisplaySize(int fsDatatype, short leadingPrecision, short fractionPrecision);
};

#endif
