#ifndef KEYRANGE_H
#define KEYRANGE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         key_range.h
 * Description:  defines base classes for objects describing key ranges
 *
 *
 * Created:      11/8/96
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "ex_ex.h"
#include "exp/ExpSqlTupp.h"
#include "executor/ex_error.h"

// forward references
class ex_globals;
class atp_struct;
class sql_buffer_pool;

/////////////////////////////////////////////////////////////////////////
//
// Class keyRangeGen
//
// This class contains compiler-generated information used by scan
// operators that can access ordered data directly.  It is a virtual
// class that defines a common interface for different types of key
// access.  The idea is to encapsulate the generation of key ranges.
//
//
/////////////////////////////////////////////////////////////////////////

#include "comexe/ComKeyRange.h"

/////////////////////////////////////////////////////////////////////////
//
// Class keyRangeEx
//
// This class contains compiler-generated information used by scan
// operators that can access ordered data directly.  It is a virtual
// class that defines a common interface for different types of key
// access.  The idea is to encapsulate the generation of key ranges.
//
//
/////////////////////////////////////////////////////////////////////////

class keyRangeEx : public ExGod {
 protected:
  const keyRangeGen &tdbKey_;  // reference to TDB side structure
  ex_globals *globals_;        // so freeResources() can get to it

  // key range information computed by getNextKeyRange() is stored here
  tupp bkData_;
  tupp ekData_;
  short bkExcludeFlag_;
  short ekExcludeFlag_;

  // work variables
  atp_struct *workAtp_;
  int excludeFlag_;
  tupp_descriptor excludeFlagTupp_;
  int dataConvErrorFlag_;
  tupp_descriptor dataConvErrorFlagTupp_;

 public:
  keyRangeEx(const keyRangeGen &tdb_key, ex_globals *globals, const short in_version, sql_buffer_pool *pool);

  virtual ~keyRangeEx();

  virtual void freeResources();  // free resources

  virtual void release();  // release

  virtual void display() const {};

  // key range iterator methods
  virtual ExeErrorCode initNextKeyRange(sql_buffer_pool *pool, atp_struct *atp0) = 0;

  enum getNextKeyRangeReturnType { NO_MORE_RANGES, FETCH_RANGE, PROBE_RANGE, EXPRESSION_ERROR };

  virtual getNextKeyRangeReturnType getNextKeyRange(atp_struct *atp0, NABoolean fetchRangeHadRows,
                                                    NABoolean detectNullRange = TRUE) = 0;

  // on the next method, we pass in NULL if a probe resulted in no
  // data; we pass in the encoded key value if data was obtained
  virtual void reportProbeResult(char *) {
    ex_assert(0, "reportProbeResult() was called when it shouldn't have been");
  };

  // when getNextKeyRange() returns FETCH_RANGE or PROBE_RANGE, the
  // caller should call the next four methods to get its hands on the
  // key values
  tupp &getBkData() { return bkData_; };

  tupp &getEkData() { return ekData_; };

  short getBkExcludeFlag() { return bkExcludeFlag_; };

  short getEkExcludeFlag() { return ekExcludeFlag_; };

  int getKeyLength() { return tdbKey_.getKeyLength(); };
};

#endif
