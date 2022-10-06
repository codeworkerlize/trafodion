/* -*-C++-*-
 *****************************************************************************
 *
 * File:         dstestpoint.h
 * Description:  Interface to the testing hooks that are used to test the
 *               recovery of failed utility operations.
 *
 *
 * Created:      December 12, 2003
 * Modified:	 July 20, 2006
 * Language:     C++
 *
 *

 *
 *****************************************************************************
*/
#ifndef NATESTPOINT_H
#define NATESTPOINT_H

#include "common/Collections.h"
#include "common/ComASSERT.h"
#include "common/NABoolean.h"
#include "common/NAString.h"
#include "common/Platform.h"

#define DETAILS_NOT_DEFINED         -1
#define RQST_LEN                    10
#define IDS_PM_ERROR_MSG_TEST_POINT 0x00005207L  // MSG_20999 from sqlutils_msg.h
#define NSK_FILE_SYSTEM_ERROR       8551

// Lists the available test points.
enum ETestPointValue {
  TESTPOINT_0,  // CmpSeabaseDDL::modifyObjectEpochCacheStartDDL after obtaining DDL lock
  TESTPOINT_1,  // label_restore (error recovery logic) in ALTER TABLE ALTER COLUMN
  TESTPOINT_2,  // the rest of these are unused at the moment
  TESTPOINT_3,
  TESTPOINT_4,
  TESTPOINT_5,
  TESTPOINT_6,
  TESTPOINT_7,
  TESTPOINT_8,
  TESTPOINT_9,

  LAST_TESTPOINT  // used to size the array in class CNATestPointArray
};

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Definition of class CNATestPoint
//
// This class contains the basic support for test points
//
//
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class CNATestPoint {
 public:
  // --------------------------------------------------------------------------
  // enums:
  // --------------------------------------------------------------------------

  // Describes the different types of test point requests
  enum ETestPointRqst { eUNKNOWN, eKILL, eERROR, eFSERROR, eTRAP, eDELAY };

  CNATestPoint(int number, int iterator = 1, ETestPointRqst rqst = eKILL);

  // --------------------------------------------------------------------------
  // constructors/destructors
  // --------------------------------------------------------------------------
  CNATestPoint(const CNATestPoint &testPoint);
  virtual ~CNATestPoint();

  // accessors
  int GetTestPoint() const { return m_iTestPoint; }
  int GetIterator() const { return m_iIterator; }
  int GetInnerLoopIterator() const { return m_iInnerLoopIterator; }
  CNATestPoint::ETestPointRqst GetRqst() const { return m_eRqst; }
  void GetRqstText(char *text);
  int GetDelayTime() const { return m_iDelayTime; }
  int GetError() const { return m_iError; }
  int GetFSError() const { return m_iFSError; }
  int GetTrapError() const { return m_iTrapError; }
  int GetDetails();

  // mutators
  void SetTestPoint(const int number) { m_iTestPoint = number; }
  void SetIterator(const int iterator) { m_iIterator = iterator; }
  void SetInnerLoopIterator(const int innerLoopIterator) { m_iInnerLoopIterator = innerLoopIterator; }
  void SetRqst(const CNATestPoint::ETestPointRqst rqst) { m_eRqst = rqst; }
  void SetDelayTime(const int delayTime);
  void SetError(const int error) { m_iError = error; }
  void SetFSError(const int fsError) { m_iFSError = fsError; }
  void SetTrapError(const int trapError);

  int Execute(void);
  void Wait(int delayTime_in_millisecs);

 protected:
  // --------------------------------------------------------------------------
  // constructors/destructors
  // --------------------------------------------------------------------------
  CNATestPoint();

 private:
  int m_iTestPoint;
  int m_iIterator;           // iteration of the outermost loop
  int m_iInnerLoopIterator;  // iteration of inner loop - 0 if no inner loop
  ETestPointRqst m_eRqst;
  int m_iDelayTime;
  int m_iError;
  int m_iFSError;
  int m_iTrapError;

  void RecursiveCall(char buffer[100000]);
};

class CNATestPointList : public NAList<CNATestPoint *> {
 private:
  CNATestPoint::ETestPointRqst ConvertStrToENum(const NAString rqstStr);

 public:
  enum EOwnership { eItemsAreOwned, eItemsArentOwned };

  CNATestPointList(EOwnership ownership = eItemsAreOwned);
  ~CNATestPointList();

  inline void AddTestPoint(const int number, const int iterator, const NAString rqstStr, const int details);

  void AddTestPoint(const int number, const int outermostLoopIterator, const int innerLoopIterator,
                    const NAString rqstStr, const int details);

  CNATestPoint *Find(const int number,
                     const int iterator,                // iteration of outermost loop
                     const int innerLoopIterator = 0);  // 0: no inner loop
 private:
  EOwnership m_ownership;
};

// =======================================================================
// In-line methods for class CNATestPointList
// =======================================================================

// ---------------------------------------------------------------------
// Method: AddTestPoint
//
// Adds a test point to the test point list
//
// Input:
//   number - the test point number
//   iterator - the iteration for the test point, that is - what to do
//              on the nth iteration of the testpoint
//   rqstStr - what to do when executed (TRAP, ERROR, FSERROR, DELAY, KILL)
//   details - optional details: e.g. how long to wait for a DELAY
// ---------------------------------------------------------------------
inline void CNATestPointList::AddTestPoint(const int number, const int iterator, const NAString rqstStr,
                                           const int details) {
  AddTestPoint(number, iterator,
               0,  // no inner loop
               rqstStr, details);
}

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Definition of class CNATestPointArray
//
// This class allows fast access to test points by test point
// number.
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class CNATestPointArray {
 public:
  CNATestPointArray(NAHeap *heap);
  ~CNATestPointArray();

  bool configureTestPoint(const NAString &testPointSpec);  // returns false if bad spec
  bool configureDelayTestPoint(enum ETestPointValue testPoint, const int delayInSeconds);
  void resetAllTestPoints();
  int executeTestPoint(enum ETestPointValue testPoint);

 private:
  NAHeap *heap_;

  CNATestPoint *testPoints_[LAST_TESTPOINT];  // array of pointers to CNATestPoint
};

#endif  // NATESTPOINT_H
