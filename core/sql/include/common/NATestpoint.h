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
 *
 *****************************************************************************
*/
#ifndef NATESTPOINT_H
#define NATESTPOINT_H

#include "common/Collections.h"
#include "common/Platform.h"
#include "common/NABoolean.h"
#include "common/NAString.h"
#include "common/ComASSERT.h"

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

  CNATestPoint(Lng32 number, Lng32 iterator = 1, ETestPointRqst rqst = eKILL);

  // --------------------------------------------------------------------------
  // constructors/destructors
  // --------------------------------------------------------------------------
  CNATestPoint(const CNATestPoint &testPoint);
  virtual ~CNATestPoint();

  // accessors
  Lng32 GetTestPoint() const { return m_iTestPoint; }
  Lng32 GetIterator() const { return m_iIterator; }
  Lng32 GetInnerLoopIterator() const { return m_iInnerLoopIterator; }
  CNATestPoint::ETestPointRqst GetRqst() const { return m_eRqst; }
  void GetRqstText(char *text);
  Lng32 GetDelayTime() const { return m_iDelayTime; }
  Int32 GetError() const { return m_iError; }
  Int32 GetFSError() const { return m_iFSError; }
  Int32 GetTrapError() const { return m_iTrapError; }
  Lng32 GetDetails();

  // mutators
  void SetTestPoint(const Lng32 number) { m_iTestPoint = number; }
  void SetIterator(const Lng32 iterator) { m_iIterator = iterator; }
  void SetInnerLoopIterator(const Lng32 innerLoopIterator) { m_iInnerLoopIterator = innerLoopIterator; }
  void SetRqst(const CNATestPoint::ETestPointRqst rqst) { m_eRqst = rqst; }
  void SetDelayTime(const Lng32 delayTime);
  void SetError(const Int32 error) { m_iError = error; }
  void SetFSError(const Int32 fsError) { m_iFSError = fsError; }
  void SetTrapError(const Int32 trapError);

  Int32 Execute(void);
  void Wait(Lng32 delayTime_in_millisecs);

 protected:
  // --------------------------------------------------------------------------
  // constructors/destructors
  // --------------------------------------------------------------------------
  CNATestPoint();

 private:
  Lng32 m_iTestPoint;
  Lng32 m_iIterator;           // iteration of the outermost loop
  Lng32 m_iInnerLoopIterator;  // iteration of inner loop - 0 if no inner loop
  ETestPointRqst m_eRqst;
  Lng32 m_iDelayTime;
  Int32 m_iError;
  Int32 m_iFSError;
  Int32 m_iTrapError;

  void RecursiveCall(char buffer[100000]);
};

class CNATestPointList : public NAList<CNATestPoint *> {
 private:
  CNATestPoint::ETestPointRqst ConvertStrToENum(const NAString rqstStr);

 public:
  enum EOwnership { eItemsAreOwned, eItemsArentOwned };

  CNATestPointList(EOwnership ownership = eItemsAreOwned);
  ~CNATestPointList();

  inline void AddTestPoint(const Lng32 number, const Lng32 iterator, const NAString rqstStr, const Int32 details);

  void AddTestPoint(const Lng32 number, const Lng32 outermostLoopIterator, const Lng32 innerLoopIterator,
                    const NAString rqstStr, const Int32 details);

  CNATestPoint *Find(const Lng32 number,
                     const Lng32 iterator,                // iteration of outermost loop
                     const Lng32 innerLoopIterator = 0);  // 0: no inner loop
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
inline void CNATestPointList::AddTestPoint(const Lng32 number, const Lng32 iterator, const NAString rqstStr,
                                           const Int32 details) {
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
  bool configureDelayTestPoint(enum ETestPointValue testPoint, const Lng32 delayInSeconds);
  void resetAllTestPoints();
  Int32 executeTestPoint(enum ETestPointValue testPoint);

 private:
  NAHeap *heap_;

  CNATestPoint *testPoints_[LAST_TESTPOINT];  // array of pointers to CNATestPoint
};

#endif  // NATESTPOINT_H
