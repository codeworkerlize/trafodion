/* -*-C++-*-
 *****************************************************************************
 *
 * File:         dstestpoint.cpp
 * Description:  Test points to help with testing the recovery of failed
 *               utility operations.
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

#include "common/NATestpoint.h"
#include "export/NAStringDef.h"

// Get the external declaration of NAAbort()
#include "common/BaseTypes.h"

// =======================================================================
// Non in-line methods for class CNATestPoint
// =======================================================================

// -----------------------------------------------------------------------
// Default constructor
// -----------------------------------------------------------------------
CNATestPoint::CNATestPoint()
    : m_iTestPoint(0),
      m_iIterator(1),
      m_eRqst(eKILL),
      m_iDelayTime(20),
      m_iError(20999),
      m_iFSError(40),
      m_iTrapError(8) {}

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
CNATestPoint::CNATestPoint(int testPoint, int iterator, CNATestPoint::ETestPointRqst rqst)
    : m_iTestPoint(testPoint),
      m_iIterator(iterator),
      m_iInnerLoopIterator(0),
      m_eRqst(rqst),
      m_iDelayTime(20),
      m_iError(20999),
      m_iFSError(40),
      m_iTrapError(8) {}

// -----------------------------------------------------------------------
// Copy constructor
// -----------------------------------------------------------------------
CNATestPoint::CNATestPoint(const CNATestPoint &testPoint)
    : m_iTestPoint(testPoint.GetTestPoint()),
      m_iIterator(testPoint.GetIterator()),
      m_iInnerLoopIterator(testPoint.GetInnerLoopIterator()),
      m_eRqst(testPoint.GetRqst()),
      m_iDelayTime(testPoint.GetDelayTime()),
      m_iError(testPoint.GetError()),
      m_iFSError(testPoint.GetFSError()),
      m_iTrapError(testPoint.GetTrapError()) {}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
CNATestPoint::~CNATestPoint() {}

// -----------------------------------------------------------------------
// Method: GetRqstText
//
// Returns the request text in character format
// The caller needs to send in a character string at least RQST_LEN
// bytes long (defined in dstestpoint.h file)
// -----------------------------------------------------------------------
void CNATestPoint::GetRqstText(char *text) {
  ComASSERT(text);
  switch (m_eRqst) {
    case CNATestPoint::eKILL:
      strcpy(text, "KILL");
      break;
    case CNATestPoint::eERROR:
      strcpy(text, "ERROR");
      break;
    case CNATestPoint::eFSERROR:
      strcpy(text, "FSERROR");
      break;
    case CNATestPoint::eTRAP:
      strcpy(text, "TRAP");
      break;
    case CNATestPoint::eDELAY:
      strcpy(text, "DELAY");
      break;
    default:
      strcpy(text, "UNKNOWN");
      break;
  }
}
// -----------------------------------------------------------------------
// Method:  GetDetails
//
// Returns detailed information about testpoints
// -----------------------------------------------------------------------
int CNATestPoint::GetDetails() {
  int value = DETAILS_NOT_DEFINED;
  switch (m_eRqst) {
    case CNATestPoint::eERROR:
      value = m_iError;
      break;
    case CNATestPoint::eFSERROR:
      value = m_iFSError;
      break;
    case CNATestPoint::eTRAP:
      value = m_iTrapError;
      break;
    case CNATestPoint::eDELAY:
      value = m_iDelayTime;
      break;
    default:
      break;
  }
  return value;
}

// -----------------------------------------------------------------------
// Method: Execute
//
// Executes the requested testpoint
// -----------------------------------------------------------------------
Int32 CNATestPoint::Execute(void) {
  Int32 executeSuccessful = 1;
  switch (m_eRqst) {
    case (CNATestPoint::eERROR): {
      if (m_iError == IDS_PM_ERROR_MSG_TEST_POINT) {
        return IDS_PM_ERROR_MSG_TEST_POINT;
      }
      break;
    }

    case (CNATestPoint::eFSERROR): {
      return NSK_FILE_SYSTEM_ERROR;
      break;
    }

    case CNATestPoint::eDELAY: {
      Wait(m_iDelayTime * 1000);
      break;
    }

    case CNATestPoint::eTRAP: {
      // The NT code does not support SIGFPE, SIGSEGV,  and SIGSTK.  So
      // to avoid special code in NSK and NT, the numeric values are used
      // in comparisons.
      // 8 - (SIGFPE): floating point exception -- arithmetic over/underflow
      if (m_iTrapError == 8) {
        Int32 divisor = 0;
        Int32 result = 100 / divisor;
      }

      // 11 - (SIGSEGV): segmentation violation -- using an invalid address
      else if (m_iTrapError == 11) {
        NAString *pString = NULL;
        pString = pString - 1;
        pString->toUpper();
      }

      // 25 - (SIGSTK):  stack overflow
      else if (m_iTrapError == 25) {
        char buffer[100000];
        RecursiveCall(buffer);
      }

      else  // invalid trap code
        executeSuccessful = 0;
      break;
    }

    case CNATestPoint::eKILL: {
      NAString msg("Utility code died at test point ");
      NAString msg_temp;
      msg_temp = LongToNAString((int)m_iTestPoint);
      msg += msg_temp;
      msg += " Iterator: ";
      msg_temp = LongToNAString((int)m_iIterator);
      msg += msg_temp;
      NAAbort("NATestpoint.cpp", __LINE__, (char *)msg.data());

      break;
    }

    default: {
      executeSuccessful = 0;
    }
  }
  return executeSuccessful;
}

// ------------------------------------------------------------------------
// Method:  SetDelayTime:
//
// This method validates the delay time and sets it up in the class.
// If the delay time is greater than 1 hours - it is set to 1 hour
// If the delay time is less than or equal to 0 - it is set to 20 seconds
// ------------------------------------------------------------------------
void CNATestPoint::SetDelayTime(const int delayTime) {
  if (delayTime <= 0)
    m_iDelayTime = 20;
  else
    m_iDelayTime = (delayTime > 3600) ? 3600 : delayTime;
}

// ------------------------------------------------------------------------
// Method:  SetTrapError:
//
// This method validates the trap error and sets it up in the class.
// The following traps are supported:
//     8 (SIGFPE):  floating point exception
//     11 (SIGSEGV): segmentation violation
//     25 (SIGSTK):  stack overflow
// If requested TRAP error is not SIGSTK, SIGSEGV, or SIGFPE,
// it is set to SIGFPE
//
// Note: The NT code does not support SIGFPE, SIGSEGV,  and SIGSTK.  So
// to avoid special code in NSK and NT, the numeric values are used
// in comparisons.
// ------------------------------------------------------------------------
void CNATestPoint::SetTrapError(const Int32 trapError) {
  if (trapError == 8 || trapError == 11 || trapError == 25)
    m_iTrapError = trapError;
  else
    m_iTrapError = DETAILS_NOT_DEFINED;
}

// -------------------------------------------------------------------
// Method:  RecursiveCall
//
//  A function that keeps calling itself, used to create a stack
//  overflow trap.
// -------------------------------------------------------------------
void CNATestPoint::RecursiveCall(char buffer[100000]) {
  char a[100000];
  RecursiveCall(a);
}

// -------------------------------------------------------------------
// Method: Wait
//
// A function that suspense the process for the specified duration
// in milliseconds. This function is copied from dsguardiancalls.cpp
// -------------------------------------------------------------------
void CNATestPoint::Wait(int delayTime_in_millisecs) {
  if (delayTime_in_millisecs == 0) return;
  // The Sleep() function can be used to give up the processor.
  Sleep(delayTime_in_millisecs);
}

// =======================================================================
// Non in-line methods for class CNATestPointList
// =======================================================================

// ---------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------
CNATestPointList::CNATestPointList(EOwnership ownership) : LIST(CNATestPoint *)(NULL), m_ownership(ownership) {}

// ---------------------------------------------------------------------
// Destructor
// ---------------------------------------------------------------------
CNATestPointList::~CNATestPointList() {}

// ---------------------------------------------------------------------
// Method: AddTestPoint
//
// Adds a test point to the test point list
//
// Input:
//   number - the test point number
//   iterator - the iteration for the test point, that is - what to do
//              on the nth iteration of the testpoint when the following
//              parameter innerLoopIterator contains the zero value.
//   innerLoopIterator - the iteration of the inner loop - is 0 if there
//                       is no inner loop; otherwise, specifies what
//                       to do on the (n*innerLoopIterator)th iteration
//                       of the test point.
//   rqstStr - what to do when executed (TRAP, ERROR, FSERROR, DELAY, KILL)
//   details - optional details: e.g. how long to wait for a DELAY
// ---------------------------------------------------------------------
void CNATestPointList::AddTestPoint(const int number, const int iterator, const int innerLoopIterator,
                                    const NAString rqstStr, const Int32 details) {
  CNATestPoint::ETestPointRqst rqst = ConvertStrToENum(rqstStr);
  CNATestPoint *pTestPoint = new CNATestPoint(number, iterator, rqst);
  pTestPoint->SetInnerLoopIterator(innerLoopIterator);
  if (details != DETAILS_NOT_DEFINED) {
    switch (rqst) {
      case CNATestPoint::eTRAP:
        pTestPoint->SetTrapError(details);
        break;
      case CNATestPoint::eERROR:
        pTestPoint->SetError(details);
        break;
      case CNATestPoint::eFSERROR:
        pTestPoint->SetFSError(details);
        break;
      case CNATestPoint::eDELAY:
        pTestPoint->SetDelayTime(details);
        break;
      default: {
      }
    }
  }

  insert(pTestPoint);
}

// ---------------------------------------------------------------------
// Method:  ConvertStrToENum
//
// This converts the request as entered by the user from the string
// value to the ETestPointRqst enum.
//
// Input:
//   rqstStr - the string value representing the request to convert
//
// Output:
//   Returns an ENUM value that represents the request
// ---------------------------------------------------------------------
CNATestPoint::ETestPointRqst CNATestPointList::ConvertStrToENum(const NAString rqstStr) {
  CNATestPoint::ETestPointRqst rqst;

  if (rqstStr == "KILL")
    rqst = CNATestPoint::eKILL;
  else if (rqstStr == "TRAP")
    rqst = CNATestPoint::eTRAP;
  else if (rqstStr == "ERROR")
    rqst = CNATestPoint::eERROR;
  else if (rqstStr == "FSERROR")
    rqst = CNATestPoint::eFSERROR;
  else if (rqstStr == "DELAY")
    rqst = CNATestPoint::eDELAY;
  else
    rqst = CNATestPoint::eUNKNOWN;

  return rqst;
}

// ---------------------------------------------------------------------
// Method:  Find
//
// This method searches the list of test points for the requested
// value
//
// Input:
//   number - the test point number to find
//   iterator - the (outermost) iteration for the test point
//   innerLoopIterator - the iteration of the inner loop - is 0 if
//                       there is no inner loop
//
// Output:
//   Returns a pointer to a CNATestPoint class or NULL, the calling
//   program should check for NULL.
// ---------------------------------------------------------------------
CNATestPoint *CNATestPointList::Find(const int number, const int iterator, const int innerLoopIterator) {
  CNATestPoint *pTestPoint;
  for (CollIndex i = 0; i < entries(); i++) {
    pTestPoint = at(i);
    if (pTestPoint && pTestPoint->GetTestPoint() == number && pTestPoint->GetIterator() == iterator &&
        pTestPoint->GetInnerLoopIterator() == innerLoopIterator)
      return pTestPoint;
  }
  return NULL;
}

// =======================================================================
// Non in-line methods for class CNATestPointArray
// =======================================================================

CNATestPointArray::CNATestPointArray(NAHeap *heap) : heap_(heap) {
  for (size_t i = 0; i < LAST_TESTPOINT; i++) {
    testPoints_[i] = NULL;
  }
}

CNATestPointArray::~CNATestPointArray() {
  resetAllTestPoints();  // get rid of all objects we own
}

// helper functions for lexical aspects of configureTestPoint method

// returns number of characters consumed; returns numeric value in "out"
// if valid, returns -1 in "out" if not valid
size_t pickOffNumeric(const char *next, size_t len, int &out) {
  size_t consumed = 0;
  out = -1;                            // to make sure there is at least one digit
  while ((len > 0) && (*next == '0'))  // strip off leading zeros
  {
    next++;
    consumed++;
    len--;
  }
  if (len > 0) {
    char number[len + 1];
    size_t i = 0;
    while ((len > 0) && isdigit(*next)) {
      number[i++] = *next;
      next++;
      consumed++;
      len--;
    }
    number[i] = '\0';
    if (i == 0)
      out = 0;
    else if (i < 10)
      out = atoi(number);
  } else if (consumed > 0)
    out = 0;  // the numeric was '0' (or multiple zeros)

  return consumed;
}

// picks off upshifted string up to ":" or end-of-string; "out" is
// assumed to be big enough to hold all of "next"; returns number of
// characters consumed
size_t pickOffUpshiftedString(const char *next, size_t len, char *out) {
  size_t consumed = 0;
  while ((len > 0) && (*next != ':')) {
    out[consumed++] = toupper(*next);
    next++;
    len--;
  }
  out[consumed] = '\0';
  return consumed;
}

bool CNATestPointArray::configureTestPoint(const NAString &testPointSpec) {
  // the testPointSpec is expected to be one of the following:
  //
  // all blank -- in which case we do nothing
  // <value>:<action>:<param> -- giving the number, action and param for the test point
  bool rc = false;  // assume testPointSpec is invalid
  const char *next = testPointSpec.data();
  size_t len = testPointSpec.length();

  while ((len > 0) && (*next == ' ')) {
    next++;
    len--;
  }
  if (len == 0)  // if the spec is all white space
    rc = true;   // do nothing and return
  else           // analyze and act on testPointSpec
  {
    int testPointValue = 0;
    size_t consumed = pickOffNumeric(next, len, testPointValue /* out */);
    if ((consumed > 0) && (testPointValue >= 0)) {
      next += consumed;  // step over <value>
      len -= consumed;
      if ((len > 0) && (*next == ':')) {
        next++;  // step over ':'
        len--;
        char action[len + 1];
        consumed = pickOffUpshiftedString(next, len, action /* out */);
        if (strcmp(action, "DELAY") == 0) {
          next += consumed;  // step over <action>
          len -= consumed;
          if (len > 0) {
            next++;  // step over ':'
            len--;
            int testPointParam = 0;
            consumed = pickOffNumeric(next, len, testPointParam /* out */);
            if ((consumed == len) && (testPointParam >= 0)) {
              // it's all good; configure a DELAY test point
              rc = configureDelayTestPoint((ETestPointValue)testPointValue, testPointParam);
            }
          }
        }
      }
    }
  }
  return rc;
}

bool CNATestPointArray::configureDelayTestPoint(enum ETestPointValue testPoint, const int delayInSeconds) {
  bool rc = false;                                       // assume testPoint is invalid
  if ((testPoint >= 0) && (testPoint < LAST_TESTPOINT))  // do nothing if testPoint is out of bounds
  {
    if (testPoints_[testPoint])  // if already configured, get rid of old one
    {
      NADELETE(testPoints_[testPoint], CNATestPoint, heap_);
      testPoints_[testPoint] = NULL;
    }
    testPoints_[testPoint] = new (heap_) CNATestPoint(testPoint, 1, CNATestPoint::eDELAY);
    testPoints_[testPoint]->SetDelayTime(delayInSeconds);
    rc = true;
  }
  return rc;
}

void CNATestPointArray::resetAllTestPoints() {
  for (size_t i = 0; i < LAST_TESTPOINT; i++) {
    if (testPoints_[i]) {
      NADELETE(testPoints_[i], CNATestPoint, heap_);
      testPoints_[i] = NULL;
    }
  }
}

Int32 CNATestPointArray::executeTestPoint(enum ETestPointValue testPoint) {
  Int32 retcode = 0;
  if ((testPoint >= 0) && (testPoint < LAST_TESTPOINT))  // do nothing if testPoint is out of bounds
  {
    if (testPoints_[testPoint])  // do nothing if testPoint is not configured
    {
      cout << "About to execute test point " << testPoint << endl;
      retcode = testPoints_[testPoint]->Execute();
      cout << "Completed executing test point " << testPoint << endl;
    }
  }
  return retcode;
}
