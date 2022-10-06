
/* -*-C++-*-
******************************************************************************
*
* File:         LmConnection.cpp
* Description:  Stores data about about JDBC/MX connection created by the SPJ
*               methods.
*
* Created:      12/20/2005
* Language:     C++
*
******************************************************************************
*/
#include "LmConnection.h"

#include "LmDebug.h"
#include "common/Platform.h"
#include "lmjni.h"

// Constructor: Initializes data members
LmConnection::LmConnection(LmLanguageManagerJava *lm, jobject connRef, LmConnectionType connType,
                           ComRoutineTransactionAttributes transactionAttrs)
    : type_(connType), refCnt_(0), lmj_(lm), jdbcConnRef_(connRef), transactionAttrs_(transactionAttrs) {}

// Destructor:
// Deletes the 'jdbcConnRef_' global reference
LmConnection::~LmConnection() {
  JNIEnv *jni = (JNIEnv *)lmj_->jniEnv_;
  jni->DeleteGlobalRef(jdbcConnRef_);
}

// Decrements 'refCnt_' by '1' and when it becomes '0'
// calls the close() method of this class.
// Asserts if 'refCnt_' value goes lesser than '0'
//
// Returns TRUE if the close() method is called, FALSE
// otherwise.
NABoolean LmConnection::decrRefCnt(ComDiagsArea *diags) {
  refCnt_--;

  if (refCnt_ == 0) {
    close(diags);
    return TRUE;
  } else if (refCnt_ < 0) {
    // refCnt_ should not go below zero. If this happens then
    // things have gone wrong in maintaining the reference count
    LM_ASSERT(refCnt_ > 0);
  }

  return FALSE;
}

// Calls the close() method on 'jdbcConnRef_' which
// is the java.sql.Connection object
// Calls the destructor of this object
void LmConnection::close(ComDiagsArea *diags) {
  LM_ASSERT(refCnt_ == 0);

  JNIEnv *jni = (JNIEnv *)lmj_->jniEnv_;

  if (jdbcConnRef_) {
    // Check if the connection was closed by SPJ method
    jboolean connIsClosed = jni->CallBooleanMethod(jdbcConnRef_, (jmethodID)lmj_->connIsClosedId_);

    LM_DEBUG1("Connection.isClosed() returned %s", jni->ExceptionOccurred() ? "an Exception" : "no Exception");
    if (jni->ExceptionOccurred()) {
      // Populate ComDiagsArea if there are any Java exceptions
      if (diags)
        lmj_->exceptionReporter_->checkJVMException(diags, 0);
      else
        jni->ExceptionClear();  // If ComDiagsArea is not available then we clear
                                // any pending exception
    } else if (!connIsClosed) {
      LM_DEBUG1("LmConnection::close() closing Connection of type %s",
                (type_ == DEFAULT_CONN ? "DEFAULT_CONNECTION" : "NON_DEFAULT_CONNECTION"));

      jni->CallVoidMethod(jdbcConnRef_, (jmethodID)lmj_->connCloseId_);

      LM_DEBUG1("Connection.close() returned %s", jni->ExceptionOccurred() ? "an Exception" : "no Exception");

      // Populate ComDiagsArea if there are any Java exceptions
      if (diags)
        lmj_->exceptionReporter_->checkJVMException(diags, 0);
      else
        jni->ExceptionClear();  // If ComDiagsArea is not available then we clear
                                // any pending exception
    }
  }

  delete this;
}
