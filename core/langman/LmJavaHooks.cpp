
/* -*-C++-*-
******************************************************************************
*
* File:         LmJavaHooks.cpp
* Description:  A class containing static functions that we register
*               as callback functions when we create a JVM
*
* Created:      June 2003
* Language:     C++
*
*
******************************************************************************
*/

#include "LmJavaHooks.h"

#include "common/ComSafePrinter.h"
#include "LmDebug.h"
#include "langman/LmExtFunc.h"
#include "common/BaseTypes.h"
#include "common/str.h"

char LmJavaHooks::textBuf_[LMJ_HOOK_TEXT_BUF_SIZE] = {'\0'};

// Exclude the following methods for coverage as they are called when JVM aborts
void JNICALL LmJavaHooks::abortHookJVM() {
  LM_DEBUG0("[HOOK] Invoking JVM abort hook");
  fprintf(stdout, "The JVM is about to abort\n");
  fflush(stderr);

  lmMakeTFDSCall("The Java virtual machine aborted", "", 0);
  // should not reach here

  abort();
}

void JNICALL LmJavaHooks::exitHookJVM(jint code) {
  LM_DEBUG1("[HOOK] Invoking JVM exit hook. Exit code is %d", (int)code);
  fprintf(stdout, "The JVM is about to exit with code %d\n", (int)code);
  fflush(stderr);

  char buf[200];
  sprintf(buf, "The Java virtual machine exited with code %d", (int)code);
  lmMakeTFDSCall(buf, "", 0);
  // should not reach here

  exit(code);
}

jint JNICALL LmJavaHooks::vfprintfHookJVM(FILE *stream, const char *fmt, va_list printArgs) {
  if (!fmt || !stream) {
    return 0;
  }

  LM_DEBUG0("[BEGIN vfprintf hook]");

  char tmpBuf[LMJ_HOOK_TEXT_BUF_SIZE] = "";

  // 1. First make a temporary copy of the output message
  ComSafePrinter safePrinter;
  safePrinter.vsnPrintf(tmpBuf, LMJ_HOOK_TEXT_BUF_SIZE, fmt, printArgs);

  // 2. Send the message to its intended FILE stream
  // Do not know why but inside this hook, writing to the FILE pointer
  // that was passed in seems to cause problems on Windows. Depending
  // on how the FILE pointer is used we have seen crashes inside JVM
  // code after hook returns, and inside fprintf. The JVM bug database
  // says a few things about the JVM being compiled with a version of
  // certain stdio structures such as va_list that may not be
  // compatible with all IDEs. Have not tried to verify that
  // though. For now on Windows we just write our temporary string to
  // stderr. And our observation has been that all JVM messages go to
  // stderr anyway, so this seems to serve the same purpose.
  //
  // For example the following line does not work
  //
  //   vfprintf(stream, fmt, printArgs)
  //
  // And neither does this
  //
  //   fprintf(stream, tmpBuf)
  //
  fprintf(stderr, "%s", tmpBuf);

  // Write as much as possible into our textBuf_ area
  int currentLength = str_len(textBuf_);
  int tmpLen = str_len(tmpBuf);
  if ((currentLength + 1) < LMJ_HOOK_TEXT_BUF_SIZE) {
    int bytesToCopy = MINOF(tmpLen + 1, LMJ_HOOK_TEXT_BUF_SIZE - currentLength);
    str_cpy_all(&textBuf_[currentLength], tmpBuf, bytesToCopy);
    textBuf_[LMJ_HOOK_TEXT_BUF_SIZE - 1] = 0;
  } else {
    LM_DEBUG0("*** WARNING: textBuf_ buffer is full");
  }

  LM_DEBUG0("[END vfprintf hook]");
  return 0;
}

void LmJavaHooks::init_vfprintfHook() { textBuf_[0] = '\0'; }
