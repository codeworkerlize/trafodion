// **********************************************************************

// **********************************************************************
#include "CmpProcess.h"

#include <cstdlib>

#include "common/ComRtUtils.h"

#include "seabed/fs.h"
#include "seabed/ms.h"

// -----------------------------------------------------------------------
// Methods for class CmpProcess
// -----------------------------------------------------------------------

/************************************************************************
constructor CmpProcess

 Get some basic information about this process and setup the class.
 The process info gathered below only needs to happen once.

************************************************************************/
CmpProcess::CmpProcess() : nodeNum_(0), pin_(0), segmentNum_(0), processStartTime_(0) {
  char programDir[100];
  short processType;
  int myCPU;
  char myNodeName[MAX_SEGMENT_NAME_LEN + 1];
  int myNodeNum;
  short myNodeNameLen = MAX_SEGMENT_NAME_LEN;
  char myProcessName[PROCESSNAME_STRING_LEN];
  pid_t pid;
  if (!ComRtGetProgramInfo(programDir, 100, processType, myCPU, pid, myNodeNum, myNodeName, myNodeNameLen,
                           processStartTime_, myProcessName)) {
    pin_ = pid;
    nodeNum_ = myNodeNum;
    // convert to local civil time
    processStartTime_ = CONVERTTIMESTAMP(processStartTime_, 0, -1, 0);
  }
}
/************************************************************************
method CmpProcess::getProcessDuration

  the number of microseconds since this process started

************************************************************************/
long CmpProcess::getProcessDuration() {
  long currentTime = getCurrentTimestamp();
  return (currentTime - processStartTime_);
}
/************************************************************************
method CmpProcess::getCurrentSystemHeapSize

 Guardian procedure calls used to get the latest memory usage information
 for this process.

************************************************************************/
int CmpProcess::getCurrentSystemHeapSize() {
  int currentSystemHeapSize = 0;

  return currentSystemHeapSize;
}
/************************************************************************
method CmpProcess::getCompilerId

parameter:
   char *id - pass an empty buffer in and get the compiler id as output

 the compiler id is made up of:

 process start timestamp - 18 digits
 node (cpu) #            - 3 digits
 pin #                   - 5 digits


************************************************************************/
void CmpProcess::getCompilerId(char *id, int len) {
  CMPASSERT(NULL != id);

  snprintf(id, len, "%018ld%03d%05d", getProcessStartTime(), getNodeNum(), getPin());
}
