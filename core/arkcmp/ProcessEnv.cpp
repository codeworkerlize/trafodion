
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ProcessEnv.C
 * Description:  The implementation of the class ProcessEnv, used to process
 *               the environment variables (defines, ...etc ) information.
 *               passed by executor.
 *
 *
 * Created:      9/05/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "arkcmp/ProcessEnv.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fstream>
#include <iostream>

#include "common/CmpCommon.h"
#include "common/Collections.h"
#include "common/Platform.h"

extern char **environ;
#define ENVIRON environ
#define PUTENV  putenv

ProcessEnv::ProcessEnv(NAMemory *heap) : heap_(heap), envs_(heap, 64) {}

ProcessEnv::~ProcessEnv() { cleanup(); }

void ProcessEnv::cleanup() {
  for (CollIndex i = 0; i < envs_.entries(); i++) NADELETEBASIC(envs_[i], heap_);
  envs_.clear();
}

// To make sure the environment variables are the same as in executor
// (add new ones, delete the ones been deleted from executor.)
// The best way will be to
//   1. delete all environment variables first
//   2. add all the environment variables from executor.
// But PUTENV(system call) in NT creates a lot of memory leak, since this
// operation is needed for every request, the leak accumulates.
//
// An alternative was explored that instead of using PUTENV, ENVIRON is
// directly manipulated to save the memory and speed up execution, but
// if later in the user's code, PUTENV is called, this system call will
// manipulate the storage of ENVIRON which caused problems.
//
// To make sure the environment is the same as in executor and the code
// linked into arkcmp is not limited to unable to call PUTENV, the following
// is done :
// . envs_ private member in ProcessEnv contains the environment variables
//   being passed over from executor last time.
// . For newenvs coming in this time, it is compared against envs_ for
//   appropriate operations.
//
// For every entry in newenvs, if
// . it is not the same as in envs_, or
// . can't find it in envs_
// do a PUTENV and change the envs_ entries.
//
// For every entry in envs_, if it can't be found in newenvs
// do a PUTENV to remove this entry of environment variable.
// and delete this entry from envs_ array.
//
// This way,
// . PUTENV is called only when there is any changes in the environment
// variables, so the leak would not be too big. Of course, there are still
// leaks, but since this is the only system call we can use, we really
// have no other choice.
// . If there is any components calling PUTENV, it will still be compatible
// with the code. Note, that is why newenvs is compared against envs_ instead
// of ENVIRON, because other components might set the environment variables
// which makes the ENVIRON different, this info was not from executor to begin
// with and can't be removed if not found in newenvs.

void ProcessEnv::setEnv(char **newenvs, int nEnvs) {
  if (!newenvs) return;

  addOrChangeEnv(newenvs, nEnvs);
  removeEnv(newenvs, nEnvs);

#ifdef _DEBUG
  dumpEnvs();  // only if debugging
#endif
}

int ProcessEnv::unsetEnv(char *env) {
  if (!env) return 0;
  int i = 0;
  while (strcmp(ENVIRON[i], env) != 0) i++;
  while (ENVIRON[i]) ENVIRON[i] = ENVIRON[++i];
  return 0;
}

void ProcessEnv::resetEnv(const char *envName) {
  if (!envName) return;

  int i;
  size_t nameLen = strlen(envName);
  CollHeap *stmtHeap = CmpCommon::statementHeap();
  NAList<int> deleteArray(stmtHeap, 16);  // 16 should be more than enough

  // find the env in existing env array
  for (i = 0; i < envs_.getSize(); i++) {
    if (envs_.used(i)) {
      char *pTemp = strchr(envs_[i], '=');
      if (pTemp)  // found '='
      {
        int envLen = (int)(pTemp - envs_[i]);
        if (envLen == nameLen && strncmp(envName, envs_[i], nameLen) == 0) {  // found matching env var name
          *(pTemp) = '\0';
          PUTENV(envs_[i]);
          NADELETEBASIC(envs_[i], heap_);
          deleteArray.insert(i);
        }
      }
    }
  }

  // remove from the env array
  for (int j = 0; j < deleteArray.entries(); j++) {
    envs_.remove(deleteArray[j]);
  }
}

int ProcessEnv::chdir(char *dir) {
  if (!dir) return 0;
  return ::chdir(dir);
}

#ifndef NDEBUG
void ProcessEnv::dumpEnvs() {
  // To test the PUTENV still works
  const char *aString = "DUMPENV=ddd";
  PUTENV((char *)aString);
  ofstream outStream("DUMPENVS");
  int i = 0;
  outStream << "ProcessEnv::dumpEnvs() " << endl << flush;
  while (ENVIRON[i]) {
    char tempstr[2048];
    strncpy(tempstr, ENVIRON[i], sizeof(tempstr));
    char *p = strchr(tempstr, '=');
    if (p) *p = 0;
    char *s;
    if (s = getenv(tempstr)) outStream << "environ[" << i << "] ---- " << tempstr << " ---> " << s << endl;
    i++;
  }
}
#endif  // NDEBUG

// For every entry in newenvs search envs_, if
// . not found, add a entry in envs_, do PUTENV
// . found but not the same, change the entry in envs_, do PUTENV

void ProcessEnv::addOrChangeEnv(char **newenvs, int nEnvs) {
  int i, j;

  for (i = 0; i < nEnvs; i++) {
    char *pTemp = strchr(newenvs[i], '=');
    if (pTemp) {
      NABoolean sameValue = FALSE;
      int envNameLen = pTemp - (newenvs[i]) + 1;  // including '='
      char *envName = new char[envNameLen + 1];
      strncpy(envName, newenvs[i], envNameLen);
      envName[envNameLen] = '\0';

      NABoolean envChanged = FALSE;
      CollIndex entriesChecked = 0;
      for (j = 0; entriesChecked < envs_.entries(); j++) {
        if (envs_.used(j)) {
          if (strcmp(newenvs[i], envs_[j]) == 0) {
            sameValue = TRUE;
            break;
          } else if (strncmp(envName, envs_[j], envNameLen) == 0) {
            envChanged = TRUE;
            break;
          }
          entriesChecked++;
        }
      }
      if (!sameValue) {
        CollIndex index = j;  // Put to the same location if value changed
        if (envChanged) {
          NADELETEBASIC(envs_[j], heap_);
          envs_.remove(j);
        } else
          index = envs_.unusedIndex();  // Insert a new env string

        UInt32 len = strlen(newenvs[i]);
        char *copyEnv = new char[len + 1];
        strcpy(copyEnv, newenvs[i]);
        copyEnv[len] = 0;

        PUTENV(copyEnv);

        char *copyEnvHeap = new (heap_) char[len + 1];
        strcpy(copyEnvHeap, newenvs[i]);
        copyEnvHeap[len] = 0;
        envs_.insertAt(index, copyEnvHeap);
      }
      delete[] envName;
    }
  }
}

// For every entry in envs_, if it can't be found in newenvs
// do a PUTENV to remove this entry of environment variable.
// and delete this entry from envs_ array.

void ProcessEnv::removeEnv(char **newenvs, int nEnvs) {
  int i, j;
  CollHeap *stmtHeap = CmpCommon::statementHeap();
  NAList<int> deleteArray(stmtHeap, 16);

  for (j = 0; j < envs_.getSize(); j++) {
    if (envs_.used(j)) {
      for (i = 0; i < nEnvs; i++)
        if (strcmp(newenvs[i], envs_[j]) == 0) break;

      if (i >= nEnvs) {
        // can't find it in newenvs, envs_[j] must have been deleted
        char *pTemp = strchr(envs_[j], '=');
        if (pTemp) {
          *(pTemp + 1) = '\0';
          PUTENV(envs_[j]);
          NADELETEBASIC(envs_[j], heap_);
          deleteArray.insert(j);
        }
      }
    }
  }

  for (j = 0; j < deleteArray.entries(); j++) {
    envs_.remove(deleteArray[j]);
  }
}
