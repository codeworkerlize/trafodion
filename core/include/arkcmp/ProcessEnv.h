
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ProcessEnv.h
 * Description:  The class declaration for the process environment. i.e.
 *               current working directory and environment variables.
 *
 *
 * Created:      07/10/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#ifndef PROCESSENV_H
#define PROCESSENV_H

#include "common/Collections.h"
#include "export/NABasicObject.h"

// ProcessEnv contains the methods for setting the runtime environment
// for certain process, including current working directory(chdir) and
// environment variables. Eventually whatever sqlci handles for environment
// setup should be set here. This is for arkcmp to set up the same run time
// environment as sqlci.

class ProcessEnv : public NABasicObject {
 public:
  ProcessEnv(CollHeap *heap);

  void cleanup();
  void setEnv(char **newenvs, int nEnvs);
  void addOrChangeEnv(char **newenvs, int nEnvs);
  void resetEnv(const char *envName);
  int unsetEnv(char *env);
  int chdir(char *dir);
  void dumpEnvs();

  virtual ~ProcessEnv();

 private:
  void removeEnv(char **newenvs, int nEnvs);

  CollHeap *heap_;
  // The following members are used to keep the environment variable info.
  NAArray<char *> envs_;
  // Copy Constructor Unimplemented, to prevent an accidental use of
  // the default byte copy operation
  // Copy Constructor Unimplemented, to prevent an accidental use of
  // the default byte copy operation
  ProcessEnv &operator=(const ProcessEnv &);
  ProcessEnv(const ProcessEnv &);
};

#endif
