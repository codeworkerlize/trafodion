
/* -*-C++-*-
******************************************************************************
*
* File:         cmpargs.h
*
* Created:
* Language:     C++
*
*
*
*
******************************************************************************
*/

#ifndef __CMP_ARGSH
#define __CMP_ARGSH

#include <stdlib.h>

#include "common/Ipc.h"
#include "common/NAString.h"

struct ControlSetting {
  NAString attrName;
  NAString attrValue;
  ControlSetting(NAString n, NAString v) : attrName(n), attrValue(v) {}
  ControlSetting() : attrName(), attrValue() {}
};
typedef NAArray<ControlSetting> SettingsArray;

class Cmdline_Args {
 public:
  Cmdline_Args();
  void processArgs(int argc, char **argv);

  NAString getAppName() const;
  NAString getMDFname() const;
  NAString getModName() const;
  bool isStatComp() const;
  NABoolean hasStmtName() const;
  void printArgs();
  void usage(int argc, char **argv);
  IpcServerAllocationMethod allocMethod() const;
  int socketArg() const;
  int portArg() const;
  bool isVerbose() const;
  bool ignoreErrors() const;
  bool replaceModule() const;
  bool noSeabaseDefTableRead() const { return noSeabaseDefTableRead_; }

  void overwriteSettings() const;

  enum LocalStatus { NOT_SET, MOD_GLOBAL, MOD_LOCAL };
  bool moduleLocal() const;
  char *getModuleDir() const;

  enum TEST_SUBJECT {
    NONE,
    HQC,
    SHARED_SEGMENT_SEQUENTIAL_SCAN,
    SHARED_META_CACHE_LOAD,
    SHARED_META_CACHE_READ,
    SHARED_META_CACHE_FIND_ALL,
    SHARED_DATA_CACHE_FIND_ALL,
    SHARED_META_CACHE_CLEAN,
    HASH_DICTIONARY_ITOR_NON_COPY_TEST,
    TEST_OBJECT_EPOCH_CACHE,
    SHARED_META_CACHE_ENABLE_DISABLE_DELETE,
    NAMED_SEMAPHORE,
    MEMORY_EXHAUSTION,
    WATCH_DLOCK,
    LIST_DLOCK,
    HASHBUCKET
  };

  enum TEST_SUBJECT testMode() { return testMode_; }
  char *testData() { return testData_; }

 private:
  void addSetting(ControlSetting s);

  NAString application_;  // application filename, default is NULL
  NAString moddef_;       // module definition filename
  NAString module_;       // ANSI module name (= compiled module filename)
  bool isStat_;           // is static recompilation request
  bool hasListing_;       // sqlcomp has to generate compiler listing
  bool isVerbose_;        // verbose mode to print out extra information
  bool ignoreErrors_;     // convert static compile errors to warnings.
                          // Statement will be recompiled at runtime. The
                          // error will be returned at runtime if
                          // the recompile fails.

  IpcServerAllocationMethod allocMethod_;
  int socketArg_;
  int portArg_;
  SettingsArray settings_;       // control query default settings from cmd line
  LocalStatus modulePlacement_;  // where user wants local module
  NAString moduleDir_;           // directory for module file

  bool noSeabaseDefTableRead_;

  enum TEST_SUBJECT testMode_;
  char *testData_;

  // process -g {moduleGlobal|moduleLocal[=OSSdirectory]}
  int doModuleGlobalLocalDir(char *arg, int argc, char **argv, int &gCount, ComDiagsArea &diags);

  // process -g {moduleGlobal|moduleLocal}
  int doModuleGlobalLocal(char *arg, int argc, char **argv, ComDiagsArea &diags);
};

inline void Cmdline_Args::addSetting(ControlSetting s) { settings_.insertAt(settings_.entries(), s); }

inline NAString Cmdline_Args::getAppName() const { return application_; }

inline NAString Cmdline_Args::getMDFname() const { return moddef_; }

inline NAString Cmdline_Args::getModName() const { return module_; }

inline bool Cmdline_Args::isStatComp() const { return isStat_; }  // end isStatComp

inline IpcServerAllocationMethod Cmdline_Args::allocMethod() const { return allocMethod_; }

inline int Cmdline_Args::socketArg() const { return socketArg_; }

inline int Cmdline_Args::portArg() const { return portArg_; }

inline bool Cmdline_Args::isVerbose() const { return isVerbose_; }

inline bool Cmdline_Args::ignoreErrors() const { return ignoreErrors_; }

#endif  // __CMP_ARGSH
