
#ifndef CMDLINE_ARGS__H
#define CMDLINE_ARGS__H

/* -*-C++-*-
 *****************************************************************************
 * File:         Cmdline_Args.h
 * Description:  This class encapsulates the command-line arguments processing
 *               for mxCompileUserModule (the program for SQL compiling a
 *               C/C++/Cobol executable/library or SQLJ ser/jar file that has
 *               embedded module definitions).
 * Created:      03/03/2003
 * Language:     C++
 *****************************************************************************
 */

#include <string>

#include "common/Platform.h"  // 64-bit

class ExtQualModuleNames;
class NAHeap;
class NAString;

class Cmdline_Args {
 public:
  Cmdline_Args();
  virtual ~Cmdline_Args();

  void processArgs(int argc, char **argv);
  static void usage();

  std::string &application() { return application_; }
  std::string &otherArgs() { return otherArgs_; }
  bool keepMdf() const { return keepMdf_; }

  // return true if this module was requested via this command-line
  bool isWanted(std::string &module);

  ExtQualModuleNames *getModuleNames() { return moduleNames_; }

 private:
  // process -g {moduleGlobal|moduleLocal}
  void doModuleLocalGlobal(const char *arg, int &gCount, NAHeap *heap);

  bool gleanModuleCatSchSpecStr(const char *namEQval, NAHeap *heap);

  // return default module catalog, schema, spec string
  const char *moduleCatalog();
  const char *moduleSchema();
  const char *moduleGroup();
  const char *moduleTableSet();
  const char *moduleVersion();

  std::string application_;  // application filename
  std::string otherArgs_;    // other command line arguments
  enum modLocEnum { False, True, NotSpecified };
  modLocEnum moduleLocal_;           // true iff -g moduleLocal specified
  bool keepMdf_;                     // true iff user wants to keep temporary mdf
  ExtQualModuleNames *moduleNames_;  // externally qualified module names
  NAString *moduleCatalog_;          // default module catalog or empty
  NAString *moduleSchema_;           // default module schema or empty
  NAString *moduleGroup_;            // default module group or empty
  NAString *moduleTableSet_;         // default module tableset or empty
  NAString *moduleVersion_;          // default module version or empty
};

#endif  // CMDLINE_ARGS__H
