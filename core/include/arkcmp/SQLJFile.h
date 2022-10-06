
#ifndef SQLJFILE__H
#define SQLJFILE__H

/* -*-C++-*-
 *****************************************************************************
 * File:         SQLJFile.h
 * Description:  This class finds & extracts embedded module definitions from
 *               NS SQLJ JAR and/or profile (*.ser) files.
 * Created:      08/06/2003
 * Language:     C++
 *****************************************************************************
 */
#include "ApplicationFile.h"

const int MAXMDF = 1024;

class SQLJFile : public ApplicationFile {
 public:
  // constructor
  SQLJFile(std::string &filename);

  // destructor
  virtual ~SQLJFile();

  // open SQLJ file for extraction of embedded module definitions.
  virtual bool openFile(Cmdline_Args &args);  // (IN): for module name list
  // return true if all OK

  // do nothing because MDFWriter error file has already been reported
  // earlier by printMDFWriterErrors() at end of SQLJFile::openFile()
  virtual void logErrors() { return; }

  // find next embedded module definition from this aplication file.
  // return false if no module definition found.
  virtual bool findNextModule(std::string &modName);  // (OUT): name of module found

  // extract embedded module definition & SQL compile it.
  // return true if all OK.
  virtual bool processModule();

  virtual int modulesFound();
  virtual int modulesExtracted();

  // return true iff this is a SQLJ JAR or profile file
  static bool isSQLJ(std::string &filename);

 private:
  // return "" or "-CMFN=modNamList_" where modNamList_ is a temp file that
  // has the list of module names to be extracted by MDFWriter.
  // requires: cmfn is long enough to hold "-CMFN=modNamList_".
  char *getModuleNamesOption(Cmdline_Args &args,  //(IN): for module name list
                             char *cmfn);         // (OUT): holds -CMFN=modNamList_

  // print MDFWriter errors to cout
  void printMDFWriterErrors(char *errFileName);

  int nExtracted_;          // number of modules extracted
  std::string modNamList_;  // file with list of module names to extract
  char mdf_[MAXMDF];        // extracted module definition filename
};

inline int SQLJFile::modulesExtracted() { return nExtracted_; }
inline int SQLJFile::modulesFound() { return modulesExtracted(); }

#endif  // SQLJFILE__H
