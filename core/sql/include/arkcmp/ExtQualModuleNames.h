
#ifndef EXTQUALMODULENAMES__H
#define EXTQUALMODULENAMES__H

/* -*-C++-*-
 *****************************************************************************
 * File:         ExtQualModuleNames.h
 * Description:  This class encapsulates the scanning, parsing, accessing of
 *               externally qualified module names for mxCompileUserModule.
 * Created:      04/11/2003
 * Language:     C++
 *****************************************************************************
 */

#include <string>
#include <vector>

struct ThreePartModuleName {
  std::string catalog, schema, module;
  bool found;
  // default constructor
  ThreePartModuleName() : catalog(), schema(), module(), found(false) {}
  // assemble 3 part name into string
  void setFullyQualifiedName(std::string &name) const { name = catalog + '.' + schema + '.' + module; }
};

class ExtQualModuleNames {
 public:
  // constructor
  ExtQualModuleNames(char *argv[], int startIndex, int argc, const char *cat, const char *sch, const char *grp,
                     const char *tgt, const char *ver);

  // destructor
  virtual ~ExtQualModuleNames();

  // process externally qualified module names
  void processExtQualModuleNames();

  // return true iff module is in this list
  bool contains(std::string &moduleName);

  // return number of module names
  int count() { return modules_.size(); }

  // return ith externally qualified module name
  const ThreePartModuleName &at(int x) { return modules_.at(x); }

  static void usage();

 private:
  std::vector<ThreePartModuleName> modules_;  // module names in canonical format

  // scanner & parser data members & methods
  std::string buffer_;              // module names as typed on command line
  std::string::const_iterator bp_;  // pointer to next character in buffer_

  const char currentChar() { return *bp_; }
  void advanceChar() { bp_++; }
  const char returnAdvanceChar() { return *bp_++; }
  bool atEnd() { return bp_ == buffer_.end(); }

  enum tokenType {
    COMMA = ',',
    DOT = '.',
    EQUAL = '=',
    LBRACE = '{',
    RBRACE = '}',
    ID,
    MODULEGROUP,
    MODULETABLESET,
    MODULEVERSION,
    SCANEOF,
    SCANERROR
  };
  tokenType currentTokenCode_;
  std::string currentToken_;
  const char *tokenString(tokenType t);
  const char *currentTokenString();

  tokenType scanner();  // scan & return next token
  tokenType nextToken() { return currentTokenCode_; }
  void match(tokenType t);    // match a terminal symbol t
  tokenType checkReserved();  // return one of: ID, MODULEGROUP,
                              // MODULETABLESET, MODULEVERSION
  // parser methods
  void parseExternallyQualifiedModuleNames();
  void parseModuleNameList();
  void parseModuleName();
  void parseModule(std::string &cat, std::string &sch, std::string &mod);
  void parseQualifiers(std::string &grp, std::string &tgt, std::string &ver);

  // compose externally qualified module name from its component names
  void composeEQMN(std::string &grp, std::string &cat, std::string &sch, std::string &mod, std::string &tgt,
                   std::string &ver, ThreePartModuleName &eqmn);

  std::string moduleCatalog_;   // default module catalog
  std::string moduleSchema_;    // default module schema
  std::string moduleGroup_;     // default module group
  std::string moduleTableSet_;  // default module tableset
  std::string moduleVersion_;   // default module version
};

#endif  // EXTQUALMODULENAMES__H
