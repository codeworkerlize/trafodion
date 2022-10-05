
#ifndef MODULE_H
#define MODULE_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Module.h
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComVersionDefs.h"
// -----------------------------------------------------------------------

class Module : public ExGod {
  char *module_name_;
  int module_name_len_;
  char *path_name_;
  int path_name_len_;
  NAHeap *heap_;
  int statementCount_;
  COM_VERSION version_;
  char *vproc_;

 public:
  Module(const char *module_name, int module_name_len, char *pathName, int pathNameLen, NAHeap *heap);
  ~Module();

  inline char *getModuleName() { return module_name_; };
  inline int getModuleNameLen() { return module_name_len_; };
  inline char *getPathName() { return path_name_; };
  inline int getPathNameLen() { return path_name_len_; };
  inline int getStatementCount() { return statementCount_; }
  inline void setStatementCount(int c) { statementCount_ = c; }
  void setVersion(COM_VERSION v) { version_ = v; }
  COM_VERSION getVersion() { return version_; }
  char *getVproc() { return vproc_; }
  void setVproc(char *v) { vproc_ = v; }
};

#endif
