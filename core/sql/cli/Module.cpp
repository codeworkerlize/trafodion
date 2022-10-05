
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Module.cpp
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

// -----------------------------------------------------------------------

#include "cli_stdh.h"

Module::Module(const char *module_name, int len, char *pathName, int pathNameLen, NAHeap *heap)
    : module_name_len_(len), path_name_len_(pathNameLen), heap_(heap), statementCount_(0), vproc_(NULL) {
  module_name_ = (char *)(heap->allocateMemory((size_t)(len + 1)));

  str_cpy_all(module_name_, module_name, len);
  module_name_[len] = 0;

  path_name_ = (char *)(heap->allocateMemory((size_t)(pathNameLen + 1)));

  str_cpy_all(path_name_, pathName, pathNameLen);
  path_name_[pathNameLen] = 0;
}

Module::~Module() {
  if (module_name_) {
    heap_->deallocateMemory(module_name_);
  }
  module_name_ = 0;
}
