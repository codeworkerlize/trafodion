
#include <iostream>
using std::cerr;
using std::endl;

#include <fstream>
using std::ofstream;

#include "common/Platform.h"
#include "cli/SQLCLIdev.h"
#include "cli/Context.h"
#include "common/str.h"
#include "exp/ExpLOBinterface.h"
#include "executor/ex_globals.h"

char *getLobErrStr(int errEnum) {
  if (errEnum < LOB_MIN_ERROR_NUM || errEnum > LOB_MAX_ERROR_NUM)
    return (char *)"Unknown LOB error";
  else
    return (char *)lobErrorEnumStr[errEnum - (int)LOB_MIN_ERROR_NUM];
}
