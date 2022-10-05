
#include <stdarg.h>

#include "sqlci/SqlciError.h"
#include "export/ComDiags.h"

extern ComDiagsArea sqlci_DA;

void SqlciError(short errorCode, ...) {
  UInt32 cur_string_p = 0;
  UInt32 cur_int_p = 0;

  va_list ap;
  ErrorParam *Param;

  ComCondition *Condition = sqlci_DA.makeNewCondition();

  Condition->setSQLCODE(-errorCode);

  va_start(ap, errorCode);
  while ((Param = va_arg(ap, ErrorParam *)) != (ErrorParam *)0) {
    if (Param->Param_type() == STRING_TYPE)
      Condition->setOptionalString(cur_string_p++, Param->Str_Param());
    else
      Condition->setOptionalInteger(cur_int_p++, Param->Int_Param());
  }

  sqlci_DA.acceptNewCondition();
};

void SqlciError2(int errorCode, ...) {
  UInt32 cur_string_p = 0;
  UInt32 cur_int_p = 0;

  va_list ap;
  ErrorParam *Param;

  ComCondition *Condition = sqlci_DA.makeNewCondition();

  Condition->setSQLCODE(errorCode);

  va_start(ap, errorCode);
  while ((Param = va_arg(ap, ErrorParam *)) != (ErrorParam *)0) {
    if ((Param->Param_type() == STRING_TYPE) && Param->Str_Param())
      Condition->setOptionalString(cur_string_p++, Param->Str_Param());
    if ((Param->Param_type() == INT_TYPE) && Param->Int_Param() != -1)
      Condition->setOptionalInteger(cur_int_p++, Param->Int_Param());
  }

  sqlci_DA.acceptNewCondition();
};

void SqlciWarning(short errorCode, ...) {
  UInt32 cur_string_p = 0;
  UInt32 cur_int_p = 0;

  va_list ap;
  ErrorParam *Param;

  ComCondition *Condition = sqlci_DA.makeNewCondition();

  Condition->setSQLCODE(errorCode);

  va_start(ap, errorCode);
  while ((Param = va_arg(ap, ErrorParam *)) != (ErrorParam *)0) {
    if (Param->Param_type() == STRING_TYPE)
      Condition->setOptionalString(cur_string_p++, Param->Str_Param());
    else
      Condition->setOptionalInteger(cur_int_p++, Param->Int_Param());
  }

  sqlci_DA.acceptNewCondition();
};
