
#ifndef _MXTraceDef_H_
#define _MXTraceDef_H_

#ifdef NA_LINUX_DEBUG
#include "seabed/trace.h"
class MXTrcHelper {
  char sName_[200];

 public:
  MXTrcHelper(const char *name) {
    strcpy(sName_, name ? name : "UNKNOWN");
    trace_printf("%s begin\n", sName_);
  }
  ~MXTrcHelper() { trace_printf("%s end\n", sName_); }
};
#define MXTRC_FUNC(F)                              MXTrcHelper _mxtrc_func_(F)
#define MXTRC(F)                                   trace_printf(F)
#define MXTRC_1(F, P1)                             trace_printf(F, P1)
#define MXTRC_2(F, P1, P2)                         trace_printf(F, P1, P2)
#define MXTRC_3(F, P1, P2, P3)                     trace_printf(F, P1, P2, P3)
#define MXTRC_4(F, P1, P2, P3, P4)                 trace_printf(F, P1, P2, P3, P4)
#define MXTRC_5(F, P1, P2, P3, P4, P5)             trace_printf(F, P1, P2, P3, P4, P5)
#define MXTRC_6(F, P1, P2, P3, P4, P5, P6)         trace_printf(F, P1, P2, P3, P4, P5, P6)
#define MXTRC_7(F, P1, P2, P3, P4, P5, P6, P7)     trace_printf(F, P1, P2, P3, P4, P5, P6, P7)
#define MXTRC_8(F, P1, P2, P3, P4, P5, P6, P7, P8) trace_printf(F, P1, P2, P3, P4, P5, P6, P7, P8)
#else
#define MXTRC_FUNC(F)
#define MXTRC(F)
#define MXTRC_1(F, P1)
#define MXTRC_2(F, P1, P2)
#define MXTRC_3(F, P1, P2, P3)
#define MXTRC_4(F, P1, P2, P3, P4)
#define MXTRC_5(F, P1, P2, P3, P4, P5)
#define MXTRC_6(F, P1, P2, P3, P4, P5, P6)
#define MXTRC_7(F, P1, P2, P3, P4, P5, P6, P7)
#define MXTRC_8(F, P1, P2, P3, P4, P5, P6, P7, P8)
#endif
#endif
