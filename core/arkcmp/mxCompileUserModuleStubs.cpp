
#ifndef NULL
#ifdef __cplusplus
#define NULL 0
#else
#define NULL ((void *)0)
#endif
#endif
#include "common/platform.h"
#include "nsk/nskport.h"
#include "common/feerrors.h"
_declspec(dllexport) PCHAR NSKGetRegKeyServerWarePath() { return NULL; }

_declspec(dllexport) PCHAR NSKGetRegKeyConfigPath() { return NULL; }

_declspec(dllexport) int_16 SECURITY_PSB_GET_(int_16 item, void *value_, int_16 max_len, int_16 *value_len_arg,
                                              int_16 pin)  // PIN identifies target process, unused on NT
{
  return FEBADPARMVALUE;
}

extern "C" _declspec(dllexport) short FILE_GETINFOLISTBYNAME_(char *name, short length_a, short *itemlist,
                                                              short numberitems, short *result, short resultmax,
                                                              short *result_len, short *erroritem) {
  return 0;
}

extern "C" _declspec(dllexport) long long COMPUTETIMESTAMP(short *date_n_time,  // IN
                                                           short *error)        // OUT OPTIONAL
{
  return 0;
};

extern "C" _declspec(dllexport) int INTERPRETTIMESTAMP(long long juliantimestamp,  // IN
                                                       short *date_n_time)         // OUT
{
  return 0;
};

extern "C" _declspec(dllexport) long long JULIANTIMESTAMP(short type, short *tuid, short *error, short node) {
  return 0;
};
