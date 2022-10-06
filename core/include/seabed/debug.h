//------------------------------------------------------------------
//

//
// Debug module
//
#ifndef __SB_DEBUG_H_
#define __SB_DEBUG_H_

#include "int/exp.h"

SB_Export void XDEBUG();

// backtrace callback
typedef enum {
  SB_BT_REASON_BEGIN = 1,  // callback begin - str is NULL
  SB_BT_REASON_END = 2,    // callback end - str is NULL
  SB_BT_REASON_STR = 3     // callback str - str contains string
} SB_BT_REASON;
typedef void (*SB_BT_CB)(SB_BT_REASON reason, const char *str);
// backtrace
SB_Export void SB_backtrace(SB_BT_CB callback);
SB_Export void SB_backtrace2(int max_rec_count, int rec_size, int *rec_count, char *records);

#endif  // !__SB_DEBUG_H_
