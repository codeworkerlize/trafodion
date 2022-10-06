//------------------------------------------------------------------
//


#ifndef __SB_INT_DIAG_H_
#define __SB_INT_DIAG_H_

#include "opts.h"

#ifdef USE_SB_DIAGS
#define SB_DIAG_UNUSED __attribute__((warn_unused_result))
#else
#define SB_DIAG_UNUSED
#endif

#ifdef USE_SB_DEPRECATED
#define SB_DIAG_DEPRECATED __attribute__((deprecated))
#else
#define SB_DIAG_DEPRECATED
#endif

#endif  // !__SB_INT_DIAG_H_
