//------------------------------------------------------------------
//


#ifndef __SB_CC_H_
#define __SB_CC_H_

typedef int _bcc_status;
typedef int bfat_16;
typedef int _xcc_status;
typedef int xfat_16;

#define _bstatus_lt(x) ((x) < 0)
#define _bstatus_gt(x) ((x) > 0)
#define _bstatus_eq(x) ((x) == 0)
#define _bstatus_le(x) ((x) <= 0)
#define _bstatus_ge(x) ((x) >= 0)
#define _bstatus_ne(x) ((x) != 0)

#define _xstatus_lt(x) ((x) < 0)
#define _xstatus_gt(x) ((x) > 0)
#define _xstatus_eq(x) ((x) == 0)
#define _xstatus_le(x) ((x) <= 0)
#define _xstatus_ge(x) ((x) >= 0)
#define _xstatus_ne(x) ((x) != 0)

#endif  // !__SB_CC_H_
