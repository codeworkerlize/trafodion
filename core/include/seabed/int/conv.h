//------------------------------------------------------------------
//

#ifndef __SB_INT_CONV_H_
#define __SB_INT_CONV_H_

#if __WORDSIZE == 64
typedef long SB_Tag_Type;
#define PFTAG "%ld"
#else
typedef int SB_Tag_Type;
#define PFTAG "%d"
#endif

#endif  // !__SB_INT_CONV_H_
