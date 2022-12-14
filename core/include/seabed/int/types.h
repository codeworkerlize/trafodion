//------------------------------------------------------------------
//

#ifndef __SB_INT_TYPES_H_

#include <stdint.h>

#define __SB_INT_TYPES_H_

#if __WORDSIZE == 64
typedef long SB_Int64_Type;
typedef unsigned long SB_Uint64_Type;
#define PFLL  "%ld"
#define PFLLX "%lx"
#define PF64  "%ld"
#define PF64X "%lx"
#define PFSZ  "%lu"
#define PFSZX "%lx"
#else
typedef long long SB_Int64_Type;
typedef unsigned long long SB_Uint64_Type;
#define PFLL  "%lld"
#define PFLLX "%llx"
#define PF64  "%lld"
#define PF64X "%llx"
#define PFSZ  "%u"
#define PFSZX "%x"
#endif
typedef long SB_Transseq_Type;
typedef struct SB_Transid_Type {
  SB_Int64_Type id[4];
} SB_Transid_Type;
typedef struct SB_Phandle_Type {
  SB_Int64_Type _data[8];
} SB_Phandle_Type;

typedef int SB_Uid_Type;

typedef int SB_Verif_Type;
#define PFVY "%d"

#endif  // !__SB_INT_TYPES_H_
