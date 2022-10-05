
#ifndef __TM_UTIL_H
#define __TM_UTIL_H

#undef int16
#define int16 short
#undef uint16
#define uint16 unsigned short
#undef int32
#define int32 int
#undef uint32
#define uint32 unsigned int

#undef int64
#if __WORDSIZE == 64
#define int64  long
#define uint64 unsigned long
#ifndef PFLLX
#define PFLLX "%lx"
#endif
#ifndef PFLLU
#define PFLLU "%lu"
#endif
#else
#define int64  long long
#define uint64 unsigned long long
#ifndef PFLLX
#define PFLLX "" PFLLX ""
#endif
#ifndef PFLLU
#define PFLLU "%llu"
#endif
#endif

#endif
