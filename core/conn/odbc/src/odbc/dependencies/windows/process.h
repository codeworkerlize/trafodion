/*************************************************************************
*
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**************************************************************************/

#ifndef _PROCESS_H_
#define _PROCESS_H_
#include <unistd.h>
#define _getpid getpid

extern void *__wrap_memcpy(void *dest, const void *src, size_t n);

/*
 * Processor architecture detection.  For more info on what's defined, see:
 *   http://www.agner.org/optimize/calling_conventions.pdf
 *   or with gcc, run: "echo | gcc -E -dM -"   
 */
#define MEMCPY_WRAPPER_PLATFORM_ERROR \
  #error "Host platform was not detected as supported by odb"

#if defined(_WIN32)
#elif defined(__PPC__) || defined(__PPC64__)
#elif defined(__x86_64__) || defined(__i386__)
__asm__ (".symver memcpy, memcpy@GLIBC_2.2.5");
#elif defined(__aarch64__)
__asm__ (".symver memcpy, memcpy@GLIBC_2.17");
#else
MEMCPY_WRAPPER_PLATFORM_ERROR
#endif
