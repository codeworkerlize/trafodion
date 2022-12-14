#ifndef DEBUG_H
#define DEBUG_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Debug.h
 * Description:  DBG() and DBGDECL() macro definitions for C debugging.
 *
 * Language:     C++
 *

 *
 *****************************************************************************
 */

/*
 *  Define macros for a clean way of putting debugging statements
 *  into your C code that can be compiled in for debugging, and
 *  not compiled (no run-time penalty) for regular use,
 *  while keeping the statement text (test scaffolding, what-have-you)
 *  within the program source.
 *
 *  // Legal:
 *  DBGDECL (static int icnt = 0;)
 *  if (x>y) DBG (printf("foo"); icnt++;)
 *  if (x>y) DBG (printf("foo"); icnt++;);
 *  DBG (if (x>y) {printf("foo"); icnt++;})
 *  DBG (if (x>y) {printf("foo"); icnt++;});
 *
 *  // Illegal:
 *  DBGDECL (static int icnt = 0);          // If NDEBUG is on, this gives a
 *  DBGDECL (static int icnt = 0;);         // null stmt (;) in declaratns.
 *  if (x>y) DBG (printf("foo"); icnt++)    // Missing terminator (;).
 *
 *  // Most consistent and error-free in both DBG and DBGDECL
 *  // is not to have a semicolon after the rparen:
 *  DBGDECL (static int icnt = 0;)
 *  if (x>y) DBG (printf("foo"); icnt++;)
 *  DBG (if (x>y) {printf("foo"); icnt++;})
 */

#ifndef NDEBUG
#include <stdlib.h> /* for getenv() */
#define DBG(statement_list) \
  { statement_list }
#define DBGDECL(declaration_list) declaration_list
#else
#define DBG(statement_list)       ;
#define DBGDECL(declaration_list) /* no declaration */
#endif

// This "ignore" is for symmetry in the using code; it just looks nicer.
#define DBGDECLDBG(ignore)        DBGDECL(static THREAD_P int DBG__ = 0;)
#define DBGSETDBG(envvar)         DBG(DBG__ = !!getenv(envvar);)
#define DBGIFB(b, statement_list) DBG(if (b){statement_list})
#define DBGIF(statement_list)     DBGIFB(DBG__, statement_list)

#endif  // DEBUG_H
