//------------------------------------------------------------------
//

//
// Trace module
//
#ifndef __SB_TRACE_H_
#define __SB_TRACE_H_

#include <stdarg.h>
#include <stdio.h>

#include "int/diag.h"
#include "int/exp.h"

SB_Export void trace_close();
SB_Export void trace_flush();
SB_Export FILE *trace_get_fd() SB_DIAG_UNUSED;
SB_Export void trace_init(char *filename, bool unique, char *prefix, bool flush);
SB_Export void trace_init2(char *filename, bool unique, char *prefix, bool flush, long long max_size);
SB_Export void trace_init3(char *filename, bool unique, char *prefix, bool flush, long long max_size, bool sig_hdlr);
SB_Export void trace_lock();
SB_Export void trace_nolock_printf(const char *format, ...) __attribute__((format(printf, 1, 2)));
SB_Export void trace_nolock_vprintf(const char *format, va_list ap);
SB_Export void trace_nolock_where_printf(const char *where, const char *format, ...)
    __attribute__((format(printf, 2, 3)));
SB_Export void trace_nolock_where_vprintf(const char *where, const char *format, va_list ap);
SB_Export void trace_print_data(void *buf, int count, int max_count);
SB_Export void trace_printf(const char *format, ...) __attribute__((format(printf, 1, 2)));
SB_Export void trace_set_assert_no_trace(bool assert_no_trace);
SB_Export void trace_set_delta(bool delta);
SB_Export void trace_set_inmem(int size);
SB_Export void trace_set_lock(bool lock);
SB_Export void trace_set_mem(int size);
SB_Export void trace_set_pname(const char *pname);
SB_Export void trace_unlock();
SB_Export void trace_vprintf(const char *format, va_list ap);
SB_Export void trace_where_printf(const char *where, const char *format, ...) __attribute__((format(printf, 2, 3)));
SB_Export void trace_where_vprintf(const char *where, const char *format, va_list ap);

#endif  // !__SB_TRACE_H_
