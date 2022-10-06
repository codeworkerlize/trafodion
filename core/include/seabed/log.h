//------------------------------------------------------------------
//

//
// Logging module
//
#ifndef __SB_LOG_H_
#define __SB_LOG_H_

#include "int/diag.h"
#include "int/exp.h"
#include "sqevlog/evl_sqlog_writer.h"

//
// seems like something reasonable
//
enum { LOG_DEFAULT_BUF_SIZE = 8000 };

//
// buffer specified
//
SB_Export int SB_log_add_array_token(char *buf, int tk_type, void *tk_value, size_t count) SB_DIAG_UNUSED;
SB_Export int SB_log_add_token(char *buf, int tk_type, void *tk_value) SB_DIAG_UNUSED;
SB_Export int SB_log_buf_used(char *buf) SB_DIAG_UNUSED;
SB_Export void SB_log_enable_logging(bool logging);
SB_Export int SB_log_init(int comp_id, char *buf, size_t buf_maxlen) SB_DIAG_UNUSED;
SB_Export int SB_log_init_compid(int comp_id) SB_DIAG_UNUSED;
SB_Export int SB_log_write(posix_sqlog_facility_t facility, int event_type, posix_sqlog_severity_t severity,
                           char *buf) SB_DIAG_UNUSED;
SB_Export int SB_log_write_str(int comp_id, int event_id, posix_sqlog_facility_t facility,
                               posix_sqlog_severity_t severity, char *str) SB_DIAG_UNUSED;

//
// thread-specific - buffer not specified
//
SB_Export int SB_log_ts_add_array_token(int tk_type, void *tk_value, size_t count) SB_DIAG_UNUSED;
SB_Export int SB_log_ts_add_token(int tk_type, void *tk_value) SB_DIAG_UNUSED;
SB_Export int SB_log_ts_init(int comp_id) SB_DIAG_UNUSED;
SB_Export int SB_log_ts_write(posix_sqlog_facility_t facility, int event_type,
                              posix_sqlog_severity_t severity) SB_DIAG_UNUSED;

#endif  // !__SB_LOG_H_
