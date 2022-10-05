

// SQL/MP SQLCA

#ifndef __SQLCA_H
#define __SQLCA_H

enum { SQL_NUM_ERROR_ENTRIES = 8, SQL_PARAMS_BUF_SIZE = 180 };

struct sql_error_struct {
  int errcode;

  int subsystem_id;

  int param_offset;

  int params_count;
};

struct sqlca_struct {
  int num_errors;

  int params_buffer_len;

  sql_error_struct errors[SQL_NUM_ERROR_ENTRIES];

  char params_buffer[SQL_PARAMS_BUF_SIZE];
};

#endif  // __SQLCA_H
