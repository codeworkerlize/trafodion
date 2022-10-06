//------------------------------------------------------------------
//


//
// Implement utrace
//

#ifndef __SB_UTRACE_H_
#define __SB_UTRACE_H_

#include <stdio.h>

template <class T>
class SB_Utrace {
 public:
  SB_Utrace(int max);
  ~SB_Utrace();

  typedef void (*PE_Type)(FILE *f, T *rec, int inx);
  T *get_entry();                             // allocate entry
  T *get_entry_at(int inx);                   // return entry
  int get_inx();                              // get current inx
  int get_max();                              // get max
  bool get_wrapped();                         // get wrapped
  void print_entries_last(const char *title,  // title prefix
                          FILE *f,            // file
                          PE_Type pf,         // print-function
                          int cnt);           // count of entries

 private:
  T *ip_buf;  // trace buffer
  int iv_inx;
  int iv_mask;
  int iv_max;
  bool iv_wrapped;
};

#include "int/utrace.inl"

#endif  // !__SB_UTRACE_H_
