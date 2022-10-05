//------------------------------------------------------------------
//


#ifndef __SB_ATOMIC_H_
#define __SB_ATOMIC_H_

//
// Atomic int
// (Encapsulate int and allow atomic operations)
//
class SB_Atomic_Int {
 public:
  inline SB_Atomic_Int() : iv_val(0) {}

  inline ~SB_Atomic_Int() {}

  inline int add_and_fetch(int pv_add) { return __sync_add_and_fetch_4(&iv_val, pv_add); }

  inline void add_val(int pv_add) { __sync_add_and_fetch_4(&iv_val, pv_add); }

  inline int read_val() { return iv_val; }

  inline void set_val(int pv_val) { iv_val = pv_val; }

  inline int sub_and_fetch(int pv_sub) { return __sync_sub_and_fetch_4(&iv_val, pv_sub); }

  inline void sub_val(int pv_sub) { __sync_sub_and_fetch_4(&iv_val, pv_sub); }

 private:
  int iv_val;
};

//
// Atomic long-long
// (Encapsulate int and allow atomic operations)
//
class SB_Atomic_Long_Long {
 public:
  inline SB_Atomic_Long_Long() : iv_val(0) {}

  inline ~SB_Atomic_Long_Long() {}

  inline long long add_and_fetch(long long pv_add) { return __sync_add_and_fetch_8(&iv_val, pv_add); }

  inline void add_val(long long pv_add) { __sync_add_and_fetch_8(&iv_val, pv_add); }

  inline long long read_val() { return iv_val; }

  inline void set_val(long long pv_val) { iv_val = pv_val; }

  inline long long sub_and_fetch(long long pv_sub) { return __sync_sub_and_fetch_8(&iv_val, pv_sub); }

  inline void sub_val(long long pv_sub) { __sync_sub_and_fetch_8(&iv_val, pv_sub); }

 private:
  long long iv_val;
};

#endif  // !__SB_ATOMIC_H_
