//------------------------------------------------------------------
//


#ifndef __SB_INT_MAP64_INL_
#define __SB_INT_MAP64_INL_

// needed by forward
SB_INLINE void SB_Ts_Map64::lock() {
    int lv_status;

    lv_status = iv_mutex.lock();
    SB_util_assert_ieq(lv_status, 0);
}

// needed by forward
SB_INLINE void SB_Ts_Map64::unlock() {
    int lv_status;

    lv_status = iv_mutex.unlock();
    SB_util_assert_ieq(lv_status, 0);
}

SB_INLINE void SB_Ts_Map64::clear() {
    lock();
    iv_map.clear();
    unlock();
}

SB_INLINE bool SB_Ts_Map64::end() {
    return (iv_iterator == iv_map.end());
}

SB_INLINE void *SB_Ts_Map64::get(SB_Int64_Type pv_key) {
    Iter lv_iter;

    lock();
    lv_iter = iv_map.find(pv_key);
    unlock();

    if (lv_iter == iv_map.end())
        return NULL;
    return lv_iter->second;
}

SB_INLINE void SB_Ts_Map64::get_end() {
    unlock();
}

SB_INLINE void *SB_Ts_Map64::get_first() {
    lock();
    iv_iterator = iv_map.begin();
    if (iv_iterator == iv_map.end())
        return NULL;
    return iv_iterator->second;
}

SB_INLINE void *SB_Ts_Map64::get_next() {
    iv_iterator++;
    if (iv_iterator == iv_map.end())
        return NULL;
    return iv_iterator->second;
}

SB_INLINE void SB_Ts_Map64::put(SB_Int64_Type pv_key, void *pp_data) {
    lock();
    iv_map.insert(std::make_pair(pv_key, pp_data));
    unlock();
}

SB_INLINE void *SB_Ts_Map64::remove(SB_Int64_Type pv_key) {
    void *lp_return = NULL;
    Iter  lv_iter;

    lock();
    lv_iter = iv_map.find(pv_key);
    if (lv_iter == iv_map.end()) {
       unlock();
       return NULL;
    }

    lp_return = lv_iter->second;
    iv_map.erase(pv_key);
    unlock();

    return lp_return;
}

SB_INLINE SB_Int64_Type SB_Ts_Map64::size() {
    return iv_map.size();
}

#endif //!__SB_INT_MAP64_INL_
