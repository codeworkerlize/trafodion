package org.apache.hadoop.hbase.coprocessor.transactional.lock;

public enum RetCode {
    OK,
    OK_LOCKED,
    OK_WITHRETRY,
    TIMEOUT,
    FINAL_TIMEOUT,
    CANCEL_FOR_DEADLOCK, // cancel for deadlock
    CANCEL_FOR_ROLLBACK,//transaction rollback
    CANCEL_FOR_SPLIT,//cancel when region split
    CANCEL_FOR_MOVE,//cancel when region balance
    CANCEL_FOR_NEW_RPC_REQUEST, //after rpc timeout, a new rpc request will arrive, and should cancel the pre request
    CANCEL_FOR_INTENTLOCK_FAIL,
    CANCEL_FOR_OTHER_REASON,
    CANCEL_FOR_NOT_ENOUGH_LOCK_RESOURCES,
    CANCEL_FOR_CLOSING_REGION,
    CANCEL_FOR_OOM
}
