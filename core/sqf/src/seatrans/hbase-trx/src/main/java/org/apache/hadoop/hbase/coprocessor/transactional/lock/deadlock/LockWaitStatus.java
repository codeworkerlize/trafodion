package org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock;

public enum LockWaitStatus {
    WAITING(1),//for logging waited lock
    CANCEL_FOR_DEADLOCK(2), // cancel for deadlock
    CANCEL_FOR_ROLLBACK(3),//transaction rollback
    CANCEL_FOR_SPLIT(4),//cancel when region split
    CANCEL_FOR_MOVE(5), // cancel when region balance
    CANCEL_FOR_NEW_RPC_REQUEST(6),
    LOCK_TIMEOUT(7), //after rpc timeout, a new rpc request will arrive, and should cancel the pre request
    CANCEL_FOR_NOT_ENOUGH_LOCK_RESOURCES(8),
    OK(9),
    FINAL_LOCK_TIMEOUT(10); // last retry of lock timeout

    private int index;

    LockWaitStatus(int index) {
        this.index = index;
    }

    public int getIndex() {
        return this.index;
    }
}
