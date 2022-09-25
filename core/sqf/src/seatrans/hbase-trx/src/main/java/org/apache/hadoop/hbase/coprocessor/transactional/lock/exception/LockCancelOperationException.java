package org.apache.hadoop.hbase.coprocessor.transactional.lock.exception;

public class LockCancelOperationException extends RuntimeException {
    public LockCancelOperationException(String message) {
        super(message);
    }

    public LockCancelOperationException(String message, Throwable cause) {
        super(message, cause);
    }

}
