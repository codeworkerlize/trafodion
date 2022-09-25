package org.apache.hadoop.hbase.coprocessor.transactional.lock.exception;

public class LockNotEnoughResourcsException extends RuntimeException {
    public LockNotEnoughResourcsException(String message) {
        super(message);
    }

    public LockNotEnoughResourcsException(String message, Throwable cause) {
        super(message, cause);
    }
}
