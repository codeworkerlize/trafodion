package org.apache.hadoop.hbase.coprocessor.transactional.lock.exception;

public class DeadLockException extends RuntimeException {
    public DeadLockException(String message) {
        super(message);
    }

    public DeadLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
