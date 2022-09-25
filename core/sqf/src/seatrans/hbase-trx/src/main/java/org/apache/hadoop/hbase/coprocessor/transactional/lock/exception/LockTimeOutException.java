package org.apache.hadoop.hbase.coprocessor.transactional.lock.exception;

public class LockTimeOutException extends RuntimeException {
    public LockTimeOutException(String message) {
        super(message);
    }

    public LockTimeOutException(String message, Throwable cause) {
        super(message, cause);
    }

}
