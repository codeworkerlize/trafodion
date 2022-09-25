package org.apache.hadoop.hbase.coprocessor.transactional.lock.exception;

public class RPCTimeOutException extends RuntimeException {
    public RPCTimeOutException(String message) {
        super(message, new Throwable("RPCTimeOutException : " + message));
    }

    public RPCTimeOutException(String message, Throwable cause) {
        super(message, new Throwable("RPCTimeOutException : " + message + cause.toString()));
    }

}
