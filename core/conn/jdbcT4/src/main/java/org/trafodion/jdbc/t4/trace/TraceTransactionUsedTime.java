package org.trafodion.jdbc.t4.trace;

import org.trafodion.jdbc.t4.T4Properties;

public abstract class TraceTransactionUsedTime {

    public void add(TraceQueryUsedTime tqut) {
        throw new UnsupportedOperationException("TraceTransactionUsedTime add function not support");
    }

    public String getQueryInfo(String transId) {
        throw new UnsupportedOperationException("TraceTransactionUsedTime get function not support");
    }

    public short getType() {
        throw new UnsupportedOperationException("TraceTransactionUsedTime getType function not support");
    }

    public long getUsedTime() {
        throw new UnsupportedOperationException("TraceTransactionUsedTime getUsedTime function not support");
    }

    public String getStartTime() {
        throw new UnsupportedOperationException("TraceTransactionUsedTime getStartTime function not support");
    }

    public void calculateTransactionUsedTime() {
        throw new UnsupportedOperationException("TraceTransactionUsedTime calculateTransactionUsedTime function not support");
    }

    public String getSql() {
        throw new UnsupportedOperationException("TraceTransactionUsedTime getSql function not support");
    }

    public String getLabel() {
        throw new UnsupportedOperationException("TraceTransactionUsedTime getLabel function not support");
    }

    public void clear() {
        throw new UnsupportedOperationException("TraceTransactionUsedTime clear function not support");
    }

    public void setTransactionStartTime(long transactionStartTime){
        throw new UnsupportedOperationException("TraceTransactionUsedTime setTransactionStartTime function not support");
    }

    public long getTransactionStartTime(){
        throw new UnsupportedOperationException("TraceTransactionUsedTime getTransactionStartTime function not support");
    }

    public void setTransactionEndTime(long transactionEndTime){
        throw new UnsupportedOperationException("TraceTransactionUsedTime setTransactionEndTime function not support");
    }

    public void setTransId(long transId) {
        throw new UnsupportedOperationException("TraceTransactionUsedTime setTransId function not support");
    }

    public long getTransId() {
        throw new UnsupportedOperationException("TraceTransactionUsedTime getTransId function not support");
    }

    public void print(T4Properties t4pro, TraceAverageUsedTime traceAverageUsedTime) {
        throw new UnsupportedOperationException("TraceTransactionUsedTime print function not support");
    }
}
