package org.trafodion.jdbc.t4.trace;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.T4LoggingUtilities;
import org.trafodion.jdbc.t4.T4Properties;

public class TraceQueryUsedTime extends TraceTransactionUsedTime {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TraceQueryUsedTime.class);

    private List<TraceTransactionUsedTime> transactionUsedTimeList = new ArrayList<TraceTransactionUsedTime>();
    private SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

    private long transactionStartTime;
    private long transactionEndTime;
    private long transactionUsedTime;

    private String label;
    private String sql;
    private long queryStartTime;
    private long queryEndTime;
    private short type;
    private long transId;

    public TraceQueryUsedTime(String label, String sql, long startTime, long endTime, short type) {
        this.label = label;
        this.sql = sql;
        this.queryStartTime = startTime;
        this.queryEndTime = endTime;
        this.type = type;
    }

    @Override
    public void add(TraceQueryUsedTime tqut) {
        transactionUsedTimeList.add(tqut);
    }

    @Override
    public short getType() {
        return type;
    }

    @Override
    public long getUsedTime() {
        return queryEndTime - queryStartTime;
    }

    @Override
    public String getStartTime() {
        return sf.format(queryStartTime);
    }

    @Override
    public void calculateTransactionUsedTime() {
        transactionUsedTime = transactionEndTime - transactionStartTime;
    }

    @Override
    public String getSql() {
        return sql;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public void clear() {
        transactionStartTime = 0;
        transactionEndTime = 0;
        transactionUsedTimeList.clear();
    }

    @Override
    public void setTransactionStartTime(long transactionStartTime){
        this.transactionStartTime = transactionStartTime;
    }

    @Override
    public long getTransactionStartTime(){
        return this.transactionStartTime;
    }

    @Override
    public void setTransactionEndTime(long transactionEndTime){
        this.transactionEndTime = transactionEndTime;
    }

    @Override
    public void setTransId(long transId){
        this.transId = transId;
    }

    @Override
    public long getTransId(){
        return this.transId;
    }

    @Override
    public String getQueryInfo(String transId) {
        if (transactionUsedTimeList.size() == 0) {
            return "null query";
        }
        int slowSqlCount = 0;
        StringBuffer sb = new StringBuffer();
        sb.append("\n");
        for (TraceTransactionUsedTime tmpTut : transactionUsedTimeList) {
            TraceQueryUsedTime tmpTqt = (TraceQueryUsedTime) tmpTut;
            long usedTime = tmpTqt.getUsedTime();
            if (usedTime >= 100) {
                slowSqlCount++;  
            } 
            switch (tmpTqt.getType()) {
                case 0:
                    sb.append("prepare txID ").append(transId)
                        .append(" Using ").append(usedTime).append(" ms")
                        .append(" SQL ").append(tmpTqt.getSql())
                        .append(" Label ")
                        .append(tmpTqt.getLabel())
                        .append("\n");
                    break;
                case 1:
                    sb.append("execute txID ").append(transId)
                        .append(" Using ").append(usedTime).append(" ms")
                        .append(" Label ")
                        .append(tmpTqt.getLabel())
                        .append("\n");
                    break;
                case 2:
                    sb.append("executeDirect txID ").append(transId)
                        .append(" Using ").append(usedTime).append(" ms")
                        .append(" SQL ").append(tmpTqt.getSql())
                        .append(" Label ")
                        .append(tmpTqt.getLabel())
                        .append("\n");
                    break;
                case 3:
                    sb.append("executeBatch txID ").append(transId)
                        .append(" Using ").append(usedTime).append(" ms")
                        .append(" Label ")
                        .append(tmpTqt.getLabel())
                        .append("\n");
                    break;
                case 4:
                    sb.append("endTransaction txID ").append(transId)
                        .append(" Using ").append(usedTime).append(" ms")
                        .append(" MTHC ").append(slowSqlCount)
                        .append("\n");
                    break;
                default:
                    sb.append(
                        "warning : non prepare or execute or executedirect enter, please check");
            }
        }
        return sb.toString();
    }

    @Override
    public void print(T4Properties t4pro, TraceAverageUsedTime traceAverageUsedTime) {
        String transType = t4pro.getClientInfoProperties().getProperty("TransactionType");
        if (transType == null) {
            transType = "defaultType";
        }
        calculateTransactionUsedTime();
        traceAverageUsedTime.setTime(transType, transactionUsedTime);
        if (t4pro.getTraceTransTime() != 0 && transactionUsedTime <= t4pro.getTraceTransTime()) {
            t4pro.getClientInfoProperties().remove("TransactionType");
            clear();
            return;
        }

        String remoteProcess = t4pro.getRemoteProcess();
        String transId = String.valueOf(getTransId());
        long averageTime = traceAverageUsedTime.get(transType);
        int transCount = traceAverageUsedTime.getCount(transType);
        String tmp = this.getQueryInfo(transId);

        if (t4pro.isLogEnable(Level.WARNING)) {
            T4LoggingUtilities.log(t4pro, Level.WARNING,
                "RP " + remoteProcess + " txID " + transId + " TT " + transType
                    + " TST " + sf.format(transactionStartTime)
                    + " TET " + sf.format(transactionEndTime)
                    + " TUT " + transactionUsedTime + " TAT "
                    + averageTime + " TC " + transCount,
                tmp);
        }
        if (LOG.isWarnEnabled()) {
            LOG.warn(
                "RP {} txID {} TT {} TST {} TET {} TUT {} TAT {} TC {} {}",
                remoteProcess, transId, transType, sf.format(transactionStartTime),
                sf.format(transactionEndTime), transactionUsedTime, averageTime, transCount, tmp);
        }
        t4pro.getClientInfoProperties().remove("TransactionType");
        clear();
    }


}
