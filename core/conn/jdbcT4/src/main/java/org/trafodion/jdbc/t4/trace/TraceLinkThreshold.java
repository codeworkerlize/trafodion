package org.trafodion.jdbc.t4.trace;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.T4LoggingUtilities;
import org.trafodion.jdbc.t4.T4Properties;

public class TraceLinkThreshold {
    private long enterDriverTimestamp;
    private long sendToMxoTimestamp;
    private long enterMxoTimestamp;
    private long enterEngineTimestamp;
    private long leaveEngineTimestamp;
    private long leaveMxoTimestamp;
    private long recvFromMxoTimestamp;
    private long leaveDriverTimestamp;

    private long driverUsedTime;
    private long mxoUsedTime;
    private long engineUsedTime;

    private boolean batchFlag;
    private boolean selectFlag;
    private boolean endOfData;

    private short odbcAPI;

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TraceLinkThreshold.class);

    private long receiveTs;

    public void initLinkThreshold() {
        enterDriverTimestamp = 0;
        sendToMxoTimestamp = 0;
        enterMxoTimestamp = 0;
        enterEngineTimestamp = 0;
        leaveEngineTimestamp = 0;
        leaveMxoTimestamp = 0;
        recvFromMxoTimestamp = 0;
        leaveDriverTimestamp = 0;

        driverUsedTime = 0;
        mxoUsedTime = 0;
        engineUsedTime = 0;

        receiveTs = 0;

        batchFlag = false;
        selectFlag = false;
        endOfData = false;
    }

    public TraceLinkThreshold(){
        initLinkThreshold();
    }

    public short getOdbcAPI() {
        return odbcAPI;
    }

    public void setOdbcAPI(short odbcAPI) {
        this.odbcAPI = odbcAPI;
    }
    public long getEnterDriverTimestamp() {
        return enterDriverTimestamp;
    }

    public void setEnterDriverTimestamp(long enterDriverTimestamp) {
        this.enterDriverTimestamp = enterDriverTimestamp;
    }

    public long getSendToMxoTimestamp() {
        return sendToMxoTimestamp;
    }

    public void setSendToMxoTimestamp(long sendToMxoTimestamp) {
        this.sendToMxoTimestamp = sendToMxoTimestamp;
    }

    public long getEnterMxoTimestamp() {
        return enterMxoTimestamp;
    }

    public void setEnterMxoTimestamp(long enterMxoTimestamp) {
        this.enterMxoTimestamp = enterMxoTimestamp;
    }

    public long getEnterEngineTimestamp() {
        return enterEngineTimestamp;
    }

    public void setEnterEngineTimestamp(long enterEngineTimestamp) {
        this.enterEngineTimestamp = enterEngineTimestamp;
    }

    public long getLeaveEngineTimestamp() {
        return leaveEngineTimestamp;
    }

    public void setLeaveEngineTimestamp(long leaveEngineTimestamp) {
        this.leaveEngineTimestamp = leaveEngineTimestamp;
    }

    public long getLeaveMxoTimestamp() {
        return leaveMxoTimestamp;
    }

    public void setLeaveMxoTimestamp(long leaveMxoTimestamp) {
        this.leaveMxoTimestamp = leaveMxoTimestamp;
    }

    public long getRecvFromMxoTimestamp() {
        return recvFromMxoTimestamp;
    }

    public void setRecvFromMxoTimestamp(long recvFromMxoTimestamp) {
        this.recvFromMxoTimestamp = recvFromMxoTimestamp;
    }

    public long getLeaveDriverTimestamp() {
        return leaveDriverTimestamp;
    }

    public void setLeaveDriverTimestamp(long leaveDriverTimestamp) {
        this.leaveDriverTimestamp = leaveDriverTimestamp;
    }

    public boolean isBatchFlag() {
        return batchFlag;
    }

    public void setBatchFlag(boolean batchFlag) {
        this.batchFlag = batchFlag;
    }

    public boolean isSelectFlag() {
        return selectFlag;
    }

    public void setSelectFlag(boolean selectFlag) {
        this.selectFlag = selectFlag;
    }

    public boolean isEndOfData() {
        return endOfData;
    }

    public void setEndOfData(boolean endOfData) {
        this.endOfData = endOfData;
    }

    public void checkLinkThreshold(String sql, T4Properties t4prop, ArrayList<TraceLinkThreshold> traceLinkList) {

        int len = traceLinkList.size();
        if (len == 0) {
            return;
        }
        TraceLinkThreshold Ts = traceLinkList.get(len - 1);
        long realLinkThreshold = Ts.getLeaveDriverTimestamp() - Ts.getEnterDriverTimestamp();
        long linkThreshold = t4prop.getLinkThreshold();
        if (t4prop.isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(t4prop, Level.FINER, "ENTRY ", "realLinkThreshold = " + realLinkThreshold);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("ENTRY. realLinkThreshold = {}", realLinkThreshold);
        }

        if (linkThreshold > 0 && realLinkThreshold >= linkThreshold) {
            StringBuffer sb = new StringBuffer();
            sb.append("\n").append("enterDriverTimestamp       ").append("sendToMxoTimestamp       ")
                .append("enterMxoTimestamp       ").append("enterEngineTimestamp       ")
                .append("leaveEngineTimestamp       ").append("leaveMxoTimestamp    ")
                .append("recvFromMxoTimestamp       ").append("leaveDriverTimestamp  ").append("odbcAPI").append("\n");
            for (TraceLinkThreshold ts : traceLinkList) {
                calculateTimestamp(ts);
                sb.append(timeStampToDate(ts.getEnterDriverTimestamp()) + fillBlanksByGivenLenForGivenStrLen(timeStampToDate(ts.getEnterDriverTimestamp())))
                    .append(timeStampToDate(ts.getSendToMxoTimestamp()) + fillBlanksByGivenLenForGivenStrLen(timeStampToDate(ts.getSendToMxoTimestamp())))
                    .append(timeStampToDate(ts.getEnterMxoTimestamp()) + fillBlanksByGivenLenForGivenStrLen(timeStampToDate(ts.getEnterMxoTimestamp())))
                    .append(timeStampToDate(ts.getEnterEngineTimestamp()) + fillBlanksByGivenLenForGivenStrLen(timeStampToDate(ts.getEnterEngineTimestamp())))
                    .append(timeStampToDate(ts.getLeaveEngineTimestamp()) + fillBlanksByGivenLenForGivenStrLen(timeStampToDate(ts.getLeaveEngineTimestamp())))
                    .append(timeStampToDate(ts.getLeaveMxoTimestamp()) + fillBlanksByGivenLenForGivenStrLen(timeStampToDate(ts.getLeaveMxoTimestamp())))
                    .append(timeStampToDate(ts.getRecvFromMxoTimestamp()) + fillBlanksByGivenLenForGivenStrLen(timeStampToDate(ts.getRecvFromMxoTimestamp())))
                    .append(timeStampToDate(ts.getLeaveDriverTimestamp()) + fillBlanksByGivenLenForGivenStrLen(timeStampToDate(ts.getLeaveDriverTimestamp())))
                    .append(ts.getOdbcAPI()).append("\n");
            }
            driverUsedTime += Ts.getLeaveDriverTimestamp() - Ts.getRecvFromMxoTimestamp();
            String tmp = "TraceLinkThreshold total = " + realLinkThreshold + ", driver = " + driverUsedTime + ", mxo = " + mxoUsedTime + ", engine = " + engineUsedTime;
            if (t4prop.isLogEnable(Level.WARNING)) {
                T4LoggingUtilities.log(t4prop, Level.WARNING, "ENTRY. ", t4prop.getRemoteProcess(), sql, tmp, sb.toString());
            }
            if (LOG.isWarnEnabled()) {
                LOG.warn("ENTRY. remoteProcess = {}, sql = {}, {}, {}", t4prop.getRemoteProcess(), sql, tmp, sb.toString());
            }
        }
        initLinkThreshold();

    }

    private void calculateTimestamp(TraceLinkThreshold ts) {
        if (receiveTs == 0) {
            driverUsedTime += ts.getSendToMxoTimestamp() - ts.getEnterDriverTimestamp();
            receiveTs = ts.getRecvFromMxoTimestamp();
        }else if ((ts.isSelectFlag() || ts.isBatchFlag()) && ts.getSendToMxoTimestamp() >= receiveTs){
            driverUsedTime += ts.getSendToMxoTimestamp() - receiveTs;
            receiveTs = ts.getRecvFromMxoTimestamp();
        }
        mxoUsedTime += ts.getLeaveMxoTimestamp() - ts.getEnterMxoTimestamp() + ts.getEnterEngineTimestamp() - ts.getLeaveEngineTimestamp();
        engineUsedTime += ts.getLeaveEngineTimestamp() - ts.getEnterEngineTimestamp();
    }

    private String timeStampToDate(long timestamp) {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        return sf.format(timestamp);
    }

    private String fillBlanksByGivenLenForGivenStrLen(String str) {
        int len = 25;
        byte[] b = new byte[len - str.length()];
        Arrays.fill(b, (byte) 0x20);
        return new String(b);
    }

}
