package org.trafodion.jdbc.t4;

public class BaseReply {
    private boolean reconn;
    private int esps;
    protected String qid;
    private int resNum = 1;

    private long tmpSec = 0;
    private long tmpUsec = 0;

    protected long enterMxoTimestamp;
    protected long enterEngineTimestamp;
    protected long leaveEngineTimestamp;
    protected long leaveMxoTimestamp;

    protected boolean linkThresholdFlag = false;

    protected void skipHeader(LogicalByteArray buf) {
        buf.setLocation(Header.sizeOf());
    }
    
    protected void processExpansion(LogicalByteArray buf) {
        int loc = buf.getLocation();
        int len = buf.getLength();
        if (loc >= len)
            return;
        int optNum = buf.extractInt();
        while (optNum > 0) {
            int optType = buf.extractInt();
            switch (optType) {
                case TRANSPORT.RES_EXTENSION_REBALANCE:
                case TRANSPORT.RES_EXTENSION_RESTART:
                case TRANSPORT.RES_EXTENSION_DISABLE:
                    if (buf.extractInt() > 0) {
                        reconn = true;
                    }
                    // do reconn !!!
                    break;
                case TRANSPORT.RES_EXTENSION_RESTORE:
                    resNum = buf.extractInt();
                    break;
                case TRANSPORT.RES_EXTENSION_ENTERMXOTIMESTAMP:
                    tmpSec = (long) buf.extractInt();
                    tmpUsec = (long) buf.extractInt();
                    if (linkThresholdFlag) {
                        enterMxoTimestamp = tmpSec * 1000 + tmpUsec / 1000;
                    }
                    break;
                case TRANSPORT.RES_EXTENSION_ENTERENGINETIMESTAMP:
                    tmpSec = (long) buf.extractInt();
                    tmpUsec = (long) buf.extractInt();
                    if (linkThresholdFlag) {
                        enterEngineTimestamp = tmpSec * 1000 + tmpUsec / 1000;
                    }
                    break;
                case TRANSPORT.RES_EXTENSION_LEAVEENGINETIMESTAMP:
                    tmpSec = (long) buf.extractInt();
                    tmpUsec = (long) buf.extractInt();
                    if (linkThresholdFlag) {
                        leaveEngineTimestamp = tmpSec * 1000 + tmpUsec / 1000;
                    }
                    break;
                case TRANSPORT.RES_EXTENSION_LEAVEMXOTIMESTAMP:
                    tmpSec = (long) buf.extractInt();
                    tmpUsec = (long) buf.extractInt();
                    if (linkThresholdFlag) {
                        leaveMxoTimestamp = tmpSec * 1000 + tmpUsec / 1000;
                    }
                    break;
                default:
                    break;
            }
            optNum--;
        }
    }

    protected int processPrepareReplayExpansion(LogicalByteArray buf) {
        int loc = buf.getLocation();
        int len = buf.getLength();
        int res = 0;
        if (loc >= len)
            return res;
        int optNum = buf.extractInt();
        int qidLen = 0;
        while (optNum > 0) {
            int optType = buf.extractInt();
            switch (optType) {
                case TRANSPORT.RES_PREPARE_EXTENSION_ESPS:
                    esps  = buf.extractInt();
                    break;
                case 1:
                    qidLen  = buf.extractInt();
                    break;
                case 2:
                    qid  = new String(buf.extractByteArray(qidLen));
                    break;
                default:
                    break;
            }
            optNum--;
        }
        return res;
    }

    protected boolean shouldReconn() {
        return reconn;
    }
    protected int getEspNum() {
        return esps;
    }

    protected int getResNum() {
        return resNum;
    }

    protected void setLinkThreshold(boolean linkThresholdFlag) {
        this.linkThresholdFlag = linkThresholdFlag;
    }

}
