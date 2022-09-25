package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONArray;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMLockMemInfoMessage extends RSMessage {

    public enum MEMORYUNIT {
        KB, MB, GB
    }

    private static final long serialVersionUID = 1L;
    @Getter
    @Setter
    private List<LMLockMemInfoMessage> regionList = null;

    @Getter
    @Setter
    String regionName;

    @Getter
    @Setter
    private long LockMemSize;

    @Getter
    @Setter
    private int usingLockNum;

    @Getter
    @Setter
    private int usingLockHolderNum;

    @Getter
    @Setter
    private int usingTransactionNum;

    @Getter
    @Setter
    private long memUsed;

    @Getter
    @Setter
    private long memMax;

    @Setter
    private MEMORYUNIT memunit;

    public LMLockMemInfoMessage() {
        super(RSMessage.MSG_TYP_LOCK_MEM_INFO_RESPONSE);
        LockMemSize = 0L;
        LockMemSize = 0L;
        regionName = "";
        regionList = null;
        memunit = MEMORYUNIT.KB;
    }

    @Override
    public void merge(RSMessage message) {
        LMLockMemInfoMessage singleNodeMessage = (LMLockMemInfoMessage) message;
        if (regionList == null) {
            regionList = new ArrayList<>();
        }
        regionList.add(singleNodeMessage);
    }

    public String getMemInfoJson() {
        if (regionList == null || regionList.isEmpty()) {
            return "{}";
        }
        JSONArray array = new JSONArray();
        long summayLockMemSize = 0L;
        for (LMLockMemInfoMessage lmLockMemInfoReqMessage : regionList) {
            JSONObject json = new JSONObject();
            long lockMemSize = lmLockMemInfoReqMessage.getLockMemSize();
            long memUsed = lmLockMemInfoReqMessage.getMemUsed();
            long memMax = lmLockMemInfoReqMessage.getMemMax();
            long percent = 0;
            if (memMax > 0)
                percent = (int) (memUsed * 1.0 / memMax * 100);
            json.put("regionName", lmLockMemInfoReqMessage.getRegionName());
            json.put("totalJmem", memMax);
            json.put("inuseJmem", memUsed);
            json.put("inuse", (percent != 0) ? String.valueOf(percent) : "Err");
            json.put("Lockmemory", memoryUnit2String(lockMemSize));
            json.put("usingLockNum", String.valueOf(lmLockMemInfoReqMessage.getUsingLockNum()));
            json.put("usingTransaction", String.valueOf(lmLockMemInfoReqMessage.getUsingTransactionNum()));
            json.put("usingLockHolderNum", String.valueOf(lmLockMemInfoReqMessage.getUsingLockHolderNum()));
            array.put(json);
            summayLockMemSize += lockMemSize;
        }
        JSONObject outputJson = new JSONObject();
        outputJson.put("summary", memoryUnit2String(summayLockMemSize));
        outputJson.put("region", array);
        return outputJson.toString();
    }

    public void getMemInfo() {
        getMemInfo(null);
    }

    public void getMemInfo(Logger LOG) {
        long summayLockMemSize = 0L;
        StringBuffer result = new StringBuffer();
        result.append("lock memory useage on each regionServer\n");
        StringUtil.generateHeader(result, LockConstants.LOCK_REGION_MEMORY_INFO_HEADER);
        if (regionList != null && !regionList.isEmpty()) {
            for (LMLockMemInfoMessage lmLockMemInfoMessage : regionList) {
                generateRow(result, lmLockMemInfoMessage);
                summayLockMemSize += lmLockMemInfoMessage.getLockMemSize();
            }
        }
        String outputStr = "summary lock memory useage\n" + memoryUnit2String(summayLockMemSize) + "\n"
                + result.toString();

        if (LOG != null) {
            LOG.info(outputStr);
        } else {
            System.out.print(outputStr);
        }
    }

    private void generateRow(StringBuffer result, LMLockMemInfoMessage memInfo) {
        List<StringBuffer> rows = new ArrayList<>();
        long memUsed = memInfo.getMemUsed();
        long memMax = memInfo.getMemMax();
        long percent = 0;
        if (memMax > 0)
            percent = (int) (memUsed * 1.0 / memMax * 100);
        StringUtil.formatOutputWide(rows, 1, memInfo.getRegionName());
        StringUtil.formatOutputWide(rows, 2, memoryUnit2String(memMax));
        StringUtil.formatOutputWide(rows, 3, memoryUnit2String(memUsed));
        StringUtil.formatOutputWide(rows, 4, (percent != 0) ? String.valueOf(percent) : "Err");
        StringUtil.formatOutputWide(rows, 5, memoryUnit2String(memInfo.getLockMemSize()));
        StringUtil.formatOutputWide(rows, 6, String.valueOf(memInfo.getUsingLockNum()));
        StringUtil.formatOutputWide(rows, 7, String.valueOf(memInfo.getUsingTransactionNum()));
        StringUtil.formatOutputWide(rows, 8, String.valueOf(memInfo.getUsingLockHolderNum()));
        for (StringBuffer row : rows) {
            result.append(row).append("\n");
        }
    }

    private String memoryUnit2String(long originalValue) {
        char unitChar;
        double value = 0.0;
        switch (memunit) {
            case KB:
                value = originalValue * 1.0 / 1024.0;
                unitChar = 'K';
                break;
            case MB:
                value = originalValue * 1.0 / 1048576.0;//1024 * 1024
                unitChar = 'M';
                break;
            default:
                value = originalValue * 1.0 / 1073741824.0;//1024 * 1024 * 1024
                unitChar = 'G';
                break;
        }
        return String.format("%.2f%c", value, unitChar);
    }
}