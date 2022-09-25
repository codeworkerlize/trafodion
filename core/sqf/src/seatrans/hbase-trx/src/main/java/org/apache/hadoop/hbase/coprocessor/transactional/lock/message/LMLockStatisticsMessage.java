package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMLockStatisticsMessage extends RSMessage {
    private static final long serialVersionUID = 1L;

    @Getter
    @Setter
    ConcurrentHashMap<Long, ConcurrentHashMap<String, Integer>> txLockNumMapMap = new ConcurrentHashMap<>();
    @Getter
    @Setter
    ConcurrentHashMap<Long, LMLockStaticisticsData> txLockNumMap = new ConcurrentHashMap<>();

    public LMLockStatisticsMessage() {
        super(MSG_TYP_LOCK_STATISTICS_INFO_RESPONSE);
    }

    public void getLockStatistics(LMLockStatisticsReqMessage lmLockInfoReqMessage) {
        RSServer rsServer = RSServer.getInstance();
        rsServer.getLockStatistics(txLockNumMapMap, txLockNumMap, lmLockInfoReqMessage.getTopN(), lmLockInfoReqMessage.getDebug(), lmLockInfoReqMessage.getTxIDs(), lmLockInfoReqMessage.getTableNames());
    }

    public void merge(LMLockStatisticsMessage remoteMessage, int topN) {
        if (topN > 0) {
            ConcurrentHashMap<Long, LMLockStaticisticsData> currentTxLockNumMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<Long, ConcurrentHashMap<String, Integer>> currentTxLockNumMapMap = new ConcurrentHashMap<>();
            for (Map.Entry<Long, LMLockStaticisticsData> entry : txLockNumMap.entrySet()) {
                if (currentTxLockNumMap.containsKey(entry.getKey())) {
                    LMLockStaticisticsData lmLSD = currentTxLockNumMap.get(entry.getKey());
                    lmLSD.setLockNum(entry.getValue().getLockNum() + lmLSD.getLockNum());
                } else {
                    currentTxLockNumMap.put(entry.getKey(), entry.getValue());
                }
            }
            for (Map.Entry<Long, ConcurrentHashMap<String, Integer>> entry : txLockNumMapMap.entrySet()) {
                if (currentTxLockNumMapMap.containsKey(entry.getKey())) {
                    currentTxLockNumMapMap.get(entry.getKey()).putAll(entry.getValue());
                } else {
                    currentTxLockNumMapMap.put(entry.getKey(), entry.getValue());
                }
            }
            
            for (Map.Entry<Long, LMLockStaticisticsData> entry : remoteMessage.getTxLockNumMap().entrySet()) {
                if (currentTxLockNumMap.containsKey(entry.getKey())) {
                    LMLockStaticisticsData lmLSD = currentTxLockNumMap.get(entry.getKey());
                    lmLSD.setLockNum(entry.getValue().getLockNum() + lmLSD.getLockNum());
                } else {
                    currentTxLockNumMap.put(entry.getKey(), entry.getValue());
                }
            }
            for (Map.Entry<Long, ConcurrentHashMap<String, Integer>> entry : remoteMessage.getTxLockNumMapMap().entrySet()) {
                if (currentTxLockNumMapMap.containsKey(entry.getKey())) {
                    currentTxLockNumMapMap.get(entry.getKey()).putAll(entry.getValue());
                } else {
                    currentTxLockNumMapMap.put(entry.getKey(), entry.getValue());
                }
            }
            txLockNumMap.clear();
            txLockNumMapMap.clear();

            LinkedHashMap<Long, LMLockStaticisticsData> sortedTxLockNumMap = LockUtils.sortMapByValue(currentTxLockNumMap, topN);

            for (Map.Entry<Long, LMLockStaticisticsData> entry : sortedTxLockNumMap.entrySet()) {
                txLockNumMap.put(entry.getKey(), entry.getValue());
                txLockNumMapMap.put(entry.getKey(), currentTxLockNumMapMap.get(entry.getKey()));
            }
        } else {
            for (Map.Entry<Long, LMLockStaticisticsData> entry : remoteMessage.getTxLockNumMap().entrySet()) {
                if (txLockNumMap.containsKey(entry.getKey())) {
                    LMLockStaticisticsData lmLSD = txLockNumMap.get(entry.getKey());
                    lmLSD.setLockNum(entry.getValue().getLockNum() + lmLSD.getLockNum());
                } else {
                    txLockNumMap.put(entry.getKey(), entry.getValue());
                }
            }
            for (Map.Entry<Long, ConcurrentHashMap<String, Integer>> entry : remoteMessage.getTxLockNumMapMap().entrySet()) {
                if (txLockNumMapMap.containsKey(entry.getKey())) {
                    txLockNumMapMap.get(entry.getKey()).putAll(entry.getValue());
                } else {
                    txLockNumMapMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private void generateRow(StringBuffer result, long txID, LMLockStaticisticsData lmLSD) {
        if (lmLSD.getLockNum() == 0) {
            return;
        }
        List<StringBuffer> rows = new ArrayList<>();
        StringUtil.formatOutputWide(rows, 1, String.valueOf(txID));
        StringUtil.formatOutputWide(rows, 2, String.valueOf(lmLSD.getLockNum()));
        StringUtil.formatOutputWide(rows, 3, lmLSD.getQueryContext());
        for (StringBuffer row : rows) {
            result.append(row).append("\n");
        }
        System.out.print(result.toString());
    }
    
    private void generateRow(StringBuffer result, long txID, String regionName, int lockNum) {
        if (lockNum == 0 && regionName != null) {
            return;
        }
        List<StringBuffer> rows = new ArrayList<>();
        StringUtil.formatOutputWide(rows, 1, String.valueOf(txID));
        if (regionName != null) {
            int idx = regionName.indexOf(",");
            StringUtil.formatOutputWide(rows, 2, regionName.substring(0, idx));
            idx = regionName.lastIndexOf(",");
            StringUtil.formatOutputWide(rows, 3, regionName.substring(idx + 1));
            StringUtil.formatOutputWide(rows, 4, String.valueOf(lockNum));
        } else {
            StringUtil.formatOutputWide(rows, 2, String.valueOf(lockNum));
        }
        for (StringBuffer row : rows) {
            result.append(row).append("\n");
        }
        System.out.print(result.toString());
    }

    public void getAllLockStatistics() {
        StringBuffer result = new StringBuffer();
        StringUtil.generateHeader(result, LockConstants.LOCK_STATISTICS_HEADER);
        System.out.print(result.toString());
        for (Map.Entry<Long, LMLockStaticisticsData> entry : txLockNumMap.entrySet()) {
            result.delete(0, result.length());
            generateRow(result, entry.getKey(), entry.getValue());
        }
        result.delete(0, result.length());
        StringUtil.generateHeader(result, LockConstants.LOCK_REGION_STATISTICS_HEADER);
        System.out.print(result.toString());
        long txID = 0;
        LinkedHashMap<String, Integer> sortedMap = null;
        for (Map.Entry<Long, ConcurrentHashMap<String, Integer>> entry : txLockNumMapMap.entrySet()) {
            txID = entry.getKey();
            sortedMap = LockUtils.sortMapByValue(entry.getValue());
            for (Map.Entry<String, Integer> innerEntry : sortedMap.entrySet()) {
                result.delete(0, result.length());
                generateRow(result, txID, innerEntry.getKey(), innerEntry.getValue());
            }
        }
    }

    public String getAllLockStatisticsJson() {
        JSONObject json = new JSONObject();
        json.put("summary", new JSONObject(txLockNumMap));
        json.put("detail", new JSONObject(txLockNumMapMap));
        return json.toString();
    }
}

