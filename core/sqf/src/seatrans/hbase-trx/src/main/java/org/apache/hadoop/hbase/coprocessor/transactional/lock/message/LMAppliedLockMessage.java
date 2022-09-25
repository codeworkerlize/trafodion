package org.apache.hadoop.hbase.coprocessor.transactional.lock.message;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;

public class LMAppliedLockMessage extends RSMessage {
    private static final long serialVersionUID = 1L;
    @Getter
    private Map<String, LMAppliedLockInfoInPerServer> serverList;

    public LMAppliedLockMessage() {
        super(MSG_TYP_LOCK_APPLIED_LOCK_RESPONSE);
        serverList = new HashMap<>();//no need concurrent
    }

    public void merge(RSMessage message) {
        LMAppliedLockMessage tmp = (LMAppliedLockMessage) message;
        //only one
        Map.Entry<String, LMAppliedLockInfoInPerServer> info = tmp.serverList.entrySet().iterator().next();
        this.serverList.put(info.getKey(), info.getValue());
    }

    public void display() {
        String splitLine = "-------------------------------------------------------------\n";
        StringBuffer result = new StringBuffer();
        List<StringBuffer> rows = new ArrayList<>();
        long sumLockHolderNum = 0;
        int count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
        for (Map.Entry<String, LMAppliedLockInfoInPerServer> entry : serverList.entrySet()) {
            String regionServerName = entry.getKey();
            LMAppliedLockInfoInPerServer info = entry.getValue();
            result.append("Queue for applied lock transaction:\n");
            result.append(splitLine);
            //lockHolder number
            result.append("RegionServer: " + regionServerName + "\n");
            result.append("The maximum number of the lockHolder on regionServer: "
                    + (info.lockHolderLimitNum == 0 ? "ulimit" : String.valueOf(info.lockHolderLimitNum)) + "\n");
            result.append("The maximum number of the lockHolder on transaction:  "
                    + (info.lockHolderPerTransLimitNum == 0 ? "ulimit" : String.valueOf(info.lockHolderPerTransLimitNum)) + "\n");
            result.append(splitLine);
            result.append("Transaction information\n");
            StringUtil.generateHeader(result, LockConstants.LOCK_APPLIED_LOCK_HEADER);
            for (Map.Entry<Long, Integer> entry2 : info.transMap.entrySet()) {
                StringUtil.formatOutputWide(rows, 1, String.valueOf(entry2.getKey()));
                int lockNum = entry2.getValue();
                if (lockNum > 0)
                    StringUtil.formatOutputWide(rows, 2, String.valueOf(lockNum));
                else
                    StringUtil.formatOutputWide(rows, 2, String.valueOf(Math.abs(lockNum)) + " - in rollback");
                //only one line
                result.append(rows.get(0) + "\n");
                rows.clear();
                count--;
                if (count == 0) {
                    System.out.print(result.toString());
                    result.delete(0, result.length());
                    count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
                }
            }
            result.append(splitLine);
            sumLockHolderNum += info.sumLockNum;
            result.append("Total: " + info.sumLockNum + " LockHolders in use.\n");
            result.append("Maximum: " + info.maxLockNum + " LockHolders in used.\n");

            //trace lockHolder applications
            if (!info.appliedList.isEmpty()) {
                result.append(splitLine);
                result.append("LockHolder details:\n");
                result.append(splitLine);
                StringUtil.generateHeader(result, LockConstants.LOCK_APPLIED_LOCK_HEADER_DEBUG);
                for (LockHolderInfo holder : info.appliedList) {
                    StringUtil.formatOutputWide(rows, 1, String.valueOf(holder.getTxID()));
                    String regionName = holder.getRegionName();
                    if (regionName != null) {
                        //TableName
                        int end = regionName.indexOf(",");
                        if (end > -1) {
                            StringUtil.formatOutputWide(rows, 2, regionName.substring(0, end));
                        } else {
                            StringUtil.formatOutputWide(rows, 2, "");
                        }
                        //RegionName
                        end = regionName.lastIndexOf(",");
                        if (end > -1) {
                            StringUtil.formatOutputWide(rows, 3, regionName.substring(end + 1));
                        } else {
                            StringUtil.formatOutputWide(rows, 3, "");
                        }
                    } else {
                        StringUtil.formatOutputWide(rows, 2, "");
                        StringUtil.formatOutputWide(rows, 3, "");
                    }
                    StringUtil.formatOutputWide(rows, 4, holder.getRowKey());
                    StringUtil.formatOutputWide(rows, 5, holder.getLockMode());
                    for (StringBuffer row : rows) {
                        result.append(row).append("\n");
                    }
                    rows.clear();
                    count--;
                    if (count == 0) {
                        System.out.print(result.toString());
                        result.delete(0, result.length());
                        count = LockConstants.NUMBER_OF_OUTPUT_LOGS_PER_TIME;
                    }
                }
            }
            result.append(splitLine);
        }
        result.append("total " + sumLockHolderNum + " LockHolders in use.");
        System.out.println(result.toString());
    }
}
