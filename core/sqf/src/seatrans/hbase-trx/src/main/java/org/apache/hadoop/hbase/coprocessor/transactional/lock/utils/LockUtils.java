package org.apache.hadoop.hbase.coprocessor.transactional.lock.utils;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.LockWaitStatus;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSServer;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockInfoReqMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockStaticisticsData;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockInfoMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.message.RSMessage;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;

public class LockUtils {

    public static int BITS_OFF[] = new int[LockConstants.LOCK_MAX_MODES];
    public static int BITS_ON[] = new int[LockConstants.LOCK_MAX_MODES];

    static {
        for (int i = 0, bit = 1; i < LockConstants.LOCK_MAX_MODES; i++, bit <<= 1) {
            BITS_ON[i] = bit;
            BITS_OFF[i] = ~bit;
        }
    }
    public static boolean existsIS(int mask){
        if ((mask & 1) == 1){
            return true;
        }
        return false;
    }
    public static boolean existsS(int mask){
        if ((mask & 2) == 2){
            return true;
        }
        return false;
    }
    public static boolean existsIX(int mask){
        if ((mask & 4) == 4){
            return true;
        }
        return false;
    }
    public static boolean existsU(int mask){
        if ((mask & 8) == 8){
            return true;
        }
        return false;
    }
    public static boolean existsX(int mask){
        if ((mask & 16) == 16){
            return true;
        }
        return false;
    }

    public static boolean existsRS(int mask) {
        if ((mask & 32) == 32){
            return true;
        }
        return false;
    }

    public static boolean existsRX(int mask) {
        if ((mask & 64) == 64){
            return true;
        }
        return false;
    }

    public static boolean isIntentionLock(int mask) {
        return existsIS(mask) || existsIX(mask);
    }

    public static int getMask(long[] locks) {
        int mask = 0;
        for(int i = 0; i < LockConstants.LOCK_MAX_MODES; i++) {
            if (locks[i] != 0) {
              mask |= BITS_ON[i];
            }
            else {
                mask &= BITS_OFF[i];
            }
        }
        return mask;
    }
    
    public static void getListOfLocksInfo(List<Long> txList, Logger LOG) {
        LMLockInfoReqMessage lmReqMsg = new LMLockInfoReqMessage(RSMessage.MSG_TYP_LOCK_INFO);
        lmReqMsg.setType("LOCK_INFO");
        lmReqMsg.setTxIDs(txList);
        lmReqMsg.setDebug(0);
        LMLockInfoMessage lmInfoMsg = null;
        try {
            lmInfoMsg = RSServer.getInstance().processLockInfo(lmReqMsg, null);
            lmInfoMsg.getAllLock(LOG);
        } catch (Exception e) {
            LOG.error("failed to get lock info", e);
        }
    }

    public static LinkedHashMap<Long, LMLockStaticisticsData> sortMapByValue(Map<Long, LMLockStaticisticsData> map, int topN) {
        LinkedHashMap<Long, LMLockStaticisticsData> sortedMap = new LinkedHashMap<>();

        List<Map.Entry<Long, LMLockStaticisticsData>> lists = new ArrayList<Map.Entry<Long, LMLockStaticisticsData>>(map.entrySet());
        Collections.sort(lists, new Comparator<Map.Entry<Long, LMLockStaticisticsData>>() {
            @Override
            public int compare(Entry<Long, LMLockStaticisticsData> o1, Entry<Long, LMLockStaticisticsData> o2) {
                // TODO Auto-generated method stub
                int q1 = o1.getValue().getLockNum();
                int q2 = o2.getValue().getLockNum();
                int p = q2 - q1;
                if (p > 0) {
                    return 1;
                } else if (p == 0) {
                    return 0;
                } else
                    return -1;
            }
        });

        if (topN > 0 && lists.size() >= topN) {
            for (Map.Entry<Long, LMLockStaticisticsData> set : lists.subList(0, topN)) {
                sortedMap.put(set.getKey(), set.getValue());
            }
        } else {
            for (Map.Entry<Long, LMLockStaticisticsData> set : lists) {
                sortedMap.put(set.getKey(), set.getValue());
            }
        }

        return sortedMap;
    }

    public static LinkedHashMap<String, Integer> sortMapByValue(Map<String, Integer> map) {
        LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<>();

        List<Map.Entry<String, Integer>> lists = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
        Collections.sort(lists, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                int q1 = o1.getValue();
                int q2 = o2.getValue();
                int p = q2 - q1;
                if (p > 0) {
                    return 1;
                } else if (p == 0) {
                    return 0;
                } else
                    return -1;
            }
        });
        for (Map.Entry<String, Integer> set : lists) {
            sortedMap.put(set.getKey(), set.getValue());
        }

        return sortedMap;
    }

    public static String getWaitLockStatus(LockWaitStatus type) {
        switch (type) {
            case CANCEL_FOR_DEADLOCK:
                return "DEAD";
            case CANCEL_FOR_ROLLBACK:
                return "ROLLBACK";
            case CANCEL_FOR_SPLIT:
                return "SPLIT";
            case CANCEL_FOR_MOVE:
                return "MOVE";
            case CANCEL_FOR_NEW_RPC_REQUEST:
                return "NEW_REQ";
            case WAITING:
                return "WAITING";
            case LOCK_TIMEOUT:
            case FINAL_LOCK_TIMEOUT:
                return "TIMEOUT";
            case CANCEL_FOR_NOT_ENOUGH_LOCK_RESOURCES:
                return "NOT_ENOUGH_LOCK_RESOURCES";
            case OK:
                return "GOT_LOCK";
            default:
                return type.name();
        }
    }

    public static long alignLockObjectSize(long size) {
        return size + (size % 8);
    }

    public static int getNodeId(long txID) {
        return (int) ((txID >> 32) & 0xFFL);
    }
}
