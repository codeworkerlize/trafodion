package org.apache.hadoop.hbase.coprocessor.transactional;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSConstants;

public class EndpointCostStats {
  private static final Log LOG = LogFactory.getLog(EndpointCostStats.class);
  private ConcurrentHashMap<Long, AtomicLong> callCount = new ConcurrentHashMap<Long, AtomicLong>();
  private ConcurrentHashMap<Long, AtomicLong> sumCost = new ConcurrentHashMap<Long, AtomicLong>();
  private static final String newLine = System.getProperty("line.separator");
  private boolean hasPrinted = false;
  private AtomicLong lockTime = new AtomicLong(0);
  private static HashMap<Long, String> funcNameMap;

  static {
    funcNameMap = new HashMap<Long, String>();
    funcNameMap.put(1L, " abortSavepoint ");
    funcNameMap.put(2L, " abortTransaction ");
    funcNameMap.put(3L, " abortTransactionMultiple ");
    funcNameMap.put(4L, " beginTransaction ");
    funcNameMap.put(5L, " commit ");
    funcNameMap.put(6L, " commitMultiple ");
    funcNameMap.put(7L, " commitIfPossible ");
    funcNameMap.put(8L, " commitRequest ");
    funcNameMap.put(9L, " commitRequestMultiple ");
    funcNameMap.put(10L, " commitSavepoint ");
    funcNameMap.put(11L, " checkAndDeleteRegionTx ");
    funcNameMap.put(12L, " checkAndDelete ");
    funcNameMap.put(13L, " checkAndPut ");
    funcNameMap.put(14L, " checkAndPutRegionTx ");
    funcNameMap.put(15L, " closeScanner ");
    funcNameMap.put(16L, " deleteMultiple ");
    funcNameMap.put(17L, " deleteMultipleNonTxn ");
    funcNameMap.put(18L, " deleteRegionTx ");
    funcNameMap.put(19L, " delete ");
    funcNameMap.put(20L, " get ");
    funcNameMap.put(21L, " getMultiple ");
    funcNameMap.put(22L, " openScanner ");
    funcNameMap.put(23L, " performScan ");
    funcNameMap.put(24L, " deleteTlogEntries ");
    funcNameMap.put(25L, " put ");
    funcNameMap.put(26L, " putMultiple ");
    funcNameMap.put(27L, " putMultipleNonTxn ");
    funcNameMap.put(28L, " recoveryRequest ");
    funcNameMap.put(29L, " getRowNum ");
    funcNameMap.put(30L, " putRegionTx ");
    funcNameMap.put(31L, " pushOnlineEpoch ");
    funcNameMap.put(32L, " putTlog ");
    //funcNameMap.put(33L, "");
    //funcNameMap.put(34L, "");

    String costThreshold = System.getenv("RECORD_TIME_COST_COPRO_ALL");
    if (costThreshold != null && false == costThreshold.trim().isEmpty())
        RSConstants.RECORD_TIME_COST_COPRO_ALL = Integer.parseInt(costThreshold);
  } 

  public void callCountPlus(Long funcId) {
    AtomicLong count = callCount.get(funcId);
    synchronized (callCount) {
      if (count == null) {
        count = new AtomicLong(0);
        callCount.put(funcId, count);
      }
    }
    count.incrementAndGet(); 
  }

  public void sumCostPlus(Long funcId, long timeCost) {
    AtomicLong cost = sumCost.get(funcId);
    synchronized (sumCost) {
      if (cost == null) {
        cost = new AtomicLong(0);
        sumCost.put(funcId, cost);
      }
    }
    cost.addAndGet(timeCost);
  }

  public void lockTimeCostPlus(long cost) {
    lockTime.addAndGet(cost);
  }

  public synchronized void printLog(long transId) {
    if (hasPrinted)
      return;

    if (RSConstants.RECORD_TIME_COST_COPRO_ALL >= 0) {
        StringBuilder loginfo = new StringBuilder();
        long totalCost = 0;
        hasPrinted = true;
        for (Entry<Long, AtomicLong> entry : sumCost.entrySet()) {
            Long funcId = entry.getKey();
            AtomicLong cost = entry.getValue();
            AtomicLong count = callCount.get(funcId);
            if (count.longValue() <= 0)
                continue;
            double avgCost = ((double)cost.longValue()) / count.longValue();
            totalCost += cost.longValue();
            loginfo.append(funcNameMap.get(funcId) + " FCC " + count + " FATC " + avgCost + " FTTC " + cost + " ");
        }
        if (totalCost >= RSConstants.RECORD_TIME_COST_COPRO_ALL) {
            loginfo.append(" TTC " + totalCost + " LTC " + lockTime.longValue() + " txID " + transId);
            LOG.warn(loginfo.toString());
        }
    }
  }

  public void clear() {
    callCount.clear();
    sumCost.clear();
    hasPrinted = false;
    lockTime = new AtomicLong(0);
  }
}
