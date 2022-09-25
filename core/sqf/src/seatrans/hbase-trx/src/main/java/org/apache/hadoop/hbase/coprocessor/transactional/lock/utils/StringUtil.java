package org.apache.hadoop.hbase.coprocessor.transactional.lock.utils;

import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.RowKey;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.deadlock.LockWait;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockMsg;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockWaitMsg;

import net.jpountz.lz4.*;

public class StringUtil {
    private static final String colSplit = " | ";
    private static char fillChar = ' ';
    private static LZ4Factory factory = LZ4Factory.safeInstance();

    /*{"txID", "table name", "rowkey", "lock mode", };
   {"txID", "table name", "rowkey", "lock mode", "holder txID"};*/
    public static void generateRow(StringBuffer result, String[] rowData) {
        List<StringBuffer> rows = new ArrayList<>();
        for (int i = 1; i <= rowData.length; i++) {
            StringUtil.formatOutputWide(rows, i, rowData[i - 1]);
        }
        for (StringBuffer row : rows) {
            result.append(row).append("\n");
        }
    }

    public static void generateHeader(StringBuffer result, String[] header) {
        generateRow(result, header);
        String[] headerLine = new String[header.length];
        char[] headerLineCharArr = null;
        Integer colWidth = 0;
        for (int i = 1; i <= headerLine.length; i++) {
            colWidth = LockConstants.COLUMN_WIDTH_MAP.get(i);
            if (colWidth == null) {
                headerLineCharArr = new char[19];
            } else {
                headerLineCharArr = new char[colWidth - 1];
            }
            Arrays.fill(headerLineCharArr, '-');

            headerLine[i - 1] = new String(headerLineCharArr);
        }
        generateRow(result, headerLine);
    }

    public static void generateNewRow(StringBuffer result, String[] rowData) {
        List<StringBuffer> rows = new ArrayList<>();
        for (int i = 1; i <= rowData.length; i++) {
            StringUtil.formatOutputWide(rows, i, rowData[i - 1]);
        }
        result.append(rows.get(0));
    }

    public static void formatOutputWide(List<StringBuffer> rows, int idx, String output) {
        Integer colWidth = LockConstants.COLUMN_WIDTH_MAP.get(idx);
        if (colWidth == null) {
            formatOutput(rows, idx, output, 20, fillChar);
        } else {
            formatOutput(rows, idx, output, colWidth, fillChar);
        }
    }

    public static void formatOutput(List<StringBuffer> rows, int idx, String output, int colWidth, char fillChar) {
        if (output == null) {
            output = "";
        }
        StringBuffer currentRow = null;
        StringBuffer newRow = null;
        if (rows.size() == 0) {
            currentRow = new StringBuffer();
            rows.add(currentRow);
        } else {
            currentRow = rows.get(0);
        }
        int currentRowLength = currentRow.length();
        int outputLen = output.length();
        int idxRow = 0;
        while (outputLen > colWidth) {
            if (output.length() > colWidth) {
                if (idx > 1) {
                    currentRow.append(colSplit);
                }
                currentRow.append(output.substring(0, colWidth));
                if (idxRow < rows.size() - 1) {
                    idxRow += 1;
                    currentRow = rows.get(idxRow);
                } else {
                    idxRow += 1;
                    currentRow = new StringBuffer();
                    if (idx > 1) {
                        String[] tmpData = new String[idx - 1];
                        for (int i = 0; i < tmpData.length; i++) {
                            tmpData[i] = "";
                        }
                        generateNewRow(currentRow, tmpData);
                    }
                    rows.add(currentRow);
                }
            }
            output = output.substring(colWidth);
            outputLen = output.length();
        }
        if (idx > 1) {
            currentRow.append(colSplit);
        }
        currentRow.append(output);

        if (outputLen < colWidth) {
            for (int i = outputLen; i < colWidth; i++) {
                currentRow.append(fillChar);
            }
        }
        for (int i = idxRow + 1; i < rows.size(); i++) {
            currentRow = rows.get(i);
            if (idx > 1) {
                currentRow.append(colSplit);
            }
            for (int j = 0; j < colWidth; j++) {
                currentRow.append(fillChar);
            }
        }
    }

    public static String getLockMode(long[] holding) {
        if (holding == null) {
            return "";
        }
        StringBuffer lockMode = new StringBuffer();
        boolean flag = false;
        for (int i=0; i< holding.length; i++) {
            if (holding[i] > 0) {
                if (!flag) {
                    flag = true;
                } else {
                    lockMode.append(",");
                }
                switch (i) {
                    case 0:
                        lockMode.append("IS");
                        break;
                    case 1:
                        lockMode.append("S");
                        break;
                    case 2:
                        lockMode.append("IX");
                        break;
                    case 3:
                        lockMode.append("U");
                        break;
                    case 4:
                        lockMode.append("X");
                        break;
                }
            }
        }
        return lockMode.toString();
    }
    
    public static void generateRowForWaitLock(StringBuffer result, SimpleDateFormat formatter, LockWait lock) {
        List<StringBuffer> rows = new ArrayList<>();
        StringUtil.formatOutputWide(rows, 1, String.valueOf(lock.getTxID()));
        String regionName = lock.getRegionName();
        //TableName
        {
            int end = regionName.indexOf(",");
            StringUtil.formatOutputWide(rows, 2, regionName.substring(0, end));
        }
        //RegionName
        StringUtil.formatOutputWide(rows, 3, regionName.substring(regionName.lastIndexOf(",") + 1));
        //RowID
        {
            RowKey rowKey = lock.getLock().getRowKey();
            if (rowKey != null) {
                String rowID = rowKey.toString();
                if (LockConstants.REMOVE_SPACE_FROM_ROWID) {
                    rowID = replaceBland(rowID);
                }
                StringUtil.formatOutputWide(rows, 4, rowID);
            } else {
                StringUtil.formatOutputWide(rows, 4, "");
            }
        }
        //lockType
        {
            String lockType = "";
            if (lock.isImplicitSavepoint()) {
                lockType += "implicit savepoint: ";
            } else if (lock.getSvptID() != -1) {
                lockType += "savepoint: ";
            }
            lockType += StringUtil.getLockMode(lock.getHolding());
            StringUtil.formatOutputWide(rows, 5, lockType);
        }
        //Holder TxIDs
        StringUtil.formatOutputWide(rows, 6, String.valueOf(lock.getHolderTxIDs()));
        //start time
        StringUtil.formatOutputWide(rows, 7, formatter.format(new Date(lock.getCreateTimestamp())));
        //durable time
        StringUtil.formatOutputWide(rows, 8, String.valueOf(lock.getDurableTime()));
        //lock status
        StringUtil.formatOutputWide(rows, 9, String.valueOf(LockUtils.getWaitLockStatus(lock.getWaitStatus())));
        StringBuffer tmp = new StringBuffer();
        for (StringBuffer row : rows) {
            tmp.append(row).append("\n");
            //result.append(row).append("\n");  will have extra headline
        }
        result.append(tmp);
        tmp = null;
    }

    public static String replaceBland(String rowID) {
        int end = rowID.length() - 1;
        for (; end >= 1; end -= 2) {
            if (rowID.charAt(end) != '0' || rowID.charAt(end - 1) != '2') {
                break;
            }
        }
        if (end < 0) {
            return "";
        }
        return rowID.substring(0, end + 1);
    }

    public static LZ4Compressor getCompressor() {
        return factory.fastCompressor();
    }

    public static LZ4SafeDecompressor getDecompressor() {
        return factory.safeDecompressor();
    }

    public static void generateRow(StringBuffer result, long txID, LMLockMsg lock) {
        if (lock.getMaskHold() == 0) {
            return;
        }
        List<StringBuffer> rows = new ArrayList<>();
        StringUtil.formatOutputWide(rows, 1, String.valueOf(txID));
        StringUtil.formatOutputWide(rows, 2, lock.getTableName());
        StringUtil.formatOutputWide(rows, 3, lock.getRegionName());
        String rowID = lock.getRowID();
        if (rowID != null && LockConstants.REMOVE_SPACE_FROM_ROWID) {
            rowID = StringUtil.replaceBland(rowID);
        }
        StringUtil.formatOutputWide(rows, 4, rowID);
        StringUtil.formatOutputWide(rows, 5, lock.getLockMode());
        for (StringBuffer row : rows) {
            result.append(row).append("\n");
        }
    }

    public static void generateRow(StringBuffer result, LMLockWaitMsg lockWaitMsg) {
        LMLockMsg lmLockMsg = lockWaitMsg.getToLock();
        lmLockMsg.setSvptID(lockWaitMsg.getSvptID());
        lmLockMsg.setParentSvptID(lockWaitMsg.getParentSvptID());
        lmLockMsg.setImplicitSavepoint(lockWaitMsg.isImplicitSavepoint());
        List<StringBuffer> rows = new ArrayList<>();
        StringUtil.formatOutputWide(rows, 1, String.valueOf(lockWaitMsg.getTxID()));
        StringUtil.formatOutputWide(rows, 2, lmLockMsg.getTableName());
        StringUtil.formatOutputWide(rows, 3, lmLockMsg.getRegionName());
        String rowID = lmLockMsg.getRowID();
        if (rowID != null && LockConstants.REMOVE_SPACE_FROM_ROWID) {
            rowID = StringUtil.replaceBland(rowID);
        }
        StringUtil.formatOutputWide(rows, 4, rowID);
        StringUtil.formatOutputWide(rows, 5, lmLockMsg.getLockMode());
        StringUtil.formatOutputWide(rows, 6, String.valueOf(lockWaitMsg.getHolderTxIDs()));
        String queryContext = lockWaitMsg.getQueryContext();
        StringUtil.formatOutputWide(rows, 7, queryContext == null ? "" : queryContext);
        StringUtil.formatOutputWide(rows, 8, String.valueOf(lockWaitMsg.getToLock().getDurableTime()));
        for (StringBuffer row : rows) {
            result.append(row).append("\n");
        }
    }

    public static int generateNonWaitRow(StringBuffer result, Map<Long, List<LMLockWaitMsg>> lockWaitMsgMap, LMLockWaitMsg lockWaitMsg, int count) {
        LMLockMsg waitHoldMsg = lockWaitMsg.getToLock();
        String tableName = waitHoldMsg.getTableName();
        String regionName = waitHoldMsg.getRegionName();
        String rowid = waitHoldMsg.getRowID();
        List<StringBuffer> allRows = new ArrayList<>();
        for (Long holderTxID : lockWaitMsg.getHolderTxIDs()) {
            if (lockWaitMsgMap.containsKey(holderTxID)) {
                continue;
            }
            List<StringBuffer> rows = new ArrayList<>();
            StringUtil.formatOutputWide(rows, 1, String.valueOf(holderTxID));
            StringUtil.formatOutputWide(rows, 2, tableName);
            StringUtil.formatOutputWide(rows, 3, regionName);
            StringUtil.formatOutputWide(rows, 4, rowid);
            StringUtil.formatOutputWide(rows, 5, "");
            StringUtil.formatOutputWide(rows, 6, "[-1]");
            StringUtil.formatOutputWide(rows, 7, "");
            allRows.addAll(rows);
        }
        if (allRows.size() > 0) {
            for (StringBuffer row : allRows) {
                result.append(row).append("\n");
            }
            count--;
        }
        return count;
    }

    public static byte[] hexToByteArray(String inHex) {
        int hexlen = inHex.length();
        byte[] result;
        if (hexlen % 2 == 1) {
            hexlen++;
            result = new byte[(hexlen / 2)];
            inHex = "0" + inHex;
        } else {
            result = new byte[(hexlen / 2)];
        }
        int j = 0;
        for (int i = 0; i < hexlen; i += 2) {
            result[j] = hexToByte(inHex.substring(i, i + 2));
            j++;
        }
        return result;
    }


    public static byte hexToByte(String inHex){
        return (byte)Integer.parseInt(inHex,16);
    }

    public static Properties readConfigFile(String configfile) throws Exception {
        InputStream in = null;
        File configFile = null;
        try {
            if (configfile.equals("default")) {
                String trafConfDir = System.getenv("TRAF_CONF");
                if (trafConfDir == null || trafConfDir.equals("")) {
                    throw new Exception("please provide lock config file or lock config parameter");
                }
                configFile = new File(trafConfDir + File.separator + "trafodion.lock.config");
            } else {
                configFile = new File(configfile);
            }
            if (!configFile.exists()) {
                throw new Exception("lock parameter configfile do not exists: " + configfile);
            }
            in = new BufferedInputStream(new FileInputStream(configFile));
            Properties properties = new Properties();
            properties.load(in);
            return properties;
        } catch (Exception e) {
            throw new  Exception("value of -configfile or value is illegal: "
                    + (configFile == null ? "" : configFile.getAbsolutePath()));
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        }
    }
}
