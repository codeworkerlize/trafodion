package com.esgyn.rs;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.message.RSMessage;

import java.io.*;
import java.util.*;

public class RSciParameterCheck {
    public static final List<String> LOCK_PARAMETERS = Arrays.asList("ENABLE_LOCK_FOR_SELECT", "LOCK_LOG_LEVEL", "LOCK_SKIP_META_TABLE",
            "LOCK_CACHE_SIZE", "LOCK_ESCALATION_THRESHOLD", "LOCK_TRYLOCK_TIMES", "REST_SERVER_URI",
            "XDC_MASTER_SLAVE", "TRANSACTION_CLEAN_INTERVAL", "DISTRIBUTED_DEADLOCK_DETECT_INTERVAL",
            "REST_CONN_FAILED_TIMES", "REST_CONNECT_TIMEOUT", "REST_READ_TIMEOUT",
            "REST_CONNECT_DETECT_INTERVAL", "ENABLE_LOCK_STATISTICS", "LOCK_WAIT_INFO_WAITTIME_THRESHOLD",
            "RECORD_TRANSACTION_LOCKNUM_THRESHOLD", "ENABLE_SAVEPOINT", "ENABLE_LONG_WAIT_LOCK_PRINTING",
            "NUMBER_OF_OUTPUT_LOGS_PER_TIME", "MAX_LOCK_NUMBER", "MAX_LOCK_NUMBER_PER_TRANS", "TRACE_LOCK_APPLIED",
            "ENABLE_TABLELOCK_FOR_FULL_SCAN", "LOCK_CLIENT_RETRIES_TIMES", "LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT",
            "ENABLE_TRACE_WAIT_LOCK_INFO", "LOCK_TIME_OUT");
    public static final List<String> RS_PARAMETERS = Arrays.asList("RECORD_SCAN_ROW_THRESHOLD", "MAX_BLOCK_CHECK_RETRY_TIMES",
            "ENABLE_ONLINE_BALANCE", "REGION_MEMORY_HIGHLOAD_THRESHOLD", "CHECK_REGION_MEMORY_INTERVALS",
            "REGION_MEMORY_WARNING_THRESHOLD", "MEMORY_ALLOC_AMPLIFICATION_FACTOR",
            "PRINT_TRANSACTION_LOG", "DISABLE_NEWOBJECT_FOR_ENDPOINT", "DISABLE_NEWOBJECT_FOR_MC",
            "RECORD_TIME_COST_COPRO", "RECORD_SCAN_ROW_THRESHOLD", "MAX_BLOCK_CHECK_RETRY_TIMES",
            "RECORD_TIME_COST_COPRO_ALL");

    public static boolean TableNamesCheck(List<String> tableNames, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_WAIT,
                RSMessage.LOCK_CLEAN, RSMessage.DEAD_LOCK, RSMessage.LOCK_STATISTICS, RSMessage.LOCK_INFO,
				RSMessage.GET_RS_PARAMETER);
        if (tableNames.size() != 0 && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-table is not match " + type;
            return true;
        }
        return false;
    }

    public static boolean TxIDsCheck(List<Long> txIDs, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_WAIT,
                RSMessage.LOCK_CLEAN, RSMessage.DEAD_LOCK, RSMessage.LOCK_STATISTICS, RSMessage.LOCK_INFO);
        if (txIDs.size() != 0 && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-tx is not match " + type;
            return true;
        }
        return false;
    }

    public static boolean TxIDsValueCheck(List<Long> txIDs, String type) {
        if (txIDs.size() > 0) {
            for (long txID : txIDs) {
                if (txID < 0) {
                    RSciCommand.parameterCheckErrorMessage = "-tx should be positive " + txIDs;
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean HolderTxIDsCheck(List<Long> holderTxIDs, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_WAIT);
        if (holderTxIDs.size() != 0 && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-htx is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean DisplayImplictitSavepointCheck(boolean displayimplictitsavepoint, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_INFO);
        if (displayimplictitsavepoint && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-displayimplictit or -di is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean DisplaySavepointIdCheck(boolean displaysavepointid, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_INFO);
        if (displaysavepointid && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-displaysavepointid or -dsi is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean TopCheck(int topN, String type) {
        if (topN != 0 && !RSMessage.LOCK_STATISTICS.equals(type)) {
            RSciCommand.parameterCheckErrorMessage = "-top is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean OutputCheck(String output, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_WAIT,
                RSMessage.LOCK_STATISTICS, RSMessage.LOCK_INFO);
        if (output != "TEXT" && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-output or -o is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean DebugCheck(int debug, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK",
            RSMessage.LOCK_INFO, "LOCK_APPLY");
        if (debug != 0 && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-debug or -d is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean ColWidthsCheck(Map<String, String> colWidths, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_WAIT,
                RSMessage.LOCK_STATISTICS, RSMessage.LOCK_INFO, RSMessage.LOCK_MEM);
        if (colWidths.size() != 0 && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-colwidth or -col is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean RemovespaceCheck(boolean removespace, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_WAIT,
                RSMessage.DEAD_LOCK, RSMessage.LOCK_INFO);
        if (removespace && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-removespace or -r is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean ConfigFileCheck(String configfile, Map<String, String> configparas, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.RS_PARAMETER, RSMessage.LOCK_PARAMETER);
        if (!configfile.equals("") && configparas.size() == 0) {
            if (!supportTypes.contains(type)) {
                RSciCommand.parameterCheckErrorMessage = "-configfile or -cf is not match " + type;
                return true;
            } else {
                try {
                    Properties properties = StringUtil.readConfigFile(configfile);
                    Set<String> configParaSet = properties.stringPropertyNames();
                    List<String> parames = null;
                    if (type.equals(RSMessage.LOCK_PARAMETER)) {
                        parames = RSciParameterCheck.LOCK_PARAMETERS;
                    } else {
                        parames = RSciParameterCheck.RS_PARAMETERS;
                    }
                    for (String configPara : configParaSet) {
                        if (!parames.contains(configPara)) {
                            RSciCommand.parameterCheckErrorMessage = "config parameter: " + configPara + " in configfile: " + configfile + " is not match " + type;
                            return true;
                        }
                    }
                } catch (Exception e) {
                    RSciCommand.parameterCheckErrorMessage = e.getMessage();
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean ConfigParasCheck(String configfile, Map<String, String> configparas, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.RS_PARAMETER, RSMessage.LOCK_PARAMETER);
        if (configfile.equals("") && configparas.size() != 0 && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-confpara or -cp is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean MemUnitParasCheck(String memUnit, String type) {
        if (!memUnit.isEmpty() && !RSMessage.LOCK_MEM.equals(type)) {
            RSciCommand.parameterCheckErrorMessage = "-memoryunit or -mu is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean ShowNonWaitCheck(boolean shownonwaiting, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_WAIT);
        if (shownonwaiting && !supportTypes.contains(type))  {
            RSciCommand.parameterCheckErrorMessage = "-shownonwaiting is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean ForceFlagCheck(boolean forceFlag, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_CLEAN, RSMessage.LOCK_PARAMETER,
                RSMessage.RS_PARAMETER);
        if (forceFlag && !(supportTypes.contains(type)))  {
            RSciCommand.parameterCheckErrorMessage = "-force is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean ShowCleanCommandCheck(boolean showcleanCommand, String type) {
        if (showcleanCommand  && !RSMessage.LOCK_CHECK.equals(type)) {
            RSciCommand.parameterCheckErrorMessage = "-showcleanCommand is not match " + type;
            return true;
        } else {
            return false;
        }
    }

    public static boolean MaxCheck(int maxRetLockInfoLimit, String type) {
        List<String> supportTypes = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_INFO);
        if (maxRetLockInfoLimit >= 0 && !supportTypes.contains(type)) {
            RSciCommand.parameterCheckErrorMessage = "-maxlockinfo is not match " + type;
            return true;
        } else {
            return false;
        }
    }
}
