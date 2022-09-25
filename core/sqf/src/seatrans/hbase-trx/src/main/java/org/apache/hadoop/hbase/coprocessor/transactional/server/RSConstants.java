package org.apache.hadoop.hbase.coprocessor.transactional.server;

public class RSConstants {
    public static int RS_LISTENER_PORT = 8888;
    public static int MEMORY_ALLOC_AMPLIFICATION_FACTOR = 6;
    //kill 'top one' transaction when regionServer's memory usage exceeds this threshold
    public static int REGION_MEMORY_HIGHLOAD_THRESHOLD = 90;
    public static int REGION_MEMORY_WARNING_THRESHOLD = 0;
    //check region memory intervals
    public static int CHECK_REGION_MEMORY_INTERVALS = 100;
    public static int PRINT_TRANSACTION_LOG = 0;
    public static boolean ONLINE_BALANCE_ENABLED = false;
    public static int DISABLE_NEWOBJECT_FOR_ENDPOINT = 1;
    public static int DISABLE_NEWOBJECT_FOR_MC = 1;
    public static int RECORD_TIME_COST_COPRO = -1;
    public static int RECORD_SCAN_ROW_THRESHOLD = -1;
    public static int RECORD_TIME_COST_COPRO_ALL = -1;
    public static int MAX_BLOCK_CHECK_RETRY_TIMES = 200;
}
