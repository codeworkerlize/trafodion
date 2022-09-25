package org.apache.hadoop.hbase.coprocessor.transactional.lock;

import java.util.Map;
import java.util.HashMap;

public class LockConstants {
    public static boolean ENABLE_ROW_LEVEL_LOCK = false;
    public static boolean HAS_VALID_LICENSE = false;
    /**
     * parameter：max.lock.cache.size
     */
    public static final int DEFALUT_LOCK_CACHE_SIZE = 32 * 1024;
    public static int TRANS_CACHE_SIZE = 2048;
    public static final int DEFALUT_LOCK_HOLDER_SIZE = 6000000;//600w
    public static int LOCK_CACHE_SIZE = DEFALUT_LOCK_CACHE_SIZE;
    public static int MAX_LOCK_NUMBER = DEFALUT_LOCK_HOLDER_SIZE;
    public static int MAX_LOCK_NUMBER_PER_TRANS = 5000000;//500w
    public static boolean TRACE_LOCK_APPLIED = false; // default is off
    /**
     * parameter：max.lock.cache.size
     */
    public static int LOCK_ESCALATION_THRESHOLD = 0; //default disable lock escalation
    /**
     * parameter: try lock times
     */
    public static int LOCK_TRYLOCK_TIMES = 4000;
    public final static int LOCK_MAX_MODES = 7;
    public final static int LOCK_MAX_TABLE = (1 << LOCK_MAX_MODES);
    public static int LOCK_MESSAGE_COMPRESS_THRESHOLD = 50 * 1024 * 1024; //50M
    public final static String LOCKMANAGER_KEY = "LOCKMANAGER_KEY";
    public static int LM_TRYCONNECT_TIMES = 3;
    public final static boolean LM_TCPNODELAY = true;
    public final static boolean LM_TCPKEEPLIVE = true;
    // REST_SERVER_URI's format: http[s]://ip1[,ip2...]:port[;http[s]://ip1[,ip2...]:port]
    public static String REST_SERVER_URI = null;
    // xdc master slave's format: master_clusterid;slave_clusterid
    public static String XDC_MASTER_SLAVE = null;
    public static final String REST_TRANSACTION_PATH = "/v1/transactions/list";
    public static final String REST_TRANSACTION_STATUS = "/v1/transactions/transid";
    public static final String REST_TRANSACTION_PARAMETER = "regionservers";
    public static final String LOCKWAIT_LOGFILENAME = "trafodion.lockwait";
    public static long TRANSACTION_CLEAN_INTERVAL = 10000;
    public static int REST_CONN_FAILED_TIMES = 10;
    public static long REST_CONNECT_DETECT_INTERVAL = 60000;
    public static int REST_CONNECT_TIMEOUT = 30000;
    public static int REST_READ_TIMEOUT = 30000;
    public static final int MAX_PRELOAD_APPLIED_LOG = 4096;
    
    public static final String[] LOCK_HEADER = {"txID", "table name", "region name", "rowid", "lock mode"};
    public static final String[] LOCK_WAIT_HEADER = {"txID", "table name", "region name", "rowid", "lock mode", "holder txID", "query context", "duration(ms)"};
    public static final String[] LOCK_WAIT_HEADER_FOR_LOG = {"txID", "table name", "region name", "rowid", "lock mode", "holder txID", "started at","duration(ms)", "final status"};
    public static final String[] LOCK_STATISTICS_HEADER = {"txID", "lock number", "query context"};
    public static final String[] LOCK_REGION_STATISTICS_HEADER = {"txID", "table name", "region name", "lock number"};
    public static final String[] LOCK_ENABLE_ROW_LOCK_HEADER = { "server name", "enable row lock" };
    public static final String[] LOCK_APPLIED_LOCK_HEADER = { "txID", "holder number" };
    public static final String[] LOCK_APPLIED_LOCK_HEADER_DEBUG = { "txID", "table name", "region name", "rowid", "lock mode" };
    public static final String[] LOCK_REGION_MEMORY_INFO_HEADER = { "server name", "total JVM memory", "inuse memory", "%", "Lock using memory", "lock number", "transaction number", "lockHolder number" };
    public static boolean REMOVE_SPACE_FROM_ROWID = false;
    public static Map<Integer, Integer> COLUMN_WIDTH_MAP = new HashMap<Integer, Integer>();
    public static int DISTRIBUTED_DEADLOCK_DETECT_INTERVAL = 6000;
    public static int LOCK_LOG_LEVEL = LockLogLevel.NOLOG;
    public static boolean ENABLE_LOCK_STATISTICS = false;
    public static int LOCK_WAIT_TIMEOUT = 3000;
    public static int LOCK_WAIT_INFO_FETCH_LOCK_INFO_TIMEOUT = LOCK_WAIT_TIMEOUT * 2 + 100;
    public static boolean ENABLE_TRACE_WAIT_LOCK_INFO = true;
    public static int LOCK_WAIT_INFO_WAITTIME_THRESHOLD = 10 * 1000;//10s
    public static int LOCK_WAIT_INFO_PRESAMPLING_THRESHOLD = LOCK_WAIT_INFO_WAITTIME_THRESHOLD - LOCK_WAIT_TIMEOUT;
    public static boolean ENABLE_LONG_WAIT_LOCK_PRINTING = false;
    public static boolean ENABLE_SAVEPOINT = true;
    public static boolean DISPLAY_IMPLICIT_SAVEPOINT = false;
    public static boolean DISPLAY_SAVEPOINT_ID = false;
    public static final char[] HEX_CHAR = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    public static boolean ENABLE_LOCK_FOR_SELECT = true;
    public static int LOCK_CLIENT_RETRIES_TIMES = 20;
    /**
    * false: only show waiting
    * true: show waiting and non waiting, and holdertxid of not waiting' is -1
    */
    public static boolean SHOW_NON_WAITING = false;
    public static boolean ENABLE_TABLELOCK_FOR_FULL_SCAN = false;
    public static final int LOCK_INFO_RESPONSE_SIZE_LIMIT = 10000;

    public static final String ZK_NODE_LOCK_PATH = "/trafodion/rowlock";
    public static final int READ_UNCOMMITTED_ACCESS_   = 0;
    public static final int READ_COMMITTED_ACCESS_     = 10;
    public static final int REPEATABLE_READ_ACCESS_    = 20;
    public static final int SERIALIZABLE_ACCESS_       = 30;
    public static final int ACCESS_TYPE_NOT_SPECIFIED_ = -1;

    static {
       COLUMN_WIDTH_MAP.put(2, 37);
       COLUMN_WIDTH_MAP.put(3, 25);
       COLUMN_WIDTH_MAP.put(4, 37);
       COLUMN_WIDTH_MAP.put(5, 18);
       COLUMN_WIDTH_MAP.put(6, 19);
       COLUMN_WIDTH_MAP.put(7, 13);
    }
    public static boolean LOCK_ENABLE_DEADLOCK_DETECT = true;
    public static boolean LOCK_SKIP_META_TABLE = true;
    public static int RECORD_TRANSACTION_LOCKNUM_THRESHOLD = 1000;
    public static int NUMBER_OF_OUTPUT_LOGS_PER_TIME = 1000;

    /*!
     * conflict matrix
     *      X   U	IX  S   IS  RS  RX
     *   IS 1	0	0   0   0   0   0
     *   S  1   0	1   0   0   0   0
     *   IX 1	1	0   1   0   0   0
     *	 U  1	1	1   0   0   0   0
     *   X  1   1	1   1   1   0   0
     *   RS 0   0	0   0   0   0   1
     *   RX 0   0	0   0   0   1   1
     */
    public static int defaultLockConflictsMatrix[] = {
            (LockMode.LOCK_X),	/* ISLOCK */
            (LockMode.LOCK_IX) | (LockMode.LOCK_X),	 /* SLOCK */
            (LockMode.LOCK_S) | (LockMode.LOCK_U) | (LockMode.LOCK_X),  /* IXLOCK */
            (LockMode.LOCK_IX) | (LockMode.LOCK_U) | (LockMode.LOCK_X),  /* ULOCK */
            (LockMode.LOCK_IS) | (LockMode.LOCK_S) | (LockMode.LOCK_IX) | (LockMode.LOCK_U) | (LockMode.LOCK_X), /* XLOCK */
            (LockMode.LOCK_RX), /* RSLOCK */
            (LockMode.LOCK_RS | LockMode.LOCK_RX) /* RXLOCK */
    };
    /*!
     * Containment matrix
     * The lock containment matrix is calculated through the conflict matrix
     *		X	U	IX	S	IS
     *	IS	0	0	0	0	1
     *	S	0	0	0	1	1
     *	IX	0	0	1	0	1
     *	U	0	1	0	1	1
     *	X	1	1	1	1	1
     */
    public static int defaultLockContainMatrix[] = {
            (LockMode.LOCK_IS),		/* ISLOCK */
            (LockMode.LOCK_IS) | (LockMode.LOCK_S),		/* SLOCK */
            (LockMode.LOCK_IS) | (LockMode.LOCK_IX),	/* IXLOCK */
            (LockMode.LOCK_IS) | (LockMode.LOCK_S) | (LockMode.LOCK_U), /* ULOCK */
            (LockMode.LOCK_IS) | (LockMode.LOCK_S) | (LockMode.LOCK_IX) | (LockMode.LOCK_U) | (LockMode.LOCK_X)	/* XLOCK */
    };
}
