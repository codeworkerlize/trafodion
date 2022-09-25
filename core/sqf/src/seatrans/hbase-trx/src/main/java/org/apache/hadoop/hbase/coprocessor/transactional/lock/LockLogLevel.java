package org.apache.hadoop.hbase.coprocessor.transactional.lock;

import org.apache.log4j.Logger;

public class LockLogLevel {
    private  static final Logger LOG = Logger.getLogger(LockLogLevel.class);
    public static final int NOLOG = 0;
    public static final int INFO = 1;
    public static final int DEBUG = 2;
    public static final int TRACE = 4;
    public static final int GEN_TESTCASE = 8;
    public static final int LW_TEST = 16;
    public static final int DEBUG_LEVEL1 = 32;

    public static boolean enableInfoLevel = false;
    public static boolean enableDebugLevel = false;
    public static boolean enableTraceLevel = false;
    public static boolean enableGenTestCaseLevel = false;
    public static boolean enableLockInfoOutputerLevel = false;
    public static boolean enableDebugLevel1 = false;

    public static void init() {
        if (LockConstants.LOCK_LOG_LEVEL == NOLOG) {
            enableInfoLevel = false;
            enableDebugLevel = false;
            enableTraceLevel = false;
            enableGenTestCaseLevel = false;
            enableLockInfoOutputerLevel = false;
            enableDebugLevel1 = false;
        } else {
            enableInfoLevel = ((LockConstants.LOCK_LOG_LEVEL & INFO) == INFO);
            enableDebugLevel = ((LockConstants.LOCK_LOG_LEVEL & DEBUG) == DEBUG);
            enableTraceLevel = ((LockConstants.LOCK_LOG_LEVEL & TRACE) == TRACE);
            enableGenTestCaseLevel = ((LockConstants.LOCK_LOG_LEVEL & GEN_TESTCASE) == GEN_TESTCASE);
            enableLockInfoOutputerLevel = ((LockConstants.LOCK_LOG_LEVEL & LW_TEST) == LW_TEST);
            enableDebugLevel1 = ((LockConstants.LOCK_LOG_LEVEL & DEBUG_LEVEL1) == DEBUG_LEVEL1);
        }
    }

    public static void parse(String envLockLogLevel) {
        try {
            LockConstants.LOCK_LOG_LEVEL = Integer.parseInt(envLockLogLevel);
            init();
            return;
        } catch (Exception e) {
            LOG.warn("LOCK_LOG_LEVEL is not integer, will parse as string");
        }
        if (envLockLogLevel.equals("info")) {
            LockConstants.LOCK_LOG_LEVEL = INFO;
        } else if (envLockLogLevel.equals("debug")) {
            LockConstants.LOCK_LOG_LEVEL = (INFO | DEBUG);
        } else if (envLockLogLevel.equals("trace")) {
            LockConstants.LOCK_LOG_LEVEL = (INFO | DEBUG | TRACE);
        } else if (envLockLogLevel.equals("gen_testcase")) {
            LockConstants.LOCK_LOG_LEVEL = GEN_TESTCASE;
        } else if (envLockLogLevel.equals("all")) {
            LockConstants.LOCK_LOG_LEVEL = (INFO | DEBUG | TRACE | GEN_TESTCASE);
        } else if (envLockLogLevel.equals("lk_test")) {
            LockConstants.LOCK_LOG_LEVEL = LW_TEST;
        } else if (envLockLogLevel.equals("debug1")) {
            LockConstants.LOCK_LOG_LEVEL = DEBUG_LEVEL1;
        } else {
            LockConstants.LOCK_LOG_LEVEL = NOLOG;
        }
        init();
    }
} 
