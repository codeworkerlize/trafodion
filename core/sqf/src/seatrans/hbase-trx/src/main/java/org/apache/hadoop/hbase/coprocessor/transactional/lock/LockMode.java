package org.apache.hadoop.hbase.coprocessor.transactional.lock;


public class LockMode {
    public final static int LOCK_IS = 1;   // Shared intent lock
    public final static int LOCK_S = 2;    // Shared lock
    public final static int LOCK_IX = 4;   // Exclusive intent lock
    public final static int LOCK_U = 8;    // Update lock
    public final static int LOCK_X = 16;   // Exclusive lock
    public final static int LOCK_RS = 32;
    public final static int LOCK_RX = 64;
    public final static int LOCK_NO = 255; // no lock

    private final static int[] LOCKMODE_INDEX = new int[65];

    static {
        LOCKMODE_INDEX[1] = 0;
        LOCKMODE_INDEX[2] = 1;
        LOCKMODE_INDEX[4] = 2;
        LOCKMODE_INDEX[8] = 3;
        LOCKMODE_INDEX[16] = 4;
        LOCKMODE_INDEX[32] = 5;
        LOCKMODE_INDEX[64] = 6;
    }

    public static int getIndex(int mode) {
        return LOCKMODE_INDEX[mode];
    }
}
