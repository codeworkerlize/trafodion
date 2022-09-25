package org.apache.hadoop.hbase.coprocessor.transactional.lock.utils;

public interface LockSizeof {
    public static final int Size_Object = 12;
    public static final int Size_Reference = 4;//-XX:+UseCompressedOops
    public static final int Size_Array = 16;
    public static final int Size_Pair = 2 * Size_Reference;
    public static final int Size_Vector = 32;
    public static final int Size_ArrayList = 24;
    public static final int Size_CopyOnWriteArraySet = 16;
    public static final int Size_HashSet = 16;
    public static final int Size_ConcurrentHashMap = 64;
    public static final int Size_LinkedBlockingQueue = 48;
    public static final int Size_AtomicInteger = 16;
    public static final int Size_Long = 24;
    public static final int Size_String = 24;
    public static final int Size_Char = 2;
    public static final int Size_Integer = 16;
    public static final int Size_Boolean = 16;
    public static final int Size_HashMap = 48;

    public long sizeof();
}