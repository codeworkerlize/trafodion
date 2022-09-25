package org.trafodion.jdbc.t4.h2Cache;

import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.T4LoggingUtilities;
import org.trafodion.jdbc.t4.T4Properties;
import org.trafodion.jdbc.t4.TrafT4Connection;

public class TrafCachePool {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TrafCachePool.class);
    private static volatile TrafCachePool trafCachePool_;
    public static ReentrantLock lock = new ReentrantLock();
    private static LinkedList<TrafCache> trafCacheList;

    static {
        trafCacheList = new LinkedList<TrafCache>(); ;
        initMap();
    }

    private TrafCachePool() {
    }

    public static TrafCachePool getTrafCachePool() {
        if (trafCachePool_ == null) {
            synchronized (TrafCachePool.class) {
                if (trafCachePool_ == null) {
                    trafCachePool_ = new TrafCachePool();
                }
            }
        }
        return trafCachePool_;
    }

    private static void initMap() {
        trafCacheList.clear();
    }

    public void initH2DataBase(TrafT4Connection t4Connection, T4Properties t4props)
            throws Exception {
        if (t4props.isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(t4props, Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}.", "start init H2 DataBase...");
        }
        TrafCache trafCache;
        lock.lock();
        try {
            if(trafCacheList.size() == 0 ){
                trafCache = new TrafCache(t4props);
                trafCacheList.add(trafCache);
            } else {
                trafCache = trafCacheList.getLast();
                if(trafCache.getTrafT4ConnectionVector().size() >= 10){
                    trafCache =  new TrafCache(t4props);
                    trafCacheList.add(trafCache);
                }
            }
            trafCache.getTrafT4ConnectionVector().add(t4Connection);

        } finally {
            lock.unlock();
        }
        trafCache.cacheTableName(t4props);
        trafCache.cacheTablesInit(t4Connection, t4props);
        t4Connection.setTrafCache_(trafCache);

    }

    public synchronized void removeConnection(TrafT4Connection t4Connection) {
        TrafCache trafCache = t4Connection.getTrafCache_();
        if (trafCacheList.size() != 0) {
            for (int i = 0; i < trafCacheList.size(); i++) {
                if (trafCacheList.get(i) == trafCache) {
                    trafCacheList.get(i).getTrafT4ConnectionVector().remove(t4Connection);
                }
                if(trafCacheList.get(i).getTrafT4ConnectionVector().size()==0){
                    trafCacheList.remove(i);
                }
            }
        }
    }
}
