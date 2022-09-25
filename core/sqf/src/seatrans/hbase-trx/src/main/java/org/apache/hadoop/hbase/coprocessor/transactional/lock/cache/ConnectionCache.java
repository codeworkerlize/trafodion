package org.apache.hadoop.hbase.coprocessor.transactional.lock.cache;

import org.apache.hadoop.hbase.coprocessor.transactional.server.RSConnection;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSConstants;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionCache implements LockSizeof {
    private static Logger LOG = Logger.getLogger(ConnectionCache.class);

    // RSConnection cache, indexed by host IP string
    private ConcurrentHashMap<String, List<RSConnection>> idleConnectionCache = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<RSConnection>> workingConnectionCache = new ConcurrentHashMap<>();
    private ReentrantLock connectionCacheLock = new ReentrantLock();

    public RSConnection borrowConnection(String destServer) {
        RSConnection rsConnection = null;
        List<RSConnection> idleConnections = null;
        List<RSConnection> workingConnections = null;
        try {
            connectionCacheLock.lock();
            idleConnections = idleConnectionCache.get(destServer);
            if (idleConnections == null) {
                idleConnections = new ArrayList<>();
                idleConnectionCache.put(destServer, idleConnections);
            }
            workingConnections = workingConnectionCache.get(destServer);
            if (workingConnections == null) {
                workingConnections = new ArrayList<>();
                workingConnectionCache.put(destServer, workingConnections);
            }
            if (idleConnections.size() == 0) {
                rsConnection = new RSConnection(destServer, RSConstants.RS_LISTENER_PORT);
            } else {
                rsConnection = idleConnections.remove(0);
            }
            if (rsConnection.getCh() == null) {
                removeConnection(destServer, rsConnection);
                return null;
            }
            workingConnections.add(rsConnection);
        } catch (Exception e) {
            LOG.error("failed to get Connection to " + destServer + " idle:" + idleConnections.size() + " working:" + workingConnections.size(), e);
        } finally {
            connectionCacheLock.unlock();
        }
        return rsConnection;
    }

    public void releseConnection(String destServer, RSConnection rsConnection) {
        if (rsConnection == null) {
            return;
        }
        try {
            connectionCacheLock.lock();
            List<RSConnection> rsConnections = workingConnectionCache.get(destServer);
            if (rsConnections == null) {
                rsConnections = new ArrayList<>();
                workingConnectionCache.put(destServer, rsConnections);
            }
            rsConnections.remove(rsConnection);

            rsConnections = idleConnectionCache.get(destServer);
            if (rsConnections == null) {
                rsConnections = new ArrayList<>();
                idleConnectionCache.put(destServer, rsConnections);
            }
            rsConnections.add(rsConnection);
        } catch (Exception e) {
            LOG.error("failed to releseConnection ", e);
        } finally {
            connectionCacheLock.unlock();
        }
    }

    public int size() {
        int size = 0;
        for (List<RSConnection> values : idleConnectionCache.values()) {
            size += values.size();
        }
        for (List<RSConnection> values : workingConnectionCache.values()) {
            size += values.size();
        }
        return size;
    }

    public int idleSize() {
        int size = 0;
        for (List<RSConnection> values : idleConnectionCache.values()) {
            size += values.size();
        }
        return size;
    }

    public int workingSize() {
        int size = 0;
        for (List<RSConnection> values : workingConnectionCache.values()) {
            size += values.size();
        }
        return size;
    }

    public String toString() {
        StringBuffer message = new StringBuffer(300);
        message.append("connectionCache size:").append(size()).append("\n");
        int idleSize = idleSize();
        int workingSize = workingSize();
        message.append("connectionCache idle size:").append(idleSize).append("\n");
        message.append("connectionCache working size:").append(workingSize).append("\n");
        if (workingSize > 0) {
            message.append("\nworkingConnectionCache:").append("\n");
            for (Map.Entry<String, List<RSConnection>> entry : workingConnectionCache.entrySet()) {
                message.append("  regionServer:").append(entry.getKey()).append(" size:").append(entry.getValue().size()).append("\n");
                int i = 1;
                for (RSConnection rsConnection : entry.getValue()) {
                    message.append("    connection:").append(i).append(" ").append(rsConnection).append("\n");
                    i++;
                }
            }
        }
        if (idleSize > 0) {
            message.append("\nidleConnectionCache:").append("\n");
            for (Map.Entry<String, List<RSConnection>> entry : idleConnectionCache.entrySet()) {
                message.append("  regionServer:").append(entry.getKey()).append(" size:").append(entry.getValue().size()).append("\n");
                int i = 1;
                for (RSConnection rsConnection : entry.getValue()) {
                    message.append("    connection:").append(i).append(" ").append(rsConnection).append("\n");
                    i++;
                }
            }
        }
        return message.toString();
    }

    public void close() {
        try {
            connectionCacheLock.lock();
            for (List<RSConnection> connections : idleConnectionCache.values()) {
                if (connections == null || connections.size() == 0) {
                    continue;
                }
                for (RSConnection connection : connections) {
                    connection.close();
                }
            }
            for (List<RSConnection> connections : workingConnectionCache.values()) {
                if (connections == null || connections.size() == 0) {
                    continue;
                }
                for (RSConnection connection : connections) {
                    connection.close();
                }
            }
        } catch (Exception e) {
            LOG.error("failed to close", e);
        } finally {
            connectionCacheLock.unlock();
        }
    }

    public void removeConnection(String destServer, RSConnection rsConnection) {
        if (rsConnection == null) {
            return;
        }
        try {
            connectionCacheLock.lock();
            List<RSConnection> rsConnections = workingConnectionCache.get(destServer);
            if (rsConnections != null) {
                rsConnections.remove(rsConnection);
            }
            rsConnection.close();
        } catch (Exception e) {
            LOG.error("failed to removeConnection ", e);
        } finally {
            connectionCacheLock.unlock();
        }
    }

    @Override
    public long sizeof() {
        long size = Size_Object;
        //4 attributes
        size += 4 * Size_Reference;
        //2 Map
        size += 2 * Size_ArrayList;
        //1 Object
        size += 16;//Size_Object + (Size_Object % 8)
        try {
            connectionCacheLock.lock();
            //idleConnectionCache
            for (Map.Entry<String, List<RSConnection>> entry : idleConnectionCache.entrySet()) {
                //key is reference
                size += Size_Reference;
                List<RSConnection> conns = entry.getValue();
                size += Size_ArrayList;
                if (!conns.isEmpty()) {
                    size += ((conns.get(0).sizeof() + Size_Reference) * conns.size());
                }
                size += Size_Pair;
            }
            //workingConnectionCache
            for (Map.Entry<String, List<RSConnection>> entry : workingConnectionCache.entrySet()) {
                //key is reference
                size += Size_Reference;
                List<RSConnection> conns = entry.getValue();
                size += Size_ArrayList;
                if (!conns.isEmpty()) {
                    size += ((conns.get(0).sizeof() + Size_Reference) * conns.size());
                }
                size += Size_Pair;
            }
        } catch (Exception e) {
            LOG.error("failed in Sizeof", e);
        } finally {
            connectionCacheLock.unlock();
        }
        return LockUtils.alignLockObjectSize(size);
    }
}
