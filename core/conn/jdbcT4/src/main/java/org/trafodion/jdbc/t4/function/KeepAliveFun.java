package org.trafodion.jdbc.t4.function;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.KeepAliveCheckReply;
import org.trafodion.jdbc.t4.T4LoggingUtilities;
import org.trafodion.jdbc.t4.T4Properties;
import org.trafodion.jdbc.t4.TrafT4Connection;

public class KeepAliveFun {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(KeepAliveFun.class);

    private static ConcurrentLinkedQueue<TrafT4Connection> connLinkQueue;
    private static HashMap<String, List<String>> ipWithDialogueIdMap;
    private static HashMap<Integer, TrafT4Connection> dialogueIdWithConnMap;
    private static HashMap<String, List<TrafT4Connection>> ipWithConnsMap;
    private static boolean taskFlag;
    static Lock lock = new ReentrantLock();

    public static void initKeepAliveFun() {
        connLinkQueue = new ConcurrentLinkedQueue<TrafT4Connection>();
        ipWithDialogueIdMap = new HashMap<String, List<String>>();
        dialogueIdWithConnMap = new HashMap<Integer, TrafT4Connection>();
        ipWithConnsMap = new HashMap<String, List<TrafT4Connection>>();
        initMap();
    }

    public static void putQueue(TrafT4Connection conn , T4Properties t4props) {
        try {
            long keepAliveTime = t4props.getKeepAliveTime();
            if (t4props.isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(t4props, Level.FINER, "ENTRY. KeepAlive Time :", keepAliveTime);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("ENTRY. conn put queue enter, KeepAlive Time : <{}>",keepAliveTime);
            }
            // continue if dcs version < 4
            if (conn.getDcsVersion() < 4) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("conn.getDcsVersion = {}", conn.getDcsVersion());
                }
                return;
            }
            connLinkQueue.offer(conn);
            if (taskFlag) {
                lock.lock();
                if (taskFlag) {
                    taskFlag = false;
                } else {
                    lock.unlock();
                    return;
                }
                lock.unlock();
                try {
                    final Timer timer = new Timer();
                    TimerTask task = new TimerTask() {
                        @Override
                        public void run() {
                            Thread.currentThread().setName("KeepAliveFunTimer");
                            doKeepAlive();
                            timer.cancel();
                        }
                    };
                    timer.schedule(task, keepAliveTime);
                } catch (Exception e) {
                    taskFlag = true;
                    LOG.warn("timer thread has already start : {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void doKeepAlive() {
        //step 1 : get dialogueId from conn
        if (connLinkQueue.isEmpty()) {
            taskFlag = true;
            return;
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("do keepAlive enter");
        }
        int count = 0;
        try {
            while (!connLinkQueue.isEmpty() && count < 100) {
                TrafT4Connection conn = connLinkQueue.poll();
                if (conn == null || conn.isClosed()) {
                    continue;
                }
                count++;
                int tmpDialigueID = conn.getDialogueId();
                String tmpIP = conn.getActiveMasterIP();
                List<TrafT4Connection> list = new ArrayList<TrafT4Connection>();
                if (ipWithConnsMap.containsKey(tmpIP)) {
                    list = ipWithConnsMap.get(tmpIP);
                }
                if (!list.contains(conn)) {
                    list.add(conn);
                }
                ipWithConnsMap.put(tmpIP, list);
                if (!dialogueIdWithConnMap.containsKey(conn.getDialogueId())) {
                    dialogueIdWithConnMap.put(conn.getDialogueId(), conn);
                }
                List<String> tmpList = new ArrayList<String>();
                if (!ipWithDialogueIdMap.containsKey(tmpIP)) {
                    tmpList.add("" + tmpDialigueID);
                    ipWithDialogueIdMap.put(tmpIP, tmpList);
                } else {
                    tmpList = ipWithDialogueIdMap.get(tmpIP);
                    if (!tmpList.contains(String.valueOf(tmpDialigueID))) {
                        tmpList.add(String.valueOf(tmpDialigueID));
                    }
                    ipWithDialogueIdMap.put(tmpIP, tmpList);
                }
            }

            //step 2 : doIO
            for (Entry<String, List<String>> entry : ipWithDialogueIdMap.entrySet()) {
                String ip = entry.getKey();
                if (LOG.isInfoEnabled()) {
                    LOG.info("keepAlive doIO with ip = <{}>", ip);
                }
                TrafT4Connection tmpConn = null;
                List<TrafT4Connection> list = ipWithConnsMap.get(ip);
                for (TrafT4Connection conn2 : list) {
                    if (!conn2.isClosed()) {
                        tmpConn = conn2;
                        break;
                    }
                }
                String arrs = entry.getValue().toString();
                int len = arrs.length();
                KeepAliveCheckReply kcr = tmpConn.startKeepAlive(arrs.substring(1, len - 1), ip);
                if (kcr != null) {
                    String lostDialogueIds = kcr.getKeepalive();
                    if (lostDialogueIds != null) {
                        String[] strs = lostDialogueIds.split(",");
                        for (String str : strs) {
                            int id = Integer.parseInt(str);
                            TrafT4Connection conn3 = null;
                            try {
                                conn3 = dialogueIdWithConnMap.get(id);
                                if (LOG.isWarnEnabled()) {
                                    LOG.warn("conn lost with remoteProcess {} ",
                                        conn3.getRemoteProcess());
                                }
                            } finally {
                                conn3.notifyIcClose();
                            }
                        }
                    }
                }
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            initMap();
        }
    }

    private static void initMap() {
        ipWithConnsMap.clear();
        ipWithDialogueIdMap.clear();
        dialogueIdWithConnMap.clear();
        taskFlag = true;
    }

}
