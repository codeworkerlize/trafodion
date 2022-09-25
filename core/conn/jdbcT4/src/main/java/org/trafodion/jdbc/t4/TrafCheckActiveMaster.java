package org.trafodion.jdbc.t4;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.utils.Utils;

public class TrafCheckActiveMaster {

    private final static org.slf4j.Logger LOG = LoggerFactory
            .getLogger(TrafCheckActiveMaster.class);
    private static ConcurrentHashMap<String, TrafCheckActiveMaster> activeMasterCacheMap;
    private static List<String> indexList;
    //ip:port
    private List<String> masterList;
    private int lastMasterIndex = 0;
    private List<String> addresses;
    private String url;
    private T4Properties t4Properties;
    private int emptyEqualsNull = 0;
    public void setEmptyEqualsNull(int emptyEqualsNull){
        this.emptyEqualsNull = emptyEqualsNull;
    }
    public boolean isEmptyEqualsNull(){
        //EMPTY STRING EQUIVALENT NULL : default is ENABLE
        return emptyEqualsNull == 0;
    }
    static {
        activeMasterCacheMap = new ConcurrentHashMap<String, TrafCheckActiveMaster>();
        indexList = new ArrayList<String>();
        for (int i = 0; i < 100; i++) {
            indexList.add(i, String.valueOf(i));
        }
    }
    public TrafCheckActiveMaster(T4Properties t4props) {
        this.addresses = t4props.getUrlList();
        this.url = t4props.getT4Url();
        this.t4Properties = t4props;
        this.masterList = new ArrayList<String>();
    }
    public static void checkTrafCheckActiveMaster(T4Properties t4props) {
        String url = t4props.getT4Url();
        int index = hash(url);
        synchronized (indexList.get(index)) {
            if (!activeMasterCacheMap.containsKey(url)) {
                activeMasterCacheMap.put(url, new TrafCheckActiveMaster(t4props));
            }
        }
    }
    private static int hash(Object key) {
        int h;
        int hash = (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
        int result = hash % 100;
        return (result < 0) ? -result : result;
    }
    public static ConcurrentHashMap<String, TrafCheckActiveMaster> getActiveMasterCacheMap() {
        return activeMasterCacheMap;
    }
    public List<String> getMasterList() {
        return masterList;
    }
    public String getAvailableMaster() {
        synchronized (this) {
            if ((lastMasterIndex + 1) % masterList.size() == 0) {
                lastMasterIndex = 0;
            } else {
                lastMasterIndex++;
            }
        }
        return masterList.get(lastMasterIndex);
    }
    public String getDcsMaster(TrafT4Connection trafT4Connection, boolean isUseUrlCache) throws SQLException {
        if (isUseUrlCache && this.masterList.size() > 0) {
            return getAvailableMaster();
        }
        synchronized (this) {
            if (isUseUrlCache) {
                if (this.masterList.size() > 0) {
                    if (this.t4Properties.isLogEnable(Level.FINER)) {
                        T4LoggingUtilities.log(this.t4Properties, Level.FINER, "ENTRY", this.url,
                                this.getMasterList());
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "ENTRY. DcsMaster has been cached,URL : <{}>,DcsMaster List : <{}>",
                                this.url, this.getMasterList());
                    }
                    return getAvailableMaster();
                }
            }
            trafT4Connection.checkLoginTimeout(trafT4Connection.getT4props());
            try {
                Object[] objectsRs = checkMaster(trafT4Connection);
                if (objectsRs == null) {
                    return TRANSPORT.ACTIVE_MASTER_REPLY_ERROR;
                }
                String realIp = Utils.parsingUrl((String) objectsRs[0]);
                String port = realIp.split(":")[1];
                for (String masterIp : (Vector<String>) objectsRs[1]) {
                    this.masterList.add(masterIp + ":" + port);
                }
                activeMasterCacheMap.put(this.url, this);
                this.setEmptyEqualsNull((Integer) objectsRs[2]);
                return realIp;
            } catch (SQLException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("ENTRY. Check Master Ip Error, URL: <{}>, errorMess: <{}>",
                            url, e.getMessage(), e);
                }
                throw e;
            }
        }
    }

    private Object[] checkMaster(TrafT4Connection trafT4Connection) throws SQLException {

        ExecutorService pool = Executors.newFixedThreadPool(this.addresses.size());
        CompletionService<Object[]> completionService = new ExecutorCompletionService<Object[]>(
                pool);
        List<Future> futures = new ArrayList<Future>();

        for (String addr : this.addresses) {
            T4DcsConnection t4DcsConnection = new T4DcsConnection(trafT4Connection.ic_, addr);
            CallableCheckMaster callableCheckMaster = new TrafCheckActiveMaster.CallableCheckMaster(
                    t4DcsConnection, addr);
            Future future = completionService.submit(callableCheckMaster);
            futures.add(future);
        }

        pool.shutdown();
        SQLException sqlException = null;
        StringBuilder errorSb = new StringBuilder();

        for (int i = 0; i < futures.size(); i++) {
            try {
                Object[] callableRs = completionService.take().get();
                if (callableRs != null) {
                    for (Future f : futures) {
                        f.cancel(true);
                    }
                    return callableRs;
                }
            } catch (Exception e) {
                sqlException = TrafT4Messages
                        .createSQLException(this.t4Properties, "check_active_master",
                                e.getMessage());
                errorSb.append(e.getMessage());
                errorSb.append("; ");
            }
        }
        if (LOG.isErrorEnabled()) {
            LOG.error(
                    "Check Master Ip Error, All error <{}>", errorSb.toString());
        }
        if(sqlException == null){
            sqlException = TrafT4Messages
                    .createSQLException(this.t4Properties, "check_active_master",
                            "CheckActiveMasterReply is null");
        }
        if (LOG.isErrorEnabled()) {
            LOG.error(
                "Check Master Ip Error, URL is <{}>, All error <{}>", url, errorSb.toString());
        }
        if (sqlException.getMessage().contains(TRANSPORT.ACTIVE_MASTER_REPLY_ERROR)) {
            return null;
        }
        throw sqlException;
    }
    class CallableCheckMaster implements Callable {

        T4DcsConnection t4DcsConnection;
        String addr;

        CallableCheckMaster(T4DcsConnection t4DcsConnection, String addr) {
            this.t4DcsConnection = t4DcsConnection;
            this.addr = addr;
        }

        @Override
        public Object[] call() throws Exception {
            try {
                CheckActiveMasterReply camr = t4DcsConnection.checkActiveMaster();
                if (camr != null) {
                    //url\ipSet\isEmptyEqualsNull
                    Object[] objects = new Object[3];
                    objects[0] = addr;
                    objects[1] = camr.getMasterIpVector();
                    objects[2] = camr.getIsEmptyEqualsNull();
                    return objects;
                }
            } catch (Exception e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(
                            "Check Active Master Reply error <{}>", e.getMessage());
                }
                throw e;
            }
            return null;
        }
    }
}
