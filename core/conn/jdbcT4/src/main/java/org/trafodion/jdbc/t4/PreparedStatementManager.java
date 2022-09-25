// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.trafodion.jdbc.t4;

import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.utils.Utils;

public abstract class PreparedStatementManager extends TrafT4Handle {

    private final static org.slf4j.Logger LOG =
            LoggerFactory.getLogger(PreparedStatementManager.class);
    
    private ConcurrentHashMap<String, CachedPreparedStatement> prepStmtsInCache_;
    private ConcurrentHashMap<String, CachedPreparedStatement> sameSqlInCache_;
    private ConcurrentHashMap<HashMap<String, Object[]>, TrafT4Statement> cqdMsgCache;
    private int maxStatements_;
    private float releasePercentage = 0f;// the num of pstmt to close when met RMS mem limit
    private T4Properties t4props;
    Level traceLevel_;
    PrintWriter out_;
    String traceId_;
    private long foundCount = 0L;
    private long totalCount = 0L;

    /**
     * return current info of cached prepared statement
     * 
     * @return
     */
    public String getCachedState() {
        StringBuilder sb = new StringBuilder();
        sb.append("Max prepared statements number <").append(maxStatements_).append(">. ");
        sb.append("Current cached prepared statements number <");

        if (prepStmtsInCache_ == null) {
            sb.append(0).append(">. ");
        } else {

            sb.append(prepStmtsInCache_.size()).append(">. ").append(Utils.lineSeparator());;
            sb.append("Cached prepared statements status : ").append(Utils.lineSeparator());
            for (Entry<String, CachedPreparedStatement> entry : prepStmtsInCache_.entrySet()) {
                String key = entry.getKey();
                CachedPreparedStatement cps = entry.getValue();
                sb.append("key <").append(key).append(">. Not used time <")
                        .append(cps.notUsedTimeMills()).append(">. Used times <")
                        .append(cps.getNoOfTimesUsed()).append(">. Prepare millisecond <")
                        .append(cps.prepareTimeMills()).append(">.").append(Utils.lineSeparator());
            }
        }
        return sb.toString();
    }

    boolean isStatementCachingEnabled() {
        if (maxStatements_ < 1) {
            return false;
        } else {
            return true;
        }
    }

    protected void clearPstmtCache(){
        if(LOG.isDebugEnabled()){
            LOG.debug("clear prepStmtsInCache and sameSqlInCache");
        }
        if(this.prepStmtsInCache_ != null && !this.prepStmtsInCache_.isEmpty()){
            this.prepStmtsInCache_.clear();
        }
        if(this.sameSqlInCache_ != null && !this.sameSqlInCache_.isEmpty()){
            this.sameSqlInCache_.clear();
        }
    }

    synchronized void reprepareCachedStmt(InterfaceConnection ic) {
        if (prepStmtsInCache_ == null || prepStmtsInCache_.size() == 0) {
            return;
        }
        if (getT4props().isLogEnable(Level.FINER)) {
            String tmp = "reprepare add cached pstmt, pstmt number " + prepStmtsInCache_.size();
            T4LoggingUtilities.log(getT4props(), Level.FINER, tmp);
        }
        reprepareCachedStmtAll(prepStmtsInCache_,ic);
        if (sameSqlInCache_ != null && sameSqlInCache_.size() != 0) {
            reprepareCachedStmtAll(sameSqlInCache_,ic);
        }
    }
    synchronized void reprepareCachedStmtAll(ConcurrentHashMap<String, CachedPreparedStatement> allStmtsInCached, InterfaceConnection ic) {
        for (Entry<String, CachedPreparedStatement> entry : allStmtsInCached.entrySet()) {
            CachedPreparedStatement cachedPstmt = entry.getValue();
            PreparedStatement tmpPstmt = cachedPstmt.getPreparedStatement();
            if (tmpPstmt instanceof TrafT4PreparedStatement) {
                TrafT4PreparedStatement pstmt = (TrafT4PreparedStatement) tmpPstmt;
                pstmt.updatePstmtIc(ic);
                try {
                    pstmt.prepare(pstmt.sql_, pstmt.queryTimeout_, pstmt.resultSetHoldability_);
                } catch (SQLException e) {
                    allStmtsInCached.remove(entry.getKey());
                }
            }
        }
    }

    /**
     * strategy 1 for remove unused preparedStmt remove the earliest created and unused preparedStmt
     * 
     * @return
     * @throws SQLException
     */
    boolean makeRoom() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

        Iterator<CachedPreparedStatement> i;
        CachedPreparedStatement cs;
        long oldest;
        long stmtTime;
        String key;

        i = (prepStmtsInCache_.values()).iterator();
        if (!i.hasNext()) {
            return false;
        }
        cs = i.next();
        stmtTime = cs.getLastUsedTime();
        key = cs.getLookUpKey();
        oldest = stmtTime;

        while (i.hasNext()) {
            cs = i.next();
            stmtTime = cs.getLastUsedTime();
            if (oldest > stmtTime) {
                oldest = stmtTime;
                key = cs.getLookUpKey();
            }
        }
        cs = prepStmtsInCache_.remove(key);
        if (cs != null) {
            // if the user has already closed the statement, hard close it
            if (cs.isInUse() == false) {
                cs.close(true);
            }

            return true;
        } else {
            return false;
        }
    }

    /**
     * Strategy for remove preparedStmt
     *
     * @return
     * @throws SQLException
     */
    protected synchronized boolean releaseOutOfUsedPreparedStmt() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

        if (prepStmtsInCache_ == null || prepStmtsInCache_.size() == 0) {
            return false;
        }
        int closeNum = (int) (prepStmtsInCache_.size() * releasePercentage / 100);
        TreeMap<Float, String> scoreMap = null;
        if (closeNum > 0) {
            scoreMap = new TreeMap<Float, String>();
        } else {
            return false;
        }

        for (Entry<String, CachedPreparedStatement> entry : prepStmtsInCache_.entrySet()) {
            String key = entry.getKey();
            CachedPreparedStatement cps = entry.getValue();
            float noUseTimeMills = -1 * cps.notUsedTimeMills() / 1000F;
            long noOfTimesUsed = cps.getNoOfTimesUsed();
            float prepareTimeMills = cps.prepareTimeMills() / 1000F;
            // TODO calc the score of each cached prepared statement.
            // note this formula may not a good strategy, can change it later.
            float score = noUseTimeMills * 0.2f + noOfTimesUsed * 0.4f + prepareTimeMills * 0.4f;
            scoreMap.put(score, key);
        }
        Iterator<Float> scores = scoreMap.keySet().iterator();
        // In a concurrent env, multi-processes of pstmt pool will have a quick increase of RMS memory,
        // and close one pstmt when it meet RMS mem limit is not enough, it may lead a new process 
        // which using pstmt pool close useful pstmt, so a percentage close might be a good way.
        boolean remove = false;
        try {
            while (closeNum > 0 && scores.hasNext()) {
                Float minScore = scores.next();

                String key = scoreMap.get(minScore);
                CachedPreparedStatement removeCps = prepStmtsInCache_.get(key);
                if (removeCps != null && !removeCps.isInUse()) {
                    remove = true;
                    prepStmtsInCache_.remove(key);
                    removeCps.close(true);
                    closeNum--;
                }
            }
        } finally {
            scoreMap.clear();
        }
        return remove;
    }

    void closePreparedStatementsAll() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

        Object[] csArray;
        Object[] csArray_same;

        CachedPreparedStatement cs;
        int i = 0;

        csArray = (prepStmtsInCache_.values()).toArray();
        csArray_same = (sameSqlInCache_.values()).toArray();
        for (i = 0; i < csArray.length; i++) {
            cs = (CachedPreparedStatement) csArray[i];
            if (cs != null) {
                cs.close(false);
            }
        }
        for (i = 0; i < csArray_same.length; i++) {
            cs = (CachedPreparedStatement) csArray_same[i];
            if (cs != null) {
                cs.close(false);
            }
        }
    }

    protected String createKey(TrafT4Connection connect, String sql, int resultSetHoldability)
            throws SQLException {
        String lookupKey = sql + connect.getCatalog() + connect.getSchema()
                + connect.getTransactionIsolation() + resultSetHoldability;
        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY", lookupKey);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("ENTRY. {}", lookupKey);
        }
        return lookupKey;
    }
    
    protected ConcurrentHashMap<String, CachedPreparedStatement> getPrepStmtsInCache() {
        return prepStmtsInCache_;
    }

    boolean closePreparedStatement(TrafT4Connection connect, String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connect, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}", connect, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        }

        CachedPreparedStatement cs;

        String lookupKey = createKey(connect, sql, resultSetHoldability);

        cs = (CachedPreparedStatement) prepStmtsInCache_.get(lookupKey);
        if (cs != null) {
            cs.setInUse(false);
            return true;
        }

        return false;
    }

    void clearPreparedStatementsAll() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        if (prepStmtsInCache_ != null) {
            prepStmtsInCache_.clear();
        }
        if (sameSqlInCache_ != null) {
            sameSqlInCache_.clear();
        }
    }

    synchronized void removePreparedStatement(TrafT4Connection connect, TrafT4PreparedStatement pStmt, int resultSetHoldability) throws SQLException {
        if (prepStmtsInCache_ != null && prepStmtsInCache_.size() != 0) {
            String lookupKey = pStmt.sql_ + connect.getCatalog() + connect.getSchema()
                    + connect.getTransactionIsolation() + resultSetHoldability;
            prepStmtsInCache_.remove(lookupKey);
        }
        if (sameSqlInCache_ != null && sameSqlInCache_.size() != 0) {
            String sameSqlKey = pStmt.sql_ + pStmt.stmtLabel_;
            sameSqlInCache_.remove(sameSqlKey);
        }
    }

   synchronized void addPreparedStatement(TrafT4Connection connect, String sql, TrafT4PreparedStatement pStmt,
            int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connect, sql,
                    pStmt, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}, {}", connect, sql, pStmt, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        }
        if (pStmt.isPreparedStmtUseESP()) {
            return;
        }

        totalCount++;
        if (totalCount == Long.MAX_VALUE) {
            totalCount = 0L;
            foundCount = 0L;
        }

        CachedPreparedStatement cachedStmt;

        String lookupKey = createKey(connect, sql, resultSetHoldability);

        cachedStmt = prepStmtsInCache_.get(lookupKey);
        if (cachedStmt != null) {
            foundCount++;
            if (!cachedStmt.getStmtLabel().equals(pStmt.stmtLabel_)) {
                String sameSqlKey = sql + pStmt.stmtLabel_;
                CachedPreparedStatement sameSqlStmt =
                        new CachedPreparedStatement(pStmt, sameSqlKey);
                sameSqlInCache_.put(sameSqlKey, sameSqlStmt);
            }
            // Update the last use time
            cachedStmt.setLastUsedInfo();
        } else {
                boolean isCacheFull = false;
                if (prepStmtsInCache_.size() >= maxStatements_) {
                    isCacheFull = true;
                    try {
                        if (releaseOutOfUsedPreparedStmt()) isCacheFull = false;
                    } catch (Exception e) {
                        // ignore
                    }
                    LOG.info("Preparedstmt pool was full. Released out of used preparedstmt {}", (isCacheFull? "failed.":"successfully"));
                }
                if (!isCacheFull) addPreparedStatement(lookupKey, pStmt);
        }
        if (totalCount % 10000 == 0) {
            NumberFormat nf = NumberFormat.getPercentInstance();
            nf.setMinimumFractionDigits(2);
            LOG.info("Current preparedstmt cache hit accurary: " + nf.format(foundCount * 1.0 / totalCount));
        }
    }

    private void addPreparedStatement(String key, TrafT4PreparedStatement pStmt) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", key, pStmt);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {} , {}", key, pStmt);
        }
        CachedPreparedStatement cachedStmt = new CachedPreparedStatement(pStmt, key);
        prepStmtsInCache_.put(key, cachedStmt);
    }

    PreparedStatement getPreparedStatement(TrafT4Connection connect, String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", connect, sql,
                    resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}, {}, {}, {}", connect, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        }

        PreparedStatement pStmt = null;
        CachedPreparedStatement cachedStmt;

        String lookupKey = createKey(connect, sql, resultSetHoldability);

        if (prepStmtsInCache_ != null) {
            cachedStmt = (CachedPreparedStatement) prepStmtsInCache_.get(lookupKey);
            if (cachedStmt != null) {
                if (!cachedStmt.isInUse()) {
                    pStmt = cachedStmt.getPreparedStatement();
                    ((org.trafodion.jdbc.t4.TrafT4PreparedStatement) pStmt).reuse(connect,
                            resultSetType, resultSetConcurrency, resultSetHoldability);
                } else {
                    pStmt = null;
                }
            }
        }
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", connect, sql,
                    resultSetType, resultSetConcurrency, resultSetHoldability, lookupKey,
                    pStmt == null ? null : ((TrafT4PreparedStatement) pStmt).stmtLabel_);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}, {}, {}, {}, {}, {}, {}", connect, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability, lookupKey,
                    pStmt == null ? null : ((TrafT4PreparedStatement) pStmt).stmtLabel_);
        }
        return pStmt;
    }

    T4Properties getT4props() {
        return t4props;
    }

    void setLogInfo(Level traceLevel, PrintWriter out) {
        this.traceLevel_ = traceLevel;
        this.out_ = out;

    }

    PreparedStatementManager(T4Properties t4props) {
        super();
        this.t4props = t4props;
        this.maxStatements_ = t4props.getMaxStatements();
        this.releasePercentage = t4props.getReleaseStatementsPercentage();
        cqdMsgCache = new ConcurrentHashMap<HashMap<String, Object[]>, TrafT4Statement>();
        if (maxStatements_ > 0) {
            prepStmtsInCache_ = new ConcurrentHashMap<String, CachedPreparedStatement>(maxStatements_);
            sameSqlInCache_ = new ConcurrentHashMap<String, CachedPreparedStatement>(maxStatements_);
        }
    }

    synchronized void storeCQDMsg(short executeAPI, int paramRowCount, int paramCount,
        Object[] paramValues, int queryTimeout, String sql, TrafT4Statement stmt) {
        StringBuffer sb = new StringBuffer();
        sb.append(executeAPI).append("&").append(paramRowCount).append("&").append(paramCount)
            .append("&").append(queryTimeout).append("&").append(sql);
        String tmpkey = sb.toString();
        HashMap<String, Object[]> tmpmap = new HashMap<String, Object[]>();
        tmpmap.put(tmpkey, paramValues);
        if (tmpmap != null && stmt != null) {
            cqdMsgCache.put(tmpmap, stmt);
        }
    }

    protected void resetCQD(InterfaceConnection ic) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            String tmp = "reset cqds if autocommit is true";
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", tmp);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", "reset cqds if autocommit is true");
        }
        for (Entry<HashMap<String, Object[]>, TrafT4Statement> stmtEntry : cqdMsgCache.entrySet()) {
            HashMap<String, Object[]> tmpMap = stmtEntry.getKey();
            TrafT4Statement stmt = stmtEntry.getValue();
            for (Entry<String, Object[]> msgEntry : tmpMap.entrySet()) {
                String[] msgstr = msgEntry.getKey().split("&");
                short executeAPI = Short.parseShort(msgstr[0]);
                int paramRowCount = Integer.parseInt(msgstr[1]);
                int paramCount = Integer.parseInt(msgstr[2]);
                int queryTimeout = Integer.parseInt(msgstr[3]);
                String sql = msgstr[4];
                Object[] paramValues = msgEntry.getValue();
                stmt.ist_.getT4statement().updateConnContext(ic);
                try {
                    stmt.ist_.execute(executeAPI, paramRowCount, paramCount, paramValues, queryTimeout, sql, stmt);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
