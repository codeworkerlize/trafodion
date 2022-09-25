package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;
import io.esgyn.utils.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.CommitUnsuccessfulException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Holds client-side transaction information. Client's use them as opaque objects passed around to transaction
 * operations.
 */
public class MTransactionState {
    
    static final Logger LOG = LogManager.getLogger(MTransactionState.class.getName());

    // Current transactionId has the following composition:
    //  int   sequenceId
    //  short nodeId
    //  short clusterId
    private final long transactionId;
    private TransState status;
    private long startId;
    private long commitId;
    private long recoveryASN;
    private AtomicInteger regionsToIgnore;

    private boolean m_HasRemotePeers;
    /**
     * 
     * requestPendingCount - how many requests send
     * requestReceivedCount - how many replies received
     * countLock - synchronize access to above
     * 
     * commitSendDone - did we sent all requests yet
     * commitSendLock - synchronize access to above
     */
    private int requestPendingCount;
    private int requestReceivedCount;
    private Object countLock;
    private boolean commitSendDone;
    private Object commitSendLock;
    private boolean hasError;
    private boolean localTransaction;
    private boolean ddlTrans;
    private static boolean sv_useConcurrentHM = false;
    private static boolean sv_localTransaction = false;
    private boolean hasRetried = false;

    static {
       String concurrentHM = System.getenv("DTM_USE_CONCURRENTHM");
       if (concurrentHM != null) 
           sv_useConcurrentHM = (Integer.parseInt(concurrentHM) == 0) ? false : true;
       else
           sv_useConcurrentHM = false;
       if (sv_useConcurrentHM) 
          LOG.info("Using ConcurrentHashMap synchronization to synchronize participatingRegions");
       else 
          LOG.info("Using synchronizedSet to synchronize participatingRegions");
        String localTxns = System.getenv("DTM_LOCAL_TRANSACTIONS");
        if (localTxns != null) {
          sv_localTransaction = (Integer.parseInt(localTxns)==0)?false:true;
        }
    }

    public Set<String> tableNames = Collections.synchronizedSet(new HashSet<String>());
    public Set<EsgynMServerLocation> sv_participating_regions;
    /**
     * Regions to ignore in the twoPase commit protocol. They were read only, or already said to abort.
     */
//    public Set<EsgynMServerLocation> regionsToIgnore
//	= Collections.synchronizedSet(new HashSet<EsgynMServerLocation>());

    private Set<EsgynMServerLocation> retryRegions 
	= Collections.synchronizedSet(new HashSet<EsgynMServerLocation>());

    private native void registerMRegion(long transid, long startid, 
					int port, byte[] hostname, long startcode, byte[] regionInfo, int peerId);

    public boolean islocalTransaction() {
      return localTransaction;
    }

    public MTransactionState(final long transactionId) {
        this.transactionId = transactionId;
        setStatus(TransState.STATE_ACTIVE);
        countLock = new Object();
        commitSendLock = new Object();
        requestPendingCount = 0;
        requestReceivedCount = 0;
        commitSendDone = false;
        hasError = false;
        ddlTrans = false;
        m_HasRemotePeers = false;
        recoveryASN = -1;
		regionsToIgnore = new AtomicInteger(0);

        if (sv_useConcurrentHM) {
          sv_participating_regions = Collections.newSetFromMap((new ConcurrentHashMap<EsgynMServerLocation, Boolean>()));
        }
        else {
          sv_participating_regions = Collections.synchronizedSet(new HashSet<EsgynMServerLocation>());
        }

	localTransaction = sv_localTransaction;
	if (sv_localTransaction) {
	    if (LOG.isTraceEnabled()) LOG.trace("MTransactionState local transaction begun"
						+ ", txId: " + transactionId
						);
        }
        else {
          if (LOG.isTraceEnabled()) LOG.trace("MTransactionState global transaction begun" 
					      + ", txId: " + transactionId
					      );
        }
    }

    public boolean addTableName(final String table) {
        boolean added =  tableNames.add(table);

        if (added) {
            if (LOG.isTraceEnabled()) LOG.trace("Adding new table name [" + table 
						+ "] to transaction state"
						+ ", txId: " + transactionId
						);
        }

        return added;

    }

    /**
     * 
     * Method  : requestAllComplete
     * Params  : None
     * Return  : True if all replies received, False if not
     * Purpose : Make sure all requests have been sent, then determine
     *           if we've received all the replies.  Non blocking version
     */
    public boolean requestAllComplete() {

        // Make sure that we've completed sending the requests
        synchronized (commitSendLock)
        {
            if (commitSendDone != true)
                return false;
        }

        synchronized (countLock)
        {
            if (requestPendingCount == requestReceivedCount)
                return true;
            return false;
        }
    }

    /**
     * 
     * Method  : requestPendingcount
     * Params  : count - how many requests were sent
     * Return  : void
     * Purpose : Record how many requests were sent
     */
    public void requestPendingCount(int count)
    {
        synchronized (countLock)
        {
            hasError = false;  // reset, just in case
            requestPendingCount = count;
        }
    }

    /**
     * 
     * Method  : requestPendingCountDec
     * Params  : None
     * Return  : void
     * Purpose : Decrease number of outstanding replies needed and wake up any waiters
     *           if we receive the last one or if the wakeUp value is true (which means
     *           we received an exception)
     */
    public void  requestPendingCountDec(boolean wakeUp)
    {
       synchronized (countLock)
       {
          requestReceivedCount++;
          if ((requestReceivedCount == requestPendingCount) || (wakeUp == true))
          {
             //Signal waiters that an error occurred
             if (wakeUp == true)
                hasError = true;

             countLock.notify();
          }
       }
    }

    /**
     * 
     * Method  : completeRequest
     * Params  : None
     * Return  : Void
     * Purpose : Hang thread until all replies have been received
     */
    public void completeRequest() throws InterruptedException, CommitUnsuccessfulException
    {
        // Make sure we've completed sending all requests first, if not, then wait
        synchronized (commitSendLock)
        {
            if (commitSendDone == false)
            {
                commitSendLock.wait();
            }
        }

        // if we haven't received all replies, then wait
        synchronized (countLock)
        {
            if (requestPendingCount > requestReceivedCount)
                countLock.wait();
        }

        if (hasError)
            throw new CommitUnsuccessfulException();

        return;

    }

    /**
     *
     * Method  : completeSendInvoke
     * Params  : count : number of requests sent
     * Return  : void
     * Purpose : wake up waiter that are waiting on completion of sending requests 
     */
    public void completeSendInvoke(int count)
    {

        // record how many requests sent
        requestPendingCount(count);

        // wake up waiters and record that we've sent all requests
        synchronized (commitSendLock)
        {
            commitSendDone = true;
            commitSendLock.notify();
        }
    }

    // Used at the client end - the one performing the mutation - e.g. the SQL process
    public void registerLocation(final EsgynMServerLocation location, final int pv_peerId) throws IOException {

        if (islocalTransaction()) {
	    if (LOG.isTraceEnabled()) LOG.trace("MTransactionState.registerLocation local transaction."
						+ "Not sending registerRegion."
						);
	    return;
        }

        byte [] lv_hostname = location.getHostName().getBytes();
        int lv_port = location.getPort();
        long lv_startcode = MonarchConstants.MONARCH_STORAGE_ENGINE_CODE; //TBD location.getStartKey();

        byte [] lv_byte_region_info = location.serialize();
	
        if (LOG.isTraceEnabled()) LOG.trace("register participant with the TM" 
					    + location.toString()
					    + ", txId: " + transactionId
					    + ", peerId: " + pv_peerId
					    + ", startId: " + startId
					    );

	registerMRegion
	    (transactionId, 
	     startId, 
	     lv_port, 
	     lv_hostname, 
	     lv_startcode, 
	     lv_byte_region_info, 
	     pv_peerId);

    }

    public boolean addRegion(final EsgynMServerLocation pv_region) {
        if (LOG.isTraceEnabled()) LOG.trace("addRegion" 
					    + ", txId: " + transactionId
					    + ", location: " + pv_region.toString()
					    );

	boolean lv_found = false;
	for (EsgynMServerLocation lv_region : sv_participating_regions) {
	    boolean lv_is_equal = lv_region.equals(pv_region);
	    if (lv_is_equal) {
		lv_found = true;
		break;
	    }
	}	    

	boolean lv_added = false;
	if (!lv_found) {
	    lv_added = sv_participating_regions.add(pv_region);
	}
	    
	if (lv_added) {
           if (LOG.isTraceEnabled()) LOG.debug("Added participant" 
					       + ", location: " + pv_region
					       + ", txId: " + transactionId
					       + ", #participants: " + sv_participating_regions.size()
					       );
        }
        else {
           if (LOG.isTraceEnabled()) LOG.trace("participant already present"
					       + ", location: " + pv_region 
					       + ", txId: " + transactionId
					       + ", #participants: " + sv_participating_regions.size()
					       );

        }

        return lv_added;
    }

    public Set<EsgynMServerLocation> getParticipatingRegions() {
        return sv_participating_regions;
    }

    public void clearParticipatingRegions() {
        sv_participating_regions.clear();
    }

    public void clearRetryRegions() {
        retryRegions.clear();
    }

    public int getRegionsToIgnore() {
       return regionsToIgnore.get();
    }

    void addRegionToIgnore(final EsgynMServerLocation region) {
        regionsToIgnore.getAndIncrement();
        region.setReadOnly(true);
    }

    Set<EsgynMServerLocation> getRetryRegions() {
        return retryRegions;
    }

    void addRegionToRetry(final EsgynMServerLocation region) {
        retryRegions.add(region);
    }


    /**
     * Get the transactionId.
     *
     * @return Return the transactionId.
     */
    public long getTransactionId() {
        return transactionId;
    }

    /**
     * Get the sequenceNum portion of the transactionId.
     *
     * @return Return the sequenceNum.
     */
    public long getTransSeqNum() {
        return transactionId & 0xFFFFFFFFL;
    }

    /**
     * Get the sequenceNum portion of the passed in transId.
     *
     * @return Return the sequenceNum.
     */
    public static long getTransSeqNum(long transId) {
    	return transId & 0xFFFFFFFFL;
    }

    /**
     * Get the originating node of the transaction.
     *
     * @return Return the nodeId.
     */
    public long getNodeId() {
        return ((transactionId >> 32) & 0xFFL);
    }
    
    /**
     * Get the originating node of the passed in transaction.
     *
     * @return Return the nodeId.
     */
    public static long getNodeId(long transId) {

        return ((transId >> 32) & 0xFFL);
    }

    /**
     * Get the originating clusterId of the transaction.
     *
     * @return Return the clusterId.
     */
    public long getClusterId() {

        return ((transactionId >> 48) & 0xFFL);

    }


    /**
     * Get the originating clusterId of the passed in transaction.
     *
     * @return Return the clusterId.
     */
    public static long getClusterId(long transId) {

        return ((transId >> 48) & 0xFFL);

    }

    /**
     * Set the startId.
     *
     */
    public void setStartId(final long id) {
        this.startId = id;
    }

    /**
     * Get the startId.
     *
     * @return Return the startId.
     */
    public long getStartId() {
        return startId;
    }

    /**
     * Set the commitId.
     *
     */
    public void setCommitId(final long id) {
        this.commitId = id;
    }

    /**
     * Get the commitId.
     *
     * @return Return the commitId.
     */
    public long getCommitId() {
        return commitId;
    }

    /**
     * Set the recoveryASN.
     *
     */
    public void setRecoveryASN(final long value) {
        this.recoveryASN = value;
    }

    /**
     * Get the recoveryASN.
     *
     * @return Return the recoveryASN.
     */
    public long getRecoveryASN() {
        return recoveryASN;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

	StringBuilder lv_return = new StringBuilder();
	
        lv_return.append("transactionId: ");
	lv_return.append(transactionId);
	lv_return.append(", startId: ");
	lv_return.append(startId);
	lv_return.append(", commitId: ");
	lv_return.append(commitId);
	lv_return.append(", participants: ");
	lv_return.append(sv_participating_regions.size());
	lv_return.append(", ignoring: ");
	lv_return.append(regionsToIgnore.get());
	lv_return.append(", hasDDL: ");
	lv_return.append(hasDDLTx());
	lv_return.append(", recoveryASN: ");
	lv_return.append(getRecoveryASN());
	lv_return.append(", remotePeers: ");
	lv_return.append(hasRemotePeers());
	lv_return.append(", state: "); 
	lv_return.append(status.toString());

	return lv_return.toString();
    }

    public int getParticipantCount() {
        return sv_participating_regions.size();
    }


    public int getRegionsRetryCount() {
        return retryRegions.size();
    }

    public String getStatus() {
      return status.toString();
    }

    public void setStatus(final TransState status) {
      this.status = status;
    }

    public boolean hasDDLTx() {
        return ddlTrans;   
    }

    public void setDDLTx(final boolean status) {
        this.ddlTrans = status;
    }

    public void setRetried(boolean val) {
        this.hasRetried = val;
    }

    public boolean hasRetried() {
      return this.hasRetried;
    }

    public void setHasRemotePeers(boolean pv_HasRemotePeers) {
       this.m_HasRemotePeers = pv_HasRemotePeers;
    }

    public boolean hasRemotePeers() {
       return this.m_HasRemotePeers;
    }

}
