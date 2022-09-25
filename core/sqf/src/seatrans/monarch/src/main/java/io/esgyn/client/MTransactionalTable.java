package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.MServerLocation;
//import io.ampool.monarch.table.internal.MRowScanner;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;
import io.esgyn.ipc.AmpoolIPC;
import io.esgyn.utils.AmpoolUtils;
import io.esgyn.utils.BagOfArgs;
import io.esgyn.utils.EsgynMServerLocation;
import io.esgyn.utils.MConnection;

import java.io.File;
import java.util.Collection;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.codec.binary.Hex;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.regionserver.transactional.SingleVersionDeleteNotSupported;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.Path;

/**
 * Monarch Table with transactional support.
 */
public class MTransactionalTable implements MTransactionalTableClient {

    static final Logger LOG = LogManager.getLogger(MTransactionalTable.class.getName());

    static String sv_config_property_ampool_coprocessor_class = "io.esgyn.ampool.coprocessor.class";
    static String sv_esgyn_coprocessor_class_default = "io.esgyn.coprocessor.AmpoolTransactions";
    static String sv_esgyn_coprocessor_class = 
	MConnection.getMConfiguration().get(sv_config_property_ampool_coprocessor_class,
					    sv_esgyn_coprocessor_class_default);
    
    static int                 retries = 30;
    static int                 delay = 1000;
    static int                 regionNotReadyDelay = 30000;

    MTable m_mtable;
    private String retryErrMsg = "Coprocessor result is null, retries exhausted";
    
    /**
     * @param tableName
     * @throws IOException
     */
    public MTransactionalTable(final String pv_table_name, Connection pv_connection) 
	throws IOException 
    {
	this(pv_table_name);
    }
    
    public MTransactionalTable(final String pv_table_name) 
	throws IOException 
    {
	m_mtable = MConnection.getTable(pv_table_name);
    }

    /**
     * @param tableName
     * @throws IOException
     */
    public MTransactionalTable(final byte[] pv_table_name, Connection pv_connection) 
	throws IOException
    {
	this(new String(pv_table_name));
    }

    public MTransactionalTable(final byte[] pv_table_name) 
	throws IOException 
    {
	this(new String(pv_table_name));
    }

    private void addLocation(final MTransactionState transactionState, EsgynMServerLocation location) 
    {

      if (LOG.isTraceEnabled()) LOG.trace("addLocation ENTRY");

      if (transactionState.addRegion(location)){
	  if (LOG.isTraceEnabled()) LOG.trace("addLocation added"
					      + ", region: " + location.toString()
					      + ", txId: " + transactionState.getTransactionId() 
					      );
      }

      if (LOG.isTraceEnabled()) LOG.trace("addLocation EXIT");
    }

    /**
     * Method for getting data from a row
     * 
     * @param get the Get to fetch
     * @return the result
     * @throws IOException
     * @since 0.20.0
     */
    public Row get(final MTransactionState transactionState, final EsgynMGet get)
	throws IOException 
    {

	return get(transactionState, get, true);

    }

    void traceRow(Row pv_result, long pv_txid) 
    {

	LOG.trace("get Exit"
		  + ", txId: " + pv_txid
		  + ", #cells in result: " + (pv_result != null ? pv_result.size():0)
		  + ", result: " + pv_result
		  );
	
	if (pv_result == null) {
	    return;
	}

	List<Cell> lv_cells = pv_result.getCells();
	if (lv_cells == null) {
	    return;
	}

	int lv_num_cells = lv_cells.size();
	for (int lv_index=0; lv_index < lv_num_cells; lv_index++) {
	    Cell lv_cell = lv_cells.get(lv_index);
	    LOG.trace(
		      "colName: " + new String(lv_cell.getColumnName())
		      + ", colType: " + lv_cell.getColumnType()
		      );
	}

    }

    public Row get(final MTransactionState transactionState, 
		       final EsgynMGet pv_get, 
		       final boolean   pv_bool_addLocation) 
	throws IOException 
    {

	if (LOG.isTraceEnabled()) LOG.trace("get Entry"
					    + ", txId: " + transactionState.getTransactionId()
					    );

	if (pv_bool_addLocation) addLocation(transactionState,
					     getRegionLocation(pv_get.getRowKey(), false)
					     );
	
	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.GetTransactional;

	AmpoolIPC.GetTransactionalRequest lv_cpr = new AmpoolIPC.GetTransactionalRequest();

	lv_cpr.m_get = pv_get; 
	lv_cpr.m_transaction_id = transactionState.getTransactionId();

	MExecutionRequest lv_request = new MExecutionRequest();
	lv_request.setArguments(lv_sr);
	lv_sr.m_request = lv_cpr;
	
	List<Object> lv_collector = m_mtable
	    .coprocessorService(sv_esgyn_coprocessor_class,
				"cpcall", 
				pv_get.getRowKey(),
				lv_request);

	if (LOG.isTraceEnabled()) LOG.trace("Back from the coprocessor"
					    + ", txId: " + transactionState.getTransactionId()
					    + ", #entries in the result: " + (lv_collector == null ? 0 : lv_collector.size())
					    );

	Row lv_result = null;

	if (lv_collector != null) {

	    if (lv_collector.size() > 0) {
		Object lv_rp_obj = lv_collector.get(0);
		if (LOG.isTraceEnabled()) LOG.trace("response object from the cp: " + lv_rp_obj
						    + ", txId: " + transactionState.getTransactionId()
						    );
		if (lv_rp_obj instanceof AmpoolIPC.GetTransactionalResponse) {
		    AmpoolIPC.GetTransactionalResponse lv_rp_cpr = (AmpoolIPC.GetTransactionalResponse) lv_rp_obj;
		    if (LOG.isTraceEnabled()) {
			LOG.trace("get cp response" 
				  + ", txId: " + transactionState.getTransactionId()
				  + ", status: " + lv_rp_cpr.m_status
				  + ", has_exception: " + lv_rp_cpr.m_has_exception
				  );
		    }

		    if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
			LOG.error("get response exception"
				  + ", txId: " + transactionState.getTransactionId()
				  , lv_rp_cpr.m_exception
				  );
			//TBD throw ?? - i think so
		    }

		    lv_result = lv_rp_cpr.m_result;
		}
	    }
	}

	if (LOG.isTraceEnabled()) {
	    traceRow(lv_result, transactionState.getTransactionId());
	}
	
	return lv_result;
    }
    
    /**
     * @param delete
     * @throws IOException
     * @since 0.20.0
     */

    public void delete(final MTransactionState transactionState, final Delete delete) 
	throws IOException 
    {
	delete(transactionState, delete, true);
    }

    public void delete(final MTransactionState transactionState, 
		       final Delete pv_delete, 
		       final boolean pv_bool_addLocation) 
	throws IOException 
    {

    	if (LOG.isTraceEnabled()) LOG.trace("delete Entry"
					    + ", addLocation: " + pv_bool_addLocation
					    + ", txId: " + transactionState.getTransactionId()
					    );
	
	if (pv_bool_addLocation) addLocation(transactionState,
					     getRegionLocation(pv_delete.getRowKey(), false)
					     );
	
	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.DeleteTransactional;

	AmpoolIPC.DeleteTransactionalRequest lv_cpr = new AmpoolIPC.DeleteTransactionalRequest();

	lv_cpr.m_delete = pv_delete.getRowKey(); 
	lv_cpr.m_transaction_id = transactionState.getTransactionId();

	MExecutionRequest lv_request = new MExecutionRequest();
	lv_request.setArguments(lv_sr);
	lv_sr.m_request = lv_cpr;
	
    	if (LOG.isTraceEnabled()) LOG.trace("delete before cp call"
					    + ", rowKey: " + new String(pv_delete.getRowKey())
					    );
	
	List<Object> lv_collector = m_mtable
	    .coprocessorService(sv_esgyn_coprocessor_class,
				"cpcall", 
				pv_delete.getRowKey(),
				lv_request);

	if (LOG.isTraceEnabled()) LOG.trace("Back from the coprocessor"
					    + ", txId: " + transactionState.getTransactionId()
					    + ", #entries in the result: " + (lv_collector == null ? 0 : lv_collector.size())
					    );
	
	if (lv_collector != null) {

	    if (lv_collector.size() > 0) {
		Object lv_rp_obj = lv_collector.get(0);
		if (LOG.isTraceEnabled()) LOG.trace("response object from the cp: " + lv_rp_obj
						    + ", txId: " + transactionState.getTransactionId()
						    );
		if (lv_rp_obj instanceof AmpoolIPC.DeleteTransactionalResponse) {
		    AmpoolIPC.DeleteTransactionalResponse lv_rp_cpr = (AmpoolIPC.DeleteTransactionalResponse) lv_rp_obj;
		    if (LOG.isTraceEnabled()) {
			LOG.trace("delete response" 
				  + ", txId: " + transactionState.getTransactionId()
				  + ", status: " + lv_rp_cpr.m_status
				  + ", has_exception: " + lv_rp_cpr.m_has_exception
				  );
		    }

		    if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
			LOG.error("delete response exception"
				  + ", txId: " + transactionState.getTransactionId()
				  , lv_rp_cpr.m_exception);
		    }
		}
	    }
	}

	if (LOG.isTraceEnabled()) LOG.trace("delete Exit"
					    + ", txId: " + transactionState.getTransactionId()
					    );
	return;
    }

    /**
     * Commit a Put to the table.
     * <p>
     * If autoFlush is false, the update is buffered.
     * 
     * @param put
     * @throws IOException
     * @since 0.20.0
     */
    public synchronized void put(final MTransactionState transactionState, final EsgynMPut put) 
	throws IOException 
    {
      put(transactionState, put, true);
    }

    public synchronized void put(final MTransactionState transactionState, 
				 final EsgynMPut pv_put, 
				 final boolean pv_bool_addLocation) 
	throws IOException 
    {

	//TBD setting this DEBUG temporarily
	if (LOG.isDebugEnabled()) LOG.debug("put Entry"
					    + ", txId: " + transactionState.getTransactionId()
					    + ", addLocation: " + pv_bool_addLocation
					    );
	
	if (pv_bool_addLocation) addLocation(transactionState,
					     getRegionLocation(pv_put.getRowKey(),
							       false)
					     );
	
	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.PutTransactional;

	AmpoolIPC.PutTransactionalRequest lv_cpr = new AmpoolIPC.PutTransactionalRequest();

	lv_cpr.m_put = pv_put; 
	lv_cpr.m_transaction_id = transactionState.getTransactionId();

	MExecutionRequest lv_request = new MExecutionRequest();
	lv_request.setArguments(lv_sr);
	lv_sr.m_request = lv_cpr;
	
	List<Object> lv_collector = m_mtable
	    .coprocessorService(sv_esgyn_coprocessor_class,
				"cpcall", 
				pv_put.getRowKey(),
				lv_request);

	if (LOG.isTraceEnabled()) LOG.trace("Back from the coprocessor"
					    + ", txId: " + transactionState.getTransactionId()
					    + ", #entries in the result: " + (lv_collector == null ? 0 : lv_collector.size())
					    );
	
	if (lv_collector != null) {
	    
	    if (lv_collector.size() <= 0) {
		LOG.error("Problem with put"
			  + ", txId: " + transactionState.getTransactionId()
			  + ", addLocation: " + pv_bool_addLocation
			  + ", location: " + getRegionLocation(pv_put.getRowKey(),false)
			  );
	    }
	    else {
		Object lv_rp_obj = lv_collector.get(0);
		if (LOG.isTraceEnabled()) LOG.trace("response object from the cp: " + lv_rp_obj
						    + ", txId: " + transactionState.getTransactionId()
						    );
		if (lv_rp_obj instanceof AmpoolIPC.PutTransactionalResponse) {
		    AmpoolIPC.PutTransactionalResponse lv_rp_cpr = (AmpoolIPC.PutTransactionalResponse) lv_rp_obj;
		    if (LOG.isTraceEnabled()) {
			LOG.trace("put response" 
				  + ", txId: " + transactionState.getTransactionId()
				  + ", status: " + lv_rp_cpr.m_status
				  + ", has_exception: " + lv_rp_cpr.m_has_exception
				  );
		    }
		    if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
			LOG.error("put response exception"
				  + ", txId: " + transactionState.getTransactionId()
				  , lv_rp_cpr.m_exception
				  );
		    }
		}
	    }
	}

	//TBD setting this DEBUG temporarily
	if (LOG.isDebugEnabled()) LOG.debug("put Exit"
					    + ", txId: " + transactionState.getTransactionId()
					    );
	return;
    }

  public synchronized Scanner getScanner(final MTransactionState transactionState, 
						final Scan scan) 
      throws IOException 
    {

    if (LOG.isTraceEnabled()) LOG.trace("Enter MTransactionalTable.getScanner");

    /* TBD
    if (scan.getCaching() <= 0) {
        scan.setCaching(getScannerCaching());
    }
    
    Long value = (long) 0;
    */

    Scanner scanner = m_mtable.getScanner(scan);
    //TBD new MRowScanner(this, transactionState, scan, value);
    
    return scanner;         
  }

  public boolean checkAndDelete(final MTransactionState transactionState,
				final byte[] row, 
				final byte[] column, 
				final byte[] value,
				final EsgynMDelete delete) 
      throws IOException 
    {

      long transactionId = transactionState.getTransactionId();

      if (LOG.isTraceEnabled()) LOG.trace("Enter MTransactionalTable.checkAndDelete"
					  + ", row: " + row 
					  + ", column: " + column 
					  + ", value: " + value
					  + ", txId: " + transactionId
					  );
      if (!Bytes.equals(row, delete.getRowKey())) {
	  throw new IOException("Action's getRowKey must match the passed row");
      }
      
      return true;
   }

    public boolean checkAndPut(final MTransactionState transactionState,
			       final byte[] pv_row, 
			       final byte[] pv_column, 
			       final byte[] pv_value, 
			       final EsgynMPut pv_put) 
	throws IOException 
    {
	
	boolean lv_return_status = true;

	long transactionId = transactionState.getTransactionId();

	if (LOG.isTraceEnabled()) LOG.trace("Enter checkAndPut"
					    + ", row: " + pv_row 
					    + ", column: " + pv_column 
					    + ", value: " + pv_value
					    + ", txId: " + transactionId
					    );
	
	if (!Bytes.equals(pv_row, pv_put.getRowKey())) {
	    throw new IOException("Action's getRowKey must match the passed row");
	}

	AmpoolIPC lv_sr = new AmpoolIPC();
	lv_sr.m_request_type = AmpoolIPC.RequestType.CheckAndPut;

	AmpoolIPC.CheckAndPutRequest lv_cpr = new AmpoolIPC.CheckAndPutRequest();

	lv_cpr.m_put = pv_put; 
	lv_cpr.m_transaction_id = transactionState.getTransactionId();
	lv_cpr.m_column = pv_column;
	lv_cpr.m_value =  pv_value;
	lv_cpr.m_row_key = pv_row;

	MExecutionRequest lv_request = new MExecutionRequest();
	lv_request.setArguments(lv_sr);
	lv_sr.m_request = lv_cpr;
	
	List<Object> lv_collector = m_mtable
	    .coprocessorService(sv_esgyn_coprocessor_class,
				"cpcall", 
				pv_put.getRowKey(),
				lv_request);
	
	if (LOG.isTraceEnabled()) LOG.trace("checkAndPut - Back from the coprocessor"
					    + ", txId: " + transactionState.getTransactionId()
					    + ", #entries in the result: " + (lv_collector == null ? 0 : lv_collector.size())
					    );
	
	if (lv_collector == null) {
	    lv_return_status = false;
	}
	else {
	    if (lv_collector.size() <= 0) {
		LOG.error("checkAndPut - Problem with the coprocessor response"
			  + ", #entries in the result: " + (lv_collector == null ? 0 : lv_collector.size())
			  + ", txId: " + transactionState.getTransactionId()
			  + ", location: " + getRegionLocation(pv_put.getRowKey(),false)
			  );
		lv_return_status = false;
	    }
	    else {
		Object lv_rp_obj = lv_collector.get(0);
		if (LOG.isTraceEnabled()) LOG.trace("checkAndPut - response object from the cp: " + lv_rp_obj
						    + ", txId: " + transactionState.getTransactionId()
						    );
		if (lv_rp_obj instanceof AmpoolIPC.CheckAndPutResponse) {
		    AmpoolIPC.CheckAndPutResponse lv_rp_cpr = (AmpoolIPC.CheckAndPutResponse) lv_rp_obj;
		    if (LOG.isTraceEnabled()) {
			LOG.trace("checkAndPut - response from the cp" 
				  + ", txId: " + transactionState.getTransactionId()
				  + ", status: " + lv_rp_cpr.m_status
				  + ", has_exception: " + lv_rp_cpr.m_has_exception
				  );
		    }
		    if (lv_rp_cpr.m_has_exception && (lv_rp_cpr.m_exception != null)) {
			lv_return_status = false;
			LOG.error("checkAndPut - exception in the response from the cp"
				  + ", txId: " + transactionState.getTransactionId()
				  , lv_rp_cpr.m_exception
				  );
			throw new IOException(lv_rp_cpr.m_exception);
		    }
		    lv_return_status = lv_rp_cpr.m_status;
		}
	    }
	}

	return lv_return_status;
    }

    /**
     * Looking forward to TransactionalRegion-side implementation
     * 
     * @param transactionState
     * @param deletes
     * @throws IOException
     */
    public void delete(final MTransactionState transactionState,
		       List<Delete> deletes)
	throws IOException 
    {

	long transactionId = transactionState.getTransactionId();

	if (LOG.isTraceEnabled()) LOG.trace("Enter MTransactionalTable.delete[]<List>"
					    + ", size: " + deletes.size() 
					    + ", txId: " + transactionId
					    );
	return;
    }

    /**
     * Put a set of rows
     * 
     * @param transactionState
     * @param puts
     * @throws IOException
     */
    public void put(final MTransactionState transactionState,
		    final List<EsgynMPut> puts) 
	throws IOException 
    {

	long transactionId = transactionState.getTransactionId();

	if (LOG.isTraceEnabled()) LOG.trace("Enter TransactionalTable.put[] <List>" 
					    + ", size: " + puts.size() 
					    + ", transid: " + transactionId
					    );

    }
	
    private int maxKeyValueSize;
    
    public EsgynMServerLocation getRegionLocation(byte[] row, boolean f)
	throws IOException 
    {
	MServerLocation lv_server_locationx = AmpoolUtils.getMServerLocationX(m_mtable, 
									      row);
	return new EsgynMServerLocation(m_mtable.getName(),
					lv_server_locationx);
    }

    public void close()  throws IOException { 
	/* TBD Monarch doesn't have a close
	m_mtable.close(); 
	*/
    }

    public void setAutoFlush(boolean autoFlush, boolean b)
    {
        /* TBD 
	   m_mtable.setAutoFlush(autoFlush, b);
	*/
    }

    public byte[][] getEndKeys()
                    throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter getEndKeys");
        return m_mtable.getMTableLocationInfo().getEndKeys();
    }

    public byte[][] getStartKeys() throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter getStartKeys");
        return m_mtable.getMTableLocationInfo().getStartKeys();
    }

    /*
    public void setWriteBufferSize(long writeBufferSize) throws IOException
    {
        m_mtable.setWriteBufferSize(writeBufferSize);
    }
    public long getWriteBufferSize()
    {
        return m_mtable.getWriteBufferSize();
    }
    */

    public byte[] getTableName()
    {
        return m_mtable.getName().getBytes();
    }

    public Scanner getScanner(Scan scan, float DOPparallelScanner) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter getScanner");
	return m_mtable.getScanner(scan);
	//return new TrafParallelClientScanner(this.connection, scan, getName(), DOPparallelScanner);       
    }

    public Row get(Get g) throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter get");
        Row lv_result = m_mtable.get(g);

	if (LOG.isTraceEnabled()) {
	    traceRow(lv_result, 0);
	}

	return lv_result;
    }
    
    public Row[] get( List<Get> g) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter get list");
        return m_mtable.get(g);
    }

    public void delete(EsgynMDelete d) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter delete");
        m_mtable.delete(d);
    }

    public void delete(List<Delete> deletes) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter delete (list)");
	//  m_mtable.delete( deletes );
    }

    public boolean checkAndPut(byte[] row, 
			       byte[] column, 
			       byte[] value, 
			       EsgynMPut put) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter checkAndPut");
        return m_mtable.checkAndPut(row,column,value,put);
    }

    public void put(EsgynMPut p) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter put )");
        m_mtable.put(p);
    }

    public void put(List<Put> p) 
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter put (List<Put>)");
        m_mtable.put(p);
    }

    public boolean checkAndDelete(byte[] row,
				  byte[] column, 
				  byte[] value,  
				  EsgynMDelete delete)
	throws IOException
    {
	if (LOG.isTraceEnabled()) LOG.trace("Enter checkAndDelete");
        return m_mtable.checkAndDelete(row,
				       column,
				       value,
				       delete);
    }

    MTable getTable() {
	return m_mtable;
    }
	
}
