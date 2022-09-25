package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;
import io.esgyn.utils.AmpoolUtils;
import io.esgyn.utils.EsgynMServerLocation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import java.io.InterruptedIOException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import java.util.List;
import java.util.Collection;

/**
 * Table with transactional support.
 */
public interface  MTransactionalTableClient  {

    /**
     * Method for getting data from a row
     * 
     * @param get the MGet to fetch
     * @return the result
     * @throws IOException
     * @since 0.20.0
     */
    Row get(final MTransactionState transactionState, 
	       final EsgynMGet get) throws IOException;

    Row get(final MTransactionState transactionState, 
	       final EsgynMGet get, 
	       final boolean bool_addLocation) throws IOException;
    
    /**
     * @param Delete
     * @throws IOException
     * @since 0.20.0
     */
    void delete(final MTransactionState transactionState, 
		final Delete Delete) throws IOException;

    void delete(final MTransactionState transactionState, 
		final Delete Delete, 
		final boolean bool_addLocation) throws IOException;

    /**
     * Commit a EsgynMPut to the table.
     * <p>
     * If autoFlush is false, the update is buffered.
     * 
     * @param put
     * @throws IOException
     * @since 0.20.0
     */
    void put(final MTransactionState transactionState, 
	     final EsgynMPut put) throws IOException ;

    void put(final MTransactionState transactionState, 
	     final EsgynMPut put, 
	     final boolean bool_addLocation) throws IOException ;

    Scanner getScanner(final MTransactionState transactionState, 
			      final Scan scan) throws IOException;

    boolean checkAndDelete(final MTransactionState transactionState,
			   final byte[] row, 
			   final byte[] column, 
			   final byte[] value,
			   final EsgynMDelete Delete) throws IOException;
    
    boolean checkAndPut(final MTransactionState transactionState,
			final byte[] row,
			final byte[] column,
			final byte[] value, 
			final EsgynMPut put) throws IOException ;

    /**
     * 
     * @param transactionState
     * @param deletes
     * @throws IOException
     */
    void delete(final MTransactionState transactionState,
		List<Delete> Deletes) throws IOException ;

    /**
     * EsgynMPut a set of rows
     * 
     * @param transactionState
     * @param puts
     * @throws IOException
     */
    void put(final MTransactionState transactionState,
	     final List<EsgynMPut> puts) 
	throws IOException ;

    EsgynMServerLocation getRegionLocation(byte[] row, boolean f)
	throws IOException;
    
    void close() throws IOException ;
        
    void setAutoFlush(boolean autoFlush, boolean b);

    byte[][] getEndKeys()
                    throws IOException;

    byte[][] getStartKeys() throws IOException;

    byte[] getTableName();

    Scanner getScanner(Scan scan, float DOPparallelScanner) throws IOException;

    Row get(Get g) throws IOException; 
    
    Row[] get( List<Get> g) throws IOException;

    void delete(EsgynMDelete d) throws IOException;

    void delete(List<Delete> deletes) throws IOException;

    boolean checkAndPut(byte[] row, 
			byte[] column, 
			byte[] value, 
			EsgynMPut put) throws IOException;

    void put(EsgynMPut p) throws IOException;

    public void put(List<Put> p) throws IOException;

    public boolean checkAndDelete(byte[] row, 
				  byte[] column, 
				  byte[] value,  
				  EsgynMDelete delete) throws IOException;

}
