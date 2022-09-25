package io.esgyn.ipc;

import io.ampool.monarch.table.*;

import io.esgyn.coprocessor.*;

import java.util.List;

public class AmpoolIPC implements java.io.Serializable {

    public enum RequestType {
	CheckAndPut,
	    CheckAndDelete,
	    GetTransactional,
	    PutTransactional,
	    DeleteTransactional,
	    MultiGet,
	    MultiPut,
	    Counter,
	    CounterTransactional,
	    GetTransactionCount,
	    GetTransactionList,
	    PrepareTransaction,
	    CommitTransaction,
	    AbortTransaction,
	    NumCPCalls
	    };
    
    public RequestType      m_request_type;
    public java.lang.Object m_request;

    public AmpoolIPC() {
    }

    public static class CounterRequest implements java.io.Serializable {
	public long m_transaction_id;
    }

    public static class CounterTransactionalRequest implements java.io.Serializable {
	public long      m_transaction_id;
	public byte[]    m_region_name;
    }

    public static class CounterTransactionalResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
	public long                  m_count;
    }

    public static class CheckAndPutRequest implements java.io.Serializable {
	public long      m_transaction_id;
	public byte[]    m_region_name;
	public byte[]    m_row_key;
	public byte[]    m_column;
	public byte[]    m_value;
	public EsgynMPut m_put;
    }

    public static class CheckAndPutResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
    }

    public static class PutTransactionalRequest implements java.io.Serializable {
	public long      m_transaction_id;
	public byte[]    m_region_name;
	public EsgynMPut m_put;
    }

    public static class PutTransactionalResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
    }

    public static class DeleteTransactionalRequest implements java.io.Serializable {
	public long         m_transaction_id;
	public byte[]       m_region_name;
	public byte[]       m_delete;
    }

    public static class DeleteTransactionalResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
    }

    public static class GetTransactionalRequest implements java.io.Serializable {
	public long   m_transaction_id;
	public byte[] m_region_name;
	public EsgynMGet   m_get;
    }

    public static class GetTransactionalResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
	public Row                m_result;
    }

    public static class GetTransactionCountResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
	public int                   m_num_transactions;
    }

    public static class GetTransactionListResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
	public int                   m_num_transactions;
        public List<String>    	     m_txid_list;
    }

    public static class AbortTransactionRequest implements java.io.Serializable {
	public long      m_transaction_id;
	public byte[]    m_region_name;
	public int       m_participant_num;
    }

    public static class AbortTransactionResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
    }

    public static class CommitRequestRequest implements java.io.Serializable {
	public long      m_transaction_id;
	public byte[]    m_region_name;
	public int       m_participant_num;
    }

    public static class CommitRequestResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
    }

    public static class CommitRequest implements java.io.Serializable {
	public long      m_transaction_id;
	public byte[]    m_region_name;
	public int       m_participant_num;
	public boolean   m_ignoreUnknownTransactionException;
    }

    public static class CommitResponse implements java.io.Serializable {
	public boolean               m_status;
	public int                   m_status_code;
	public boolean               m_has_exception;
	public java.lang.Throwable   m_exception;
    }

}
