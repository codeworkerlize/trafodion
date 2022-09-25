package io.esgyn.utils;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.MServerLocation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EsgynMServerLocation 
    implements Comparable<EsgynMServerLocation>
{
    
    static final Logger LOG = LogManager.getLogger(EsgynMServerLocation.class.getName());

    MServerLocation  m_server_location;
    String          m_table_name;
    public int      peerId = 0;
    public boolean  readOnly = false;

    private EsgynMServerLocation() {}

    public EsgynMServerLocation(String          pv_table_name,
				MServerLocation pv_server_location)
    {
	m_table_name = pv_table_name;
	m_server_location = pv_server_location;
	setReadOnly(false);
    }
    
    @Override
    public int compareTo(EsgynMServerLocation pv_o) {
      if (LOG.isTraceEnabled()) LOG.trace("compareTo ENTRY: " + pv_o);

      int result = getPort() - pv_o.getPort();
      if (result != 0){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo ports differ:"
					     + ", mine: " + getPort() 
					     + ", other: " + pv_o.getPort()
					     );
         return result;
      }

      result = getBucketId() - pv_o.getBucketId();
      if (result != 0){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo bucketIds differ:"
					     + ", mine: " + getBucketId() 
					     + ", other: " + pv_o.getBucketId()
					     );
         return result;
      }

      result = getTableName().compareTo(pv_o.getTableName());
      if (result != 0){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo tablenames differ:"
					     + ", mine: " + getTableName() 
					     + ", other: " + pv_o.getTableName()
					     );
         return result;
      }
      
      result = getHostName().compareTo(pv_o.getHostName());
      if (result != 0){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo hostnames differ:"
					     + ", mine: " + getHostName() 
					     + ", other: " + pv_o.getHostName()
					     );
         return result;
      }
      
      if (LOG.isTraceEnabled()) LOG.trace("compareTo EsgynMServerLocation result is: " + result );
      return result;
    }

    @Override
    public boolean equals(Object pv_o) {
      if (LOG.isTraceEnabled()) LOG.trace("equals: ENTRY: " + pv_o);

      if (this == pv_o) {
	  if (LOG.isTraceEnabled()) LOG.trace("equals: same object: " + pv_o);
	  return true;
      }
      
      if (pv_o == null) {
	  if (LOG.isTraceEnabled()) LOG.trace("equals: pv_o is null");
	  return false;
      }

      if (!(pv_o instanceof EsgynMServerLocation)) {
	  if (LOG.isTraceEnabled()) LOG.trace("equals: pv_o is not an instance of " + this);
	  return false;
      }

      return this.compareTo((EsgynMServerLocation)pv_o) == 0;

    }

    public String getTableName() {
	return m_table_name;
    }
    
    public int getBucketId() {
	return m_server_location.getBucketId();
    }
    
    public String getHostName() {
	return m_server_location.getHostName();
    }
    
    public int getPort() {
	return m_server_location.getPort();
    }

    public boolean getReadOnly() {
        return this.readOnly;
    }

    public void setReadOnly(final boolean value) {
        this.readOnly = value;
    }

    public byte[] getStartKey() {
	return m_server_location.getStartKey();
    }
	
    public byte[] getEndKey() {
	return m_server_location.getEndKey();
    }
	
    static public EsgynMServerLocation deserialize(byte[] pv_ba_esl) {

	if (LOG.isTraceEnabled()) LOG.trace("Enter EsgynMServerLocation deserialize");

	ByteBuffer lv_bb = ByteBuffer.wrap(pv_ba_esl);

 	int lv_ba_table_name_length = lv_bb.getInt();
	byte[] lv_ba_table_name = new byte[lv_ba_table_name_length];
	lv_bb.get(lv_ba_table_name, 0, lv_ba_table_name_length);

 	int lv_ba_host_name_length = lv_bb.getInt();
	byte[] lv_ba_host_name = new byte[lv_ba_host_name_length];
	lv_bb.get(lv_ba_host_name, 0, lv_ba_host_name_length);

	int lv_port = lv_bb.getInt();
	int lv_bucket_id = lv_bb.getInt();

 	int lv_ba_start_key_length = lv_bb.getInt();
	byte[] lv_ba_start_key = null;
	if (lv_ba_start_key_length > 0) {
	    lv_ba_start_key = new byte[lv_ba_start_key_length];
	    lv_bb.get(lv_ba_start_key, 0, lv_ba_start_key_length);
	}
	else {
	    lv_ba_start_key = new byte[0];
	}

 	int lv_ba_end_key_length = lv_bb.getInt();
	byte[] lv_ba_end_key = null;
	if (lv_ba_end_key_length > 0) {
	    lv_ba_end_key = new byte[lv_ba_end_key_length];
	    lv_bb.get(lv_ba_end_key, 0, lv_ba_end_key_length);
	}
	else {
	    lv_ba_end_key = new byte[0];
	}

	if (LOG.isTraceEnabled()) LOG.trace("deserialize"
					    + ", hostname: " + new String(lv_ba_host_name)
					    + ", port: " + lv_port
					    + ", bucketid: " + lv_bucket_id
					    + ", start key len: " + lv_ba_start_key.length
					    + ", end key len: " + lv_ba_end_key.length
					    );

	MServerLocation lv_sl = new MServerLocation(new String(lv_ba_host_name),
						    lv_port,
						    lv_bucket_id,
						    lv_ba_start_key,
						    lv_ba_end_key
						    );

	EsgynMServerLocation lv_esl = new EsgynMServerLocation(new String(lv_ba_table_name),
							       lv_sl);

	if (LOG.isTraceEnabled()) LOG.trace("Exit EsgynMServerLocation deserialize"
					    + ", location: " + lv_esl
					    );
	return lv_esl;

    }

    public byte[] serialize() {
	
	ByteBuffer lv_bb = ByteBuffer.allocate(512);

	lv_bb.putInt(this.getTableName().getBytes().length);
	lv_bb.put(this.getTableName().getBytes());

	lv_bb.putInt(this.getHostName().getBytes().length);
	lv_bb.put(this.getHostName().getBytes());

	lv_bb.putInt(this.getPort());

	lv_bb.putInt(this.getBucketId());

	lv_bb.putInt(this.getStartKey().length);
	if (LOG.isTraceEnabled()) LOG.trace("EsgynMServerLocation serialize" 
					    + ", startKey: " + new String(this.getStartKey())
					    + ", startKeyLen: " + this.getStartKey().length
					    + ", endKeyLen: " + this.getEndKey().length
					    );
	if (this.getStartKey().length > 0) {
	    lv_bb.put(this.getStartKey());
	}

	lv_bb.putInt(this.getEndKey().length);
	if (this.getEndKey().length > 0) {
	    lv_bb.put(this.getEndKey());
	}

	return lv_bb.array();
    }

    public boolean isTableRecodedDropped() 
    {
	//TBD
	return false;
    }
    
    public ServerName getServerName()
    {
	return ServerName.valueOf(getHostName(),
				  getPort(),
				  MonarchConstants.MONARCH_STORAGE_ENGINE_CODE);
    }

    @Override
    public String toString() 
    {
	
	StringBuilder lv_return = new StringBuilder();
	
	lv_return.append(m_table_name);
	lv_return.append(":");
	lv_return.append(m_server_location.toString());

	return lv_return.toString();
    }

}
