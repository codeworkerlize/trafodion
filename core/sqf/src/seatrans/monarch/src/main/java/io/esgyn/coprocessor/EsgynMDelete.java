package io.esgyn.coprocessor;

import io.ampool.monarch.table.*;

import org.apache.hadoop.hbase.client.Durability;

public class EsgynMDelete extends Delete implements java.io.Serializable {

    Durability m_durability = Durability.USE_DEFAULT;

    public EsgynMDelete(byte[] rowKey) {
	super(rowKey);
    }

    long getTimeStamp() {
	return super.getTimestamp();
    }

    void setTimeStamp(long p_timestamp) {
	super.setTimestamp(p_timestamp);
    }

    byte[] getRow() {
	return super.getRowKey();
    }

    public void setDurability(Durability d) {
	m_durability = d;
    }

    public Durability getDurability() {
	return m_durability;
    }

    public String toString() {
	return (" delete rowKey: " + new String(super.getRowKey()));

    }
}
