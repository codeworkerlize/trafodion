package io.esgyn.coprocessor;

import io.ampool.monarch.table.*;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EsgynMGet 
    implements java.io.Serializable 
{

    static final Logger LOG = LogManager.getLogger(EsgynMGet.class.getName());

    private byte[] m_row_key;
    private Long m_row_timestamp;
    private List<byte[]> m_columns;

    public EsgynMGet(byte[] p_row_key) {
	m_row_key = p_row_key;
	m_columns = new ArrayList<byte[]>();
    }

    public void addColumn(byte[] p_column) {
	addColumn(new String(p_column));
    }

    public void addColumn(String p_column) {
	boolean lv_found = false;

	ListIterator<byte[]> lv_it;
	for (lv_it = m_columns.listIterator();
	     lv_it.hasNext();) {
	    String lv_column_name = new String(lv_it.next());
	    if (p_column.compareTo(lv_column_name) == 0) {
		lv_found = true;
		break;
	    }
	}

	if (lv_found) {
	    if (LOG.isInfoEnabled()) LOG.info("EsgynMGet: addColumn: "
					      + ", column: " + p_column
					      + " already exists in the EsgynMGet object");
	    return;
	}

	m_columns.add(p_column.getBytes());
	
    }

    public byte[] getRowKey() {
	return m_row_key;
    }

    public void setRowTimeStamp(Long p_row_timestamp) {
	m_row_timestamp = p_row_timestamp;
    }

    public Long getRowTimeStamp() {
	return m_row_timestamp;
    }

    public List<byte[]> getColumnNameList() {
	return m_columns;
    }

    public String toString() {
	StringBuilder lv_s = new StringBuilder();
	lv_s.append("EsgynMGet [");
	lv_s.append("rowkey: " + new String(m_row_key));
	lv_s.append(", #cols: " + m_columns.size());

	lv_s.append(", col names:[");
	boolean lv_first = true;
	ListIterator<byte[]> lv_it;
	for (lv_it = m_columns.listIterator();
	     lv_it.hasNext();) {
	    if (!lv_first) {
		lv_s.append(", ");
	    }
	    lv_first = false;
	    lv_s.append(new String(lv_it.next()));
	}
	lv_s.append("]");

	lv_s.append("]");

	return lv_s.toString();
    }
}
