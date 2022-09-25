package org.trafodion.sql.parquet;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.example.data.simple.*;

import org.apache.parquet.example.data.Group;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import org.trafodion.sql.TrafParquetFileReader;

import org.trafodion.sql.parquet.TrafRecordConverter;

public class TrafGroup extends Group {

    private final GroupType schema;

    private Object[] data = null;

    private TrafParquetFileReader m_tpfr = null;
    private TrafRecordConverter m_record_converter;

    private boolean b_populateBB = false;

    private int m_num_filled_bytes = 0;

    private int m_num_columns = 0;
    private int m_col_num_last_added = -1;
 
    @SuppressWarnings("unchecked")
    public TrafGroup(GroupType schema) {
	this.schema = schema;
	
	m_record_converter = null;
	m_num_columns = schema.getFields().size();
    }

    public void init(TrafRecordConverter p_record_converter) {
	m_col_num_last_added = -1;
	m_tpfr = null;

	m_record_converter = p_record_converter;
	if (m_record_converter != null) {
	    m_tpfr = m_record_converter.getTPFR();
	}

	if (m_tpfr == null) {
	    if (this.data == null) {
		this.data = new Object[m_num_columns];
	    }
	    else {
		for (int i = 0; i < m_num_columns; i++) {
		    this.data[i] = null;
		}
	    }
	}
	else {
	    b_populateBB = true;
	    m_tpfr.initRow();
	}
    }

    public void end() {
	//System.out.println("TG.end");
	if (b_populateBB) {
	    /*
	    System.out.println("TG.end - populateBB"
			       + ", m_num_columns: " + m_num_columns
			       + ", m_col_num_last_added: " + m_col_num_last_added
			       );
	    */
	    fillNull(m_num_columns);
	    m_num_filled_bytes = m_tpfr.endRow();
	}
    }

    public int getNumFilledBytes() {
	return m_num_filled_bytes;
    }

    @Override
    public String toString() {
	return toString("");
    }

    public String toString(String indent) {
	String result = "";
	int i = 0;
	for (Type field : schema.getFields()) {
	    String name = field.getName();
	    result += indent + name;
	    if (! b_populateBB) {
		Object value = data[i];
		++i;
		if (value == null) {
		    result += ": NULL\n";
		} else if (value instanceof Group) {
		    result += "\n" + ((TrafGroup)value).toString(indent+"  ");
		} else {
		    result += ": " + value.toString() + "\n";
		}
	    }
	}

	return result;
    }

    @Override
    public TrafGroup addGroup(int fieldIndex) {
	TrafGroup g = new TrafGroup(schema.getType(fieldIndex).asGroupType());
	add(fieldIndex, g);
	return g;
    }

    @Override
    public Group getGroup(int fieldIndex, int index) {
	return (Group)getValue(fieldIndex, index);
    }

    private Object getValue(int fieldIndex, int index) {
	//      System.out.println("tg.getValue(), fld_idx: " + fieldIndex + ", idx: " + index);
	return data[fieldIndex];
    }

    private void add(int fieldIndex, Primitive value) {
	/*
	  System.out.println("TG.add"
			     + ", fldIndex: " + fieldIndex
			     + ", value: " + value
			     + ", value class: " + value.getClass().getName()
			     + ", recordConverter: " + m_record_converter
			     + ", tpfr: " + m_tpfr
			     );
	*/
	data[fieldIndex] = value;
    }

    @Override
    public int getFieldRepetitionCount(int fieldIndex) {
	return data[fieldIndex] == null ? 0 : 1;
    }

    @Override
    public String getValueToString(int fieldIndex, int index) {
	return String.valueOf(getValue(fieldIndex, index));
    }

    @Override
    public String getString(int fieldIndex, int index) {
	return ((BinaryValue)getValue(fieldIndex, index)).getString();
    }

    @Override
    public int getInteger(int fieldIndex, int index) {
	return ((IntegerValue)getValue(fieldIndex, index)).getInteger();
    }

    @Override
    public long getLong(int fieldIndex, int index) {
	return ((LongValue)getValue(fieldIndex, index)).getLong();
    }

    @Override
    public double getDouble(int fieldIndex, int index) {
	return ((DoubleValue)getValue(fieldIndex, index)).getDouble();
    }

    @Override
    public float getFloat(int fieldIndex, int index) {
	return ((FloatValue)getValue(fieldIndex, index)).getFloat();
    }

    @Override
    public boolean getBoolean(int fieldIndex, int index) {
	return ((BooleanValue)getValue(fieldIndex, index)).getBoolean();
    }

    @Override
    public Binary getBinary(int fieldIndex, int index) {
	return ((BinaryValue)getValue(fieldIndex, index)).getBinary();
    }

    public NanoTime getTimeNanos(int fieldIndex, int index) {
	return NanoTime.fromInt96((Int96Value)getValue(fieldIndex, index));
    }

    @Override
    public Binary getInt96(int fieldIndex, int index) {
	return ((Int96Value)getValue(fieldIndex, index)).getInt96();
    }

    public void fillNull(int pv_curr_field_index) {
	for (int i = m_col_num_last_added + 1; i < pv_curr_field_index; i++) {
	    m_tpfr.fillNull();
	}
	m_col_num_last_added = pv_curr_field_index;
    }

    @Override
    public void add(int fieldIndex, int value) {
	/*
	System.out.println("add(int,int)" 
			   + ", index: " + fieldIndex
			   + ", val: " + value
			   );
	*/
	if (b_populateBB) {
	    fillNull(fieldIndex);
	    m_tpfr.fillInt(fieldIndex, value);
	}
	else {
	    add(fieldIndex, new IntegerValue(value));
	}
    }

    @Override
    public void add(int fieldIndex, long value) {
	if (b_populateBB) {
	    fillNull(fieldIndex);
	    m_tpfr.fillLong(fieldIndex, value);
	}
	else {
	    add(fieldIndex, new LongValue(value));
	}
    }

    @Override
    public void add(int fieldIndex, String value) {
	/*
	System.out.println("add"
			   + ", field index: " + fieldIndex
			   + ", value(String): " + value
			   );
	*/
	if (b_populateBB) {
	    fillNull(fieldIndex);
	}
	else {
	    add(fieldIndex, new BinaryValue(Binary.fromString(value)));
	}
    }

    @Override
    public void add(int fieldIndex, NanoTime value) {
	if (b_populateBB) {
	    fillNull(fieldIndex);
	}
	else {
	    add(fieldIndex, value.toInt96());
	}
    }

    @Override
    public void add(int fieldIndex, boolean value) {
	if (b_populateBB) {
	    fillNull(fieldIndex);
	    m_tpfr.fillBoolean(value);
	}
	else {
	    add(fieldIndex, new BooleanValue(value));
	}
    }

    @Override
    public void add(int fieldIndex, Binary value) {
	//System.out.println("TG Binary:" + fieldIndex + ", val:" + value);
	switch (getType().getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName()) {
	case BINARY:
	    if (b_populateBB) {
		fillNull(fieldIndex);
		m_tpfr.fillBinary(value, fieldIndex);
	    }
	    else { 
		add(fieldIndex, new BinaryValue(value));
	    }
	    break;

	case FIXED_LEN_BYTE_ARRAY:
	    if (b_populateBB) {
		fillNull(fieldIndex);
		m_tpfr.fillDecimalWithBinary(value, fieldIndex);
	    }
	    else { 
		add(fieldIndex, new BinaryValue(value));
	    }
	    break;

	case INT96:
	    if (b_populateBB) {
		fillNull(fieldIndex);
		m_tpfr.fillTimestampwithBinary(value);
	    }
	    else {
		add(fieldIndex, new Int96Value(value));
	    }
	    break;
	default:
	    throw new UnsupportedOperationException(
						    getType().asPrimitiveType().getName() + " not supported for Binary");
	}
    }

    @Override
    public void add(int fieldIndex, float value) {
	if (b_populateBB) {
	    fillNull(fieldIndex);
	    m_tpfr.fillFloat(value);
	}
	else {
	    add(fieldIndex, new FloatValue(value));
	}
    }

    @Override
    public void add(int fieldIndex, double value) {
	if (b_populateBB) {
	    fillNull(fieldIndex);
	    m_tpfr.fillDouble(value);
	}
	else {
	    add(fieldIndex, new DoubleValue(value));
	}
    }

    @Override
    public void add(int fieldIndex, Group value) {
	if (b_populateBB) {
	    fillNull(fieldIndex);
	}
	else {
	    data[fieldIndex] = value;
	}
    }

    @Override
    public GroupType getType() {
	return schema;
    }

    @Override
    public void writeValue(int field, int index, RecordConsumer recordConsumer) {
	((Primitive)getValue(field, index)).writeValue(recordConsumer);
    }

}
