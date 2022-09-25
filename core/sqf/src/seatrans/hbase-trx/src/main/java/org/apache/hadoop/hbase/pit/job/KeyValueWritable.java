package org.apache.hadoop.hbase.pit.job;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author hjw
 */
public class KeyValueWritable implements WritableComparable<KeyValueWritable> {
    private Cell value;

    public KeyValueWritable() {}

    public KeyValueWritable(Cell value) {
        set(value);
    }

    public Cell get() { return value; }

    public void set(Cell value) { this.value = value; }

    @Override
    public int compareTo(KeyValueWritable o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(CellUtil.cloneValue(value));
    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
