package org.apache.hadoop.hbase.coprocessor.transactional.lock;

import lombok.Getter;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.codec.binary.Hex;

public class RowKey implements Comparable<RowKey>, LockSizeof {
    @Getter
    private final byte[] data;
    private int hashCode = -1;

    public RowKey(byte[] data) {
        this.data = data;
    }

    public RowKey(String hexData) {
        this.data = StringUtil.hexToByteArray(hexData);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof RowKey)) {
            return false;
        }
        if (data == null) {
            if (((RowKey) other).data == null) {
                return true;
            } else {
                return false;
            }
        } else if (((RowKey) other).data == null) {
            return false;
        }
        return Bytes.equals(this.data,((RowKey)other).data);
    }

    @Override
    public int hashCode() {
        if (hashCode == -1) {
            hashCode = Bytes.hashCode(data);
        }
        return hashCode;
    }

    @Override
    public String toString() {
        if (data == null || data.length == 0) {
            return "";
        }
        StringBuilder rowID = new StringBuilder(data.length * 2);
        for(byte rowKyeByte : data) {
            rowID.append(LockConstants.HEX_CHAR[(rowKyeByte >>> 4) & 0xf]);
            rowID.append(LockConstants.HEX_CHAR[rowKyeByte & 0xf]);

        }
        return rowID.toString();
    }

    @Override
    public int compareTo(RowKey other) {
        return Bytes.compareTo(this.data, other.data);
    }

    @Override
    public long sizeof() {
        long size = Size_Object;
        //2 attributes
        size += 2 * Size_Reference;
        //1 int
        size += 4;
        //data
        if (data != null) {
            size += Size_Array + data.length;
        }

        return LockUtils.alignLockObjectSize(size);
    }
}
