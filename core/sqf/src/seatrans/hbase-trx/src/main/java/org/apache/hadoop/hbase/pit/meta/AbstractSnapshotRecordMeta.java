package org.apache.hadoop.hbase.pit.meta;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hbase.pit.SnapshotMetaRecordType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * create 2020.10.29
 * @author lh,hjw
 */
public abstract class AbstractSnapshotRecordMeta<T> implements Serializable {
    static final Log LOG = LogFactory.getLog(AbstractSnapshotRecordMeta.class);
    private static final long serialVersionUID = -6864370808895151978L;

    /**
     * HBase Column value   [key],[version],[recordType]
     */
    protected static final String SEPARATOR = ",";
    /**
     * HBase Column RowKey, tag is tagName, table is table name, else timestamp
     */
    protected T key = null;
    /**
     * @link SnapshotMetaRecordType.version
     */
    protected int version;
    /**
     * HBase Column type, define by SnapshotMetaRecordType
     */
    protected int recordType;

    public static final String EMPTY = "";


    public AbstractSnapshotRecordMeta() {}

    public AbstractSnapshotRecordMeta(T key, int version, int recordType) {
        this.key = key;
        this.version = version;
        this.recordType = recordType;
    }

    public AbstractSnapshotRecordMeta(String column) {
        String[] fieldArray = column.split(",");
        this.version = Integer.parseInt(fieldArray[1]);
        this.recordType = Integer.parseInt(fieldArray[2]);
    }

    public static SnapshotMetaRecordType getRecordTypeByValue(String column) {
        String[] fieldArray = column.split(",");
        if (!SnapshotMetaRecordType.getSnapshotMetaTypeMap().containsKey(Integer.parseInt(fieldArray[2]))) {
            throw new IllegalArgumentException("column has illegal type ");
        }
        return SnapshotMetaRecordType.getSnapshotMetaTypeMap().get(Integer.parseInt(fieldArray[2]));
    }

    protected void initField(T key, String version, String recordType) throws IOException {
        this.key = key;
        this.version = Integer.parseInt(version);
        if (this.version != SnapshotMetaRecordType.getVersion()){
            IOException ioe = new IOException("Unexpected record version found: "
                    + this.version + " expected: " + SnapshotMetaRecordType.getVersion());
            LOG.error("initField exception ", ioe);
            throw ioe;
        }
        this.recordType = Integer.parseInt(recordType);
    }
    /**
     * attribute string which is concatenated by separator
     * @return attribute string
     */
    public abstract String getColumn();

    /**
     * mkString all field by @see SEPARATOR
     * @return eg: [key],[version],[recordType]
     */
    public String getFieldString() {
        return String.valueOf(key)
                .concat(SEPARATOR)
                .concat(String.valueOf(version))
                .concat(SEPARATOR)
                .concat(String.valueOf(recordType));
    }

    /**
     * isRight is false, throw new IllegalArgumentException
     * @param isRight isRight
     * @param exceptionInfo exceptionInfo
     */
    public void verify(boolean isRight, String exceptionInfo) {
        if (!isRight) {
            throw new IllegalArgumentException(exceptionInfo);
        }
    }

    public T getKey() {
        return key;
    }

    public void setKey(T key) {
        this.key = key;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getRecordType() {
        return recordType;
    }

    public void setRecordType(int recordType) {
        this.recordType = recordType;
    }

    /**
     * There is a compilation problem with the stringutils class
     * Judge whether the corresponding string is empty
     * @param cs: String
     * @return if true, cs is not empty
     */
    public static boolean isNotEmpty(CharSequence cs) {
        return !isEmpty(cs);
    }
    /**
     * There is a compilation problem with the stringutils class
     * Judge whether the corresponding string is empty
     * @param cs: String
     * @return if true, cs is empty
     */
    public static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    /**
     * There is a compilation problem with the stringutils class
     * Concatenate strings with separators
     * @param iterable
     * @param separator
     * @return
     */
    public static String join(Iterable<?> iterable, String separator) {
        if (iterable == null) {
            return null;
        }
        return join(iterable.iterator(), separator);
    }
    /**
     * There is a compilation problem with the stringutils class
     * Concatenate strings with separators
     * @param iterator
     * @param separator
     * @return
     */
    public static String join(Iterator<?> iterator, String separator) {

        // handle null, zero and one elements before building a buffer
        if (iterator == null) {
            return null;
        }
        if (!iterator.hasNext()) {
            return EMPTY;
        }
        Object first = iterator.next();
        if (!iterator.hasNext()) {
            return ObjectUtils.toString(first);
        }

        // two or more elements
        // Java default is 16, probably too small
        StringBuilder buf = new StringBuilder(256);
        if (first != null) {
            buf.append(first);
        }

        while (iterator.hasNext()) {
            if (separator != null) {
                buf.append(separator);
            }
            Object obj = iterator.next();
            if (obj != null) {
                buf.append(obj);
            }
        }
        return buf.toString();
    }

    /**
     * Checks if the CharSequence contains only Unicode digits.
     * @param cs
     * @return
     */
    public static boolean isNumeric(CharSequence cs) {
        if (cs == null || cs.length() == 0) {
            return false;
        }
        int sz = cs.length();
        for (int i = 0; i < sz; i++) {
            if (Character.isDigit(cs.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }
}
