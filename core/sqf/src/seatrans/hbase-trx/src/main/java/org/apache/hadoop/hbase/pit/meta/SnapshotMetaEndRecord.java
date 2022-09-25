package org.apache.hadoop.hbase.pit.meta;

import org.apache.hadoop.hbase.pit.SnapshotMetaRecordType;

/**
 * create 2020.10.29
 * @author lh,hjw
 * HBase column value, RowKey is @see SnapshotMetaStartRecord completionTime
 * Used to mark the end of a backup to facilitate scan speed of 
 * scan (startrowkey and endrowkey are indeed)
 */
public class SnapshotMetaEndRecord extends AbstractSnapshotRecordMeta<Long> {
    /**
     * constructor methods SnapshotMetaEndRecord(String column)
     * verify column spilt by AbstractSnapshotRecordMeta SEPARATOR length
     */
    private static final int FIELD_MIN_SIZE = 5;
    /**
     * HBase Column type, define by SnapshotMetaRecordType
     * verify in constructor methods SnapshotMetaEndRecord(String column)
     */
    private static final int SNAPSHOT_META_RECORD_TYPE = SnapshotMetaRecordType.SNAPSHOT_END_RECORD.getValue();
    /**
     * @see SnapshotMetaStartRecord key
     */
    private long tagStartKey;
    /**
     * @see SnapshotMetaStartRecord userTag
     */
    private String userTag;

    public SnapshotMetaEndRecord() {
    }

    public SnapshotMetaEndRecord(String column) {
        String[] fieldArray = column.split(SEPARATOR);
        // check value size
        super.verify(fieldArray.length >= FIELD_MIN_SIZE, "column size is not wrong");

        super.key = Long.parseLong(fieldArray[0]);
        super.version = Integer.parseInt(fieldArray[1]);
        super.recordType = Integer.parseInt(fieldArray[2]);
        // check record type
        super.verify(SNAPSHOT_META_RECORD_TYPE == recordType, "column type is wrong");

        this.tagStartKey = Long.parseLong(fieldArray[3]);
        this.userTag = fieldArray[4];
    }

    public SnapshotMetaEndRecord(long key, int version, long tagStartKey, String userTag) {
        super(key, version, SNAPSHOT_META_RECORD_TYPE);
        this.tagStartKey = tagStartKey;
        this.userTag = userTag;
    }

    @Override
    public String getColumn() {
        return super.getFieldString()
                .concat(SEPARATOR)
                .concat(String.valueOf(tagStartKey))
                .concat(SEPARATOR)
                .concat(String.valueOf(userTag));
    }

    public long getTagStartKey() {
        return tagStartKey;
    }

    public void setTagStartKey(long tagStartKey) {
        this.tagStartKey = tagStartKey;
    }

    public String getUserTag() {
        return userTag;
    }

    public void setUserTag(String userTag) {
        this.userTag = userTag;
    }

    @Override
    public String toString() {
        return "SnapshotMetaEndRecord{" +
                "tagStartKey=" + tagStartKey +
                ", userTag='" + userTag + '\'' +
                ", key=" + key +
                ", version=" + version +
                ", recordType=" + recordType +
                '}';
    }
}
