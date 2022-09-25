package org.apache.hadoop.hbase.pit.meta;

import org.apache.hadoop.hbase.pit.SnapshotMetaRecordType;

/**
 * create 2020.10.29
 * @author lh,hjw
 * With tag_ [tagName] is used as a rowkey for the backup tag record index
 * eg: tag_pit_incr1_00212470526059308556  ->  tag_pit_incr1_00212470526059308556,100,8,1080310833231899,1080310833273500,1080310833285300
 */
public class SnapshotTagIndexRecord extends AbstractSnapshotRecordMeta<String> {
    /**
     * Record types stored in HBase
     */
    private static final int SNAPSHOT_META_RECORD_TYPE = SnapshotMetaRecordType.SNAPSHOT_TAG_INDEX_RECORD.getValue();
    /**
     * The number of index timestamps in HBase storage value
     */
    private static final int FIELD_MIN_SIZE = 5;
    /**
     * Determine the startrowkey of scan. The value is snapshot metastartrecord key
     */
    private long startKey;
    /**
     * Determine the endrowkey of scan. The value is snapshot metaendrecord key
     */
    private long endKey = -1L;

    public static final String TAG_PREFIX = "tag_";

    private static final String TAG_SUFFIX = "_index";
    private static final String TAG_COMPLETE_PREFIX = "tag_index_";
    /**
     * @see org.apache.hadoop.hbase.util.Bytes#toLong(byte[])
     */
    private static final int EXPLAIN_WRONG_LENGTH_OR_OFFSET = 8;

    public SnapshotTagIndexRecord() { }

    public SnapshotTagIndexRecord(String column) {
        String[] fieldArray = column.split(SEPARATOR);
        // check value size
        super.verify(fieldArray.length >= FIELD_MIN_SIZE, "column size is not wrong");

        super.key = fieldArray[0];
        super.version = Integer.parseInt(fieldArray[1]);
        super.recordType = Integer.parseInt(fieldArray[2]);
        // check record type
        super.verify(SNAPSHOT_META_RECORD_TYPE == recordType, "column type is wrong");
        //Assign startKey to improve scan efficiency
        this.startKey = Long.parseLong(fieldArray[3]);
        this.endKey = Long.parseLong(fieldArray[4]);
    }

    public SnapshotTagIndexRecord(String key, int version, long startKey, long endKey) {
        super(getRowKey(key), version, SNAPSHOT_META_RECORD_TYPE);
        this.startKey = startKey;
        this.endKey = endKey;
    }

    @Override
    public String getColumn() {
        return super.getFieldString()
                .concat(SEPARATOR)
                .concat(String.valueOf(startKey))
                .concat(SEPARATOR)
                .concat(String.valueOf(endKey));
    }

    public long getStartKey() {
        return startKey;
    }

    public void setStartKey(long startKey) {
        this.startKey = startKey;
    }

    public long getEndKey() {
        return endKey;
    }

    public void setEndKey(long endKey) {
        this.endKey = endKey;
    }

    @Override
    public String toString() {
        return "SnapshotTagIndexRecord{" +
                "startKey=" + startKey +
                ", endKey=" + endKey +
                ", key=" + key +
                ", version=" + version +
                ", recordType=" + recordType +
                '}';
    }

    /**
     * Key prefix increases corresponding
     * @param tagName t
     * @return tagName -> tag_tagName
     */
    public static String getRowKey(String tagName){
        // For these types of tags, no index update is performed
        boolean isNotAdd = tagName.endsWith("_SAVE_") ||
                tagName.endsWith("_TM_") ||
                tagName.endsWith("_BRC_") ||
                tagName.endsWith("_RESTORED_") ||
                tagName.startsWith("SNAPSHOT_PEER_");
        if (isNotAdd) {
            return null;
        }
        return TAG_COMPLETE_PREFIX.concat(tagName);
    }

    /**
     * Key prefix increases corresponding
     * @param tagName t
     * @return old version index rowkey
     */
    public static String getOldRowKey(String tagName){
        // For these types of tags, no index update is performed
        boolean isNotAdd = tagName.endsWith("_SAVE_") ||
                tagName.endsWith("_TM_") ||
                tagName.endsWith("_BRC_") ||
                tagName.endsWith("_RESTORED_") ||
                tagName.startsWith("SNAPSHOT_PEER_");
        if (isNotAdd) {
            return null;
        }
        return tagName.length() < EXPLAIN_WRONG_LENGTH_OR_OFFSET ?
                TAG_PREFIX.concat(tagName).concat(TAG_SUFFIX) : TAG_PREFIX.concat(tagName);
    }

    /**
     * all tag index have 'tag_' prefix, someone has '_index' suffix
     * @param rowKey HBase RowKey
     * @return tagName,eg: tag_a_index -> a, tag_fullbk_00212475554334837709 -> fullbk_00212475554334837709
     */
    public static String getTagName(String rowKey) {
        if (AbstractSnapshotRecordMeta.isEmpty(rowKey)) { return null; }
        if (rowKey.startsWith(TAG_COMPLETE_PREFIX)) {
            return rowKey.substring(TAG_COMPLETE_PREFIX.length());
        }
        if (rowKey.startsWith(TAG_PREFIX)) {
            return rowKey.substring(TAG_PREFIX.length());
        }
        return null;
    }
}
