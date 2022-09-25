package org.apache.hadoop.hbase.pit.meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.pit.SnapshotMeta;
import org.apache.hadoop.hbase.pit.SnapshotMetaRecordType;

import java.util.*;

/**
 * create 2020.10.29
 * @author lh,hjw
 * In table_ [tablename] is used as a rowkey for the regular backup record index of the table
 * eg: table_TRAFODION.GQ.NEWTABLE1 -> table_TRAFODION.GQ.NEWTABLE1,100,8,1080310833231899,1080310833273500,1080310833285300
 */
public class SnapshotTableIndexRecord extends AbstractSnapshotRecordMeta<String> {
    static final Log LOG = LogFactory.getLog(SnapshotTableIndexRecord.class);
    /**
     * Record types stored in HBase
     */
    private static final int SNAPSHOT_META_RECORD_TYPE = SnapshotMetaRecordType.SNAPSHOT_TABLE_INDEX_RECORD.getValue();

    public static final int INCR_TYPE = 0;

    public static final int NORMAL_TYPE = 1;
    /**
     * The number of index timestamps in HBase storage value
     */
    private static final int CACHE_SIZE = 5;
    /**
     * constructor methods SnapshotMetaEndRecord(String column)
     * verify column spilt by AbstractSnapshotRecordMeta SEPARATOR length
     */
    private static final int FIELD_MIN_SIZE = 4;
    /**
     * Specifies the latest CACHE_SIZE history regular backup queue for tablename
     */
    private final LinkedList<Long> snapShots = new LinkedList<>();
    /**
     * Specifies the latest CACHE_SIZE history incremental  backup queue for tablename
     */
    private final LinkedList<Long> incrSnapshots = new LinkedList<>();
    /**
     * Prefix of all HBase rowkeys in this record
     */
    private static final String TABLE_PREFIX = "table_";
    /**
     * It is used to segment incremental records and full records in HBase records
     */
    private static final String INCR_SEPARATOR = "#";

    /**
     * The metadata table contains the tag name, which is replaced
     */
    private static final String MD_REGEX = "\\._BACKUP_.+\\.";


    public SnapshotTableIndexRecord(String column) {
        String[] fieldArray = column.split(SEPARATOR);
        List<String> fieldList = Arrays.asList(fieldArray);
        // check value size
        super.verify(fieldArray.length >= FIELD_MIN_SIZE, "column size is not wrong");

        super.key = fieldArray[0];
        super.version = Integer.parseInt(fieldArray[1]);
        super.recordType = Integer.parseInt(fieldArray[2]);
        // check record type
        super.verify(SNAPSHOT_META_RECORD_TYPE == recordType, "column type is wrong");

        String snapshotRootKeyStr = String.join(SEPARATOR, fieldList.subList(3, fieldList.size()));
        // Two queues are divided by INCR_SEPARATOR
        String[] rootKeyArray = snapshotRootKeyStr.split(INCR_SEPARATOR);
        this.str2queue(rootKeyArray.length > 1 ? rootKeyArray[1] : null);
        this.strToIncrQueue(rootKeyArray[0]);
    }

    public SnapshotTableIndexRecord(String key, int version, String snapshotRootKeys, String incrSnapshotRootKeys) {
        super(getRowKey(key), version, SNAPSHOT_META_RECORD_TYPE);
        str2queue(snapshotRootKeys);
        this.strToIncrQueue(incrSnapshotRootKeys);
    }

    /**
     * Convert the string separated by [separator] into 
     * queue and the number of them does not exceed CACHE_SIZE
     * @param snapshotRootKeys General snapshot timestamp string
     */
    private void str2queue(String snapshotRootKeys) {
        if (isNotEmpty(snapshotRootKeys)) {
            for (String s : snapshotRootKeys.split(SEPARATOR)) {
                add(Long.parseLong(s), NORMAL_TYPE);
            }
        }
    }

    /**
     * Converts a string separated by r] to a qeue
     * @param snapshotRootKeys Incremental snapshot timestamp string
     */
    private void strToIncrQueue(String snapshotRootKeys) {
        if (isNotEmpty(snapshotRootKeys)) {
            for (String s : snapshotRootKeys.split(SEPARATOR)) {
                add(Long.parseLong(s), INCR_TYPE);
            }
        }
    }

    /**
     * Add elements to the queue to specify the queue
     * @param timestamp RowKey
     * @param type Queue type
     */
    public void add(long timestamp, int type) {
        switch (type) {
            case INCR_TYPE:
                add(incrSnapshots, timestamp);
                break;
            case NORMAL_TYPE:
                add(snapShots, timestamp);
                break;
        }
    }

    /**
     * Add elements to the queue to specify the queue
     * @param timestamp RowKey
     * @param type Queue type
     */
    public void add(long timestamp, int type, String tagName) {
        // For these types of tags, no index update is performed
        boolean isNotAdd = tagName.endsWith("_SAVE_") ||
                tagName.endsWith("_TM_") ||
                tagName.endsWith("_BRC_") ||
                tagName.endsWith("_RESTORED_") ||
                tagName.startsWith("SNAPSHOT_PEER_");

        if(isNotAdd) {
            LOG.warn("tag:" + tagName + "is not suit tag");
            return;
        }
        switch (type) {
            case INCR_TYPE:
                add(incrSnapshots, timestamp);
                break;
            case NORMAL_TYPE:
                add(snapShots, timestamp);
                break;
        }
    }

    /**
     * Add elements to the queue with no more than cache_ SIZE
     * @param timestamp RowKey
     */
    private void add(LinkedList<Long> list, long timestamp) {
        // This record exists in the index list, skipping
        if (list.contains(timestamp)) {
            return;
        }
        // The index pair column is too long, clear the early data
        if (list.size() >= CACHE_SIZE) {
            list.poll();
        }
        list.add(timestamp);
        Collections.sort(list);
    }
    /**
     * Converts the queue to a string separated by [separator]
     * @return str
     */
    public String getSnapshotRootKeysStr() {
        return join(snapShots, SEPARATOR);
    }

    /**
     * Converts the queue to a string separated by [separator]
     * @return str
     */
    public String getIncrSnapshotRootKeysStr() {
        return join(incrSnapshots, SEPARATOR);
    }
    @Override
    public String getColumn() {
        return super.getFieldString()
                .concat(SEPARATOR)
                .concat(getIncrSnapshotRootKeysStr())
                .concat(INCR_SEPARATOR)
                .concat(getSnapshotRootKeysStr());
    }

    public static String getRowKey(String tableName) {
        if (tableName.contains("LOBCHUNKS_")) {
            return null;
        }
        return TABLE_PREFIX.concat(replaceTagName(tableName));
    }

    public LinkedList<Long> getSnapShots() {
        return snapShots;
    }

    public LinkedList<Long> getIncrSnapshots() {
        return incrSnapshots;
    }

    /**
     * There are tagnames in some tablenames, which should be eliminated
     * @param tableName t
     * @return eg: tableName_tagName -> tableName
     */
    public static String replaceTagName(String tableName) {
        return isNotEmpty(tableName) ? tableName.replaceAll(MD_REGEX, ".") : tableName;
    }


    @Override
    public String toString() {
        return "SnapshotTableIndexRecord{" +
                "snapShots=" + snapShots +
                ", incrSnapshots=" + incrSnapshots +
                ", key=" + key +
                ", version=" + version +
                ", recordType=" + recordType +
                '}';
    }

}
