package org.apache.hadoop.hbase.pit.meta;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.pit.SnapshotMetaRecordType;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author hjw
 * @date 2021/01/07
 */
public class SnapshotMetaRecord extends AbstractSnapshotRecordMeta<Long> {
    static final Log LOG = LogFactory.getLog(SnapshotMetaRecord.class);
    /**
     * constructor methods SnapshotMetaEndRecord(String column)
     * verify column spilt by AbstractSnapshotRecordMeta SEPARATOR length
     *  // TODO size need change
     */
    private static final int FIELD_MIN_SIZE = 5;
    /**
     * HBase Column type, define by SnapshotMetaRecordType
     * verify in constructor methods SnapshotMetaEndRecord(String column)
     */
    private static final int SNAPSHOT_META_RECORD_TYPE = SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue();

    private long supersedingSnapshot;
    private long restoredSnapshot;
    private long restoreStartTime;
    private long hiatusTime;
    private String tableName;
    private String userTag;
    private String snapshotName;
    private String snapshotPath;
    private boolean userGenerated;
    private boolean incrementalTable;
    private boolean sqlMetaData;
    private boolean inLocalFs;
    private boolean excluded;
    private long mutationStartTime;
    private final String archivePath;
    private String metaStatements;
    private int numLobs;
    private Set<Long> lobSnapshots;
    private Set<Long> dependentSnapshots;

    public SnapshotMetaRecord(String column) throws IOException {
        String[] fieldArray = column.split(SEPARATOR);
        // check value size
        verify(fieldArray.length >= FIELD_MIN_SIZE, "column size is not wrong");

        super.initField(Long.parseLong(fieldArray[0]), fieldArray[1], fieldArray[2]);
        // check record type
        verify(SNAPSHOT_META_RECORD_TYPE == recordType, "column type is wrong");
        this.supersedingSnapshot = Long.parseLong(fieldArray[3], 10);
        this.restoredSnapshot = Long.parseLong(fieldArray[4], 10);
        this.restoreStartTime = Long.parseLong(fieldArray[5], 10);
        this.hiatusTime = Long.parseLong(fieldArray[6], 10);
        this.tableName = fieldArray[7];
        this.userTag = fieldArray[8];
        this.snapshotName = fieldArray[9];
        this.snapshotPath = fieldArray[10];
        this.userGenerated = Boolean.parseBoolean(fieldArray[11]);
        this.incrementalTable = Boolean.parseBoolean(fieldArray[12]);
        this.sqlMetaData = Boolean.parseBoolean(fieldArray[13]);
        this.inLocalFs = Boolean.parseBoolean(fieldArray[14]);
        this.excluded = Boolean.parseBoolean(fieldArray[15]);
        this.mutationStartTime = fieldArray[16].contains("false") ? 0L : Long.parseLong(fieldArray[16], 10);
        this.archivePath = fieldArray[17];
        this.metaStatements = fieldArray[18];

        this.numLobs = Integer.parseInt(fieldArray[19]);
        List<String> fieldList = new ArrayList<>(Arrays.asList(fieldArray));

        // Can be replaced with lambda expressions, but EsgynDB does not support JDK1.8
        Set<Long> lobSnapshotSet = new HashSet<>(numLobs);
        for (String str : fieldList.subList(20, 20 + numLobs)) {
            lobSnapshotSet.add(Long.parseLong(str));
        }
        this.lobSnapshots = lobSnapshotSet;
        Set<Long> dependentSnapshotSet = new HashSet<>(numLobs);
        for (String str : fieldList.subList(21 + numLobs, fieldList.size())) {
            lobSnapshotSet.add(Long.parseLong(str));
        }
        this.dependentSnapshots = dependentSnapshotSet;
    }

    @Override
    public String getColumn() {
        return super.getFieldString() + SEPARATOR + this.supersedingSnapshot
                + SEPARATOR + this.restoredSnapshot
                + SEPARATOR + this.restoreStartTime
                + SEPARATOR + this.hiatusTime
                + SEPARATOR + this.tableName
                + SEPARATOR + this.userTag
                + SEPARATOR + this.snapshotName
                + SEPARATOR + this.snapshotPath
                + SEPARATOR + this.userGenerated
                + SEPARATOR + this.incrementalTable
                + SEPARATOR + this.sqlMetaData
                + SEPARATOR + this.inLocalFs
                + SEPARATOR + this.excluded
                + SEPARATOR + this.mutationStartTime
                + SEPARATOR + this.archivePath
                + SEPARATOR + this.metaStatements
                + SEPARATOR + this.numLobs
                + SEPARATOR + join(this.lobSnapshots, SEPARATOR)
                + SEPARATOR + join(this.dependentSnapshots, SEPARATOR);
    }

    @Override
    public String toString() {
        return "SnapshotMetaRecord{" +
                "supersedingSnapshot=" + supersedingSnapshot +
                ", restoredSnapshot=" + restoredSnapshot +
                ", restoreStartTime=" + restoreStartTime +
                ", hiatusTime=" + hiatusTime +
                ", tableName='" + tableName + '\'' +
                ", userTag='" + userTag + '\'' +
                ", snapshotName='" + snapshotName + '\'' +
                ", snapshotPath='" + snapshotPath + '\'' +
                ", userGenerated=" + userGenerated +
                ", incrementalTable=" + incrementalTable +
                ", sqlMetaData=" + sqlMetaData +
                ", inLocalFs=" + inLocalFs +
                ", excluded=" + excluded +
                ", mutationStartTime=" + mutationStartTime +
                ", archivePath='" + archivePath + '\'' +
                ", metaStatements='" + metaStatements + '\'' +
                ", numLobs=" + numLobs +
                ", lobSnapshots=" + lobSnapshots +
                ", dependentSnapshots=" + dependentSnapshots +
                ", key=" + key +
                ", version=" + version +
                ", recordType=" + recordType +
                '}';
    }

    public long getSupersedingSnapshot() {
        return supersedingSnapshot;
    }

    public long getRestoredSnapshot() {
        return restoredSnapshot;
    }

    public long getRestoreStartTime() {
        return restoreStartTime;
    }

    public long getHiatusTime() {
        return hiatusTime;
    }

    public String getTableName() {
        return tableName;
    }

    public String getUserTag() {
        return userTag;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getSnapshotPath() {
        return snapshotPath;
    }

    public boolean isUserGenerated() {
        return userGenerated;
    }

    public boolean isIncrementalTable() {
        return incrementalTable;
    }

    public boolean isSqlMetaData() {
        return sqlMetaData;
    }

    public boolean isInLocalFs() {
        return inLocalFs;
    }

    public boolean isExcluded() {
        return excluded;
    }

    public long getMutationStartTime() {
        return mutationStartTime;
    }

    public String getArchivePath() {
        return archivePath;
    }

    public String getMetaStatements() {
        return metaStatements;
    }

    public int getNumLobs() {
        return numLobs;
    }

    public Set<Long> getLobSnapshots() {
        return lobSnapshots;
    }

    public Set<Long> getDependentSnapshots() {
        return dependentSnapshots;
    }
}
