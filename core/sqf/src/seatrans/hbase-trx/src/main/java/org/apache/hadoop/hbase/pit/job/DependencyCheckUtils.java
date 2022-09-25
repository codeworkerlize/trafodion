package org.apache.hadoop.hbase.pit.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.pit.meta.AbstractSnapshotRecordMeta;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * @author hjw
 * @date 20210830
 */
public class DependencyCheckUtils {
    private static final Logger LOG = Logger.getLogger(DependencyCheckUtils.class);

    private final static String SEPARATOR_HDFS_DEPENDENCY = ",";

    private final static String CONF_DEPENDENCY_HDFS_PATH = "/tmp/br_restore/lib";
    private final static String CONF_ENV_TRAF_HOME_KEY = "TRAF_HOME";
    private final static String CONF_ENV_HBASE_HOME_KEY = "HBASE_HOME";
    private final static String CONF_ENV_HBASE_LIB_PATH_KEY = "HBASE_LIB_PATH";
    private final static String CONF_ENV_JAR_SUFFIX = "jar";

    public static void checkDependency(Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        // 上传目录不存在则创建
        if (!fs.exists(new Path(CONF_DEPENDENCY_HDFS_PATH))) {
            fs.mkdirs(new Path(CONF_DEPENDENCY_HDFS_PATH));
        }
        // 获取依赖，依赖包含hbase-trx,hbase-common,hbase-client,hbase-protocol,hbase-server
        // 先从环境变量中获取数据库安装路径
        String trafHomePath = System.getenv(CONF_ENV_TRAF_HOME_KEY);
        if (AbstractSnapshotRecordMeta.isEmpty(trafHomePath)) {
            throw new IllegalArgumentException("please set " + CONF_ENV_TRAF_HOME_KEY);
        }
        String dbDependencyPath = trafHomePath + "/export/lib";
        listAndUpload(fs, new File(dbDependencyPath), dbDependencyPath);

        // 检查HBase/Hadoop一系列相关依赖
        String hbasePath = System.getenv(CONF_ENV_HBASE_LIB_PATH_KEY);
        if (AbstractSnapshotRecordMeta.isEmpty(hbasePath)) {
            hbasePath = System.getenv(CONF_ENV_HBASE_HOME_KEY);
            if (AbstractSnapshotRecordMeta.isEmpty(hbasePath)) {
                throw new IllegalArgumentException("please set " + CONF_ENV_TRAF_HOME_KEY + "/" + CONF_ENV_HBASE_LIB_PATH_KEY);
            } else {
                hbasePath = hbasePath + "/lib";
            }
        }
        listAndUpload(fs, new File(hbasePath), hbasePath);
    }

    private static void listAndUpload(FileSystem fs, File file, String path) throws IOException {
        if (file == null || !file.exists() || file.isFile()) {
            throw new IllegalArgumentException("hbase dependency [" + path + "] error, please check it ");
        }
        for (String fileName : file.list()) {
            String localPath = path + File.separator + fileName;
            String filePath = CONF_DEPENDENCY_HDFS_PATH + File.separator + fileName;
            // copy to hdfs
            if (fileName.endsWith(CONF_ENV_JAR_SUFFIX) && !fs.exists(new Path(filePath))) {
                LOG.warn("upload file:" + localPath);
                fs.copyFromLocalFile(new Path(localPath), new Path(CONF_DEPENDENCY_HDFS_PATH));
            }
        }
    }

    public static String getDependencyHdfsPath(Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        StringBuilder dependencyList = new StringBuilder();
        for (FileStatus fileStatus : fs.listStatus(new Path(CONF_DEPENDENCY_HDFS_PATH))) {
            if (fileStatus.isFile()) {
                if (dependencyList.length() > 0) {
                    dependencyList.append(SEPARATOR_HDFS_DEPENDENCY).append(fileStatus.getPath());
                } else {
                    dependencyList.append(fileStatus.getPath());
                }
            }
        }
        return dependencyList.toString();
    }
}
