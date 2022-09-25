// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.apache.hadoop.hbase.pit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CorruptHFileException;
import org.apache.hadoop.hbase.io.hfile.HFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * fullPath: /user/trafodion/tagName/pit1_00212470562934258730/sn1/.hbase-snapshot/sn1/
 *
 * fullSnapshotDir:
 * tmpPath        /user/trafodion/{tagName}/.snapshots/sn1/.hbase-snapshot/.tmp
 * snapShotPath   /user/trafodion/{tagName}/.snapshots/sn1/.hbase-snapshot/.sn1
 * fullIncrmentDir: /user/trafodion/{tagName}/.incrments/mutationFile1
 */
public class CheckSnapshot {
    static final Log logger = LogFactory.getLog(CheckSnapshot.class);

    //Snapshot Root Path
    private Path source;

    private FileSystem hdfs;

    private Set<String> MutationFiles;

    private Set<String> SnapshotSNs;

    private Map<String, SnapshotInfo> snapshotInfoMap;

    private final String SNAPSHOT_PATH = "/.snapshots";

    private final String INCRMENTS_PATH = "/.incrments";

    public CheckSnapshot(Path source) {
        this.source = source;
        this.SnapshotSNs = new HashSet<>();
        this.MutationFiles = new HashSet<>();
        this.snapshotInfoMap = new HashMap<>();
    }

    public void init() {

        // region init HDFS Client.
        try {
            this.hdfs = FileSystem.get(new URI(source.toUri().toString()), new Configuration());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (URISyntaxException e) {
            logger.error(e.getMessage(), e);
        }
        // endregion

        // region load Snapshot Info.
        try {
            // Increment Snapshot.
            this.SnapshotSNs.clear();
//            this.SnapshotInfos.clear();
            this.MutationFiles.clear();
            this.snapshotInfoMap.clear();

            List<Path> subSec = new ArrayList<>();

            //find snapshotFiles
            //step 1: find snapshotFile files list
            //  /user/trafodion/{tagName}/.snapshots/sn1
            FileStatus[] snapshotRoots = listStatus(new Path[]{new Path(source.toUri() + SNAPSHOT_PATH)});

            Path[] snapshotRootsPath = new Path[snapshotRoots.length];
            for (int i = 0; i < snapshotRoots.length; i++) {
                snapshotRootsPath[i] = snapshotRoots[i].getPath();
            }

            //step 2: find .hbase-snapshot files
            //  /user/trafodion/{tagName}/.snapshots/sn1/.hbase-snapshot
            FileStatus[] hbaseSnapshotFiles = listStatus(snapshotRootsPath);
            Path [] hbaseSnapshotPath = new Path[hbaseSnapshotFiles.length];
            for (int i = 0; i < hbaseSnapshotFiles.length; i++) {
                hbaseSnapshotPath[i] = hbaseSnapshotFiles[i].getPath();
            }

            //step 3: find real snapFiles
            // /user/trafodion/{tagName}/.snapshots/sn1/.hbase-snapshot/sn1
            // /user/trafodion/{tagName}/.snapshots/sn1/.hbase-snapshot/.tmp
            FileStatus[] realSnapShotFiles = listStatus(hbaseSnapshotPath);
            List<Path> tmpPath = new ArrayList<>(hbaseSnapshotFiles.length);

            for (FileStatus file : realSnapShotFiles) {
                if(file.getPath().getName().equals(".tmp")){
                    tmpPath.add(file.getPath());
                }
                //  path sn1/.hbase-snapshot/.sn1  sn1 directory exist,add to snapshotInfoMap
                if (file.getPath().getName().equals(file.getPath().getParent().getParent().getName())) {
                    SnapshotInfo snapshotInfo = new SnapshotInfo(file.getPath().getParent().getParent());
                    snapshotInfoMap.put(file.getPath().getName(), snapshotInfo);
                }
            }

            //step 4: .tmp not empty, remove from snapshotInfoMap
            //find .tmp child file and directory,if not empty snapshot is error
            //eg: //user/trafodion/{tagName}/.snapshots/sn1/.hbase-snapshot/.tmp/fileX
            FileStatus[] tempSnapShotFiles = listStatus(tmpPath.toArray(new Path[tmpPath.size()]));
            for (FileStatus file : tempSnapShotFiles) {
                String snapshotNum = file.getPath().getParent().getParent().getParent().getName();
                snapshotInfoMap.remove(snapshotNum);
            }

            //end find snapshotFiles



            //find mutation Files
            // /user/trafodion/{tagName}/.incrments/mutationFile1~n
            FileStatus[] mutationFiles = listStatus(new Path[]{new Path(source.toUri() + INCRMENTS_PATH)});

            for (FileStatus mutationFile : mutationFiles) {
                if(mutationFile.isFile()){
                    MutationFiles.add(mutationFile.getPath().getName());
                }
            }
            //end find mutation Files
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        // endregion
    }

    private FileStatus [] listStatus(Path [] paths){
        FileStatus[] fileStatuses = {};
        try {
            fileStatuses = hdfs.listStatus(paths);
        }catch (Exception e){
            logger.warn("listStatus pathSize:"+paths.length);
        }
        return fileStatuses;
    }

    class SnapshotDirFilter implements PathFilter {

        public boolean accept(Path path) {
            return path.getName().equals(".hbase-snapshot");
        }
    }

    class SnapshotInfo {
        private String sn;
        private Path dir;
        private String parentPath;

        public SnapshotInfo(Path path) {
            this.sn = path.getName();
            this.dir = path;
            this.parentPath = path.getParent().getName();
        }

        public Path getDir() {
            return dir;
        }

        /**
         * is error function.
         *
         * @return
         */
        public boolean isIncrementSnapshot() {
            return false;
        }
    }

    public Set<String> getSnapshotSNs() {
        return this.SnapshotSNs;
    }

    public Set<String> getMutationFiles() {
        return this.MutationFiles;
    }

    public List<String> check(List<String> snapshots) {
        List<String> result = new ArrayList<>();

        if (snapshots == null || snapshots.size() == 0) {
            return result;
        }

        for (String file : snapshots) {
            if (!this.MutationFiles.contains(file) && !this.snapshotInfoMap.containsKey(file)) {
                result.add(file);
            }
        }
        return result;
    }

    private List<String> check(List<String> snapshots, String parent) {
        List<String> result = new ArrayList<>();
        if (snapshots == null || snapshots.size() == 0) {
            return result;
        }

        for (String file : snapshots) {
            SnapshotInfo snapshotInfo = snapshotInfoMap.get(file);
            if (null == snapshotInfo || !Objects.equals(snapshotInfo.parentPath, parent)) {
                result.add(file);
            }
        }
        return result;
    }

    public static List<String> checkMutationClose(List<Path> mutationPath, Configuration config) {
        List<String> result = new ArrayList<>();
        if(mutationPath.isEmpty()){
            return result;
        }
        try {
            FileSystem fileSystem = FileSystem.get(config);
            Configuration copyConfig = new Configuration(config);
            //set hbase.bucketcache.ioengine and hbase.blockcache.use.external empty,only use l1 @See CacheConfig#getL2
            copyConfig.set(HConstants.BUCKET_CACHE_IOENGINE_KEY,"");
            copyConfig.set("hbase.blockcache.use.external","");
            CacheConfig cacheConfig = new CacheConfig(copyConfig);
            for (Path readPath : mutationPath) {
                try (HFile.Reader reader = HFile.createReader(fileSystem, readPath, cacheConfig, config)) {

                } catch (CorruptHFileException e) {
                    //if mutation file not close,throw Trailer exception
                    result.add(e.getMessage());
                    logger.warn("checkMutationClose-CorruptHFileException: ", e);
                } catch (FileNotFoundException e) {
                    result.add(e.getMessage());
                    logger.warn("checkMutationClose-FileNotFoundException: ", e);
                } catch (Exception e) {
                    logger.warn("checkMutationClose-Exception : " + readPath, e);
                }
            }
        }catch (Exception e){
            logger.warn("checkMutationClose error ", e);
        }
        return result;
    }

}