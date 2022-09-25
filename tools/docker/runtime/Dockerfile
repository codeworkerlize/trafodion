FROM centos:7

LABEL description="EsgynDB single node docker image builder"

RUN echo -e "[mariadb] \\nname=MariaDB \\nbaseurl=http://yum.mariadb.org/5.5/centos7-amd64 \
             \\ngpgkey=https://yum.mariadb.org/RPM-GPG-KEY-MariaDB \\ngpgcheck=0" > /etc/yum.repos.d/MariaDB.repo

RUN yum install -y wget java-1.8.0-openjdk-devel.x86_64 which openssh* rsync initscripts MariaDB-server MariaDB-client net-tools apr apr-util expect gzip gnuplot libiodbc-devel lzo lzop perl-DBD-SQLite perl-Params-Validate perl-Time-HiRes protobuf sqlite snappy unixODBC-devel unzip lsof openssl && yum clean all

ENV HADOOP_VERSION="2.7.6"
ENV HBASE_VERSION="1.1.2"
ENV ZOOKEEPER_VERSION="3.4.6"
ENV HIVE_VERSION="1.2.2"

ENV HOSTNAME=localhost
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HBASE_HOME=/opt/hbase-${HBASE_VERSION}
ENV HBASE_LIB=${HBASE_HOME}/lib
ENV ZOOKEEPER_HOME=/opt/zookeeper-${ZOOKEEPER_VERSION}
ENV ZOOKEEPER_DATA_DIR=/opt/data/zookeeper
ENV HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin
ENV TRAF_DIR=/opt/trafodion TRAF_CFG_DIR=/etc/trafodion TRAF_HOME=/opt/trafodion/esgyndb
ENV TRAF_CONF=$TRAF_HOME/conf TRAF_VAR=$TRAF_HOME/tmp

COPY ./config /opt

# install hadoop, hbase, zookeeper, hive
RUN wget http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    wget http://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz && \
    wget http://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/zookeeper-${ZOOKEEPER_VERSION}.tar.gz && \
    wget http://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    wget https://cdn.mysql.com//Downloads/Connector-J/mysql-connector-java-5.1.47.tar.gz && \
    tar xf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && tar xf hbase-${HBASE_VERSION}-bin.tar.gz -C /opt/ && \
    tar xf zookeeper-${ZOOKEEPER_VERSION}.tar.gz -C /opt/ && tar xf apache-hive-${HIVE_VERSION}-bin.tar.gz -C /opt/ && \
    tar xf mysql-connector-java-5.1.47.tar.gz && mv mysql-connector-java-5.1.47/mysql-connector-java-5.1.47-bin.jar /opt/apache-hive-${HIVE_VERSION}-bin/lib/ && \
    rm -rf /hadoop-${HADOOP_VERSION}.tar.gz && rm -rf /hbase-${HBASE_VERSION}-bin.tar.gz && \
    rm -rf /zookeeper-${ZOOKEEPER_VERSION}.tar.gz && rm -rf /apache-hive-${HIVE_VERSION}-bin.tar.gz && rm -rf /mysql-connector-java-5.1.47.tar.gz && \
    rm -rf /opt/hadoop-${HADOOP_VERSION}/share/doc && rm -rf /opt/hbase-${HBASE_VERSION}/docs && rm -rf /opt/zookeeper-${ZOOKEEPER_VERSION}/docs

RUN mkdir -p /dfs $ZOOKEEPER_DATA_DIR

# config hadoop
RUN echo -e "export JAVA_HOME=${JAVA_HOME}" >> /opt/hadoop-${HADOOP_VERSION}/etc/hadoop/hadoop-env.sh && \
    echo -e "export JAVA_HOME=${JAVA_HOME} \\nexport HBASE_MANAGES_ZK=false" >> /opt/hbase-${HBASE_VERSION}/conf/hbase-env.sh && \
    echo -e "HADOOP_HOME=${HADOOP_HOME}" > ${HIVE_HOME}/conf/hive-env.sh && \
    echo -e "clientPort=2181 \\ndataDir=$ZOOKEEPER_DATA_DIR \\ntickTime=2000 \\ndataLogDir=$ZOOKEEPER_DATA_DIR" >> /opt/zookeeper-${ZOOKEEPER_VERSION}/conf/zoo.cfg && \
    mv /opt/core-site.xml /opt/hadoop-${HADOOP_VERSION}/etc/hadoop/core-site.xml && \
    mv /opt/hdfs-site.xml /opt/hadoop-${HADOOP_VERSION}/etc/hadoop/hdfs-site.xml && \
    mv /opt/hbase-site.xml /opt/hbase-${HBASE_VERSION}/conf/hbase-site.xml && \
    mv /opt/hive-site.xml /opt/apache-hive-${HIVE_VERSION}-bin/conf/hive-site.xml

# namenode format
RUN /bin/bash /opt/hadoop-${HADOOP_VERSION}/bin/hdfs namenode -format

# init hive metastore
RUN /etc/init.d/mysql start && mysqladmin -u root password && mysql -u root -p -e "create database hive;\
    create database metastore;\
    create user 'hive'@'%' identified by 'hive';create user 'hive'@'localhost' identified by 'hive';grant all on hive.* to hive@'%';\
    grant all on hive.* to hive@'localhost';grant all on metastore.* to hive@'%';grant all on metastore.* to hive@'localhost';"

# set passwordless ssh
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo -e "StrictHostKeyChecking no \nUserKnownHostsFile /dev/null" >> ~/.ssh/config

ARG ESGYNDB_DOWNLOAD_LINK
ARG ESGYNDB_VERSION

# get esgyn build
RUN echo "10.1.40.25 downloads.esgyn.local" >> /etc/hosts && \
    wget ${ESGYNDB_DOWNLOAD_LINK} && \
    mkdir -p ${TRAF_DIR} && chmod 700 ${TRAF_DIR} && \
    mkdir -p ${TRAF_HOME} && mkdir -p ${TRAF_CFG_DIR} && mkdir -p ${TRAF_DIR}/sqllogs && \
    tar xf /esgynDB_server-${ESGYNDB_VERSION}-RH7-x86_64.tar.gz -C ${TRAF_HOME} && chmod 700 ${TRAF_HOME} && \
    rm -rf /esgynDB_server-${ESGYNDB_VERSION}-RH7-x86_64.tar.gz

# env_setup
RUN sed -i "s!export TAR_DOWNLOAD_ROOT=.*!export TAR_DOWNLOAD_ROOT=${TRAF_DIR}/sqllogs!g" ${TRAF_HOME}/sqenvcom.sh && \
    sed -i "s!export CACERTS_DIR=.*!export CACERTS_DIR=${TRAF_DIR}/cacerts!g" ${TRAF_HOME}/sqenvcom.sh && \
    sed -i "s!sqllogdir=.*!sqllogdir=${TRAF_DIR}/sqllogs!g" ${TRAF_HOME}/sql/scripts/genms && \
    sed -i "s!cacertsdir=.*!cacertsdir=${TRAF_DIR}/cacerts!g" ${TRAF_HOME}/sql/scripts/genms && \
    cp -f ${TRAF_HOME}/sysinstall/home/trafodion/.bashrc /root/.bashrc

# copy_file
RUN mv /opt/trafodion_config ${TRAF_CFG_DIR}/trafodion_config && \
    mv /opt/config.xml ${TRAF_CONF}/dbmgr/config.xml && \
    mv /opt/dcs-site-cloud.xml ${TRAF_CONF}/dcs/dcs-site-cloud.xml && \
    mv /opt/dcs-site.xml ${TRAF_CONF}/dcs/dcs-site.xml && \
    mv /opt/rest-site.xml ${TRAF_CONF}/rest/rest-site.xml && \
    cp ${TRAF_HOME}/mgblty/opentsdb/etc/opentsdb/opentsdb.conf ${TRAF_CONF}/mgblty/opentsdb/ && \
    cp ${TRAF_HOME}/mgblty/bosun/conf/bosun.conf ${TRAF_CONF}/mgblty/bosun/

# hbase_trx
RUN cp ${TRAF_HOME}/export/lib/trafodion-utility-* ${HBASE_LIB} && chmod a+r ${HBASE_LIB}/trafodion-utility-* && \
    cp ${TRAF_HOME}/export/lib/hbase-trx-apache1_1-${ESGYNDB_VERSION}.jar ${HBASE_LIB} && chmod a+r ${HBASE_LIB}/hbase-trx-*

# traf_config
RUN echo -e "begin node\nnode-id=0;node-name=localhost;cores=0-1;processors=1;roles=connection,aggregation,storage\nend node\n\nbegin overflow\nhdd $TRAF_VAR\nend overflow\n" > $TRAF_CONF/sqconfig && \
    sed -i 's/tsdbHost = .*/tsdbHost = localhost:5242/g' ${TRAF_CONF}/mgblty/bosun/bosun.conf && \
    sed -i 's/tsd.network.port = .*/tsd.network.port = 5242/g' ${TRAF_CONF}/mgblty/opentsdb/opentsdb.conf && \
    sed -i "s/tsd.storage.hbase.zk_quorum = .*/tsd.storage.hbase.zk_quorum = ${HOSTNAME}/g" ${TRAF_CONF}/mgblty/opentsdb/opentsdb.conf && \
    if [ -f /etc/timezone ]; then TIMEZONE=`cat /etc/timezone`;elif [ -h /etc/localtime ]; then TIMEZONE=`readlink /etc/localtime | sed "/posix/d;s/.*\/usr\/share\/zoneinfo\///"`; \
    else checksum=`md5sum /etc/localtime | cut -d' ' -f1`;TIMEZONE=`find /usr/share/zoneinfo/ -maxdepth 1 -type f -exec md5sum {} \; | grep "^$checksum" | \
    sed "/posix/d;s/.*\/usr\/share\/zoneinfo\///"`;fi && \
    sed -i "s#\"timeZoneName\".*#\"timeZoneName\">$TIMEZONE</entry>#g" ${TRAF_CONF}/dbmgr/config.xml && \
    sed -i "s/tsd.core.timezone = .*/tsd.core.timezone = $TIMEZONE/g" ${TRAF_CONF}/mgblty/opentsdb/opentsdb.conf && \
    sed -i '$a\tsd.storage.hbase.data_table=TRAF_RSRVD_7:tsdb\ntsd.storage.hbase.tree_table=TRAF_RSRVD_7:tsdb-tree\ntsd.storage.hbase.uid_table=TRAF_RSRVD_7:tsdb-uid\ntsd.storage.hbase.meta_table=TRAF_RSRVD_7:tsdb-meta' ${TRAF_CONF}/mgblty/opentsdb/opentsdb.conf && \
    sed -i "s/TSDPORT=.*/TSDPORT=5242/g" ${TRAF_HOME}/mgblty/tcollector/startstop && \
    echo -e "${HOSTNAME}" > ${TRAF_CONF}/dcs/masters && echo -e "${HOSTNAME} 4" > ${TRAF_CONF}/dcs/servers

CMD ["/opt/start.sh"]

EXPOSE 4205
