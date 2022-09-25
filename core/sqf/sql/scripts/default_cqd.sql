insert into "_MD_".defaults values('ATTEMPT_ESP_PARALLELISM','OFF','ATTEMPT_ESP_PARALLELISM',0); --关闭esp并发
insert into "_MD_".defaults values('DETAILED_STATISTICS','OFF','DETAILED_STATISTICS',0); --关闭rms日志
insert into "_MD_".defaults values('EXPLAIN_IN_RMS','OFF','EXPLAIN_IN_RMS',0); --关闭rms的query plan日志，无法执行explain for qid查看执行计划
insert into "_MD_".defaults values('GENERATE_EXPLAIN','OFF','GENERATE_EXPLAIN',0);
insert into "_MD_".defaults values('HBASE_CACHE_BLOCKS','ON','HBASE_CACHE_BLOCKS',0);
insert into "_MD_".defaults values('HBASE_REGION_SERVER_MAX_HEAP_SIZE','31744','HBASE_REGION_SERVER_MAX_HEAP_SIZE',0);
insert into "_MD_".defaults values('MDAM_SCAN_METHOD','OFF','MDAM_SCAN_METHOD',0); --关闭MDAM
insert into "_MD_".defaults values('MODE_COMPATIBLE_1','ON','MODE_COMPATIBLE_1',0);--rownum和rowid支持
insert into "_MD_".defaults values('VARCHAR_PARAM_DEFAULT_SIZE','8000','VARCHAR_PARAM_DEFAULT_SIZE',0); --解决参数过长的问题
insert into "_MD_".defaults values('QUERY_TEXT_CACHE','ON','QUERY_TEXT_CACHE',0); --prepare编译时间问题
insert into "_MD_".defaults values('QUERY_CACHE','65536','QUERY_CACHE',0); --调大query cache，默认16MB
insert into "_MD_".defaults values('CANCEL_QUERY_ALLOWED','OFF','CANCEL_QUERY_ALLOWED',0);
insert into "_MD_".defaults values('HBASE_DATA_BLOCK_ENCODING_OPTION','FAST_DIFF','HBASE_DATA_BLOCK_ENCODING_OPTION',0); --建表默认encoding
insert into "_MD_".defaults values('HBASE_COMPRESSION_OPTION','SNAPPY','HBASE_COMPRESSION_OPTION',0); --建表默认压缩格式
insert into "_MD_".defaults values('HBASE_MEMSTORE_FLUSH_SIZE_OPTION','1073741824','HBASE_MEMSTORE_FLUSH_SIZE_OPTION',0); --建表默认flush大小
insert into "_MD_".defaults values('TRAF_DEFAULT_COL_CHARSET','UTF8','TRAF_DEFAULT_COL_CHARSET',0); --建表默认编码为UTF8
insert into "_MD_".defaults values('TRAF_COL_LENGTH_IS_CHAR','OFF','TRAF_COL_LENGTH_IS_CHAR',0); --建表默认为bytes而非chars
insert into "_MD_".defaults values('DYNAMIC_PARAM_DEFAULT_CHARSET','UTF8','DYNAMIC_PARAM_DEFAULT_CHARSET',0);  --支持中文参数
insert into "_MD_".defaults values('TRAF_ENABLE_METADATA_LOAD_IN_CACHE','ON','TRAF_ENABLE_METADA',0);  -- preload metadata
insert into "_MD_".defaults values('OR_PRED_TO_SEMIJOIN','1','OR_PRED_TO_SEMIJOIN',0);  -- OR predicate count
insert into "_MD_".defaults values('OR_PRED_TO_SEMIJOIN_PROBES_MAX_RATIO','1.1','OR_PRED_TO_SEMIJOIN_PROBES_MAX_RATIO',0);  -- or predicate to semijoin probes max ratio
insert into "_MD_".defaults values('OR_PRED_TO_SEMIJOIN_TABLE_MIN_SIZE','5000','OR_PRED_TO_SEMIJOIN_TABLE_MIN_SIZE',0);  -- the min row count for table which can be transform to semijion