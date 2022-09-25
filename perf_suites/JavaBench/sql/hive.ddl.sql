create external table lineitem
(
    L_ORDERKEY                   BIGINT
  , L_LINENUMBER                 INT
  , L_PARTKEY                    INT
  , L_SUPPKEY                    INT
  , L_QUANTITY                   STRING
  , L_EXTENDEDPRICE              STRING     
  , L_DISCOUNT                   STRING
  , L_TAX                        STRING
  , L_RETURNFLAG                 STRING
  , L_LINESTATUS                 STRING
  , L_SHIPDATE                   STRING
  , L_COMMITDATE                 STRING   
  , L_RECEIPTDATE                STRING    
  , L_SHIPINSTRUCT               STRING
  , L_SHIPMODE                   STRING
  , L_COMMENT                    STRING
)
row format delimited fields terminated by '|' 
location '/testdata/lineitem';

create external table lineitem_2
(
    L_ORDERKEY                   BIGINT
  , L_LINENUMBER                 INT
  , L_PARTKEY                    INT
  , L_SUPPKEY                    INT
  , L_QUANTITY                   STRING
  , L_EXTENDEDPRICE              STRING     
  , L_DISCOUNT                   STRING
  , L_TAX                        STRING
  , L_RETURNFLAG                 STRING
  , L_LINESTATUS                 STRING
  , L_SHIPDATE                   STRING
  , L_COMMITDATE                 STRING   
  , L_RECEIPTDATE                STRING    
  , L_SHIPINSTRUCT               STRING
  , L_SHIPMODE                   STRING
  , L_COMMENT                    STRING
)
row format delimited fields terminated by '|' 
location '/testdata/lineitem_2';

create external table lineitem_5
(
    L_ORDERKEY                   BIGINT
  , L_LINENUMBER                 INT
  , L_PARTKEY                    INT
  , L_SUPPKEY                    INT
  , L_QUANTITY                   STRING
  , L_EXTENDEDPRICE              STRING     
  , L_DISCOUNT                   STRING
  , L_TAX                        STRING
  , L_RETURNFLAG                 STRING
  , L_LINESTATUS                 STRING
  , L_SHIPDATE                   STRING
  , L_COMMITDATE                 STRING   
  , L_RECEIPTDATE                STRING    
  , L_SHIPINSTRUCT               STRING
  , L_SHIPMODE                   STRING
  , L_COMMENT                    STRING
)
row format delimited fields terminated by '|' 
location '/testdata/lineitem_5';

