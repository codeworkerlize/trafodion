-- Per Anoop on 6/5/15 disabling the following cqds for now.
-- cqd COMP_BOOL_226 'ON';
-- cqd COMPRESSED_INTERNAL_FORMAT 'ON';
-- cqd COMPRESSED_INTERNAL_FORMAT_BMO 'ON';
-- cqd COMPRESSED_INTERNAL_FORMAT_DEFRAG_RATIO '100';
-- cqd ALLOW_INCOMPATIBLE_ASSIGNMENT 'ON';

-- Per Suresh to improve the load times.
cqd hive_num_esps_per_datanode '8' ;

load with no recovery into TPCH_CUSTOMER_100 ( select * from hive.hive.TPCH_CUSTOMER_100 );
load with no recovery into TPCH_LINEITEM_100 ( select * from hive.hive.TPCH_LINEITEM_100 );
load with no recovery into TPCH_NATION_100 ( select * from hive.hive.TPCH_NATION_100 );
load with no recovery into TPCH_ORDERS_100 ( select * from hive.hive.TPCH_ORDERS_100 );
load with no recovery into TPCH_PARTSUPP_100 ( select * from hive.hive.TPCH_PARTSUPP_100 );
load with no recovery into TPCH_PART_100 ( select * from hive.hive.TPCH_PART_100 );
load with no recovery into TPCH_REGION_100 ( select * from hive.hive.TPCH_REGION_100 );
load with no recovery into TPCH_SUPPLIER_100 ( select * from hive.hive.TPCH_SUPPLIER_100 );
