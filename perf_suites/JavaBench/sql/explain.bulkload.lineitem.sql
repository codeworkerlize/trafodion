prepare xx from
load transform into LINEITEM
select
  L_ORDERKEY      
, L_LINENUMBER    
, L_PARTKEY       
, L_SUPPKEY       
, CAST(L_QUANTITY AS DECIMAL(15,2))      
, CAST(L_EXTENDEDPRICE AS DECIMAL(15,2))      
, CAST(L_DISCOUNT AS DECIMAL(15,2))      
, CAST(L_TAX AS DECIMAL(15,2))        
, L_RETURNFLAG    
, L_LINESTATUS  
, CAST(L_SHIPDATE AS DATE)     
, CAST(L_COMMITDATE AS DATE)    
, CAST(L_RECEIPTDATE AS DATE)       
, L_SHIPINSTRUCT  
, L_SHIPMODE      
, L_COMMENT       
from 
hive.hive.LINEITEM
;

explain options 'f' xx;