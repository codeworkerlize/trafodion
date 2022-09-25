DROP TABLE MYORC2;
CREATE TABLE MYORC2
(
  SS_SOLD_DATE_SK                  int
, SS_SOLD_TIME_SK                  int
, SS_ITEM_SK                       int
, SS_CUSTOMER_SK                   int
, SS_CDEMO_SK                      int
, SS_HDEMO_SK                      int
, SS_ADDR_SK                       int
, SS_STORE_SK                      int
, SS_PROMO_SK                      int
, SS_TICKET_NUMBER                 int
, SS_QUANTITY                      int
, SS_WHOLESALE_COST                float
, SS_LIST_PRICE                    float
, SS_SALES_PRICE                   float
, SS_EXT_DISCOUNT_AMT              float
, SS_EXT_SALES_PRICE               float
, SS_EXT_WHOLESALE_COST            float
, SS_EXT_LIST_PRICE                float
, SS_EXT_TAX                       float
, SS_COUPON_AMT                    float
, SS_NET_PAID                      float
, SS_NET_PAID_INC_TAX              float
, SS_NET_PROFIT                    float
)
PARTITIONED BY (DATESTAMP STRING) 
CLUSTERED BY (SS_CDEMO_SK) 
INTO 2 BUCKETS STORED AS ORC;

INSERT OVERWRITE TABLE MYORC2 PARTITION (DATESTAMP = '1997-03-17') SELECT * FROM STORE2_SALES_ORC WHERE SS_SOLD_DATE_SK > 2450000 AND SS_SOLD_DATE_SK < 2452600;

