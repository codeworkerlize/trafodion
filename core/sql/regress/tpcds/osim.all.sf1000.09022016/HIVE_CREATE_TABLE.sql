 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.SHIP_MODE
  ( 
    SM_SHIP_MODE_SK                  int
  , SM_SHIP_MODE_ID                  string
  , SM_TYPE                          string
  , SM_CODE                          string
  , SM_CARRIER                       string
  , SM_CONTRACT                      string
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.STORE_SALES
  ( 
    SS_SOLD_TIME_SK                  int
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
  PARTITIONED BY (SS_SOLD_DATE_SK int)
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.WEB_RETURNS
  ( 
    WR_RETURNED_TIME_SK              int
  , WR_ITEM_SK                       int
  , WR_REFUNDED_CUSTOMER_SK          int
  , WR_REFUNDED_CDEMO_SK             int
  , WR_REFUNDED_HDEMO_SK             int
  , WR_REFUNDED_ADDR_SK              int
  , WR_RETURNING_CUSTOMER_SK         int
  , WR_RETURNING_CDEMO_SK            int
  , WR_RETURNING_HDEMO_SK            int
  , WR_RETURNING_ADDR_SK             int
  , WR_WEB_PAGE_SK                   int
  , WR_REASON_SK                     int
  , WR_ORDER_NUMBER                  int
  , WR_RETURN_QUANTITY               int
  , WR_RETURN_AMT                    float
  , WR_RETURN_TAX                    float
  , WR_RETURN_AMT_INC_TAX            float
  , WR_FEE                           float
  , WR_RETURN_SHIP_COST              float
  , WR_REFUNDED_CASH                 float
  , WR_REVERSED_CHARGE               float
  , WR_ACCOUNT_CREDIT                float
  , WR_NET_LOSS                      float
  )
  PARTITIONED BY (WR_RETURNED_DATE_SK int)
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.HOUSEHOLD_DEMOGRAPHICS
  ( 
    HD_DEMO_SK                       int
  , HD_INCOME_BAND_SK                int
  , HD_BUY_POTENTIAL                 string
  , HD_DEP_COUNT                     int
  , HD_VEHICLE_COUNT                 int
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.WAREHOUSE
  ( 
    W_WAREHOUSE_SK                   int
  , W_WAREHOUSE_ID                   string
  , W_WAREHOUSE_NAME                 string
  , W_WAREHOUSE_SQ_FT                int
  , W_STREET_NUMBER                  string
  , W_STREET_NAME                    string
  , W_STREET_TYPE                    string
  , W_SUITE_NUMBER                   string
  , W_CITY                           string
  , W_COUNTY                         string
  , W_STATE                          string
  , W_ZIP                            string
  , W_COUNTRY                        string
  , W_GMT_OFFSET                     float
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.STORE
  ( 
    S_STORE_SK                       int
  , S_STORE_ID                       string
  , S_REC_START_DATE                 date
  , S_REC_END_DATE                   date
  , S_CLOSED_DATE_SK                 int
  , S_STORE_NAME                     string
  , S_NUMBER_EMPLOYEES               int
  , S_FLOOR_SPACE                    int
  , S_HOURS                          string
  , S_MANAGER                        string
  , S_MARKET_ID                      int
  , S_GEOGRAPHY_CLASS                string
  , S_MARKET_DESC                    string
  , S_MARKET_MANAGER                 string
  , S_DIVISION_ID                    int
  , S_DIVISION_NAME                  string
  , S_COMPANY_ID                     int
  , S_COMPANY_NAME                   string
  , S_STREET_NUMBER                  string
  , S_STREET_NAME                    string
  , S_STREET_TYPE                    string
  , S_SUITE_NUMBER                   string
  , S_CITY                           string
  , S_COUNTY                         string
  , S_STATE                          string
  , S_ZIP                            string
  , S_COUNTRY                        string
  , S_GMT_OFFSET                     float
  , S_TAX_PERCENTAGE                 float
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.STORE_SALES_SORTED_ORC_TIME
  ( 
    SS_SOLD_TIME_SK                  int
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
  PARTITIONED BY (SS_SOLD_DATE_SK int)
  CLUSTERED BY (ss_sold_time_sk)
  SORTED BY (ss_sold_time_sk)
  INTO 1 BUCKETS 
  stored as orc 
;
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.CUSTOMER
  ( 
    C_CUSTOMER_SK                    int
  , C_CUSTOMER_ID                    string
  , C_CURRENT_CDEMO_SK               int
  , C_CURRENT_HDEMO_SK               int
  , C_CURRENT_ADDR_SK                int
  , C_FIRST_SHIPTO_DATE_SK           int
  , C_FIRST_SALES_DATE_SK            int
  , C_SALUTATION                     string
  , C_FIRST_NAME                     string
  , C_LAST_NAME                      string
  , C_PREFERRED_CUST_FLAG            string
  , C_BIRTH_DAY                      int
  , C_BIRTH_MONTH                    int
  , C_BIRTH_YEAR                     int
  , C_BIRTH_COUNTRY                  string
  , C_LOGIN                          string
  , C_EMAIL_ADDRESS                  string
  , C_LAST_REVIEW_DATE_SK            int
  )
  CLUSTERED BY (c_customer_sk)
  SORTED BY (c_customer_sk)
  INTO 6 BUCKETS 
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.INCOME_BAND
  ( 
    IB_INCOME_BAND_SK                int
  , IB_LOWER_BOUND                   int
  , IB_UPPER_BOUND                   int
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.ITEM
  ( 
    I_ITEM_SK                        int
  , I_ITEM_ID                        string
  , I_REC_START_DATE                 date
  , I_REC_END_DATE                   date
  , I_ITEM_DESC                      string
  , I_CURRENT_PRICE                  float
  , I_WHOLESALE_COST                 float
  , I_BRAND_ID                       int
  , I_BRAND                          string
  , I_CLASS_ID                       int
  , I_CLASS                          string
  , I_CATEGORY_ID                    int
  , I_CATEGORY                       string
  , I_MANUFACT_ID                    int
  , I_MANUFACT                       string
  , I_SIZE                           string
  , I_FORMULATION                    string
  , I_COLOR                          string
  , I_UNITS                          string
  , I_CONTAINER                      string
  , I_MANAGER_ID                     int
  , I_PRODUCT_NAME                   string
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.STORE_SALES_SORTED_ORC_ITEM
  ( 
    SS_SOLD_TIME_SK                  int
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
  PARTITIONED BY (SS_SOLD_DATE_SK int)
  CLUSTERED BY (ss_item_sk)
  SORTED BY (ss_item_sk)
  INTO 1 BUCKETS 
  stored as orc 
;
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.WEB_SITE
  ( 
    WEB_SITE_SK                      int
  , WEB_SITE_ID                      string
  , WEB_REC_START_DATE               date
  , WEB_REC_END_DATE                 date
  , WEB_NAME                         string
  , WEB_OPEN_DATE_SK                 int
  , WEB_CLOSE_DATE_SK                int
  , WEB_CLASS                        string
  , WEB_MANAGER                      string
  , WEB_MKT_ID                       int
  , WEB_MKT_CLASS                    string
  , WEB_MKT_DESC                     string
  , WEB_MARKET_MANAGER               string
  , WEB_COMPANY_ID                   int
  , WEB_COMPANY_NAME                 string
  , WEB_STREET_NUMBER                string
  , WEB_STREET_NAME                  string
  , WEB_STREET_TYPE                  string
  , WEB_SUITE_NUMBER                 string
  , WEB_CITY                         string
  , WEB_COUNTY                       string
  , WEB_STATE                        string
  , WEB_ZIP                          string
  , WEB_COUNTRY                      string
  , WEB_GMT_OFFSET                   float
  , WEB_TAX_PERCENTAGE               float
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.CUSTOMER_ADDRESS
  ( 
    CA_ADDRESS_SK                    int
  , CA_ADDRESS_ID                    string
  , CA_STREET_NUMBER                 string
  , CA_STREET_NAME                   string
  , CA_STREET_TYPE                   string
  , CA_SUITE_NUMBER                  string
  , CA_CITY                          string
  , CA_COUNTY                        string
  , CA_STATE                         string
  , CA_ZIP                           string
  , CA_COUNTRY                       string
  , CA_GMT_OFFSET                    float
  , CA_LOCATION_TYPE                 string
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.CATALOG_SALES
  ( 
    CS_SOLD_TIME_SK                  int
  , CS_SHIP_DATE_SK                  int
  , CS_BILL_CUSTOMER_SK              int
  , CS_BILL_CDEMO_SK                 int
  , CS_BILL_HDEMO_SK                 int
  , CS_BILL_ADDR_SK                  int
  , CS_SHIP_CUSTOMER_SK              int
  , CS_SHIP_CDEMO_SK                 int
  , CS_SHIP_HDEMO_SK                 int
  , CS_SHIP_ADDR_SK                  int
  , CS_CALL_CENTER_SK                int
  , CS_CATALOG_PAGE_SK               int
  , CS_SHIP_MODE_SK                  int
  , CS_WAREHOUSE_SK                  int
  , CS_ITEM_SK                       int
  , CS_PROMO_SK                      int
  , CS_ORDER_NUMBER                  int
  , CS_QUANTITY                      int
  , CS_WHOLESALE_COST                float
  , CS_LIST_PRICE                    float
  , CS_SALES_PRICE                   float
  , CS_EXT_DISCOUNT_AMT              float
  , CS_EXT_SALES_PRICE               float
  , CS_EXT_WHOLESALE_COST            float
  , CS_EXT_LIST_PRICE                float
  , CS_EXT_TAX                       float
  , CS_COUPON_AMT                    float
  , CS_EXT_SHIP_COST                 float
  , CS_NET_PAID                      float
  , CS_NET_PAID_INC_TAX              float
  , CS_NET_PAID_INC_SHIP             float
  , CS_NET_PAID_INC_SHIP_TAX         float
  , CS_NET_PROFIT                    float
  )
  PARTITIONED BY (CS_SOLD_DATE_SK int)
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.REASON_1
  ( 
    WEB_SITE_SK                      int
  , WEB_SITE_ID                      string
  , WEB_REC_START_DATE               date
  , WEB_REC_END_DATE                 date
  , WEB_NAME                         string
  , WEB_OPEN_DATE_SK                 int
  , WEB_CLOSE_DATE_SK                int
  , WEB_CLASS                        string
  , WEB_MANAGER                      string
  , WEB_MKT_ID                       int
  , WEB_MKT_CLASS                    string
  , WEB_MKT_DESC                     string
  , WEB_MARKET_MANAGER               string
  , WEB_COMPANY_ID                   int
  , WEB_COMPANY_NAME                 string
  , WEB_STREET_NUMBER                string
  , WEB_STREET_NAME                  string
  , WEB_STREET_TYPE                  string
  , WEB_SUITE_NUMBER                 string
  , WEB_CITY                         string
  , WEB_COUNTY                       string
  , WEB_STATE                        string
  , WEB_ZIP                          string
  , WEB_COUNTRY                      string
  , WEB_GMT_OFFSET                   float
  , WEB_TAX_PERCENTAGE               float
  )
  stored as textfile 
;
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.CALL_CENTER
  ( 
    CC_CALL_CENTER_SK                int
  , CC_CALL_CENTER_ID                string
  , CC_REC_START_DATE                date
  , CC_REC_END_DATE                  date
  , CC_CLOSED_DATE_SK                int
  , CC_OPEN_DATE_SK                  int
  , CC_NAME                          string
  , CC_CLASS                         string
  , CC_EMPLOYEES                     int
  , CC_SQ_FT                         int
  , CC_HOURS                         string
  , CC_MANAGER                       string
  , CC_MKT_ID                        int
  , CC_MKT_CLASS                     string
  , CC_MKT_DESC                      string
  , CC_MARKET_MANAGER                string
  , CC_DIVISION                      int
  , CC_DIVISION_NAME                 string
  , CC_COMPANY                       int
  , CC_COMPANY_NAME                  string
  , CC_STREET_NUMBER                 string
  , CC_STREET_NAME                   string
  , CC_STREET_TYPE                   string
  , CC_SUITE_NUMBER                  string
  , CC_CITY                          string
  , CC_COUNTY                        string
  , CC_STATE                         string
  , CC_ZIP                           string
  , CC_COUNTRY                       string
  , CC_GMT_OFFSET                    float
  , CC_TAX_PERCENTAGE                float
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.DATE_DIM_54
  ( 
    D_DATE_SK                        int
  , D_DATE_ID                        string
  , D_DATE                           date
  , D_MONTH_SEQ                      int
  , D_WEEK_SEQ                       int
  , D_QUARTER_SEQ                    int
  , D_YEAR                           int
  , D_DOW                            int
  , D_MOY                            int
  , D_DOM                            int
  , D_QOY                            int
  , D_FY_YEAR                        int
  , D_FY_QUARTER_SEQ                 int
  , D_FY_WEEK_SEQ                    int
  , D_DAY_NAME                       string
  , D_QUARTER_NAME                   string
  , D_HOLIDAY                        string
  , D_WEEKEND                        string
  , D_FOLLOWING_HOLIDAY              string
  , D_FIRST_DOM                      int
  , D_LAST_DOM                       int
  , D_SAME_DAY_LY                    int
  , D_SAME_DAY_LQ                    int
  , D_CURRENT_DAY                    string
  , D_CURRENT_WEEK                   string
  , D_CURRENT_MONTH                  string
  , D_CURRENT_QUARTER                string
  , D_CURRENT_YEAR                   string
  )
  CLUSTERED BY (d_date_sk)
  INTO 54 BUCKETS 
  stored as textfile 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.CUSTOMER_DEMOGRAPHICS
  ( 
    CD_DEMO_SK                       int
  , CD_GENDER                        string
  , CD_MARITAL_STATUS                string
  , CD_EDUCATION_STATUS              string
  , CD_PURCHASE_ESTIMATE             int
  , CD_CREDIT_RATING                 string
  , CD_DEP_COUNT                     int
  , CD_DEP_EMPLOYED_COUNT            int
  , CD_DEP_COLLEGE_COUNT             int
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.PROMOTION
  ( 
    P_PROMO_SK                       int
  , P_PROMO_ID                       string
  , P_START_DATE_SK                  int
  , P_END_DATE_SK                    int
  , P_ITEM_SK                        int
  , P_COST                           float
  , P_RESPONSE_TARGET                int
  , P_PROMO_NAME                     string
  , P_CHANNEL_DMAIL                  string
  , P_CHANNEL_EMAIL                  string
  , P_CHANNEL_CATALOG                string
  , P_CHANNEL_TV                     string
  , P_CHANNEL_RADIO                  string
  , P_CHANNEL_PRESS                  string
  , P_CHANNEL_EVENT                  string
  , P_CHANNEL_DEMO                   string
  , P_CHANNEL_DETAILS                string
  , P_PURPOSE                        string
  , P_DISCOUNT_ACTIVE                string
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.REASON
  ( 
    R_REASON_SK                      int
  , R_REASON_ID                      string
  , R_REASON_DESC                    string
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.REASON_2
  ( 
    R_REASON_SK                      int
  , R_REASON_ID                      string
  , R_REASON_DESC                    string
  )
  stored as orc 
;
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.STORE_SALES_1STRIPEPERFILE
  ( 
    SS_SOLD_TIME_SK                  int
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
  PARTITIONED BY (SS_SOLD_DATE_SK int)
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.CATALOG_PAGE
  ( 
    CP_CATALOG_PAGE_SK               int
  , CP_CATALOG_PAGE_ID               string
  , CP_START_DATE_SK                 int
  , CP_END_DATE_SK                   int
  , CP_DEPARTMENT                    string
  , CP_CATALOG_NUMBER                int
  , CP_CATALOG_PAGE_NUMBER           int
  , CP_DESCRIPTION                   string
  , CP_TYPE                          string
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.STORE_SALES_SORTED_ORC_STORE
  ( 
    SS_SOLD_TIME_SK                  int
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
  PARTITIONED BY (SS_SOLD_DATE_SK int)
  CLUSTERED BY (ss_store_sk)
  SORTED BY (ss_store_sk)
  INTO 1 BUCKETS 
  stored as orc 
;
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.INVENTORY
  ( 
    INV_DATE_SK                      int
  , INV_ITEM_SK                      int
  , INV_WAREHOUSE_SK                 int
  , INV_QUANTITY_ON_HAND             int
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.DATE_DIM
  ( 
    D_DATE_SK                        int
  , D_DATE_ID                        string
  , D_DATE                           date
  , D_MONTH_SEQ                      int
  , D_WEEK_SEQ                       int
  , D_QUARTER_SEQ                    int
  , D_YEAR                           int
  , D_DOW                            int
  , D_MOY                            int
  , D_DOM                            int
  , D_QOY                            int
  , D_FY_YEAR                        int
  , D_FY_QUARTER_SEQ                 int
  , D_FY_WEEK_SEQ                    int
  , D_DAY_NAME                       string
  , D_QUARTER_NAME                   string
  , D_HOLIDAY                        string
  , D_WEEKEND                        string
  , D_FOLLOWING_HOLIDAY              string
  , D_FIRST_DOM                      int
  , D_LAST_DOM                       int
  , D_SAME_DAY_LY                    int
  , D_SAME_DAY_LQ                    int
  , D_CURRENT_DAY                    string
  , D_CURRENT_WEEK                   string
  , D_CURRENT_MONTH                  string
  , D_CURRENT_QUARTER                string
  , D_CURRENT_YEAR                   string
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.TIME_DIM
  ( 
    T_TIME_SK                        int
  , T_TIME_ID                        string
  , T_TIME                           int
  , T_HOUR                           int
  , T_MINUTE                         int
  , T_SECOND                         int
  , T_AM_PM                          string
  , T_SHIFT                          string
  , T_SUB_SHIFT                      string
  , T_MEAL_TIME                      string
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.STORE_RETURNS
  ( 
    SR_RETURN_TIME_SK                int
  , SR_ITEM_SK                       int
  , SR_CUSTOMER_SK                   int
  , SR_CDEMO_SK                      int
  , SR_HDEMO_SK                      int
  , SR_ADDR_SK                       int
  , SR_STORE_SK                      int
  , SR_REASON_SK                     int
  , SR_TICKET_NUMBER                 int
  , SR_RETURN_QUANTITY               int
  , SR_RETURN_AMT                    float
  , SR_RETURN_TAX                    float
  , SR_RETURN_AMT_INC_TAX            float
  , SR_FEE                           float
  , SR_RETURN_SHIP_COST              float
  , SR_REFUNDED_CASH                 float
  , SR_REVERSED_CHARGE               float
  , SR_STORE_CREDIT                  float
  , SR_NET_LOSS                      float
  )
  PARTITIONED BY (SR_RETURNED_DATE_SK int)
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.WEB_PAGE
  ( 
    WP_WEB_PAGE_SK                   int
  , WP_WEB_PAGE_ID                   string
  , WP_REC_START_DATE                date
  , WP_REC_END_DATE                  date
  , WP_CREATION_DATE_SK              int
  , WP_ACCESS_DATE_SK                int
  , WP_AUTOGEN_FLAG                  string
  , WP_CUSTOMER_SK                   int
  , WP_URL                           string
  , WP_TYPE                          string
  , WP_CHAR_COUNT                    int
  , WP_LINK_COUNT                    int
  , WP_IMAGE_COUNT                   int
  , WP_MAX_AD_COUNT                  int
  )
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.WEB_SALES
  ( 
    WS_SOLD_TIME_SK                  int
  , WS_SHIP_DATE_SK                  int
  , WS_ITEM_SK                       int
  , WS_BILL_CUSTOMER_SK              int
  , WS_BILL_CDEMO_SK                 int
  , WS_BILL_HDEMO_SK                 int
  , WS_BILL_ADDR_SK                  int
  , WS_SHIP_CUSTOMER_SK              int
  , WS_SHIP_CDEMO_SK                 int
  , WS_SHIP_HDEMO_SK                 int
  , WS_SHIP_ADDR_SK                  int
  , WS_WEB_PAGE_SK                   int
  , WS_WEB_SITE_SK                   int
  , WS_SHIP_MODE_SK                  int
  , WS_WAREHOUSE_SK                  int
  , WS_PROMO_SK                      int
  , WS_ORDER_NUMBER                  int
  , WS_QUANTITY                      int
  , WS_WHOLESALE_COST                float
  , WS_LIST_PRICE                    float
  , WS_SALES_PRICE                   float
  , WS_EXT_DISCOUNT_AMT              float
  , WS_EXT_SALES_PRICE               float
  , WS_EXT_WHOLESALE_COST            float
  , WS_EXT_LIST_PRICE                float
  , WS_EXT_TAX                       float
  , WS_COUPON_AMT                    float
  , WS_EXT_SHIP_COST                 float
  , WS_NET_PAID                      float
  , WS_NET_PAID_INC_TAX              float
  , WS_NET_PAID_INC_SHIP             float
  , WS_NET_PAID_INC_SHIP_TAX         float
  , WS_NET_PROFIT                    float
  )
  PARTITIONED BY (WS_SOLD_DATE_SK int)
  stored as orc 
;
 
/* Trafodion DDL */
 
 
/* Hive DDL */
CREATE TABLE TPCDS_SF1000.CATALOG_RETURNS
  ( 
    CR_RETURNED_TIME_SK              int
  , CR_ITEM_SK                       int
  , CR_REFUNDED_CUSTOMER_SK          int
  , CR_REFUNDED_CDEMO_SK             int
  , CR_REFUNDED_HDEMO_SK             int
  , CR_REFUNDED_ADDR_SK              int
  , CR_RETURNING_CUSTOMER_SK         int
  , CR_RETURNING_CDEMO_SK            int
  , CR_RETURNING_HDEMO_SK            int
  , CR_RETURNING_ADDR_SK             int
  , CR_CALL_CENTER_SK                int
  , CR_CATALOG_PAGE_SK               int
  , CR_SHIP_MODE_SK                  int
  , CR_WAREHOUSE_SK                  int
  , CR_REASON_SK                     int
  , CR_ORDER_NUMBER                  int
  , CR_RETURN_QUANTITY               int
  , CR_RETURN_AMOUNT                 float
  , CR_RETURN_TAX                    float
  , CR_RETURN_AMT_INC_TAX            float
  , CR_FEE                           float
  , CR_RETURN_SHIP_COST              float
  , CR_REFUNDED_CASH                 float
  , CR_REVERSED_CHARGE               float
  , CR_STORE_CREDIT                  float
  , CR_NET_LOSS                      float
  )
  PARTITIONED BY (CR_RETURNED_DATE_SK int)
  stored as orc 
;
 
/* Trafodion DDL */
 
