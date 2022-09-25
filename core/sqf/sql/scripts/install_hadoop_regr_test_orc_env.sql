log install_hadoop_regr_test_orc_env.log ;
set schema hive.hive ;
cqd hive_max_string_length_in_bytes '50' ;
drop table customer_orc;
create table customer_orc
(
    c_customer_sk             int,
    c_customer_id             string,
    c_current_cdemo_sk        int,
    c_current_hdemo_sk        int,
    c_current_addr_sk         int,
    c_first_shipto_date_sk    int,
    c_first_sales_date_sk     int,
    c_salutation              string,
    c_first_name              string,
    c_last_name               string,
    c_preferred_cust_flag     string,
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           string,
    c_login                   string,
    c_email_address           string,
    c_last_review_date        string
)
stored as orc location '/user/trafodion/hive/tpcds/customer_orc' ;
insert into customer_orc select * from customer;

drop table customer_address_orc;
create table customer_address_orc
(
    ca_address_sk             int,
    ca_address_id             string,
    ca_street_number          string,
    ca_street_name            string,
    ca_street_type            string,
    ca_suite_number           string,
    ca_city                   string,
    ca_county                 string,
    ca_state                  string,
    ca_zip                    string,
    ca_country                string,
    ca_gmt_offset             float,
    ca_location_type          string
)
stored as orc location '/user/trafodion/hive/tpcds/customer_address_orc' ;
insert into customer_address_orc select * from customer_address;

drop table customer_demographics_orc;
create table customer_demographics_orc
(
    cd_demo_sk                int,
    cd_gender                 string,
    cd_marital_status         string,
    cd_education_status       string,
    cd_purchase_estimate      int,
    cd_credit_rating          string,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int 
)
stored as orc location '/user/trafodion/hive/tpcds/customer_demographics_orc';
insert into customer_demographics_orc select * from customer_demographics
 order by cd_gender, cd_marital_status, cd_education_status;

drop table household_demographics_orc;
create table household_demographics_orc
(
    hd_demo_sk                int,
    hd_income_band_sk         int,
    hd_buy_potential          string,
    hd_dep_count              int,
    hd_vehicle_count          int
)
stored as orc location '/user/trafodion/hive/tpcds/household_demographics_orc' ;
insert into household_demographics_orc select * from household_demographics;

drop table promotion_orc;
create table promotion_orc
(
    p_promo_sk                int,
    p_promo_id                string,
    p_start_date_sk           int,
    p_end_date_sk             int,
    p_item_sk                 int,
    p_cost                    float,
    p_response_target         int,
    p_promo_name              string,
    p_channel_dmail           string,
    p_channel_email           string,
    p_channel_catalog         string,
    p_channel_tv              string,
    p_channel_radio           string,
    p_channel_press           string,
    p_channel_event           string,
    p_channel_demo            string,
    p_channel_details         string,
    p_purpose                 string,
    p_discount_active         string 
)
stored as orc location '/user/trafodion/hive/tpcds/promotion_orc';
insert into promotion_orc select * from promotion;

drop table store_orc;
create table store_orc
(
    s_store_sk                int,
    s_store_id                string,
    s_rec_start_date          date,
    s_rec_end_date            date,
    s_closed_date_sk          int,
    s_store_name              string,
    s_number_employees        int,
    s_floor_space             int,
    s_hours                   string,
    s_manager                 string,
    s_market_id               int,
    s_geography_class         string,
    s_market_desc             string,
    s_market_manager          string,
    s_division_id             int,
    s_division_name           string,
    s_company_id              int,
    s_company_name            string,
    s_street_number           string,
    s_street_name             string,
    s_street_type             string,
    s_suite_number            string,
    s_city                    string,
    s_county                  string,
    s_state                   string,
    s_zip                     string,
    s_country                 string,
    s_gmt_offset              float,
    s_tax_precentage          float                  
)
stored as orc location '/user/trafodion/hive/tpcds/store_orc' ;
insert into store_orc select * from store;

drop table store_sales_orc;
create table store_sales_orc
(
    ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk                int,
    ss_customer_sk            int,
    ss_cdemo_sk               int,
    ss_hdemo_sk               int,
    ss_addr_sk                int,
    ss_store_sk               int,
    ss_promo_sk               int,
    ss_ticket_number          int,
    ss_quantity               int,
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float                  
)
stored as orc location '/user/trafodion/hive/tpcds/store_sales_orc' tblproperties ("orc.stripe.size"="1048576") ;
insert into store_sales_orc select * from store_sales
  order by ss_item_sk;


drop table store2_orc;
create table store2_orc
(
    s_store_sk                int,
    s_store_id                string,
    s_rec_start_date          date,
    s_rec_end_date            date,
    s_closed_date_sk          int,
    s_store_name              string,
    s_number_employees        int,
    s_floor_space             int,
    s_hours                   string,
    s_manager                 string,
    s_market_id               int,
    s_geography_class         string,
    s_market_desc             string,
    s_market_manager          string,
    s_division_id             int,
    s_division_name           string,
    s_company_id              int,
    s_company_name            string,
    s_street_number           string,
    s_street_name             string,
    s_street_type             string,
    s_suite_number            string,
    s_city                    string,
    s_county                  string,
    s_state                   string,
    s_zip                     string,
    s_country                 string,
    s_gmt_offset              float,
    s_tax_precentage          float                  
)
stored as orc location '/user/trafodion/hive/tpcds/store2_orc' ;
insert into store2_orc select * from store_orc 
  order by s_rec_start_date;

drop table store2_sales_orc;
create table store2_sales_orc
(
    ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk                int,
    ss_customer_sk            int,
    ss_cdemo_sk               int,
    ss_hdemo_sk               int,
    ss_addr_sk                int,
    ss_store_sk               int,
    ss_promo_sk               int,
    ss_ticket_number          int,
    ss_quantity               int,
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float                  
)
stored as orc location '/user/trafodion/hive/tpcds/store2_sales_orc' ;
insert into store2_sales_orc select * from store_sales_orc
  order by ss_item_sk;

drop table date_dim_orc;
create table date_dim_orc
(
    d_date_sk                 int,
    d_date_id                 string,
    d_date                    date,
    d_month_seq               int,
    d_week_seq                int,
    d_quarter_seq             int,
    d_year                    int,
    d_dow                     int,
    d_moy                     int,
    d_dom                     int,
    d_qoy                     int,
    d_fy_year                 int,
    d_fy_quarter_seq          int,
    d_fy_week_seq             int,
    d_day_name                string,
    d_quarter_name            string,
    d_holiday                 string,
    d_weekend                 string,
    d_following_holiday       string,
    d_first_dom               int,
    d_last_dom                int,
    d_same_day_ly             int,
    d_same_day_lq             int,
    d_current_day             string,
    d_current_week            string,
    d_current_month           string,
    d_current_quarter         string,
    d_current_year            string 
)
stored as orc location '/user/trafodion/hive/tpcds/date_dim_orc';
insert into date_dim_orc select * from date_dim 
  order by d_date;

drop table item_orc;
create table item_orc
(
    i_item_sk                 int,
    i_item_id                 string,
    i_rec_start_date          date,
    i_rec_end_date            date,
    i_item_desc               string,
    i_current_price           float,
    i_wholesale_cost          float,
    i_brand_id                int,
    i_brand                   string,
    i_class_id                int,
    i_class                   string,
    i_category_id             int,
    i_category                string,
    i_manufact_id             int,
    i_manufact                string,
    i_size                    string,
    i_formulation             string,
    i_color                   string,
    i_units                   string,
    i_container               string,
    i_manager_id              int,
    i_product_name            string
)
stored as orc location '/user/trafodion/hive/tpcds/item_orc';
insert into item_orc select * from item
  order by i_rec_start_date;

drop table time_dim_orc;
create table time_dim_orc
(
    t_time_sk                 int,
    t_time_id                 string,
    t_time                    int,
    t_hour                    int,
    t_minute                  int,
    t_second                  int,
    t_am_pm                   string,
    t_shift                   string,
    t_sub_shift               string,
    t_meal_time               string
)
stored as orc location '/user/trafodion/hive/tpcds/time_dim_orc';
insert into time_dim_orc select * from time_dim 
  order by t_time;

drop table store_sales_sorted_orc;
create table store_sales_sorted_orc
(
   ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk                int,
    ss_customer_sk            int,
    ss_cdemo_sk               int,
    ss_hdemo_sk               int,
    ss_addr_sk                int,
    ss_store_sk               int,
    ss_promo_sk               int,
    ss_ticket_number          int,
    ss_quantity               int,
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float
)
clustered by (ss_sold_date_sk)
sorted by (ss_sold_date_sk, ss_sold_time_sk, ss_item_sk) into 8 buckets
stored as ORC
location '/user/trafodion/hive/tpcds/store_sales_sorted_orc'
;

insert into table store_sales_sorted_orc 
select * from store_sales order by ss_sold_date_sk, ss_sold_time_sk, ss_item_sk;

drop table date_dim_orc_bucketed;
create table date_dim_orc_bucketed
(
    d_date_sk                 int,
    d_date_id                 string,
    d_date                    date,
    d_month_seq               int,
    d_week_seq                int,
    d_quarter_seq             int,
    d_year                    int,
    d_dow                     int,
    d_moy                     int,
    d_dom                     int,
    d_qoy                     int,
    d_fy_year                 int,
    d_fy_quarter_seq          int,
    d_fy_week_seq             int,
    d_day_name                string,
    d_quarter_name            string,
    d_holiday                 string,
    d_weekend                 string,
    d_following_holiday       string,
    d_first_dom               int,
    d_last_dom                int,
    d_same_day_ly             int,
    d_same_day_lq             int,
    d_current_day             string,
    d_current_week            string,
    d_current_month           string,
    d_current_quarter         string,
    d_current_year            string 
)
clustered by (d_date_sk) into 8 buckets
stored as ORC
location '/user/trafodion/hive/tpcds/date_dim_orc_bucketed';

insert into table date_dim_orc_bucketed
select * from date_dim;

drop table customer_orc_bucketed;
create table customer_orc_bucketed
(
    c_customer_sk             int,
    c_customer_id             string,
    c_current_cdemo_sk        int,
    c_current_hdemo_sk        int,
    c_current_addr_sk         int,
    c_first_shipto_date_sk    int,
    c_first_sales_date_sk     int,
    c_salutation              string,
    c_first_name              string,
    c_last_name               string,
    c_preferred_cust_flag     string,
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           string,
    c_login                   string,
    c_email_address           string,
    c_last_review_date        string
)
clustered by (c_customer_sk) into 8 buckets
stored as ORC
location '/user/trafodion/hive/tpcds/customer_orc_bucketed';

insert into table customer_orc_bucketed
select * from customer;

 drop table date_dim_orc_decimal_ts;
 create table date_dim_orc_decimal_ts 
 (
    d_date_sk                 decimal(18,0),
    d_date_id                 string,
    d_date                    timestamp,
    d_month_seq               int,
    d_week_seq                int,
    d_quarter_seq             int,
    d_year                    int,
    d_dow                     int,
    d_moy                     int,
    d_dom                     int,
    d_qoy                     int,
    d_fy_year                 int,
    d_fy_quarter_seq          int,
    d_fy_week_seq             int,
    d_day_name                string,
    d_quarter_name            string,
    d_holiday                 string,
    d_weekend                 string,
    d_following_holiday       string,
    d_first_dom               int,
    d_last_dom                int,
    d_same_day_ly             int,
    d_same_day_lq             int,
    d_current_day             string,
    d_current_week            string,
    d_current_month           string,
    d_current_quarter         string,
    d_current_year            string 
)
stored as orc location '/user/trafodion/hive/tpcds/date_dim_orc_decimal_ts';

insert into table date_dim_orc_decimal_ts
select * from date_dim ;
log ;
exit ;
