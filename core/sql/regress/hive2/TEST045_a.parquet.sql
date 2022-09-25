-- @@@ START COPYRIGHT @@@
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
-- @@@ END COPYRIGHT @@@

drop table customer_ddl;
create external table customer_ddl
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
row format delimited fields terminated by '|'
location '/user/trafodion/hive/exttables/customer_ddl';

drop table customer_ddl_parquet;
create external table customer_ddl_parquet
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
stored as parquet
location '/user/trafodion/hive/exttables/customer_ddl_parquet';

drop table customer_temp_parquet;
create external table customer_temp_parquet
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
stored as parquet
location '/user/trafodion/hive/exttables/customer_temp_parquet';

drop table customer_p_parquet;
create table customer_p_parquet
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
    -- c_preferred_cust_flag     string, -- partitioning key
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           string,
    c_login                   string,
    c_email_address           string,
    c_last_review_date        string
)
partitioned by (c_preferred_cust_flag string)
stored as parquet
;

drop table hivenonp_parquet;
create table hivenonp_parquet
(
    id    int,
    col2  int,
    p1    int,
    p2    string,
    p1t   timestamp,
    p1d   timestamp
)
stored as parquet;

drop table hivepi_parquet;
create table hivepi_parquet
(
    id    int,
    col2  int
)
partitioned by (p1 int)
stored as parquet
;

drop table hiveps_parquet;
create table hiveps_parquet
(
    id    int,
    col2  int
)
partitioned by (p2 string)
stored as parquet
;

drop table hivepis_parquet;
create table hivepis_parquet
(
    id    int,
    col2  int
)
partitioned by (p1 int, p2 string)
stored as parquet
;

drop table hivepts_parquet;
create table hivepts_parquet
(
    id    int,
    col2  int
)
partitioned by (p1t timestamp, p2 string)
stored as parquet
;

-- create partitioned Hive tables with PARQUET files
drop table hivepio_parquet;
create table hivepio_parquet
(
    id    int,
    col2  int
)
partitioned by (p1 int)
stored as parquet;

drop table hivepdo_parquet;
create table hivepdo_parquet
(
    id    int,
    col2  int
)
partitioned by (p1d timestamp)
stored as parquet;

drop table hivepiso_parquet;
create table hivepiso_parquet
(
    id    int,
    col2  int
)
partitioned by (p1 int, p2 string)
stored as parquet;

