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

drop table customer_ddl_orc;
create external table customer_ddl_orc
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
stored as orc
location '/user/trafodion/hive/exttables/customer_ddl_orc';

drop table customer_temp_orc;
create external table customer_temp_orc
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
stored as orc
location '/user/trafodion/hive/exttables/customer_temp_orc';

drop table customer_p_orc;
create table customer_p_orc
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
stored as orc
;

drop table hivenonp_orc;
create table hivenonp_orc
(
    id    int,
    col2  int,
    p1    int,
    p2    string,
    p1t   timestamp,
    p1d   date
)
stored as orc;

drop table hivepi_orc;
create table hivepi_orc
(
    id    int,
    col2  int
)
partitioned by (p1 int)
stored as orc
;

drop table hiveps_orc;
create table hiveps_orc
(
    id    int,
    col2  int
)
partitioned by (p2 string)
stored as orc
;

drop table hivepis_orc;
create table hivepis_orc
(
    id    int,
    col2  int
)
partitioned by (p1 int, p2 string)
stored as orc
;

drop table hivepts_orc;
create table hivepts_orc
(
    id    int,
    col2  int
)
partitioned by (p1t timestamp, p2 string)
stored as orc
;

-- create partitioned Hive tables with ORC files
drop table hivepio_orc;
create table hivepio_orc
(
    id    int,
    col2  int
)
partitioned by (p1 int)
stored as orc;

drop table hivepdo_orc;
create table hivepdo_orc
(
    id    int,
    col2  int
)
partitioned by (p1d date)
stored as orc;

drop table hivepiso_orc;
create table hivepiso_orc
(
    id    int,
    col2  int
)
partitioned by (p1 int, p2 string)
stored as orc;

