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

drop table customer_ddl_avro;
create external table customer_ddl_avro
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
stored as avro
location '/user/trafodion/hive/exttables/customer_ddl_avro';

drop table customer_temp_avro;
create external table customer_temp_avro
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
stored as avro
location '/user/trafodion/hive/exttables/customer_temp_avro';

drop table customer_p_avro;
create table customer_p_avro
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
stored as avro
;

drop table hivenonp_avro;
create table hivenonp_avro
(
    id    int,
    col2  int,
    p1    int,
    p2    string,
    p1t   date,
    p1d   date
)
stored as avro;

drop table hivepi_avro;
create table hivepi_avro
(
    id    int,
    col2  int
)
partitioned by (p1 int)
stored as avro
;

drop table hiveps_avro;
create table hiveps_avro
(
    id    int,
    col2  int
)
partitioned by (p2 string)
stored as avro
;

drop table hivepis_avro;
create table hivepis_avro
(
    id    int,
    col2  int
)
partitioned by (p1 int, p2 string)
stored as avro
;

drop table hivepts_avro;
create table hivepts_avro
(
    id    int,
    col2  int
)
partitioned by (p1t date, p2 string)
stored as avro
;

-- create partitioned Hive tables with AVRO files
drop table hivepio_avro;
create table hivepio_avro
(
    id    int,
    col2  int
)
partitioned by (p1 int)
stored as avro;

drop table hivepdo_avro;
create table hivepdo_avro
(
    id    int,
    col2  int
)
partitioned by (p1d date)
stored as avro;

drop table hivepiso_avro;
create table hivepiso_avro
(
    id    int,
    col2  int
)
partitioned by (p1 int, p2 string)
stored as avro;

