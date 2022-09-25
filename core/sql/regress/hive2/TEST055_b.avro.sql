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

insert overwrite table customer_ddl_avro
select 
    c_customer_sk,
    c_customer_id,
    c_current_cdemo_sk,
    c_current_hdemo_sk,
    c_current_addr_sk,
    c_first_shipto_date_sk,
    c_first_sales_date_sk,
    c_salutation,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_day,
    c_birth_month,
    c_birth_year,
    c_birth_country,
    c_login,
    c_email_address,
    c_last_review_date
from customer 
where c_customer_sk < 25000;

drop table newtable_avro;
create table newtable_avro(a string) stored as avro;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table hiveps_avro partition (p2) select id, col2, p2 from hivenonp_avro;
insert into table hivepio_avro partition (p1) select id, col2, p1 from hivenonp_avro;
insert into table hivepdo_avro partition (p1d) select id, col2, p1d from hivenonp_avro;
insert into table hivepiso_avro partition (p1, p2) select id, col2, p1, p2 from hivenonp_avro;
