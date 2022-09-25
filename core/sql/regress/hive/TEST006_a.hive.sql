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

drop table promotion_seq;
drop table promotion_comp;
drop table PERF_TRANSACTIONS_ARCHIVE_BUS_CD_VCHAR ;
drop table PERF_TRANSACTIONS_ARCHIVE_BUS_CD_VCHAR_ORC ;

-- Create a sequence file version of the promotion table.
create table promotion_seq
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
stored as sequencefile;

-- Populate it.
insert into table promotion_seq 
  select * from promotion 
  where p_promo_sk < 100;

-- Setup for compression
SET hive.exec.compress.output=true; 
SET io.seqfile.compression.type=BLOCK;

-- Create a compressed sequence file version of the promotion table.
create table promotion_comp
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
stored as sequencefile;

-- Populate it.
insert into table promotion_comp 
  select * from promotion
  where p_promo_sk < 100;

-- subdir test, not seq. file
CREATE TABLE PERF_TRANSACTIONS_ARCHIVE_BUS_CD_VCHAR
  (
    KEY                            varchar(200)
  , SOURCE_SYSTEM_CD                 varchar(200)
  , BLOTTER_CD                       varchar(200)
  , ACCOUNTING_POSTING_DT            varchar(200)
  , ACCOUNT_NUMBER                   varchar(200)
  , INSTRUMENT_CD                    varchar(200)
  , SUB_LEDGER_CD                    varchar(200)
  , SUB_LEDGER_NM                    varchar(200)
  , SECURITY_CASH_INDICATOR_CD       varchar(200)
  , SECURITY_CASH_INDICATOR_NM       varchar(200)
  , TRANSACTION_ID                   varchar(200)
  , TRADE_DT                         varchar(200)
  , SETTLEMENT_DT                    varchar(200)
  , ACCOUNT_BASE_CCY                 varchar(200)
  , INSTRUMENT_LOCAL_CCY             varchar(200)
  , SOURCE_TRANSACTION_STATUS_CD     varchar(200)
  , TRANSACTION_STATUS_CD            varchar(200)
  , TRANSACTION_CANCEL_IN            varchar(200)
  , TRANSACTION_REVERSAL_IN          varchar(200)
  , SOURCE_TRANSACTION_TYPE_CD       varchar(200)
  , TRANSACTION_TYPE_CD              varchar(200)
  , TRANSACTION_TYPE_NM              varchar(200)
  , ENTITLEMENT_QTY                  decimal(18,6)
  , TRANSACTION_QTY                  decimal(18,6)
  , FX_RATE                          decimal(18,6)
  , ORIGINAL_FACE_AM                 decimal(18,6)
  , BASE_PRICE                       decimal(18,6)
  , LOCAL_PRICE                      decimal(18,6)
  , USD_PRICE                        decimal(18,6)
  , BASE_GROSS_AM                    decimal(18,6)
  , LOCAL_GROSS_AM                   decimal(18,6)
  , USD_GROSS_AM                     decimal(18,6)
  , BASE_NET_AM                      decimal(18,6)
  , LOCAL_NET_AM                     decimal(18,6)
  , USD_NET_AM                       decimal(18,6)
  , BASE_PRINCIPAL_CASH_AM           decimal(18,6)
  , LOCAL_PRINCIPAL_CASH_AM          decimal(18,6)
  , USD_PRINCIPAL_CASH_AM            decimal(18,6)
  , BASE_INCOME_CASH_AM              decimal(18,6)
  , LOCAL_INCOME_CASH_AM             decimal(18,6)
  , USD_INCOME_CASH_AM               decimal(18,6)
  , BASE_MARKET_VALUE_AM             decimal(18,6)
  , LOCAL_MARKET_VALUE_AM            decimal(18,6)
  , USD_MARKET_VALUE_AM              decimal(18,6)
  , BASE_COST_BASIS_AM               decimal(18,6)
  , LOCAL_COST_BASIS_AM              decimal(18,6)
  , USD_COST_BASIS_AM                decimal(18,6)
  , BASE_ACCRUED_INTEREST_AM         decimal(18,6)
  , LOCAL_ACCRUED_INTEREST_AM        decimal(18,6)
  , USD_ACCRUED_INTEREST_AM          decimal(18,6)
  , BASE_ORDINARY_INCOME_AM          decimal(18,6)
  , LOCAL_ORDINARY_INTEREST_AM       decimal(18,6)
  , USD_ORDINARY_INTEREST_AM         decimal(18,6)
  , BASE_CCY_GAIN_LOSS_AM            decimal(18,6)
  , LOCAL_CCY_GAIN_LOSS_AM           decimal(18,6)
  , USD_CCY_GAIN_LOSS_AM             decimal(18,6)
  , BASE_ST_REALIZED_GAIN_LOSS_AM    decimal(18,6)
  , LOCAL_ST_REALIZED_GAIN_LOSS_AM   decimal(18,6)
  , USD_ST_REALIZED_GAIN_LOSS_AM     decimal(18,6)
  , BASE_LT_REALIZED_GAIN_LOSS_AM    decimal(18,6)
  , LOCAL_LT_REALIZED_GAIN_LOSS_AM   decimal(18,6)
  , USD_LT_REALIZED_GAIN_LOSS_AM     decimal(18,6)
  , BASE_TOTAL_REALIZED_GAIN_LOSS_AM decimal(18,6)
  , LOCAL_TOTAL_REALIZED_GAIN_LOSS_AM decimal(18,6)
  , USD_TOTAL_REALIZED_GAIN_LOSS_AM  decimal(18,6)
  , BASE_COMMISSION_AM               decimal(18,6)
  , LOCAL_COMMISSION_AM              decimal(18,6)
  , USD_COMMISSION_AM                decimal(18,6)
  , BASE_SEC_FEE_AM                  decimal(18,6)
  , LOCAL_SEC_FEE_AM                 decimal(18,6)
  , USD_SEC_FEE_AM                   decimal(18,6)
  , BASE_VALUE_ADDED_TAX_AM          decimal(18,6)
  , LOCAL_VALUE_ADDED_TAX_AM         decimal(18,6)
  , USD_VALUE_ADDED_TAX_AM           decimal(18,6)
  , BASE_WITHHOLD_TAX_AM             decimal(18,6)
  , LOCAL_WITHHOLD_TAX_AM            decimal(18,6)
  , USD_WITHHOLD_TAX_AM              decimal(18,6)
  , FX_BUY_CCY_CD                    varchar(200)
  , FX_SELL_CCY_CD                   varchar(200)
  , FX_BUY_QTY                       decimal(18,6)
  , FX_SELL_QTY                      decimal(18,6)
  , FX_CONTRACT_PRICE                decimal(18,6)
  , EXECUTING_BROKER_CD              varchar(200)
  , EXECUTING_BROKER_SHORT_NM        varchar(200)
  , CLEARING_BROKER_CD               varchar(200)
  , CLEARING_BROKER_SHORT_NM         varchar(200)
  , TRANSACTION_DESC                 varchar(200)
  , STMT_TRANSACTION_IN              varchar(200)
  , CHECK_NO                         int
  , BASE_CARRY_VALUE_AM              decimal(18,6)
  , LOCAL_CARRY_VALUE_AM             decimal(18,6)
  , USD_CARRY_VALUE_AM               decimal(18,6)
  , SOURCE_CREATE_DT                 timestamp
  , SOURCE_UPDATE_DT                 timestamp
  , RECORD_UPDATE_DT                 varchar(200)
  , BASE_WITHHOLD_TAX_REDUCED_AM     decimal(18,6)
  , LOCAL_WITHHOLD_TAX_REDUCED_AM    decimal(18,6)
  , USD_WITHHOLD_TAX_REDUCED_AM      decimal(18,6)
  , BASE_WITHHOLD_TAX_REMAINING_AM   decimal(18,6)
  , LOCAL_WITHHOLD_TAX_REMAINING_AM  decimal(18,6)
  , USD_WITHHOLD_TAX_REMAINING_AM    decimal(18,6)
  , WITHHOLD_TAX_TYPE_CD             varchar(200)
  , IRA_REPORTING_YEAR               varchar(200)
  , IRA_TYPE_CD                      varchar(200)
  , IRA_TYPE_NM                      varchar(200)
  , WITHDRAWAL_TYPE_CD               varchar(200)
  , WITHDRAWAL_TYPE_NM               varchar(200)
  , BASE_EXTERNAL_BROKERAGE_COMMISSION_AM decimal(18,6)
  , LOCAL_EXTERNAL_BROKERAGE_COMMISSION_AM decimal(18,6)
  , USD_EXTERNAL_BROKERAGE_COMMISSION_AM decimal(18,6)
  , BASE_JPM_BROKERAGE_COMMISSION_AM decimal(18,6)
  , LOCAL_JPM_BROKERAGE_COMMISSION_AM decimal(18,6)
  , USD_JPM_BROKERAGE_COMMISSION_AM  decimal(18,6)
  , BASE_EXTERNAL_FEE_AM             decimal(18,6)
  , LOCAL_EXTERNAL_FEE_AM            decimal(18,6)
  , USD_EXTERNAL_FEE_AM              decimal(18,6)
  , BASE_JPM_FEE_AM                  decimal(18,6)
  , LOCAL_JPM_FEE_AM                 decimal(18,6)
  , USD_JPM_FEE_AM                   decimal(18,6)
  , BASE_STAMP_DUTY_AM               decimal(18,6)
  , LOCAL_STAMP_DUTY_AM              decimal(18,6)
  , USD_STAMP_DUTY_AM                decimal(18,6)
  , BASE_CANTONAL_TAX_AM             decimal(18,6)
  , LOCAL_CANTONAL_TAX_AM            decimal(18,6)
  , USD_CANTONAL_TAX_AM              decimal(18,6)
  , EODDATE                          varchar(200)
  , SOURCE_SYSTEM_UNIQUE_KEY         varchar(200)
  , BASE_FX_REVENUE_AMT              decimal(18,6)
  , LOCAL_FX_REVENUE_AMT             decimal(18,6)
  , USD_FX_REVENUE_AMT               decimal(18,6)
  , REVERSAL_TRANSACTION_ID          varchar(200)
  , CUSTODIAN_ID                     varchar(200)
  , CUSTODIAN_PORTFOLIO_ID           varchar(200)
  , CUSTODIAN_NM                     varchar(200)
  , EOD_IN                           varchar(200)
  , RUBRIC_ID                        varchar(200)
  , BROKER_RATE                      varchar(200)
  , UNIQUE_TRANSACTION_ID            varchar(200)
  )
  PARTITIONED BY (BUSINESS_SYSTEM_CD string,BUCKET_DT string)
  stored as parquet
  location '/user/trafodion/hive/exttables/perf_transactions_archive_bus_cd_vchar'
;

insert into table PERF_TRANSACTIONS_ARCHIVE_BUS_CD_VCHAR partition(BUSINESS_SYSTEM_CD='AA1', BUCKET_DT='2017-01-01') values ('happy',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);
insert into table PERF_TRANSACTIONS_ARCHIVE_BUS_CD_VCHAR partition(BUSINESS_SYSTEM_CD='AA2', BUCKET_DT='2017-01-01') values ('happy',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);


-- subdir test, for ORC
CREATE TABLE PERF_TRANSACTIONS_ARCHIVE_BUS_CD_VCHAR_ORC
  (
    KEY                            varchar(200)
  , SOURCE_SYSTEM_CD                 varchar(200)
  , BLOTTER_CD                       varchar(200)
  , ACCOUNTING_POSTING_DT            varchar(200)
  , ACCOUNT_NUMBER                   varchar(200)
  , INSTRUMENT_CD                    varchar(200)
  , SUB_LEDGER_CD                    varchar(200)
  , SUB_LEDGER_NM                    varchar(200)
  , SECURITY_CASH_INDICATOR_CD       varchar(200)
  , SECURITY_CASH_INDICATOR_NM       varchar(200)
  , TRANSACTION_ID                   varchar(200)
  , TRADE_DT                         varchar(200)
  , SETTLEMENT_DT                    varchar(200)
  , ACCOUNT_BASE_CCY                 varchar(200)
  , INSTRUMENT_LOCAL_CCY             varchar(200)
  , SOURCE_TRANSACTION_STATUS_CD     varchar(200)
  , TRANSACTION_STATUS_CD            varchar(200)
  , TRANSACTION_CANCEL_IN            varchar(200)
  , TRANSACTION_REVERSAL_IN          varchar(200)
  , SOURCE_TRANSACTION_TYPE_CD       varchar(200)
  , TRANSACTION_TYPE_CD              varchar(200)
  , TRANSACTION_TYPE_NM              varchar(200)
  , ENTITLEMENT_QTY                  decimal(18,6)
  , TRANSACTION_QTY                  decimal(18,6)
  , FX_RATE                          decimal(18,6)
  , ORIGINAL_FACE_AM                 decimal(18,6)
  , BASE_PRICE                       decimal(18,6)
  , LOCAL_PRICE                      decimal(18,6)
  , USD_PRICE                        decimal(18,6)
  , BASE_GROSS_AM                    decimal(18,6)
  , LOCAL_GROSS_AM                   decimal(18,6)
  , USD_GROSS_AM                     decimal(18,6)
  , BASE_NET_AM                      decimal(18,6)
  , LOCAL_NET_AM                     decimal(18,6)
  , USD_NET_AM                       decimal(18,6)
  , BASE_PRINCIPAL_CASH_AM           decimal(18,6)
  , LOCAL_PRINCIPAL_CASH_AM          decimal(18,6)
  , USD_PRINCIPAL_CASH_AM            decimal(18,6)
  , BASE_INCOME_CASH_AM              decimal(18,6)
  , LOCAL_INCOME_CASH_AM             decimal(18,6)
  , USD_INCOME_CASH_AM               decimal(18,6)
  , BASE_MARKET_VALUE_AM             decimal(18,6)
  , LOCAL_MARKET_VALUE_AM            decimal(18,6)
  , USD_MARKET_VALUE_AM              decimal(18,6)
  , BASE_COST_BASIS_AM               decimal(18,6)
  , LOCAL_COST_BASIS_AM              decimal(18,6)
  , USD_COST_BASIS_AM                decimal(18,6)
  , BASE_ACCRUED_INTEREST_AM         decimal(18,6)
  , LOCAL_ACCRUED_INTEREST_AM        decimal(18,6)
  , USD_ACCRUED_INTEREST_AM          decimal(18,6)
  , BASE_ORDINARY_INCOME_AM          decimal(18,6)
  , LOCAL_ORDINARY_INTEREST_AM       decimal(18,6)
  , USD_ORDINARY_INTEREST_AM         decimal(18,6)
  , BASE_CCY_GAIN_LOSS_AM            decimal(18,6)
  , LOCAL_CCY_GAIN_LOSS_AM           decimal(18,6)
  , USD_CCY_GAIN_LOSS_AM             decimal(18,6)
  , BASE_ST_REALIZED_GAIN_LOSS_AM    decimal(18,6)
  , LOCAL_ST_REALIZED_GAIN_LOSS_AM   decimal(18,6)
  , USD_ST_REALIZED_GAIN_LOSS_AM     decimal(18,6)
  , BASE_LT_REALIZED_GAIN_LOSS_AM    decimal(18,6)
  , LOCAL_LT_REALIZED_GAIN_LOSS_AM   decimal(18,6)
  , USD_LT_REALIZED_GAIN_LOSS_AM     decimal(18,6)
  , BASE_TOTAL_REALIZED_GAIN_LOSS_AM decimal(18,6)
  , LOCAL_TOTAL_REALIZED_GAIN_LOSS_AM decimal(18,6)
  , USD_TOTAL_REALIZED_GAIN_LOSS_AM  decimal(18,6)
  , BASE_COMMISSION_AM               decimal(18,6)
  , LOCAL_COMMISSION_AM              decimal(18,6)
  , USD_COMMISSION_AM                decimal(18,6)
  , BASE_SEC_FEE_AM                  decimal(18,6)
  , LOCAL_SEC_FEE_AM                 decimal(18,6)
  , USD_SEC_FEE_AM                   decimal(18,6)
  , BASE_VALUE_ADDED_TAX_AM          decimal(18,6)
  , LOCAL_VALUE_ADDED_TAX_AM         decimal(18,6)
  , USD_VALUE_ADDED_TAX_AM           decimal(18,6)
  , BASE_WITHHOLD_TAX_AM             decimal(18,6)
  , LOCAL_WITHHOLD_TAX_AM            decimal(18,6)
  , USD_WITHHOLD_TAX_AM              decimal(18,6)
  , FX_BUY_CCY_CD                    varchar(200)
  , FX_SELL_CCY_CD                   varchar(200)
  , FX_BUY_QTY                       decimal(18,6)
  , FX_SELL_QTY                      decimal(18,6)
  , FX_CONTRACT_PRICE                decimal(18,6)
  , EXECUTING_BROKER_CD              varchar(200)
  , EXECUTING_BROKER_SHORT_NM        varchar(200)
  , CLEARING_BROKER_CD               varchar(200)
  , CLEARING_BROKER_SHORT_NM         varchar(200)
  , TRANSACTION_DESC                 varchar(200)
  , STMT_TRANSACTION_IN              varchar(200)
  , CHECK_NO                         int
  , BASE_CARRY_VALUE_AM              decimal(18,6)
  , LOCAL_CARRY_VALUE_AM             decimal(18,6)
  , USD_CARRY_VALUE_AM               decimal(18,6)
  , SOURCE_CREATE_DT                 timestamp
  , SOURCE_UPDATE_DT                 timestamp
  , RECORD_UPDATE_DT                 varchar(200)
  , BASE_WITHHOLD_TAX_REDUCED_AM     decimal(18,6)
  , LOCAL_WITHHOLD_TAX_REDUCED_AM    decimal(18,6)
  , USD_WITHHOLD_TAX_REDUCED_AM      decimal(18,6)
  , BASE_WITHHOLD_TAX_REMAINING_AM   decimal(18,6)
  , LOCAL_WITHHOLD_TAX_REMAINING_AM  decimal(18,6)
  , USD_WITHHOLD_TAX_REMAINING_AM    decimal(18,6)
  , WITHHOLD_TAX_TYPE_CD             varchar(200)
  , IRA_REPORTING_YEAR               varchar(200)
  , IRA_TYPE_CD                      varchar(200)
  , IRA_TYPE_NM                      varchar(200)
  , WITHDRAWAL_TYPE_CD               varchar(200)
  , WITHDRAWAL_TYPE_NM               varchar(200)
  , BASE_EXTERNAL_BROKERAGE_COMMISSION_AM decimal(18,6)
  , LOCAL_EXTERNAL_BROKERAGE_COMMISSION_AM decimal(18,6)
  , USD_EXTERNAL_BROKERAGE_COMMISSION_AM decimal(18,6)
  , BASE_JPM_BROKERAGE_COMMISSION_AM decimal(18,6)
  , LOCAL_JPM_BROKERAGE_COMMISSION_AM decimal(18,6)
  , USD_JPM_BROKERAGE_COMMISSION_AM  decimal(18,6)
  , BASE_EXTERNAL_FEE_AM             decimal(18,6)
  , LOCAL_EXTERNAL_FEE_AM            decimal(18,6)
  , USD_EXTERNAL_FEE_AM              decimal(18,6)
  , BASE_JPM_FEE_AM                  decimal(18,6)
  , LOCAL_JPM_FEE_AM                 decimal(18,6)
  , USD_JPM_FEE_AM                   decimal(18,6)
  , BASE_STAMP_DUTY_AM               decimal(18,6)
  , LOCAL_STAMP_DUTY_AM              decimal(18,6)
  , USD_STAMP_DUTY_AM                decimal(18,6)
  , BASE_CANTONAL_TAX_AM             decimal(18,6)
  , LOCAL_CANTONAL_TAX_AM            decimal(18,6)
  , USD_CANTONAL_TAX_AM              decimal(18,6)
  , EODDATE                          varchar(200)
  , SOURCE_SYSTEM_UNIQUE_KEY         varchar(200)
  , BASE_FX_REVENUE_AMT              decimal(18,6)
  , LOCAL_FX_REVENUE_AMT             decimal(18,6)
  , USD_FX_REVENUE_AMT               decimal(18,6)
  , REVERSAL_TRANSACTION_ID          varchar(200)
  , CUSTODIAN_ID                     varchar(200)
  , CUSTODIAN_PORTFOLIO_ID           varchar(200)
  , CUSTODIAN_NM                     varchar(200)
  , EOD_IN                           varchar(200)
  , RUBRIC_ID                        varchar(200)
  , BROKER_RATE                      varchar(200)
  , UNIQUE_TRANSACTION_ID            varchar(200)
  )
  PARTITIONED BY (BUSINESS_SYSTEM_CD string,BUCKET_DT string)
  stored as orc
  location '/user/trafodion/hive/exttables/perf_transactions_archive_bus_cd_vchar_orc'
;
insert into table PERF_TRANSACTIONS_ARCHIVE_BUS_CD_VCHAR_ORC partition(BUSINESS_SYSTEM_CD='AA1', BUCKET_DT='2017-01-01') values ('happy',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);
insert into table PERF_TRANSACTIONS_ARCHIVE_BUS_CD_VCHAR_ORC partition(BUSINESS_SYSTEM_CD='AA2', BUCKET_DT='2017-01-01') values ('happy',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);

