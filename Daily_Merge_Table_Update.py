# Databricks notebook source
# MAGIC %md
# MAGIC ## This notebook extracts the daily channel-level spend, install and ltv info and saves them into a table named "ua.skan_performance_merge_ltv"
# MAGIC ### Note this table has a report delay of 3 days. This is because of the delay in SKAN postback

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql as ps
import pandas as pd
from enum import Enum
from typing import List, Tuple, Union
from pathlib import Path
from datetime import datetime, date, timedelta

# COMMAND ----------

class Environment(Enum):
  DEVELOPMENT = 1
  PRODUCTION = 2

ENVIRONMENT = Environment.PRODUCTION

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define MetaData

# COMMAND ----------

SAVE_FLAG = True

DATE_FORMAT = '%Y-%m-%d'

today = date.today()
REPORT_DELAY_DAYS = 3
START_DT = '2021-01-01'
END_DT = datetime.strftime(today - timedelta(days=REPORT_DELAY_DAYS), DATE_FORMAT)
GAME_LIST = ['Cookie Jam','Cookie Jam Blast','Bingo Pop','Emoji Blitz','Harry Potter','Jurassic World Alive','Jurassic World the Game','Lovelink','Genies and Gems','Panda Pop','Mahjong','Solitaire Showtime']
LUDIA_TITLES = ['Jurassic World Alive', 'Lovelink', 'Jurassic World the Game']
MARKET_LIST = ['IT', 'GO']
FACT_PROMO_FILTER_COND = "CHANNEL_NAME not like '%UNTRUSTED%'" # may also include PROMOTION_NAME not like 'RT_%' and PROMOTION_NAME not like 'XP_%'
SKAN_FILTER_COND = "CHANNEL_NAME not like '%UNTRUSTED%'"

CHANNEL_TABLE_NAME = "ua.skan_performance_channel_ltv"
CAMPAIGN_TABLE_NAME = "ua.skan_performance_campaign_ltv"
DBFS_SAVE_DIR = '/mnt/jc-analytics-databricks-work/home/dongb/UA/SKAN/SKAN_Performance_Merge_PLTV'
CHANNEL_PARQUET_PATH = str(Path(DBFS_SAVE_DIR, f"channel_{datetime.strftime(today, DATE_FORMAT)}.parquet"))
CAMPAIGN_PARQUET_PATH = str(Path(DBFS_SAVE_DIR, f"campaign_{datetime.strftime(today, DATE_FORMAT)}.parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Campaign Spend, Install IAP and AD from fact_promo Table

# COMMAND ----------

def get_campaign_without_sub_ltv() -> ps.DataFrame:
  """
  Return a dataframe with campaign-level daily spend, install, rev, retention and ltv
  """
  sql = f"""
with pr_promo_tb as (
  select
    APPLICATION_FAMILY_NAME,
    MARKET_CD,
    MVP_CAMPAIGN_TYPE,
    USER_SOURCE_TYPE_CD,
    case 
      when USER_SOURCE_TYPE_CD in ('MM', 'MK', 'MC') then
        case
          when CHANNEL_NAME = 'SKANUAC' then 'ADWORDS'
          when CHANNEL_NAME = 'SKANFB' then 'SGNFB'
          else CHANNEL_NAME
        end
      else 'ORGANIC'
    end as CHANNEL_NAME,
    case 
      when USER_SOURCE_TYPE_CD in ('MM', 'MK', 'MC') then PROMOTION_NAME
      else 'ORGANIC'
    end as PROMOTION_NAME,
    CALENDAR_DT,
    EXPENSE_AMT,
    USER_QTY,
    RETENTION_DAY_001_QTY,
    RETENTION_DAY_003_QTY,
    RETENTION_DAY_007_QTY,
    REVS_DAY_001_AMT,
    AD_REVS_DAY_001_AMT,
    subscriptions_revs_day_001_amt,
    REVS_DAY_003_AMT,
    AD_REVS_DAY_003_AMT,
    subscriptions_revs_day_003_amt,
    REVS_DAY_007_AMT,
    AD_REVS_DAY_007_AMT,
    subscriptions_revs_day_007_amt,
    REVS_DAY_014_AMT,
    AD_REVS_DAY_014_AMT,
    subscriptions_revs_day_014_amt,
    REVS_DAY_028_AMT,
    AD_REVS_DAY_028_AMT,
    subscriptions_revs_day_028_amt,
    LTV_365_LASTEST_VAL,
    AD_LTV_365_LASTEST_VAL
  from pr_analytics_agg.fact_promotion_expense_daily
  where APPLICATION_FAMILY_NAME in {tuple(GAME_LIST)} and MARKET_CD in {tuple(MARKET_LIST)} and {FACT_PROMO_FILTER_COND} and CALENDAR_DT between '{START_DT}' and '{END_DT}' and MVP_CAMPAIGN_TYPE = 'New Installs'
),
without_total_pltv as (
select     
  APPLICATION_FAMILY_NAME,
  MARKET_CD,
  USER_SOURCE_TYPE_CD,
  CHANNEL_NAME,
  PROMOTION_NAME,
  CALENDAR_DT,
  sum(EXPENSE_AMT) as SPEND,
  sum(USER_QTY) as INSTALL_NUM,
  sum(RETENTION_DAY_001_QTY) as RETENTION_DAY_001_QTY,
  sum(RETENTION_DAY_003_QTY) as RETENTION_DAY_003_QTY,
  sum(RETENTION_DAY_007_QTY) as RETENTION_DAY_007_QTY,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then REVS_DAY_001_AMT*0.616
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then REVS_DAY_001_AMT*0.525
         else REVS_DAY_001_AMT*0.7 
      end
  ) as IAP_REVS_DAY_001_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then AD_REVS_DAY_001_AMT*0.88
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then AD_REVS_DAY_001_AMT*0.75
         else AD_REVS_DAY_001_AMT*0.
      end
  ) as AD_REVS_DAY_001_AMT,
  sum(
       ifnull(case when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_001_amt*0.616 
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_001_amt*0.525
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' then subscriptions_revs_day_001_amt *0.7
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_001_amt*0.748
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_001_amt*0.638
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' then subscriptions_revs_day_001_amt *0.85
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_001_amt*0.748 
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_001_amt*0.638
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) then subscriptions_revs_day_001_amt*0.85
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_001_amt*0.616 
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_001_amt*0.525
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) then subscriptions_revs_day_001_amt*0.7
             else 0 end, 0)
  ) as SUB_REVS_DAY_001_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then REVS_DAY_003_AMT*0.616
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then REVS_DAY_003_AMT*0.525
         else REVS_DAY_003_AMT*0.7 
      end
  ) as IAP_REVS_DAY_003_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then AD_REVS_DAY_003_AMT*0.88
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then AD_REVS_DAY_003_AMT*0.75
         else AD_REVS_DAY_003_AMT*0.
      end
  ) as AD_REVS_DAY_003_AMT,
  sum(
       ifnull(case when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_003_amt*0.616 
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_003_amt*0.525
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' then subscriptions_revs_day_003_amt *0.7
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_003_amt*0.748
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_003_amt*0.638
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' then subscriptions_revs_day_003_amt *0.85
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_003_amt*0.748 
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_003_amt*0.638
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) then subscriptions_revs_day_003_amt*0.85
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_003_amt*0.616 
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_003_amt*0.525
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) then subscriptions_revs_day_003_amt*0.7
             else 0 end, 0)
  ) as SUB_REVS_DAY_003_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then REVS_DAY_007_AMT*0.616
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then REVS_DAY_007_AMT*0.525
         else REVS_DAY_007_AMT*0.7 
      end
  ) as IAP_REVS_DAY_007_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then AD_REVS_DAY_007_AMT*0.88
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then AD_REVS_DAY_007_AMT*0.75
         else AD_REVS_DAY_007_AMT*0.
      end
  ) as AD_REVS_DAY_007_AMT,
  sum(
       ifnull(case when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_007_amt*0.616 
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_007_amt*0.525
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' then subscriptions_revs_day_007_amt *0.7
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_007_amt*0.748
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_007_amt*0.638
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' then subscriptions_revs_day_007_amt *0.85
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_007_amt*0.748 
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_007_amt*0.638
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) then subscriptions_revs_day_007_amt*0.85
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_007_amt*0.616 
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_007_amt*0.525
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) then subscriptions_revs_day_007_amt*0.7
             else 0 end, 0)
  ) as SUB_REVS_DAY_007_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then REVS_DAY_014_AMT*0.616
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then REVS_DAY_014_AMT*0.525
         else REVS_DAY_014_AMT*0.7 
      end
  ) as IAP_REVS_DAY_014_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then AD_REVS_DAY_014_AMT*0.88
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then AD_REVS_DAY_014_AMT*0.75
         else AD_REVS_DAY_014_AMT*0.
      end
  ) as AD_REVS_DAY_014_AMT,
  sum(
       ifnull(case when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_014_amt*0.616 
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_014_amt*0.525
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' then subscriptions_revs_day_014_amt *0.7
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_014_amt*0.748
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_014_amt*0.638
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' then subscriptions_revs_day_014_amt *0.85
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_014_amt*0.748 
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_014_amt*0.638
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) then subscriptions_revs_day_014_amt*0.85
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_014_amt*0.616 
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_014_amt*0.525
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) then subscriptions_revs_day_014_amt*0.7
             else 0 end, 0)
  ) as SUB_REVS_DAY_014_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then REVS_DAY_028_AMT*0.616
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then REVS_DAY_028_AMT*0.525
         else REVS_DAY_028_AMT*0.7 
      end
  ) as IAP_REVS_DAY_028_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then AD_REVS_DAY_028_AMT*0.88
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then AD_REVS_DAY_028_AMT*0.75
         else AD_REVS_DAY_028_AMT*0.
      end
  ) as AD_REVS_DAY_028_AMT,
  sum(
       ifnull(case when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_028_amt*0.616 
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_028_amt*0.525
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' then subscriptions_revs_day_028_amt *0.7
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_028_amt*0.748
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_028_amt*0.638
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' then subscriptions_revs_day_028_amt *0.85
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_028_amt*0.748 
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_028_amt*0.638
             when MARKET_CD = 'IT' and CALENDAR_DT < date_add(current_date(),-366) then subscriptions_revs_day_028_amt*0.85
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Harry Potter' then subscriptions_revs_day_028_amt*0.616 
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then subscriptions_revs_day_028_amt*0.525
             when MARKET_CD = 'IT' and CALENDAR_DT >= date_add(current_date(),-366) then subscriptions_revs_day_028_amt*0.7
             else 0 end, 0)
  ) as SUB_REVS_DAY_028_AMT,
  sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then LTV_365_LASTEST_VAL*0.616
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then LTV_365_LASTEST_VAL*0.525
         else LTV_365_LASTEST_VAL*0.7 
      end
  ) as IAP_LTV,
  sum(
      case 
        when APPLICATION_FAMILY_NAME = 'Harry Potter' then AD_LTV_365_LASTEST_VAL*0.88
        when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then AD_LTV_365_LASTEST_VAL*0.75
        else AD_LTV_365_LASTEST_VAL 
      end
  ) as AD_LTV
from pr_promo_tb
group by 1, 2, 3, 4, 5, 6
having sum(EXPENSE_AMT) > 0 or sum(USER_QTY) > 0
order by 1, 2, 3, 4, 5, 6
)
select *
from without_total_pltv
  """
  df = spark.sql(sql)
  return df

# COMMAND ----------

if ENVIRONMENT == Environment.DEVELOPMENT:
  campaign_without_sub_pltv_df = get_campaign_without_sub_ltv()
  display(campaign_without_sub_pltv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Campaign SUB_LTV from ua.individul and install table

# COMMAND ----------

def get_campaign_sub_ltv() -> ps.DataFrame:
  """
  Return a dataframe with campaign-level daily sub pltv
  """
  sql = f"""
with indi_max_project_dt as (
  select
    ACCOUNT_ID, 
    USER_ID, 
    APPLICATION_CD, 
    max(projection_dt) as MAX_PROJECTION_DT
  from ua.internal_individual_ltv
  group by 1, 2, 3
),
latest_sub_ltv as (
  select 
    a.ACCOUNT_ID, 
    a.USER_ID, 
    a.APPLICATION_CD, 
    IAP_PROJECTEDREVENUE_365 as IAP_LTV,
    AD_PROJECTEDREVENUE_365 as AD_LTV,
    SUB_PROJECTEDREVENUE_365 as SUB_LTV
  from ua.internal_individual_ltv a inner join indi_max_project_dt b
  on a.ACCOUNT_ID = b.ACCOUNT_ID and a.USER_ID = b.USER_ID and a.APPLICATION_CD = b.APPLICATION_CD and a.PROJECTION_DT = b.MAX_PROJECTION_DT
),
indi_sub_ltv as (
  select
    a.APPLICATION_FAMILY_NAME,
    a.MARKET_CD,
    a.USER_SOURCE_TYPE_CD,
    a.CHANNEL_NAME,
    a.PROMOTION_NAME,
    a.INSTALL_DT as CALENDAR_DT,
    b.IAP_LTV,
    b.AD_LTV,
    b.SUB_LTV
  from pr_analytics_delta.install a inner join latest_sub_ltv b
  on a.APPLICATION_CD = b.APPLICATION_CD and a.ACCOUNT_ID = b.ACCOUNT_ID and a.USER_ID = b.USER_ID
)
select 
  APPLICATION_FAMILY_NAME,
  MARKET_CD,
  USER_SOURCE_TYPE_CD,
  case
    when USER_SOURCE_TYPE_CD in ('MM', 'MK', 'MC') then CHANNEL_NAME 
    else 'ORGANIC'
  end as CHANNEL_NAME,
  case
    when USER_SOURCE_TYPE_CD in ('MM', 'MK', 'MC') then PROMOTION_NAME
    else 'ORGANIC'
  end as PROMOTION_NAME,
  CALENDAR_DT,
  count(1) as INDIVIDUAL_INSTALLS,
  sum(
       ifnull(case when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then SUB_LTV*0.616 
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then SUB_LTV*0.525
             when MARKET_CD = 'GO' and CALENDAR_DT < '2022-01-01' then SUB_LTV *0.7
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Harry Potter' then SUB_LTV*0.748
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then SUB_LTV*0.638
             when MARKET_CD = 'GO' and CALENDAR_DT >= '2022-01-01' then SUB_LTV *0.85
             when MARKET_CD = 'IT' and APPLICATION_FAMILY_NAME = 'Harry Potter' then SUB_LTV*0.616 
             when MARKET_CD = 'IT' and APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then SUB_LTV*0.525
             when MARKET_CD = 'IT' then SUB_LTV*0.7
             else 0 end, 0)
  ) as SUB_LTV
from indi_sub_ltv
where APPLICATION_FAMILY_NAME in {tuple(GAME_LIST)} and MARKET_CD in {tuple(MARKET_LIST)} and CALENDAR_DT between '{START_DT}' and '{END_DT}'
group by 1, 2, 3, 4, 5, 6
order by 1, 2, 3, 4, 5, 6
  """
  df = spark.sql(sql)
  return df

# COMMAND ----------

if ENVIRONMENT == Environment.DEVELOPMENT:
  campaign_sub_pltv_df = get_campaign_sub_ltv()
  display(campaign_sub_pltv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge campaign_sub_ltv and campaign_without_sub to get campaign_details

# COMMAND ----------

def get_campaign_details(campaign_without_sub_pltv_df: ps.DataFrame, campaign_sub_pltv_df: ps.DataFrame) -> ps.DataFrame:
  """
  Merge campaign_without_sub_pltv_df and campaign_sub_pltv_df to get campaign details. 
  """
  df = campaign_without_sub_pltv_df.join(campaign_sub_pltv_df, on=['APPLICATION_FAMILY_NAME', 'MARKET_CD', 'USER_SOURCE_TYPE_CD', 'CHANNEL_NAME', 'PROMOTION_NAME', 'CALENDAR_DT'], how='left')
  df = df.na.fill(0, subset=['INDIVIDUAL_INSTALLS', 'SUB_LTV'])
  # ludia_df = df.where(F.col('APPLICATION_FAMILY_NAME').isin(LUDIA_TITLES))
  # jc_df = df.where(~F.col('APPLICATION_FAMILY_NAME').isin(LUDIA_TITLES))
  # ludia_df = ludia_df.withColumn('TOTAL_LTV', (F.sum('IAP_LTV') + F.sum('AD_LTV') + F.sum('SUB_LTV')))
  # jc_df = jc_df.withColumn('TOTAL_LTV', (F.sum('IAP_LTV') + F.sum('AD_LTV')))
  # df = ludia_df.unionByName(jc_df)
  df = df.withColumn('TOTAL_LTV', F.expr('IAP_LTV + AD_LTV + SUB_LTV'))
  df = df.drop('INDIVIDUAL_INSTALLS')
  return df

# COMMAND ----------

if ENVIRONMENT == Environment.DEVELOPMENT:
  campaign_details_df = get_campaign_details(campaign_without_sub_pltv_df, campaign_sub_pltv_df)
  display(campaign_details_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get SKAN Campaign pLTV from ua.skan_ltv

# COMMAND ----------

def get_skan_campaign_ltv() -> ps.DataFrame:
  """
  Get SKAN campaign level pLTV
  """
  sql = f"""
  select
    APPLICATION_FAMILY_NAME,
    'IT' as MARKET_CD,
    'SKAN' as SOURCE,
    CHANNEL_NAME,
    PROMOTION_CD as PROMOTION_NAME,
    INSTALL_DT as CALENDAR_DT,
    sum(INSTALL_NUM) as INSTALL_NUM,
    0 as RETENTION_DAY_001_QTY,
    0 as RETENTION_DAY_003_QTY,
    0 as RETENTION_DAY_007_QTY,
    0 as IAP_REVS_DAY_001_AMT,
    0 as AD_REVS_DAY_001_AMT,
    0 as SUB_REVS_DAY_001_AMT,
    0 as IAP_REVS_DAY_003_AMT,
    0 as AD_REVS_DAY_003_AMT,
    0 as SUB_REVS_DAY_003_AMT,
    0 as IAP_REVS_DAY_007_AMT,
    0 as AD_REVS_DAY_007_AMT,
    0 as SUB_REVS_DAY_007_AMT,
    0 as IAP_REVS_DAY_014_AMT,
    0 as AD_REVS_DAY_014_AMT,
    0 as SUB_REVS_DAY_014_AMT,
    0 as IAP_REVS_DAY_028_AMT,
    0 as AD_REVS_DAY_028_AMT,
    0 as SUB_REVS_DAY_028_AMT,
    sum(
      case 
         when APPLICATION_FAMILY_NAME = 'Harry Potter' then LTV_365_LATEST_VAL*0.616
         when APPLICATION_FAMILY_NAME = 'Jurassic World the Game' then LTV_365_LATEST_VAL*0.525
         else LTV_365_LATEST_VAL*0.7 
      end
    ) as IAP_LTV,
    0 as AD_LTV,
    0 as SUB_LTV,
    sum(
      case 
        when application_family_name = 'Bingo Pop' then LTV_365_LATEST_VAL*0.77
        when application_family_name = 'Cookie Jam' then LTV_365_LATEST_VAL*0.90
        when application_family_name = 'Cookie Jam Blast' then LTV_365_LATEST_VAL*1.09
        when application_family_name = 'Emoji Blitz' then LTV_365_LATEST_VAL*0.95
        when application_family_name = 'Genies and Gems' then LTV_365_LATEST_VAL*1.20
        when application_family_name = 'Harry Potter' and channel_name in ('SGNFB','TIKTOK','GOOGLE ADS','SKANUAC','MOLOCO','ADWORDS') then LTV_365_LATEST_VAL*0.85
        when application_family_name = 'Harry Potter' and channel_name not in ('SGNFB','TIKTOK','GOOGLE ADS','SKANUAC','MOLOCO','ADWORDS') then LTV_365_LATEST_VAL*0.77
        when application_family_name = 'Jurassic World Alive' then LTV_365_LATEST_VAL*0.91
        when application_family_name = 'Jurassic World the Game' then LTV_365_LATEST_VAL*0.80
        when application_family_name = 'Mahjong' then LTV_365_LATEST_VAL*1.28
        when application_family_name = 'Panda Pop' then LTV_365_LATEST_VAL*1.03 
        else LTV_365_LATEST_VAL*0.7 
      end
    ) as TOTAL_LTV
  from ua.skan_ltv
  where APPLICATION_FAMILY_NAME in {tuple(GAME_LIST)} and {SKAN_FILTER_COND} and INSTALL_DT >= '{START_DT}' and INSTALL_DT < current_date()
  group by 1, 2, 3, 4, 5, 6
  order by 1, 2, 3, 4, 5, 6
  """
  df = spark.sql(sql)
  return df

# COMMAND ----------

if ENVIRONMENT == Environment.DEVELOPMENT:
  skan_campaign_ltv_df = get_skan_campaign_ltv()
  display(skan_campaign_ltv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join SKAN with Non-SKAN to Get Complete LTV Info

# COMMAND ----------

def get_complete_campaign_details(campaign_details_df: ps.DataFrame, skan_campaign_ltv_df: ps.DataFrame) -> ps.DataFrame:
  """
  For SKAN campaigns, use SKAN LTV and general spend; For non-SKAN campaigns, use general details. Join on APPLICATION_FAMILY_NAME, MARKET_CD, CHANNEL_NAME, PROMOTION_NAME, CALENDAR_DT
  """
  # get skan campaign details
  campaign_spend_df = campaign_details_df.select('APPLICATION_FAMILY_NAME', 'MARKET_CD', 'USER_SOURCE_TYPE_CD', 'CHANNEL_NAME', 'PROMOTION_NAME', 'CALENDAR_DT', 'SPEND')
  skan_campaign_details_df = skan_campaign_ltv_df.join(campaign_spend_df, on=['APPLICATION_FAMILY_NAME', 'MARKET_CD', 'CHANNEL_NAME', 'PROMOTION_NAME', 'CALENDAR_DT'], how='left')
  skan_campaign_details_df = skan_campaign_details_df.na.fill({'USER_SOURCE_TYPE_CD': 'MK', 'SPEND': 0})
  
  # get non-skan campaign details 
  other_campaign_details_df = campaign_details_df.join(skan_campaign_details_df, on=['APPLICATION_FAMILY_NAME', 'MARKET_CD', 'CHANNEL_NAME', 'PROMOTION_NAME', 'CALENDAR_DT'], how='leftanti')
  other_campaign_details_df = other_campaign_details_df.withColumn('SOURCE', F.lit('Non-SKAN'))
  
  campaign_details_df = skan_campaign_details_df.unionByName(other_campaign_details_df)
  campaign_details_df = campaign_details_df.withColumnRenamed('INSTALL_NUM', 'INSTALL')
  return campaign_details_df

# COMMAND ----------

if ENVIRONMENT == Environment.DEVELOPMENT:
  campaign_details_df = get_complete_campaign_details(campaign_details_df, skan_campaign_ltv_df)
  display(campaign_details_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate campaign_details_df on Channel

# COMMAND ----------

def aggregate_campaign_to_channel_details(campaign_details_df: ps.DataFrame) -> ps.DataFrame:
  """
  Take a campaign-level detail df and aggregate into channel-level details
  The input df includes 'APPLICATION_FAMILY_NAME', 'MARKET_CD', 'SOURCE', 'USER_SOURCE_TYPE_CD', 'CHANNEL_NAME', 'CALENDAR_DT', spend, install_num, retention, rev, and ltv
  """
  channel_details_df = campaign_details_df.\
                              groupby('APPLICATION_FAMILY_NAME', 'MARKET_CD', 'SOURCE', 'USER_SOURCE_TYPE_CD', 'CHANNEL_NAME', 'CALENDAR_DT').\
                              agg(
                                F.sum('SPEND').alias('SPEND'),
                                F.sum('INSTALL').alias('INSTALL'),
                                F.sum('RETENTION_DAY_001_QTY').alias('RETENTION_DAY_001_QTY'),
                                F.sum('RETENTION_DAY_003_QTY').alias('RETENTION_DAY_003_QTY'),
                                F.sum('RETENTION_DAY_007_QTY').alias('RETENTION_DAY_007_QTY'),
                                F.sum('IAP_REVS_DAY_001_AMT').alias('IAP_REVS_DAY_001_AMT'),
                                F.sum('AD_REVS_DAY_001_AMT').alias('AD_REVS_DAY_001_AMT'),
                                F.sum('SUB_REVS_DAY_001_AMT').alias('SUB_REVS_DAY_001_AMT'),
                                F.sum('IAP_REVS_DAY_003_AMT').alias('IAP_REVS_DAY_003_AMT'),
                                F.sum('AD_REVS_DAY_003_AMT').alias('AD_REVS_DAY_003_AMT'),
                                F.sum('SUB_REVS_DAY_003_AMT').alias('SUB_REVS_DAY_003_AMT'),
                                F.sum('IAP_REVS_DAY_007_AMT').alias('IAP_REVS_DAY_007_AMT'),
                                F.sum('AD_REVS_DAY_007_AMT').alias('AD_REVS_DAY_007_AMT'),
                                F.sum('SUB_REVS_DAY_007_AMT').alias('SUB_REVS_DAY_007_AMT'),
                                F.sum('IAP_REVS_DAY_014_AMT').alias('IAP_REVS_DAY_014_AMT'),
                                F.sum('AD_REVS_DAY_014_AMT').alias('AD_REVS_DAY_014_AMT'),
                                F.sum('SUB_REVS_DAY_014_AMT').alias('SUB_REVS_DAY_014_AMT'),
                                F.sum('IAP_REVS_DAY_028_AMT').alias('IAP_REVS_DAY_028_AMT'),
                                F.sum('AD_REVS_DAY_028_AMT').alias('AD_REVS_DAY_028_AMT'),
                                F.sum('SUB_REVS_DAY_028_AMT').alias('SUB_REVS_DAY_028_AMT'),
                                F.sum('IAP_LTV').alias('IAP_LTV'),
                                F.sum('AD_LTV').alias('AD_LTV'),
                                F.sum('SUB_LTV').alias('SUB_LTV'),
                                F.sum('TOTAL_LTV').alias('TOTAL_LTV')
                              )
  return channel_details_df

# COMMAND ----------

if ENVIRONMENT == Environment.DEVELOPMENT:
  channel_details_df = aggregate_campaign_to_channel_details(campaign_details_df)
  display(channel_details_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### QA Result

# COMMAND ----------

def aggregate_to_game_details(channel_details_df: ps.DataFrame) -> ps.DataFrame:
  """
  Aggregate campaign- or channel-level performance into game-level metrics, which includes market_cd, source, last_install_dt, total_spend, total_installs and total_ltv
  """
  game_agg_df = channel_details_df.\
                  groupby('APPLICATION_FAMILY_NAME', 'MARKET_CD', 'SOURCE').\
                  agg(
                    F.max('CALENDAR_DT').alias('LAST_INSTALL_DT'),
                    F.sum('SPEND').alias('TOTAL_SPEND'),
                    F.sum('INSTALL').alias('TOTAL_INSTALL')
                  )
  return game_agg_df

class PredictionError(Exception):
  def __init__(self, game, market_cd, source, exist_max_install_dt, exist_total_spend, exist_total_install_num, new_max_install_dt, new_total_spend, new_total_install_num):
    self.game = game
    self.market_cd = market_cd
    self.source = source
    self.exist_max_install_dt = exist_max_install_dt
    self.exist_total_spend = exist_total_spend
    self.exist_total_install_num = exist_total_install_num
    self.new_max_install_dt = new_max_install_dt
    self.new_total_spend = new_total_spend
    self.new_total_install_num = new_total_install_num
    super().__init__()

  def __str__(self):
    msg = f"""
        Prediction error for {self.game} {self.market_cd} {self.source}:
        Before prediction, there are {self.exist_total_install_num} installs up to {datetime.strftime(self.exist_max_install_dt, DATE_FORMAT)} for ${self.exist_total_spend:.2f}.
        After prediction, there are {self.new_total_install_num} installs up to {datetime.strftime(self.new_max_install_dt, DATE_FORMAT)} for ${self.new_total_spend:.2f}.
         """
    return msg

def get_old_channel_details_df() -> ps.DataFrame:
  sql = f"select * from {CAMPAIGN_TABLE_NAME}"
  df = spark.sql(sql)
  return df
  
def qa_result_by_game(row: pd.Series) -> None:
  if (row['NEW_LAST_INSTALL_DT'] < row['OLD_LAST_INSTALL_DT']) or \
     (row['NEW_TOTAL_INSTALL'] < (row['OLD_TOTAL_INSTALL']-1)):
    raise PredictionError(row['APPLICATION_FAMILY_NAME'], row['MARKET_CD'], row['SOURCE'], row['OLD_LAST_INSTALL_DT'], row['OLD_TOTAL_SPEND'], row['OLD_TOTAL_INSTALL'], row['NEW_LAST_INSTALL_DT'], row['NEW_TOTAL_SPEND'], row['NEW_TOTAL_INSTALL'])
    
def qa_result(old_channel_details_df: ps.DataFrame, new_channel_details_df: ps.DataFrame) -> None:
  """
  QA the new merge result. If on any platform, we find a smaller last_install_dt, or a smaller spend, or a smaller install_num, report an error
  """
  old_agg_df = aggregate_to_game_details(old_channel_details_df).toPandas()
  old_agg_df = old_agg_df.rename(columns={
    'LAST_INSTALL_DT': 'OLD_LAST_INSTALL_DT',
    'TOTAL_SPEND': 'OLD_TOTAL_SPEND',
    'TOTAL_INSTALL': 'OLD_TOTAL_INSTALL'
  })
  
  new_agg_df = aggregate_to_game_details(new_channel_details_df).toPandas()
  new_agg_df = new_agg_df.rename(columns={
    'LAST_INSTALL_DT': 'NEW_LAST_INSTALL_DT',
    'TOTAL_SPEND': 'NEW_TOTAL_SPEND',
    'TOTAL_INSTALL': 'NEW_TOTAL_INSTALL'
  })
  
  comp_agg_df = old_agg_df.merge(new_agg_df, on=['APPLICATION_FAMILY_NAME', 'MARKET_CD', 'SOURCE'], how='inner')
  comp_agg_df.apply(qa_result_by_game, axis=1)

# COMMAND ----------

if ENVIRONMENT == Environment.DEVELOPMENT:
  old_channel_details_df = get_old_channel_details_df()
  qa_result(old_channel_details_df, channel_details_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Result

# COMMAND ----------

def revise_schema(df: ps.DataFrame) -> ps.DataFrame:
  """
  Change certain column names to lower case
  """
  columns = ['APPLICATION_FAMILY_NAME', 'MARKET_CD', 'CHANNEL_NAME', 'USER_SOURCE_TYPE_CD', 'PROMOTION_NAME', 'CALENDAR_DT', 'SOURCE', 'INSTALL', 'SPEND']
  for column in columns:
    df = df.withColumnRenamed(column, column.lower())
  return df

def save_result(campaign_details_df: ps.DataFrame, channel_details_df: ps.DataFrame) -> None:
  campaign_details_df = revise_schema(campaign_details_df)
  campaign_details_df.write.format('parquet').mode('overwrite').partitionBy('APPLICATION_FAMILY_NAME').save(CAMPAIGN_PARQUET_PATH)
  campaign_details_df.write.mode('overwrite').partitionBy('APPLICATION_FAMILY_NAME').saveAsTable(CAMPAIGN_TABLE_NAME)
  
  channel_details_df = revise_schema(channel_details_df)
  channel_details_df.write.format('parquet').mode('overwrite').partitionBy('APPLICATION_FAMILY_NAME').save(CHANNEL_PARQUET_PATH)
  channel_details_df.write.mode('overwrite').partitionBy('APPLICATION_FAMILY_NAME').saveAsTable(CHANNEL_TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Function

# COMMAND ----------

def merge_skan_performance_channel_details():
  # get campaign details
  campaign_without_sub_pltv_df = get_campaign_without_sub_ltv()
  campaign_sub_pltv_df = get_campaign_sub_ltv()
  campaign_details_df = get_campaign_details(campaign_without_sub_pltv_df, campaign_sub_pltv_df)
  
  # get skan channel details
  skan_campaign_ltv_df = get_skan_campaign_ltv()
  
  # Join SKAN with Non-SKAN to Get Complete LTV Info
  campaign_details_df = get_complete_campaign_details(campaign_details_df, skan_campaign_ltv_df)
  
  # Aggregate campaign_details_df on Channel
  channel_details_df = aggregate_campaign_to_channel_details(campaign_details_df)
  
  # qa
  old_channel_details_df = get_old_channel_details_df()
  qa_result(old_channel_details_df, channel_details_df)
  
  # save result
  if SAVE_FLAG:
    save_result(campaign_details_df, channel_details_df)
    
  return campaign_details_df, channel_details_df

# COMMAND ----------

if ENVIRONMENT == Environment.PRODUCTION:
  campaign_details_df, channel_details_df = merge_skan_performance_channel_details()
  display(channel_details_df)

# COMMAND ----------


