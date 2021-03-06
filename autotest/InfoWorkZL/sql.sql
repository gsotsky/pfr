CREATE TEMP TABLE tmp_table ON COMMIT DROP AS  
  with
    WZL as (
      select
        DIA.SNILSCS as SNILS,
        M.YEAR_NAME as REPORT_YEAR,
        M.MONTH_NUM_IN_YEAR as REPORT_MONTH,
        M.MONTH_START_DATE,
        M.MONTH_END_DATE
      from D_INDIVIDUAL_ACCOUNT as DIA
      join D_SPU_AGENT_DATA as AD on DIA.INDIVIDUAL_ACCOUNT_GID = AD.INDIVIDUAL_ACCOUNT_GID
      join F_SPU_AND_ASV_WORKING_ZL as ZL on ZL.INDIVIDUAL_ACCOUNT_GID = DIA.INDIVIDUAL_ACCOUNT_GID
      join D_MONTH as M on ZL.MONTH_GID = M.MONTH_GID
      where ZL.END_DATE = '9999-12-31'
        and ('2016-01-01'::date <= M.MONTH_START_DATE) and (M.MONTH_END_DATE < to_date((extract(year from localtimestamp(0)) + 1)::text, 'YYYY'))
    )
  select distinct
    WZL.SNILS,
    IC.LASTNAME::text,
    IC.FIRSTNAME::text,
    IC.MIDDLENAME::text,
 min(WZL.REPORT_YEAR || WZL.REPORT_MONTH) as MIN_PERIOD,
    max(WZL.REPORT_YEAR || WZL.REPORT_MONTH) as MAX_PERIOD
  from WZL
  join D_ASV_INSURER_CARD as IC on WZL.SNILS = IC.INSSNILS and WZL.MONTH_END_DATE >= IC.REGDATE and IC.UNREGDATE >= WZL.MONTH_START_DATE
  --join D_ASV_LEGAL_DATA as LD on LD.AGENT_GID = IC.INSURER_AGENT_GID
  --join F_ASV_INSURERS as I on I.INSURER_AGENT_GID = IC.INSURER_AGENT_GID
  --join D_ASV_LEGAL_ADDRESS as LA on LA.LEGAL_INSURER_ADDRESS_GID = I.LEGAL_INSURER_ADDRESS_GID
  group by wzl.snils, ic.lastname, ic.firstname, ic.middlename
  limit 10;
  
SELECT
  SNILS,
  LASTNAME,
  FIRSTNAME,
  MIDDLENAME,
  LPAD(SUBSTRING(MIN_PERIOD from 5 for 2), 2, '0') || '.' || SUBSTRING(MIN_PERIOD from 1 for 4) as MIN_PERIOD,
  LPAD(SUBSTRING(MAX_PERIOD from 5 for 2), 2, '0') || '.' || SUBSTRING(MAX_PERIOD from 1 for 4) as MAX_PERIOD
FROM tmp_table;
