drop table DMR005_LIST_STAJ_EXP if exists; --20181120 для Льготников соединение идёт по INNER JOIN
--общие расчеты 
#P_COMMENT# /*
drop table tmp_check_SPU_STAJ_AGR_OBSH if exists;
create table tmp_check_SPU_STAJ_AGR_OBSH (checkk varchar(64), ttime timestamp default  current_timestamp) ;
#P_COMMENT# */

-- Шаг 1
#P_COMMENT# /*
insert into tmp_check_SPU_STAJ_AGR_OBSH (checkk)  values ('DMR005_LIST_STAJ_GID');
#P_COMMENT# */

drop table DMR005_LIST_STAJ_GID if exists; --TMP_F_SPU_STAJ_AGGR_XX01_GID
create table DMR005_LIST_STAJ_GID as
(
select distinct
ia.INDIVIDUAL_ACCOUNT_GID
from
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia
where
ia.UPDATE_DATE between to_timestamp('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss') and to_timestamp('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss')
and '#P_FLG_INC#' = 'INC'
AND ia.SRC_CODE = 2
#P_SRC_CONDITION#

UNION

select distinct
ia.INDIVIDUAL_ACCOUNT_GID
from
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE ie
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia
		ON ia.AGENT_GID = ie.AGENT_GID 
		AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ia.START_DATE AND ia.END_DATE
		AND ia.SRC_CODE = 2
		AND '#P_FLG_INC#' = 'INC'	
		AND ie.SRC_CODE=2	
		and ie.UPDATE_DATE between to_timestamp('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss') and to_timestamp('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss')
		#P_SRC_CONDITION#

UNION

select distinct
ia.INDIVIDUAL_ACCOUNT_GID
FROM
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_AND_SPU_DOCUMENT ED 
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE ie
	ON IE.INDIVIDUAL_EXPERIENCE_GID = ED.INDIVIDUAL_EXPERIENCE_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ie.START_DATE AND ie.END_DATE
	AND ED.SRC_CODE=2
	AND ED.UPDATE_DATE between to_timestamp('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss') and to_timestamp('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss')
	AND ie.SRC_CODE=2	
	AND '#P_FLG_INC#' = 'INC'
INNER JOIN 
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia 
	ON ia.AGENT_GID = ie.AGENT_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ia.START_DATE AND ia.END_DATE
	AND ia.SRC_CODE = 2		
	#P_SRC_CONDITION# 	
	
UNION

select distinct
ia.INDIVIDUAL_ACCOUNT_GID
FROM
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.SPU_DOCUMENT SD 
inner join 	
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_AND_SPU_DOCUMENT ED 							
	ON ED.SPU_DOCUMENT_SZV_DETAIL_GID = SD.SPU_DOCUMENT_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ED.START_DATE AND ED.END_DATE
	AND SD.SRC_CODE=2 
	and sd.UPDATE_DATE between to_timestamp('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss') and to_timestamp('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss')
	AND ED.SRC_CODE=2
	AND '#P_FLG_INC#' = 'INC'
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE ie
	ON IE.INDIVIDUAL_EXPERIENCE_GID = ED.INDIVIDUAL_EXPERIENCE_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ie.START_DATE AND ie.END_DATE	
	AND ie.SRC_CODE=2	
INNER JOIN 
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia 
	ON ia.AGENT_GID = ie.AGENT_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ia.START_DATE AND ia.END_DATE
	AND ia.SRC_CODE = 2		
	#P_SRC_CONDITION# 	

UNION

select distinct
ia.INDIVIDUAL_ACCOUNT_GID
FROM
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.SPU_DOCUMENT_TYPE DT 	
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.SPU_DOCUMENT SD 
	ON SD.SPU_DOCUMENT_TYPE_GID  = DT.SPU_DOCUMENT_TYPE_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN SD.START_DATE AND SD.END_DATE
	AND DT.SRC_CODE=2	
	AND SD.SRC_CODE=2 
	and DT.UPDATE_DATE between to_timestamp('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss') and to_timestamp('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss')
	AND '#P_FLG_INC#' = 'INC'
inner join 	
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_AND_SPU_DOCUMENT ED 							
	ON ED.SPU_DOCUMENT_SZV_DETAIL_GID = SD.SPU_DOCUMENT_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ED.START_DATE AND ED.END_DATE
	AND SD.SRC_CODE=2 
	and sd.UPDATE_DATE between to_timestamp('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss') and to_timestamp('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss')
	AND ED.SRC_CODE=2	
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE ie
	ON IE.INDIVIDUAL_EXPERIENCE_GID = ED.INDIVIDUAL_EXPERIENCE_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ie.START_DATE AND ie.END_DATE	
	AND ie.SRC_CODE=2	
INNER JOIN 
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia 
	ON ia.AGENT_GID = ie.AGENT_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ia.START_DATE AND ia.END_DATE
	AND ia.SRC_CODE = 2		
	#P_SRC_CONDITION# 	

UNION

select distinct
ia.INDIVIDUAL_ACCOUNT_GID
FROM
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_DETAIL IED 
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_AND_SPU_DOCUMENT ED 	
	ON ED.INDIVIDUAL_EXPERIENCE_GID = IED.INDIVIDUAL_EXPERIENCE_GID		
	AND ED.SRC_CODE=2				
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN IED.START_DATE AND IED.END_DATE						
	AND IED.SRC_CODE = 2	
	and IED.UPDATE_DATE between to_timestamp('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss') and to_timestamp('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss')
	AND '#P_FLG_INC#' = 'INC'
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE ie
	ON IE.INDIVIDUAL_EXPERIENCE_GID = ED.INDIVIDUAL_EXPERIENCE_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ie.START_DATE AND ie.END_DATE	
	AND ie.SRC_CODE=2	
INNER JOIN 
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia 
	ON ia.AGENT_GID = ie.AGENT_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ia.START_DATE AND ia.END_DATE
	AND ia.SRC_CODE = 2		
	#P_SRC_CONDITION# 

UNION

select distinct
ia.INDIVIDUAL_ACCOUNT_GID
FROM
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_REASON IER 	
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_DETAIL IED 						
	ON IED.INDIVIDUAL_EXPERIENCE_REASON_GID = IER.INDIVIDUAL_EXPERIENCE_REASON_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN IED.START_DATE AND IED.END_DATE	
	AND IER.SRC_CODE = 2
	AND IED.SRC_CODE = 2
	and IER.UPDATE_DATE between to_timestamp('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss') and to_timestamp('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss')
	AND '#P_FLG_INC#' = 'INC'
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_AND_SPU_DOCUMENT ED 	
	ON ED.INDIVIDUAL_EXPERIENCE_GID = IED.INDIVIDUAL_EXPERIENCE_GID		
	AND ED.SRC_CODE=2				
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN IED.START_DATE AND IED.END_DATE
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE ie
	ON IE.INDIVIDUAL_EXPERIENCE_GID = ED.INDIVIDUAL_EXPERIENCE_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ie.START_DATE AND ie.END_DATE	
	AND ie.SRC_CODE=2	
INNER JOIN 
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia 
	ON ia.AGENT_GID = ie.AGENT_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ia.START_DATE AND ia.END_DATE
	AND ia.SRC_CODE = 2		
	#P_SRC_CONDITION# 

UNION

select distinct
ia.INDIVIDUAL_ACCOUNT_GID
FROM
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.ZMTD_FLAG_VALUE CT
INNER JOIN 
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_REASON IER 				
	ON IER.REASON_TYPE_FLG_GID = CT.FLAG_VALUE_GID 
	and CT.UPDATE_DATE between to_timestamp('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss') and to_timestamp('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss')
--AND CT.ENTITY_CODE = 425 
	AND CT.SRC_CODE = 2
--AND FLAG_GID = 241
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN IER.START_DATE AND IER.END_DATE
	AND IER.SRC_CODE = 2
	AND '#P_FLG_INC#' = 'INC'
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_DETAIL IED 						
	ON IED.INDIVIDUAL_EXPERIENCE_REASON_GID = IER.INDIVIDUAL_EXPERIENCE_REASON_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN IED.START_DATE AND IED.END_DATE	
	AND IED.SRC_CODE = 2
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_AND_SPU_DOCUMENT ED 	
	ON ED.INDIVIDUAL_EXPERIENCE_GID = IED.INDIVIDUAL_EXPERIENCE_GID		
	AND ED.SRC_CODE=2				
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN IED.START_DATE AND IED.END_DATE
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE ie
	ON IE.INDIVIDUAL_EXPERIENCE_GID = ED.INDIVIDUAL_EXPERIENCE_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ie.START_DATE AND ie.END_DATE	
	AND ie.SRC_CODE=2	
INNER JOIN 
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia 
	ON ia.AGENT_GID = ie.AGENT_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ia.START_DATE AND ia.END_DATE
	AND ia.SRC_CODE = 2		
	#P_SRC_CONDITION# 						
)
DISTRIBUTE on (INDIVIDUAL_ACCOUNT_GID);

#P_COMMENT# /*
insert into tmp_check_SPU_STAJ_AGR_OBSH (checkk)  values ('DMR005_LIST_STAJ_AGENT');
#P_COMMENT# */

drop table DMR005_LIST_STAJ_AGENT if exists; --20181120 по этой части у Льготников полное совпадение
create table DMR005_LIST_STAJ_AGENT as --TMP_F_SPU_STAJ_AGGR_XX02_AGENT
(
select
distinct
ie.INDIVIDUAL_EXPERIENCE_GID,
ie.EXPERIENCE_START_DATE as PERIOD_BEGIN,
ie.EXPERIENCE_END_DATE as PERIOD_END,
--ia.AGENT_GID,
ia.INDIVIDUAL_ACCOUNT_GID,
ED.SPU_DOCUMENT_SZV_DETAIL_GID,
ia.REG_DATE
--,'0'::boolean as FLG_CLOSE
FROM
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia 
INNER JOIN 
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE ie
		ON ia.AGENT_GID = ie.AGENT_GID 
		AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ia.START_DATE AND ia.END_DATE
		AND ia.SRC_CODE = 2
		--AND ia.INDIVIDUAL_ACCOUNT_GID not in (1,0) --+
		AND '#P_FLG_INC#' = 'ALL'
		AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ie.START_DATE AND ie.END_DATE
		AND ie.SRC_CODE=2	
		#P_SRC_CONDITION# 	
JOIN #ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_AND_SPU_DOCUMENT ED 	
		ON IE.INDIVIDUAL_EXPERIENCE_GID = ED.INDIVIDUAL_EXPERIENCE_GID 
		AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ED.START_DATE AND ED.END_DATE
		AND ED.SRC_CODE=2
where IE.EXPERIENCE_START_DATE <> '7777-01-01 00:00:00' 
AND IE.EXPERIENCE_END_DATE <> '7777-01-01 00:00:00'
AND ie.VALID_TO_DATE = '9999-12-31 23:59:59'

UNION ALL

select
distinct
ie.INDIVIDUAL_EXPERIENCE_GID,
ie.EXPERIENCE_START_DATE as PERIOD_BEGIN,
ie.EXPERIENCE_END_DATE as PERIOD_END,
--ia.AGENT_GID,
ia.INDIVIDUAL_ACCOUNT_GID,
ED.SPU_DOCUMENT_SZV_DETAIL_GID,
ia.REG_DATE
from
DMR005_LIST_STAJ_GID gid
inner join
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_ACCOUNT ia 
	on gid.INDIVIDUAL_ACCOUNT_GID=ia.INDIVIDUAL_ACCOUNT_GID
	and to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ia.START_DATE AND ia.END_DATE
	AND ia.SRC_CODE = 2
	AND '#P_FLG_INC#' = 'INC'
INNER JOIN 
#ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE ie
		ON ia.AGENT_GID = ie.AGENT_GID 		
		AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ie.START_DATE AND ie.END_DATE
		AND ie.SRC_CODE=2		
JOIN #ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_AND_SPU_DOCUMENT ED 	
		ON IE.INDIVIDUAL_EXPERIENCE_GID = ED.INDIVIDUAL_EXPERIENCE_GID 
		AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN ED.START_DATE AND ED.END_DATE
		AND ED.SRC_CODE=2
where IE.EXPERIENCE_START_DATE <> '7777-01-01 00:00:00' 
AND IE.EXPERIENCE_END_DATE <> '7777-01-01 00:00:00'	
AND ie.VALID_TO_DATE = '9999-12-31 23:59:59'	
)
DISTRIBUTE ON(SPU_DOCUMENT_SZV_DETAIL_GID,INDIVIDUAL_EXPERIENCE_GID);

#P_COMMENT# /*
insert into tmp_check_SPU_STAJ_AGR_OBSH (checkk)  values ('DMR005_LIST_STAJ_SPU');
#P_COMMENT# */

drop table DMR005_LIST_STAJ_SPU if exists; --20181120 по этой части у Льготников полное совпадение
create table DMR005_LIST_STAJ_SPU as --TMP_F_SPU_STAJ_AGGR_XX03_SPU
(
select
distinct
INDIVIDUAL_EXPERIENCE_GID,
PERIOD_BEGIN,
PERIOD_END,
--AGENT_GID,
tmp.INDIVIDUAL_ACCOUNT_GID,
REG_DATE,
DT.CODE
--,'0'::boolean as FLG_CLOSE
FROM
DMR005_LIST_STAJ_AGENT tmp
inner join
 #ps_nz_DWH.Database#.#ps_nz_DWH.Username#.SPU_DOCUMENT SD 								
	ON tmp.SPU_DOCUMENT_SZV_DETAIL_GID = SD.SPU_DOCUMENT_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN sd.START_DATE AND sd.END_DATE
	AND SD.SRC_CODE=2 
	AND SD.VALID_TO_DATE = '9999-12-31 23:59:59' --VALID_TO_DATE ? SPU_DOCUMENT_END
	--sd.SPU_DOCUMENT_END = '9999-12-31 23:59:59'
	AND SD.ACCOUNT_DATE > '1000-01-01 00:00:00'
inner JOIN #ps_nz_DWH.Database#.#ps_nz_DWH.Username#.SPU_DOCUMENT_TYPE DT 							
	ON SD.SPU_DOCUMENT_TYPE_GID  = DT.SPU_DOCUMENT_TYPE_GID 
	AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN dt.START_DATE AND dt.END_DATE
	AND DT.SRC_CODE=2	
	AND DT.CODE IN 
	('СЗВ-1','СЗВ-3','СЗВ-4-1','СЗВ-4-2','СЗВ-6-1','СЗВ-6-2','СЗВ-6-3','СЗВ-6-4','РСВ1-2014','РСВ1-2015',--'СЗВ-К',
	'СЗВ-К+','СЗВ-СП', 'СЗВ-ИСХ', 'СЗВ-СТАЖ'  ,'СЗВ-КОРР', 'СЗВ-К', 'СЗВ7', 'РСВР-РСВ2', 'ФП', 'СПВ-1', 'СПВ-2', 'РД ДоСВ', 'РасчСВ_ОПС') --в отличие от Треб. добавилось СЗВ-К просто +СЗВ7 по маппингу
)
DISTRIBUTE ON(INDIVIDUAL_EXPERIENCE_GID);


#P_COMMENT# /*
insert into tmp_check_SPU_STAJ_AGR_OBSH (checkk)  values ('DMR005_LIST_STAJ_EXP');
#P_COMMENT# */

--нужно сначала получить массив данных, выбрав их наименьшее количество
--потом уже для этого массива рассматривать кейсы или делать какие-то фильтры
--добавляется поле EXP_CODE (?)

--TMP_F_SPU_STAJ_AGGR_XX04_ADD
create table DMR005_LIST_STAJ_EXP as --20181120 для Льготников добавляется ограничение на INDIVIDUAL_EXPERIENCE_REASON.CODE 
(
select 
*
from
(
	select
	distinct
	PERIOD_BEGIN,
	PERIOD_END,
	--AGENT_GID,
	INDIVIDUAL_ACCOUNT_GID,
	REG_DATE,
	nvl(IER.CODE,'-') as IER_CODE,
	CT.NUM,
	nvl(ME.ENTITY_CODE,437) as entity_code,--?
	TMP.CODE,
	CASE WHEN TMP.CODE not in('СЗВ-К+','СЗВ-К') and ME.ENTITY_CODE = 437 AND (MIN(ME.ENTITY_CODE) OVER (PARTITION BY tmp.INDIVIDUAL_ACCOUNT_GID, IED.INDIVIDUAL_EXPERIENCE_GID )= 436) then 0 else 1 end as FLG_TAKE
	--,'0'::boolean as FLG_CLOSE
	/*CASE WHEN CT.NUM = 2 AND IER.CODE IN ('АДМИНИСТР','ДЛДЕТИ','НЕОПЛ','ЧАЭС') --здесь рассматриваются не все коды, которые в общем могут быть
		THEN 1 
		ELSE 0 --в расчет
	END AS EXP_LIMIT,

	CASE WHEN ME.ENTITY_CODE = 437 AND (MIN(ME.ENTITY_CODE) OVER (PARTITION BY IE.AGENT_GID, IED.INDIVIDUAL_EXPERIENCE_GID )= 436) and  DT.CODE != 'СЗВ-К+' 
		THEN 1 
		ELSE 0 --в работу
	END AS T_CHCK,

	CASE WHEN DT.CODE='СЗВ-К+' AND EXPERIENCE_START_DATE<REG_DATE AND COALESCE(IER.CODE,'-' ) IN  ('РАБОТА', 'РАБСВПК', 'РАБВОВ', 'РАБЛОК') 
		THEN 1 
	WHEN DT.CODE = 'СЗВ-К+' AND EXPERIENCE_START_DATE<'2002-01-01' AND COALESCE(IER.CODE,'-' ) IN  ('РАБЗАГР', 'БЕЗР', 'ПРЗАГР', 'ПРОЖСУПР', 'РЕАБИЛИТ', 'СЛДАПБЛ', 'СЛПРИЗ', 'СТРАХБЛ', 'СЛУЖБА', 'ТРУДЛИЦО', 'УХОД-ДЕТИ', 'УХОД-ИНВД') 
		THEN 1 
	WHEN DT.CODE IN ('СЗВ-1','СЗВ-3') AND EXPERIENCE_START_DATE<'2002-01-01' 												
		THEN 1 
	WHEN DT.CODE IN ('СЗВ-4-1','СЗВ-4-2') AND EXPERIENCE_START_DATE<'2010-01-01' AND EXPERIENCE_END_DATE>'2001-12-31' 	
		THEN 1 
	WHEN DT.CODE IN ('СЗВ-6-1','СЗВ-6-2','СЗВ-6-3','СЗВ-6-4') AND EXPERIENCE_START_DATE<'2014-01-01' AND EXPERIENCE_END_DATE>'2009-12-31'  	
		THEN 1 
	WHEN DT.CODE IN ('РСВ1-2014','РСВ1-2015', 'СЗВ7') AND EXPERIENCE_START_DATE<'2017-01-01' AND EXPERIENCE_END_DATE>'2013-12-31'  	
		THEN 1 
		WHEN DT.CODE = 'СЗВ-СТАЖ' AND EXPERIENCE_END_DATE>'2016-12-31'   	
		THEN 1 
	WHEN DT.CODE = 'СЗВ-ИСХ' AND EXPERIENCE_START_DATE<'2017-01-01' 												
		THEN 1 
	WHEN DT.CODE IN ('СЗВ-СП'  ,'СЗВ-КОРР' ) 																											
		THEN 1 --в работу
		ELSE 0 
	END AS REG_ROW*/
	,case when not (NUM = 2 and IER_CODE IN ('АДМИНИСТР','ДЛДЕТИ','НЕОПЛ','ЧАЭС') --исключить все такие документы
					) then 1 else 0 end 
	as FLG_STAJ_AGR,
	--20181120 для Льготников нет этого ограничения! вместо этого DWH.ZMTD_FLAG_VALUE.NUM in (3,5,6)
	case when NUM in (3,5,6) 
		and CT.FLAG_VALUE_GID is not null 
		and nvl(IER.CODE,'-') IN ('27-СМ', '27-СМХР', '28-СМ', '28-СМХР', 'ХИРУРСМ', '27-ГД', '27-ГДХР', '28-ГД', '28-ГДХР', 'ЗП81ГД', 'ХИРУРГД', '27-ПД', '27-ПДРК', '28-ПД', '28-ПДРК', 'ЗП80ПД', 'ЗП80ПДРК', 'ЗГД', 'ЗГДС', 'ЗГГС', 'ЗМД', 'ЗМС', 'РКС', 'МКС', 'ТВОРЧ15', 'ТВОРЧ20', 'ТВОРЧ25', 'ТВОРЧ30')
		then 1 else 0 end as FLG_STAJ_LGT
	FROM
	DMR005_LIST_STAJ_SPU TMP
	LEFT JOIN #ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_DETAIL IED 	--почему лефт и что будет, если данные null?
							ON TMP.INDIVIDUAL_EXPERIENCE_GID = IED.INDIVIDUAL_EXPERIENCE_GID 
							--AND IED.END_DATE = '9999-12-31'
							AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN IED.START_DATE AND IED.END_DATE						
							AND IED.SRC_CODE = 2
	LEFT JOIN #ps_nz_DWH.Database#.#ps_nz_DWH.Username#.M_INDIVIDUAL_EXPERIENCE_DETAIL ME 	
							ON IED.INDIVIDUAL_EXPERIENCE_DETAIL_GID = ME.INDIVIDUAL_EXPERIENCE_DETAIL_GID 
							AND ME.SRC_CODE = 2
							AND ME.ENTITY_CODE in (436,437)
	LEFT JOIN #ps_nz_DWH.Database#.#ps_nz_DWH.Username#.INDIVIDUAL_EXPERIENCE_REASON IER 	
							ON IED.INDIVIDUAL_EXPERIENCE_REASON_GID = IER.INDIVIDUAL_EXPERIENCE_REASON_GID 
							AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN IER.START_DATE AND IER.END_DATE	
							AND IER.SRC_CODE = 2
	LEFT JOIN #ps_nz_DWH.Database#.#ps_nz_DWH.Username#.ZMTD_FLAG_VALUE CT				
							ON IER.REASON_TYPE_FLG_GID = CT.FLAG_VALUE_GID 
							--AND CT.ENTITY_CODE = 425 
							--AND CT.SRC_CODE = 2
							AND FLAG_GID = 241
							AND to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') BETWEEN CT.START_DATE AND CT.END_DATE
)t

) 
DISTRIBUTE ON(INDIVIDUAL_ACCOUNT_GID)
organize on (FLG_TAKE,FLG_STAJ_AGR,FLG_STAJ_LGT);

#P_COMMENT# /*
insert into tmp_check_SPU_STAJ_AGR_OBSH (checkk)  values ('END');
#P_COMMENT# */

--здесь нельзя удалять первую с gid и последнюю
#P_COMMENT# /*
drop table DMR005_LIST_STAJ_SPU if exists;
#P_COMMENT# */