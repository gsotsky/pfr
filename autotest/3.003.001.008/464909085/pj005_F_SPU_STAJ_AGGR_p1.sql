drop table F_SPU_STAJ_AGGR_p1 if exists;

#P_COMMENT# /*
drop table tmp_check_F_SPU_STAJ_AGGRE if exists;
create table tmp_check_F_SPU_STAJ_AGGRE (checkk varchar(64), ttime timestamp default  current_timestamp) ;
#P_COMMENT# */

-- Шаг 1
#P_COMMENT# /*
insert into tmp_check_F_SPU_STAJ_AGGRE (checkk)  values ('TMP_F_SPU_STAJ_AGGR_XX01_PERIODS');
#P_COMMENT# */

drop table TMP_F_SPU_STAJ_AGGR_XX01_PERIODS if exists;
create table TMP_F_SPU_STAJ_AGGR_XX01_PERIODS as
(
select
--AGENT_GID,
INDIVIDUAL_ACCOUNT_GID,
GRP_NUM,
MIN(PERIOD_BEGIN) DT_BEG,
MAX(PERIOD_END) DT_END								
from
(
	select E.*,
	SUM(GRP_START)OVER(PARTITION BY INDIVIDUAL_ACCOUNT_GID ORDER BY PERIOD_BEGIN,PERIOD_END) GRP_NUM
	from
	(	
		select t.*,
		CASE WHEN PERIOD_BEGIN > 1 + MAX(PERIOD_END)OVER(PARTITION BY INDIVIDUAL_ACCOUNT_GID ORDER BY PERIOD_BEGIN,PERIOD_END ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 
		THEN 1 ELSE 0 END AS GRP_START --если 1, то начало нового периода, для которого берется period_begin из текущей строки
		from
		(
		--и для енд, и для старт первый день стажа тоже берем
		--по алгоритму решили, что 31 день превращаем в 30 просто так везде
			select
			distinct
			PERIOD_BEGIN,
			PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where CODE IN ('СЗВ-СП','СЗВ-КОРР')
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1

			UNION ALL 

			select
			distinct
			PERIOD_BEGIN, 			
			min(PERIOD_END,'2016-12-31') as PERIOD_END, --данные до 31.12.2016 
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE = 'СЗВ-ИСХ' 
			AND PERIOD_BEGIN<'2017-01-01'--начало 31.12.2016 тоже входит
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1

			UNION ALL

			select
			distinct
			max(PERIOD_BEGIN,'2014-01-01') as PERIOD_BEGIN,-- за 2014-2016
			min(PERIOD_END,'2016-12-31') as PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE IN ('РСВ1-2014','РСВ1-2015', 'СЗВ7') 
			AND PERIOD_END>'2013-12-31'--2014.01.01 тоже входит
			AND PERIOD_BEGIN<'2017-01-01' --2016.12.31 тоже входит
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1

			UNION ALL

			select
			distinct
			max(PERIOD_BEGIN,'2010-01-01') as PERIOD_BEGIN, --за 2010-2013
			min(PERIOD_END,'2013-12-31') as PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE IN ('СЗВ-6-1','СЗВ-6-2','СЗВ-6-3','СЗВ-6-4') 
			AND PERIOD_END>'2009-12-31' 
			AND PERIOD_BEGIN<'2014-01-01' --больше или больше и равно?
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1

			UNION ALL

			select
			distinct
			max(PERIOD_BEGIN,'2002-01-01') as PERIOD_BEGIN, --за 2002-2009
			min(PERIOD_END,'2009-12-31') as PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where CODE IN ('СЗВ-4-1','СЗВ-4-2') 
			AND PERIOD_END>'2001-12-31'
			AND PERIOD_BEGIN<'2010-01-01' --больше или больше и равно?
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1

			UNION ALL

			select
			distinct
			--max(REG_DATE,PERIOD_BEGIN) as PERIOD_BEGIN, --с даты регистрации до 31.12.2001
			PERIOD_BEGIN as PERIOD_BEGIN, --с даты регистрации до 31.12.2001
			min(PERIOD_END,'2001-12-31') as PERIOD_END, --
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where CODE IN ('СЗВ-1','СЗВ-3') 
			--AND PERIOD_END>=REG_DATE --по маппингу не нужно - ежу
			AND PERIOD_BEGIN<'2002-01-01'
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1

			UNION ALL

			select
			distinct
			PERIOD_BEGIN,
			min(PERIOD_END,'2001-12-31') as PERIOD_END, --до 2002
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where
			CODE in ('СЗВ-К+', 'СЗВ-К') 
			AND COALESCE(IER_CODE,'-' ) IN ('РАБЗАГР','БЕЗР','ПРЗАГР','ПРОЖСУПР','РЕАБИЛИТ','СЛДАПБЛ','СЛПРИЗ','СТРАХБЛ','СЛУЖБА','ТРУДЛИЦО','УХОД-ДЕТИ','УХОД-ИНВД')
			AND PERIOD_BEGIN<'2002-01-01' --до 2002 года
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1

			UNION ALL

			select
			distinct
			PERIOD_BEGIN,
			min(PERIOD_END,REG_DATE) as PERIOD_END, --до даты регистрации
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where
			CODE in ('СЗВ-К+', 'СЗВ-К')  
			AND COALESCE(IER_CODE,'-' ) IN  ('РАБОТА', 'РАБСВПК', 'РАБВОВ', 'РАБЛОК')  
			--AND PERIOD_END>=REG_DATE
			AND PERIOD_BEGIN<REG_DATE
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1
			--до даты регистрации ЗЛ в системе ОПС
			
			UNION ALL

			select
			distinct
			max(PERIOD_BEGIN,'2017-01-01') as PERIOD_BEGIN,-- за 2014-2016
			PERIOD_END as PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE IN ('СЗВ-СТАЖ') 
			AND PERIOD_END>'2016-12-31'		
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1
			
			UNION ALL
			
			--- новое
			select
			distinct
			max(PERIOD_BEGIN,'2010-01-01') as PERIOD_BEGIN,-- за 2010-2016
			min(PERIOD_END,'2016-12-31') as PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE IN ('РСВР-РСВ2') 
			AND PERIOD_BEGIN>'2009-12-31'
			AND PERIOD_END<'2017-01-01'
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1
			
			UNION ALL
			
			--- новое
			select
			distinct
			max(PERIOD_BEGIN,'2017-01-01') as PERIOD_BEGIN,-- с 2017
			PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE IN ('РасчСВ_ОПС') 
			AND PERIOD_BEGIN>'2016-12-31'
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1
		
			UNION ALL
			
			--- новое
			select
			distinct
			max(PERIOD_BEGIN,'2017-01-01') as PERIOD_BEGIN,-- с 2017
			PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE IN ('РД ДоСВ') 
			AND PERIOD_BEGIN>'2016-12-31'
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1
		
			UNION ALL
			
			--- новое
			select
			distinct
			max(PERIOD_BEGIN,'2014-01-01') as PERIOD_BEGIN,-- за 2014-2016
			min(PERIOD_END,'2016-12-31') as PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE IN ('СПВ-2') 
			AND PERIOD_BEGIN>'2013-12-31'
			AND PERIOD_END<'2017-01-01'
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1
			
			UNION ALL
			
			--- новое
			select
			distinct
			max(PERIOD_BEGIN,'2010-01-01') as PERIOD_BEGIN,-- за 2010-2013
			min(PERIOD_END,'2013-12-31') as PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE IN ('СПВ-1') 
			AND PERIOD_BEGIN>'2009-12-31'
			AND PERIOD_END<'2014-01-01'
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1
			
			UNION ALL
			--- новое
			select
			distinct
			max(PERIOD_BEGIN,'2002-01-01') as PERIOD_BEGIN,-- за 2002-2009
			min(PERIOD_END,'2009-12-31') as PERIOD_END,
			--AGENT_GID,
			INDIVIDUAL_ACCOUNT_GID,
			REG_DATE
			from
			DMR005_LIST_STAJ_EXP
			where 
			CODE IN ('ФП') 
			AND PERIOD_BEGIN>'2001-12-31'
			AND PERIOD_END<'2010-01-01'
			and FLG_TAKE=1
			and FLG_STAJ_AGR=1
		
		) t
		where PERIOD_END >= PERIOD_BEGIN --??
	) E
)M
GROUP BY --AGENT_GID,
GRP_NUM,INDIVIDUAL_ACCOUNT_GID
)
DISTRIBUTE ON(INDIVIDUAL_ACCOUNT_GID);

#P_COMMENT# /*
insert into tmp_check_F_SPU_STAJ_AGGRE (checkk)  values ('F_SPU_STAJ_AGGR_p1');
#P_COMMENT# */

create table F_SPU_STAJ_AGGR_p1 as
(
--вставка нового
select 
distinct
INDIVIDUAL_ACCOUNT_GID,
sum(
(min(DATE_PART('DAY',DT_END::date),30::bigint) - min(DATE_PART('DAY',DT_BEG::date),30::bigint) + 1) --решили (по традиции СПУ данных), что 31 день приравнивается к 30
+ (DATE_PART('MONTH',DT_END::date) - DATE_PART('MONTH',DT_BEG::date))*30 
+ (DATE_PART('YEAR',DT_END::date) - DATE_PART('YEAR',DT_BEG::date))*30*12 
)
sm, --нормализированные дни
trunc(sm::numeric/360) YY_STAJ,
mod(sm,360) as mm_month,
trunc(mm_month::numeric/30) MM_STAJ,
mod(mm_month,30) DD_STAJ,
to_date('01.01.1000 00:00:00', 'dd.mm.yyyy hh24:mi:ss') as START_DATE,
to_date('31.12.9999 00:00:00', 'dd.mm.yyyy hh24:mi:ss') as END_DATE,
to_timestamp('#P_UPDATE_DATE#', 'dd.mm.yyyy hh24:mi:ss') as UPDATE_DATE,
1::smallint as FLG_INS
from
TMP_F_SPU_STAJ_AGGR_XX01_PERIODS src
where not exists
(
select 1
from #ps_nz_DMR005.Database#.#ps_nz_DMR005.Username#.F_SPU_STAJ_AGGR trg
where trg.INDIVIDUAL_ACCOUNT_GID=src.INDIVIDUAL_ACCOUNT_GID
)
group by INDIVIDUAL_ACCOUNT_GID

UNION ALL
--обновление
select
distinct
INDIVIDUAL_ACCOUNT_GID,
sm,
YY_STAJn as YY_STAJ,
mm_month,
MM_STAJn as MM_STAJ,
DD_STAJn as DD_STAJ,
to_date('01.01.1000 00:00:00', 'dd.mm.yyyy hh24:mi:ss') as START_DATE,
to_date('31.12.9999 00:00:00', 'dd.mm.yyyy hh24:mi:ss') as END_DATE,
to_timestamp('#P_UPDATE_DATE#', 'dd.mm.yyyy hh24:mi:ss') as UPDATE_DATE,
0::smallint as FLG_INS
from
(
	select 
	distinct
	src.INDIVIDUAL_ACCOUNT_GID,
	sum(
	(min(DATE_PART('DAY',DT_END::date),30::bigint) - min(DATE_PART('DAY',DT_BEG::date),30::bigint) + 1) --решили (по традиции СПУ данных), что 31 день приравнивается к 30
	+ (DATE_PART('MONTH',DT_END::date) - DATE_PART('MONTH',DT_BEG::date))*30 
	+ (DATE_PART('YEAR',DT_END::date) - DATE_PART('YEAR',DT_BEG::date))*30*12 
	)
	sm, --нормализированные дни
	trunc(sm::numeric/360) YY_STAJn,
	mod(sm,360) as mm_month,
	trunc(mm_month::numeric/30) MM_STAJn,
	mod(mm_month,30) DD_STAJn,
	trg.end_date,
	trg.YY_STAJ,
	trg.MM_STAJ,
	trg.DD_STAJ
	from
	TMP_F_SPU_STAJ_AGGR_XX01_PERIODS src
	inner join
	#ps_nz_DMR005.Database#.#ps_nz_DMR005.Username#.F_SPU_STAJ_AGGR trg
		on trg.INDIVIDUAL_ACCOUNT_GID=src.INDIVIDUAL_ACCOUNT_GID	
	group by 
	src.INDIVIDUAL_ACCOUNT_GID,
	trg.end_date,
	trg.YY_STAJ,
	trg.MM_STAJ,
	trg.DD_STAJ	
)t
where to_date('31.12.9999 00:00:00', 'dd.mm.yyyy hh24:mi:ss')!=t.END_DATE
	or YY_STAJn!=t.YY_STAJ
	or MM_STAJn!=t.MM_STAJ
	or DD_STAJn!=t.DD_STAJ


UNION ALL
--удаление для ALL
select 
distinct
trg.INDIVIDUAL_ACCOUNT_GID,
0 as sm, --нормализированные дни
trg.YY_STAJ,
0 as mm_month,
trg.MM_STAJ,
trg.DD_STAJ,
trg.START_DATE,
to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') - 1 as END_DATE,
to_timestamp('#P_UPDATE_DATE#', 'dd.mm.yyyy hh24:mi:ss') as UPDATE_DATE,
0::smallint as FLG_INS
from
	(
	select 
	INDIVIDUAL_ACCOUNT_GID
	from
	#ps_nz_DMR005.Database#.#ps_nz_DMR005.Username#.F_SPU_STAJ_AGGR ia
	where not exists
	(
	select 1
	from TMP_F_SPU_STAJ_AGGR_XX01_PERIODS src
	where ia.INDIVIDUAL_ACCOUNT_GID=src.INDIVIDUAL_ACCOUNT_GID
	)
	AND '#P_FLG_INC#' = 'ALL'
	AND ia.END_DATE=to_date('31.12.9999 00:00:00', 'dd.mm.yyyy hh24:mi:ss')
	#P_SRC_CONDITION#
	) t
inner join 
#ps_nz_DMR005.Database#.#ps_nz_DMR005.Username#.F_SPU_STAJ_AGGR trg
	on trg.INDIVIDUAL_ACCOUNT_GID=t.INDIVIDUAL_ACCOUNT_GID

UNION ALL
--удаление Inc
select 
distinct
trg.INDIVIDUAL_ACCOUNT_GID,
0 as sm, --нормализированные дни
trg.YY_STAJ,
0 as mm_month,
trg.MM_STAJ,
trg.DD_STAJ,
trg.START_DATE,
to_date('#P_END_DATE#', 'dd.mm.yyyy hh24:mi:ss') - 1 as END_DATE,
to_timestamp('#P_UPDATE_DATE#', 'dd.mm.yyyy hh24:mi:ss') as UPDATE_DATE,
0::smallint as FLG_INS
from
	(
	select 	
	INDIVIDUAL_ACCOUNT_GID
	from
	DMR005_LIST_STAJ_GID ia	
	where not exists
	(
	select 1
	from TMP_F_SPU_STAJ_AGGR_XX01_PERIODS src
	where ia.INDIVIDUAL_ACCOUNT_GID=src.INDIVIDUAL_ACCOUNT_GID
	)
	AND '#P_FLG_INC#' = 'INC'
	#P_SRC_CONDITION#
	) t
inner join 
#ps_nz_DMR005.Database#.#ps_nz_DMR005.Username#.F_SPU_STAJ_AGGR trg
	on trg.INDIVIDUAL_ACCOUNT_GID=t.INDIVIDUAL_ACCOUNT_GID
	AND trg.END_DATE=to_date('31.12.9999 00:00:00', 'dd.mm.yyyy hh24:mi:ss')
)
DISTRIBUTE ON(INDIVIDUAL_ACCOUNT_GID);

#P_COMMENT# /*
insert into tmp_check_F_SPU_STAJ_AGGRE (checkk)  values ('end');
#P_COMMENT# */

#P_COMMENT# /*
drop table TMP_F_SPU_STAJ_AGGR_XX01_PERIODS  if exists;
#P_COMMENT# */