SELECT distinct SNILS, --СНИЛС без КС
--             IE.INDIVIDUAL_EXPERIENCE_GID,
             ID.INDIVIDUAL_ACCOUNT_GID, --идентификатор ЗЛ в ИАП
             ID.REG_DATE, --дата регистрации ЗЛ в ПФР
             DT.CODE, --код документа, по которому отражен стаж
             EXPERIENCE_START_DATE AS PERIOD_BEGIN, --стажевый период:дата начала
             EXPERIENCE_END_DATE  AS PERIOD_END,--стажевый период:дата окончания
    ma.sid as РЕГ_НОМЕР, --рег.номер страхователя
             360*(DATE_PART('year',PERIOD_END)-DATE_PART('year',PERIOD_BEGIN) ) 
             +30*(DATE_PART('month',PERIOD_END)-DATE_PART('month',PERIOD_BEGIN) )
             + min(DATE_PART('day',PERIOD_END)::int,30::int)-min(DATE_PART('day',PERIOD_BEGIN)::int,30::int)+1 as P_day1, --количество дней стажа за период
             IER.name --код основания исчисления стажа
FROM (((((TST_DWH.TST_DWH_USR.INDIVIDUAL_EXPERIENCE IE 
JOIN TST_DWH.TST_DWH_USR.INDIVIDUAL_ACCOUNT "ID" 
 ON ((((IE.AGENT_GID = "ID".AGENT_GID) AND ("ID".END_DATE = '9999-12-31'::DATE)) AND ("ID".SRC_CODE IN (2, 127))))) 
JOIN TST_DWH.TST_DWH_USR.INDIVIDUAL_EXPERIENCE_AND_SPU_DOCUMENT ED 
 ON ((((IE.INDIVIDUAL_EXPERIENCE_GID = ED.INDIVIDUAL_EXPERIENCE_GID) AND (ED.END_DATE = '9999-12-31'::DATE)) AND (ED.SRC_CODE IN (2, 127))))) 
Left JOIN TST_DWH.TST_DWH_USR.SPU_DOCUMENT_SZV_DETAIL SZV 
 ON ((((ED.SPU_DOCUMENT_SZV_DETAIL_GID = SZV.SPU_DOCUMENT_SZV_DETAIL_GID) AND (SZV.END_DATE = '9999-12-31'::DATE)) AND (SZV.SRC_CODE IN (2, 127))))) 
JOIN TST_DWH.TST_DWH_USR.SPU_DOCUMENT SD 
 ON (((((ED.SPU_DOCUMENT_SZV_DETAIL_GID = SD.SPU_DOCUMENT_GID) AND (SD.END_DATE = '9999-12-31'::DATE)) AND (SD.SRC_CODE IN (2, 127))) AND (SD.VALID_TO_DATE = '9999-12-31 23:59:59'::"TIMESTAMP")))) 
join TST_DWH.TST_DWH_USR.m_agent ma on ma.agent_gid=sd.insurer_agent_gid and ma.src_code=2 
JOIN TST_DWH.TST_DWH_USR.SPU_DOCUMENT_TYPE DT 
 ON ((((SD.SPU_DOCUMENT_TYPE_GID = DT.SPU_DOCUMENT_TYPE_GID) AND (DT.END_DATE = '9999-12-31'::DATE)) AND (DT.SRC_CODE IN (2, 127))))) 
JOIN TST_DWH.TST_DWH_USR.INDIVIDUAL_EXPERIENCE_DETAIL IED
 ON IE.INDIVIDUAL_EXPERIENCE_GID=IED.INDIVIDUAL_EXPERIENCE_GID
JOIN TST_DWH.TST_DWH_USR.INDIVIDUAL_EXPERIENCE_REASON IER
 ON IER.INDIVIDUAL_EXPERIENCE_REASON_GID=IED.INDIVIDUAL_EXPERIENCE_REASON_GID
 WHERE "ID".Snils in (70985583) -- СНИЛС без КС(без лидирующих нулей), например, если СНИЛС ЗЛ 012-345-678 19, то следует ввести 12345678
 AND (IE.END_DATE = '9999-12-31'::DATE) AND (IE.SRC_CODE IN (2, 127));