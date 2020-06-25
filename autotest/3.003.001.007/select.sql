select ia.SNILSCS --ia.* -- count(*)
FROM      ${DMR005_SCHEMA}.${DMR_USR}.D_MSK_CERTIFICATE_P1           c
    join  ${DMR005_SCHEMA}.${DMR_USR}.D_MSK_APPLICANT_DATA_P1        dad
	   on   dad.MSK_APPLICANT_DATA_GID = c.MSK_APPLICANT_DATA_GID
	    and dad.end_date = '31.12.9999 00:00:00'::date
	    and c.end_date   = '31.12.9999 00:00:00'::date 
		and coalesce(c.MSK_CERTIFICATE_SERIES_GID,0) = 0                                                    -- 13064
	join  ${DMR005_SCHEMA}.${DMR_USR}.D_MSK_TERMINATION_REASON_TYPE tr 
	   on c.MSK_TERMINATION_REASON_TYPE_GID=tr.MSK_TERMINATION_REASON_TYPE_GID 
	      and tr.end_date = '9999-12-31 00:00:00' 	                                                                               -- 13064
	join ${DMR005_SCHEMA}.${DMR_USR}.D_INDIVIDUAL_ACCOUNT       ia 
	   on dad.SNILS= ia.snilscs  
	      and  ia.SRC_CODE= 6  