    -- SQL выбора данных и проверки сервиса fbdpnsu2 по заявкам 8168088 8045455 и 8169069 
    -- комментарии по настройке проверки и и интепретация её результатов в конце SQL 
    with fbdp_ia as (  select agent_gid, formatted_snils, snils, update_date 
		       from  (select  ia.agent_gid ,  ia.formatted_snils,  ia.snils,  ia.update_date 
				   ,  ia.START_DATE 
				   , max( ia.START_DATE) over(partition by ia.snils) as msd
			      from v_d_fbdp_individual_account ia 
			      where ia.end_date = '9999-12-31'::date 
			     ) tp 
		       where tp.START_DATE = msd
                    ) --select * from fbdp_ia limit 1000 
          ,gsp   as (  select ia.snils , ia.formatted_snils
                             ,g.*
			     ,ac.privilege_end_date
    	                     , row_number() over(partition by ia.snils order by coalesce(g.gsp_start_date,'1000-01-01'::date) DESC, coalesce(g.gsp_end_date,'9999-12-31'::date) DESC, g.gsp_fixing_gid DESC) as nr      
                       from         fbdp_ia                     ia 
                               join v_d_fbdp_pensioner_gsp      g  on g.agent_gid = ia.agent_gid 
			       join v_d_fbdp_gsp_agent_category ac on g.agent_gid = ac.agent_gid 
			  left join rsmaster.af_gsp_category c1    on c1.code > 0 and c1.code =  case when trim(g.gsp_category_1_code)=any(array['-','']) then '0' 
			                                                                              when trim(translate(g.gsp_category_1,'.+0123456789',''))='' then coalesce(g.gsp_category_1_code,'0') 
			                                                                         else '0' end::bigint 
			  left join rsmaster.af_gsp_category c2    on c2.code > 0 and c2.code =  case when trim(g.gsp_category_2_code)=any(array['-','']) then '0' 
			                                                                              when trim(translate(g.gsp_category_2,'.+0123456789',''))='' then coalesce(g.gsp_category_2_code,'0') 
			                                                                         else '0' end::bigint 
                     ) 
--                      select *  
--                      from gsp 
--                      where gsp.nr=1 and gsp.snils= 269689565 --273179316 --101536154 
--                        and gsp.gsp_closure_date<= current_date 
--                        and current_date > gsp.gsp_start_date 
-- 	               and gsp.gsp_end_date > date_trunc('year',current_date)
--  	               and gsp.privilege_end_date > date_trunc('year',current_date)
--                        and (gsp.nsu1_current_gsp_flg <>'Нет' or gsp.nsu2_current_gsp_flg<>'Нет' or gsp.nsu3_current_gsp_flg <>'Нет')  
--                        and NOT(gsp.code = any(array['ПРЕ','СНЯ'])) ; -- code <> 1878, code = 1960      всего: 3838 , БЕЗ УСЛОВИЙ-11070 
            select distinct g.SNILS,g.FORMATTED_SNILS
                   ,t_fbdpnsu2.* 
	           --,g.*
	           --,ac.PRIVILEGE_START_DATE as ac_PRIVILEGE_START_DATE ,ac.PRIVILEGE_END_DATE as ac_PRIVILEGE_END_DATE , ac.* -- ia.SNILS,ia.FORMATTED_SNILS,gsp.*  count(*)
	    from   gsp g                                            
  	      join LATERAL (select * 
	                    from api_service.c_fbdpnsu2(g.SNILS) 
	                   ) t_fbdpnsu2 on 1=1 --and t_fbdpnsu2.gsp_agent_gid  is NULL          
	    where g.nr=1 --and g.snils= 269689565 --273179316 --101536154 
                       and g.gsp_closure_date<= current_date 
                       and current_date > g.gsp_start_date 
	               and g.gsp_end_date > date_trunc('year',current_date)
 	               and g.privilege_end_date > date_trunc('year',current_date)
                       and (g.nsu1_current_gsp_flg <>'Нет' or g.nsu2_current_gsp_flg<>'Нет' or g.nsu3_current_gsp_flg <>'Нет')  
   --                  and NOT(g.code = any(array['ПРЕ','СНЯ']))--здесь при новой версии функции c_fbdpnsu2 ДОЛЖНЫ выдаваться данные по НСУ(в таблице результат справа), 
                                                                --      поскольку операции отличны от 'ПРЕ','СНЯ' 
                                                                --      и значение gsp_closure_date<=current_date игнорируется фукцией     
                       and (g.code = any(array['ПРЕ','СНЯ']))   --здесь при новой версии функции c_fbdpnsu2 НЕ ДОЛЖНЫ выдаваться данные по НСУ, посльку операции 'ПРЕ','СНЯ' 
                                                                --      и значение gsp_closure_date<=current_date    
	                                                        -- при старой версии функции c_fbdpnsu2 данные по НСУ выдаются и не выдаются вперемежку в обеих случаях,
	                                                        --   что в зависимости от операции code является ошибкой(или не является) 
	                                                        -- при проверке должно быть откомментировано только одно из выше указанных условий  
	      order by g.SNILS
     	      limit 100
   ; 