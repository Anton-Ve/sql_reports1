COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := 'value3';
   l_SHEET_NM   VARCHAR2(32) := '';
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'CREATE';
   l_ENABLE     number := 1;
   l_EXPORT     number := 0;
   l_LINE_BUILD number := 6;
   l_DESCR      clob := Q'#лицевые счета4#';
   l_SQL_HEADER clob := Q'##';
   l_SQL_QUERY  clob := Q'#select * from ${value3} #'; 
   l_SQL_DATA   clob := Q'# 
  select 
  
  distinct 
  
  pr.prem_id
 
 ,max(case when trunc(mr.read_dttm,'mm') >= to_date('01.09.2022','dd.mm.yyyy') 
                                 and trunc(mr.read_dttm,'mm') <= to_date('30.09.2022','dd.mm.yyyy') then re.read_type_flg else null end) pok1
 ,max(case when trunc(mr.read_dttm,'mm') >= to_date('01.10.2022','dd.mm.yyyy') 
                                 and trunc(mr.read_dttm,'mm') <= to_date('31.10.2022','dd.mm.yyyy') then re.read_type_flg else null end) pok2  

 ,max(case when trunc(mr.read_dttm,'mm') >= to_date('01.11.2022','dd.mm.yyyy') 
                                 and trunc(mr.read_dttm,'mm') <= to_date('30.11.2022','dd.mm.yyyy') then re.read_type_flg else null end) pok3  

 ,max(case when trunc(mr.read_dttm,'mm') >= to_date('01.12.2022','dd.mm.yyyy') 
                                 and trunc(mr.read_dttm,'mm') <= to_date('31.12.2022','dd.mm.yyyy') then re.read_type_flg else null end) pok4  

 ,max(case when trunc(mr.read_dttm,'mm') >= to_date('01.01.2023','dd.mm.yyyy') 
                                 and trunc(mr.read_dttm,'mm') <= to_date('31.01.2023','dd.mm.yyyy') then re.read_type_flg else null end) pok5  

 ,max(case when trunc(mr.read_dttm,'mm') >= to_date('01.02.2023','dd.mm.yyyy') 
                                 and trunc(mr.read_dttm,'mm') <= to_date('28.02.2023','dd.mm.yyyy') then re.read_type_flg else null end) pok6
 
 ,max(case when trunc(mr.read_dttm,'mm') >= to_date('01.03.2023','dd.mm.yyyy') 
                                 and trunc(mr.read_dttm,'mm') <= to_date('31.03.2023','dd.mm.yyyy') then re.read_type_flg else null end) pok7
                                 
 ,max(case when trunc(mr.read_dttm,'mm') >= to_date('01.04.2023','dd.mm.yyyy') 
                                 and trunc(mr.read_dttm,'mm') <= to_date('30.04.2023','dd.mm.yyyy') then re.read_type_flg else null end) pok8
                                 
 ,max(case when trunc(mr.read_dttm,'mm') >= to_date('01.05.2023','dd.mm.yyyy') 
                                 and trunc(mr.read_dttm,'mm') <= to_date('31.05.2023','dd.mm.yyyy') then re.read_type_flg else null end) pok9 
                                       
 ,max(case when h.removal_dttm is null then 'да'  
           when h.removal_dttm is not null then 'нет' end)  pu                                                                 
                                                                                                    
 from ci_acct a
 
 left join ci_sa sa on a.acct_id=sa.acct_id
 
 left join ci_prem pr on sa.char_prem_id=pr.prem_id 
 
 left join ci_sa_sp spp on sa.sa_id=spp.sa_id
 
 left join ci_sp_mtr_hist h on spp.sp_id=h.sp_id
 
 left join ci_mtr_config c on h.mtr_config_id=c.mtr_config_id
 
 left join ci_mtr mtr on c.mtr_id=mtr.mtr_id
 
 left join ci_mr mr on c.mtr_config_id=mr.mtr_config_id
 
 left join ci_reg_read re on mr.mr_id=re.mr_id

where 1=1 

and pr.prem_id in(select prem_id from ${value2})
--and sa.acct_id='8371563000'
--and pr.prem_id='8966162241'
group by  
 pr.prem_id
   #';
   l_index number;
begin
    SELECT t.idx into l_index from VZ_TASK_BASE t where t.TASK_CD = l_task_cd and t.CAT_CD = l_cat_cd;

  merge into VZ_TASK_BASE_LINE tgt
  using (
     SELECT l_index      TASK_IDX
            , l_seq      SEQ
            , l_ENTITY_NM  ENTITY_NM
            , l_SHEET_NM   SHEET_NM
            , l_SHEET_COL  SHEET_COL
            , l_SQL_OP     SQL_OP
            , l_ENABLE     ENABLE
            , l_EXPORT     EXPORT
            , l_LINE_BUILD LINE_BUILD
            , l_DESCR      DESCR
            , l_SQL_HEADER SQL_HEADER
            , l_SQL_QUERY  SQL_QUERY
            , l_SQL_DATA   SQL_DATA
        FROM dual
  ) src
  on (tgt.TASK_IDX = src.TASK_IDX and tgt.SEQ = src.SEQ)
  WHEN MATCHED THEN
    UPDATE set tgt.ENTITY_NM       = src.ENTITY_NM
         , tgt.SHEET_NM        = src.SHEET_NM
                 , tgt.SHEET_COL       = src.SHEET_COL
         , tgt.SQL_OP          = src.SQL_OP
         , tgt.ENABLE          = src.ENABLE
         , tgt.EXPORT          = src.EXPORT
         , tgt.LINE_BUILD      = src.LINE_BUILD
         , tgt.DESCR           = src.DESCR
                 , tgt.SQL_HEADER      = src.SQL_HEADER
         , tgt.SQL_QUERY       = src.SQL_QUERY
         , tgt.SQL_DATA        = src.SQL_DATA
        WHERE tgt.LINE_BUILD < src.LINE_BUILD
  WHEN NOT MATCHED THEN
     INSERT( tgt.TASK_IDX, tgt.SEQ, tgt.ENTITY_NM, tgt.SHEET_NM, tgt.SHEET_COL, tgt.SQL_OP, tgt.ENABLE, tgt.EXPORT, tgt.LINE_BUILD, tgt.DESCR, tgt.SQL_HEADER, tgt.SQL_QUERY, tgt.SQL_DATA)  
     VALUES( src.TASK_IDX, src.SEQ, src.ENTITY_NM, src.SHEET_NM, src.SHEET_COL, src.SQL_OP, src.ENABLE, src.EXPORT, src.LINE_BUILD, src.DESCR, src.SQL_HEADER, src.SQL_QUERY, src.SQL_DATA)  
  ;
  commit;
end;
/
