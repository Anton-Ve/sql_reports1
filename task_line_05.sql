COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := 'value4';
   l_SHEET_NM   VARCHAR2(32) := 'Ћист1';
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'CREATE';
   l_ENABLE     number := 1;
   l_EXPORT     number := 1;
   l_LINE_BUILD number := 1;
   l_DESCR      clob := Q'#лицевые счета4#';
   l_SQL_HEADER clob := Q'##';
   l_SQL_QUERY  clob := Q'#select * from ${value4} #'; 
   l_SQL_DATA   clob := Q'# 
  select
distinct
t.ush,
t.st,
t.mo,
t.city,
t.address4,
t.address3,
t.address2,
t.num1,
t.prem_id,
t.prnt_prem_id,
t.prem_type,
tt.pu,
t.t1,
case when tt.pok1='30' then '—истемный расчет' 
      when tt.pok1='60'  then 'ќбычное показание' 
       when tt.pok1='45' then 'ѕерерассчитано системой' end as descr1,    
t.t2,
case when tt.pok2='30' then '—истемный расчет' 
      when tt.pok2='60'  then 'ќбычное показание' 
       when tt.pok2='45' then 'ѕерерассчитано системой' end as descr2,
t.t3,
case when tt.pok3='30' then '—истемный расчет' 
      when tt.pok3='60'  then 'ќбычное показание' 
       when tt.pok3='45' then 'ѕерерассчитано системой' end as descr3,
t.t4,
case when tt.pok4='30' then '—истемный расчет' 
      when tt.pok4='60'  then 'ќбычное показание' 
       when tt.pok4='45' then  'ѕерерассчитано системой' end as descr4,
t.t5,
case when tt.pok5='30' then '—истемный расчет' 
      when tt.pok5='60'  then 'ќбычное показание' 
       when tt.pok5='45' then  'ѕерерассчитано системой' end as descr5,
t.t6,
case when tt.pok6='30' then '—истемный расчет' 
      when tt.pok6='60'  then 'ќбычное показание' 
       when tt.pok6='45' then  'ѕерерассчитано системой' end as descr6,
t.t7,
case when tt.pok7='30' then '—истемный расчет' 
      when tt.pok7='60'  then 'ќбычное показание' 
       when tt.pok7='45' then  'ѕерерассчитано системой' end as descr7,
t.t8,
case when tt.pok8='30' then '—истемный расчет' 
      when tt.pok8='60'  then 'ќбычное показание' 
       when tt.pok8='45' then  'ѕерерассчитано системой' end as descr8,
t.t9,
case when tt.pok9='30' then '—истемный расчет' 
      when tt.pok9='60'  then 'ќбычное показание' 
       when tt.pok9='45' then 'ѕерерассчитано системой' end as descr9,
t.kol_top2,
t.kol_top3,
t.fias,
t.bild
from ${value} t
left join ${value3} tt on tt.prem_id=t.prem_id
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
