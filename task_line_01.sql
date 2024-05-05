COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := 'cis';
   l_SHEET_NM   VARCHAR2(32) := '';
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'CREATE';
   l_ENABLE     number := 1;
   l_EXPORT	    number := 0;
   l_LINE_BUILD number := 4;
   l_DESCR      clob := Q'#лицевые счета#';
   l_SQL_HEADER clob := Q'##';
   l_SQL_QUERY  clob := Q'#select * from ${cis}#';
   l_SQL_DATA   clob := 
Q'#
select 
distinct 
(select l.descr
  from ci_char_val_l l
   where 1=1
    and l.char_type_cd='U4ASTOK'
     and l.language_cd='RUS'
      and l.char_val=
(select max(ac.char_val) keep(dense_rank last order by ac.effdt)
  from ci_acct_char ac
   where 1=1
    and ac.char_type_cd='U4ASTOK'
     and ac.acct_id=a.acct_id)) ush
,(select d.descr50
  from ci_spr_l d
   where 1=1
    and d.language_cd='RUS'
     and d.spr_cd=
(select max(re.spr_cd) keep(dense_rank last order by re.effdt)
 from ci_sa_rel re
  where 1=1
   and re.sa_id=sa.sa_id
    and re.sa_rel_type_cd='SF '))st
,(select z.descr
  from ci_char_val_l z
   where 1=1
    and z.language_cd='RUS'
     and z.char_type_cd='MO'
      and z.char_val=
 (select max(prr.char_val) keep(dense_rank last order by prr.effdt)
  from ci_prem_char prr
   where 1=1
    and prr.prem_id=pr.prem_id
     and prr.char_type_cd='MO')) mo
 ,pr.city

,pr.address4

 ,pr.address3

,pr.address2
 
 ,pr.num1

,pr.prem_id
 
 ,pr.prnt_prem_id

,(select k.descr
   from ci_prem_type_l k
    where 1=1
     and k.language_cd='RUS'
      and k.prem_type_cd=pr.prem_type_cd )prem_type                                                                        
,(select min(rpp.adhoc_char_val) 
   from ci_prem_char rpp
    where 1=1
     and rpp.char_type_cd='KOL-PROP'
      and trunc(rpp.effdt,'mm') <= to_date('01.10.2022','dd.mm.yyyy') 
        and rpp.prem_id=pr.prem_id ) kol_top2                                                                  
,(select max(rpp.adhoc_char_val) keep(dense_rank last order by rpp.effdt )
   from ci_prem_char rpp
    where 1=1
     and rpp.char_type_cd='KOL-PROP'
       and rpp.prem_id=pr.prem_id ) kol_top3  
,(select max(rr.adhoc_char_val)keep(dense_rank last order by rr.effdt) 
   from ci_prem_char rr
    where 1=1
     and rr.prem_id=pr.prem_id
      and rr.char_type_cd='FIAS') fias
,( select max(c.adhoc_char_val)keep(dense_rank last order by c.effdt)
    from ci_prem_char c
     where 1=1
      and c.prem_id=r.prem_id
       and c.char_type_cd='BUILD_C') bild 
,sa.sa_id

from ci_acct a

left join ci_sa sa on a.acct_id=sa.acct_id

left join ci_prem pr on sa.char_prem_id=pr.prem_id

left join ci_prem r on r.prem_id=pr.prnt_prem_id

where 1=1
and a.cis_division in (select /*+ cardinality 1*/ CAST(Trim(r.VL) as char(5)) cis_division from TABLE( VZ_REPORT_API.split('${PARAMETER_01}')) r)  
and exists (select 1 from ci_acct_char ac where a.acct_id=ac.acct_id and ac.char_type_cd='SVC-TYPE' and ac.char_val='HW-CW-TN')
and rownum<10000   
   #';
   l_index number;
begin
    SELECT t.idx into l_index from VZ_TASK_BASE t where t.TASK_CD = l_task_cd and t.CAT_CD = l_cat_cd;

	merge into VZ_TASK_BASE_LINE tgt
	using (
	   SELECT l_index      TASK_IDX
            , l_seq 	   SEQ
            , l_ENTITY_NM  ENTITY_NM
            , l_SHEET_NM   SHEET_NM
            , l_SHEET_COL  SHEET_COL
            , l_SQL_OP 	   SQL_OP
            , l_ENABLE 	   ENABLE
            , l_EXPORT 	   EXPORT
            , l_LINE_BUILD LINE_BUILD
            , l_DESCR 	   DESCR
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
