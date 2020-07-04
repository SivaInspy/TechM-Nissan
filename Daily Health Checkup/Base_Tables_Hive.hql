select bic.*, wflw.sts_cd from drive.wrkflw_base wflw inner join drive.bic_base bic on wflw.dcmnt_id = bic.bic_ky where wflw.dcmnt_typ_tx='BIC';
select wflw.dcmnt_id from drive.wrkflw_base wflw where wflw.dcmnt_typ_tx='BIC' and wflw.sts_cd like "Dele%"
select hs.hs_ky,hs.dcmnt_id, max(hs.crte_ts) from hs_base hs where hs.dcmnt_typ_tx='BIC' group by hs.hs_ky,hs.dcmnt_id
select wflw.wrkflw_ky,wflw.dcmnt_id, max(wflw.crte_ts) from drive.wrkflw_base wflw where wflw.dcmnt_typ_tx='BIC' group by wflw.wrkflw_ky,wflw.dcmnt_id

select crte_usr_id from drive.wrkflw_base limit 1
select crte_usr_id from drive.bic_base limit 1

select * from drive.wrkflw_base wflw where wflw.dcmnt_typ_tx='BIC' and wflw.crte_usr_id="x987731" and wflw.dcmnt_id=1000000000000018;


select wflw.sts_cd, bic.* from drive.wrkflw_base wflw inner join drive.bic_base bic on wflw.dcmnt_id=1000000000000018 where wflw.dcmnt_typ_tx='BIC';
select wflw.sts_cd, bic.* from drive.wrkflw_base wflw inner join drive.bic_base bic on wflw.dcmnt_id=1000000000000074 where wflw.dcmnt_typ_tx='BIC';
select wflw.sts_cd, bic.* from drive.wrkflw_base wflw inner join drive.bic_base bic on wflw.dcmnt_id=451201900216000 where wflw.dcmnt_typ_tx='BIC';

select rpm.*, wflw.sts_cd from drive.wrkflw_base wflw inner join drive.rqst_prt_mvmnt_base rpm on wflw.dcmnt_id = rpm.rqst_prt_mvmnt_ky where wflw.dcmnt_typ_tx = 'RPM';
select wflw.dcmnt_id from drive.wrkflw_base wflw where wflw.dcmnt_typ_tx='RPM' and wflw.sts_cd like "Dele%"
select hs.hs_ky,hs.dcmnt_id, max(hs.crte_ts) from hs_base hs where hs.dcmnt_typ_tx='RPM' group by hs.hs_ky,hs.dcmnt_id
select wflw.wrkflw_ky,wflw.dcmnt_id, max(wflw.crte_ts) from drive.wrkflw_base wflw where wflw.dcmnt_typ_tx='RPM' group by wflw.wrkflw_ky,wflw.dcmnt_id


select count(wflw.sts_cd) from drive.wrkflw_base wflw inner join drive.rqst_prt_mvmnt_base rpm ON rpm.rqst_prt_mvmnt_ky NOT IN (select wflw.dcmnt_id from drive.wrkflw_base wflw where wflw.dcmnt_typ_tx="RPM");

select wflw.dcmnt_id from drive.wrkflw_base wflw where wflw.dcmnt_typ_tx="RPM" where wflw.dcmnt_id NOT IN (select rpm.rqst_prt_mvmnt_ky from drive.rqst_prt_mvmnt_base rpm)
