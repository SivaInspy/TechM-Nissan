set hive.support.quoted.identifiers=none;

DROP TABLE IF EXISTS ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs_cmltv;

CREATE TABLE ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs_cmltv
STORED AS ORC 
AS
SELECT 
`^(crte.*|updt.*)?+.+`, '0' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 0
UNION ALL
SELECT 
`^(crte.*|updt.*)?+.+`, '1' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 1
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '2' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 2
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '3' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 3
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '4' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 4
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '5' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 5
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '6' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 6
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '7' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 7
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '8' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 8
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '9' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 9
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '10' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 10
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '11' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 11
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '12' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 12
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '13' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 13
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '14' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 14
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '15' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 15
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '16' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 16
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '17' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 17
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '18' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 18
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '19' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 19
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '20' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 20
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '21' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 21
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '22' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 22
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '23' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 23
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '24' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 24
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '25' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 25
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '26' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 26
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '27' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 27
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '28' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 28
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '29' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 29
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '30' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 30
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '31' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 31
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '32' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 32
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '33' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 33
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '34' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 34
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '35' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 35
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '36' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 36
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '37' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 37
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '38' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 38
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '39' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 39
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '40' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 40
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '41' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 41
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '42' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 42
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '43' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 43
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '44' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 44
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '45' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 45
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '46' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 46
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '47' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 47
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '48' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 48
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '49' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 49
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '50' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 50
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '51' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 51
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '52' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 52
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '53' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 53
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '54' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 54
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '55' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 55
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '56' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 56
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '57' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 57
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '58' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 58
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '59' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 59
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, '60' AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS >= 60
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, NULL AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE VEHICLE_RECORD = 1 AND VHCL_MIS IS NULL
UNION ALL
SELECT `^(crte.*|updt.*)?+.+`, CLM_MIS AS MIS,
"IR_COMBINED_DFLT_CMLT_FULL" AS CRTE_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS,
"IR_COMBINED_DFLT_CMLT_FULL" AS UPDT_USR_ID,
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS
FROM ${dbName}.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs
WHERE CLAIM_RECORD = 1 AND 
(cast(CLM_MIS as int) <= 60 OR
CLM_MIS is NULL)
;
