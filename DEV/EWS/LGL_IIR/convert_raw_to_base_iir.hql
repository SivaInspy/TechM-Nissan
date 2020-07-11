use lgl_iir;
INSERT OVERWRITE TABLE incdnt_ivstgn_base SELECT `^(CMPST_KY|UPDT_TS)?+.+` FROM incdnt_ivstgn_raw;
INSERT OVERWRITE TABLE incdnt_ivstgn_trd_base SELECT `^(CMPST_KY|UPDT_TS)?+.+` FROM incdnt_ivstgn_trd_raw;