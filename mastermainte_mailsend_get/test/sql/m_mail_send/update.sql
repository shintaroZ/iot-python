update M_MAIL_SEND 
set DELETE_FLG = 1
where MAIL_SEND_ID = %(mailSendId)d
;
