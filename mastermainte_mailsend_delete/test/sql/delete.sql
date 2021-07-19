delete from `M_MAIL_SEND`
where
    MAIL_SEND_ID = '%(mailSendId)s'
and DELETE_COUNT = %(whereDeleteCount)d
;