update `M_MAIL_SEND` set
    `DELETE_COUNT` = %(deleteCount)d
where
    MAIL_SEND_ID = '%(mailSendId)s'
and DELETE_COUNT = %(whereDeleteCount)d
;