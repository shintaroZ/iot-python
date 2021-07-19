update `M_MAIL_SEND` set
    `DELETE_COUNT` = %(deleteCount)d
    , `UPDATED_AT` = '%(updatedAt)s'
    , `UPDATED_USER` = '%(updatedUser)s'
    , `VERSION` = VERSION + 1
where
    MAIL_SEND_ID = '%(mailSendId)s'
and DELETE_COUNT = 0
;