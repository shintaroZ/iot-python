update `M_MAIL_SEND` set
    `DELETE_FLG` = %(deleteFlg)d
    , `UPDATED_AT` = '%(updatedAt)s'
    , `UPDATED_USER` = '%(updatedUser)s'
where
    MAIL_SEND_ID = '%(mailSendId)s'
and DELETE_FLG = 0
;