update T_MAIL_SEND_MANAGED
set
    SEND_STATUS = %(sendStatus)d
    , UPDATED_AT = '%(updatedAt)s'
    , VERSION = VERSION + 1
where
    %(whereParams)s
;