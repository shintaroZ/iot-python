update T_MAIL_SEND_MANAGED
set
    SEND_STATUS = %(sendStatus)d
    , UPDATED_AT = '%(updatedAt)s'
    , VERSION = VERSION + 1
where
    DATA_COLLECTION_SEQ = '%(dataCollectionSeq)s'
and DETECTION_DATETIME = '%(detectionDateTime)s'
and LIMIT_SUB_NO = %(limitSubNo)d
and MAIL_SEND_ID = %(mailSendId)d
;