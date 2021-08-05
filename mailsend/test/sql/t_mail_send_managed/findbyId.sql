select
    tmsm.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tmsm.DETECTION_DATETIME as detectionDateTime
    , tmsm.LIMIT_SUB_NO as limitSubNo
    , tmsm.MAIL_SEND_SEQ as mailSendSeq
    , tmsm.SEND_STATUS as sendStatus
from
    T_MAIL_SEND_MANAGED tmsm
where
    tmsm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and tmsm.DETECTION_DATETIME = '%(detectionDateTime)s'
and tmsm.LIMIT_SUB_NO = %(limitSubNo)d
and tmsm.MAIL_SEND_SEQ = %(mailSendSeq)d
 ;