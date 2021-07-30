select
    tmsm.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tmsm.DETECTION_DATETIME as detectionDateTime
    , tmsm.LIMIT_SUB_NO as limitSubNo
    , tmsm.MAIL_SEND_SEQ as mailSendSeq
    , tmsm.SEND_STATUS as sendStatus
from
    T_MAIL_SEND_MANAGED tmsm
where
    tmsm.MAIL_SEND_MANAGED_SEQ = %(mailSendManagedSeq)d
 ;