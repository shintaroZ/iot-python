/* メール通知管理から最新日付の抽出 */
select
    tmsm.DETECTION_DATETIME as detectionDateTime
from
    T_MAIL_SEND_MANAGED tmsm 
where
    tmsm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and tmsm.LIMIT_SUB_NO = %(limitSubNo)d
and tmsm.MAIL_SEND_SEQ = %(mailSendSeq)d
and not exists ( 
    select
        1 
    from
        T_MAIL_SEND_MANAGED tmsmSub 
    where
        tmsm.DATA_COLLECTION_SEQ = tmsmSub.DATA_COLLECTION_SEQ 
        and tmsm.LIMIT_SUB_NO = tmsmSub.LIMIT_SUB_NO 
        and tmsm.MAIL_SEND_SEQ = tmsmSub.MAIL_SEND_SEQ 
        and tmsm.DETECTION_DATETIME < tmsmSub.DETECTION_DATETIME
)
