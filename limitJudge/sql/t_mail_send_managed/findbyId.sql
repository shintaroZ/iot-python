/* メール通知管理から最新日付の抽出 */
select
    max(tmsm.DETECTION_DATETIME) as detectionDateTime
from
    T_MAIL_SEND_MANAGED tmsm 
where
    tmsm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and tmsm.MAIL_SEND_SEQ = %(mailSendSeq)d
;
