/* 閾値成立管理からメール通知管理に存在しないレコードを追加 */
insert ignore
into `T_MAIL_SEND_MANAGED`(
    `DATA_COLLECTION_SEQ`
    , `DETECTION_DATETIME`
    , `LIMIT_SUB_NO`
    , `MAIL_SEND_SEQ`
    , `SEND_STATUS`
    , `CREATED_AT`
)
select
    tlhm.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tlhm.DETECTION_DATETIME as detectionDateTime
    , tlhm.LIMIT_SUB_NO as limitSubNo
    , %(mailSendSeq)d as mailSendSeq
    , '%(sendStatus)s' as sendStatus
    , '%(createdAt)s' as createdAt
from
    T_LIMIT_HIT_MANAGED tlhm
where
    tlhm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and '%(detectionDateTime)s' < tlhm.DETECTION_DATETIME