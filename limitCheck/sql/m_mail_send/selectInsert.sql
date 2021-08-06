insert
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
    left outer join T_MAIL_SEND_MANAGED tmsm
        on tlhm.DATA_COLLECTION_SEQ = tmsm.DATA_COLLECTION_SEQ
        and tlhm.LIMIT_SUB_NO = tmsm.LIMIT_SUB_NO
        and not exists (
            select
                1
            from
                T_MAIL_SEND_MANAGED tmsmSub
            where
                tmsm.DATA_COLLECTION_SEQ = tmsmSub.DATA_COLLECTION_SEQ
                and tmsm.LIMIT_SUB_NO = tmsmSub.LIMIT_SUB_NO
                and tmsm.DETECTION_DATETIME < tmsmSub.DETECTION_DATETIME
        )
where
    tlhm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
%(whereParam)s
and ifnull(tmsm.DETECTION_DATETIME, str_to_date('19000101000000', '%%Y%%m%%d%%k%%i%%s')) < tlhm.DETECTION_DATETIME
and tlhm.DETECTION_DATETIME <= '%(timeStamp)s'