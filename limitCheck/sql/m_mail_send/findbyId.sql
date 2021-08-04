select
    mms.MAIL_SEND_SEQ as mailSendSeq
    , mms.MAIL_SEND_ID as mailSendId
    , ifnull(tmsm.DETECTION_DATETIME, str_to_date('19000101000000', '%%Y%%m%%d%%k%%i%%s')) as beforeDetectionDateTime
    , ifnull(tmsm.UPDATED_AT, str_to_date('19000101000000', '%%Y%%m%%d%%k%%i%%s')) as beforeMailSendDateTime
from
    M_MAIL_SEND mms
    left outer join T_MAIL_SEND_MANAGED tmsm
        on mms.MAIL_SEND_SEQ = tmsm.MAIL_SEND_SEQ
        and tmsm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
        and tmsm.LIMIT_SUB_NO = %(limitSubNo)d
        and tmsm.SEND_STATUS = 2
        and not exists (
            select
                1
            from
                T_MAIL_SEND_MANAGED tmsmSub
            where
                tmsm.DATA_COLLECTION_SEQ = tmsmSub.DATA_COLLECTION_SEQ
                and tmsm.MAIL_SEND_SEQ = tmsmSub.MAIL_SEND_SEQ
                and tmsm.LIMIT_SUB_NO = tmsmSub.LIMIT_SUB_NO
                and tmsm.SEND_STATUS = tmsmSub.SEND_STATUS
                and tmsm.DETECTION_DATETIME < tmsmSub.DETECTION_DATETIME
        )
where
    mms.DELETE_FLG = 0
    and not exists (
        select
            1
        from
            M_MAIL_SEND mmsSub
        where
            mms.MAIL_SEND_ID = mmsSub.MAIL_SEND_ID
            and mms.VERSION < mmsSub.VERSION
    )
order by
    mms.MAIL_SEND_ID;

