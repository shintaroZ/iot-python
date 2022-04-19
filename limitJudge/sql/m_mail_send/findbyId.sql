select
    mms.MAIL_SEND_SEQ as mailSendSeq
    , mms.MAIL_SEND_ID as mailSendId
    , ifnull(tmsm.updatedAt, str_to_date('19000101000000', '%%Y%%m%%d%%k%%i%%s')) as beforeMailSendDateTime
from
    M_MAIL_SEND mms
    inner join M_MAIL_SEND_EQUIPMENT mmseqp
    on mmseqp.MAIL_SEND_SEQ = mms.MAIL_SEND_SEQ
    and mmseqp.EQUIPMENT_ID = '%(equipmentId)s'
    left outer join (
        select
            max(tmsmSub.UPDATED_AT) as updatedAt
            , tmsmSub.MAIL_SEND_SEQ as mailSendSeq
        from
            T_MAIL_SEND_MANAGED tmsmSub
        where
            tmsmSub.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
        and tmsmSub.SEND_STATUS = 2
        group by
            tmsmSub.MAIL_SEND_SEQ
    ) tmsm
    on tmsm.mailSendSeq = mms.MAIL_SEND_SEQ
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
    mms.MAIL_SEND_ID desc;

