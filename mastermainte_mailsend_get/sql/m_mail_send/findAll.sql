select
    mms.MAIL_SEND_ID as mailSendId
    , mms.EMAIL_ADDRESS as emailAddress
    , mms.SEND_WEEK_TYPE as sendWeekType
    , mms.SEND_FREQUANCY as sendFrequancy
    , mms.SEND_TIME_FROM as sendTimeFrom
    , mms.SEND_TIME_TO as sendTimeTo
    , mms.MAIL_SUBJECT as mailSubject
    , mms.MAIL_TEXT_HEADER as mailTextHeader
    , mms.MAIL_TEXT_BODY as mailTextBody
    , mms.MAIL_TEXT_FOOTER as mailTextFooter
    , mmse.EQUIPMENT_ID as equipmentId
    , DATE_FORMAT(mms.CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as createdAt
    , DATE_FORMAT(mms.UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as updatedAt
    , mms.UPDATED_USER as updatedUser
    , mms.VERSION as version 
from
    M_MAIL_SEND mms 
    left outer join M_MAIL_SEND_EQUIPMENT mmse 
        on mmse.MAIL_SEND_SEQ = mms.MAIL_SEND_SEQ 
where
    DELETE_FLG = 0 
    and not exists ( 
        select
            1 
        from
            M_MAIL_SEND mmsSub 
        where
            mms.MAIL_SEND_ID = mmsSub.MAIL_SEND_ID 
            and mms.VERSION < mmsSub.VERSION
    )
