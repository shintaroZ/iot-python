select
    mms.mailSendSeq
    , mms.emailAddress
    , mms.sendWeekType
    , mms.sendFrequancy
    , mms.sendTimeFromDt
    , mms.sendTimeToDt
    , mms.mailSubject
    , mms.mailTextHeader
    , mms.mailTextBody
    , mms.mailTextFooter

from
     (
        select
            mmsSub.MAIL_SEND_SEQ as mailSendSeq
            , mmsSub.DELETE_FLG as deleteFlg
            , mmsSub.VERSION as version
            , mmsSub.EMAIL_ADDRESS as emailAddress
            , mmsSub.SEND_WEEK_TYPE as sendWeekType
            , mmsSub.SEND_FREQUANCY as sendFrequancy
            , case
                /* Fromの前日 or 当日判定*/
                when (mmsSub.SEND_TIME_TO < mmsSub.SEND_TIME_FROM and DATE_FORMAT(CURRENT_TIMESTAMP, '%%k%%i%%s') < mmsSub.SEND_TIME_TO)
                then str_to_date(concat(DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), '%%Y%%m%%d'), mmsSub.SEND_TIME_FROM), '%%Y%%m%%d%%k%%i%%s')
                else str_to_date(concat(DATE_FORMAT(CURRENT_DATE, '%%Y%%m%%d'), mmsSub.SEND_TIME_FROM), '%%Y%%m%%d%%k%%i%%s')
              end as sendTimeFromDt
            , case
                /* Fromの翌日 or 当日判定*/
                when (mmsSub.SEND_TIME_TO < mmsSub.SEND_TIME_FROM and mmsSub.SEND_TIME_TO < DATE_FORMAT(CURRENT_TIMESTAMP, '%%k%%i%%s'))
                then str_to_date(concat(DATE_FORMAT(DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY), '%%Y%%m%%d'), mmsSub.SEND_TIME_TO), '%%Y%%m%%d%%k%%i%%s')
                else str_to_date(concat(DATE_FORMAT(CURRENT_DATE, '%%Y%%m%%d'), mmsSub.SEND_TIME_TO), '%%Y%%m%%d%%k%%i%%s')
              end as sendTimeToDt
            , mmsSub.MAIL_SUBJECT as mailSubject
            , mmsSub.MAIL_TEXT_HEADER as mailTextHeader
            , mmsSub.MAIL_TEXT_BODY as mailTextBody
            , mmsSub.MAIL_TEXT_FOOTER as mailTextFooter
        from
            M_MAIL_SEND mmsSub
    ) mms
where
    mms.deleteFlg = 0
and now() between mms.sendTimeFromDt and mms.sendTimeToDt
and not exists (
    select
        1
    from
        M_PUBLIC_HOLIDAY mph
    where
        mms.sendWeekType = 1
        and CURRENT_DATE = mph.PUBLIC_HOLIDAY_DATE
)
and exists (
    select
        1
    from
        T_MAIL_SEND_MANAGED tmsm
    where
        mms.mailSendSeq = tmsm.MAIL_SEND_SEQ
)
;
