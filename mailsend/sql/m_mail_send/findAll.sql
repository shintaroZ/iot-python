select
    M.*
from
    (
    select
        mms.MAIL_SEND_ID as mailSendId
        , mms.DELETE_COUNT as deleteCount
        , mms.EMAIL_ADDRESS as emailAddress
        , mms.SEND_WEEK_TYPE as sendWeekType
        , mms.SEND_FREQUANCY as sendFrequancy
        , str_to_date(concat(DATE_FORMAT(CURRENT_DATE, '%%Y%%m%%d'), mms.SEND_TIME_FROM), '%%Y%%m%%d%%k%%i%%s') as sendTimeFromDt
        , case
            when (mms.SEND_TIME_FROM <= mms.SEND_TIME_TO)
            then str_to_date(concat(DATE_FORMAT(CURRENT_DATE, '%%Y%%m%%d'), mms.SEND_TIME_TO), '%%Y%%m%%d%%k%%i%%s')
            else str_to_date(concat(DATE_FORMAT(DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY), '%%Y%%m%%d'), mms.SEND_TIME_TO), '%%Y%%m%%d%%k%%i%%s')
          end as sendTimeToDt
        , mms.MAIL_SUBJECT as mailSubject
        , mms.MAIL_TEXT as mailText
    from
        M_MAIL_SEND mms
    ) M

where
    M.deleteCount = 0
and now() between M.sendTimeFromDt and M.sendTimeToDt
and not exists(
    select
        1
    from
        M_PUBLIC_HOLIDAY mph
    where
        M.sendWeekType = 1
--     and str_to_date('2021-07-22', '%%Y-%%m-%%d') = mph.PUBLIC_HOLIDAY_DATE
      and CURRENT_DATE = mph.PUBLIC_HOLIDAY_DATE
)
;
