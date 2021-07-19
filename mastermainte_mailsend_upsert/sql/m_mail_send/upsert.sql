insert
into `M_MAIL_SEND`(
    `MAIL_SEND_ID`
    , `DELETE_COUNT`
    , `EMAIL_ADDRESS`
    , `SEND_WEEK_TYPE`
    , `SEND_FREQUANCY`
    , `SEND_TIME_FROM`
    , `SEND_TIME_TO`
    , `MAIL_SUBJECT`
    , `MAIL_TEXT`
    , `CREATED_AT`
    , `UPDATED_USER`
    , `VERSION`
)
values (
     %(mailSendId)d
    , 0
    , '%(emailAddress)s'
    , %(sendWeekType)d
    , %(sendFrequancy)d
    , '%(sendTimeFrom)s'
    , '%(sendTimeTo)s'
    , '%(mailSubject)s'
    , '%(mailText)s'
    , '%(createdAt)s'
    , '%(updatedUser)s'
    , 0
)
ON DUPLICATE KEY UPDATE
      EMAIL_ADDRESS = values(EMAIL_ADDRESS)
    , SEND_WEEK_TYPE = values(SEND_WEEK_TYPE)
    , SEND_FREQUANCY = values(SEND_FREQUANCY)
    , SEND_TIME_FROM = values(SEND_TIME_FROM)
    , SEND_TIME_TO = values(SEND_TIME_TO)
    , MAIL_SUBJECT = values(MAIL_SUBJECT)
    , MAIL_TEXT = values(MAIL_TEXT)
    , UPDATED_AT = '%(updatedAt)s'
    , UPDATED_USER = values(UPDATED_USER)
    , VERSION = VERSION + 1
;
