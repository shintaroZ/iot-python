insert
into `M_MAIL_SEND`(
    `MAIL_SEND_SEQ`
    , `MAIL_SEND_ID`
    , `DELETE_FLG`
    , `VERSION`
    , `EMAIL_ADDRESS`
    , `SEND_WEEK_TYPE`
    , `SEND_FREQUANCY`
    , `SEND_TIME_FROM`
    , `SEND_TIME_TO`
    , `MAIL_SUBJECT`
    , `MAIL_TEXT_HEADER`
    , `MAIL_TEXT_BODY`
    , `MAIL_TEXT_FOOTER`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `UPDATED_USER`
)
values (
    %(mailSendSeq)d
    , %(mailSendId)d
    , %(deleteFlg)d
    , %(version)d
    , '%(emailAddress)s'
    , %(sendWeekType)d
    , %(sendFrequancy)d
    , '%(sendTimeFrom)s'
    , '%(sendTimeTo)s'
    , '%(mailSubject)s'
    , '%(mailTextHeader)s'
    , '%(mailTextBody)s'
    , '%(mailTextFooter)s'
    , CURRENT_TIMESTAMP
    , 'devUser'
)
on DUPLICATE key update
    MAIL_SEND_ID = values(MAIL_SEND_ID)
    ,DELETE_FLG = values(DELETE_FLG)
    ,VERSION = values(VERSION)
    ,EMAIL_ADDRESS = values(EMAIL_ADDRESS)
    ,SEND_WEEK_TYPE = values(SEND_WEEK_TYPE)
    ,SEND_FREQUANCY = values(SEND_FREQUANCY)
    ,SEND_TIME_FROM = values(SEND_TIME_FROM)
    ,SEND_TIME_TO = values(SEND_TIME_TO)
    ,MAIL_SUBJECT = values(MAIL_SUBJECT)
    ,MAIL_TEXT_HEADER = values(MAIL_TEXT_HEADER)
    ,MAIL_TEXT_BODY = values(MAIL_TEXT_BODY)
    ,MAIL_TEXT_FOOTER = values(MAIL_TEXT_FOOTER)
    ,UPDATED_AT = CURRENT_TIMESTAMP
;
