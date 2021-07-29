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
    %(insert_mailTextHeader)s
    , `MAIL_TEXT_BODY`
    %(insert_mailTextFooter)s
    , `CREATED_AT`
    , `UPDATED_USER`
)
values (
     %(mailSendSeq)d
    , %(mailSendId)d
    , 0
    , %(version)d
    , '%(emailAddress)s'
    , %(sendWeekType)d
    , %(sendFrequancy)d
    , '%(sendTimeFrom)s'
    , '%(sendTimeTo)s'
    , '%(mailSubject)s'
    %(values_mailTextHeader)s
    , '%(mailTextBody)s'
    %(values_mailTextFooter)s
    , '%(createdAt)s'
    , '%(updatedUser)s'
)
;
