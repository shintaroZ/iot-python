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
    , `UPDATED_USER`
)
values (
     0
    , 1
    , 0
    , 0
    , 'shintaro_otoi@icloud.com'
    , 0
    , 1
    , '070000'
    , '230000'
    , '閾値メールSubject'
    , '閾値メールHeader'
    , '閾値メールBody'
    , '閾値メールFooter'
    , CURRENT_TIMESTAMP
    , 'devUser'
)
;
