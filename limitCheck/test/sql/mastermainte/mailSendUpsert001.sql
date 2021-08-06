/* テストデータ
 * メール通知マスタシーケンス:1
 */
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
    1
    , '0'
    , '0'
    , 1
    , 'shintaro_otoi@icloud.com'
    , '0'
    , '1'
    , '000000'
    , '235959'
    , '閾値メールSubject2'
    , '閾値メールHeader2'
    , '閾値メールBody2'
    , '閾値メールFooter2'
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