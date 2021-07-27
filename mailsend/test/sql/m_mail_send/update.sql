update M_MAIL_SEND mms
set
    mms.EMAIL_ADDRESS = '%(emailAddress)s'
    , mms.SEND_WEEK_TYPE = %(sendWeekType)d
    , mms.SEND_FREQUANCY = %(sendFrequancy)d
    , mms.SEND_TIME_FROM = '%(sendTimeFrom)s'
    , mms.SEND_TIME_TO = '%(sendTimeTo)s'
    , mms.MAIL_SUBJECT = '%(mailSubject)s'
    , mms.MAIL_TEXT = '%(mailText)s'
    , mms.UPDATED_AT = '%(createdAt)s'
    , mms.UPDATED_USER = 'unitTest'
    , mms.VERSION = mms.VERSION + 1
where
    MAIL_SEND_ID = %(mailSendId)d
    and DELETE_COUNT = 0