select
	MAIL_SEND_ID as mailSendId
	, EMAIL_ADDRESS as emailAddress
	, SEND_WEEK_TYPE as sendWeekType
	, SEND_FREQUANCY as sendFrequancy
	, SEND_TIME_FROM as sendTimeFrom
	, SEND_TIME_TO as sendTimeTo
	, MAIL_SUBJECT as mailSubject
	, MAIL_TEXT as mailText
    , DATE_FORMAT(CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as createdAt
    , DATE_FORMAT(UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as updatedAt
	, UPDATED_USER as updatedUser
	, VERSION as version
from
	M_MAIL_SEND mms
where
	DELETE_COUNT = 0