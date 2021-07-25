select
	 MAIL_SEND_ID as mailSendId
	,DELETE_COUNT as deleteCount
	,EMAIL_ADDRESS as emailAddress
	,SEND_WEEK_TYPE as sendWeekType
	,SEND_FREQUANCY as sendFrequancy
	,SEND_TIME_FROM as sendTimeFrom
	,SEND_TIME_TO as sendTimeTo
	,MAIL_SUBJECT as mailSubject
	,MAIL_TEXT as mailText
	,CREATED_AT as createdAt
	,UPDATED_AT as updatedAt
	,UPDATED_USER as updatedUser
	,VERSION as version
from
    M_MAIL_SEND
where
	MAIL_SEND_ID = %(mailSendId)d
and DELETE_COUNT = %(deleteCount)d
;

