select
	mms.MAIL_SEND_SEQ as mailSendSeq
	, mms.MAIL_SEND_ID as mailSendId
	, mms.DELETE_FLG as deleteFlg
	, mms.VERSION as version
	, mms.EMAIL_ADDRESS as emailAddress
	, mms.SEND_WEEK_TYPE as sendWeekType
	, mms.SEND_FREQUANCY as sendFrequancy
	, mms.SEND_TIME_FROM as sendTimeFrom
	, mms.SEND_TIME_TO as sendTimeTo
	, mms.MAIL_SUBJECT as mailSubject
	, mms.MAIL_TEXT_HEADER as mailTextHeader
	, mms.MAIL_TEXT_BODY as mailTextBody
	, mms.MAIL_TEXT_FOOTER as mailTextFooter
	, mms.CREATED_AT as createdAt
	, mms.UPDATED_AT as updatedAt
	, mms.UPDATED_USER as updatedUser
from
    M_MAIL_SEND mms
where
	mms.MAIL_SEND_ID = %(mailSendId)d
and not exists (
	select
		1
	from
		M_MAIL_SEND mmsSub
	where
		mms.MAIL_SEND_ID = mmsSub.MAIL_SEND_ID
	and mms.VERSION < mmsSub.VERSION
)
;

