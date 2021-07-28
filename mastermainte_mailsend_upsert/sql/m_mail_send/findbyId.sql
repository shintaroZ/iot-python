select
	mms.VERSION as version
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

