select
    ms.DELETE_COUNT as deleteCount
from
    M_MAIL_SEND ms
where
	ms.DELETE_COUNT = %(deleteCount)d
and ms.MAIL_SEND_ID = '%(mailSendId)s'
;

