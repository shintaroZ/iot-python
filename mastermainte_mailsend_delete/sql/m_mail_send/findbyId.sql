select
    M.DELETE_COUNT as deleteCount
from
    M_MAIL_SEND M
where
	0 < M.DELETE_COUNT
	and MAIL_SEND_ID = %(mailSendId)d
	and not exists (
		select
			1
		from
			M_MAIL_SEND Sub
		where
			M.MAIL_SEND_ID = Sub.MAIL_SEND_ID
		and M.DELETE_COUNT < Sub.DELETE_COUNT
	)
;

