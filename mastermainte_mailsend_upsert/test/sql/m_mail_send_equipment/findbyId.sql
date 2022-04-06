select
    MAIL_SEND_SEQ as mailSendSeq
    , EQUIPMENT_ID as equipmentId
    , CREATED_AT as createdAt
from
    M_MAIL_SEND_EQUIPMENT
where
    MAIL_SEND_SEQ = %(mailSendSeq)d
order by
	EQUIPMENT_ID
;
