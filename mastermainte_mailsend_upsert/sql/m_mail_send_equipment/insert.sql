insert
into `M_MAIL_SEND_EQUIPMENT`(
    `MAIL_SEND_SEQ`
    , `EQUIPMENT_ID`
    , `CREATED_AT`
)
values (
     %(mailSendSeq)d
    , '%(equipmentId)s'
    , '%(createdAt)s'
)
;
