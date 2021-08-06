insert
into `T_MAIL_SEND_MANAGED`(
    `DATA_COLLECTION_SEQ`
    , `DETECTION_DATETIME`
    , `LIMIT_SUB_NO`
    , `MAIL_SEND_SEQ`
    , `SEND_STATUS`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `VERSION`
)
values  (
    4
    , '2021/07/20 13:00:00'
    , '1'
    , 0
    , '0'
    , CURRENT_TIMESTAMP
    , null
    , 0
)
on duplicate key update
	DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
	, DETECTION_DATETIME = values(DETECTION_DATETIME)
	, LIMIT_SUB_NO = values(LIMIT_SUB_NO)
	, MAIL_SEND_SEQ = values(MAIL_SEND_SEQ)
	, SEND_STATUS = values(SEND_STATUS)
	, UPDATED_AT = CURRENT_TIMESTAMP
	, VERSION = VERSION +1
;
