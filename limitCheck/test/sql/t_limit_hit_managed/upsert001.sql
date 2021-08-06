insert
into `T_LIMIT_HIT_MANAGED`(
    `DATA_COLLECTION_SEQ`
    , `DETECTION_DATETIME`
    , `LIMIT_SUB_NO`
    , `CREATED_AT`
)
values (
    4
    , '2021/08/01 6:16:43.014'
    , '2'
    , CURRENT_TIMESTAMP
)
on duplicate key update
    DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
    , DETECTION_DATETIME = values(DETECTION_DATETIME)
    , LIMIT_SUB_NO = values(LIMIT_COUNT)
;

