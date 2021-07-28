insert
into `M_LIMIT`(
    `DATA_COLLECTION_SEQ`
    , `LIMIT_SUB_NO`
    , `LIMIT_JUDGE_TYPE`
    , `LIMIT_VALUE`
    , `CREATED_AT`
)
values (
    1
    , 1
    , 2
    , -12
    , '2021/07/28 16:30:31'
)
ON DUPLICATE KEY UPDATE
    DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
;
