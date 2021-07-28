insert
into `M_LIMIT`(
    `DATA_COLLECTION_SEQ`
    , `LIMIT_SUB_NO`
    , `LIMIT_JUDGE_TYPE`
    , `LIMIT_VALUE`
    , `CREATED_AT`
)
values (
    0
    , 2
    , 0
    , 7
    , '2021/07/28 16:30:31'
)
ON DUPLICATE KEY UPDATE
    DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
;
