insert
into `M_LIMIT_CHECK`(
    `DATA_COLLECTION_SEQ`
    , `LIMIT_COUNT_TYPE`
    , `LIMIT_COUNT`
    , `LIMIT_COUNT_RESET_RANGE`
    , `ACTION_RANGE`
    , `NEXT_ACTION`
    , `CREATED_AT`
)
values (
    0
    , '1'
    , 5
    , 3
    , 2
    , 1
    , '2021/07/28 16:40:29'
    )
ON DUPLICATE KEY UPDATE
    DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
;