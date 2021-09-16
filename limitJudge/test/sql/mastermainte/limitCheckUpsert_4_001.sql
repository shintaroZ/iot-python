/*
閾値条件マスタのテストデータ
データ定義マスタシーケンス:4
*/
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
    4
    , '1'
    , 1
    , 0
    , 2
    , '1'
    , CURRENT_TIMESTAMP
    )
on duplicate key update
    DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
    , LIMIT_COUNT_TYPE = values(LIMIT_COUNT_TYPE)
    , LIMIT_COUNT = values(LIMIT_COUNT)
    , LIMIT_COUNT_RESET_RANGE = values(LIMIT_COUNT_RESET_RANGE)
    , ACTION_RANGE = values(ACTION_RANGE)
    , NEXT_ACTION = values(NEXT_ACTION)
;
