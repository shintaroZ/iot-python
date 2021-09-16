/*
閾値マスタのテストデータ
データ定義マスタシーケンス:1
*/
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
    , '1'
    , '2'
    , -12
    , CURRENT_TIMESTAMP
    )
   ,(
    4
    , '2'
    , '1'
    , 0
    , CURRENT_TIMESTAMP
    )
   ,(
    4
    , '3'
    , '0'
    , 7
    , CURRENT_TIMESTAMP
    )
on duplicate key update
    DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
    , LIMIT_SUB_NO = values(LIMIT_SUB_NO)
    , LIMIT_JUDGE_TYPE = values(LIMIT_JUDGE_TYPE)
    , LIMIT_VALUE = values(LIMIT_VALUE)
;