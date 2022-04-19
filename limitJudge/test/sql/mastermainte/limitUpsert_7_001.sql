/*
閾値マスタのテストデータ
データ定義マスタシーケンス:7
*/
insert 
into ins001.`M_LIMIT`( 
    `DATA_COLLECTION_SEQ`
    , `LIMIT_SUB_NO`
    , `LIMIT_JUDGE_TYPE`
    , `LIMIT_VALUE`
    , `CREATED_AT`
) 
values 
(
	7
	, '1'
	, '1'
	, 1
	, CURRENT_TIMESTAMP
)
on duplicate key update
    DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
    , LIMIT_SUB_NO = values(LIMIT_SUB_NO)
    , LIMIT_JUDGE_TYPE = values(LIMIT_JUDGE_TYPE)
    , LIMIT_VALUE = values(LIMIT_VALUE)
;
