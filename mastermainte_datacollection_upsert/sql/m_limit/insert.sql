insert
into `M_LIMIT`(
    `DATA_COLLECTION_SEQ`
    , `LIMIT_SUB_NO`
    , `LIMIT_JUDGE_TYPE`
    , `LIMIT_VALUE`
    , `CREATED_AT`
)
values (
     %(dataCollectionSeq)d
    , %(limitSubNo)d
    , %(limitJudgeType)d
    , %(limitValue)f
    , '%(createdAt)s'
)