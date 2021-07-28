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
     %(dataCollectionSeq)d
    , %(limitCountType)d
    , %(limitCount)d
    , %(limitCountResetRange)d
    , %(actionRange)d
    , %(nextAction)d
    , '%(createdAt)s'
)
;
