
insert 
into `M_LIMIT_CHECK`( 
    `DATA_COLLECTION_SEQ`
    , `LIMIT_CHECK_SEQ`
    , `LIMIT_COUNT_TYPE`
    , `LIMIT_COUNT`
    , `LIMIT_COUNT_RESET_RANGE`
    , `ACTION_RANGE`
    , `NEXT_ACTION`
    , `CREATED_AT`
    , `UPDATED_USER`
    , `VERSION`
) 
values ( 
     %(dataCollectionSeq)d
    , %(limitCheckSeq)d
    , %(limitCountType)d
    , %(limitCount)d
    , %(limitCountResetRange)d
    , %(actionRange)d
    , %(nextAction)d
    , '%(createdAt)s'
    , '%(updatedUser)s'
    , %(version)d
)
ON DUPLICATE KEY UPDATE
    UPDATED_AT = 
        case
            when LIMIT_CHECK_SEQ <> values(LIMIT_CHECK_SEQ) 
             or LIMIT_COUNT_TYPE <> values(LIMIT_COUNT_TYPE)
             or LIMIT_COUNT <> values(LIMIT_COUNT)
             or LIMIT_COUNT_RESET_RANGE <> values(LIMIT_COUNT_RESET_RANGE)
             or ACTION_RANGE <> values(ACTION_RANGE)
             or NEXT_ACTION <> values(NEXT_ACTION)
            then '%(updatedAt)s'
            else UPDATED_AT
        end
    , UPDATED_USER = 
        case
            when LIMIT_CHECK_SEQ <> values(LIMIT_CHECK_SEQ) 
             or LIMIT_COUNT_TYPE <> values(LIMIT_COUNT_TYPE)
             or LIMIT_COUNT <> values(LIMIT_COUNT)
             or LIMIT_COUNT_RESET_RANGE <> values(LIMIT_COUNT_RESET_RANGE)
             or ACTION_RANGE <> values(ACTION_RANGE)
             or NEXT_ACTION <> values(NEXT_ACTION)
            then values(UPDATED_USER)
            else UPDATED_USER
        end
    , VERSION = 
        case
            when LIMIT_CHECK_SEQ <> values(LIMIT_CHECK_SEQ) 
             or LIMIT_COUNT_TYPE <> values(LIMIT_COUNT_TYPE)
             or LIMIT_COUNT <> values(LIMIT_COUNT)
             or LIMIT_COUNT_RESET_RANGE <> values(LIMIT_COUNT_RESET_RANGE)
             or ACTION_RANGE <> values(ACTION_RANGE)
             or NEXT_ACTION <> values(NEXT_ACTION)
            then values(VERSION)
            else VERSION
        end
    , LIMIT_CHECK_SEQ = if(LIMIT_CHECK_SEQ = values(LIMIT_CHECK_SEQ), LIMIT_CHECK_SEQ, values(LIMIT_CHECK_SEQ) )
    , LIMIT_COUNT_TYPE = if(LIMIT_COUNT_TYPE = values(LIMIT_COUNT_TYPE), LIMIT_COUNT_TYPE, values(LIMIT_COUNT_TYPE) )
    , LIMIT_COUNT = if(LIMIT_COUNT = values(LIMIT_COUNT), LIMIT_COUNT, values(LIMIT_COUNT) )
    , LIMIT_COUNT_RESET_RANGE = if(LIMIT_COUNT_RESET_RANGE = values(LIMIT_COUNT_RESET_RANGE), LIMIT_COUNT_RESET_RANGE, values(LIMIT_COUNT_RESET_RANGE) )
    , ACTION_RANGE = if(ACTION_RANGE = values(ACTION_RANGE), ACTION_RANGE, values(ACTION_RANGE) )
    , NEXT_ACTION = if(NEXT_ACTION = values(NEXT_ACTION), NEXT_ACTION, values(NEXT_ACTION) )

;
