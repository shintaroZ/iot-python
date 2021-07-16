
insert 
into `M_LIMIT`( 
    `LIMIT_CHECK_SEQ`
    , `LIMIT_SUB_NO`
    , `LIMIT_JUDGE_TYPE`
    , `LIMIT_VALUE`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `UPDATED_USER`
    , `VERSION`
) 
values ( 
     %(limitCheckSeq)d
    , %(limitSubNo)d
    , %(limitJudgeType)d
    , %(limitValue)f
    , '%(createdAt)s'
    , '%(updatedAt)s'
    , '%(updatedUser)s'
    , %(version)d
)