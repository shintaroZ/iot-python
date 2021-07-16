
insert 
into `M_LINK_FLG`( 
    `DATA_COLLECTION_SEQ`
    , `SAVING_FLG`
    , `LIMIT_CHECK_FLG`
    , `CREATED_AT`
    , `UPDATED_USER`
    , `VERSION`
) 
values ( 
     %(dataCollectionSeq)d
    , %(savingFlg)d
    , %(limitCheckFlg)d
    , '%(createdAt)s'
    , '%(updatedUser)s'
    , %(version)d
)
ON DUPLICATE KEY UPDATE
    SAVING_FLG = if(SAVING_FLG = values(SAVING_FLG), values(SAVING_FLG), SAVING_FLG)
    ,LIMIT_CHECK_FLG = if(LIMIT_CHECK_FLG = values(LIMIT_CHECK_FLG), values(LIMIT_CHECK_FLG), LIMIT_CHECK_FLG)
    ,UPDATED_AT = 
        case
            when SAVING_FLG <> values(SAVING_FLG) 
             and LIMIT_CHECK_FLG <> values(LIMIT_CHECK_FLG)
            then '%(updatedAt)s'
            else UPDATED_AT
        end
    ,UPDATED_USER = 
        case
            when SAVING_FLG <> values(SAVING_FLG) 
             and LIMIT_CHECK_FLG <> values(LIMIT_CHECK_FLG)
            then values(UPDATED_USER)
            else UPDATED_USER
        end
    ,VERSION = 
        case
            when SAVING_FLG <> values(SAVING_FLG) 
             and LIMIT_CHECK_FLG <> values(LIMIT_CHECK_FLG)
            then values(VERSION)
            else VERSION
        end
;