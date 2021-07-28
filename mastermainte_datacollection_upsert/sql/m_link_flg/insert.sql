insert
into `M_LINK_FLG`(
    `DATA_COLLECTION_SEQ`
    , `SAVING_FLG`
    , `LIMIT_CHECK_FLG`
    , `CREATED_AT`
)
values (
     %(dataCollectionSeq)d
    , %(savingFlg)d
    , %(limitCheckFlg)d
    , '%(createdAt)s'
)
;