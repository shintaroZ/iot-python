/* 蓄積あり、閾値判定あり */
insert
into `M_LINK_FLG` (
    `DATA_COLLECTION_SEQ`
    , `SAVING_FLG`
    , `LIMIT_CHECK_FLG`
    , `CREATED_AT`
)
values (
    4
    , '1'
    , '1'
    , '2021/07/28 16:30:31'
)
ON DUPLICATE KEY UPDATE
    DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
;
