/* ignore付だが呼び出し元で重複データは制限される */
insert ignore
into `T_LIMIT_HIT_MANAGED`(
      `DATA_COLLECTION_SEQ`
    , `DETECTION_DATETIME`
    , `LIMIT_SUB_NO`
    , `CREATED_AT`
)
values (
      %(dataCollectionSeq)d
    , '%(timeStamp)s'
    , %(limitSubNo)d
    , '%(createdAt)s'
)
;