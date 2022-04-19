
insert 
into `T_SOUNDFILE_HISTORY`( 
    `DATA_COLLECTION_SEQ`
    , `CREATED_DATETIME`
    , `FILE_TYPE`
    , `FILE_NAME`
    , `CREATED_AT`
) 
values ( 
    %(dataCollectionSeq)d
    , '%(created_datetime)s'
    , '%(fileType)s'
    , '%(fileName)s'
    , '%(created_at)s'
)
ON DUPLICATE KEY UPDATE 
    `FILE_TYPE` = values(FILE_TYPE)
    , `FILE_NAME` =values(FILE_NAME)
;