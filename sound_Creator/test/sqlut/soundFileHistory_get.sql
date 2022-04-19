select
    `DATA_COLLECTION_SEQ` as dataCollectionSeq
    , `CREATED_DATETIME` as created_datetime
    , `FILE_TYPE` as fileType
    , `FILE_NAME` as fileName
from
	T_SOUNDFILE_HISTORY
where
	DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and CREATED_DATETIME = '%(createdDatetime)s'