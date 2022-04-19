select
    `DATA_COLLECTION_SEQ` as dataCollectionSeq
    , DATE_FORMAT(CREATED_DATETIME, '%%Y/%%m/%%d %%H:%%i:%%s.%%f') as createdDateTime
    , `FILE_TYPE` as fileType
    , `FILE_NAME` as fileName
    , DATE_FORMAT(CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as createdAt
from
    T_SOUNDFILE_HISTORY

%(p_whereParams)s
    
order by
	CREATED_DATETIME
	, FILE_TYPE
;