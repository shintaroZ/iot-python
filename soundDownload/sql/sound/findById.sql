select
    FILE_NAME as fileName
from
    T_SOUNDFILE_HISTORY
where
    DATA_COLLECTION_SEQ = %(p_dataCollectionSeq)d
and CREATED_DATETIME = '%(p_createdDateTime)s'
and FILE_TYPE = %(p_fileType)d