select
    FILE_NAME as fileName
    ,EDGE_NAME as edgeName
    ,CREATED_DATETIME as createdDatetime
from
    T_SOUNDFILE_HISTORY
where
    SOUND_ID = '%(p_sound_id)s'