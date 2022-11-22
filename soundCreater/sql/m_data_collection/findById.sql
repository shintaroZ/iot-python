select
    M.DATA_COLLECTION_SEQ as dataCollectionSeq
from
    M_DATA_COLLECTION M
where
    M.DEVICE_ID = '%(deviceId)s'
and M.DELETE_FLG = 0
and M.EDGE_TYPE = 2
and not exists (
    select
        1
    from
        M_DATA_COLLECTION Sub
    where
        M.DEVICE_ID = Sub.DEVICE_ID
    and M.DELETE_FLG = Sub.DELETE_FLG
    and M.EDGE_TYPE = Sub.EDGE_TYPE
    and M.VERSION < Sub.VERSION
)
;