select
    count(*) as count
from
    T_PUBLIC_TIMESERIES
where
    DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and RECEIVED_DATETIME = '%(receivedDateTime)s'
;