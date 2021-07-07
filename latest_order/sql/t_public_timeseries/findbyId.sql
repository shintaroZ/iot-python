select
    count(*) as count
from
    T_PUBLIC_TIMESERIES
where
    DATA_COLLECTION_SEQ = %(p_dataCollectionSeq)d
and RECEIVED_DATETIME = '%(p_receivedDateTime)s'
;