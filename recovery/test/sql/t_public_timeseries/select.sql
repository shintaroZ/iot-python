select
    DATA_COLLECTION_SEQ as dataCollectionSeq
    ,RECEIVED_DATETIME as receivedDateTime
    ,SENSOR_VALUE as sensorValue
    ,CREATED_AT as createdDateTime
from
    T_PUBLIC_TIMESERIES
where
    DATA_COLLECTION_SEQ = %(p_dataCollectionSeq)d
and RECEIVED_DATETIME = '%(p_receivedDateTime)s'
;