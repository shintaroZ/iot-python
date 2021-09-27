/* 時系列から最新の閾値成立管理より未来分を取得 */
select
    tpt.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tpt.RECEIVED_DATETIME as receivedDatetime
    , tpt.SENSOR_VALUE as sensorValue
    , tpt.CREATED_AT as createdAt
from
    T_PUBLIC_TIMESERIES tpt
where
    tpt.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and '%(detectionDateTime)s' < tpt.RECEIVED_DATETIME
%(whereParam)s

order by
    tpt.RECEIVED_DATETIME desc
;