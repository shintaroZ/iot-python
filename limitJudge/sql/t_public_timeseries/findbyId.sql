/* 時系列より閾値成立管理からセンサの受信タイムスタンプ以前のものを抽出 */
select
    tpt.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tpt.RECEIVED_DATETIME as receivedDatetime
    , tpt.SENSOR_VALUE as sensorValue
    , tpt.CREATED_AT as createdAt
from
    T_PUBLIC_TIMESERIES tpt
where
    tpt.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
%(whereParam)s

order by
    tpt.RECEIVED_DATETIME desc

