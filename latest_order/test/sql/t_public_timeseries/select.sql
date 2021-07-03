select
    COLLECTION_TYPE as collectionType
    ,RECEIVED_DATETIME as receivedDateTime
    ,SENSOR_NAME as sensorName
    ,SENSOR_VALUE as sensorValue
    ,SENSOR_UNIT as sensorUnit
    ,CREATED_AT as createdDateTime
    ,UPDATED_AT as updateDateTime
    ,VERSION as version
from
    T_PUBLIC_TIMESERIES
where
    COLLECTION_TYPE = '%(p_collectionType)s'
and RECEIVED_DATETIME = '%(p_receivedDateTime)s'
and SENSOR_NAME = '%(p_sensorName)s'
;