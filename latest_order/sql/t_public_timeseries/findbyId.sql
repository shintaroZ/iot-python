select
    count(*) as count
from
    T_PUBLIC_TIMESERIES
where
    COLLECTION_TYPE = '%(p_collectionType)s'
and RECEIVED_DATETIME = '%(p_receivedDateTime)s'
and SENSOR_NAME = '%(p_sensorName)s'
;