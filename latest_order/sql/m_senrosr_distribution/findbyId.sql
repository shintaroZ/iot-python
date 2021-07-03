select
    SENSOR_NAME as sensorName
    ,SENSOR_UNIT as sensorUnit
    ,TABLE_NAME as tableName
    ,CORRECTION_MAGNIFICATION as correctionMagnification
    ,COLLECTION_TYPE as correctionType
from
    M_SENROSR_DISTRIBUTION
where
    DEVICE_ID = '%(p_deviceId)s'
and SENSOR_ID = '%(p_sensorId)s'
for update
;
