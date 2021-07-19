update `M_DATA_COLLECTION` M set
    `DELETE_COUNT` = %(deleteCount)d
where
    DEVICE_ID = '%(deviceId)s'
and SENSOR_ID = '%(sensorId)s'
and DELETE_COUNT = %(whereDeleteCount)d
;