update `M_DATA_COLLECTION` set
    `DELETE_COUNT` = %(deleteCount)d
    , `UPDATED_AT` = '%(updatedAt)s'
    , `UPDATED_USER` = '%(updatedUser)s'
    , `VERSION` = VERSION + 1
where
    DEVICE_ID = '%(deviceId)s'
and SENSOR_ID = '%(sensorId)s'
and DELETE_COUNT = 0
;