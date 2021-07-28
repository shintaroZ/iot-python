update `M_DATA_COLLECTION` set
    `DELETE_FLG` = %(deleteFlg)d
    , `UPDATED_AT` = '%(updatedAt)s'
    , `UPDATED_USER` = '%(updatedUser)s'
where
    DEVICE_ID = '%(deviceId)s'
and SENSOR_ID = '%(sensorId)s'
and DELETE_FLG = 0
;