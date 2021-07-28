update M_DATA_COLLECTION set DELETE_FLG = 1
where DEVICE_ID = '%(deviceId)s'
and SENSOR_ID = '%(sensorId)s'
;
