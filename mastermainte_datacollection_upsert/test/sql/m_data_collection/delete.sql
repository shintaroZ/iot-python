delete from M_DATA_COLLECTION 
where DEVICE_ID = '%(deviceId)s'
and SENSOR_ID = '%(sensorId)s'
;
