select
    mdc.DELETE_COUNT as deleteCount
from
    M_DATA_COLLECTION mdc
where
	mdc.DELETE_COUNT = %(deleteCount)d
and mdc.DEVICE_ID = '%(deviceId)s'
and mdc.SENSOR_ID = '%(sensorId)s'
;

