select
	mdc.VERSION as version
from
    M_DATA_COLLECTION mdc
where
	not exists (
		select
			1
		from
			M_DATA_COLLECTION mdcSub
		where
			mdc.DEVICE_ID = mdcSub.DEVICE_ID
		and mdc.SENSOR_ID = mdcSub.SENSOR_ID
		and mdc.VERSION < mdcSub.VERSION
	)
	%(p_whereParams)s
;

