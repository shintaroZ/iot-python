select
    mdc.DELETE_COUNT as deleteCount
from
    M_DATA_COLLECTION mdc
where
	0 < mdc.DELETE_COUNT
	%(p_whereParams)s
	and not exists (
		select
			1
		from
			M_DATA_COLLECTION mdcSub
		where
			mdc.DEVICE_ID = mdcSub.DEVICE_ID
		and mdc.SENSOR_ID = mdcSub.SENSOR_ID
		and mdc.DELETE_COUNT < mdcSub.DELETE_COUNT
	)
;

