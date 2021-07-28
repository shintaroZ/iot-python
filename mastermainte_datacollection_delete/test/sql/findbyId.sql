select
    mdc.DELETE_FLG as deleteFlg
from
    M_DATA_COLLECTION mdc
	inner join M_LINK_FLG mlf
    	on mlf.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
	left outer join M_LIMIT_CHECK mlc
		on mlc.DATA_COLLECTION_SEQ  = mdc.DATA_COLLECTION_SEQ
	left outer join M_LIMIT ml
	    on ml.DATA_COLLECTION_SEQ = mlc.DATA_COLLECTION_SEQ

where
	mdc.DEVICE_ID = '%(deviceId)s'
and mdc.SENSOR_ID = '%(sensorId)s'
;

