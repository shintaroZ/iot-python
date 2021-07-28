select
    mdc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , ifnull(mdc.SENSOR_UNIT, '') as sensorUnit
    , ifnull(mdc.STATUS_TRUE, '') as statusTrue
    , ifnull(mdc.STATUS_FALSE, '') as statusFalse
    , mdc.REVISION_MAGNIFICATION as revisionMagnification
    , mdc.X_COORDINATE as xCoordinate
    , mdc.Y_COORDINATE as yCoordinate
    , mdc.VERSION as version
from
    M_DATA_COLLECTION mdc
	inner join M_LINK_FLG mlf
    	on mlf.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
	left outer join M_LIMIT_CHECK mlc
		on mlc.DATA_COLLECTION_SEQ  = mdc.DATA_COLLECTION_SEQ
	left outer join M_LIMIT ml
	    on ml.DATA_COLLECTION_SEQ = mlc.DATA_COLLECTION_SEQ
where
	mdc.DELETE_FLG = 0
and not exists (
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

