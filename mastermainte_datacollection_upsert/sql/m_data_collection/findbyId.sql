select
    mdc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , ifnull(mdc.SENSOR_UNIT, '') as sensorUnit
    , ifnull(mdc.STATUS_TRUE, '') as statusTrue
    , ifnull(mdc.STATUS_FALSE, '') as statusFalse
    , mdc.REVISION_MAGNIFICATION as revisionMagnification
    , mdc.X_COORDINATE as xCoordinate
    , mdc.Y_COORDINATE as yCoordinate
    
    , mlc.LIMIT_CHECK_SEQ as limitCheckSeq
    
    , greatest(ifnull(mdc.VERSION, 0), 
    		   ifnull(mlf.VERSION, 0), 
    		   ifnull(mlc.VERSION, 0), 
    		   ifnull(ml.VERSION, 0)) as version
from
    M_DATA_COLLECTION mdc 
	inner join M_LINK_FLG mlf 
    	on mlf.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ 
	left outer join M_LIMIT_CHECK mlc 
		on mlc.DATA_COLLECTION_SEQ  = mdc.DATA_COLLECTION_SEQ 
	left outer join M_LIMIT ml 
	    on ml.LIMIT_CHECK_SEQ = mlc.LIMIT_CHECK_SEQ
where
	mdc.DELETE_COUNT = 0
	%(p_whereParams)s
;

