insert 
into `T_LIMIT_HIT_MANAGED`( 
    `LIMIT_HIT_MANAGED_SEQ`
    , `DATA_COLLECTION_SEQ`
    , `DETECTION_DATETIME`
    , `LIMIT_SUB_NO`
    , `LIMIT_HIT_STATUS`
    , `CREATED_AT`
) 
values ( 
    %(limitHitManagedSeq)d
    , %(dataCollectionSeq)d
    , '%(detectionDateTime)s'
    , %(limitSubNo)d
    , '0'
    , '%(createdAt)s'
)
on duplicate key update
	LIMIT_HIT_STATUS = '%(limitHitStatus)s'
	, UPDATED_AT = '%(updatedAt)s'
	, VERSION = VERSION + 1
;