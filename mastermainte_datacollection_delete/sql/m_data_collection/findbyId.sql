select
    mdc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , mlc.LIMIT_CHECK_SEQ as limitCheckSeq
from
    M_DATA_COLLECTION mdc 
	left outer join M_LIMIT_CHECK mlc 
		on mlc.DATA_COLLECTION_SEQ  = mdc.DATA_COLLECTION_SEQ 

%(p_whereParams)s
;

