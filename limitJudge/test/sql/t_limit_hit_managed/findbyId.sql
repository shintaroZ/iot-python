select
    tlhm.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tlhm.DETECTION_DATETIME as detectionDateTime
    , tlhm.LIMIT_SUB_NO as limitSubNo
from
    T_LIMIT_HIT_MANAGED tlhm
where
    tlhm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and tlhm.DETECTION_DATETIME = '%(detectionDateTime)s'
and tlhm.LIMIT_SUB_NO = %(limitSubNo)d
 ;