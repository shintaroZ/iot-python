select
    tlhm.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tlhm.DETECTION_DATETIME as detectionDateTime
    , tlhm.LIMIT_SUB_NO as limitSubNo
from
    T_LIMIT_HIT_MANAGED tlhm
where
    tmsm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and tmsm.DETECTION_DATETIME = %(detectionDateTime)d
and tmsm.LIMIT_SUB_NO = %(limitSubNo)d
 ;