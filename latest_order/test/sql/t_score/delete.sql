delete from T_SCORE
where DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and DETECTION_DATETIME = '%(detectionDatetime)s'
;