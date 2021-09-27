/* 閾値成立管理から最新日付の抽出 */
select
    max(tlhm.DETECTION_DATETIME) as detectionDateTime
from
    T_LIMIT_HIT_MANAGED tlhm 
where
    tlhm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
;