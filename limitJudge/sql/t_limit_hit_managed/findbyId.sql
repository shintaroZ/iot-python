/* 閾値成立管理から最新日付の抽出 */
select
    tlhm.DETECTION_DATETIME as detectionDateTime
from
    T_LIMIT_HIT_MANAGED tlhm 
where
    tlhm.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and not exists ( 
        select
            1 
        from
            T_LIMIT_HIT_MANAGED tlhmSub 
        where
            tlhm.DATA_COLLECTION_SEQ = tlhmSub.DATA_COLLECTION_SEQ 
        and tlhm.DETECTION_DATETIME < tlhmSub.DETECTION_DATETIME
    )
;