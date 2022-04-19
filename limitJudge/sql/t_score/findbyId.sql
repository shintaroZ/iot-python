/* スコアデータから最新の閾値成立管理より未来分を取得 */
select
    ts.DATA_COLLECTION_SEQ as dataCollectionSeq
    , ts.DETECTION_DATETIME as receivedDatetime
    , ts.DETECTION_FLAG as sensorValue
from
    T_SCORE ts
where
    ts.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and '%(detectionDateTime)s' < ts.DETECTION_DATETIME
%(whereParam)s

order by
    ts.DETECTION_DATETIME desc
    
%(limitParam)s
;