/* 時系列より閾値成立管理からセンサの受信タイムスタンプ以前のものを抽出 */
select
    tpt.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tpt.RECEIVED_DATETIME as receivedDatetime
    , tpt.SENSOR_VALUE as sensorValue
    , tpt.CREATED_AT as createdAt
from
    T_PUBLIC_TIMESERIES tpt
    left outer join T_LIMIT_HIT_MANAGED tlhm
        on tpt.DATA_COLLECTION_SEQ = tlhm.DATA_COLLECTION_SEQ
        and not exists(
            select
                1
            from
                T_LIMIT_HIT_MANAGED tlhmSub
            where
                tlhm.DATA_COLLECTION_SEQ = tlhmSub.DATA_COLLECTION_SEQ
            and tlhm.DETECTION_DATETIME < tlhmSub.DETECTION_DATETIME
        )

where
    tpt.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
and tpt.RECEIVED_DATETIME <= '%(timeStamp)s'   /* センサの受信タイムスタンプ以前の過去分 */
and ifnull(tlhm.DETECTION_DATETIME, str_to_date('19000101000000', '%%Y%%m%%d%%k%%i%%s') ) < tpt.RECEIVED_DATETIME    /* 閾値成立管理より新しいもの */
%(whereParam)s

order by
    tpt.RECEIVED_DATETIME desc

