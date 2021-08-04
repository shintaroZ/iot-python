select
    mdc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , mdc.DEVICE_ID as deviceId
    , mdc.SENSOR_ID as sensorId
    , mlc.LIMIT_COUNT_TYPE as limitCountType
    , mlc.LIMIT_COUNT as limitCount
    , mlc.LIMIT_COUNT_RESET_RANGE as limitCountResetRange
    , mlc.ACTION_RANGE as actionRange
    , mlc.NEXT_ACTION as nextAction
    , ml.LIMIT_SUB_NO as limitSubNo
    , ml.LIMIT_JUDGE_TYPE as limitJudgeType
    , ml.LIMIT_VALUE as limitValue
    , ifnull( 
        tlhb.LIMIT_HIT_DATETIME
        , str_to_date('19000101000000', '%%Y%%m%%d%%k%%i%%s')
    ) as limitHitDatetime 
from
    M_DATA_COLLECTION mdc 
    inner join M_LIMIT_CHECK mlc 
        on mdc.DATA_COLLECTION_SEQ = mlc.DATA_COLLECTION_SEQ 
    inner join M_LIMIT ml 
        on mdc.DATA_COLLECTION_SEQ = ml.DATA_COLLECTION_SEQ 
    left outer join T_LIMIT_HIT_BEFORE tlhb
        on mdc.DATA_COLLECTION_SEQ = tlhb.DATA_COLLECTION_SEQ 
where
    mdc.DELETE_FLG = 0
and mdc.DEVICE_ID = '%(deviceId)s'
and mdc.SENSOR_ID = '%(sensorId)s'
;