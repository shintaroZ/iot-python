select
    mdc.EDGE_TYPE as edgeType
    , mdc.EQUIPMENT_ID as equipmentId
    , mdc.DEVICE_ID as deviceId
    , mdc.SENSOR_ID as sensorId
    , mdc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , mdc.SENSOR_NAME as sensorName
    , ifnull(mdc.SENSOR_UNIT, '') as sensorUnit
    , ifnull(mdc.STATUS_TRUE, '') as statusTrue
    , ifnull(mdc.STATUS_FALSE, '') as statusFalse
    , mdc.COLLECTION_VALUE_TYPE as collectionValueType
    , mdc.COLLECTION_TYPE as collectionType
    , mct.COLLECTION_TYPE_NAME as collectionTypeName
    , mdc.REVISION_MAGNIFICATION as revisionMagnification
    , mlf.SAVING_FLG as savingFlg
    , mlf.LIMIT_CHECK_FLG as limitCheckFlg
    , mlc.LIMIT_COUNT_TYPE as limitCountType
    , mlc.LIMIT_COUNT as limitCount
    , mlc.LIMIT_COUNT_RESET_RANGE as limitCountResetRange
    , mlc.ACTION_RANGE as actionRange
    , mlc.NEXT_ACTION as nextAction
    , ml.LIMIT_SUB_NO as limitSubNo
    , ml.LIMIT_JUDGE_TYPE as limitJudgeType
    , ml.LIMIT_VALUE as limitValue
    , DATE_FORMAT(mdc.CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as createdAt
    , ifnull( 
        DATE_FORMAT(mdc.UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s')
        , ''
    ) as updatedAt
    , ifnull(mdc.UPDATED_USER, '') as updatedUser
    , mdc.VERSION as version 
from
    M_DATA_COLLECTION mdc 
    inner join M_LINK_FLG mlf 
        on mlf.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ 
    left outer join M_LIMIT_CHECK mlc 
        on mlc.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ 
    left outer join M_LIMIT ml 
        on ml.DATA_COLLECTION_SEQ = mlc.DATA_COLLECTION_SEQ 
    inner join M_COLLECTION_TYPE mct 
        on mct.EDGE_TYPE = mdc.EDGE_TYPE 
        and mct.COLLECTION_TYPE = mdc.COLLECTION_TYPE 
where
    mdc.DELETE_FLG = 0 
    and mdc.EDGE_TYPE in (1,2)
    and not exists ( 
        select
            1 
        from
            M_DATA_COLLECTION mdcSub 
        where
            mdc.DEVICE_ID = mdcSub.DEVICE_ID 
            and mdc.SENSOR_ID = mdcSub.SENSOR_ID 
            and mdc.VERSION < mdcSub.VERSION
    ) 
    %(p_whereParams)s
order by
    mdc.DEVICE_ID
    , mdc.SENSOR_ID
    , ml.LIMIT_SUB_NO;
