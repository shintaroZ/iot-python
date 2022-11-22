select
    mdc.EDGE_TYPE as edgeType
    , mdc.DEVICE_ID as deviceId
    , mdc.SENSOR_ID as sensorId
    , mdc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , mdc.SENSOR_NAME as sensorName
    , mdc.COLLECTION_VALUE_TYPE as collectionValueType
    , mlf.SAVING_FLG as savingFlg
    , DATE_FORMAT(mdc.CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as createdAt
    , ifnull(DATE_FORMAT(mdc.UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s'), '') as updatedAt
    , ifnull(mdc.UPDATED_USER, '') as updatedUser
    , mdc.VERSION as version 
from
    M_DATA_COLLECTION mdc 
    inner join M_LINK_FLG mlf 
        on mlf.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ 
where
    mdc.DELETE_FLG = 0 
    and mdc.EDGE_TYPE = 3
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
    , mdc.SENSOR_ID;
