select
    me.EQUIPMENT_ID as equipmentId
    , me.EQUIPMENT_NAME as equipmentName
    , me.X_COORDINATE as xCoordinate
    , me.Y_COORDINATE as yCoordinate
    , mdc.DEVICE_ID as deviceId
    , mdc.SENSOR_ID as sensorId
    , mdc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , mdc.SENSOR_NAME as sensorName
    , ifnull(mdc.SENSOR_UNIT, '') as sensorUnit
    , ifnull(mdc.STATUS_TRUE, '') as statusTrue
    , ifnull(mdc.STATUS_FALSE, '') as statusFalse
    , mdc.COLLECTION_VALUE_TYPE as collectionValueType
    , mdc.COLLECTION_TYPE as collectionType
    , mdc.REVISION_MAGNIFICATION as revisionMagnification
    , mlf.SAVING_FLG as savingFlg
    , mlf.LIMIT_CHECK_FLG as limitCheckFlg
    , DATE_FORMAT(me.CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as createdAt
    , ifnull( DATE_FORMAT(me.UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s'), '') as updatedAt
    , ifnull(me.UPDATED_USER, '') as updatedUser
    , me.VERSION as version 
from
    M_EQUIPMENT me 
    left outer join (
            select
                m.EQUIPMENT_ID
                , m.DEVICE_ID
                , m.SENSOR_ID
                , m.DATA_COLLECTION_SEQ
                , m.SENSOR_NAME
                , m.SENSOR_UNIT
                , m.STATUS_TRUE
                , m.STATUS_FALSE
                , m.COLLECTION_VALUE_TYPE
                , m.COLLECTION_TYPE
                , m.REVISION_MAGNIFICATION
                , m.DELETE_FLG
                , m.VERSION 
            from
                M_DATA_COLLECTION m 
            where
                m.DELETE_FLG = 0 
                and not exists ( 
                    select
                        1 
                    from
                        M_DATA_COLLECTION mdcSub 
                    where
                        m.DEVICE_ID = mdcSub.DEVICE_ID 
                        and m.SENSOR_ID = mdcSub.SENSOR_ID 
                        and m.VERSION < mdcSub.VERSION
                )
        ) mdc 
        on me.EQUIPMENT_ID = mdc.EQUIPMENT_ID
    left outer join M_LINK_FLG mlf 
        on mlf.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ 
where
    me.DELETE_FLG = 0 
    and not exists ( 
        select
            1 
        from
            M_EQUIPMENT meSub 
        where
            me.EQUIPMENT_ID = meSub.EQUIPMENT_ID 
            and me.VERSION < meSub.VERSION
    ) 
	%(p_whereParams)s
	
order by
    me.EQUIPMENT_ID
;

