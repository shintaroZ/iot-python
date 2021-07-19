select
    mdc.DATA_COLLECTION_SEQ as dataCollectionSeq
    ,mdc.SENSOR_NAME as sensorName
    ,mdc.SENSOR_UNIT as sensorUnit
    ,mdc.STATUS_TRUE as statusTrue
    ,mdc.STATUS_FALSE as statusFalse
    ,mdc.COLLECTION_VALUE_TYPE as collectionValueType
    ,mdc.REVISION_MAGNIFICATION as revisionMagification
    ,mlf.SAVING_FLG  as savingFlg
    ,mlf.LIMIT_CHECK_FLG as limitCheckFlg
from
    M_DATA_COLLECTION mdc inner join M_LINK_FLG mlf
    on mdc.DATA_COLLECTION_SEQ  = mlf.DATA_COLLECTION_SEQ
where
    mdc.DEVICE_ID = '%(p_deviceId)s'
and mdc.SENSOR_ID = '%(p_sensorId)s'
and mdc.DELETE_COUNT = 0
;
