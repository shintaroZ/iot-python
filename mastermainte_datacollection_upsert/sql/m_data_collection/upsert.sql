insert 
into `M_DATA_COLLECTION`( 
    `DEVICE_ID`
    , `SENSOR_ID`
    , `DATA_COLLECTION_SEQ`
    , `SENSOR_NAME`
    , `SENSOR_UNIT`
    , `STATUS_TRUE`
    , `STATUS_FALSE`
    , `COLLECTION_VALUE_TYPE`
    , `COLLECTION_TYPE`
    , `REVISION_MAGNIFICATION`
    , `X_COORDINATE`
    , `Y_COORDINATE`
    , `CREATED_AT`
    , `UPDATED_USER`
    , `VERSION`
) 
values ( 
      '%(deviceId)s'
    , '%(sensorId)s'
    , %(dataCollectionSeq)d
    , '%(sensorName)s'
    , '%(sensorUnit)s'
    , '%(statusTrue)s'
    , '%(statusFalse)s'
    , %(collectionValueType)d
    , %(collectionType)d
    , %(revisionMagnification)f
    , %(xCoordinate)f
    , %(yCoordinate)f
    , '%(createdAt)s'
    , '%(updatedUser)s'
    , %(version)d
)
ON DUPLICATE KEY UPDATE
     DATA_COLLECTION_SEQ = if(DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ), values(DATA_COLLECTION_SEQ), DATA_COLLECTION_SEQ)
    ,SENSOR_NAME = if(SENSOR_NAME = values(SENSOR_NAME), values(SENSOR_NAME), SENSOR_NAME)
    ,SENSOR_UNIT = if(SENSOR_UNIT = values(SENSOR_UNIT), values(SENSOR_UNIT), SENSOR_UNIT)
    ,STATUS_TRUE = if(STATUS_TRUE = values(STATUS_TRUE), values(STATUS_TRUE), STATUS_TRUE)
    ,STATUS_FALSE = if(STATUS_FALSE = values(STATUS_FALSE), values(STATUS_FALSE), STATUS_FALSE)
    ,COLLECTION_VALUE_TYPE = if(COLLECTION_VALUE_TYPE = values(COLLECTION_VALUE_TYPE), values(COLLECTION_VALUE_TYPE), COLLECTION_VALUE_TYPE)
    ,COLLECTION_TYPE = if(COLLECTION_TYPE = values(COLLECTION_TYPE), values(COLLECTION_TYPE), COLLECTION_TYPE)
    ,REVISION_MAGNIFICATION = if(REVISION_MAGNIFICATION = values(REVISION_MAGNIFICATION), values(REVISION_MAGNIFICATION), REVISION_MAGNIFICATION)
    ,X_COORDINATE = if(X_COORDINATE = values(X_COORDINATE), values(X_COORDINATE), X_COORDINATE)
    ,Y_COORDINATE = if(Y_COORDINATE = values(Y_COORDINATE), values(Y_COORDINATE), Y_COORDINATE)
    ,UPDATED_AT = 
        case
            when DATA_COLLECTION_SEQ <> values(DATA_COLLECTION_SEQ) 
             and SENSOR_NAME <> values(SENSOR_NAME)
             and SENSOR_UNIT <> values(SENSOR_UNIT)
             and STATUS_TRUE <> values(STATUS_TRUE)
             and STATUS_FALSE <> values(STATUS_FALSE)
             and COLLECTION_VALUE_TYPE <> values(COLLECTION_VALUE_TYPE)
             and COLLECTION_TYPE <> values(COLLECTION_TYPE)
             and REVISION_MAGNIFICATION <> values(REVISION_MAGNIFICATION)
             and X_COORDINATE <> values(X_COORDINATE)
             and Y_COORDINATE <> values(Y_COORDINATE)
            then '%(updatedAt)s'
            else UPDATED_AT
        end
    ,UPDATED_USER = 
        case
            when DATA_COLLECTION_SEQ <> values(DATA_COLLECTION_SEQ) 
             and SENSOR_NAME <> values(SENSOR_NAME)
             and SENSOR_UNIT <> values(SENSOR_UNIT)
             and STATUS_TRUE <> values(STATUS_TRUE)
             and STATUS_FALSE <> values(STATUS_FALSE)
             and COLLECTION_VALUE_TYPE <> values(COLLECTION_VALUE_TYPE)
             and COLLECTION_TYPE <> values(COLLECTION_TYPE)
             and REVISION_MAGNIFICATION <> values(REVISION_MAGNIFICATION)
             and X_COORDINATE <> values(X_COORDINATE)
             and Y_COORDINATE <> values(Y_COORDINATE)
            then values(UPDATED_USER)
            else UPDATED_USER
        end
    ,VERSION = 
        case
            when DATA_COLLECTION_SEQ <> values(DATA_COLLECTION_SEQ) 
             and SENSOR_NAME <> values(SENSOR_NAME)
             and SENSOR_UNIT <> values(SENSOR_UNIT)
             and STATUS_TRUE <> values(STATUS_TRUE)
             and STATUS_FALSE <> values(STATUS_FALSE)
             and COLLECTION_VALUE_TYPE <> values(COLLECTION_VALUE_TYPE)
             and COLLECTION_TYPE <> values(COLLECTION_TYPE)
             and REVISION_MAGNIFICATION <> values(REVISION_MAGNIFICATION)
             and X_COORDINATE <> values(X_COORDINATE)
             and Y_COORDINATE <> values(Y_COORDINATE)
            then values(VERSION)
            else VERSION
        end
;