insert 
into `M_DATA_COLLECTION`( 
    `DEVICE_ID`
    , `SENSOR_ID`
    , `DELETE_COUNT`
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
    , 0
    , %(dataCollectionSeq)d
    , '%(sensorName)s'
    , %(sensorUnit)s
    , %(statusTrue)s
    , %(statusFalse)s
    , %(collectionValueType)d
    , %(collectionType)d
    , %(revisionMagnification)s
    , %(xCoordinate)s
    , %(yCoordinate)s
    , '%(createdAt)s'
    , '%(updatedUser)s'
    , %(version)d
)
ON DUPLICATE KEY UPDATE
      DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
    , SENSOR_NAME = values(SENSOR_NAME)
    , SENSOR_UNIT = values(SENSOR_UNIT)
    , STATUS_TRUE = values(STATUS_TRUE)
    , STATUS_FALSE = values(STATUS_FALSE)
    , COLLECTION_VALUE_TYPE = values(COLLECTION_VALUE_TYPE)
    , COLLECTION_TYPE = values(COLLECTION_TYPE)
    , REVISION_MAGNIFICATION = values(REVISION_MAGNIFICATION)
    , X_COORDINATE = values(X_COORDINATE)
    , Y_COORDINATE = values(Y_COORDINATE)
    , UPDATED_AT = '%(updatedAt)s'
    , UPDATED_USER = values(UPDATED_USER)
    , VERSION = values(VERSION)
;