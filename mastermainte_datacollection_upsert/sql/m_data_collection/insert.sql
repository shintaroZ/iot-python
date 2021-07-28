insert
into `M_DATA_COLLECTION`(
    `DATA_COLLECTION_SEQ`
    , `DEVICE_ID`
    , `SENSOR_ID`
    , `DELETE_FLG`
    , `VERSION`
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
)
values (
    %(dataCollectionSeq)d
    , '%(deviceId)s'
    , '%(sensorId)s'
    , 0
    , %(version)d
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
);