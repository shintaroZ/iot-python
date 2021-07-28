insert
into `M_DATA_COLLECTION`(
    `DATA_COLLECTION_SEQ`
    , `DEVICE_ID`
    , `SENSOR_ID`
    , `DELETE_FLG`
    , `VERSION`
    , `SENSOR_NAME`
    %(insert_sensorUnit)s
    %(insert_statusTrue)s
    %(insert_statusFalse)s
    , `COLLECTION_VALUE_TYPE`
    , `COLLECTION_TYPE`
    %(insert_revisionMagnification)s
    %(insert_xCoordinate)s
    %(insert_yCoordinate)s
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
    %(values_sensorUnit)s
    %(values_statusTrue)s
    %(values_statusFalse)s
    , %(collectionValueType)d
    , %(collectionType)d
    %(values_revisionMagnification)s
    %(values_xCoordinate)s
    %(values_yCoordinate)s
    , '%(createdAt)s'
    , '%(updatedUser)s'
);