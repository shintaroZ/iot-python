insert
into `M_DATA_COLLECTION`(
    `DATA_COLLECTION_SEQ`
    , `EDGE_TYPE`
    , `EQUIPMENT_ID`
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
    , `CREATED_AT`
    , `UPDATED_USER`
)
values (
    %(dataCollectionSeq)d
    , %(edgeType)d
    , '%(equipmentId)s'
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
    , '%(createdAt)s'
    , '%(updatedUser)s'
);