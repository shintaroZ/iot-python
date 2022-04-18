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
    , `SENSOR_UNIT`
    , `STATUS_TRUE`
    , `STATUS_FALSE`
    , `COLLECTION_VALUE_TYPE`
    , `COLLECTION_TYPE`
    , `REVISION_MAGNIFICATION`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `UPDATED_USER`
)
values (
    101
    , '2'
    , 'UT_EQP001'
    , '101'
    , ''
    , '0'
    , 0
    , 'Mononeテストエッジ(xxx.xxx.xxx.xxx)'
    , ''
    , ''
    , ''
    , '0'
    , 1
    , null
    , '2021/07/28 16:40:25'
    , null
    , 'devUser'
)
ON DUPLICATE KEY UPDATE
    DATA_COLLECTION_SEQ = values(DATA_COLLECTION_SEQ)
    , DEVICE_ID = values(DEVICE_ID)
    , SENSOR_ID = values(SENSOR_ID)
    , VERSION = values(VERSION)

;