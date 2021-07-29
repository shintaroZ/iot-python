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
    , `UPDATED_AT`
    , `UPDATED_USER`
)
values (
    1
    , '700400015-66DEF1DE'
    , 's002'
    , '0'
    , 0
    , '湿度testSensor'
    , '%%'
    , ''
    , ''
    , '0'
    , 2
    , 0.01
    , 1234.56
    , 2345.67
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