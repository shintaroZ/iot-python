/*
テストデータ期間
データ定義マスタシーケンス:4, 1
受信日時:2021/08/05 00:00:00～2021/08/05 23:59:59
*/

insert
into `T_PUBLIC_TIMESERIES`(
    `DATA_COLLECTION_SEQ`
    , `RECEIVED_DATETIME`
    , `SENSOR_VALUE`
    , `CREATED_AT`
)
values (
    4
    , '2021/08/05 12:00:00.000'
    , -12.1
    , CURRENT_TIMESTAMP
),(
    4
    , '2021/08/05 12:00:10.000'
    , -12
    , CURRENT_TIMESTAMP
),(
    4
    , '2021/08/05 12:00:20.000'
    , -11.9
    , CURRENT_TIMESTAMP
),(
    4
    , '2021/08/05 12:00:30.000'
    , 6.9
    , CURRENT_TIMESTAMP
),(
    4
    , '2021/08/05 12:00:40.000'
    , 7
    , CURRENT_TIMESTAMP
),(
    4
    , '2021/08/05 12:00:50.000'
    , 7.1
    , CURRENT_TIMESTAMP
),(
    1
    , '2021/08/05 12:00:00.000'
    , 4.56
    , CURRENT_TIMESTAMP
),(
    1
    , '2021/08/05 12:00:10.000'
    , 4.57
    , CURRENT_TIMESTAMP
),(
    1
    , '2021/08/05 12:00:20.000'
    , 4.56
    , CURRENT_TIMESTAMP
),(
    1
    , '2021/08/05 12:00:30.000'
    , 4.57
    , CURRENT_TIMESTAMP
),(
    1
    , '2021/08/05 12:00:40.000'
    , 4.56
    , CURRENT_TIMESTAMP
),(
    1
    , '2021/08/05 12:00:50.000'
    , 4.57
    , CURRENT_TIMESTAMP
)
on duplicate key update
	SENSOR_VALUE = values(SENSOR_VALUE)
;
