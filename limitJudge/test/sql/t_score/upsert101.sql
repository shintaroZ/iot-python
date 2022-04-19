/*
テストデータ期間
データ定義マスタシーケンス:4, 1
受信日時:2021/08/05 00:00:00～2021/08/05 23:59:59
*/

insert 
into ins001.`T_SCORE`( 
    `DATA_COLLECTION_SEQ`
    , `DETECTION_DATETIME`
    , `DETECTION_DATE`
    , `DETECTION_TIME`
    , `DETECTION_FLAG`
    , `DETECTION_MIN`
    , `DETECTION_MAX`
    , `DETECTION_VALUE`
    , `DETECTION_THRESHOLD`
    , `SLIDING_UPPER`
    , `SLIDING_LOWER`
    , `CREATED_AT`
) 
values ( 
    7
    , '2021/08/05 00:00:00'
    , '2021/08/05'
    , '00:00:00'
    , '0'
    , - 1254.8
    , - 226.06
    , - 1245.89
    , 1000
    , 1000
    , - 900
    , '2021/05/19 21:02:04'
),( 
    7
    , '2021/08/05 00:10:00'
    , '2021/08/05'
    , '00:10:00'
    , '1'
    , - 1254.8
    , - 226.06
    , - 1245.89
    , 1000
    , 1000
    , - 900
    , '2021/05/19 21:02:04'
),( 
    7
    , '2021/08/05 00:20:00'
    , '2021/08/05'
    , '00:20:00'
    , '0'
    , - 1254.8
    , - 226.06
    , - 1245.89
    , 1000
    , 1000
    , - 900
    , '2021/05/19 21:02:04'
)

on duplicate key update
	DETECTION_FLAG = values(DETECTION_FLAG)
;
