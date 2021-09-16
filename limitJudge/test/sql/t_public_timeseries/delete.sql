/*
時系列
データ定義マスタシーケンス:4, 1
受信日時:2021/08/05 00:00:00～2021/08/05 23:59:59
*/

delete from T_PUBLIC_TIMESERIES
where
    DATA_COLLECTION_SEQ in (4, 1)
and RECEIVED_DATETIME between '2021/08/05 00:00:00' and '2021/08/05 23:59:59'
;