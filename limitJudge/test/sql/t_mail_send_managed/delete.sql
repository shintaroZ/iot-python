/*
メール通知マスタ管理
データ定義マスタシーケンス:4, 1
受信日時:2021/08/05 00:00:00～2021/08/05 23:59:59
*/

delete
from
    T_MAIL_SEND_MANAGED
where
    DATA_COLLECTION_SEQ in (4, 1)
and DETECTION_DATETIME between '2021/08/05 00:00:00' and '2021/08/05 23:59:59'
;