import unittest
from unittest import mock
import datetime
from datetime import datetime as dt
import json
import pprint
import lambda_function
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー

def createEvent(query_file_path):
    f = open(query_file_path, 'r')
    return json.load(f)

class LambdaFunctionTest(unittest.TestCase):

    RDS = None
    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(cls):
        global RDS
        lambda_function.initConfig("eg-iot-develop")
        lambda_function.setLogger(initCommon.getLogger(lambda_function.LOG_LEVEL))
        # lambda_function.setLogger(initCommon.getLogger("DEBUG"))
        RDS = rdsCommon.rdsCommon(lambda_function.LOGGER
                                , lambda_function.DB_HOST
                                , lambda_function.DB_PORT
                                , lambda_function.DB_USER
                                , lambda_function.DB_PASSWORD
                                , lambda_function.DB_NAME
                                , lambda_function.DB_CONNECT_TIMEOUT)
        lambda_function.setRds(RDS)
        
    # ----------------------------------------------------------------------
    # getMasterRetentionReriods()の正常系テスト
    # レコード情報が返却されること。
    # ----------------------------------------------------------------------
    def test_getMasterRetentionReriods_001(self):
        print("===== %s start =====" % "test_getMasterRetentionReriods_001")
        
        # 保持期間マスタの初期値セット
        RDS.execute(initCommon.getQuery("test/sql/m_retention_periods/upsert001.sql"))
        RDS.commit()

        result = lambda_function.getMasterRetentionReriods()
        for r in result:
            print (r)
    # ----------------------------------------------------------------------
    # getMaintenancePartitions()の正常系テスト
    # レコード情報が返却されること。
    # ----------------------------------------------------------------------
    def test_getMaintenancePartitions_001(self):
        print("===== %s start =====" % "test_getMaintenancePartitions_001")
        # 公開DBの初期値セット
        RDS.execute(initCommon.getQuery("test/sql/t_tmp/delete.sql"))
        RDS.execute(initCommon.getQuery("test/sql/t_tmp/insert001.sql")
                    , {"receivedDatetime" : "2020/03/01 00:00:00"})
        RDS.commit

        result = lambda_function.getMaintenancePartitions("TMP_TEMPERATURE","RECEIVED_DATETIME")
        self.assertEqual(result[0]["partitionKey"], "p20200229")
        self.assertEqual(result[0]["partitionDate"].strftime('%Y/%m/%d %H:%M:%S'),  "2020/03/01 00:00:00")

    # ----------------------------------------------------------------------
    # convertPartitionList()の正常系テスト
    # partitionKeyのListが返却されること。
    # ----------------------------------------------------------------------
    def test_convertPartitionList_001(self):
        print("===== %s start =====" % "test_convertPartitionList_001")

        para =[]
        para.append({"partitionKey" : "p20210229"})
        para.append({"partitionKey" : "p20210301"})
        para.append({"partitionKeyxxxxx" : "p20210302"})
        result = lambda_function.convertPartitionList(para);
        self.assertTrue("p20210229" in result)
        self.assertTrue("p20210301" in result)
        self.assertFalse("p20210302" in result)

    # ----------------------------------------------------------------------
    # getFuturePartitions()の正常系テスト
    # 未来日のパーティションが返却されること。
    # ----------------------------------------------------------------------
    def test_getFuturePartitions_001(self):
        print("===== %s start =====" % "test_getFuturePartitions_001")

        # 現在時刻
        nowdateTime = initCommon.getSysDateJst()
        nowdate = datetime.datetime(nowdateTime.year, nowdateTime.month, nowdateTime.day, 0, 0, 0)
        para = 3
        result = lambda_function.getFuturePartitions(para);
        self.assertEqual(result[0]["partitionKey"], (nowdate + datetime.timedelta(days=-1)).strftime("p%Y%m%d") )
        self.assertEqual(result[0]["partitionDate"], nowdate)
        self.assertEqual(result[1]["partitionKey"], (nowdate + datetime.timedelta(days=0)).strftime("p%Y%m%d") )
        self.assertEqual(result[1]["partitionDate"],(nowdate + datetime.timedelta(days=1)))
        self.assertEqual(result[2]["partitionKey"], (nowdate + datetime.timedelta(days=1)).strftime("p%Y%m%d") )
        self.assertEqual(result[2]["partitionDate"],(nowdate + datetime.timedelta(days=2)))
        self.assertEqual(result[3]["partitionKey"], (nowdate + datetime.timedelta(days=2)).strftime("p%Y%m%d") )
        self.assertEqual(result[3]["partitionDate"],(nowdate + datetime.timedelta(days=3)))

    # ----------------------------------------------------------------------
    # createSqlPartiton()の正常系テスト
    # ALTER TABLE パーティション句の構文が返却されること。
    # ----------------------------------------------------------------------
    def test_createSqlPartiton_001(self):
        print("===== %s start =====" % "test_createSqlPartiton_001")
        para = []
        para.append({ "partitionKey" : "p20210228"
                    , "partitionDate" : dt.strptime("2021/03/01", "%Y/%m/%d")})
        para.append({ "partitionKey" : "p20210301"
                    , "partitionDate" : dt.strptime("2021/03/02", "%Y/%m/%d")})

        result = lambda_function.createSqlPartiton(para);
        self.assertEqual(result, "PARTITION p20210228 VALUES LESS THAN (TO_DAYS('2021/03/01 00:00:00')),\r\n" \
                                  "PARTITION p20210301 VALUES LESS THAN (TO_DAYS('2021/03/02 00:00:00'))" )

    # ----------------------------------------------------------------------
    # createNewPartition()の正常系テスト
    # パーティションが新規作成されること。
    # ----------------------------------------------------------------------
    def test_createNewPartition_001(self):
        print("===== %s start =====" % "test_createNewPartition_001")
        # パーティション解除
        try:
            RDS.execute(initCommon.getQuery("test/sql/t_tmp/removePartition.sql"))
            RDS.commit()
        except Exception  as e:
            print("partition解除済みのため、そのまま継続")

        paramTableName = "TMP_TEMPERATURE"
        paramPartitionColumnName = "RECEIVED_DATETIME"
        paramPartitioStr = "PARTITION p20210228 VALUES LESS THAN (TO_DAYS('2021-03/01 00:00:00'))" \
                           ",PARTITION p20210301 VALUES LESS THAN (TO_DAYS('2021-03/02 00:00:00'))" \
                           ",PARTITION p20210302 VALUES LESS THAN (TO_DAYS('2021-03/03 00:00:00'))"
        lambda_function.createNewPartition(paramTableName, paramPartitionColumnName, paramPartitioStr);

        # パーティション確認
        pResult = RDS.fetchall(initCommon.getQuery("test/sql/t_tmp/selectPartition.sql"))
        self.assertTrue(pResult[0]["partitionName"] is not None)


    # ----------------------------------------------------------------------
    # createNewPartition()の異常系テスト
    # パーティション作成失敗時、エラーログが出力されること。
    # ----------------------------------------------------------------------
    def test_createNewPartition_002(self):
        print("===== %s start =====" % "test_createNewPartition_002")
        paramTableName = "TMP_TEMPERATURExxx"
        paramPartitionColumnName = "RECEIVED_DATETIME"
        paramPartitioStr = "PARTITION p20210228 VALUES LESS THAN (TO_DAYS('2021-03/01 00:00:00'))"

        with self.assertRaises(Exception):
            print("test_createNewPartition_002:Exception is throw")
            lambda_function.createNewPartition(paramTableName, paramPartitionColumnName, paramPartitioStr);

    # ----------------------------------------------------------------------
    # addPartition()の正常系テスト
    # パーティションが追加されること。
    # ----------------------------------------------------------------------
    def test_addPartition_001(self):
        print("===== %s start =====" % "test_addPartition_001")
        # 予めパーティション作成
        LambdaFunctionTest.test_createNewPartition_001(self)

        paramTableName = "TMP_TEMPERATURE"
        paramPartitioStr = "PARTITION p20210303 VALUES LESS THAN (TO_DAYS('2021-03/04 00:00:00'))"
        lambda_function.addPartition(paramTableName, paramPartitioStr);

        # パーティション確認
        pResult = RDS.fetchall(initCommon.getQuery("test/sql/t_tmp/selectPartition.sql"))
        self.assertTrue(pResult[0]["partitionName"] is not None)

    # ----------------------------------------------------------------------
    # addPartition()の正常系テスト
    # パーティション作成失敗時、エラーログが出力されること。
    # ----------------------------------------------------------------------
    def test_addPartition_002(self):
        print("===== %s start =====" % "test_addPartition_002")
        # パーティション解除
        try:
            RDS.execute(initCommon.getQuery("test/sql/t_tmp/removePartition.sql"))
            RDS.commit()
        except Exception  as e:
            print("partition解除済みのため、そのまま継続")

        paramTableName = "TMP_TEMPERATURE"
        paramPartitioStr = "PARTITION p20210303 VALUES LESS THAN (TO_DAYS('2021-03/04 00:00:00'))"

        with self.assertRaises(Exception):
            print("test_addPartition_002:Exception is throw")
            lambda_function.addPartition(paramTableName, paramPartitioStr);

    # ----------------------------------------------------------------------
    # dropPartition()の正常系テスト
    # パーティションが削除されパーティション内のレコードが削除されること。
    # ----------------------------------------------------------------------
    def test_dropPartition_001(self):
        print("===== %s start =====" % "test_dropPartition_001")
        # 公開DBの初期値セット
        RDS.execute(initCommon.getQuery("test/sql/t_tmp/delete.sql"))
        RDS.execute(initCommon.getQuery("test/sql/t_tmp/insert001.sql"), {"receivedDatetime" : "2021/02/28 00:00:00"})
        RDS.execute(initCommon.getQuery("test/sql/t_tmp/insert001.sql"), {"receivedDatetime" : "2021/02/28 23:59:59"})
        RDS.execute(initCommon.getQuery("test/sql/t_tmp/insert001.sql"), {"receivedDatetime" : "2021/03/01 00:00:00"})
        RDS.execute(initCommon.getQuery("test/sql/t_tmp/insert001.sql"), {"receivedDatetime" : "2021/03/01 23:59:59"})
        RDS.execute(initCommon.getQuery("test/sql/t_tmp/insert001.sql"), {"receivedDatetime" : "2021/03/02 00:00:00"})
        RDS.execute(initCommon.getQuery("test/sql/t_tmp/insert001.sql"), {"receivedDatetime" : "2021/03/02 23:59:59"})
        RDS.commit()
        
        # 予めパーティション作成
        LambdaFunctionTest.test_createNewPartition_001(self)

        paramTableName = "TMP_TEMPERATURE"
        paramDropPartitionStr = "p20210228,p20210301"
        lambda_function.dropPartition(paramTableName, paramDropPartitionStr);

        # レコード確認
        result = RDS.fetchall(initCommon.getQuery("test/sql/t_tmp/select.sql"))
        self.assertEqual(result[0]["receivedDateTime"].strftime('%Y/%m/%d %H:%M:%S'),  "2021/03/02 00:00:00")
        self.assertEqual(result[1]["receivedDateTime"].strftime('%Y/%m/%d %H:%M:%S'),  "2021/03/02 23:59:59")

    # ----------------------------------------------------------------------
    # dropPartition()の異常系テスト
    # パーティション削除失敗時、エラーログが出力されること。
    # ----------------------------------------------------------------------
    def test_dropPartition_002(self):
        print("===== %s start =====" % "test_dropPartition_002")

        # パーティション解除
        try:
            RDS.execute(initCommon.getQuery("test/sql/t_tmp/removePartition.sql"))
            RDS.commit()
        except Exception  as e:
            print("partition解除済みのため、そのまま継続")

        paramTableName = "TMP_TEMPERATURE"
        paramPartitioStr = "PARTITION p20210303 VALUES LESS THAN (TO_DAYS('2021-03/04 00:00:00'))"

        with self.assertRaises(Exception):
            print("test_dropPartition_002:Exception is throw")
            lambda_function.dropPartition(paramTableName, paramDropPartitionStr);

    # ----------------------------------------------------------------------
    # getMargeTable()の正常系テスト
    # partitionKeyの重複を除いてテーブル結合した結果が返却されること。
    # ----------------------------------------------------------------------
    def test_getMargeTable_001(self):
        print("===== %s start =====" % "test_getMargeTable_001")

        baseTable = []
        baseTable.append({"partitionKey" : "p20210227", "partitionDate" : "2021/02/28 00:00:00"})
        baseTable.append({"partitionKey" : "p20210228", "partitionDate" : "2021/03/01 00:00:00"})
        baseTable.append({"partitionKey" : "p20210301", "partitionDate" : "2021/03/02 00:00:00"})

        addTable = []
        addTable.append({"partitionKey" : "p20210301", "partitionDate" : "2021/03/02 00:00:00"})
        addTable.append({"partitionKey" : "p20210302", "partitionDate" : "2021/03/03 00:00:00"})

        result = lambda_function.getMargeTable(baseTable, addTable);
        self.assertEqual(result[0]["partitionKey"], "p20210227")
        self.assertEqual(result[1]["partitionKey"], "p20210228")
        self.assertEqual(result[2]["partitionKey"], "p20210301")
        self.assertEqual(result[3]["partitionKey"], "p20210302")

    # ----------------------------------------------------------------------
    # getDiffTable()の正常系テスト
    # partitionListの最大値より大きい差分のテーブルが返却されること。
    # ----------------------------------------------------------------------
    def test_getDiffTable_001(self):
        print("===== %s start =====" % "test_getDiffTable_001")

        baseTable = []
        baseTable.append({"partitionKey" : "p20210227", "partitionDate" : "2021/02/28 00:00:00"})
        baseTable.append({"partitionKey" : "p20210228", "partitionDate" : "2021/03/01 00:00:00"})
        baseTable.append({"partitionKey" : "p20210301", "partitionDate" : "2021/03/02 00:00:00"})
        baseTable.append({"partitionKey" : "p20210302", "partitionDate" : "2021/03/03 00:00:00"})

        partitionList = ["p20210227", "p20210301"]
        result = lambda_function.getDiffTable(baseTable, partitionList);
        self.assertEqual(result[0]["partitionKey"], "p20210302")


    # ----------------------------------------------------------------------
    # getDropPartition()の正常系テスト
    # partitionListの最大値より大きい差分のテーブルが返却されること。
    # ----------------------------------------------------------------------
    def test_getDropPartition_001(self):
        print("===== %s start =====" % "test_getDropPartition_001")

        baseTable = []
        baseTable.append({"partitionKey" : "p20210227", "partitionDate" : "2021/02/28 00:00:00"})
        baseTable.append({"partitionKey" : "p20210228", "partitionDate" : "2021/03/01 00:00:00"})
        baseTable.append({"partitionKey" : "p20210301", "partitionDate" : "2021/03/02 00:00:00"})
        baseTable.append({"partitionKey" : "p20210302", "partitionDate" : "2021/03/03 00:00:00"})

        partitionList = ["p20210303"]

        retentionRange = 5

        # mock使用:システム時刻を3/6とする。
        with mock.patch("initCommon.getSysDateJst") as sysDate_mock:
            sysDate_mock.return_value = dt.strptime("2021/03/06", "%Y/%m/%d")
            result = lambda_function.getDropPartition(baseTable, partitionList, retentionRange);

        self.assertEqual(result, "p20210227,p20210228")