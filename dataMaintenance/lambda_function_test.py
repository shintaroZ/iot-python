import unittest
from unittest import mock
import datetime
from datetime import datetime as dt
import json
import pprint
import lambda_function
import pymysql

def createEvent(query_file_path):
    f = open(query_file_path, 'r')
    return json.load(f)

# トランザクションなし
def dbExecute(paramFileName, paramMap = {}):

    result = None
    try:
        con = pymysql.connect(host=lambda_function.DB_HOST,
                              port=lambda_function.DB_PORT,
                              user=lambda_function.DB_USER,
                              passwd=lambda_function.DB_PASSWORD,
                              db=lambda_function.DB_NAME,
                              connect_timeout=lambda_function.DB_CONNECT_TIMEOUT)

        query = lambda_function.get_query(paramFileName)
        with con.cursor(pymysql.cursors.DictCursor) as cur:
            if len(paramMap) == 0:
                cur.execute(query)
            else:
                cur.execute(query % paramMap)
            result = cur.fetchall()
        con.commit()
    except Exception  as e:
        print('dbExecute failed : %s' % e)
        raise(e)
    return result
class LambdaFunctionTest(unittest.TestCase):

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(cls):
        lambda_function.init("config/setting01.ini")

#     # テスト終了時に1回だけ呼び出される
#     # クラスメソッドとして定義する
#     @classmethod
#     def tearDownClass(cls):
#     def setUp(self):
#     def tearDown(self):

    # ----------------------------------------------------------------------
    # init()の正常系テスト
    # 設定ファイルパスが有効な場合、設定値がグローバル変数に定義されること。
    # ----------------------------------------------------------------------
    def test_init_001(self):
        event = createEvent('test/function/input001.json')
        lambda_function.init(event["setting"])
        self.assertEqual(lambda_function.LOG_LEVEL, "INFO")
        self.assertEqual(lambda_function.DB_HOST, "mysql.cpp9recuwclr.ap-northeast-1.rds.amazonaws.com")
        self.assertEqual(lambda_function.DB_PORT, 3306)
        self.assertEqual(lambda_function.DB_USER, "admin")
        self.assertEqual(lambda_function.DB_PASSWORD, "Yz8aBhFr")
        self.assertEqual(lambda_function.DB_NAME, "ins001")
        self.assertEqual(lambda_function.DB_CONNECT_TIMEOUT, 3)
        self.assertEqual(lambda_function.PARTITION_FUTURE_RANGE, 5)


    # ----------------------------------------------------------------------
    # init()の異常系テスト
    # 設定ファイルパスが無効な場合、Exceptionがスローされること。
    # ----------------------------------------------------------------------
    def test_init_002(self):
        event = createEvent('test/function/input002.json')
        with self.assertRaises(Exception):
            print("test_init_002:Exception is throw")
            lambda_function.init(event["setting"])

    # ----------------------------------------------------------------------
    # customTime()の正常系テスト
    # 現在日時のtime.struct_time型が返却されること。
    # ----------------------------------------------------------------------
    def test_customTime_001(self):
        lambda_function.initConfig("config/setting01.ini")
        result = lambda_function.customTime()
        dtNow = datetime.datetime.now()
        self.assertEqual(result.tm_year, int(dtNow.strftime('%Y')))
        self.assertEqual(result.tm_mon, int(dtNow.strftime('%m')))
        self.assertEqual(result.tm_mday, int(dtNow.strftime('%d')))
        self.assertEqual(result.tm_hour, int(dtNow.strftime('%H')))
        self.assertEqual(result.tm_min, int(dtNow.strftime('%M')))
        self.assertEqual(result.tm_sec, int(dtNow.strftime('%S')))

    # ----------------------------------------------------------------------
    # getSysDateJst()の正常系テスト
    # 現在日時のdatetime型が返却されること。
    # ----------------------------------------------------------------------
    def test_getSysDateJst_001(self):
        lambda_function.initConfig("config/setting01.ini")
        result = lambda_function.getSysDateJst()
        dtNow = datetime.datetime.now()
        self.assertEqual(int(result.strftime('%Y')), int(dtNow.strftime('%Y')))
        self.assertEqual(int(result.strftime('%m')), int(dtNow.strftime('%m')))
        self.assertEqual(int(result.strftime('%d')), int(dtNow.strftime('%d')))
        self.assertEqual(int(result.strftime('%H')), int(dtNow.strftime('%H')))
        self.assertEqual(int(result.strftime('%M')), int(dtNow.strftime('%M')))
        self.assertEqual(int(result.strftime('%S')), int(dtNow.strftime('%S')))

    # ----------------------------------------------------------------------
    # initLogger()の正常系テスト
    # ロガーの設定が行われ正常終了すること。
    # ----------------------------------------------------------------------
    def test_initLogger_001(self):
        lambda_function.initConfig("config/setting01.ini")
        lambda_function.initLogger()

    # ----------------------------------------------------------------------
    # initRds()の正常系テスト
    # 接続先が正常の場合、コネクションが確立して正常終了すること。
    # ----------------------------------------------------------------------
    def test_initRds_001(self):
        lambda_function.initConfig("config/setting01.ini")
        lambda_function.initRds()

    # ----------------------------------------------------------------------
    # initRds()の異常系テスト
    # 接続先が不正の場合、タイムアウトしてExceptionがスローされること。
    # ----------------------------------------------------------------------
    def test_initRds_002(self):
        lambda_function.initConfig("config/setting01.ini")
        # DBポートを不正値に変更
        lambda_function.setDbPort(9999)
        with self.assertRaises(Exception):
            print("test_initRds_002:Exception is throw")
            lambda_function.initRds()

    # ----------------------------------------------------------------------
    # get_query()の正常系テスト
    # 指定したファイルの内容が返却されること。
    # ----------------------------------------------------------------------
    def test_get_query_001(self):
        lambda_function.initConfig("config/setting01.ini")
        result = lambda_function.get_query("test/function/input002.json")
        self.assertIsNotNone(result)

    # ----------------------------------------------------------------------
    # getMasterRetentionReriods()の正常系テスト
    # レコード情報が返却されること。
    # ----------------------------------------------------------------------
    def test_getMasterRetentionReriods_001(self):
        # 保持期間マスタの初期値セット
        dbExecute("test/sql/m_retention_periods/upsert001.sql")

        result = lambda_function.getMasterRetentionReriods()
        self.assertEqual(result[0]["tableName"], "TMP_TEMPERATURE")
        self.assertEqual(result[0]["partitionColumnName"], "RECEIVED_DATETIME")
        self.assertEqual(result[0]["retentionDayUnit"], 5)

    # ----------------------------------------------------------------------
    # getMaintenancePartitions()の正常系テスト
    # レコード情報が返却されること。
    # ----------------------------------------------------------------------
    def test_getMaintenancePartitions_001(self):
        # 公開DBの初期値セット
        dbExecute("test/sql/t_tmp/delete.sql")
        dbExecute("test/sql/t_tmp/insert001.sql", {"receivedDatetime" : "2020/03/01 00:00:00"})

        result = lambda_function.getMaintenancePartitions("TMP_TEMPERATURE","RECEIVED_DATETIME")
        self.assertEqual(result[0]["partitionKey"], "p20200229")
        self.assertEqual(result[0]["partitionDate"].strftime('%Y/%m/%d %H:%M:%S'),  "2020/03/01 00:00:00")

    # ----------------------------------------------------------------------
    # convertPartitionList()の正常系テスト
    # partitionKeyのListが返却されること。
    # ----------------------------------------------------------------------
    def test_convertPartitionList_001(self):

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

        # 現在時刻
        nowdateTime = lambda_function.getSysDateJst()
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
        # パーティション解除
        try:
            dbExecute("test/sql/t_tmp/removePartition.sql")
        except Exception  as e:
            print("partition解除済みのため、そのまま継続")

        paramTableName = "TMP_TEMPERATURE"
        paramPartitionColumnName = "RECEIVED_DATETIME"
        paramPartitioStr = "PARTITION p20210228 VALUES LESS THAN (TO_DAYS('2021-03/01 00:00:00'))" \
                           ",PARTITION p20210301 VALUES LESS THAN (TO_DAYS('2021-03/02 00:00:00'))" \
                           ",PARTITION p20210302 VALUES LESS THAN (TO_DAYS('2021-03/03 00:00:00'))"
        lambda_function.createNewPartition(paramTableName, paramPartitionColumnName, paramPartitioStr);

        # パーティション確認
        pResult = dbExecute("test/sql/t_tmp/selectPartition.sql")
        self.assertTrue(pResult[0]["partitionName"] is not None)


    # ----------------------------------------------------------------------
    # createNewPartition()の異常系テスト
    # パーティション作成失敗時、エラーログが出力されること。
    # ----------------------------------------------------------------------
    def test_createNewPartition_002(self):
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
        # 予めパーティション作成
        LambdaFunctionTest.test_createNewPartition_001(self)

        paramTableName = "TMP_TEMPERATURE"
        paramPartitioStr = "PARTITION p20210303 VALUES LESS THAN (TO_DAYS('2021-03/04 00:00:00'))"
        lambda_function.addPartition(paramTableName, paramPartitioStr);

        # パーティション確認
        pResult = dbExecute("test/sql/t_tmp/selectPartition.sql")
        self.assertTrue(pResult[0]["partitionName"] is not None)

    # ----------------------------------------------------------------------
    # addPartition()の正常系テスト
    # パーティション作成失敗時、エラーログが出力されること。
    # ----------------------------------------------------------------------
    def test_addPartition_002(self):
        # パーティション解除
        try:
            dbExecute("test/sql/t_tmp/removePartition.sql")
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
        # 公開DBの初期値セット
        dbExecute("test/sql/t_tmp/delete.sql")
        dbExecute("test/sql/t_tmp/insert001.sql", {"receivedDatetime" : "2021/02/28 00:00:00"})
        dbExecute("test/sql/t_tmp/insert001.sql", {"receivedDatetime" : "2021/02/28 23:59:59"})
        dbExecute("test/sql/t_tmp/insert001.sql", {"receivedDatetime" : "2021/03/01 00:00:00"})
        dbExecute("test/sql/t_tmp/insert001.sql", {"receivedDatetime" : "2021/03/01 23:59:59"})
        dbExecute("test/sql/t_tmp/insert001.sql", {"receivedDatetime" : "2021/03/02 00:00:00"})
        dbExecute("test/sql/t_tmp/insert001.sql", {"receivedDatetime" : "2021/03/02 23:59:59"})

        # 予めパーティション作成
        LambdaFunctionTest.test_createNewPartition_001(self)

        paramTableName = "TMP_TEMPERATURE"
        paramDropPartitionStr = "p20210228,p20210301"
        lambda_function.dropPartition(paramTableName, paramDropPartitionStr);

        # レコード確認
        result = dbExecute("test/sql/t_tmp/select.sql")
        self.assertEqual(result[0]["receivedDateTime"].strftime('%Y/%m/%d %H:%M:%S'),  "2021/03/02 00:00:00")
        self.assertEqual(result[1]["receivedDateTime"].strftime('%Y/%m/%d %H:%M:%S'),  "2021/03/02 23:59:59")

    # ----------------------------------------------------------------------
    # dropPartition()の異常系テスト
    # パーティション削除失敗時、エラーログが出力されること。
    # ----------------------------------------------------------------------
    def test_dropPartition_002(self):

        # パーティション解除
        try:
            dbExecute("test/sql/t_tmp/removePartition.sql")
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

        baseTable = []
        baseTable.append({"partitionKey" : "p20210227", "partitionDate" : "2021/02/28 00:00:00"})
        baseTable.append({"partitionKey" : "p20210228", "partitionDate" : "2021/03/01 00:00:00"})
        baseTable.append({"partitionKey" : "p20210301", "partitionDate" : "2021/03/02 00:00:00"})
        baseTable.append({"partitionKey" : "p20210302", "partitionDate" : "2021/03/03 00:00:00"})

        partitionList = ["p20210303"]

        retentionRange = 5

        # mock使用:システム時刻を3/6とする。
        with mock.patch("lambda_function.getSysDateJst") as sysDate_mock:
            sysDate_mock.return_value = dt.strptime("2021/03/06", "%Y/%m/%d")
            result = lambda_function.getDropPartition(baseTable, partitionList, retentionRange);

        self.assertEqual(result, "p20210227,p20210228")