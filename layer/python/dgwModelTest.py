import unittest
from unittest import mock
import datetime
from datetime import datetime as dt
import json
import pymysql
import initCommon
import configparser
import sys
import dgwModel


class DgwModelTest(unittest.TestCase):
    
    LOGGER = None
    
    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(self):
        self.LOGGER = initCommon.getLogger("INFO")
            
    # ------------------------------------------------------
    # inputParamなしの場合、空のインスタンスが生成されること。
    # ------------------------------------------------------
    def test_init_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        dgwMdl = dgwModel.DgwModel(self.LOGGER)
        
        self.assertEqual(type(dgwMdl), dgwModel.DgwModel)
        self.assertEqual(dgwMdl.getLogger(), self.LOGGER)
        self.assertEqual(dgwMdl.getDeviceId(), None)
        
    # ------------------------------------------------------
    # inputParamありの場合、DGWモデルの構造体が生成されること。
    # ------------------------------------------------------
    def test_init_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/function/input001.json")
        json_dict = json.loads(param)
        
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            dgwMdl = dgwModel.DgwModel(self.LOGGER, r)
            
            self.assertEqual(type(dgwMdl), dgwModel.DgwModel)
            self.assertEqual(dgwMdl.getDeviceId(), "700400015-66DEF1DE")
            self.assertEqual(dgwMdl.getRequestTimeStamp(), "2021-02-16 12:56:38.895")
            self.assertEqual(dgwMdl.getRecords()[0].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[0].getSensorId(), "s001")
            self.assertEqual(dgwMdl.getRecords()[0].getTimeStamp(), "2021-02-16 12:56:38.895")
            self.assertEqual(dgwMdl.getRecords()[0].getValue(), 1231)
            self.assertEqual(dgwMdl.getRecords()[1].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[1].getSensorId(), "s002")
            self.assertEqual(dgwMdl.getRecords()[1].getTimeStamp(), "2021-02-16 12:56:38.895")
            self.assertEqual(dgwMdl.getRecords()[1].getValue(), 2356)
            self.assertEqual(dgwMdl.getRecords()[2].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[2].getSensorId(), "s001")
            self.assertEqual(dgwMdl.getRecords()[2].getTimeStamp(), "2021-02-16 12:57:38.895")
            self.assertEqual(dgwMdl.getRecords()[2].getValue(), 4321)
        
    # ------------------------------------------------------
    # 読込み確認、コンストラクタと同じ挙動となること。
    # ------------------------------------------------------
    def test_read_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        dgwMdl = dgwModel.DgwModel(self.LOGGER)
        
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/function/input001.json")
        json_dict = json.loads(param)
        
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            dgwMdl = dgwModel.DgwModel(self.LOGGER)
            dgwMdl.read(r)
            
            self.assertEqual(type(dgwMdl), dgwModel.DgwModel)
            self.assertEqual(dgwMdl.getDeviceId(), "700400015-66DEF1DE")
            self.assertEqual(dgwMdl.getRequestTimeStamp(), "2021-02-16 12:56:38.895")
            self.assertEqual(dgwMdl.getRecords()[0].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[0].getSensorId(), "s001")
            self.assertEqual(dgwMdl.getRecords()[0].getTimeStamp(), "2021-02-16 12:56:38.895")
            self.assertEqual(dgwMdl.getRecords()[0].getValue(), 1231)
            self.assertEqual(dgwMdl.getRecords()[1].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[1].getSensorId(), "s002")
            self.assertEqual(dgwMdl.getRecords()[1].getTimeStamp(), "2021-02-16 12:56:38.895")
            self.assertEqual(dgwMdl.getRecords()[1].getValue(), 2356)
            self.assertEqual(dgwMdl.getRecords()[2].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[2].getSensorId(), "s001")
            self.assertEqual(dgwMdl.getRecords()[2].getTimeStamp(), "2021-02-16 12:57:38.895")
            self.assertEqual(dgwMdl.getRecords()[2].getValue(), 4321)
            
            
    # ------------------------------------------------------
    # 不正なフォーマットの場合、Exceptionがスローされること。
    # ------------------------------------------------------
    def test_read_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/function/input004.json")
        json_dict = json.loads(param)
        
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            dgwMdl = dgwModel.DgwModel(self.LOGGER)
            
            isError = False
            try:
                dgwMdl.read(r)
            except Exception as ex:
                isError = True
                print(ex)
            self.assertTrue(isError)
            
    # ------------------------------------------------------
    # Setterで値更新後、モデルに値が反映されていること。
    # ------------------------------------------------------
    def test_setter_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/function/input001.json")
        json_dict = json.loads(param)
        
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            dgwMdl = dgwModel.DgwModel(self.LOGGER, r)
            
            # 値更新
            dgwMdl.setDeviceId("d999")
            dgwMdl.setRequestTimeStamp("2022-11-01 12:00:00.000")
            for r2 in dgwMdl.getRecords():
                r2.setSensorId("s999")
                r2.setTimeStamp("2022-11-01 03:00:00")
                r2.setValue("hoge")
        
            self.assertEqual(dgwMdl.getDeviceId(), "d999")
            self.assertEqual(dgwMdl.getRequestTimeStamp(), "2022-11-01 12:00:00.000")
            self.assertEqual(dgwMdl.getRecords()[0].getSensorId(), "s999")
            self.assertEqual(dgwMdl.getRecords()[0].getTimeStamp(), "2022-11-01 03:00:00")
            self.assertEqual(dgwMdl.getRecords()[0].getValue(), "hoge")
            
    # ------------------------------------------------------
    # 辞書型の戻り値で返却されること。
    # ------------------------------------------------------
    def test_getResult_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/function/input003.json")
        json_dict = json.loads(param)
        
        dgwMdl_1 = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[0])
        dgwMdl_2 = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[1])
        
        print(dgwMdl_1.getResult())
        print(dgwMdl_2.getResult())
            
        self.assertEqual(type(dgwMdl_1.getResult()), dict)
        self.assertEqual(dgwMdl_1.getDeviceId(), "UT_DEVICE001")
        self.assertEqual(dgwMdl_1.getRequestTimeStamp(), "2021-07-05 12:00:00.000")
        
        self.assertEqual(type(dgwMdl_2.getResult()), dict)
        self.assertEqual(dgwMdl_2.getDeviceId(), "UT_DEVICE001")
        self.assertEqual(dgwMdl_2.getRequestTimeStamp(), "2021-07-05 12:30:00.000")
        
        
    # ------------------------------------------------------
    # レコード追加後、モデルに値が反映されること。
    # ------------------------------------------------------
    def test_addRecord_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/function/input001.json")
        json_dict = json.loads(param)
        
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            dgwMdl = dgwModel.DgwModel(self.LOGGER, r)
            
            # add
            dgwMdl.addRecord("add1", "2022-12-01 01:00:00", 999)
            dgwMdl.addRecord("add2", "2022-12-01 02:00:00", "TRUE")
            print(dgwMdl.getResult())
        
            self.assertEqual(type(dgwMdl), dgwModel.DgwModel)
            self.assertEqual(dgwMdl.getDeviceId(), "700400015-66DEF1DE")
            self.assertEqual(dgwMdl.getRequestTimeStamp(), "2021-02-16 12:56:38.895")
            self.assertEqual(dgwMdl.getRecords()[0].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[0].getSensorId(), "s001")
            self.assertEqual(dgwMdl.getRecords()[0].getTimeStamp(), "2021-02-16 12:56:38.895")
            self.assertEqual(dgwMdl.getRecords()[0].getValue(), 1231)
            self.assertEqual(dgwMdl.getRecords()[1].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[1].getSensorId(), "s002")
            self.assertEqual(dgwMdl.getRecords()[1].getTimeStamp(), "2021-02-16 12:56:38.895")
            self.assertEqual(dgwMdl.getRecords()[1].getValue(), 2356)
            self.assertEqual(dgwMdl.getRecords()[2].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[2].getSensorId(), "s001")
            self.assertEqual(dgwMdl.getRecords()[2].getTimeStamp(), "2021-02-16 12:57:38.895")
            self.assertEqual(dgwMdl.getRecords()[2].getValue(), 4321)
            
            self.assertEqual(dgwMdl.getRecords()[3].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[3].getSensorId(), "add1")
            self.assertEqual(dgwMdl.getRecords()[3].getTimeStamp(), "2022-12-01 01:00:00")
            self.assertEqual(dgwMdl.getRecords()[3].getValue(), 999)
            self.assertEqual(dgwMdl.getRecords()[4].getLogger(), self.LOGGER)
            self.assertEqual(dgwMdl.getRecords()[4].getSensorId(), "add2")
            self.assertEqual(dgwMdl.getRecords()[4].getTimeStamp(), "2022-12-01 02:00:00")
            self.assertEqual(dgwMdl.getRecords()[4].getValue(), "TRUE")
            
    # ------------------------------------------------------
    # 不正なフォーマットの場合、Exceptionが発生すること。
    # ------------------------------------------------------
    def test_isOwnerValue_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/function/input005.json")
        json_dict = json.loads(param)
        
        print("---- deviceIdなし")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[0])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
            
        print("---- requestTimeStampなし")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[1])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
        
        print("---- recordsなし")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[2])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
        
        print("---- sensorIdなし")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[3])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
        
        print("---- timeStampなし")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[4])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
        
        print("---- valueなし")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[5])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
        
        
    # ------------------------------------------------------
    # 不正なフォーマットの場合、Exceptionが発生すること。
    # ------------------------------------------------------
    def test_isOwnerValue_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/function/input006.json")
        json_dict = json.loads(param)
        
        print("---- deviceId不正")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[0])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
            
        print("---- requestTimeStamp不正")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[1])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
        
        print("---- sensorId不正")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[2])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
        
        print("---- timeStamp不正")
        isError = False
        try:
            dgwMdl = dgwModel.DgwModel(self.LOGGER, json_dict.get("receivedMessages")[3])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
        