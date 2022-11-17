import unittest
from unittest import mock
import datetime
from datetime import datetime as dt
import json
import pymysql
import initCommon
import configparser
import sys
import opcModel


class OpcModelTest(unittest.TestCase):
    
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
        opcMdl = opcModel.OpcModel(self.LOGGER)
        
        self.assertEqual(type(opcMdl), opcModel.OpcModel)
        self.assertEqual(opcMdl.getLogger(), self.LOGGER)
        self.assertEqual(opcMdl.getAssetId(), None)
    
    # ------------------------------------------------------
    # 数値型のinputParamありの場合、OPCモデルの構造体が生成されること。
    # ------------------------------------------------------
    def test_init_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input001.json")
        json_dict = json.loads(param)
    
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            opcMdl = opcModel.OpcModel(self.LOGGER, r)
    
            self.assertEqual(type(opcMdl), opcModel.OpcModel)
            self.assertEqual(opcMdl.getType(), "PropertyValueUpdate")
            self.assertEqual(opcMdl.getAssetId(), "cb24680e-b029-44c4-a7e6-feff0e4e780e")
            self.assertEqual(opcMdl.getPropertyId(), "eeb6b315-d2b1-4573-a582-337e2d02554f")
            self.assertEqual(opcMdl.getValues()[0].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[0].getTimeInSeconds(), 1667314801)
            self.assertEqual(opcMdl.getValues()[0].getOffsetInNanos(), 68000000)
            self.assertEqual(opcMdl.getValues()[0].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[0].getValue(), 47)
            self.assertEqual(opcMdl.getValues()[0].isValidate(), True)
            self.assertEqual(opcMdl.getValues()[0].getTimeStampStr(), "2022-11-01 15:00:01.068")
            
            self.assertEqual(opcMdl.getValues()[1].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[1].getTimeInSeconds(), 1667314781)
            self.assertEqual(opcMdl.getValues()[1].getOffsetInNanos(), 61000000)
            self.assertEqual(opcMdl.getValues()[1].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[1].getValue(), 46)
            self.assertEqual(opcMdl.getValues()[1].isValidate(), True)
            self.assertEqual(opcMdl.getValues()[1].getTimeStampStr(), "2022-11-01 14:59:41.061")
    
    # ------------------------------------------------------
    # 文字型のinputParamありの場合、OPCモデルの構造体が生成されること。
    # ------------------------------------------------------
    def test_init_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input002.json")
        json_dict = json.loads(param)
    
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            opcMdl = opcModel.OpcModel(self.LOGGER, r)
    
            self.assertEqual(type(opcMdl), opcModel.OpcModel)
            self.assertEqual(opcMdl.getType(), "PropertyValueUpdate")
            self.assertEqual(opcMdl.getAssetId(), "cb24680e-b029-44c4-a7e6-feff0e4e780e")
            self.assertEqual(opcMdl.getPropertyId(), "eeb6b315-d2b1-4573-a582-337e2d02554f")
            self.assertEqual(opcMdl.getValues()[0].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[0].getTimeInSeconds(), 1667314801)
            self.assertEqual(opcMdl.getValues()[0].getOffsetInNanos(), 68000000)
            self.assertEqual(opcMdl.getValues()[0].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[0].getValue(), "aaaa")
            self.assertEqual(opcMdl.getValues()[1].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[1].getTimeInSeconds(), 1667314781)
            self.assertEqual(opcMdl.getValues()[1].getOffsetInNanos(), 61000000)
            self.assertEqual(opcMdl.getValues()[1].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[1].getValue(), "bbbb")
            
    # ------------------------------------------------------
    # bool型のinputParamありの場合、OPCモデルの構造体が生成されること。
    # ------------------------------------------------------
    def test_init_004(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input003.json")
        json_dict = json.loads(param)
    
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            opcMdl = opcModel.OpcModel(self.LOGGER, r)
    
            self.assertEqual(type(opcMdl), opcModel.OpcModel)
            self.assertEqual(opcMdl.getType(), "PropertyValueUpdate")
            self.assertEqual(opcMdl.getAssetId(), "cb24680e-b029-44c4-a7e6-feff0e4e780e")
            self.assertEqual(opcMdl.getPropertyId(), "eeb6b315-d2b1-4573-a582-337e2d02554f")
            self.assertEqual(opcMdl.getValues()[0].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[0].getTimeInSeconds(), 1667314801)
            self.assertEqual(opcMdl.getValues()[0].getOffsetInNanos(), 68000000)
            self.assertEqual(opcMdl.getValues()[0].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[0].getValue(), True)
            self.assertEqual(opcMdl.getValues()[1].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[1].getTimeInSeconds(), 1667314781)
            self.assertEqual(opcMdl.getValues()[1].getOffsetInNanos(), 61000000)
            self.assertEqual(opcMdl.getValues()[1].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[1].getValue(), False)
            
    # ------------------------------------------------------
    # double型のinputParamありの場合、OPCモデルの構造体が生成されること。
    # ------------------------------------------------------
    def test_init_005(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input004.json")
        json_dict = json.loads(param)
    
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            opcMdl = opcModel.OpcModel(self.LOGGER, r)
    
            self.assertEqual(type(opcMdl), opcModel.OpcModel)
            self.assertEqual(opcMdl.getType(), "PropertyValueUpdate")
            self.assertEqual(opcMdl.getAssetId(), "cb24680e-b029-44c4-a7e6-feff0e4e780e")
            self.assertEqual(opcMdl.getPropertyId(), "eeb6b315-d2b1-4573-a582-337e2d02554f")
            self.assertEqual(opcMdl.getValues()[0].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[0].getTimeInSeconds(), 1667314801)
            self.assertEqual(opcMdl.getValues()[0].getOffsetInNanos(), 68000000)
            self.assertEqual(opcMdl.getValues()[0].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[0].getValue(), 10.0)
            self.assertEqual(opcMdl.getValues()[1].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[1].getTimeInSeconds(), 1667314781)
            self.assertEqual(opcMdl.getValues()[1].getOffsetInNanos(), 61000000)
            self.assertEqual(opcMdl.getValues()[1].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[1].getValue(), 10.1)
            
    # ------------------------------------------------------
    # 読込み確認、コンストラクタと同じ挙動となること。
    # ------------------------------------------------------
    def test_read_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        opcMdl = opcModel.OpcModel(self.LOGGER)
    
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input001.json")
        json_dict = json.loads(param)
    
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            opcMdl = opcModel.OpcModel(self.LOGGER)
            opcMdl.read(r)
            
            self.assertEqual(type(opcMdl), opcModel.OpcModel)
            self.assertEqual(opcMdl.getType(), "PropertyValueUpdate")
            self.assertEqual(opcMdl.getAssetId(), "cb24680e-b029-44c4-a7e6-feff0e4e780e")
            self.assertEqual(opcMdl.getPropertyId(), "eeb6b315-d2b1-4573-a582-337e2d02554f")
            self.assertEqual(opcMdl.getValues()[0].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[0].getTimeInSeconds(), 1667314801)
            self.assertEqual(opcMdl.getValues()[0].getOffsetInNanos(), 68000000)
            self.assertEqual(opcMdl.getValues()[0].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[0].getValue(), 47)
            self.assertEqual(opcMdl.getValues()[1].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[1].getTimeInSeconds(), 1667314781)
            self.assertEqual(opcMdl.getValues()[1].getOffsetInNanos(), 61000000)
            self.assertEqual(opcMdl.getValues()[1].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[1].getValue(), 46)
    
    
    # ------------------------------------------------------
    # 不正なフォーマットの場合、Exceptionがスローされること。
    # ------------------------------------------------------
    def test_read_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input005.json")
        json_dict = json.loads(param)
    
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            opcMdl = opcModel.OpcModel(self.LOGGER)
    
            isError = False
            try:
                opcMdl.read(r)
            except Exception as ex:
                print(ex)
                isError = True
            self.assertTrue(isError)
    
    # ------------------------------------------------------
    # Setterで値更新後、モデルに値が反映されていること。
    # ------------------------------------------------------
    def test_setter_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input001.json")
        json_dict = json.loads(param)
        
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            opcMdl = opcModel.OpcModel(self.LOGGER, r)
            
            # 値更新
            opcMdl.setType("T999")
            opcMdl.setAssetId("A999")
            opcMdl.setPropertyId("P999")
            for r2 in opcMdl.getValues():
                r2.setTimeInSeconds(111)
                r2.setOffsetInNanos(222)
                r2.setQuality("hoge")
                r2.setValue("aaa")
        
            self.assertEqual(opcMdl.getType(), "T999")
            self.assertEqual(opcMdl.getAssetId(), "A999")
            self.assertEqual(opcMdl.getPropertyId(), "P999")
            self.assertEqual(opcMdl.getValues()[0].getTimeInSeconds(), 111)
            self.assertEqual(opcMdl.getValues()[0].getOffsetInNanos(), 222)
            self.assertEqual(opcMdl.getValues()[0].getQuality(), "hoge")
            self.assertEqual(opcMdl.getValues()[0].getValue(), "aaa")
    
    # ------------------------------------------------------
    # 辞書型の戻り値で返却されること。
    # ------------------------------------------------------
    def test_getResult_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input001.json")
        json_dict = json.loads(param)
    
        opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[0])
        opcMdlDict = opcMdl.getResult()
        print(opcMdlDict)
    
        self.assertEqual(type(opcMdlDict), dict)
        self.assertEqual(opcMdlDict.get("type"), "PropertyValueUpdate")
        self.assertEqual(opcMdlDict.get("payload").get("assetId"), "cb24680e-b029-44c4-a7e6-feff0e4e780e")
        self.assertEqual(opcMdlDict.get("payload").get("propertyId"), "eeb6b315-d2b1-4573-a582-337e2d02554f")

        self.assertEqual(opcMdlDict.get("payload").get("values")[0].get("timestamp").get("timeInSeconds"), 1667314801)
        self.assertEqual(opcMdlDict.get("payload").get("values")[0].get("timestamp").get("offsetInNanos"), 68000000)
        self.assertEqual(opcMdlDict.get("payload").get("values")[0].get("quality"), "GOOD")
        self.assertEqual(opcMdlDict.get("payload").get("values")[0].get("value").get("integerValue"), 47)
    
    
    # ------------------------------------------------------
    # レコード追加後、モデルに値が反映されること。
    # ------------------------------------------------------
    def test_addRecord_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input001.json")
        json_dict = json.loads(param)
    
        self.assertTrue(0 < len(json_dict.get("receivedMessages")))
        for r in json_dict.get("receivedMessages"):
            opcMdl = opcModel.OpcModel(self.LOGGER, r)
    
    
            # add
            opcMdl.addValue(12345, 6789, "BAD", 1.0)
            print(opcMdl.getResult())
    
            self.assertEqual(type(opcMdl), opcModel.OpcModel)
            self.assertEqual(opcMdl.getType(), "PropertyValueUpdate")
            self.assertEqual(opcMdl.getAssetId(), "cb24680e-b029-44c4-a7e6-feff0e4e780e")
            self.assertEqual(opcMdl.getPropertyId(), "eeb6b315-d2b1-4573-a582-337e2d02554f")
            self.assertEqual(opcMdl.getValues()[0].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[0].getTimeInSeconds(), 1667314801)
            self.assertEqual(opcMdl.getValues()[0].getOffsetInNanos(), 68000000)
            self.assertEqual(opcMdl.getValues()[0].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[0].getValue(), 47)
            self.assertEqual(opcMdl.getValues()[1].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[1].getTimeInSeconds(), 1667314781)
            self.assertEqual(opcMdl.getValues()[1].getOffsetInNanos(), 61000000)
            self.assertEqual(opcMdl.getValues()[1].getValue(), 46)
            self.assertEqual(opcMdl.getValues()[1].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[2].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[2].getTimeInSeconds(), 1667314811)
            self.assertEqual(opcMdl.getValues()[2].getOffsetInNanos(), 74000000)
            self.assertEqual(opcMdl.getValues()[2].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[2].getValue(), 47)
            self.assertEqual(opcMdl.getValues()[3].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[3].getTimeInSeconds(), 1667314791)
            self.assertEqual(opcMdl.getValues()[3].getOffsetInNanos(), 68000000)
            self.assertEqual(opcMdl.getValues()[3].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[3].getValue(), 47)
            self.assertEqual(opcMdl.getValues()[4].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[4].getTimeInSeconds(), 1667314821)
            self.assertEqual(opcMdl.getValues()[4].getOffsetInNanos(), 68000000)
            self.assertEqual(opcMdl.getValues()[4].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[4].getValue(), 48)
            self.assertEqual(opcMdl.getValues()[5].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[5].getTimeInSeconds(), 1667314831)
            self.assertEqual(opcMdl.getValues()[5].getOffsetInNanos(), 62000000)
            self.assertEqual(opcMdl.getValues()[5].getQuality(), "GOOD")
            self.assertEqual(opcMdl.getValues()[5].getValue(), 48)
            # add分
            self.assertEqual(opcMdl.getValues()[6].getLogger(), self.LOGGER)
            self.assertEqual(opcMdl.getValues()[6].getTimeInSeconds(), 12345)
            self.assertEqual(opcMdl.getValues()[6].getOffsetInNanos(), 6789)
            self.assertEqual(opcMdl.getValues()[6].getQuality(), "BAD")
            self.assertEqual(opcMdl.getValues()[6].getValue(), 1.0)
    
    # # ------------------------------------------------------
    # 不正なフォーマットの場合、Exceptionが発生すること。
    # ------------------------------------------------------
    def test_isOwnerValue_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # テスト用パラメータファイル読み込み
        param = initCommon.getQuery("test/opcua/input006.json")
        json_dict = json.loads(param)
    
        print("---- typeなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[0])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- payloadなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[1])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- assetIdなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[2])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- propertyIdなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[3])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- valuesなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[4])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- timestampなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[5])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
    
        print("---- timeInSecondsなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[6])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- offsetInNanosなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[7])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- qualityなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[8])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- valueなし")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[9])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- value空")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[10])
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
        param = initCommon.getQuery("test/opcua/input007.json")
        json_dict = json.loads(param)
    
        print("---- typeが数値")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[0])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- assetIdが数値")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[1])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- propertyIdが数値")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[2])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- timeInSecondsが文字列")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[3])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- offsetInNanosが文字列")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[4])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
        print("---- qualityが数値")
        isError = False
        try:
            opcMdl = opcModel.OpcModel(self.LOGGER, json_dict.get("receivedMessages")[5])
        except Exception as ex:
            isError = True
            print(ex)
        self.assertTrue(isError)
    
    