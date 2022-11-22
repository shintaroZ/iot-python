import logging
import datetime
import sys
import pymysql
import time
import pika
import ssl
from enum import IntEnum
import json
import initCommon

# -------------------------
# OPC-UAモデル基底クラス
# -------------------------
class OpcValueModel:
    # クラス変数
    LOGGER = None
    
    # OPC-UA基底モデル
    opcModelValueDict = {}
    
    # Key定数
    TIMESTAMP = "timestamp"
    TIMEINSECONDS = "timeInSeconds"
    OFFSETINNANOS = "offsetInNanos"
    QUALITY = "quality"
    VALUE = "value"
    INTEGER_VALUE = "integerValue"
    STRING_VALUE = "stringValue"
    BOOLEAN_VALUE = "booleanValue"
    DOUBLE_VALUE = "doubleValue"
    
    # --------------------------------------------------
    # コンストラクタ 
    # --------------------------------------------------
    # logger(logger)      : ロガー
    # inputDict(logger)   : OPCモデル
    # --------------------------------------------------
    def __init__(self, logger, inputDict = {}):
        self.LOGGER = logger

        timeStampDict = {
                self.TIMEINSECONDS : None
                , self.OFFSETINNANOS : None
            }
        valueDict = {
                self.INTEGER_VALUE : None
                , self.STRING_VALUE : None
                , self.BOOLEAN_VALUE : None
                , self.DOUBLE_VALUE : None
            }
        self.opcModelValueDict = {
                self.TIMESTAMP : timeStampDict
                , self.QUALITY : None
                , self.VALUE : valueDict
            }
        
        if 0 < len(inputDict):
            self.read(inputDict)

    # --------------------------------------------------
    # Getter
    # --------------------------------------------------
    def getLogger(self):
        return self.LOGGER
    
    def getTimeInSeconds(self):
        return self.opcModelValueDict.get(self.TIMESTAMP).get(self.TIMEINSECONDS)

    def getOffsetInNanos(self):
        return self.opcModelValueDict.get(self.TIMESTAMP).get(self.OFFSETINNANOS)
    
    def getQuality(self):
        return self.opcModelValueDict.get(self.QUALITY)
    
    def getValue(self):
        if self.opcModelValueDict.get(self.VALUE).get(self.INTEGER_VALUE) is not None:
            return self.opcModelValueDict.get(self.VALUE).get(self.INTEGER_VALUE)
        elif self.opcModelValueDict.get(self.VALUE).get(self.STRING_VALUE) is not None:
            return self.opcModelValueDict.get(self.VALUE).get(self.STRING_VALUE)
        elif self.opcModelValueDict.get(self.VALUE).get(self.BOOLEAN_VALUE) is not None:
            return self.opcModelValueDict.get(self.VALUE).get(self.BOOLEAN_VALUE)
        elif self.opcModelValueDict.get(self.VALUE).get(self.DOUBLE_VALUE) is not None:
            return self.opcModelValueDict.get(self.VALUE).get(self.DOUBLE_VALUE)
        else:
            return None
        
    def isValidate(self):
        return self.isValue(self.opcModelValueDict)
    
    def getTimeStampStr(self, tz=datetime.timezone.utc, format="%Y-%m-%d %H:%M:%S.%f"):
        epoksec = self.opcModelValueDict.get(self.TIMESTAMP).get(self.TIMEINSECONDS) + self.opcModelValueDict.get(self.TIMESTAMP).get(self.OFFSETINNANOS) / 1000000000
        return datetime.datetime.fromtimestamp(epoksec, tz).strftime(format)[:-3]
    
    def getResult(self):
        return self.opcModelValueDict
        
    # --------------------------------------------------
    # Setter
    # --------------------------------------------------
    def setTimeInSeconds(self, timeInSeconds):
        self.opcModelValueDict[self.TIMESTAMP][self.TIMEINSECONDS] = timeInSeconds

    def setOffsetInNanos(self, offsetInNanos):
        self.opcModelValueDict[self.TIMESTAMP][self.OFFSETINNANOS] = offsetInNanos
        
    def setQuality(self, quality):
        self.opcModelValueDict[self.QUALITY] = quality
        
    def setValue(self, value):
        # 一旦クリア
        self.opcModelValueDict[self.VALUE][self.DOUBLE_VALUE] = None
        self.opcModelValueDict[self.VALUE][self.INTEGER_VALUE] = None
        self.opcModelValueDict[self.VALUE][self.BOOLEAN_VALUE] = None
        self.opcModelValueDict[self.VALUE][self.STRING_VALUE] = None
        
        # データ型に応じて動的にKeyを変更
        if initCommon.isValidateFloat(value, False):
            self.opcModelValueDict[self.VALUE][self.DOUBLE_VALUE] = value
        elif initCommon.isValidateNumber(value, False):
            self.opcModelValueDict[self.VALUE][self.INTEGER_VALUE] = value
        elif initCommon.isValidateBoolean(value, False):
            self.opcModelValueDict[self.VALUE][self.BOOLEAN_VALUE] = value
        elif initCommon.isValidateString(value, False):
            self.opcModelValueDict[self.VALUE][self.STRING_VALUE] = value
        
        
    # --------------------------------------------------
    # 読込み
    # inputDict(dict)   : OPC-UA基底モデル
    # --------------------------------------------------
    def read(self, inputDict):
        
        if (self.isValue(inputDict)) == False:
            raise Exception("読込みに失敗しました。%s", inputDict)
        
        self.opcModelValueDict = inputDict.copy()
    # --------------------------------------------------
    # 値判定 
    # value(dict)      : 値
    # --------------------------------------------------
    def isValue(self, value):
        isResult = False
        
        # Key判定
        isResult = (self.TIMESTAMP in value) and \
                   (self.TIMEINSECONDS) in value.get(self.TIMESTAMP) and \
                   (self.OFFSETINNANOS) in value.get(self.TIMESTAMP) and \
                   (self.QUALITY in value) and \
                   (self.VALUE in value) and \
                   (self.INTEGER_VALUE in value.get(self.VALUE) or 
                    self.STRING_VALUE in value.get(self.VALUE) or 
                    self.BOOLEAN_VALUE in value.get(self.VALUE) or 
                    self.DOUBLE_VALUE in value.get(self.VALUE))
                   
        if isResult == False:
            self.LOGGER.error("フォーマット不正です。%s", value)
            return False
        
        # データ型判定
        isResult = (initCommon.isValidateNumber(value.get(self.TIMESTAMP).get(self.TIMEINSECONDS))) and \
                   (initCommon.isValidateNumber(value.get(self.TIMESTAMP).get(self.OFFSETINNANOS))) and \
                   (initCommon.isValidateString(value.get(self.QUALITY)))
                   
        if isResult == False:
            self.LOGGER.error("データ型が不正です。%s", value)
            return False
            
        return True

# -------------------------
# OPC-UAモデルクラス
# -------------------------
class OpcModel:

    # クラス変数
    LOGGER = None
    
    # OPC-UAモデル
    resultDict = {}
    
    # Key定数
    TYPE = "type"
    PAYLOAD = "payload"
    ASSET_ID = "assetId"
    PROPERTY_ID = "propertyId"
    VALUES = "values"
    
        
    # --------------------------------------------------
    # コンストラクタ 
    # --------------------------------------------------
    # logger(logger)      : ロガー
    # inputDict(logger)   : OPCモデル
    # --------------------------------------------------
    def __init__(self, logger, inputDict = {}):
        self.LOGGER = logger

        payloadDict = {
                self.ASSET_ID : None
                , self.PROPERTY_ID : None
                , self.VALUES : []
            }
        
        self.resultDict = {
                self.TYPE : None
                , self.PAYLOAD : payloadDict
            }
        
        if 0 < len(inputDict):
            self.read(inputDict)

    # --------------------------------------------------
    # Getter
    # --------------------------------------------------
    def getLogger(self):
        return self.LOGGER

    def getAssetId(self):
        return self.resultDict.get(self.PAYLOAD).get(self.ASSET_ID)
    
    def getPropertyId(self):
        return self.resultDict.get(self.PAYLOAD).get(self.PROPERTY_ID)

    def getType(self):
        return self.resultDict.get(self.TYPE)
    
    def getValues(self):
        return self.resultDict.get(self.PAYLOAD).get(self.VALUES)
    
    def getResult(self):
        return {
                self.TYPE : self.getType()
                , self.PAYLOAD : {
                        self.ASSET_ID : self.getAssetId()
                        , self.PROPERTY_ID : self.getPropertyId()
                        , self.VALUES : [
                                v.getResult() for v in self.getValues()
                            ]
                    }
            }
    
    # --------------------------------------------------
    # Setter
    # --------------------------------------------------
    def setType(self, type):
        self.resultDict[self.TYPE] = type
     
    def setAssetId(self, assetId):
        self.resultDict[self.PAYLOAD][self.ASSET_ID] = assetId
    
    def setPropertyId(self, propertyId):
        self.resultDict[self.PAYLOAD][self.PROPERTY_ID] = propertyId
      

    # --------------------------------------------------
    # 読込み
    # inputDict(dict)   : OPCモデル
    # --------------------------------------------------
    def read(self, inputDict):
        
        if self.isOwnerValue(inputDict) == False:
            raise Exception("読込みに失敗しました。%s", inputDict) 
        
        self.setType(inputDict.get(self.TYPE))
        self.setAssetId(inputDict.get(self.PAYLOAD).get(self.ASSET_ID))
        self.setPropertyId(inputDict.get(self.PAYLOAD).get(self.PROPERTY_ID))
        
        for v in inputDict.get(self.PAYLOAD).get(self.VALUES):
            valueModel = OpcValueModel(self.LOGGER, v)
            self.resultDict[self.PAYLOAD][self.VALUES].append(valueModel)
    
                    
    # --------------------------------------------------
    # 値判定 
    # value(dict)      : 値
    # --------------------------------------------------
    def isOwnerValue(self, inputDict):
        isResult = False
        
        # Key判定
        isResult = (self.TYPE in inputDict) and \
                   (self.PAYLOAD in inputDict) and \
                   (self.ASSET_ID in inputDict.get(self.PAYLOAD)) and \
                   (self.PROPERTY_ID in inputDict.get(self.PAYLOAD)) and \
                   (self.VALUES in inputDict.get(self.PAYLOAD)) 
        if isResult == False:
            self.LOGGER.error("フォーマット不正です。%s", inputDict)
            return False
        
        # データ型判定
        isResult = (initCommon.isValidateString(inputDict.get(self.TYPE))) and \
                   (initCommon.isValidateString(inputDict.get(self.PAYLOAD).get(self.ASSET_ID))) and \
                   (initCommon.isValidateString(inputDict.get(self.PAYLOAD).get(self.PROPERTY_ID)))

        if isResult == False:
            self.LOGGER.error("データ型が不正です。%s", inputDict)
            return False
        
        return True
    # --------------------------------------------------
    # 値追加
    # timeInSeconds(int)  : 秒単位の時間   
    # offsetInNanos(int)  : ナノ秒オフセット
    # quality(int)        : 品質
    # value(obj)          : 値
    # --------------------------------------------------
    def addValue(self, timeInSeconds, offsetInNanos, quality, value):

        valueModel = OpcValueModel(self.LOGGER)
        valueModel.setTimeInSeconds(timeInSeconds)
        valueModel.setOffsetInNanos(offsetInNanos)
        valueModel.setQuality(quality)
        valueModel.setValue(value)
        
        if valueModel.isValidate():
            self.resultDict[self.PAYLOAD][self.VALUES].append(valueModel)
        
    
    # --------------------------------------------------
    # 値一覧の追加 
    # values(array)     : 値一覧
    # --------------------------------------------------
    def addValues(self, values):
        
        for v in values:
            valueModel = OpcValueModel(self.LOGGER, v)
            self.resultDict[self.PAYLOAD][self.VALUES].append(valueModel)