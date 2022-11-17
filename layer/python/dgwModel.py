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
# DGW基底モデルクラス
# -------------------------
class DgwRecordModel:

    # クラス変数
    LOGGER = None
    
    # DGW基底モデル
    dgwRecordDict = {}
    
    # Key定数
    SENSOR_ID = "sensorId"
    TIMESTAMP = "timeStamp"
    VALUE = "value"
    
    # --------------------------------------------------
    # コンストラクタ 
    # --------------------------------------------------
    # logger(logger)      : ロガー
    # inputDict(logger)   : DGW基底モデル
    # --------------------------------------------------
    def __init__(self, logger, inputDict = {}):
        self.LOGGER = logger

        self.dgwRecordDict = {
                self.SENSOR_ID : None
                , self.TIMESTAMP : None
                , self.VALUE : None
            }
        
        if 0 < len(inputDict):
            self.read(inputDict)

    # --------------------------------------------------
    # Getter
    # --------------------------------------------------
    def getLogger(self):
        return self.LOGGER
    
    def getSensorId(self):
        return self.dgwRecordDict.get(self.SENSOR_ID)
    
    def getTimeStamp(self):
        return self.dgwRecordDict.get(self.TIMESTAMP)
    
    def getValue(self):
        return self.dgwRecordDict.get(self.VALUE)
    
    def getResult(self):
        return self.dgwRecordDict
    
    # --------------------------------------------------
    # Setter
    # --------------------------------------------------
    def setSensorId(self, sensorId):
        self.dgwRecordDict[self.SENSOR_ID] = sensorId
        
    def setTimeStamp(self, timeStamp):
        self.dgwRecordDict[self.TIMESTAMP] = timeStamp
        
    def setValue(self, value):
        self.dgwRecordDict[self.VALUE] = value
        
             
    # --------------------------------------------------
    # 読込み
    # inputDict(dict)   : DGW基底モデル
    # --------------------------------------------------
    def read(self, inputDict):
        
        if (self.isRecord(inputDict)) == False:
            raise Exception("読込みに失敗しました。%s", inputDict)
        
        self.dgwRecordDict = inputDict.copy()
        
        
    # --------------------------------------------------
    # 値判定 
    # record(dict)      : 値
    # --------------------------------------------------
    def isRecord(self, record):
        isResult = False
        
        # Key判定
        isResult = (self.SENSOR_ID in record) and \
                   (self.TIMESTAMP in record) and \
                   (self.VALUE in record)
                   
        if isResult == False:
            self.LOGGER.error("フォーマット不正です。%s", record)
            return False
        
        # データ型判定
        isResult = (initCommon.isValidateString(record.get(self.SENSOR_ID))) and \
                   (initCommon.validateTimeStamp(record.get(self.TIMESTAMP))) 
                   
        if isResult == False:
            self.LOGGER.error("データ型が不正です。%s", record)
            return False
            
        return True
        
# -------------------------
# DGWモデルクラス
# -------------------------
class DgwModel:

    # クラス変数
    LOGGER = None
    
    # DGWモデル
    resultDict = {}
    
    # Key定数
    DEVICE_ID = "deviceId"
    REQUESTTIMESTAMP = "requestTimeStamp"
    RECORDS = "records"
        
    # --------------------------------------------------
    # コンストラクタ 
    # --------------------------------------------------
    # logger(logger)      : ロガー
    # inputDict(dict)     : DGWモデル
    # --------------------------------------------------
    def __init__(self, logger, inputDict = {}):
        self.LOGGER = logger

        self.resultDict = {
                self.DEVICE_ID : None
                , self.REQUESTTIMESTAMP : None
                , self.RECORDS : []
            }
        
        if 0 < len(inputDict):
            self.read(inputDict)

    # --------------------------------------------------
    # Getter
    # --------------------------------------------------
    def getLogger(self):
        return self.LOGGER

    def getResult(self):
        return {
                self.DEVICE_ID : self.getDeviceId()
                , self.REQUESTTIMESTAMP : self.getRequestTimeStamp()
                , self.RECORDS: [
                         r.getResult() for r in self.getRecords()
                    ]
                
            }

    def getDeviceId(self):
        return self.resultDict.get(self.DEVICE_ID)
    
    def getRequestTimeStamp(self):
        return self.resultDict.get(self.REQUESTTIMESTAMP)
    
    def getRecords(self):
        return self.resultDict.get(self.RECORDS)
    
    # --------------------------------------------------
    # Setter
    # --------------------------------------------------
    def setDeviceId(self, deviceId):
        self.resultDict[self.DEVICE_ID] = deviceId
     
    def setRequestTimeStamp(self, requestTimeStamp):
        self.resultDict[self.REQUESTTIMESTAMP] = requestTimeStamp
        
    # --------------------------------------------------
    # 読込み
    # inputDict(dict)   : DGWモデル
    # --------------------------------------------------
    def read(self, inputDict):
        
        if self.isOwnerValue(inputDict) == False:
            raise Exception("読込みに失敗しました。", inputDict) 
        
        self.setDeviceId(inputDict.get(self.DEVICE_ID))
        self.setRequestTimeStamp(inputDict.get(self.REQUESTTIMESTAMP))
        
        for v in inputDict.get(self.RECORDS):
            recordModel = DgwRecordModel(self.LOGGER, v)
            self.resultDict[self.RECORDS].append(recordModel)
    
    # --------------------------------------------------
    # レコード追加
    # sensorId(int)       : センサID   
    # timeStamp(int)      : タイムスタンプ(UTC)
    # value(obj)          : 値
    # --------------------------------------------------
    def addRecord(self, sensorId, timeStamp, value):

        recordModel = DgwRecordModel(self.LOGGER)
        recordModel.setSensorId(sensorId)
        recordModel.setTimeStamp(timeStamp)
        recordModel.setValue(value)
        self.resultDict[self.RECORDS].append(recordModel)
    
    # --------------------------------------------------
    # レコード一覧の追加 
    # records(array)     : レコード一覧
    # --------------------------------------------------
    def addRecords(self, records):
        
        for v in records:
            recordModel = DgwRecordModel(self.LOGGER, v)
            self.resultDict[self.RECORDS].append(recordModel)
    
    # --------------------------------------------------
    # 値判定 
    # value(dict)      : 値
    # --------------------------------------------------
    def isOwnerValue(self, inputDict):
        isResult = False
        
        # Key判定
        isResult = (self.DEVICE_ID in inputDict) and \
                   (self.REQUESTTIMESTAMP in inputDict) and \
                   (self.RECORDS in inputDict)
                   
        if isResult == False:
            self.LOGGER.error("フォーマット不正です。%s", inputDict)
            return False
        
        # データ型判定
        isResult = (initCommon.isValidateString(inputDict.get(self.DEVICE_ID))) and \
                   (initCommon.validateTimeStamp(inputDict.get(self.REQUESTTIMESTAMP))) 
                   
        if isResult == False:
            self.LOGGER.error("データ型が不正です。%s", inputDict)
            return False
            
        
        return True