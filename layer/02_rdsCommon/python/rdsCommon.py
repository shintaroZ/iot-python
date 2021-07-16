import logging
import datetime
import sys
import pymysql
import time

# -------------------------
# RDS接続共通クラス
# -------------------------
class rdsCommon:

    # クラス変数
    LOGGER = None
    CONNECT = None

    # --------------------------------------------------
    # プロパティ
    # --------------------------------------------------
    def getConnect(self):
        global CONNECT
        return CONNECT
    def getLogger(self):
        global LOGGER
        return LOGGER

    # --------------------------------------------------
    # コンストラクタ パラメータを元にRDS接続を行う
    # --------------------------------------------------
    # logger(logger)      : ロガー
    # host(str)           : 接続先エンドポイント
    # port(int)           : 接続先ポート番号
    # user(str)           : ユーザ
    # password(str)       : パスワード
    # dbName(str)         : 接続先インスタンス
    # connectTimeout(int) : 接続タイムアウト(s)
    # autoCommit(bool)    : 自動コミット
    # --------------------------------------------------
    def __init__(self, logger=None, host="localhost", port=3306, user="hoge", password="hoge" ,dbName="hoge"
                 ,connectTimeout=3, autoCommit=False):
        global LOGGER, CONNECT
        LOGGER = logger

        try:
            LOGGER.debug('RDS connect start')
            CONNECT = pymysql.connect(host=host, port=port, user=user, passwd=password, db=dbName,
                                      connect_timeout=connectTimeout, autocommit=autoCommit)
            LOGGER.debug('Success connecting to RDS mysql instance')
        except Exception  as e:
            LOGGER.error('Fail connecting to RDS mysql instance')
            raise(e)

    # --------------------------------------------------
    # デストラクタ コネクションをクローズ
    # --------------------------------------------------
    def __del__(self):
        if self.CONNECT is not None:
            CONNECT.close()


    # --------------------------------------------------
    # Select結果を全件取得 配列 + 辞書形式で返却
    # query(str)  : SQL文
    # params(dict) : パラメータ
    # --------------------------------------------------
    def fetchall(self, query, params={}):
        LOGGER.debug('SQL:\r\n' + query % params)
        with CONNECT.cursor(pymysql.cursors.DictCursor) as cur:
            try:
                cur.execute(query % params)
                result = cur.fetchall()
            except Exception  as e:
                LOGGER.error('fetchall error');
                cur.close()
                raise(e)
        return result


    # --------------------------------------------------
    # Select結果の先頭1件のみ取得 辞書形式で返却
    # query(str)  : SQL文
    # params(dict) : パラメータ
    # --------------------------------------------------
    def fetchone(self, query, params={}):
        LOGGER.debug('SQL:\r\n' + query % params)
        with CONNECT.cursor(pymysql.cursors.DictCursor) as cur:
            try:
                cur.execute(query % params)
                result = cur.fetchone()
            except Exception  as e:
                LOGGER.error('fetchone error');
                cur.close()
                raise(e)
        return result

    # --------------------------------------------------
    # リトライ付更新処理-共通化
    # query(str)         : SQL文
    # params(dict)       : パラメータ
    # retryMaxCount(int) : リトライ回数
    # retryInterval(int) : リトライ時のインターバル(ms)
    # --------------------------------------------------
    def execute(self, query, params={}, retryMaxCount=3, retryInterval=500):
        retryCount = 0
        retryFlg = True
        LOGGER.debug('SQL:\r\n' + query % params)
        with CONNECT.cursor(pymysql.cursors.DictCursor) as cur:
            while (retryFlg):
                try:
                    cur.execute("SET SESSION time_zone = 'Asia/Tokyo';")
                    cur.execute(query % params)
                    retryFlg = False
                    LOGGER.debug('登録に成功しました。')
                except Exception as e:
                    retryCount = retryCount + 1
                    if retryCount < retryMaxCount:
                        LOGGER.warning('登録に失敗しました。リトライします。(リトライ回数:%d)' % retryCount)
                        time.sleep(retryInterval / 1000)
                    else:
                        LOGGER.error('リトライ回数を超過しました。登録処理を終了します。: %s' % e)
                        retryFlg = False
                        raise (e)

    # --------------------------------------------------
    # 手動コミット
    # --------------------------------------------------
    def commit(self):
        CONNECT.commit()

    # --------------------------------------------------
    # 手動ロールバック
    # --------------------------------------------------
    def rollBack(self):
        CONNECT.rollback()
