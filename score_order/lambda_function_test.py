
import json
import unittest
import lambda_function
import initCommon # カスタムレイヤー
import rdsCommon  # カスタムレイヤー

def createEvent(query_file_path):
    f = open(query_file_path, 'r')
    return json.load(f)

# --------------------------------------------------
# スコアデータ作成機能テスト
# --------------------------------------------------
class LambdaFunctionTest(unittest.TestCase):

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(cls):
        lambda_function.initConfig("config/setting01.xml")
        lambda_function.setLogger(initCommon.getLogger(lambda_function.LOG_LEVEL))


    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # スコアデータが新規登録されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("====== test_lambda_handler_001 =====")
        event = createEvent("test/function/input001.json")
        lambda_function.lambda_handler(event, None)

    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # scoreがnullの場合にWARNログが出力されてスキップされること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("====== test_lambda_handler_002 =====")
        event = createEvent("test/function/input002.json")
        lambda_function.lambda_handler(event, None)

    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # scoreが9分割でない場合にWARNログが出力されてスキップされること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("====== test_lambda_handler_003 =====")
        event = createEvent("test/function/input003.json")
        lambda_function.lambda_handler(event, None)
    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # エッジ名がない場合、ERRORログが出力されて終了すること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_004(self):
        print("====== test_lambda_handler_004 =====")
        event = createEvent("test/function/input004.json")
        with self.assertRaises(SystemExit):
            lambda_function.lambda_handler(event, None)

# if __name__ == "__main__":
#     unittest.main()