# -*- coding: utf-8 -*-
"""
DWH_DB_DB_DATA_COPY Glue Job

- RedshiftのRAW層からSTORAGE層へデータコピーを行うGlue Job
- AWS Secrets ManagerからRedshift接続情報を取得
- PEP8に準拠し、docstringを付与
- コード品質: 100%のテストカバレッジを目指す（本例ではテストコード省略）
"""

import sys
import os
import json
import logging
import configparser

# Glue特有の引数解析ライブラリ
from awsglue.utils import getResolvedOptions

import boto3
import pg8000


class GlueLogger:
    """
    Glueジョブで使用するカスタムロガー。
    
    log_message_config.iniファイルからメッセージを読み込み、
    指定されたログレベルに応じて出力を行う。
    """
    def __init__(self, config_path='log_message_config.ini', default_level=logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(default_level)

        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        self.messages = {}
        if os.path.exists(config_path):
            self.load_messages(config_path)

    def load_messages(self, config_path):
        """
        log_message_config.iniからメッセージを読み込み、self.messagesに格納する。
        """
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')
        for section in config.sections():
            for key, value in config.items(section):
                # 例: self.messages["INFO.job_start"] = "ジョブ開始: {0}"
                self.messages[f"{section}.{key}"] = value

    def trace(self, msg_key, *args):
        """TRACEレベルのログを出力する。"""
        # Python標準loggingにはTRACEレベルがないため、DEBUGで代用
        self.logger.debug(self._get_message(msg_key, *args))

    def debug(self, msg_key, *args):
        """DEBUGレベルのログを出力する。"""
        self.logger.debug(self._get_message(msg_key, *args))

    def info(self, msg_key, *args):
        """INFOレベルのログを出力する。"""
        self.logger.info(self._get_message(msg_key, *args))

    def warn(self, msg_key, *args):
        """WARNレベルのログを出力する。"""
        self.logger.warning(self._get_message(msg_key, *args))

    def error(self, msg_key, *args):
        """ERRORレベルのログを出力する。"""
        self.logger.error(self._get_message(msg_key, *args))

    def fatal(self, msg_key, *args):
        """FATALレベルのログを出力する。"""
        self.logger.critical(self._get_message(msg_key, *args))

    def _get_message(self, msg_key, *args):
        """
        log_message_config.iniに設定されたメッセージを取得し、argsをformatして返す。
        """
        message = self.messages.get(msg_key, msg_key)  # 未定義キーの場合はキー名をそのまま出力
        return message.format(*args)


def get_secret(secret_name):
    """
    AWS Secrets ManagerからRedshift接続情報を取得する。

    :param secret_name: シークレット名
    :return: シークレット情報を格納した辞書
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response['SecretString']
    # 例: {"username":"xxxx","password":"xxxx","host":"xxxx","port":"5439","dbName":"xxxx"}
    return json.loads(secret_string)


def get_redshift_connection(secret_dict):
    """
    Redshiftに接続するためのpg8000のコネクションを取得する。

    :param secret_dict: Secrets Managerから取得した辞書形式の接続情報
    :return: pg8000接続オブジェクト
    """
    try:
        conn = pg8000.connect(
            database=secret_dict['dbName'],
            user=secret_dict['username'],
            password=secret_dict['password'],
            host=secret_dict['host'],
            port=int(secret_dict['port'])
        )
        return conn
    except Exception as err:
        raise RuntimeError(f"Redshiftへの接続に失敗しました: {err}") from err


def run_queries(conn, queries):
    """
    指定されたクエリリストをRedshiftで順番に実行する。

    :param conn: Redshiftのコネクションオブジェクト
    :param queries: 実行するクエリのリスト
    """
    with conn.cursor() as cursor:
        for query in queries:
            cursor.execute(query)
        conn.commit()


def main():
    """
    Glue Jobのメイン処理。
    1. パラメータ取得
    2. Secrets Managerから接続情報を取得
    3. Redshiftに接続
    4. テーブルコピー(トランケート -> INSERT SELECT)
    5. ログ出力
    """

    # Glueジョブの引数定義
    # 例: --SECRET_NAME my_redshift_secret --RAW_SCHEMA raw_schema --RAW_TABLE raw_table
    #     --STORAGE_SCHEMA storage_schema --STORAGE_TABLE storage_table
    args = getResolvedOptions(
        sys.argv,
        [
            'SECRET_NAME',
            'RAW_SCHEMA',
            'RAW_TABLE',
            'STORAGE_SCHEMA',
            'STORAGE_TABLE'
        ]
    )

    secret_name = args['SECRET_NAME']
    raw_schema = args['RAW_SCHEMA']
    raw_table = args['RAW_TABLE']
    storage_schema = args['STORAGE_SCHEMA']
    storage_table = args['STORAGE_TABLE']

    # カスタムロガーの初期化
    logger = GlueLogger(config_path='log_message_config.ini', default_level=logging.INFO)

    # ジョブ開始ログ
    logger.info("INFO.job_start", "DWH_DB_DB_DATA_COPYジョブ")

    # 1. Secrets ManagerからRedshift接続情報を取得
    logger.debug("DEBUG.fetch_secret", secret_name)
    secret_dict = get_secret(secret_name)

    # 2. Redshiftコネクション取得
    logger.debug("DEBUG.connect_redshift")
    conn = get_redshift_connection(secret_dict)
    logger.info("INFO.connect_redshift_success")

    # 3. テーブルコピー用クエリ作成 (例: raw -> storage)
    truncate_storage = f"TRUNCATE TABLE {storage_schema}.{storage_table}"
    insert_storage = (
        f"INSERT INTO {storage_schema}.{storage_table} "
        f"SELECT * FROM {raw_schema}.{raw_table}"
    )

    queries_to_run = [
        truncate_storage,
        insert_storage
    ]

    # 4. クエリ実行 (データコピー)
    try:
        logger.info("INFO.data_copy_start")
        run_queries(conn, queries_to_run)
        logger.info("INFO.data_copy_end")
    except Exception as e:
        logger.error("ERROR.data_copy_fail", str(e))
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("INFO.connection_closed")

    # ジョブ終了ログ
    logger.info("INFO.job_end", "DWH_DB_DB_DATA_COPYジョブ")


# メイン処理起動
if __name__ == "__main__":
    main()
