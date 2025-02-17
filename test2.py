import sys
import boto3
import logging
import redshift_connector
import json
from awsglue.utils import getResolvedOptions

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_secret(secret_name, jobnet_id):
    """
    AWS Secrets Manager から Redshift の接続情報を取得する。
    
    引数:
        secret_name (str): AWS Secrets Manager に保存されている Redshift 認証情報のキー名。
        jobnet_id (str): ジョブネット ID。
    
    戻り値:
        dict: Redshift の接続情報を含む辞書。
    """
    try:
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logging.error(f"E_DWH_JB_DB_DATA_COPY_004: {jobnet_id} Redshift 接続情報の取得が異常終了。")
        raise e

def connect_to_redshift(credentials, jobnet_id):
    """
    redshift_connector を使用して Redshift に接続する。
    
    引数:
        credentials (dict): Redshift の接続認証情報。
        jobnet_id (str): ジョブネット ID。
    
    戻り値:
        redshift_connector.Connection: Redshift の接続オブジェクト。
    """
    try:
        conn = redshift_connector.connect(
            database=credentials['dbname'],
            user=credentials['username'],
            password=credentials['password'],
            host=credentials['host'],
            port=int(credentials['port'])
        )
        return conn
    except Exception as e:
        logging.error(f"E_DWH_JB_DB_DATA_COPY_005: {jobnet_id} Redshift に接続が異常終了。")
        raise e

def execute_sql(conn, source_schema, source_table, target_schema, target_table, jobnet_id):
    """
    Redshift のデータコピー SQL を実行する。
    
    引数:
        conn (redshift_connector.Connection): Redshift の接続オブジェクト。
        source_schema (str): ソーステーブルのスキーマ。
        source_table (str): ソーステーブルの名前。
        target_schema (str): ターゲットテーブルのスキーマ。
        target_table (str): ターゲットテーブルの名前。
        jobnet_id (str): ジョブネット ID。
    """
    sql = f"""
        TRUNCATE TABLE {target_schema}.{target_table};
        INSERT INTO {target_schema}.{target_table} 
        SELECT * FROM {source_schema}.{source_table};
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()
        logging.info(f"I_DWH_JB_DB_DATA_COPY_002: {jobnet_id} Redshift にデータ登録が正常終了。(登録先スキーマ: {target_schema}; 登録先テーブル: {target_table})")
    except Exception as e:
        conn.rollback()
        logging.error(f"E_DWH_JB_DB_DATA_COPY_001: {jobnet_id} Redshift にデータ登録が異常終了。(登録先スキーマ: {target_schema}; 登録先テーブル: {target_table})")
        raise e

def main():
    """
    Glue Job のエントリーポイント。パラメータを取得し、データコピー処理を実行する。
    """
    args = getResolvedOptions(sys.argv, ['SECRET_NAME', 'JOBNET_ID', 'SOURCE_SCHEMA', 'SOURCE_TABLE', 'TARGET_SCHEMA', 'TARGET_TABLE'])
    
    secret_name = args['SECRET_NAME']
    jobnet_id = args['JOBNET_ID']
    source_schema = args['SOURCE_SCHEMA']
    source_table = args['SOURCE_TABLE']
    target_schema = args['TARGET_SCHEMA']
    target_table = args['TARGET_TABLE']
    
    logging.info(f"I_DWH_JB_DB_DATA_COPY_001: {jobnet_id} DWH 内データ取り込み処理を開始します。")
    
    try:
        credentials = get_secret(secret_name, jobnet_id)
        conn = connect_to_redshift(credentials, jobnet_id)
        execute_sql(conn, source_schema, source_table, target_schema, target_table, jobnet_id)
        logging.info(f"I_DWH_JB_DB_DATA_COPY_003: {jobnet_id} DWH 内データ取り込み処理が正常終了しました。")
    except Exception as e:
        logging.error(f"E_DWH_JB_DB_DATA_COPY_002: {jobnet_id} 例外発生しました。(登録先スキーマ: {target_schema}; 登録先テーブル: {target_table}; スタックトレース: {str(e)})")
        sys.exit(1)  # 異常発生時に Glue Job を異常終了させる
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
