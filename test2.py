import sys
import json
import logging
import psycopg2
from awsglue.utils import getResolvedOptions
import boto3


class GlueLogger:
    """Glue Job 专用的日志类"""

    def __init__(self, config_path=None, default_level=logging.INFO):
        self.logger = logging.getLogger("glue_job_logger")
        self.logger.setLevel(default_level)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def info(self, key, message=""):
        self.logger.info(f"{key} - {message}")

    def debug(self, key, message=""):
        self.logger.debug(f"{key} - {message}")

    def error(self, key, message=""):
        self.logger.error(f"{key} - {message}")


def get_secret(secret_name):
    """ 从 AWS Secrets Manager 获取 Redshift 连接信息 """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_redshift_connection(secret_dict):
    """ 连接 Redshift 并返回连接对象 """
    conn = psycopg2.connect(
        dbname=secret_dict["dbname"],
        user=secret_dict["username"],
        password=secret_dict["password"],
        host=secret_dict["host"],
        port=secret_dict["port"]
    )
    return conn


def run_queries(conn, queries):
    """ 执行 SQL 语句 """
    with conn.cursor() as cursor:
        for query in queries:
            cursor.execute(query)
        conn.commit()


def main():
    """
    Glue Job 的主处理逻辑：
    1. 获取 Glue Job 参数
    2. 从 Secrets Manager 获取 Redshift 连接信息
    3. 连接 Redshift
    4. 根据 SCHEMA_NO 选择不同的 SQL 语句
    5. 执行 SQL 语句
    """

    # 解析 Glue Job 参数
    args = getResolvedOptions(
        sys.argv,
        [
            'SECRET_NAME',
            'SCHEMA_NO',
            'RAW_SCHEMA',
            'RAW_TABLE',
            'STORAGE_SCHEMA',
            'STORAGE_TABLE',
            'ANALYSIS_SCHEMA',
            'ANALYSIS_TABLE'
        ]
    )

    secret_name = args['SECRET_NAME']
    schema_no = int(args['SCHEMA_NO'])
    raw_schema = args['RAW_SCHEMA']
    raw_table = args['RAW_TABLE']
    storage_schema = args['STORAGE_SCHEMA']
    storage_table = args['STORAGE_TABLE']
    analysis_schema = args['ANALYSIS_SCHEMA']
    analysis_table = args['ANALYSIS_TABLE']

    # 初始化日志
    logger = GlueLogger()

    # 记录 Glue Job 开始日志
    logger.info("INFO.job_start", "DWH_DB_DB_DATA_COPY Job 开始执行")

    # 1. 从 Secrets Manager 获取 Redshift 连接信息
    logger.debug("DEBUG.fetch_secret", secret_name)
    secret_dict = get_secret(secret_name)

    # 2. 获取 Redshift 连接
    logger.debug("DEBUG.connect_redshift")
    conn = get_redshift_connection(secret_dict)
    logger.info("INFO.connect_redshift_success", "Redshift 连接成功")

    # 3. 根据 SCHEMA_NO 选择 SQL 逻辑
    if schema_no == 1:
        # 蓄积层：从 RAW 复制到 STORAGE
        truncate_sql = f"TRUNCATE TABLE {storage_schema}.{storage_table}"
        insert_sql = f"INSERT INTO {storage_schema}.{storage_table} SELECT * FROM {raw_schema}.{raw_table}"
        logger.info("INFO.schema_selection", "SCHEMA_NO=1: RAW -> STORAGE")
    elif schema_no == 2:
        # 利用层：从 STORAGE 复制到 ANALYSIS
        truncate_sql = f"TRUNCATE TABLE {analysis_schema}.{analysis_table}"
        insert_sql = f"INSERT INTO {analysis_schema}.{analysis_table} SELECT * FROM {storage_schema}.{storage_table}"
        logger.info("INFO.schema_selection", "SCHEMA_NO=2: STORAGE -> ANALYSIS")
    else:
        logger.error("ERROR.invalid_schema_no", f"无效的 SCHEMA_NO: {schema_no}")
        raise ValueError(f"无效的 SCHEMA_NO: {schema_no}")

    queries_to_run = [truncate_sql, insert_sql]

    # 4. 执行 SQL 语句
    try:
        logger.info("INFO.data_copy_start", "开始数据复制")
        run_queries(conn, queries_to_run)
        logger.info("INFO.data_copy_end", "数据复制完成")
    except Exception as e:
        logger.error("ERROR.data_copy_fail", str(e))
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("INFO.connection_closed", "Redshift 连接已关闭")

    # 记录 Glue Job 结束日志
    logger.info("INFO.job_end", "DWH_DB_DB_DATA_COPY Job 执行完成")


if __name__ == "__main__":
    main()
