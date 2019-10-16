import os
import uuid
from base64 import *

import boto3

from common.logger_utility import LoggerUtility
from common.redshift import RedshiftManager, RedshiftConnection
from common.template_loader import TemplateLoader

FUNCTION_LOGIC = os.environ['FUNCTION_LOGIC']
REDSHIFT_SQL_DIR = os.path.join('/tmp/', 'redshift_sql')
CONFIG_BUCKET = os.environ['CONFIG_BUCKET']
SQL_KEY_PREFIX = os.environ['SQL_KEY_PREFIX']

if not os.path.exists(REDSHIFT_SQL_DIR):
    os.mkdir(REDSHIFT_SQL_DIR)


def __persist_records_to_redshift(manifest_s3key_name, table_name, sql_file_name, batch_id, is_historical):
    """

    :param manifest_s3key_name:
    :param table_name:
    :param sql_file_name:
    :param batch_id:
    :param is_historical:
    :return:
    """
    try:

        s3_resource = boto3.resource('s3')  # region_name='us-east-1'
        LoggerUtility.log_info("Started persistence for table_name - {}".format(table_name))
        curated_bucket = os.environ['CURATED_BUCKET_NAME']
        LoggerUtility.log_info("Manifest s3 key = {}".format(manifest_s3key_name))
        redshift_manager = __make_redshift_manager()
        # Download the file from S3 to REDSHIFT_SQL_DIR path
        query_file_temp_name = str(uuid.uuid4()) + sql_file_name
        s3_resource.Bucket(CONFIG_BUCKET).download_file(SQL_KEY_PREFIX + "/" + sql_file_name,
                                                        REDSHIFT_SQL_DIR + "/" + query_file_temp_name)
        LoggerUtility.log_info("Downloaded file from S3 - {}".format(query_file_temp_name))
        dw_schema_name = "dw_waze"
        elt_schema_name = "elt_waze"

        if is_historical:
            dw_schema_name = "dw_waze_history"
            elt_schema_name = "elt_waze_history"

        redshift_manager.execute_from_file(query_file_temp_name,
                                           curated_bucket_name=curated_bucket,
                                           manifest_curated_key=manifest_s3key_name,
                                           batchIdValue=batch_id,
                                           dw_schema_name=dw_schema_name,
                                           elt_schema_name=elt_schema_name)
        # delete the file once executed
        os.remove(REDSHIFT_SQL_DIR + "/" + query_file_temp_name)
    except Exception as e:
        LoggerUtility.log_info("Failed to persist curated data to redshift for table "
                               "name - {} with exception - {}".format(table_name, e))
        raise


def __make_redshift_manager():
    encrypted_redshift_master_password = os.environ['REDSHIFT_MASTER_PASSWORD']
    decrypted_redshift_master_password = boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(encrypted_redshift_master_password))['Plaintext'].decode('utf-8')

    redshift_connection = RedshiftConnection(
        os.environ['REDSHIFT_MASTER_USERNAME'],
        decrypted_redshift_master_password,
        os.environ['REDSHIFT_JDBC_URL']
    )
    query_loader = TemplateLoader(REDSHIFT_SQL_DIR)
    return RedshiftManager(
        region_name='us-east-1',
        redshift_role_arn=os.environ['REDSHIFT_ROLE_ARN'],
        redshift_connection=redshift_connection,
        query_loader=query_loader
    )


def persist_curated_datasets(event, batch_id):
    LoggerUtility.set_level()

    table_name = event["tablename"]
    manifest_url_parameter = table_name + "url"
    manifest_url = event[manifest_url_parameter]
    is_historical = event["is_historical"] == 'true'
    sql_file_name = FUNCTION_LOGIC + "_" + table_name + ".sql"
    __persist_records_to_redshift(manifest_url, table_name, sql_file_name, batch_id, is_historical)
