from common.logger_utility import *
from concurrent.futures import ThreadPoolExecutor
import functools
from common.redshift import RedshiftManager, RedshiftConnection
from common.template_loader import TemplateLoader
from root import PROJECT_DIR
import boto3
from boto3.dynamodb.conditions import Attr, Key
from base64 import *

ENCRYPTED_REDSHIFT_MASTER_PASSWORD = os.environ['REDSHIFT_MASTER_PASSWORD']
DECRYPTED_REDSHIFT_MASTER_PASSWORD = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_REDSHIFT_MASTER_PASSWORD))['Plaintext'].decode('utf-8')
FUNCTION_LOGIC = os.environ['FUNCTION_LOGIC']
REDSHIFT_SQL_DIR = os.path.join(PROJECT_DIR, 'redshift_sql')

 

def __persist_records_to_redshift(manifest_s3key_name, table_name, sql_file_name, batch_id):
    try:
        LoggerUtility.logInfo("Started persistence for table_name - {}".format(table_name))
        curated_bucket = os.environ['CURATED_BUCKET_NAME']
        LoggerUtility.logInfo("Manifest s3 key = {}".format(manifest_s3key_name))
        redshift_manager = __make_redshift_manager()
        redshift_manager.execute_from_file(sql_file_name,
                                                   curated_bucket_name=curated_bucket,
                                                   manifest_curated_key=manifest_s3key_name,
                                                   batchIdValue=batch_id)
        
    except Exception as e:
        LoggerUtility.logInfo("Failed to persist curated data to redshift for table "
                              "name - {} with exception - {}".format(table_name, e))
        raise



def __make_redshift_manager():
    redshift_connection = RedshiftConnection(
        os.environ['REDSHIFT_MASTER_USERNAME'],
        DECRYPTED_REDSHIFT_MASTER_PASSWORD,
        os.environ['REDSHIFT_JDBC_URL']
    )
    query_loader = TemplateLoader(REDSHIFT_SQL_DIR)
    return RedshiftManager(
        region_name='us-east-1',
        redshift_role_arn=os.environ['REDSHIFT_ROLE_ARN'],
        redshift_connection=redshift_connection,
        query_loader=query_loader
    )


def persist_curated_datasets(event, context, batch_id):
    LoggerUtility.setLevel()
    
    tableName=event["tablename"]
    maifestUrlParameter=tableName+"url"
    manifestURl=event[maifestUrlParameter]
    sql_file_name=FUNCTION_LOGIC+"_"+tableName+".sql"
    __persist_records_to_redshift(manifestURl,tableName,sql_file_name, batch_id)
