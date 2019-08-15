import os

os.environ['FUNCTION_LOGIC'] = 'FUNCTION_LOGIC'
os.environ['CONFIG_BUCKET'] = 'CONFIG_BUCKET'
os.environ['SQL_KEY_PREFIX'] = 'SQL_KEY_PREFIX'
os.environ['CURATED_BUCKET_NAME'] = "curated_bucket_name"

import pytest
import boto3
from lambdas import persist_curated_dataset_lambda_handler
from unittest import mock

manifest_s3key_name = "manifest_s3key_name"
table_name = "table_name"
sql_file_name = "sql_file_name"
batch_id = "batch_id"


def Any(cls):
    """
    Checks if a parameter is any of a particular data type.
    :param cls:
    :return:
    """
    class Any(cls):
        def __eq__(self, other):
            return True
    return Any()


def test__make_redshift_manager(monkeypatch):
    """
    Tests that the redshift manager is instantiated correctly.
    :param monkeypatch:
    :return:
    """
    os.environ['REDSHIFT_MASTER_PASSWORD'] = "super secret"
    os.environ['REDSHIFT_MASTER_USERNAME'] = "tester"
    os.environ['REDSHIFT_JDBC_URL'] = "www.url.com"
    os.environ['REDSHIFT_ROLE_ARN'] = "redshift_role_arn"

    class MockBoto3Client:
        def __init__(*args, **kwargs):
            pass

        @staticmethod
        def decrypt(*args, **kwargs):
            return {'Plaintext': os.environ['REDSHIFT_MASTER_PASSWORD'].encode('utf-8')}

    def mock_b64decode(*args, **kwargs):
        pass

    class MockRedshiftConnection:

        def __init__(self, user, password, redshift_jdbc_url):

            self.user = user
            self.password = password
            self.redshift_jdbc_url = redshift_jdbc_url

    class MockRedshiftManager:

        def __init__(self, region_name, redshift_role_arn, redshift_connection, query_loader):

            self.region_name = region_name
            self.redshift_role_arn = redshift_role_arn
            self.redshift_connection = redshift_connection
            self.query_loader = query_loader

    # try overloading TemplateLoader init with monkeypatch and see what happens
    def mock_template_loader(*args, **kwargs):
        return

    monkeypatch.setattr(boto3, "client", MockBoto3Client)

    persist_curated_dataset_lambda_handler.b64decode = mock_b64decode
    persist_curated_dataset_lambda_handler.RedshiftConnection = MockRedshiftConnection
    persist_curated_dataset_lambda_handler.RedshiftManager = MockRedshiftManager
    persist_curated_dataset_lambda_handler.TemplateLoader = mock_template_loader

    redshift_manager = persist_curated_dataset_lambda_handler.__make_redshift_manager()

    assert redshift_manager.region_name == 'us-east-1'
    assert redshift_manager.redshift_role_arn == os.environ['REDSHIFT_ROLE_ARN']
    assert redshift_manager.redshift_connection.user == os.environ['REDSHIFT_MASTER_USERNAME']
    assert redshift_manager.redshift_connection.password == os.environ['REDSHIFT_MASTER_PASSWORD']
    assert redshift_manager.redshift_connection.redshift_jdbc_url == os.environ['REDSHIFT_JDBC_URL']
    assert redshift_manager.query_loader == mock_template_loader()


def test__persist_records_to_redshift_historical():
    """

    :return:
    """

    is_historical = True

    boto3.resource = mock.MagicMock()
    os.remove = mock.MagicMock()

    persist_curated_dataset_lambda_handler.__make_redshift_manager = mock.MagicMock()

    persist_curated_dataset_lambda_handler.__persist_records_to_redshift(
        manifest_s3key_name, table_name, sql_file_name, batch_id, is_historical)

    persist_curated_dataset_lambda_handler.__make_redshift_manager().execute_from_file.assert_called_once_with(
        Any(str),
        batchIdValue='batch_id',
        curated_bucket_name='curated_bucket_name',
        dw_schema_name='dw_waze_history',
        elt_schema_name='elt_waze_history',
        manifest_curated_key='manifest_s3key_name'
    )


def test__persist_records_to_redshift_not_historical():

    is_historical = False

    boto3.resource = mock.MagicMock()
    os.remove = mock.MagicMock()

    persist_curated_dataset_lambda_handler.__make_redshift_manager = mock.MagicMock()

    persist_curated_dataset_lambda_handler.__persist_records_to_redshift(
        manifest_s3key_name, table_name, sql_file_name, batch_id, is_historical)

    persist_curated_dataset_lambda_handler.__make_redshift_manager().execute_from_file.assert_called_once_with(
        Any(str),
        batchIdValue='batch_id',
        curated_bucket_name='curated_bucket_name',
        dw_schema_name='dw_waze',
        elt_schema_name='elt_waze',
        manifest_curated_key='manifest_s3key_name'
    )


def test_persist_curated_datasets():

    event = {
        "tablename": "tablename",
        "is_historical": "true",
        "tablenameurl": "tablenameurl"
    }

    persist_curated_dataset_lambda_handler.__persist_records_to_redshift = mock.MagicMock()

    persist_curated_dataset_lambda_handler.persist_curated_datasets(event, None, batch_id)

    persist_curated_dataset_lambda_handler.__persist_records_to_redshift.assert_called_with(
        "tablenameurl", "tablename", "FUNCTION_LOGIC_tablename.sql", batch_id, True
    )
