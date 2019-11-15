from lambdas.persist_curated_dataset_lambda_handler import *


def lambda_handler(event, context):
    LoggerUtility.set_level()
    LoggerUtility.log_info("Batch Id from previous lambda - {}".format(event['batchId']))
    batch_id = event['batchId']
    persist_curated_datasets(event, batch_id)
    return event
