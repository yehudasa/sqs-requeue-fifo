import json
import os
import logging

import boto3
from botocore.exceptions import ClientError


sqs_target = os.environ.get('SQS_TARGET')
num_shards = int(os.environ.get('SQS_NUM_MESSAGE_GROUPS'))


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

sqs = boto3.resource('sqs')


def get_queue(name):
    """
    Gets an SQS queue by name.

    Usage is shown in usage_demo at the end of this module.

    :param name: The name that was used to create the queue.
    :return: A Queue object.
    """
    try:
        queue = sqs.get_queue_by_name(QueueName=name)
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue

def send_message(queue, group_id, dedup_id, message_body, message_attributes=None):
    """
    Send a message to an Amazon SQS queue.

    Usage is shown in usage_demo at the end of this module.

    :param queue: The queue that receives the message.
    :param message_body: The body text of the message.
    :param message_attributes: Custom attributes of the message. These are key-value
                               pairs that can be whatever you want.
    :return: The response from SQS that contains the assigned message ID.
    """
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body,
            MessageGroupId=group_id,
            MessageDeduplicationId=dedup_id,
            MessageAttributes=message_attributes
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response
        

def ceph_str_hash_linux(s):
    hash = 0

    for c in s:
        o = ord(c)
        hash = (hash + (o << 4) + (o >> 4)) * 11

    return hash

def message_group_num(obj_key):
    return ceph_str_hash_linux(obj_key) % num_shards
    

def handle_record(record, queue, context):
    logger.info('## RECORD')
    logger.info(record)

    s3 = record['s3']
    bucket = s3['bucket']['name']
    obj = s3['object']
    obj_key = obj['key']
    
    group_id = str(message_group_num(obj_key))
    
    dedup_id = record['responseElements']['x-amz-id-2']

    rs = json.dumps(record)
    send_message(queue, group_id, dedup_id, rs)


def lambda_handler(event, context):
    # TODO implement
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    
    logger.info('## SQS_TARGET')
    logger.info(sqs_target)
    
    
    queue = get_queue(sqs_target)
    
    logger.info('## EVENT')
    logger.info(event)
    
    for r in event['Records']:
        handle_record(r, queue, context)
        

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }
