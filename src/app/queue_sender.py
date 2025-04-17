"""Handles message queue publishing for RabbitMQ and SQS.

This module is used to send analyzed stock data to the appropriate output queue.
"""

import json
import os

import boto3
import pika

from app.logger import setup_logger

logger = setup_logger(__name__)

SQS_CLIENT = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))


def send_to_rabbitmq(data: dict) -> None:
    """Send a message to a RabbitMQ exchange.

    Args:
    ----
        data (dict): The message to send.

    """
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST", "rabbitmq"))
        )
        channel = connection.channel()
        channel.basic_publish(
            exchange=os.getenv("RABBITMQ_EXCHANGE", "stock_analysis"),
            routing_key=os.getenv("RABBITMQ_ROUTING_KEY", "ichimoku"),
            body=json.dumps(data),
        )
        connection.close()
        logger.info("Sent message to RabbitMQ.")
    except Exception as e:
        logger.error(f"Failed to send to RabbitMQ: {e}")


def send_to_sqs(data: dict) -> None:
    """Send a message to an AWS SQS queue.

    Args:
    ----
        data (dict): The message to send.

    """
    try:
        response = SQS_CLIENT.send_message(
            QueueUrl=os.getenv("AWS_SQS_QUEUE_URL", ""),
            MessageBody=json.dumps(data),
        )
        logger.info(f"Sent message to SQS: {response.get('MessageId')}")
    except Exception as e:
        logger.error(f"Failed to send to SQS: {e}")


def publish_to_queue(messages: list[dict]) -> None:
    """Dispatch multiple messages to the appropriate queue.

    Args:
    ----
        messages (list[dict]): List of analysis results to be published.

    """
    queue_type = os.getenv("QUEUE_TYPE", "rabbitmq").lower()

    for message in messages:
        if queue_type == "sqs":
            send_to_sqs(message)
        elif queue_type == "rabbitmq":
            send_to_rabbitmq(message)
        else:
            logger.error("Unsupported QUEUE_TYPE. Must be 'rabbitmq' or 'sqs'.")
