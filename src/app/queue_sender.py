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

    This function sends a JSON-serialized message to a RabbitMQ exchange, using
    the specified routing key and exchange name from environment variables.

    Args:
    ----
        data (dict): The message to send.

    Returns:
    -------
        None: No return value.

    Raises:
    ------
        Exception: If the message could not be sent.

    """
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST", "rabbitmq"))
        )
        channel = connection.channel()

        # Publish the message
        channel.basic_publish(
            # Get the exchange name from the environment
            exchange=os.getenv("RABBITMQ_EXCHANGE", "stock_analysis"),
            # Get the routing key from the environment
            routing_key=os.getenv("RABBITMQ_ROUTING_KEY", "ichimoku"),
            # JSON-serialize the message
            body=json.dumps(data),
        )

        # Close the connection
        connection.close()
        logger.info("Sent message to RabbitMQ.")
    except Exception as e:
        logger.error(f"Failed to send to RabbitMQ: {e}")


def send_to_sqs(data: dict[str, any]) -> None:
    """Send a message to an AWS SQS queue.

    This function sends a JSON-serialized message to an AWS SQS queue, using
    the specified queue URL from environment variables.

    Args:
    ----
        data (dict[str, any]): The message to send.

    Returns:
    -------
        None: No return value.

    Raises:
    ------
        Exception: If the message could not be sent.

    """
    try:
        # Get the queue URL from the environment
        queue_url: str = os.getenv("AWS_SQS_QUEUE_URL", "")

        # Send the message
        response: dict[str, any] = SQS_CLIENT.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(data),
        )

        # Log the message ID
        logger.info(f"Sent message to SQS: {response.get('MessageId')}")
    except Exception as e:
        # Log any errors
        logger.error(f"Failed to send to SQS: {e}")


def publish_to_queue(messages: list[dict[str, any]]) -> None:
    """Dispatch multiple messages to the appropriate queue.

    This function takes a list of analysis results and sends them to the
    appropriate message queue. The type of queue is determined by the
    environment variable "QUEUE_TYPE", which should be set to either
    "rabbitmq" or "sqs".

    Args:
    ----
        messages (list[dict[str, any]]): List of analysis results to be published.

    Returns:
    -------
        None

    """
    queue_type: str = os.getenv("QUEUE_TYPE", "rabbitmq").lower()

    for message in messages:
        if queue_type == "sqs":
            send_to_sqs(message)
        elif queue_type == "rabbitmq":
            send_to_rabbitmq(message)
        else:
            logger.error("Unsupported QUEUE_TYPE. Must be 'rabbitmq' or 'sqs'.")
