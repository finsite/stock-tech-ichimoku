# """
# Handles message queue consumption for RabbitMQ and SQS.

# This module receives stock data, applies Ichimoku Cloud analysis, and sends processed
# results to the output handler.
# """

# import json
# import os
# import time

# import boto3
# import pika
# from botocore.exceptions import BotoCoreError, NoCredentialsError

# from app.logger import setup_logger
# from app.output_handler import send_to_output
# from app.processor import compute_ichimoku_cloud

# logger = setup_logger(__name__)

# # Queue configuration
# QUEUE_TYPE = os.getenv("QUEUE_TYPE", "rabbitmq").lower()
# RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
# RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "stock_analysis")
# RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "analysis_queue")
# RABBITMQ_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY", "#")
# SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "")
# SQS_REGION = os.getenv("SQS_REGION", "us-east-1")

# # SQS Client (if needed)
# sqs_client = None
# if QUEUE_TYPE == "sqs":
#     try:
#         sqs_client = boto3.client("sqs", region_name=SQS_REGION)
#         logger.info(f"SQS client initialized for region {SQS_REGION}")
#     except (BotoCoreError, NoCredentialsError) as e:
#         logger.error("Failed to initialize SQS client: %s", e)
#         sqs_client = None


# def connect_to_rabbitmq() -> pika.BlockingConnection:
#     """
#     Establishes a connection to RabbitMQ. This function will retry up to 5 times if the
#     connection fails, waiting 5 seconds between each attempt.

#     Returns
#     -------
#         pika.BlockingConnection: The established RabbitMQ connection.

#     Raises
#     ------
#         ConnectionError: If the connection cannot be established after retries.
#     """
#     retries: int = 5
#     while retries > 0:
#         try:
#             conn: pika.BlockingConnection = pika.BlockingConnection(
#                 pika.ConnectionParameters(host=RABBITMQ_HOST)
#             )
#             if conn.is_open:
#                 logger.info("Connected to RabbitMQ")
#                 return conn
#         except Exception as e:
#             retries -= 1
#             logger.warning("RabbitMQ connection failed: %s. Retrying in 5s...", e)
#             time.sleep(5)
#     raise ConnectionError(f"Could not connect to RabbitMQ after {retries} retries")


# def consume_rabbitmq() -> None:
#     """
#     Consumes messages from RabbitMQ and applies Ichimoku Cloud analysis.

#     The messages are expected to be in JSON format with the following structure:
#     {
#         "symbol": str,
#         "timestamp": int,
#         "source": str,
#         "data": list[dict[str, any]]
#     }

#     The `data` field is a list of dictionaries containing the stock data.
#     Each dictionary should contain the keys "date", "open", "high", "low", "close".

#     Returns
#     -------
#         None
#     """
#     connection: pika.BlockingConnection = connect_to_rabbitmq()
#     channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()

#     # Declare the exchange and queue
#     channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type="topic", durable=True)
#     channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

#     # Bind the queue to the exchange
#     channel.queue_bind(
#         exchange=RABBITMQ_EXCHANGE, queue=RABBITMQ_QUEUE, routing_key=RABBITMQ_ROUTING_KEY
#     )

#     def callback(
#         ch: pika.adapters.blocking_connection.BlockingChannel,
#         method: pika.spec.Basic.Deliver,
#         properties: pika.spec.BasicProperties,
#         body: bytes,
#     ) -> None:
#         """
#         Callback function for the RabbitMQ consumer.

#         This function is called when a message is received from RabbitMQ.

#         Args:
#         ----
#             ch: The channel object.
#             method: Delivery details.
#             properties: Message properties.
#             body: The message body as bytes.

#         Returns:
#         -------
#             None
#         """
#         try:
#             message: dict = json.loads(body)
#             logger.info("Received message: %s", message)

#             df: pd.DataFrame = compute_ichimoku_cloud(pd.DataFrame(message["data"]))
#             result: dict = {
#                 "symbol": message.get("symbol"),
#                 "timestamp": message.get("timestamp"),
#                 "source": "IchimokuCloud",
#                 "analysis": df.to_dict(orient="records"),
#             }

#             send_to_output(result)
#             ch.basic_ack(delivery_tag=method.delivery_tag)
#         except json.JSONDecodeError:
#             logger.error("Invalid JSON: %s", body)
#             ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
#         except Exception as e:
#             logger.error("Error processing message: %s", e)
#             ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

#     # Start consuming messages
#     channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
#     logger.info("Waiting for messages from RabbitMQ...")

#     try:
#         channel.start_consuming()
#     except KeyboardInterrupt:
#         logger.info("Gracefully stopping RabbitMQ consumer...")
#         channel.stop_consuming()
#     finally:
#         connection.close()
#         logger.info("RabbitMQ connection closed.")


# def consume_sqs() -> None:
#     """
#     Consumes messages from an SQS queue.

#     This function continuously polls an SQS queue for messages,
#     processes them using the `compute_ichimoku_cloud` function,
#     and sends the result to the output handler.

#     :return: None
#     """
#     if not sqs_client or not SQS_QUEUE_URL:
#         logger.error("SQS not initialized or missing queue URL.")
#         return

#     logger.info("Polling for SQS messages...")

#     while True:
#         try:
#             # Poll the SQS queue for messages
#             response = sqs_client.receive_message(
#                 QueueUrl=SQS_QUEUE_URL,
#                 MaxNumberOfMessages=10,
#                 WaitTimeSeconds=10,
#             )

#             # Process each message
#             for msg in response.get("Messages", []):
#                 try:
#                     # Load the message body as JSON
#                     body: dict[str, any] = json.loads(msg["Body"])
#                     logger.info("Received SQS message: %s", body)

#                     # Compute the Ichimoku Cloud analysis
#                     df: pd.DataFrame = compute_ichimoku_cloud(pd.DataFrame(body["data"]))
#                     result: dict[str, any] = {
#                         "symbol": body.get("symbol"),
#                         "timestamp": body.get("timestamp"),
#                         "source": "IchimokuCloud",
#                         "analysis": df.to_dict(orient="records"),
#                     }

#                     # Send the result to the output handler
#                     send_to_output(result)

#                     # Delete the message from the queue
#                     sqs_client.delete_message(
#                         QueueUrl=SQS_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"]
#                     )
#                     logger.info("Deleted SQS message: %s", msg["MessageId"])
#                 except Exception as e:
#                     logger.error("Error processing SQS message: %s", e)
#         except Exception as e:
#             logger.error("SQS polling failed: %s", e)
#             time.sleep(5)


# def consume_messages() -> None:
#     """
#     Entry point to consume messages based on QUEUE_TYPE.

#     This function checks the value of the QUEUE_TYPE environment variable and
#     calls the appropriate message consumer function.

#     Parameters
#     ----------
#         None

#     Returns
#     -------
#         None
#     """
#     if QUEUE_TYPE == "rabbitmq":
#         # Consume messages from RabbitMQ
#         consume_rabbitmq()
#     elif QUEUE_TYPE == "sqs":
#         # Consume messages from SQS
#         consume_sqs()
#     else:
#         logger.error("Invalid QUEUE_TYPE specified. Use 'rabbitmq' or 'sqs'.")
"""
Handles message queue consumption for RabbitMQ and SQS.

This module receives stock data, applies Ichimoku Cloud analysis, and sends processed
results to the output handler.
"""

import json
import os
import time
from typing import Any

import boto3
import pandas as pd
import pika
from botocore.exceptions import BotoCoreError, NoCredentialsError
from pika.adapters.blocking_connection import BlockingChannel

from app.logger import setup_logger
from app.output_handler import send_to_output
from app.processor import compute_ichimoku_cloud

logger = setup_logger(__name__)

# Queue configuration
QUEUE_TYPE = os.getenv("QUEUE_TYPE", "rabbitmq").lower()
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "stock_analysis")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "analysis_queue")
RABBITMQ_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY", "#")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "")
SQS_REGION = os.getenv("SQS_REGION", "us-east-1")

# SQS Client (if needed)
sqs_client = None
if QUEUE_TYPE == "sqs":
    try:
        sqs_client = boto3.client("sqs", region_name=SQS_REGION)
        logger.info(f"SQS client initialized for region {SQS_REGION}")
    except (BotoCoreError, NoCredentialsError) as e:
        logger.error("Failed to initialize SQS client: %s", e)
        sqs_client = None


def connect_to_rabbitmq() -> pika.BlockingConnection:
    """
    Establishes a connection to RabbitMQ with retries.

    Returns
    -------
    pika.BlockingConnection
    """
    retries: int = 5
    while retries > 0:
        try:
            conn: pika.BlockingConnection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            if conn.is_open:
                logger.info("Connected to RabbitMQ")
                return conn
        except Exception as e:
            retries -= 1
            logger.warning("RabbitMQ connection failed: %s. Retrying in 5s...", e)
            time.sleep(5)
    raise ConnectionError(f"Could not connect to RabbitMQ after {retries} retries")


def consume_rabbitmq() -> None:
    """Consumes messages from RabbitMQ and applies Ichimoku Cloud analysis."""
    connection = connect_to_rabbitmq()
    channel: BlockingChannel = connection.channel()

    channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.queue_bind(
        exchange=RABBITMQ_EXCHANGE, queue=RABBITMQ_QUEUE, routing_key=RABBITMQ_ROUTING_KEY
    )

    def callback(ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """RabbitMQ message callback."""
        try:
            message: dict[str, Any] = json.loads(body)
            logger.info("Received message: %s", message)

            df: pd.DataFrame = compute_ichimoku_cloud(pd.DataFrame(message["data"]))
            result: dict[str, Any] = {
                "symbol": message.get("symbol"),
                "timestamp": message.get("timestamp"),
                "source": "IchimokuCloud",
                "analysis": df.to_dict(orient="records"),
            }

            send_to_output(result)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            logger.error("Invalid JSON: %s", body)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error("Error processing message: %s", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
    logger.info("Waiting for messages from RabbitMQ...")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Gracefully stopping RabbitMQ consumer...")
        channel.stop_consuming()
    finally:
        connection.close()
        logger.info("RabbitMQ connection closed.")


def consume_sqs() -> None:
    """Consumes messages from an SQS queue."""
    if not sqs_client or not SQS_QUEUE_URL:
        logger.error("SQS not initialized or missing queue URL.")
        return

    logger.info("Polling for SQS messages...")

    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10,
            )

            for msg in response.get("Messages", []):
                try:
                    body: dict[str, Any] = json.loads(msg["Body"])
                    logger.info("Received SQS message: %s", body)

                    df: pd.DataFrame = compute_ichimoku_cloud(pd.DataFrame(body["data"]))
                    result: dict[str, Any] = {
                        "symbol": body.get("symbol"),
                        "timestamp": body.get("timestamp"),
                        "source": "IchimokuCloud",
                        "analysis": df.to_dict(orient="records"),
                    }

                    send_to_output(result)

                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"]
                    )
                    logger.info("Deleted SQS message: %s", msg["MessageId"])
                except Exception as e:
                    logger.error("Error processing SQS message: %s", e)
        except Exception as e:
            logger.error("SQS polling failed: %s", e)
            time.sleep(5)


def consume_messages() -> None:
    """Entry point to consume messages based on QUEUE_TYPE."""
    if QUEUE_TYPE == "rabbitmq":
        consume_rabbitmq()
    elif QUEUE_TYPE == "sqs":
        consume_sqs()
    else:
        logger.error("Invalid QUEUE_TYPE specified. Use 'rabbitmq' or 'sqs'.")
