"""Queue handler for stock-tech-ichimoku.

Consumes messages from RabbitMQ or SQS, applies Ichimoku Cloud analysis,
and sends results to the output handler.
"""

import json
import time

import boto3
import pandas as pd
import pika
from botocore.exceptions import BotoCoreError, NoCredentialsError
from pika.exceptions import AMQPConnectionError

from app import config
from app.logger import setup_logger
from app.output_handler import send_to_output
from app.processor import compute_ichimoku_cloud

logger = setup_logger(__name__)


def connect_to_rabbitmq() -> pika.BlockingConnection:
    """Connects to RabbitMQ using config-based credentials."""
    retries = 5
    retry_delay = config.get_polling_interval()
    while retries > 0:
        try:
            credentials = pika.PlainCredentials(
                config.get_rabbitmq_user(), config.get_rabbitmq_password()
            )
            parameters = pika.ConnectionParameters(
                host=config.get_rabbitmq_host(),
                port=config.get_rabbitmq_port(),
                virtual_host=config.get_rabbitmq_vhost(),
                credentials=credentials,
                blocked_connection_timeout=30,
            )
            connection = pika.BlockingConnection(parameters)
            if connection.is_open:
                logger.info("Connected to RabbitMQ")
                return connection
        except (AMQPConnectionError, Exception) as e:
            retries -= 1
            logger.warning("RabbitMQ connection failed: %s. Retrying in %ss...", e, retry_delay)
            time.sleep(retry_delay)
    raise ConnectionError("RabbitMQ connection failed after retries")


def consume_rabbitmq() -> None:
    """Consumes messages from RabbitMQ."""
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.exchange_declare(
        exchange=config.get_rabbitmq_exchange(), exchange_type="topic", durable=True
    )
    channel.queue_declare(queue=config.get_rabbitmq_queue(), durable=True)
    channel.queue_bind(
        exchange=config.get_rabbitmq_exchange(),
        queue=config.get_rabbitmq_queue(),
        routing_key=config.get_rabbitmq_routing_key(),
    )

    def callback(ch, method, properties, body: bytes) -> None:
        """

        Parameters
        ----------
        ch :
            param method:
        properties :
            param body: bytes:
        method :
            param body: bytes:
        body :
            bytes:
        body: bytes :


        Returns
        -------

        """
        try:
            message = json.loads(body)
            logger.info("📩 Received message: %s", message)

            df = compute_ichimoku_cloud(pd.DataFrame(message["data"]))
            result = {
                "symbol": message.get("symbol"),
                "timestamp": message.get("timestamp"),
                "source": "IchimokuCloud",
                "analysis": df.to_dict(orient="records"),
            }

            send_to_output(result)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            logger.error("❌ Invalid JSON: %s", body)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error("❌ Error processing message: %s", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=config.get_rabbitmq_queue(), on_message_callback=callback)
    logger.info("📡 Waiting for messages from RabbitMQ...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Gracefully stopping RabbitMQ consumer...")
        channel.stop_consuming()
    finally:
        connection.close()
        logger.info("RabbitMQ connection closed.")


def consume_sqs() -> None:
    """Consumes messages from Amazon SQS."""
    queue_url = config.get_sqs_queue_url()
    region = config.get_sqs_region()
    polling_interval = config.get_polling_interval()
    batch_size = config.get_batch_size()

    try:
        sqs_client = boto3.client("sqs", region_name=region)
    except (BotoCoreError, NoCredentialsError) as e:
        logger.error("Failed to initialize SQS client: %s", e)
        return

    logger.info("📡 Polling for SQS messages...")

    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=batch_size,
                WaitTimeSeconds=10,
            )

            for msg in response.get("Messages", []):
                try:
                    body = json.loads(msg["Body"])
                    logger.info("📩 Received SQS message: %s", body)

                    df = compute_ichimoku_cloud(pd.DataFrame(body["data"]))
                    result = {
                        "symbol": body.get("symbol"),
                        "timestamp": body.get("timestamp"),
                        "source": "IchimokuCloud",
                        "analysis": df.to_dict(orient="records"),
                    }

                    send_to_output(result)

                    sqs_client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
                    )
                    logger.info("✅ Deleted SQS message: %s", msg.get("MessageId"))
                except json.JSONDecodeError:
                    logger.error("❌ Invalid JSON in SQS message: %s", msg["Body"])
                except Exception as e:
                    logger.error("❌ Error processing SQS message: %s", e)
        except Exception as e:
            logger.error("SQS polling failed: %s", e)
            time.sleep(polling_interval)


def consume_messages() -> None:
    """Dispatches to the appropriate queue consumer."""
    queue_type = config.get_queue_type()
    if queue_type == "rabbitmq":
        consume_rabbitmq()
    elif queue_type == "sqs":
        consume_sqs()
    else:
        logger.error("❌ Invalid QUEUE_TYPE specified: %s", queue_type)
