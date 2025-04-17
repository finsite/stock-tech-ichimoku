"""Main entry point for Stock-Tech-Ichimoku.

This script starts the Ichimoku Cloud analysis service by consuming messages
from a configured queue and processing them for analysis output.

Attributes
----------
    logger (logging.Logger): Configured logger for the application.

"""

import os
import sys

# Add 'src/' to Python's module search path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.logger import setup_logger
from app.queue_handler import consume_messages

# Initialize logger
logger = setup_logger(__name__)


def main() -> None:
    """Main entry point of the application.

    This function starts the Ichimoku analysis service by consuming messages
    from the configured message queue and processing them for technical insights.

    Returns
    -------
        None

    """
    logger.info("Starting Ichimoku Cloud Analysis Service...")
    consume_messages()


if __name__ == "__main__":
    main()
