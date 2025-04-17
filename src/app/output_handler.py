"""Module to handle output of Ichimoku analysis to various targets.

This module logs the processed results and prints them to stdout.
"""

import json

from app.logger import setup_logger

# Initialize logger
logger = setup_logger(__name__)


def send_to_output(data: dict[str, any]) -> None:
    """Outputs processed Ichimoku Cloud analysis results.

    Args:
    ----
        data (dict[str, any]): The processed analysis result.

    Returns:
    -------
        None

    """
    try:
        # Format data as indented JSON
        formatted_output = json.dumps(data, indent=4)

        # Log the output
        logger.info("Sending data to output:\n%s", formatted_output)

        # Placeholder output method (e.g., print to console)
        print(formatted_output)

    except Exception as e:
        logger.error(f"Failed to send output: {e}")
