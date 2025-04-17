"""Module to handle logging for Ichimoku Analysis Service.

This module sets up a logger with a standard format and INFO level.

Args:
----
    name (str): Name of the logger.

Returns:
-------
    logging.Logger: The configured logger.

"""

import logging


def setup_logger(name: str = "ichimoku") -> logging.Logger:
    """Sets up a logger with a standard format and INFO level.

    Args:
    ----
        name (str): Name of the logger.

    Returns:
    -------
        logging.Logger: Configured logger.

    """
    # Create or retrieve a logger with the specified name
    logger: logging.Logger = logging.getLogger(name)

    # Check if the logger already has handlers to avoid duplicate logs
    if not logger.hasHandlers():
        # Create a stream handler to log to the console
        handler: logging.StreamHandler = logging.StreamHandler()
        # Define a logging format
        formatter: logging.Formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        # Set the formatter for the handler
        handler.setFormatter(formatter)
        # Add the handler to the logger
        logger.addHandler(handler)
        # Set the logging level to INFO
        logger.setLevel(logging.INFO)

    return logger
