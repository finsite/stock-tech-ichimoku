"""Module to process stock data using Ichimoku Cloud analysis.

The Ichimoku Cloud is a set of technical indicators used to gauge momentum,
as well as to identify trend direction and potential buy/sell signals.

This module provides the `compute_ichimoku_cloud` function to compute the
Ichimoku Cloud indicators for a given DataFrame of stock data.
"""

import pandas as pd

from app.logger import setup_logger
from app.output_handler import send_to_output
# Initialize logger
logger = setup_logger(__name__)


def compute_ichimoku_cloud(df: pd.DataFrame) -> pd.DataFrame:
    """Computes Ichimoku Cloud indicators and adds them to the DataFrame.

    Args:
    ----
        df (pd.DataFrame): DataFrame containing 'High', 'Low', and 'Close' prices.

    Returns:
    -------
        pd.DataFrame: DataFrame with Ichimoku Cloud indicators added.

    """
    try:
        # Validate input
        for col in ["High", "Low", "Close"]:
            if col not in df.columns:
                logger.error(f"Missing column '{col}' in input data.")
                return pd.DataFrame()

        # Tenkan-sen (Conversion Line): 9-period high+low average
        df["tenkan_sen"] = (
            df["High"].rolling(window=9).max() + df["Low"].rolling(window=9).min()
        ) / 2

        # Kijun-sen (Base Line): 26-period high+low average
        df["kijun_sen"] = (
            df["High"].rolling(window=26).max() + df["Low"].rolling(window=26).min()
        ) / 2

        # Senkou Span A (Leading Span A): (Tenkan-sen + Kijun-sen)/2, shifted 26 periods ahead
        df["senkou_span_a"] = ((df["tenkan_sen"] + df["kijun_sen"]) / 2).shift(26)

        # Senkou Span B (Leading Span B): 52-period high+low average, shifted 26 periods ahead
        df["senkou_span_b"] = (
            (df["High"].rolling(window=52).max() + df["Low"].rolling(window=52).min()) / 2
        ).shift(26)

        # Chikou Span (Lagging Span): Close shifted 26 periods back
        df["chikou_span"] = df["Close"].shift(-26)

        logger.info("Ichimoku Cloud indicators calculated successfully.")
        return df

    except Exception as e:
        logger.error(f"Error computing Ichimoku Cloud: {e}")
        return pd.DataFrame()
