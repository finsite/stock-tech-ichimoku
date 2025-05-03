"""
Ichimoku Moving Average Utilities.

Calculates Tenkan-sen, Kijun-sen, and Senkou Span B from stock data.
"""

from typing import cast

import pandas as pd
from pandas import Series

from app.logger import setup_logger

logger = setup_logger(__name__)


def calculate_ichimoku_components(data: pd.DataFrame) -> dict[str, Series]:
    """
    Calculate Ichimoku components from historical OHLC stock data.

    Args:
        data (pd.DataFrame): Historical stock price data.

    Returns:
        dict[str, Series]: Dictionary containing Ichimoku indicators.
    """
    try:
        high_prices = cast(Series, data["High"])
        low_prices = cast(Series, data["Low"])

        tenkan_sen = (high_prices.rolling(window=9).max() + low_prices.rolling(window=9).min()) / 2
        kijun_sen = (high_prices.rolling(window=26).max() + low_prices.rolling(window=26).min()) / 2
        senkou_span_b = (
            high_prices.rolling(window=52).max() + low_prices.rolling(window=52).min()
        ) / 2

        return {
            "TenkanSen": tenkan_sen,
            "KijunSen": kijun_sen,
            "SenkouSpanB": senkou_span_b,
        }

    except KeyError as e:
        logger.error(f"Missing column in input data: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error calculating Ichimoku components: {e}")
        return {}
