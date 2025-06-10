"""Ichimoku Moving Average Utilities.

Calculates Tenkan-sen, Kijun-sen, and Senkou Span B from stock data.
"""

from typing import Any, cast

import pandas as pd
from pandas import DataFrame, Series

from app.utils.setup_logger import setup_logger

logger = setup_logger(__name__)


def analyze(data: dict[str, Any]) -> dict[str, Any]:
    """Main entrypoint for Ichimoku Moving Average analysis.

    Args:
    ----
        data (dict[str, Any]): Input message containing 'symbol', 'timestamp', and 'history'.

    :param data: dict[str:
    :param Any: param data: dict[str:
    :param Any: param data: dict[str:
    :param Any: param data:
    :param Any: param data:
    :param data: dict[str:
    :param data: dict[str:
    :param Any: param data: dict[str:
    :param Any:
    :param data: dict[str:
    :param Any]:

    """
    try:
        df = pd.DataFrame(data.get("history", []))
        symbol = data.get("symbol", "N/A")
        timestamp = data.get("timestamp", "N/A")

        if df.empty or not {"High", "Low"}.issubset(df.columns):
            logger.warning("Missing or invalid history data for symbol: %s", symbol)
            return {
                "symbol": symbol,
                "timestamp": timestamp,
                "error": "Missing or invalid history data",
            }

        components = calculate_ichimoku_components(df)

        result = {
            "symbol": symbol,
            "timestamp": timestamp,
            "source": "IchimokuComponents",
            "components": {k: v.dropna().tolist() for k, v in components.items()},
        }

        logger.info("Processed Ichimoku components for %s at %s", symbol, timestamp)
        return result

    except Exception as e:
        logger.error("Ichimoku component analysis failed: %s", e)
        return {
            "symbol": data.get("symbol", "N/A"),
            "timestamp": data.get("timestamp", "N/A"),
            "error": str(e),
        }


def calculate_ichimoku_components(data: DataFrame) -> dict[str, Series]:
    """Calculate Ichimoku components from historical OHLC stock data.

    Args:
    ----
        data (pd.DataFrame): Historical stock price data.

    :param data: DataFrame:
    :param data: DataFrame:
    :param data: DataFrame:
    :param data: type data: DataFrame :
    :param data: type data: DataFrame :
    :param data: DataFrame:
    :param data: DataFrame:
    :param data: DataFrame:
    :param data: DataFrame:

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
        logger.error("Missing column in input data: %s", e)
        return {}
    except Exception as e:
        logger.error("Error calculating Ichimoku components: %s", e)
        return {}
