"""Module for calculating Ichimoku Cloud components.

This module provides functionality to compute:
- Tenkan-sen (Conversion Line)
- Kijun-sen (Base Line)
- Senkou Span A (Leading Span A)
- Senkou Span B (Leading Span B)
- Chikou Span (Lagging Span)

These indicators are useful for trend direction, support/resistance levels,
and potential buy/sell signals.
"""

import pandas as pd


def calculate_ichimoku(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate Ichimoku Cloud indicators for the given stock data.

    Args:
    ----
        data (pd.DataFrame): A DataFrame containing 'High', 'Low', and 'Close' columns.

    Returns:
    -------
        pd.DataFrame: The input DataFrame with Ichimoku indicator columns added.

    """
    high = data["High"]
    low = data["Low"]
    close = data["Close"]

    # Tenkan-sen (Conversion Line)
    data["tenkan_sen"] = (high.rolling(window=9).max() + low.rolling(window=9).min()) / 2

    # Kijun-sen (Base Line)
    data["kijun_sen"] = (high.rolling(window=26).max() + low.rolling(window=26).min()) / 2

    # Senkou Span A (Leading Span A)
    data["senkou_span_a"] = ((data["tenkan_sen"] + data["kijun_sen"]) / 2).shift(26)

    # Senkou Span B (Leading Span B)
    data["senkou_span_b"] = (
        (high.rolling(window=52).max() + low.rolling(window=52).min()) / 2
    ).shift(26)

    # Chikou Span (Lagging Span)
    data["chikou_span"] = close.shift(-26)

    return data
