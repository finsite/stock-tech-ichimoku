"""
Module for calculating Ichimoku Cloud components.

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
    """
    Calculate Ichimoku Cloud indicators for the given stock data.

    The Ichimoku Cloud is a set of technical indicators used to gauge momentum,
    as well as to identify trend direction and potential buy/sell signals.

    Ichimoku Cloud components:
    - Tenkan-sen (Conversion Line): a mid-point between the highest high and lowest low
      over the past 9 periods.
    - Kijun-sen (Base Line): a mid-point between the highest high and lowest low over
      the past 26 periods.
    - Senkou Span A (Leading Span A): the average of Tenkan-sen and Kijun-sen, shifted
      forward 26 periods.
    - Senkou Span B (Leading Span B): the average of the highest high and lowest low
      over the past 52 periods, shifted forward 26 periods.
    - Chikou Span (Lagging Span): the current price shifted back 26 periods.

    Args:
    ----
        data (pd.DataFrame): A DataFrame containing 'High', 'Low', and 'Close' columns.

    Returns:
    -------
        pd.DataFrame: The input DataFrame with Ichimoku indicator columns added.
    """
    high: pd.Series = data["High"]
    low: pd.Series = data["Low"]
    close: pd.Series = data["Close"]

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
