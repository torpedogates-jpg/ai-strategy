import sys
sys.path.append('../..')  # Add backtesting directory for util.py
import pandas as pd
from util import load_kline


def load_ticker_set_sma(source: str = "binance", market: str = "spot", market_sub: str = "um",
                        ticker: str = "1m", years: list = None, symbols: list = None) -> pd.DataFrame:
    """
    Load ticker data and add 7, 25, 99 period moving averages.

    Args:
        source: Data source (default: "binance")
        market: Market type - "spot" or "future" (default: "spot")
        market_sub: Market subtype for futures - "um" or "cm" (default: "um")
        ticker: Timeframe - 1m, 3m, 5m, 15m, 30m, 1h, 4h, 8h, 12h, 1d (default: "1m")
        years: List of years to load (e.g., [2024, 2025]). None loads all years.
        symbols: List of symbols to filter (e.g., ["BTCUSDT", "ETHUSDT"]). None loads all symbols.

    Returns:
        DataFrame with additional columns: ma7, ma25, ma99
    """
    df = load_kline(source=source, market=market, market_sub=market_sub,
                    timeframe=ticker, years=years, symbols=symbols)

    # Calculate moving averages per symbol
    df['ma7'] = df.groupby('symbol')['Close'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    df['ma25'] = df.groupby('symbol')['Close'].transform(lambda x: x.rolling(window=25, min_periods=1).mean())
    df['ma99'] = df.groupby('symbol')['Close'].transform(lambda x: x.rolling(window=99, min_periods=1).mean())

    return df


if __name__ == "__main__":
    # Test the function
    df = load_ticker_set_sma(market="spot", ticker="1m", years=[2025], symbols=["FORMUSDT"])
    print(df.head(20))
    print(f"\nShape: {df.shape}")
    print(f"\nColumns: {df.columns.tolist()}")
