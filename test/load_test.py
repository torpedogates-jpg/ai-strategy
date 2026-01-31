import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from util import load_kline, load_parquet
from sma import load_ticker_set_sma


def test_load_kline():
    """Test load_kline for SUIUSDT, year 2025"""
    df = load_kline(
        market="spot",
        timeframe="1m",
        years=[2025],
        symbols=["SUIUSDT"]
    )
    print(f"load_kline shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(df.head())
    return df


def test_load_parquet():
    """Test load_parquet for SUIUSDT, year 2025"""
    df = load_parquet(
        market="spot",
        symbol="SUIUSDT",
        data_type="aggTrades",
        years=[2025]
    )
    print(f"load_parquet shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(df.head())
    return df


def test_load_ticker_set_sma():
    """Test load_ticker_set_sma for SUIUSDT, year 2025"""
    df = load_ticker_set_sma(
        market="spot",
        ticker="1m",
        years=[2025],
        symbols=["SUIUSDT"]
    )
    print(f"load_ticker_set_sma shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(df.head())
    return df


if __name__ == "__main__":
    print("=" * 50)
    print("Testing load_kline")
    print("=" * 50)
    test_load_kline()

    print("\n" + "=" * 50)
    print("Testing load_parquet")
    print("=" * 50)
    test_load_parquet()

    print("\n" + "=" * 50)
    print("Testing load_ticker_set_sma")
    print("=" * 50)
    test_load_ticker_set_sma()
