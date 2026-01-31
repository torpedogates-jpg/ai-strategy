import os
import glob
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional


def remove_old_cache(folder: str, older_than_days: int = 1):
    """
    Remove cache files older than specified days based on timestamp in filename.

    Args:
        folder: Cache folder path
        older_than_days: Remove files with timestamp older than this many days (default: 1)
    """
    if not os.path.exists(folder):
        return

    now = datetime.now()
    threshold = now - timedelta(days=older_than_days)

    for filename in os.listdir(folder):
        if not filename.endswith('.parquet'):
            continue

        # Extract timestamp from filename pattern: {symbol}_{ticker}-{year}.{ts}.parquet
        parts = filename.rsplit('.', 2)  # Split from right: [base, ts, 'parquet']
        if len(parts) >= 3:
            try:
                ts = int(parts[-2])
                file_time = datetime.fromtimestamp(ts)
                if file_time < threshold:
                    file_path = os.path.join(folder, filename)
                    os.remove(file_path)
                    print(f"Removed old cache: {filename}")
            except (ValueError, OSError):
                continue

BASE_DIR = os.getenv('TRADE_DATA', "/trade_data")

def find_latest_file(file_pattern):
    """
    Find the latest file based on end date in filename.
    
    Args:
        file_pattern: Glob pattern to match files
        dir_path: Directory path for error messages
    
    Returns:
        Path to the latest file
    """
    # Find all matching files
    matching_files = glob.glob(file_pattern)
    
    if not matching_files:
        raise FileNotFoundError(f"No files found matching pattern: {file_pattern}")
    
    latest_file = None
    latest_end_date = None
    
    for file_path in matching_files:
        filename = os.path.basename(file_path)
        parts = filename.replace('.parquet', '').split('_')
        
        if len(parts) >= 4:
            end_date_str = parts[-1]
            
            try:
                if len(end_date_str) == 8:  # YYYYMMDD format
                    end_date = datetime.strptime(end_date_str, '%Y%m%d')
                elif len(end_date_str) == 10:  # YYYY-MM-DD format
                    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                else:
                    end_date = datetime.fromisoformat(end_date_str.replace('_', '-'))
                
                if latest_end_date is None or end_date > latest_end_date:
                    latest_end_date = end_date
                    latest_file = file_path
                    
            except ValueError:
                continue
    
    if latest_file is None:
        raise FileNotFoundError(f"Could not find valid data files in: {file_pattern}")
    
    return latest_file

def load_parquet(source: str = "binance", market: str = "future", market_sub: str = "um",
              data_type: str = "kline", symbol: str = None, detail: str = None,
              years: list = None) -> pd.DataFrame:
    """
    Load parquet data from structured directory path.

    Args:
        source: Data source (e.g., "binance")
        market: Market type (e.g., "future", "spot")
        market_sub: Market subtype (e.g., "um", "cm")
        data_type: Type of data (e.g., "klines", "trades")
        symbol: Trading symbol (e.g., "BTCUSDT")
        detail: Timeframe detail (e.g., "1d", "1h", "1m")
        years: List of years to load for aggTrades (e.g., [2024, 2025]). If None, load all files.

    Returns:
        DataFrame containing the loaded data
    """

    if symbol is None:
        raise ValueError("symbol parameter is required")

    # Construct the directory path
    if market == "spot":
        dir_path = os.path.join(BASE_DIR, source, market, data_type, symbol)
    else: # future
        dir_path = os.path.join(BASE_DIR, source, market, market_sub, data_type, symbol)


    if not os.path.exists(dir_path):
        raise FileNotFoundError(f"Directory not found: {dir_path}")

    # Handle aggTrades with years parameter - load and combine all matching files
    if data_type == "aggTrades":
        file_pattern = os.path.join(dir_path, f"{symbol}_{data_type}_*.parquet")
        matching_files = glob.glob(file_pattern)

        if not matching_files:
            raise FileNotFoundError(f"No files found matching pattern: {file_pattern}")

        # Filter by years if specified
        if years is not None:
            years_str = [str(y) for y in years]
            filtered_files = []
            for f in matching_files:
                filename = os.path.basename(f)
                if any(year in filename for year in years_str):
                    filtered_files.append(f)
            matching_files = filtered_files

            if not matching_files:
                raise FileNotFoundError(f"No files found for years {years} in: {dir_path}")

        # Sort files to ensure consistent ordering
        matching_files.sort()

        try:
            dfs = []
            for file_path in matching_files:
                df = pd.read_parquet(file_path)
                print(f"Loaded data from: {file_path}, shape: {df.shape}")
                dfs.append(df)

            # Combine all dataframes
            if len(dfs) == 1:
                df = dfs[0]
            else:
                df = pd.concat(dfs, ignore_index=True)
                print(f"Combined {len(dfs)} files, total shape: {df.shape}")

            return df
        except Exception as e:
            raise RuntimeError(f"Error loading parquet files: {str(e)}")

    # For other data types, use the original logic
    file_name = None
    if data_type == "depth":
        file_name = f"{symbol}_{source}_{market}_*_*.parquet"
    else:
        file_name = f"{symbol}_{data_type}_*_*.parquet" if (data_type == "trades" or data_type == "metrics") else f"{symbol}_{detail}_*_*.parquet"
    file_pattern = os.path.join(dir_path, file_name)

    latest_file = find_latest_file(file_pattern)

    # Load the latest file
    try:
        df = pd.read_parquet(latest_file)
        print(f"Loaded data from: {latest_file}")
        print(f"Data shape: {df.shape}")

        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        rename_dict = {col: col.capitalize() for col in df.columns if col.lower() in ['open', 'high', 'low', 'close', 'volume']}
        df = df.rename(columns=rename_dict)
        return df
    except Exception as e:
        raise RuntimeError(f"Error loading parquet file {latest_file}: {str(e)}")
    



def load_funding_rate(source: str = "binance", symbol: str = None) -> pd.DataFrame:
    """
    Load funding rate data from structured directory path.
    
    Args:
        source: Data source (e.g., "binance")
        symbol: Trading symbol (e.g., "BTCUSDT")
    
    Returns:
        DataFrame containing the funding rate data
    """
    if symbol is None:
        raise ValueError("symbol parameter is required")
    
    # Construct the directory path for funding rate data
    # Assuming funding rate data is stored in a specific structure
    dir_path = os.path.join(BASE_DIR, source, "future", "um", "fundingRate", symbol)
    
    if not os.path.exists(dir_path):
        raise FileNotFoundError(f"Directory not found: {dir_path}")
    
    # Pattern to match files: {symbol}_funding_rate_*_*.parquet
    file_pattern = os.path.join(dir_path, f"{symbol}_fundingRate_*_*.parquet")
    
    latest_file = find_latest_file(file_pattern)
    
    # Load the latest file
    try:
        df = pd.read_parquet(latest_file)
        print(f"Loaded funding rate data from: {latest_file}")
        print(f"Data shape: {df.shape}")
        return df
    except Exception as e:
        raise RuntimeError(f"Error loading funding rate parquet file {latest_file}: {str(e)}")




def load_data_old(symbol: str, type: str = "um"):
    """Legacy function - kept for backward compatibility"""
    return load_data(symbol=symbol, market_sub=type)


def show_all_rows(df: pd.DataFrame):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        display(df)


def load_kline(source: str = "binance", market: str = "spot", market_sub: str = "um",
               timeframe: str = "1m", years: Optional[list] = None, symbols: Optional[list] = None) -> pd.DataFrame:
    """
    Load kline data from aggTrades_kline directory (per-symbol structure).

    Directory structure: aggTrades_kline/{symbol}/{symbol}_kline_{timeframe}_{year}.parquet

    Available timeframes: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 8h, 12h, 1d

    Args:
        source: Data source (default: "binance")
        market: Market type - "spot" or "future" (default: "spot")
        market_sub: Market subtype for futures - "um" or "cm" (default: "um")
        timeframe: Timeframe - 1m, 3m, 5m, 15m, 30m, 1h, 4h, 8h, 12h, 1d (default: "1m")
        years: List of years to load (e.g., [2024, 2025]). None loads all years.
        symbols: List of symbols to load (e.g., ["BTCUSDT", "ETHUSDT"]). Required.

    Returns:
        DataFrame containing the kline data with columns:
        symbol, time, Open, High, Low, Close, qty, qty_usd, buyer_qty, seller_qty,
        avg_price, buyer_avg_price, seller_avg_price

    Example:
        # Load 1m data for 2025, specific symbols
        df = load_kline(market="spot", timeframe="1m", years=[2025], symbols=["AAVEUSDT", "ADAUSDT"])

        # Load 1h data for multiple years
        df = load_kline(market="spot", timeframe="1h", years=[2024, 2025], symbols=["AAVEUSDT"])
    """
    valid_timeframes = ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "1d"]
    if timeframe not in valid_timeframes:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Valid options: {valid_timeframes}")

    if symbols is None or len(symbols) == 0:
        raise ValueError("symbols parameter is required and cannot be empty")

    # Construct directory path
    if market == "spot":
        dir_path = os.path.join(BASE_DIR, source, market, "aggTrades_kline")
    else:  # future
        dir_path = os.path.join(BASE_DIR, source, market, market_sub, "aggTrades_kline")

    if not os.path.exists(dir_path):
        raise FileNotFoundError(f"Directory not found: {dir_path}")

    # Setup cache directory
    cache_dir = os.path.join(dir_path, "_cache")
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    # Remove old cache files
    remove_old_cache(cache_dir, older_than_days=1)

    # Determine years for cache naming
    years_for_cache = years if years is not None else ["all"]

    # Try to load from cache first
    cached_dfs = []
    all_cached = True
    ts = int(datetime.now().timestamp())

    for symbol in symbols:
        cache_pattern = os.path.join(cache_dir, f"{symbol}_{timeframe}-{'_'.join(map(str, years_for_cache))}.*.parquet")
        cached_files = glob.glob(cache_pattern)

        if cached_files:
            cached_files.sort(reverse=True)
            cached_file = cached_files[0]
            df_cached = pd.read_parquet(cached_file)
            print(f"Cache hit: {cached_file}, shape: {df_cached.shape}")
            cached_dfs.append(df_cached)
        else:
            all_cached = False
            break

    if all_cached:
        if len(cached_dfs) == 1:
            df = cached_dfs[0]
        else:
            df = pd.concat(cached_dfs, ignore_index=True)
        df = df.sort_values(['symbol', 'time']).reset_index(drop=True)
        print(f"All {len(symbols)} symbols loaded from cache, total shape: {df.shape}")
        return df

    # Load from source files
    try:
        all_dfs = []

        for symbol in symbols:
            symbol_dir = os.path.join(dir_path, symbol)
            if not os.path.exists(symbol_dir):
                print(f"Warning: Symbol directory not found: {symbol_dir}")
                continue

            # Find matching files for this symbol
            file_pattern = os.path.join(symbol_dir, f"{symbol}_kline_{timeframe}_*.parquet")
            matching_files = glob.glob(file_pattern)

            if not matching_files:
                print(f"Warning: No files found for {symbol} with timeframe {timeframe}")
                continue

            # Filter by years if specified
            if years is not None:
                years_str = [str(y) for y in years]
                filtered_files = []
                for f in matching_files:
                    filename = os.path.basename(f)
                    # Match pattern like "{symbol}_kline_1m_2025.parquet" or "{symbol}_kline_1m_2026-01-23.parquet"
                    if any(f"_{year}." in filename or f"_{year}-" in filename for year in years_str):
                        filtered_files.append(f)
                matching_files = filtered_files

            if not matching_files:
                print(f"Warning: No files found for {symbol} with years {years}")
                continue

            # Sort and load files
            matching_files.sort()
            symbol_dfs = []

            for file_path in matching_files:
                df = pd.read_parquet(file_path)
                print(f"Loaded: {file_path}, shape: {df.shape}")
                symbol_dfs.append(df)

            if symbol_dfs:
                if len(symbol_dfs) == 1:
                    symbol_df = symbol_dfs[0]
                else:
                    symbol_df = pd.concat(symbol_dfs, ignore_index=True)

                # Save to cache
                cache_file = os.path.join(cache_dir, f"{symbol}_{timeframe}-{'_'.join(map(str, years_for_cache))}.{ts}.parquet")
                symbol_df.to_parquet(cache_file, index=False)
                print(f"Cached: {cache_file}, shape: {symbol_df.shape}")

                all_dfs.append(symbol_df)

        if not all_dfs:
            raise FileNotFoundError(f"No data found for symbols {symbols}")

        # Combine all symbol dataframes
        if len(all_dfs) == 1:
            df = all_dfs[0]
        else:
            df = pd.concat(all_dfs, ignore_index=True)
            print(f"Combined {len(all_dfs)} symbols, total shape: {df.shape}")

        # Sort by symbol and time
        df = df.sort_values(['symbol', 'time']).reset_index(drop=True)

        return df

    except Exception as e:
        raise RuntimeError(f"Error loading kline files: {str(e)}")


def load_kline_symbols(source: str = "binance", market: str = "spot", market_sub: str = "um") -> list:
    """
    Get list of all available symbols in aggTrades_kline directory.

    Args:
        source: Data source (default: "binance")
        market: Market type - "spot" or "future" (default: "spot")
        market_sub: Market subtype for futures - "um" or "cm" (default: "um")

    Returns:
        Sorted list of symbol names (e.g., ["AAVEUSDT", "BTCUSDT", ...])

    Example:
        symbols = load_kline_symbols(market="spot")
        print(f"Found {len(symbols)} symbols")
    """
    # Construct directory path
    if market == "spot":
        dir_path = os.path.join(BASE_DIR, source, market, "aggTrades_kline")
    else:  # future
        dir_path = os.path.join(BASE_DIR, source, market, market_sub, "aggTrades_kline")

    if not os.path.exists(dir_path):
        raise FileNotFoundError(f"Directory not found: {dir_path}")

    # Get all subdirectories (each is a symbol), excluding _cache
    symbols = [d for d in os.listdir(dir_path)
               if os.path.isdir(os.path.join(dir_path, d)) and d != '_cache']

    return sorted(symbols)


if __name__ == "__main__":
    # Simple test for load_data using a sample symbol
    try:
        # df = load_parquet(symbol="BTCUSDT", detail="1h")
        # df = load_funding_rate(symbol="BTCUSDT")
        
        # df = load_parquet(market="spot", symbol="SOMIUSDT", data_type="aggTrades")
        # df = load_parquet(market="spot", symbol="SOMIUSDT", data_type="trades")
        # df = load_parquet(market="spot", symbol="MMTUSDT", data_type="aggTrades")
        df = load_parquet(market="future", market_sub="um", symbol="MMTUSDT", data_type="metrics")
        print(df.shape)
    except Exception as e:
        print(f"Test failed: {e}")




