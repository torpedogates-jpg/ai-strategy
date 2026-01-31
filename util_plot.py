import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def plot_ohlcv(df, time_col='time', go_to_time=None, ticker='1m', symbol=None,
               data_bars=120, show_bars=30):
    """
    Plot OHLCV candlestick chart with MAs and volume using Plotly FigureWidget.
    Auto-rescales Y-axis when panning.

    Parameters:
    - df: DataFrame with OHLCV data (must have: time, Open, High, Low, Close, and optionally volume columns)
    - time_col: Name of the time column (default: 'time')
    - go_to_time: str like '2025-12-30' or '2025-12-19 09:26:00' to center the view
    - ticker: Timeframe string for calculating time deltas ('1m', '1h', '1d', etc.)
    - symbol: Symbol name for title (optional)
    - data_bars: number of bars to load on each side (default 120)
    - show_bars: number of bars to show on each side initially (default 30)

    Returns:
        Plotly FigureWidget
    """
    # Parse ticker to get timedelta
    ticker_map = {
        '1m': pd.Timedelta(minutes=1),
        '3m': pd.Timedelta(minutes=3),
        '5m': pd.Timedelta(minutes=5),
        '15m': pd.Timedelta(minutes=15),
        '30m': pd.Timedelta(minutes=30),
        '1h': pd.Timedelta(hours=1),
        '4h': pd.Timedelta(hours=4),
        '8h': pd.Timedelta(hours=8),
        '12h': pd.Timedelta(hours=12),
        '1d': pd.Timedelta(days=1),
    }
    td = ticker_map.get(ticker, pd.Timedelta(minutes=1))

    # Prepare dataframe with standardized column names
    df_plot = df.copy()

    # Standardize column names (lowercase)
    col_map = {}
    for col in df_plot.columns:
        if col.lower() == 'open':
            col_map[col] = 'open'
        elif col.lower() == 'high':
            col_map[col] = 'high'
        elif col.lower() == 'low':
            col_map[col] = 'low'
        elif col.lower() == 'close':
            col_map[col] = 'close'
        elif col.lower() in ['volume', 'qty', 'quantity']:
            col_map[col] = 'volume'
    df_plot = df_plot.rename(columns=col_map)

    # Ensure time column is named 'time'
    if time_col != 'time' and time_col in df_plot.columns:
        df_plot = df_plot.rename(columns={time_col: 'time'})

    # Calculate MAs if not present
    if 'ma7' not in df_plot.columns and 'MA7' not in df_plot.columns:
        df_plot['ma7'] = df_plot['close'].rolling(window=7, min_periods=1).mean()
    if 'ma25' not in df_plot.columns and 'MA25' not in df_plot.columns:
        df_plot['ma25'] = df_plot['close'].rolling(window=25, min_periods=1).mean()
    if 'ma99' not in df_plot.columns and 'MA99' not in df_plot.columns:
        df_plot['ma99'] = df_plot['close'].rolling(window=99, min_periods=1).mean()

    # Standardize MA column names
    ma_cols = {}
    for col in df_plot.columns:
        if col.lower() == 'ma7':
            ma_cols[col] = 'MA7'
        elif col.lower() == 'ma25':
            ma_cols[col] = 'MA25'
        elif col.lower() == 'ma99':
            ma_cols[col] = 'MA99'
    df_plot = df_plot.rename(columns=ma_cols)

    # Ensure time column is timezone-naive for consistent comparison
    if df_plot['time'].dt.tz is not None:
        df_plot['time'] = df_plot['time'].dt.tz_localize(None)

    # Set center time
    if go_to_time is None:
        center_time = df_plot['time'].max()
    else:
        center_time = pd.to_datetime(go_to_time)
        # Ensure center_time is also timezone-naive
        if hasattr(center_time, 'tz') and center_time.tz is not None:
            center_time = center_time.tz_localize(None)

    # Data range (larger - for panning)
    data_start = center_time - (td * data_bars)
    data_end = center_time + (td * data_bars)

    # View range (smaller - what's shown initially)
    view_start = center_time - (td * show_bars)
    view_end = center_time + (td * show_bars)

    # Filter data
    df_view = df_plot[(df_plot['time'] >= data_start) & (df_plot['time'] <= data_end)].copy()
    if len(df_view) == 0:
        print(f"No data found around {go_to_time}")
        return None

    # Calculate y-axis range based on visible area only
    df_visible = df_view[(df_view['time'] >= view_start) & (df_view['time'] <= view_end)]
    if len(df_visible) == 0:
        df_visible = df_view

    y_min = df_visible['low'].min() * 0.998
    y_max = df_visible['high'].max() * 1.002
    vol_max = df_visible['volume'].max() * 1.1 if 'volume' in df_visible.columns else 1

    print(f"Loaded {len(df_view)} candles, showing {len(df_visible)} around {go_to_time}")

    # Create Figure (use regular Figure instead of FigureWidget for better compatibility)
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.7, 0.3]
    )

    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df_view['time'],
            open=df_view['open'],
            high=df_view['high'],
            low=df_view['low'],
            close=df_view['close'],
            name='OHLC'
        ),
        row=1, col=1
    )

    # MA7 - Yellow
    if 'MA7' in df_view.columns:
        fig.add_trace(
            go.Scatter(x=df_view['time'], y=df_view['MA7'], mode='lines', name='MA7',
                       line=dict(color='yellow', width=1)),
            row=1, col=1
        )

    # MA25 - Purple
    if 'MA25' in df_view.columns:
        fig.add_trace(
            go.Scatter(x=df_view['time'], y=df_view['MA25'], mode='lines', name='MA25',
                       line=dict(color='purple', width=1)),
            row=1, col=1
        )

    # MA99 - Teal
    if 'MA99' in df_view.columns:
        fig.add_trace(
            go.Scatter(x=df_view['time'], y=df_view['MA99'], mode='lines', name='MA99',
                       line=dict(color='rgb(40, 86, 89)', width=1)),
            row=1, col=1
        )

    # Volume bars
    if 'volume' in df_view.columns:
        colors = ['green' if close >= open else 'red'
                  for close, open in zip(df_view['close'], df_view['open'])]
        fig.add_trace(
            go.Bar(x=df_view['time'], y=df_view['volume'], name='Volume', marker_color=colors),
            row=2, col=1
        )

    # Highlight target time with vertical line
    fig.add_vline(x=center_time, line=dict(color='red', width=2, dash='dash'))

    # Add annotation for target time
    fig.add_annotation(
        x=center_time, y=1.02, yref='paper',
        text=f"Target: {go_to_time}", showarrow=False,
        font=dict(color='red', size=12), bgcolor='rgba(0,0,0,0.5)'
    )

    # Title
    title = f"{symbol + ' ' if symbol else ''}{ticker} OHLCV - {go_to_time}"

    fig.update_layout(
        title=title,
        xaxis_rangeslider_visible=False,
        height=600,
        autosize=True,
        template='plotly_dark',
        xaxis=dict(range=[view_start, view_end]),
        xaxis2=dict(range=[view_start, view_end]),
        yaxis=dict(range=[y_min, y_max]),
        yaxis2=dict(range=[0, vol_max]),
        dragmode='pan',
        margin=dict(l=50, r=50, t=50, b=50)
    )

    return fig
