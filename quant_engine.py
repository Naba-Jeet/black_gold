# quant_engine.py
import pandas as pd
import numpy as np
from config import WEIGHTS

def calculate_crack_spreads(wti, rbob, ho):
    merged = pd.merge(wti[['date', 'price']], rbob[['date', 'price']], on='date', how='inner', suffixes=('_wti', '_rbob'))
    merged = pd.merge(merged, ho[['date', 'price']], on='date', how='inner')
    merged.rename(columns={'price': 'price_ho'}, inplace=True)
    
    if merged.empty:
        return pd.DataFrame(columns=['date', 'price_wti', 'price_rbob', 'price_ho', 'crack_spread'])
    
    merged['crack_spread'] = (2 * merged['price_rbob']) + (1 * merged['price_ho']) - (3 * merged['price_wti'])
    return merged

def calculate_z_score(brent_spr):
    price = brent_spr['price']
    std = price.std()
    if std == 0 or pd.isna(std):
        return 0  # No volatility = neutral
    return (price.iloc[-1] - price.mean()) / std

def detect_liquidity_sweeps(df):
    df = df.copy()
    df['low_20'] = df['low'].rolling(window=20).min().shift(1)
    df['sweep'] = (df['low'] < df['low_20']) & (df['price'] > df['low_20']) & (df['vol'] > df['vol'].rolling(20).mean())
    return df

def detect_liquidity_sweeps_v2(df):
    df = df.copy()
    df['low_20'] = df['low'].rolling(window=20).min().shift(1)
    df['vol_ma_20'] = df['vol'].rolling(20).mean()
    df['vol_ratio'] = df['vol'] / df['vol_ma_20']
    
    # Sweep with volume confirmation
    df['sweep'] = (
        (df['low'] < df['low_20']) &           # Break recent low
        (df['price'] > df['low_20']) &          # Reversal
        (df['vol_ratio'] > 1.5) &               # Volume spike (150% of avg)
        (df['price'] > df['open'])              # Close in upper half (buying)
    )
    return df

def calculate_inventory_shock(stocks_df):
    """
    Calculates the difference between Actual and Forecast.
    Returns a dictionary to provide both raw values and formatted strings.
    """
    def clean_m(val):
        if isinstance(val, str):
            return float(val.replace('M', '')) * 1_000_000
        if pd.notnull(val):
            return float(val) * 1_000_000
        return 0.0

    try:
        actual = clean_m(stocks_df['actual'].iloc[-1])
        forecast = clean_m(stocks_df['forecast'].iloc[-1])
        shock = actual - forecast

        return {
            "value": shock,
            "unit": "BBL",
            "abs_m": shock / 1_000_000
        }
    except Exception as e:
        return {"value": 0.0, "unit": "BBL", "abs_m": 0.0}

def calculate_inv_momentum(stocks_df):
    """4-Week Moving Average of changes. Negative = Structural Draw"""
    def clean_m(val):
        if isinstance(val, str): return float(val.replace('M', ''))
        return float(val) if pd.notnull(val) else 0

    stocks_df['clean_actual'] = stocks_df['actual'].apply(clean_m)
    momentum = stocks_df['clean_actual'].tail(4).mean()
    return momentum

def calculate_rsi(df, period=14):
    df = df.copy()
    delta = df['price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rs = rs.replace([np.inf, -np.inf], np.nan)  # Handle division edge cases
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.fillna(50)  # Fill NaN with neutral value
    return rsi

def calculate_vwap(df, window=20):
    v = df['vol']
    p = (df['high'] + df['low'] + df['price']) / 3
    pv = p * v
    vol_sum = v.rolling(window=window).sum()
    vol_sum = vol_sum.replace(0, np.nan)  # Avoid division by zero
    vwap = pv.rolling(window=window).sum() / vol_sum
    return vwap.fillna(df['price']) 

def calculate_sr_levels(df, window=10):
    """Detects Support and Resistance based on local swing highs/lows."""
    res = df['high'].rolling(window=window).max().iloc[-1]
    sup = df['low'].rolling(window=window).min().iloc[-1]
    return sup, res

def generate_convergence_verdict(current_crack, crack_mean, z_score, net_pos, inv_shock, inv_mom, rsi):
    score = 0
    reasons = []

    if current_crack > crack_mean:
        score += WEIGHTS["crack_spread"]; reasons.append("Refinery demand is high (Crack Spread Expanding)")

    if z_score > 1.5:
        score += WEIGHTS["z_score"]; reasons.append("WTI undervalued vs Brent (Arbitrage Force)")

    if net_pos < -10000:
        score += WEIGHTS["cot_squeeze"]; reasons.append("Hedge funds trapped in shorts (Short Squeeze)")

    if inv_shock < 0:
        score += WEIGHTS["inv_surprise"]; reasons.append("Inventory Surprise: Actual < Forecast (Bullish Shock)")

    if inv_mom < 0:
        score += WEIGHTS["inv_momentum"]; reasons.append("Structural Draw: 4-week inventory trend is bearish")

    if rsi.iloc[-1] > 70:
        score += WEIGHTS["price_overext"]; reasons.append("Warning: Price is Overbought (RSI > 70)")
    elif rsi.iloc[-1] < 30:
        score -= WEIGHTS["price_overext"]; reasons.append("Warning: Price is Oversold (RSI < 30)")

    verdict = "BULLISH" if score >= 6 else "NEUTRAL" if score >= 3 else "BEARISH"
    return verdict, reasons

def generate_detailed_force_matrix(current_crack, crack_mean, z_score, net_pos, inv_shock, inv_mom, rsi):
    """Returns a detailed breakdown of each force's contribution."""
    matrix = {}

        # Validate inputs
    if pd.isna(z_score): z_score = 0
    if pd.isna(current_crack) or pd.isna(crack_mean): current_crack, crack_mean = 0, 0
    if pd.isna(net_pos): net_pos = 0
    if pd.isna(inv_shock): inv_shock = 0
    if pd.isna(inv_mom): inv_mom = 0
    if rsi.empty or pd.isna(rsi.iloc[-1]): rsi_val = 50 
    else: rsi_val = rsi.iloc[-1]

    matrix['Refinery Demand'] = WEIGHTS["crack_spread"] if current_crack > crack_mean else -WEIGHTS["crack_spread"]
    matrix['Global Arbitrage Force'] = WEIGHTS["z_score"] if z_score > 1.5 else 0
    matrix['Short Squeeze'] = WEIGHTS["cot_squeeze"] if net_pos < -10000 else 0
    matrix['Supply Demand Shock'] = WEIGHTS["inv_surprise"] if inv_shock < 0 else -WEIGHTS["inv_surprise"]
    matrix['Inv Momentum'] = WEIGHTS["inv_momentum"] if inv_mom < 0 else -WEIGHTS["inv_momentum"]
    
    matrix['Overextension'] = WEIGHTS["price_overext"] if rsi.iloc[-1] > 80 else 0

    total_score = sum(matrix.values())
    verdict = "BULLISH" if total_score >= 6 else "NEUTRAL" if total_score >= 3 else "BEARISH"

    return matrix, total_score, verdict

def calculate_volume_profile(df, window=20, bins=50):
    """
    Groups volume by price levels
    Returns price levels with highest volume concentration (POC, VAH, VAL)
    """
    recent = df.tail(window)
    recent = df.tail(window)
    if recent.empty or len(recent) < 2:
        return {"poc": 0, "vah": 0, "val": 0, "profile": {}}
    
    price_min = recent['low'].min()
    price_max = recent['high'].max()
    
    # Create price bins
    bins_array = np.linspace(price_min, price_max, bins)
    
    # Distribute volume proportionally to each bar's price range
    volume_profile = {}
    for idx, row in recent.iterrows():
        bar_range = row['high'] - row['low']
        if bar_range == 0:
            mid_price = row['close']
            volume_profile[mid_price] = volume_profile.get(mid_price, 0) + row['vol']
        else:
            # Distribute volume evenly across the bar's price range
            for bin_price in bins_array:
                if row['low'] <= bin_price <= row['high']:
                    volume_profile[bin_price] = volume_profile.get(bin_price, 0) + (row['vol'] / bins)
    
    # Calculate Point of Control (POC), Value Area High/Low
    if not volume_profile:
        return {"poc": 0, "vah": 0, "val": 0, "profile": {}}
    
    poc_price = max(volume_profile, key=volume_profile.get)
    sorted_volumes = sorted(volume_profile.items(), key=lambda x: x[1], reverse=True)
    total_vol = sum(volume_profile.values())
    va_vol = total_vol * 0.70  # 70% of volume
    
    cum_vol = 0
    va_prices = []
    for price, vol in sorted_volumes:
        va_prices.append(price)
        cum_vol += vol
        if cum_vol >= va_vol:
            break
    
    return {
        "poc": poc_price,
        "vah": max(va_prices),
        "val": min(va_prices),
        "profile": volume_profile
    }


def calculate_realized_volatility(wti_df, window=30):
    """
    Calculates realized volatility from historical WTI daily prices.
    
    Formula:
    1. Calculate daily log returns: ln(price_t / price_t-1)
    2. Compute std dev of last N returns (N = window parameter)
    3. Annualize: multiply by √252 (trading days/year)
    
    Args:
        wti_df: DataFrame with 'price' column (daily WTI prices)
        window: Number of days to use for RV calculation (default 30, range 10-60)
    
    Returns: annualized volatility as percentage (0-100)
    """
    if wti_df.empty or len(wti_df) < window + 1:
        return 0.0
    
    # Get last (window + 1) prices to calculate window returns
    recent_prices = wti_df['price'].tail(window + 1)
    
    # Calculate daily log returns
    log_returns = np.log(recent_prices / recent_prices.shift(1)).dropna()
    
    # Standard deviation of returns
    std_dev = log_returns.std()
    
    if pd.isna(std_dev) or std_dev == 0:
        return 0.0
    
    # Annualize: multiply by sqrt(252 trading days)
    annualized_vol = std_dev * np.sqrt(252)
    
    # Convert to percentage
    return annualized_vol * 100


def calculate_vol_premium(wti_df, ovx_df, rv_window=30):
    """
    Calculates the Vol Premium: OVX (implied vol) - RV (realized vol)
    
    Args:
        wti_df: DataFrame with WTI daily prices
        ovx_df: DataFrame with OVX data
        rv_window: Days of price history for RV calculation (default 30)
    
    Returns:
    {
        'vol_premium': float,          # OVX - RV (positive = overpriced)
        'ovx_current': float,          # Current OVX reading
        'rv_window_days': int,         # Window used for RV calculation
        'rv_current': float,           # Realized volatility
        'signal': str,                 # 'EXPENSIVE' | 'CHEAP' | 'NEUTRAL'
        'recommendation': str          # Trading recommendation
    }
    """
    try:
        # Calculate realized volatility with specified window
        rv_current = calculate_realized_volatility(wti_df, window=rv_window)
        
        # Get current OVX
        if ovx_df.empty or len(ovx_df) == 0:
            return {
                'vol_premium': 0,
                'ovx_current': 0,
                'rv_window_days': rv_window,
                'rv_current': rv_current,
                'signal': 'NO_DATA',
                'recommendation': 'Insufficient OVX data'
            }
        
        ovx_current = float(ovx_df['price'].iloc[-1])
        
        # Calculate vol premium
        vol_premium = ovx_current - rv_current
        
        # Determine signal
        if vol_premium > 15:
            signal = 'EXPENSIVE'
            recommendation = 'Options overpriced → Sell volatility (short straddles/calls)'
        elif vol_premium < -10:
            signal = 'CHEAP'
            recommendation = 'Options underpriced → Buy volatility (long straddles/calls)'
        else:
            signal = 'NEUTRAL'
            recommendation = 'No clear vol edge → Use directional trades only'
        
        return {
            'vol_premium': vol_premium,
            'ovx_current': ovx_current,
            'rv_window_days': rv_window,
            'rv_current': rv_current,
            'signal': signal,
            'recommendation': recommendation
        }
    
    except Exception as e:
        return {
            'vol_premium': 0,
            'ovx_current': 0,
            'rv_window_days': rv_window,
            'rv_current': 0,
            'signal': 'ERROR',
            'recommendation': f'Volatility calculation error: {str(e)}'
        }