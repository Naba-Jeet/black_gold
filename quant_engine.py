# quant_engine.py
import pandas as pd
import numpy as np
from config import WEIGHTS

def calculate_crack_spreads(wti, rbob, ho):
    merged = pd.merge(wti[['Date', 'Price']], rbob[['Date', 'Price']], on='Date', suffixes=('_wti', '_rbob'))
    merged = pd.merge(merged, ho[['Date', 'Price']], on='Date')
    merged.rename(columns={'Price': 'Price_ho'}, inplace=True)
    merged['crack_spread'] = (2 * merged['Price_rbob']) + (1 * merged['Price_ho']) - (3 * merged['Price_wti'])
    return merged

def calculate_z_score(brent_spr):
    price = brent_spr['Price']
    return (price.iloc[-1] - price.mean()) / price.std()

def detect_liquidity_sweeps(df):
    df = df.copy()
    df['low_20'] = df['Low'].rolling(window=20).min().shift(1)
    df['sweep'] = (df['Low'] < df['low_20']) & (df['Price'] > df['low_20']) & (df['Vol.'] > df['Vol.'].rolling(20).mean())
    return df

def calculate_inventory_shock(stocks_df):
    """
    Calculates the difference between Actual and Forecast.
    Returns a dictionary to provide both raw values and formatted strings.
    """
    def clean_m(val):
        if isinstance(val, str): 
            # Handle cases like '10.263M' or '2.300M'
            return float(val.replace('M', '')) * 1_000_000
        if pd.notnull(val):
            return float(val) * 1_000_000
        return 0.0

    try:
        # Ensure we are working with the latest row
        actual = clean_m(stocks_df['Actual'].iloc[-1])
        forecast = clean_m(stocks_df['Forecast'].iloc[-1])
        shock = actual - forecast
        
        return {
            "value": shock, 
            "unit": "BBL", 
            "abs_m": shock / 1_000_000
        }
    except Exception as e:
        # Fallback in case of data corruption
        return {"value": 0.0, "unit": "BBL", "abs_m": 0.0}

def calculate_inv_momentum(stocks_df):
    """4-Week Moving Average of changes. Negative = Structural Draw"""
    def clean_m(val):
        if isinstance(val, str): return float(val.replace('M', ''))
        return float(val) if pd.notnull(val) else 0
    
    stocks_df['clean_actual'] = stocks_df['Actual'].apply(clean_m)
    momentum = stocks_df['clean_actual'].tail(4).mean()
    return momentum

def calculate_rsi(df, period=14):
    delta = df['Price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_vwap(df, window=20):
    """Calculates Rolling VWAP for the specified window."""
    v = df['Vol.']
    p = (df['High'] + df['Low'] + df['Price']) / 3
    # Rolling VWAP logic: sum(P*V) / sum(V) over window
    pv = p * v
    return pv.rolling(window=window).sum() / v.rolling(window=window).sum()

def calculate_sr_levels(df, window=10):
    """Detects Support and Resistance based on local swing highs/lows."""
    # Resistance: Local Maxima
    res = df['High'].rolling(window=window).max().iloc[-1]
    # Support: Local Minima
    sup = df['Low'].rolling(window=window).min().iloc[-1]
    return sup, res

def generate_convergence_verdict(current_crack, crack_mean, z_score, net_pos, inv_shock, inv_mom, rsi):
    score = 0
    reasons = []
    
    # 1. Refinery Demand
    if current_crack > crack_mean: 
        score += WEIGHTS["crack_spread"]; reasons.append("Refinery demand is high (Crack Spread Expanding)")
    
    # 2. Arbitrage
    if z_score > 1.5: 
        score += WEIGHTS["z_score"]; reasons.append("WTI undervalued vs Brent (Arbitrage Force)")
    
    # 3. Squeeze
    if net_pos < -10000: 
        score += WEIGHTS["cot_squeeze"]; reasons.append("Hedge funds trapped in shorts (Short Squeeze)")
    
    # 4. Inventory Shock (Bullish if Actual < Forecast)
    if inv_shock < 0: 
        score += WEIGHTS["inv_surprise"]; reasons.append("Inventory Surprise: Actual < Forecast (Bullish Shock)")
        
    # 5. Structural Momentum (Bullish if 4-week avg is negative)
    if inv_mom < 0: 
        score += WEIGHTS["inv_momentum"]; reasons.append("Structural Draw: 4-week inventory trend is bearish")

    # 6. Overextension Penalty
    if rsi.iloc[-1] > 70: 
        score += WEIGHTS["price_overext"]; reasons.append("Warning: Price is Overbought (RSI > 70)")
    elif rsi.iloc[-1] < 30:
        score -= WEIGHTS["price_overext"]; reasons.append("Warning: Price is Oversold (RSI < 30)")
        
    verdict = "BULLISH" if score >= 6 else "NEUTRAL" if score >= 3 else "BEARISH"
    return verdict, reasons

def generate_detailed_force_matrix(current_crack, crack_mean, z_score, net_pos, inv_shock, inv_mom, rsi):
    """Returns a detailed breakdown of each force's contribution."""
    matrix = {}
    
    # 1. Refinery Demand
    matrix['Refinery Demand'] = WEIGHTS["crack_spread"] if current_crack > crack_mean else -WEIGHTS["crack_spread"]
    
    # 2. Arbitrage
    matrix['Global Arb'] = WEIGHTS["z_score"] if z_score > 1.5 else 0
    
    # 3. Squeeze
    matrix['Short Squeeze'] = WEIGHTS["cot_squeeze"] if net_pos < -10000 else 0
    
    # 4. Inventory Shock
    matrix['S&D Shock'] = WEIGHTS["inv_surprise"] if inv_shock < 0 else -WEIGHTS["inv_surprise"]
    
    # 5. Structural Trend
    matrix['Inv Momentum'] = WEIGHTS["inv_momentum"] if inv_mom < 0 else -WEIGHTS["inv_momentum"]
    
    # 6. Price Overextension (Penalty)
    matrix['Overextension'] = WEIGHTS["price_overext"] if rsi.iloc[-1] > 70 else 0
    
    total_score = sum(matrix.values())
    verdict = "BULLISH" if total_score >= 6 else "NEUTRAL" if total_score >= 3 else "BEARISH"
    
    return matrix, total_score, verdict