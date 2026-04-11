# quant_engine.py
import pandas as pd
import numpy as np
from config import WEIGHTS

def calculate_crack_spreads(wti, rbob, ho):
    merged = pd.merge(wti[['date', 'price']], rbob[['date', 'price']], on='date', suffixes=('_wti', '_rbob'))
    merged = pd.merge(merged, ho[['date', 'price']], on='date')
    merged.rename(columns={'price': 'price_ho'}, inplace=True)
    merged['crack_spread'] = (2 * merged['price_rbob']) + (1 * merged['price_ho']) - (3 * merged['price_wti'])
    return merged

def calculate_z_score(brent_spr):
    price = brent_spr['price']
    return (price.iloc[-1] - price.mean()) / price.std()

def detect_liquidity_sweeps(df):
    df = df.copy()
    df['low_20'] = df['low'].rolling(window=20).min().shift(1)
    df['sweep'] = (df['low'] < df['low_20']) & (df['price'] > df['low_20']) & (df['vol'] > df['vol'].rolling(20).mean())
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
    delta = df['price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_vwap(df, window=20):
    """Calculates Rolling VWAP for the specified window."""
    v = df['vol']
    p = (df['high'] + df['low'] + df['price']) / 3
    pv = p * v
    return pv.rolling(window=window).sum() / v.rolling(window=window).sum()

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

    matrix['Refinery Demand'] = WEIGHTS["crack_spread"] if current_crack > crack_mean else -WEIGHTS["crack_spread"]
    matrix['Global Arb'] = WEIGHTS["z_score"] if z_score > 1.5 else 0
    matrix['Short Squeeze'] = WEIGHTS["cot_squeeze"] if net_pos < -10000 else 0
    matrix['S&D Shock'] = WEIGHTS["inv_surprise"] if inv_shock < 0 else -WEIGHTS["inv_surprise"]
    matrix['Inv Momentum'] = WEIGHTS["inv_momentum"] if inv_mom < 0 else -WEIGHTS["inv_momentum"]
    matrix['Overextension'] = WEIGHTS["price_overext"] if rsi.iloc[-1] > 70 else 0

    total_score = sum(matrix.values())
    verdict = "BULLISH" if total_score >= 6 else "NEUTRAL" if total_score >= 3 else "BEARISH"

    return matrix, total_score, verdict