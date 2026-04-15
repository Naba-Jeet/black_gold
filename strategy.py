# strategy.py
"""
Volume Profile Strategy - Leading Support/Resistance Detection
Implements: POC, VAH/VAL, HVN, LVN, Volume Gaps
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional


def build_volume_profile(df: pd.DataFrame, window: int = 20, bins: int = 50) -> Dict:
    """
    Build volume-at-price histogram from OHLCV data.

    Distributes volume across price bins proportionally to each bar's range.

    Args:
        df: DataFrame with 'high', 'low', 'vol' columns
        window: Number of bars to consider
        bins: Number of price bins for histogram

    Returns:
        Dict with 'profile' (price->volume), 'price_bins', 'volume_bins'
    """
    recent = df.tail(window).copy()

    if recent.empty or len(recent) < 2:
        return {"profile": {}, "price_bins": [], "volume_bins": [], "total_volume": 0}

    price_min = recent['low'].min()
    price_max = recent['high'].max()

    if price_max == price_min:
        return {"profile": {}, "price_bins": [], "volume_bins": [], "total_volume": 0}

    # Create price bins
    bins_array = np.linspace(price_min, price_max, bins)
    bin_width = (price_max - price_min) / bins

    # Initialize volume profile
    volume_profile = {price: 0.0 for price in bins_array}

    # Distribute volume proportionally across price range
    for idx, row in recent.iterrows():
        bar_range = row['high'] - row['low']
        bar_vol = row['vol']

        if bar_range == 0 or pd.isna(bar_vol) or bar_vol == 0:
            continue

        # Find which bins this bar overlaps
        start_bin = max(0, int((row['low'] - price_min) / bin_width))
        end_bin = min(bins - 1, int((row['high'] - price_min) / bin_width) + 1)

        # Distribute volume across overlapping bins
        vol_per_bin = bar_vol / max(1, end_bin - start_bin)

        for bin_idx in range(start_bin, end_bin + 1):
            if 0 <= bin_idx < bins:
                volume_profile[bins_array[bin_idx]] += vol_per_bin

    return {
        "profile": volume_profile,
        "price_bins": list(bins_array),
        "volume_bins": [volume_profile.get(p, 0) for p in bins_array],
        "total_volume": sum(volume_profile.values())
    }


def calculate_poc(profile: Dict[float, float]) -> float:
    """
    Calculate Point of Control - price level with highest volume.

    Args:
        profile: Dict mapping price -> volume

    Returns:
        POC price level
    """
    if not profile or all(v == 0 for v in profile.values()):
        return 0.0

    return max(profile, key=profile.get)


def calculate_value_area(profile: Dict[float, float], percentage: float = 0.70) -> Dict:
    """
    Calculate Value Area (VAH, VAL, POC) - 70% volume range.

    Args:
        profile: Dict mapping price -> volume
        percentage: Percentage of volume to include (default 70%)

    Returns:
        Dict with 'poc', 'vah', 'val'
    """
    if not profile or all(v == 0 for v in profile.values()):
        return {"poc": 0.0, "vah": 0.0, "val": 0.0}

    poc = calculate_poc(profile)
    sorted_volumes = sorted(profile.items(), key=lambda x: x[1], reverse=True)
    total_vol = sum(profile.values())
    target_vol = total_vol * percentage

    # Build value area from POC outward
    cum_vol = 0.0
    va_prices = [poc]

    for price, vol in sorted_volumes:
        cum_vol += vol
        va_prices.append(price)
        if cum_vol >= target_vol:
            break

    return {
        "poc": poc,
        "vah": max(va_prices),
        "val": min(va_prices)
    }


def detect_hvn_lvn(profile: Dict[float, float], price_bins: List[float],
                   hvn_threshold: float = 1.5, lvn_threshold: float = 0.5) -> Dict:
    """
    Detect High Volume Nodes (HVN) and Low Volume Nodes (LVN).

    HVN: Local volume peaks > threshold * average volume
    LVN: Local volume troughs < threshold * average volume

    Args:
        profile: Dict mapping price -> volume
        price_bins: List of price levels
        hvn_threshold: Multiplier above average for HVN
        lvn_threshold: Multiplier below average for LVN

    Returns:
        Dict with 'hvn' (list of prices) and 'lvn' (list of prices)
    """
    if not profile or len(profile) < 3:
        return {"hvn": [], "lvn": []}

    volumes = list(profile.values())
    avg_vol = np.mean(volumes)

    hvn_prices = []
    lvn_prices = []

    prices = sorted(profile.keys())

    for i, price in enumerate(prices):
        vol = profile[price]

        # Check neighbors for local extrema
        left_vol = profile[prices[i-1]] if i > 0 else vol
        right_vol = profile[prices[i+1]] if i < len(prices) - 1 else vol

        is_local_peak = vol >= left_vol and vol >= right_vol
        is_local_trough = vol <= left_vol and vol <= right_vol

        if is_local_peak and vol > avg_vol * hvn_threshold:
            hvn_prices.append(price)
        elif is_local_trough and vol < avg_vol * lvn_threshold:
            lvn_prices.append(price)

    return {"hvn": hvn_prices, "lvn": lvn_prices}


def detect_volume_gaps(profile: Dict[float, float], price_bins: List[float],
                       gap_threshold: float = 0.1) -> List[Dict]:
    """
    Detect volume gaps - price ranges with near-zero volume between HVNs.

    Args:
        profile: Dict mapping price -> volume
        price_bins: Sorted list of price levels
        gap_threshold: Max volume ratio to consider as gap

    Returns:
        List of gap dicts with 'start_price', 'end_price', 'gap_size'
    """
    if not profile or len(price_bins) < 3:
        return []

    volumes = [profile.get(p, 0) for p in price_bins]
    avg_vol = np.mean([v for v in volumes if v > 0]) if volumes else 0
    min_vol_threshold = avg_vol * gap_threshold

    gaps = []
    in_gap = False
    gap_start = None

    for i, (price, vol) in enumerate(zip(price_bins, volumes)):
        if vol < min_vol_threshold:
            if not in_gap:
                in_gap = True
                gap_start = price
        else:
            if in_gap and gap_start is not None:
                gap_end = price
                gap_size = gap_end - gap_start
                if gap_size > 0:
                    gaps.append({
                        "start_price": gap_start,
                        "end_price": gap_end,
                        "gap_size": gap_size
                    })
                in_gap = False
                gap_start = None

    return gaps


def calculate_volume_profile_signals(df: pd.DataFrame, window: int = 20,
                                     bins: int = 50) -> Dict:
    """
    Main function - Calculate all Volume Profile signals.

    Args:
        df: DataFrame with 'high', 'low', 'price', 'vol' columns
        window: Number of bars to consider (user-configurable)
        bins: Number of price bins for histogram

    Returns:
        Dict with all VP signals:
        - poc: Point of Control
        - vah: Value Area High
        - val: Value Area Low
        - hvn: List of High Volume Nodes
        - lvn: List of Low Volume Nodes
        - gaps: List of Volume Gaps
        - profile: Raw volume profile data
        - current_price_position: 'above_poc', 'below_poc', 'in_value_area'
        - poc_migration: 'rising', 'falling', 'neutral'
    """
    # Build volume profile
    vp_data = build_volume_profile(df, window=window, bins=bins)

    if not vp_data['profile'] or vp_data['total_volume'] == 0:
        return {
            "poc": 0.0, "vah": 0.0, "val": 0.0,
            "hvn": [], "lvn": [], "gaps": [],
            "profile": {}, "current_price_position": "unknown",
            "poc_migration": "unknown"
        }

    # Calculate core levels
    va = calculate_value_area(vp_data['profile'])
    nodes = detect_hvn_lvn(vp_data['profile'], vp_data['price_bins'])
    gaps = detect_volume_gaps(vp_data['profile'], vp_data['price_bins'])

    # Current price position
    current_price = df['price'].iloc[-1]
    if current_price > va['vah']:
        price_position = "above_value_area"
    elif current_price < va['val']:
        price_position = "below_value_area"
    elif current_price > va['poc']:
        price_position = "above_poc"
    elif current_price < va['poc']:
        price_position = "below_poc"
    else:
        price_position = "at_poc"

    # POC Migration (trend signal)
    poc_migration = "neutral"
    if window >= 10:
        # Compare recent POC to prior POC
        half_window = window // 2
        prior_vp = build_volume_profile(df.iloc[:-half_window], window=half_window, bins=bins)
        if prior_vp['profile']:
            prior_poc = calculate_poc(prior_vp['profile'])
            if va['poc'] > prior_poc * 1.001:
                poc_migration = "rising"
            elif va['poc'] < prior_poc * 0.999:
                poc_migration = "falling"

    return {
        "poc": va['poc'],
        "vah": va['vah'],
        "val": va['val'],
        "hvn": nodes['hvn'],
        "lvn": nodes['lvn'],
        "gaps": gaps,
        "profile": vp_data['profile'],
        "price_bins": vp_data['price_bins'],
        "volume_bins": vp_data['volume_bins'],
        "current_price_position": price_position,
        "poc_migration": poc_migration,
        "total_volume": vp_data['total_volume']
    }


def generate_vp_signals(vp_data: Dict, current_price: float) -> List[Dict]:
    """
    Generate trading signals from Volume Profile data.

    Args:
        vp_data: Dict from calculate_volume_profile_signals
        current_price: Current market price

    Returns:
        List of signal dicts with 'type', 'direction', 'level', 'strength', 'description'
    """
    signals = []

    if vp_data.get('poc') == 0:
        return signals

    # Signal 1: POC Position
    if current_price > vp_data['poc'] * 1.01:
        signals.append({
            "type": "poc_position",
            "direction": "bullish",
            "level": vp_data['poc'],
            "strength": "moderate",
            "description": f"Price trading above POC (${vp_data['poc']:.2f}) - bullish bias"
        })
    elif current_price < vp_data['poc'] * 0.99:
        signals.append({
            "type": "poc_position",
            "direction": "bearish",
            "level": vp_data['poc'],
            "strength": "moderate",
            "description": f"Price trading below POC (${vp_data['poc']:.2f}) - bearish bias"
        })

    # Signal 2: POC Migration
    if vp_data.get('poc_migration') == 'rising':
        signals.append({
            "type": "poc_migration",
            "direction": "bullish",
            "level": vp_data['poc'],
            "strength": "strong",
            "description": "POC migrating higher - institutional accumulation"
        })
    elif vp_data.get('poc_migration') == 'falling':
        signals.append({
            "type": "poc_migration",
            "direction": "bearish",
            "level": vp_data['poc'],
            "strength": "strong",
            "description": "POC migrating lower - institutional distribution"
        })

    # Signal 3: Value Area Breakout
    if current_price > vp_data['vah'] * 1.01:
        signals.append({
            "type": "va_breakout",
            "direction": "bullish",
            "level": vp_data['vah'],
            "strength": "strong",
            "description": f"Price broke above VAH (${vp_data['vah']:.2f}) - target 1.618x VA height"
        })
    elif current_price < vp_data['val'] * 0.99:
        signals.append({
            "type": "va_breakout",
            "direction": "bearish",
            "level": vp_data['val'],
            "strength": "strong",
            "description": f"Price broke below VAL (${vp_data['val']:.2f}) - target 1.618x VA height"
        })

    # Signal 4: HVN Support/Resistance
    for hvn in vp_data.get('hvn', [])[:3]:  # Top 3 HVNs
        if hvn < current_price * 0.98:
            signals.append({
                "type": "hvn_support",
                "direction": "bullish",
                "level": hvn,
                "strength": "moderate",
                "description": f"HVN at ${hvn:.2f} acting as support"
            })
        elif hvn > current_price * 1.02:
            signals.append({
                "type": "hvn_resistance",
                "direction": "bearish",
                "level": hvn,
                "strength": "moderate",
                "description": f"HVN at ${hvn:.2f} acting as resistance"
            })

    # Signal 5: LVN Breakout Path
    for lvn in vp_data.get('lvn', [])[:2]:  # Top 2 LVNs
        if abs(current_price - lvn) / current_price < 0.02:  # Within 2%
            signals.append({
                "type": "lvn_entry",
                "direction": "neutral",
                "level": lvn,
                "strength": "weak",
                "description": f"Price near LVN at ${lvn:.2f} - fast move expected"
            })

    return signals


def calculate_vp_targets_sl(vp_data: Dict, entry_price: float,
                            direction: str) -> Dict:
    """
    Calculate target prices and stop loss based on Volume Profile.

    Args:
        vp_data: Dict from calculate_volume_profile_signals
        entry_price: Entry price for the trade
        direction: 'long' or 'short'

    Returns:
        Dict with 'targets' (T1, T2, T3) and 'stop_loss'
    """
    if not vp_data or vp_data.get('poc') == 0:
        return {"targets": [], "stop_loss": 0.0}

    targets = []
    stop_loss = 0.0

    # Value Area height for extensions
    va_height = vp_data['vah'] - vp_data['val']

    if direction == 'long':
        # T1: POC (mean reversion)
        if entry_price < vp_data['poc']:
            targets.append(("T1", vp_data['poc'], "POC mean reversion"))

        # T2: Next HVN
        hvns_above = [h for h in vp_data.get('hvn', []) if h > entry_price]
        if hvns_above:
            targets.append(("T2", hvns_above[0], "HVN resistance"))

        # T3: VAH extension
        targets.append(("T3", vp_data['vah'] + va_height * 0.618, "VAH 1.618 extension"))

        # Stop Loss: Below VAL or HVN cluster
        if vp_data.get('val') > 0:
            stop_loss = vp_data['val'] * 0.995  # Just below VAL
        elif vp_data.get('hvn'):
            hvns_below = [h for h in vp_data['hvn'] if h < entry_price]
            if hvns_below:
                stop_loss = hvns_below[-1] * 0.995

    elif direction == 'short':
        # T1: POC (mean reversion)
        if entry_price > vp_data['poc']:
            targets.append(("T1", vp_data['poc'], "POC mean reversion"))

        # T2: Next HVN
        hvns_below = [h for h in vp_data.get('hvn', []) if h < entry_price]
        if hvns_below:
            targets.append(("T2", hvns_below[0], "HVN support"))

        # T3: VAL extension
        targets.append(("T3", vp_data['val'] - va_height * 0.618, "VAL 1.618 extension"))

        # Stop Loss: Above VAH or HVN cluster
        if vp_data.get('vah') > 0:
            stop_loss = vp_data['vah'] * 1.005  # Just above VAH
        elif vp_data.get('hvn'):
            hvns_above = [h for h in vp_data['hvn'] if h > entry_price]
            if hvns_above:
                stop_loss = hvns_above[0] * 1.005

    return {
        "targets": targets,
        "stop_loss": stop_loss,
        "risk_reward": (targets[0][1] - entry_price) / (entry_price - stop_loss) if stop_loss > 0 and targets else 0
    }
