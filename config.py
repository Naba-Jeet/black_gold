# config.py
DB_FILE = "crude_flow.db"

REQUIRED_SCHEMAS = {
    "wti_ohlc": ["Date", "Price", "Open", "High", "Low", "Vol."],
    "wti_brent_spread": ["Date", "Price"],
    "api_stocks": ["Release date", "Actual", "Forecast", "Previous"],
    "eia_stocks": ["Release date", "Actual", "Forecast", "Previous"],
    "cot_data": ["As_of_Date_In_Form_YYMMDD", "M_Money_Positions_Long_ALL", "M_Money_Positions_Short_ALL"],
    "gasoline_rbob": ["Date", "Price", "Vol."],
    "heating_oil": ["Date", "Price", "Vol."],
}

PRIMARY_KEYS = {
    "wti_ohlc": "Date",
    "wti_brent_spread": "Date",
    "api_stocks": "Release date",
    "eia_stocks": "Release date",
    "cot_data": "As_of_Date_In_Form_YYMMDD",
    "gasoline_rbob": "Date",
    "heating_oil": "Date",
}

# Updated Weights for Convergence Verdict
WEIGHTS = {
    "crack_spread": 2,
    "z_score": 3,
    "cot_squeeze": 3,
    "inv_surprise": 2,      # New: Actual vs Forecast
    "inv_momentum": 2,      # New: 4-week trend
    "price_overext": -2     # New: Penalty if RSI is too high/low
}
