DB_FILE = "crude_flow.db"

REQUIRED_SCHEMAS = {
    "wti_ohlc": ["date", "price", "open", "high", "low", "vol"],
    "wti_brent_spread": ["date", "price"],
    "api_stocks": ["release_date", "actual", "forecast", "previous"],
    "eia_stocks": ["release_date", "actual", "forecast", "previous"],
    "cot_data": ["as_of_date_in_form_yymmdd", "m_money_positions_long_all", "m_money_positions_short_all"],
    "gasoline_rbob": ["date", "price", "vol"],
    "heating_oil": ["date", "price", "vol"],
    "ovx_data": ["date", "price", "open", "high", "low"],
}

PRIMARY_KEYS = {
    "wti_ohlc": "date",
    "wti_brent_spread": "date",
    "api_stocks": "release_date",
    "eia_stocks": "release_date",
    "cot_data": "as_of_date_in_form_yymmdd",
    "gasoline_rbob": "date",
    "heating_oil": "date",
    "ovx_data": "date",
}

# External Data Source Links
DATA_SOURCES = {
    "wti_ohlc": {
        "label": "WTI OHLC Data - Daily",
        "url": "https://in.investing.com/commodities/crude-oil-historical-data"
    },
    "wti_brent_spread": {
        "label": "WTI-Brent Spread - Daily",
        "url": "https://www.investing.com/commodities/brent-wti-crude-spread-futures-historical-data"
    },
    "api_stocks": {
        "label": "API Weekly Inventory - Wednesday (2:00 AM IST)",
        "url": "https://www.investing.com/economic-calendar/api-weekly-crude-stock-656"
    },
    "eia_stocks": {
        "label": "EIA Weekly Inventory - Wednesday (8:00 PM IST)",
        "url": "https://in.investing.com/economic-calendar/eia-crude-oil-inventories-75"
    },
    "cot_data": {
        "label": "Commitments of Traders Positioning - Saturday (1:00 AM IST)",
        "url": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm"
    },
    "gasoline_rbob": {
        "label": "Gasoline RBOB - Daily",
        "url": "https://in.investing.com/commodities/gasoline-rbob-historical-data"
    },
    "heating_oil": {
        "label": "Heating Oil - Daily",
        "url": "https://in.investing.com/commodities/heating-oil-historical-data"
    },
    "ovx_data": {
        "label": "CBOE Crude Oil Volatility (OVX)- Daily",
        "url": "https://in.investing.com/indices/cboe-crude-oil-volatility-historical-data"
    },
}

WEIGHTS = {
    "crack_spread": 2,
    "z_score": 3,
    "cot_squeeze": 3,
    "inv_surprise": 2,
    "inv_momentum": 2,
    "price_overext": -2
}