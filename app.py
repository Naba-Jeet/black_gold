# app.py
import streamlit as st
from ui_components import render_ingestion_page, render_terminal_page, render_quant_page, render_data_explorer_page
from data_engine import load_from_db
from quant_engine import calculate_crack_spreads

st.set_page_config(
    page_title="Crude Flow Terminal", 
    page_icon="🛢️", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

st.sidebar.title("🛢️ Crude Flow Terminal")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Main Menu",
    [
        "📦 Data Ingestion", 
        "📊 Data Explorer",      # NEW PAGE
        "🎯 Predictive Terminal", 
        "📈 Quant Analysis", 
        "🧮 Logic Center"
    ]
)

st.sidebar.markdown("---")
st.sidebar.info("System Status: **Operational**\nDatabase: **DuckDB Columnar**\nMode: **Predictive Flow**")

if page == "📦 Data Ingestion":
    render_ingestion_page()

elif page == "📊 Data Explorer":
    render_data_explorer_page()

elif page == "🎯 Predictive Terminal":
    try:
        wti = load_from_db("wti_ohlc")
        brent_spr = load_from_db("wti_brent_spread")
        cot = load_from_db("cot_data")
        rbob = load_from_db("gasoline_rbob")
        ho = load_from_db("heating_oil")
        eia = load_from_db("eia_stocks")
        cracks = calculate_crack_spreads(wti, rbob, ho)
        render_terminal_page(wti, brent_spr, cot, rbob, ho, cracks, eia)
    except Exception as e:
        st.error("⚠️ **Database Error**: Data is missing or corrupted.")
        st.info("Please navigate to the 'Data Ingestion' page and upload all 7 required CSV files.")
        st.exception(e)

elif page == "📈 Quant Analysis":
    try:
        wti_ohlc = load_from_db("wti_ohlc")
        render_quant_page(wti_ohlc)
    except Exception as e:
        st.error("⚠️ **Data Error**: WTI OHLC data not found.")
        st.info("Please upload the WTI OHLC file in the Ingestion page.")
        st.exception(e)

elif page == "🧮 Logic Center":
    st.title("🧮 The Mathematical Foundation")
    st.markdown("""
        This terminal transitions from **Descriptive Analytics** (what happened) to **Predictive Flow** (what is forced to happen).
        
        ### 1. The Refinery Vacuum (Crack Spread)
        **Formula:**
        $$C_s = 2R + H - 3W$$
        where $R$ = RBOB Gasoline, $H$ = Heating Oil, $W$ = WTI Crude
        
        ### 2. The Arbitrage Force (Z-Score)
        **Formula:**
        $$z = \\frac{(B - W) - \\mu}{\\sigma}$$
        where $B$ = Brent, $W$ = WTI, $\\mu$ = historical mean, $\\sigma$ = standard deviation
        
        ### 3. The Sentiment Squeeze (COT Net Position)
        **Formula:**
        $$P_{net} = L_{managed} - S_{managed}$$
        where $L_{managed}$ = Managed Money Long Positions, $S_{managed}$ = Managed Money Short Positions
        
        ### 4. Inventory Shock (Supply & Demand)
        **Formula:**
        $$\\Delta I = A - F$$
        where $A$ = Actual weekly inventory change (BBL), $F$ = Market forecast (BBL)
        
        ### 5. Inventory Momentum (4-Week Trend)
        **Formula:**
        $$M = \\frac{1}{4}\\sum_{i=0}^{3} A_{t-i}$$
        where negative $M$ indicates structural draw, positive indicates build
        
        ### 6. Relative Strength Index (RSI)
        **Formula:**
        $$\\text{RSI}_{n} = 100 - \\frac{100}{1 + \\text{RS}}, \\quad \\text{RS} = \\frac{\\bar{g}}{\\bar{l}}$$
        where $\\bar{g}$ = average gains, $\\bar{l}$ = average losses over period $n=14$
        
        ### 7. Volume-Weighted Average Price (VWAP)
        **Formula:**
        $$\\text{VWAP} = \\frac{\\sum_{i=1}^{n} P_i \\cdot V_i}{\\sum_{i=1}^{n} V_i}$$
        where $P_i = \\frac{H_i + L_i + C_i}{3}$ (typical price), $V_i$ = volume
        
        ---
        
        ### Force Matrix Scoring
        The **Convergence Verdict** aggregates all forces with weighted contributions:
        $$\\text{Score} = \\sum w_j \\cdot f_j$$
        where:
        - **Score ≥ 6** → **BULLISH** (high confluence)
        - **3 ≤ Score < 6** → **NEUTRAL** (mixed signals)
        - **Score < 3** → **BEARISH** (strong headwinds)
        """)
