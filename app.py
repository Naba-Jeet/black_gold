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
    # Add week filter to sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📅 Predictive Terminal Settings")
    weeks_lookback = st.sidebar.slider(
        "Lookback Period (weeks)",
        min_value=1,
        max_value=104,  # 2 years
        value=4,  # default 1 months
        help="Calculate metrics using the last X weeks of data"
    )
    
    # Load each dataset independently with error tracking
    datasets = {}
    availability = {}

    # Attempt to load each dataset
    try:
        datasets['wti'] = load_from_db("wti_ohlc")
        availability['wti'] = datasets['wti'] is not None and len(datasets['wti']) > 0
    except:
        availability['wti'] = False

    try:
        datasets['brent_spr'] = load_from_db("wti_brent_spread")
        availability['brent_spr'] = datasets['brent_spr'] is not None and len(datasets['brent_spr']) > 0
    except:
        availability['brent_spr'] = False

    try:
        datasets['cot'] = load_from_db("cot_data")
        availability['cot'] = datasets['cot'] is not None and len(datasets['cot']) > 0
    except:
        availability['cot'] = False

    try:
        datasets['rbob'] = load_from_db("gasoline_rbob")
        availability['rbob'] = datasets['rbob'] is not None and len(datasets['rbob']) > 0
    except:
        availability['rbob'] = False

    try:
        datasets['ho'] = load_from_db("heating_oil")
        availability['ho'] = datasets['ho'] is not None and len(datasets['ho']) > 0
    except:
        availability['ho'] = False

    try:
        datasets['eia'] = load_from_db("eia_stocks")
        availability['eia'] = datasets['eia'] is not None and len(datasets['eia']) > 0
    except:
        availability['eia'] = False

    try:
        datasets['tanker'] = load_from_db("tanker_flow")
        availability['tanker'] = datasets['tanker'] is not None and len(datasets['tanker']) > 0
    except:
        availability['tanker'] = False

    # Calculate crack spreads only if all dependencies exist
    if availability['wti'] and availability['rbob'] and availability['ho']:
        datasets['cracks'] = calculate_crack_spreads(datasets['wti'], datasets['rbob'], datasets['ho'])
        availability['cracks'] = True
    else:
        datasets['cracks'] = None
        availability['cracks'] = False

    # Render terminal with availability flags
    render_terminal_page(datasets, availability, weeks_lookback)

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
                
        ### Refined Products & Crude Oil Correlation
        
        **The Refinery Economics Nexus:**
        
        The crack spread formula uses three linked commodities because they represent the **refinery profit margin**—the heart of crude demand.
        
        #### What is Gasoline RBOB (Reformulated Blendstock for Oxygenate Blending)?
        
        - **Definition:** The benchmark for US unleaded gasoline futures traded on NYMEX
        - **Why it matters:** 55% of crude oil refining output becomes gasoline
        - **Price driver:** Tied to US transportation demand, seasonal (peak in spring/summer driving season)
        - **Market signal:** When RBOB rallies vs WTI, refiners earn higher margins → incentive to buy more crude
        
        #### What is Heating Oil?
        
        - **Definition:** Distillate fuel used for space heating and diesel engines
        - **Why it matters:** 25% of crude oil refining output becomes heating oil
        - **Price driver:** Seasonal (peaks in winter when heating demand rises), correlated to global diesel demand
        - **Market signal:** When heating oil rallies, it signals strong demand for distillates → refineries buy more crude
        
        #### The Correlation Story
        
        | Commodity | % of Crude Output | Primary Driver | Season |
        |-----------|------------------|----------------|--------|
        | **WTI Crude** | 100% (baseline) | Supply shocks, geopolitics, inventory | Year-round |
        | **RBOB Gasoline** | 55% | Transportation demand, driving season | Spring/Summer peak |
        | **Heating Oil** | 25% | Heating demand, global diesel need | Winter peak |
        
        **Why they move together:**
        - All three are refined from the same barrel of crude
        - When refined product prices are high, refineries buy more crude → WTI rises
        - When refined product prices are low, refineries cut crude purchases → WTI falls
        - **Decoupling = Refinery stress:** If RBOB and heating oil stay flat but WTI spikes, refineries squeeze and reduce runs
        
        #### Historical Correlations
        
        - **RBOB vs WTI:** +0.87 (very strong) — They move lock-step
        - **Heating Oil vs WTI:** +0.82 (strong) — Seasonal variance adds noise
        - **RBOB + HO vs WTI:** +0.91 (very strong) — Together, they're an excellent leading indicator
        
        **What strong correlation means:**
        When RBOB and heating oil BOTH rally with WTI → **Refinery profit expansion** → Bullish for crude (demand driven)
        When WTI rallies but RBOB/HO lag → **Refinery margin compression** → Bearish for crude (supply glut, demand weakness)
        """)
