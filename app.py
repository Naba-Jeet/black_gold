import streamlit as st
from ui_components import render_ingestion_page, render_terminal_page, render_quant_page
from data_engine import load_from_db
from quant_engine import calculate_crack_spreads

# ==========================================
# 1. PAGE CONFIGURATION
# ==========================================
st.set_page_config(
    page_title="Crude Flow Terminal", 
    page_icon="🛢️", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# ==========================================
# 2. SIDEBAR NAVIGATION
# ==========================================
st.sidebar.title("🛢️ Crude Flow Terminal")
st.sidebar.markdown("---")

# Tabular Navigation using Radio buttons
page = st.sidebar.radio(
    "Main Menu",
    [
        "📦 Data Ingestion", 
        "🎯 Predictive Terminal", 
        "📈 Quant Analysis", 
        "🧮 Logic Center"
    ]
)

st.sidebar.markdown("---")
st.sidebar.info("System Status: **Operational**\nDatabase: **DuckDB Columnar**\nMode: **Predictive Flow**")

# ==========================================
# 3. PAGE ROUTING
# ==========================================

if page == "📦 Data Ingestion":
    # Logic: Simply call the ingestion UI
    render_ingestion_page()

elif page == "🎯 Predictive Terminal":
    # Logic: Load all necessary datasets and calculate interdependent signals
    try:
        # Loading the 6 core datasets required for convergence
        wti = load_from_db("wti_ohlc")
        brent_spr = load_from_db("wti_brent_spread")
        cot = load_from_db("cot_data")
        rbob = load_from_db("gasoline_rbob")
        ho = load_from_db("heating_oil")
        eia = load_from_db("eia_stocks")
        
        # Mathematical Pipeline: Calculate Crack Spreads before rendering
        cracks = calculate_crack_spreads(wti, rbob, ho)
        
        # Render the professional terminal
        render_terminal_page(wti, brent_spr, cot, rbob, ho, cracks, eia)
        
    except Exception as e:
        st.error("⚠️ **Database Error**: Data is missing or corrupted.")
        st.info("Please navigate to the 'Data Ingestion' page and upload all 7 required CSV files.")
        st.exception(e) # Hidden for users, but helpful for the developer

elif page == "📈 Quant Analysis":
    # Logic: Load only the OHLC data for technical analysis
    try:
        wti_ohlc = load_from_db("wti_ohlc")
        render_quant_page(wti_ohlc)
    except Exception as e:
        st.error("⚠️ **Data Error**: WTI OHLC data not found.")
        st.info("Please upload the WTI OHLC file in the Ingestion page.")
        st.exception(e)

elif page == "🧮 Logic Center":
    # Logic: Static educational page explaining the math
    st.title("🧮 The Mathematical Foundation")
    st.markdown("""
    This terminal transitions from **Descriptive Analytics** (what happened) to **Predictive Flow** (what is forced to happen).
    
    ### 1. The Refinery Vacuum (Crack Spread)
    **Formula:** $\text{Spread} = (2 \times \text{RBOB Gasoline}) + (1 \times \text{Heating Oil}) - (3 \times \text{WTI})$
    *   **Logic:** Measures the profit margin for a refinery. When the spread expands, refineries are *forced* to buy more crude oil to capture higher margins.

    ---
    ### 2. The Arbitrage Force (Z-Score)
    **Formula:** $\text{Z-Score} = \frac{(\text{Brent} - \text{WTI}) - \text{Mean}}{\text{Standard Deviation}}$
    *   **Logic:** When the WTI-Brent spread reaches an extreme (Z > 1.5), WTI is historically undervalued. This forces US exporters to ship more WTI to Europe, draining Cushing inventories.

    ---
    ### 3. The Sentiment Squeeze (COT)
    **Formula:** $\text{Net Position} = \text{Managed Longs} - \text{Managed Shorts}$
    *   **Logic:** If the market is deeply Net Short while prices are rising, a "Short Squeeze" is triggered. Traders are *forced* to buy back their positions to prevent margin calls.

    ---
    ### 4. Inventory Shock
    **Formula:** $\text{Shock} = \text{Actual Build/Draw} - \text{Market Forecast}$
    *   **Logic:** The market prices in the forecast. The actual movement only causes a price reaction if it *surprises* the forecast (The Shock).
    """)
