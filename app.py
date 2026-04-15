# app.py
import streamlit as st
from ui_components import render_ingestion_page, render_terminal_page, render_quant_page, render_data_explorer_page, render_volume_profile_page
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

page = st.sidebar.pills(
    "Pages",
    [
        "🎯 Predictive Terminal",
        "📈 Quant Analysis",
        "📊 Volume Profile",
        "📦 Data Ingestion",
        "📊 Data Explorer",
        "🧮 Logic Center"
    ],
    width=200,
    #selection_mode='single',
    default="🎯 Predictive Terminal"
)

#st.sidebar.markdown("---")
#st.sidebar.info("System Status: **Operational**\nDatabase: **DuckDB Columnar**\nMode: **Predictive Flow**")

if page == "🎯 Predictive Terminal":
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

    # Volatility settings
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Volatility Settings")
    rv_window = st.sidebar.slider(
        "Realized Volatility Window (days)",
        min_value=10,
        max_value=60,
        value=30,
        step=5,
        help="Calculate 30-day RV using last X days of price data"
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

    # Calculate crack spreads only if all dependencies exist
    if availability['wti'] and availability['rbob'] and availability['ho']:
        datasets['cracks'] = calculate_crack_spreads(datasets['wti'], datasets['rbob'], datasets['ho'])
        availability['cracks'] = True
    else:
        datasets['cracks'] = None
        availability['cracks'] = False
    
    # Load OVX data from DuckDB
    try:
        datasets['ovx'] = load_from_db("ovx_data")
        availability['ovx'] = datasets['ovx'] is not None and len(datasets['ovx']) > 0
    except:
        availability['ovx'] = False

    # Render terminal with availability flags
    render_terminal_page(datasets, availability, weeks_lookback, rv_window)

elif page == "📊 Data Explorer":
    render_data_explorer_page()

elif page == "📦 Data Ingestion":
    render_ingestion_page()

elif page == "📈 Quant Analysis":
    try:
        wti_ohlc = load_from_db("wti_ohlc")
        render_quant_page(wti_ohlc)
    except Exception as e:
        st.error("⚠️ **Data Error**: WTI OHLC data not found.")
        st.info("Please upload the WTI OHLC file in the Ingestion page.")
        st.exception(e)

elif page == "📊 Volume Profile":
    try:
        wti_ohlc = load_from_db("wti_ohlc")
        render_volume_profile_page(wti_ohlc)
    except Exception as e:
        st.error("⚠️ **Data Error**: WTI OHLC data not found.")
        st.info("Please upload the WTI OHLC file in the Ingestion page.")
        st.exception(e)

elif page == "🧮 Logic Center":
    st.title("🧮 The Mathematical Foundation")
    st.markdown("""
        This terminal transitions from **Descriptive Analytics** (what happened) to **Predictive Flow** (what is forced to happen).
        
        ### 1. The Refinery Vacuum (Crack Spread)
        **What it is:** The profit margin refineries earn by converting crude oil into refined products (gasoline + heating oil).
        
        **Why it matters:** When the crack spread is wide (high), refineries are highly profitable → they buy more crude oil to maximize output → demand increases → WTI price rises. When narrow (low), refineries cut production → demand falls → WTI price falls.
        
        **Formula breakdown:** 
        $C_s = 2R + H - 3W$
        - 2 barrels of RBOB gasoline
        - 1 barrel of heating oil
        - Minus 3 barrels of WTI crude (typical yield from one barrel)
        
        **How to interpret:**
        - **Crack > Historical mean** → Refinery margins are expanding → Bullish for crude (refineries incentivized to buy)
        - **Crack < Historical mean** → Margins contracting → Bearish for crude (refineries reducing production)
        - **Extreme highs** → Unsustainable; refineries will eventually reduce runs when margins normalize
        
        **Trading signal:** Crack Spread = +2 weight in Force Matrix (contributes to bullish score when expanding)
        
        ### 2. The Arbitrage Force (Z-Score)
        **What it is:** Measures how far the Brent-WTI spread has deviated from its historical average, expressed in standard deviations.
        
        **Why it matters:** When WTI is much cheaper than Brent, arbitrageurs (traders) can profit by buying WTI and selling Brent → this buying pressure lifts WTI prices. When WTI is expensive vs Brent, the reverse happens.
        
        **Formula breakdown:** 
        $$z = \\frac{(B - W) - \\mu}{\\sigma}$$
        - B = Brent price
        - W = WTI price
        - μ = historical mean of the spread
        - σ = standard deviation
        
        **How to interpret:**
        - **z > +1.5** → WTI is cheap vs Brent by 1.5+ standard deviations → Arbitrage opportunity (buy WTI) → Bullish
        - **z < -1.5** → WTI is expensive vs Brent by 1.5+ standard deviations → Arb opportunity (sell WTI) → Bearish
        - **-1.5 < z < +1.5** → Spread is normal, no extreme mispricing → Neutral
        
        **Trading signal:** Z-Score = +3 weight in Force Matrix (strongest contributor to directional moves)
                
        **Formula:**
        $$z = \\frac{(B - W) - \\mu}{\\sigma}$$
        where $B$ = Brent, $W$ = WTI, $\\mu$ = historical mean, $\\sigma$ = standard deviation
        
        ### 3. The Sentiment Squeeze (COT Net Position)
        **What it is:** The net positioning of large speculators (Managed Money) in WTI futures — the difference between their long positions and short positions.
        
        **Why it matters:** Extreme positioning creates a "squeeze." When speculators are trapped on one side:
        - **Net SHORT (many shorts trapped)** → Forced covering creates buying pressure → WTI rises
        - **Net LONG (many longs trapped)** → Forced liquidation creates selling pressure → WTI falls
        
        **Formula breakdown:** 
        $$P_{net} = L_{managed} - S_{managed}$$
        - $L_{managed}$ = Managed Money Long positions
        - $S_{managed}$ = Managed Money Short positions
        
        **How to interpret:**
        - **Net < -10,000 contracts** → Short squeeze territory → Force Short Squeeze = +3 (bullish)
        - **Net > +10,000 contracts** → Long squeeze territory → Force Long Squeeze = -3 (bearish)
        - **-10,000 < Net < +10,000** → Balanced positioning → No extreme squeeze risk → Neutral
        
        **Trading signal:** COT Squeeze = +3 weight in Force Matrix. This is a leveraged signal — when combined with other bullish metrics, it amplifies upside potential (and vice versa for bearish).
        
        **Note:** COT data is released weekly on Fridays with 3-day lag, so positions can shift before you can execute. Best used as confirmation, not sole entry signal.        
        
        **Formula:**
        $$P_{net} = L_{managed} - S_{managed}$$
        where $L_{managed}$ = Managed Money Long Positions, $S_{managed}$ = Managed Money Short Positions
        
        ### 4. Inventory Shock (Supply & Demand)
        **What it is:** The surprise or shock when actual weekly inventory change differs from the market's forecast.
        
        **Why it matters:** Inventory data drives immediate price moves because it reveals actual supply/demand imbalance. If actual inventory draw is much larger than forecast, it shocks the market that supply is tighter than expected → prices spike up. Vice versa for builds.
        
        **Formula breakdown:** 
        $$\\Delta I = A - F$$
        - A = Actual weekly inventory change (barrels)
        - F = Forecast (consensus estimate)
        
        **How to interpret:**
        - **Shock < 0 (negative, larger draw than expected)** → Supply is tighter → Bullish = +2
        - **Shock > 0 (larger build than expected)** → Supply is more abundant → Bearish = -2
        - **Shock ≈ 0** → Market expectations met → Neutral = 0
        
        **Example:** Market forecast says crude inventory up 2M BBL, but actual is up 5M BBL → Shock = +3M BBL (bearish, more supply than expected)
        
        **Trading signal:** S&D Shock = +2 weight in Force Matrix. Most volatile component — often drives the largest single-day WTI moves when EIA releases (Wednesdays 10:30am ET).

        **Formula:**
        $$\\Delta I = A - F$$
        where $A$ = Actual weekly inventory change (BBL), $F$ = Market forecast (BBL)
        
        ### 5. Inventory Momentum (4-Week Trend)
        **What it is:** The average direction and magnitude of inventory changes over the last 4 weeks — tells you if supply is building or drawing structurally (not just one-week noise).
        
        **Why it matters:** A single large draw might be a fluke; but 4 consecutive weeks of draws signals a **structural trend** that refineries are run-down and demand is strong → longer-term bullish. Conversely, 4 weeks of builds = structural glut.
        
        **Formula breakdown:** 
        $$M = \\frac{1}{4}\\sum_{i=0}^{3} A_{t-i}$$
        - Average of last 4 weeks of actual inventory changes
        
        **How to interpret:**
        - **Momentum < 0 (negative, structural draw)** → Supply pressure over time → Bullish = +2
        - **Momentum > 0 (positive, structural build)** → Inventory accumulating → Bearish = -2
        - **Momentum ≈ 0** → No structural trend → Neutral = 0
        
        **Example:** Last 4 weeks: -2M, -1.5M, -2.5M, -1M BBL changes → Average = -1.75M → Structural draw (bullish)
        
        **Trading signal:** Inv Momentum = +2 weight in Force Matrix. Smoother than S&D Shock (no single-week spikes) — good for trend confirmation over 1-month horizon.
        
        **Difference from S&D Shock:** Shock tells you if THIS WEEK was a surprise. Momentum tells you if the TREND is bullish or bearish.        
        
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
                
         ### 8. Implied Volatility vs Realized Volatility (Vol Premium)
        
        **The Vol Premium Signal:**
        
        $$\\text{Vol Premium} = \\text{OVX}_{t} - \\text{RV}_{30d}$$
        
        where $\\text{OVX}$ = CBOE Crude Oil Volatility Index (market-priced 30-day IV)
        and $\\text{RV}_{30d}$ = 30-day realized volatility from historical prices
        
        **Interpretation:**
        - **Vol Premium > +15:** Options expensive → Market overpaying for protection → Sell volatility
        - **Vol Premium < -10:** Options cheap → Market underpricing actual moves → Buy volatility  
        - **±10 range:** Neutral → No vol edge, use directional trades only
        
        **Key Insight:** Vol Premium tells you **HOW to trade** the direction (futures vs options, ITM vs OTM), not **WHAT direction** to trade. It is a strategy overlay on top of the directional Force Score.
        
        **Example:**
        - Force Score = BULLISH + Vol Premium > +15 → Long WTI futures or deep ITM calls (avoid OTM — overpriced)
        - Force Score = BULLISH + Vol Premium < -10 → Buy OTM calls (cheap leverage at a discount)
        - Force Score = NEUTRAL (3-6) → Skip options entirely; if trading, use price-action filters only
        
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
