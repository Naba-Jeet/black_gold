# ui_components.py
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import duckdb, io
from config import DATA_SOURCES, PRIMARY_KEYS
from data_engine import validate_df, normalize_columns
from quant_engine import calculate_z_score, calculate_volume_profile, generate_detailed_force_matrix, detect_liquidity_sweeps_v2, calculate_inventory_shock, calculate_inv_momentum, calculate_rsi, calculate_vwap, calculate_sr_levels
from strategy import calculate_volume_profile_signals, generate_vp_signals, calculate_vp_targets_sl


# ==========================================
# DATA INGESTION PAGE (With Source Links)
# ==========================================
def render_ingestion_page():
    st.title("🏗️ Crude Oil Specific Data Upload")
    st.markdown("Upload your CSVs below. The system validates the schema and performs an **Upsert** (Update/Insert) into the columnar warehouse.")

    if 'uploaded_files' not in st.session_state:
        st.session_state.uploaded_files = {}

    # Layout: 3 Columns for Uploaders
    cols = st.columns(3)

    for idx, (key, info) in enumerate(DATA_SOURCES.items()):
        with cols[idx % 3]:
            # Display Label with External Link
            st.markdown(f"**📤 {info['label']}**")
            st.markdown(f"[🔗 Download Data]({info['url']})")

            uploaded_file = st.file_uploader(f"Upload CSV", type=["csv"], key=f"up_{key}", label_visibility="collapsed")

            if uploaded_file:
                df = pd.read_csv(io.StringIO(uploaded_file.getvalue().decode('utf-8')))
                df = normalize_columns(df)
                is_valid, msg = validate_df(df, key)

                if is_valid:
                    st.success(f"✅ Valid")
                    st.session_state.uploaded_files[key] = df
                else:
                    st.error(f"❌ {msg}")

    st.divider()

    # Data Preview Section
    if st.session_state.uploaded_files:
        st.subheader("👀 Data Preview")
        selected_file = st.selectbox("Select file to preview", list(st.session_state.uploaded_files.keys()))
        st.dataframe(st.session_state.uploaded_files[selected_file].head(10), use_container_width=True)

    # Commit Button
    if st.button("🚀 Commit All Valid Data to Warehouse", type="primary"):
        if not st.session_state.uploaded_files:
            st.warning("No files have been uploaded yet!")
        else:
            from data_engine import upsert_to_duckdb, clean_volume
            logs = []
            for key, df in st.session_state.uploaded_files.items():
                for col in df.columns:
                    if "vol" in col:
                        df[col] = df[col].apply(clean_volume)
                result = upsert_to_duckdb(df, key)
                logs.append(f"**{key}**: {result}")

            st.success("Warehouse Sync Complete!")
            for log in logs:
                st.write(log)
            st.balloons()
# ==========================================
# DATA EXPLORER PAGE (Fixed)
# ==========================================
def render_data_explorer_page():
    st.title("📊 Data Explorer")
    st.markdown("Query the underlying DuckDB tables directly. Select a table and fetch the last **X** days of data.")

    # Connect to DB and get table list
    conn = duckdb.connect("crude_flow.db")
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    conn.close()

    if not tables:
        st.warning("No tables found in the database. Please upload data first.")
        return

    table_names = [t[0] for t in tables]

    col1, col2 = st.columns(2)
    with col1:
        selected_table = st.selectbox("Select Table", table_names)
    with col2:
        days_limit = st.number_input("Last X Days/Rows", min_value=1, max_value=1000, value=30)

    if st.button("🔍 Fetch Data", type="primary"):
        try:
            conn = duckdb.connect("crude_flow.db")

            date_col = PRIMARY_KEYS.get(selected_table, "date")
            query = f"SELECT * FROM {selected_table} ORDER BY {date_col} ASC"
            df = conn.execute(query).df()
            conn.close()

            st.subheader(f"📁 {selected_table} ({len(df)} rows)")
            st.dataframe(df, use_container_width=True)

            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name=f"{selected_table}_export.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"Query Error: {e}")



def render_terminal_page(datasets, availability, weeks_lookback=26, rv_window=30):
    """
    datasets: dict with keys: wti, brent_spr, cot, rbob, ho, cracks, eia
    availability: dict tracking which datasets loaded successfully
    """
    st.title("📶 Crude Flow Predictive Terminal")

    # status_cols = st.columns(8)
    # dataset_names = ["WTI OHLC", "Brent Spread", "COT", "RBOB", "HO", "EIA", "Cracks"]
    # for idx, (name, avail_key) in enumerate(zip(dataset_names, ["wti", "brent_spr", "cot", "rbob", "ho", "eia", "cracks"])):
    #     with status_cols[idx]:
    #         if availability.get(avail_key, False):
    #             st.success(f"✅ {name}")
    #         else:
    #             st.warning(f"⏳ {name}")

    # Check if minimum data exists
    min_requirements_met = availability.get('wti') and availability.get('cracks')
    if not min_requirements_met:
        st.error("⚠️ **Insufficient Data**: WTI OHLC and Crack Spread (WTI+RBOB+HO) are required. Please upload missing files.")
        st.info("Navigate to **Data Ingestion** to upload CSV files.")
        return
    
    # Filter data to last X weeks
    from datetime import timedelta
    
    def filter_by_weeks(df, date_col, weeks):
        if df.empty:
            return df
        max_date = pd.to_datetime(df[date_col]).max()
        cutoff_date = max_date - timedelta(weeks=weeks)
        return df[pd.to_datetime(df[date_col]) >= cutoff_date].reset_index(drop=True)
    
    # Apply filters to available datasets
    wti_f = filter_by_weeks(datasets.get('wti'), 'date', weeks_lookback)
    brent_spr_f = filter_by_weeks(datasets.get('brent_spr'), 'date', weeks_lookback)
    cot_f = filter_by_weeks(datasets.get('cot'), 'as_of_date_in_form_yymmdd', weeks_lookback)
    cracks_f = filter_by_weeks(datasets.get('cracks'), 'date', weeks_lookback)
    eia_f = filter_by_weeks(datasets.get('eia'), 'release_date', weeks_lookback)
    
    # Initialize metrics dict (only calculate if data exists)
    metrics = {}

    # 1. Crack Spread
    if availability['cracks'] and cracks_f is not None:
        metrics['current_crack'] = cracks_f['crack_spread'].iloc[-1]
        metrics['crack_mean'] = cracks_f['crack_spread'].mean()
    else:
        metrics['current_crack'] = 0
        metrics['crack_mean'] = 0

    # 2. COT Squeeze
    if availability['cot'] and cot_f is not None:
        metrics['net_pos'] = cot_f['m_money_positions_long_all'].iloc[-1] - cot_f['m_money_positions_short_all'].iloc[-1]
    else:
        metrics['net_pos'] = 0

    # 3. S&D Shock
    if availability['eia'] and eia_f is not None:
        metrics['inv_shock'] = calculate_inventory_shock(eia_f)
    else:
        metrics['inv_shock'] = {"value": 0.0, "unit": "BBL", "abs_m": 0.0}

    # 4. Z-Score
    if availability['brent_spr'] and brent_spr_f is not None:
        metrics['z_score'] = calculate_z_score(brent_spr_f)
    else:
        metrics['z_score'] = 0

    # 5. Inventory Momentum
    if availability['eia'] and eia_f is not None:
        metrics['inv_mom'] = calculate_inv_momentum(eia_f)
    else:
        metrics['inv_mom'] = 0

    # 6. RSI
    if availability['wti'] and wti_f is not None:
        metrics['rsi'] = calculate_rsi(wti_f)
    else:
        metrics['rsi'] = pd.Series([50] * 14)  # Neutral default

    force_matrix, total_score, verdict = generate_detailed_force_matrix(
        metrics['current_crack'], metrics['crack_mean'], metrics['z_score'], 
        metrics['net_pos'], metrics['inv_shock']['value'], metrics['inv_mom'], 
        metrics['rsi']
    )

    st.divider()

        # --- VOLATILITY OVERLAY SECTION ---
            # WTI Latest Date
    if availability.get('wti') and datasets.get('wti') is not None and not datasets['wti'].empty:
        wti_latest_date = pd.to_datetime(datasets['wti']['date']).max().strftime('%Y-%m-%d')
        st.badge(f"Last WTI Crude OHLC Date: **{wti_latest_date}** | Metrics calculated from last **{weeks_lookback}** weeks of data", color='violet', icon=":material/date_range:")

        
    if availability['wti'] and wti_f is not None and len(wti_f) > 0:
        poc_data = calculate_volume_profile(wti_f, window=weeks_lookback)
        # st.info(f"**Point of Control (POC): ${poc_data['poc']:.2f}**")
        p_high = poc_data['vah']
        p_low = poc_data['val']
        # st.info(f"**Value Area (High): ${poc_data['vah']:.2f}**")
        # st.info(f"**Value Area (Low): ${poc_data['val']:.2f}**")
    else:
        p_high = 1
        p_high = 0

        # st.warning("⏳ WTI data not available for Volume Profile calculation")

    st.subheader("📊 Volatility Premium Analysis (Strategy Overlay)")
    
    if availability.get('wti') and availability.get('ovx'):
        from quant_engine import calculate_vol_premium
        
        ovx_f = datasets.get('ovx')
        # Use FULL wti dataset (not filtered) for accurate RV calculation
        wti_full = datasets.get('wti')
        
        if ovx_f is not None and len(ovx_f) > 0 and wti_full is not None and len(wti_full) > 0:
            vol_data = calculate_vol_premium(wti_full, ovx_f, rv_window=rv_window)
            
            vol_col1, vol_col2, vol_col3, vol_col4 = st.columns(4)
            
            with vol_col1:
                st.metric(
                    "OVX (Implied Vol)", 
                    f"{vol_data['ovx_current']:.2f}",
                    help="CBOE Crude Oil Volatility Index — market-priced 30-day IV"
                )
            
            with vol_col2:
                st.metric(
                    f"RV ({rv_window}d)", 
                    f"{vol_data['rv_current']:.2f}",
                    help=f"Realized Volatility from last {rv_window} days of WTI prices"
                )
            
            with vol_col3:
                st.metric(
                    "Vol Premium (Option Pricing)",
                    f"{vol_data['vol_premium']:.2f}",
                    delta=vol_data['signal'],
                    help="OVX minus RV — positive means options are expensive",
                    border=True,
                    format="dollar"
                )
            
            # with vol_col4:
            #     if vol_data['signal'] == 'EXPENSIVE':
            #         st.error(f"⚠️ {vol_data['signal']}")
            #     elif vol_data['signal'] == 'CHEAP':
            #         st.success(f"✅ {vol_data['signal']}")
            #     else:
            #         st.info(f"🟩 {vol_data['signal']}")
            
            # Recommendation box
            # st.info(f"**Strategy Recommendation:** {vol_data['recommendation']}")
            
            # Combined directional + vol signal
            st.markdown("#### 🧭 Strategy (Volatility + Trend + S/D Analysis)")
            if total_score >= 6:
                if vol_data['vol_premium'] < -10:
                    st.success("🚀 **IDEAL SETUP:** Bullish Score + Cheap IV → Buy OTM calls for leverage at discount")
                elif vol_data['vol_premium'] > 15:
                    st.warning("⚡ **OPPORTUNE:** Bullish Score + Expensive IV → Use futures or deep ITM calls (avoid OTM — overpriced)")
                else:
                    st.info("📈 **BULLISH:** Balanced IV → Use any directional vehicle (futures, calls, spreads)")
            elif total_score < 3:
                if vol_data['vol_premium'] < -10:
                    st.metric(
                        label="⚠️ HEDGED DOWNSIDE",
                        value = f"Vol. Prem: {vol_data['vol_premium']:.2f} + Agg Score: {total_score}",
                        help="Bearish directional bias but options are cheap → use OTM puts to hedge at discount",
                        border=True, delta=f"Bearish Score: {total_score}", delta_color="red", delta_arrow="down",
                        format = "dollar"
                    )

                    # st.badge("⚠️ **HEDGED DOWNSIDE:** Bearish Score + Cheap IV → Buy OTM puts for protection at discount")
                elif vol_data['vol_premium'] > 15:
                    st.warning("📉 **SELL VOL:** Bearish Score + Expensive IV → Short straddles or covered calls")
                else:
                    st.error("🔻 **BEARISH:** Neutral IV → Use futures (avoid expensive premium hedges)")
            else:
                st.info("🔄 **NEUTRAL SCORE:** Price action matters more — use RSI, VWAP, S/R filters")
        else:
            st.warning("⏳ OVX data not available for volatility analysis. Upload OVX CSV via Data Ingestion page.")
    else:
        st.warning("⏳ WTI or OVX data missing. Upload both files to enable volatility overlay.")

    # Add info box showing active filter
    # st.info(f"📊 **Metrics calculated from last {weeks_lookback} weeks of data**")


    m1, m2, m3, m4, m5 = st.columns(5)

    if availability['cracks']:
        if metrics['current_crack'] > metrics['crack_mean']:
            m1.metric("Crack Spread", f"${metrics['current_crack']:.2f}", delta="Expanding", delta_arrow="up", delta_color="green")
        elif metrics['current_crack'] < metrics['crack_mean']:
            m1.metric("Crack Spread", f"${metrics['current_crack']:.2f}", delta="Contracting", delta_arrow="down", delta_color="red")
        else:
            m1.metric("Crack Spread", f"${metrics['current_crack']:.2f}", delta="Stable", delta_arrow="off", delta_color="blue")
    else:
        m1.warning("No Crack Data")
    
    if availability['cot']:
        if metrics['net_pos'] < -10000:
            m2.metric("Squeeze Factor", f"{metrics['net_pos']}", delta="Short Squeeze", delta_color="green", delta_arrow="up")
        elif metrics['net_pos'] > 10000:
            m2.metric("Squeeze Factor", f"{metrics['net_pos']}", delta="Long Squeeze", delta_color="red", delta_arrow="down")
        else:
            m2.metric("Squeeze Factor", f"{metrics['net_pos']}", delta="Neutral", delta_color="blue", delta_arrow="off")
    else:
        m2.warning("No COT Data")
    # Condition	Signal	Meaning
    # net_pos < -10000	Short Squeeze 📈 (Bullish)	Too many shorts trapped → forced to cover → price up
    # net_pos > 10000	Long Squeeze 📉 (Bearish)	Too many longs trapped → forced to liquidate → price down
    # -10000 ≤ net_pos ≤ 10000	Balanced (Neutral)	Positions are equilibrated, no extreme squeeze risk


    if availability['eia']:
        if metrics['inv_shock']['value'] < 0:
            m3.metric("Supply-Demand Shock", f"{metrics['inv_shock']['abs_m']:.2f}M BBL", 
            delta="Bullish", delta_color="green", delta_arrow="up")
        elif metrics['inv_shock']['value'] > 0:
            m3.metric("Supply-Demand Shock", f"{metrics['inv_shock']['abs_m']:.2f}M BBL", 
            delta="Bearish", delta_color="red", delta_arrow="down")
        else:
            m3.metric("Supply-Demand Shock", f"{metrics['inv_shock']['abs_m']:.2f}M BBL", 
            delta="Neutral", delta_color="blue", delta_arrow="off")
    else:
        m3.warning("No EIA Data")
#     Scenario	Value	Signal	Meaning
#     Actual < Forecast	Negative (e.g., -2M BBL)	Bullish 📈	More draw than expected = less supply in market
#     Actual > Forecast	Positive (e.g., +3M BBL)	Bearish 📉	More build than expected = more supply in market
#     Actual = Forecast	~0	Neutral	Expectations met, no surprise

    if availability['brent_spr']:
        if metrics['z_score'] > 1.5:
            m4.metric("Brent-WTI Spread -Z Score", f"{metrics['z_score']:.2f}", 
            delta="Bullish 📈", delta_color="green", delta_arrow="up")
        elif metrics['z_score'] < -1.5:
            m4.metric("Brent-WTI Spread -Z Score", f"{metrics['z_score']:.2f}", 
            delta="Bearish 📉", delta_color="red", delta_arrow="down")
        else:
            m4.metric("Brent-WTI Spread -Z Score", f"{metrics['z_score']:.2f}", 
            delta="Neutral", delta_color="blue", delta_arrow="off")
    else:
        m4.warning("No Spread Data")
#   The Z-Score measures how far the Brent-WTI spread is from its historical average, in terms of standard deviations.
#   Z-Score	Interpretation	Signal	Trading Implication
#   z > 1.5	Spread is very high vs history	Bullish for WTI 📈	WTI is undervalued → Arb opportunity (buy WTI)
#   z < -1.5	Spread is very low vs history	Bearish for WTI 📉	WTI is overvalued → Arb opportunity (sell WTI)
#   -1.5 < z < 1.5	Spread is normal	Neutral	No strong arbitrage signal

    if availability['wti']:
        if metrics['rsi'].iloc[-1] > 80:
            m5.metric("RSI (14)", f"{metrics['rsi'].iloc[-1]:.1f}", 
            delta="Overbought", delta_color="red", delta_arrow="up")
        elif metrics['rsi'].iloc[-1] < 20:
            m5.metric("RSI (14)", f"{metrics['rsi'].iloc[-1]:.1f}", 
            delta="Oversold", delta_color="green", delta_arrow="down")
        else:
            m5.metric("RSI (14)", f"{metrics['rsi'].iloc[-1]:.1f}", 
            delta="Neutral", delta_color="blue", delta_arrow="off")
    else:
        m5.warning("No WTI Data")

    st.divider()

    # --- THE VERDICT SECTION ---
    color = "green" if verdict == "BULLISH" else "grey" if verdict == "NEUTRAL" else "red"
    st.markdown(f"<h4 style='text-align: center; color: {color};'>FINAL VERDICT: {verdict}</h4>", unsafe_allow_html=True)
    
    target_metric, sl_metric = st.columns(2)

    if verdict == "BEARISH":
        target_metric.metric(
            label="Probable Target Price",
            value=f"${p_low:.2f}",
            help="Price tends to move towards value",
            border=True, delta="Value Area High", delta_color="green", delta_arrow="up"
        )
        sl_metric.metric(
            label="Probable Stop Loss",
            value=f"${p_high:.2f}",
            help="Price tends to move away from value",
            border=True, delta="Value Area Low", delta_color="red", delta_arrow="down"
        )
    if verdict == "BULLISH":
        target_metric.metric(
            label="Probable Target Price",
            value=f"${p_high:.2f}",
            help="Price tends to move towards value",
            border=True, delta="Value Area High", delta_color="green", delta_arrow="up"
        )
        sl_metric.metric(
            label="Probable Stop Loss",
            value=f"${p_low:.2f}",
            help="Price tends to move away from value",
            border=True, delta="Value Area Low", delta_color="red", delta_arrow="down"
        )
    if verdict == "NEUTRAL":
        target_metric.metric(
            label="Value Area High",
            value=f"${p_high:.2f}",
            help="Upper bound of value area from volume profile",
            border=True, delta="Value Area High", delta_color="green", delta_arrow="up"
        )
        sl_metric.metric(
            label="Value Area Low",
            value=f"${p_low:.2f}",
            help="Lower bound of value area from volume profile",
            border=True, delta="Value Area Low", delta_color="red", delta_arrow="down"
        )

    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.write("### 🛠️ Detailed Force Matrix")
        for force, weight in force_matrix.items():
            normalized = (weight + 3) / 6
            st.write(f"**{force}** ({weight})")
            st.progress(normalized)

    with col_right:
        st.write("### ⚖️ Aggregate Pressure")
        st.markdown(f"**Total Score: {total_score}**")
        
        if total_score >= 6:
            st.success("🔼 **Score ≥ 6: BULLISH**\nHigh confluence of forced bullish factors. Strong upside pressure.")
        elif total_score >= 3:
            st.warning("🟩 **Score 3-6: NEUTRAL**\nMixed signals - conflicting pressures. No clear directional bias.")
        else:
            st.error("🔻 **Score < 3: BEARISH**\nStrong bearish confluence. Multiple structural headwinds.")
    # --- S&D ANALYSIS SECTION ---
    st.divider()
    st.subheader("📊 S&D Momentum Analysis")

    if availability['eia'] and eia_f is not None and len(eia_f) > 0:
        def clean_m(val): return float(val.replace('M','')) if isinstance(val,str) else float(val) if pd.notnull(val) else 0
        eia_f_copy = eia_f.copy()
        eia_f_copy['shock'] = eia_f_copy['actual'].apply(clean_m) - eia_f_copy['forecast'].apply(clean_m)
        eia_f_copy['cum_shock'] = eia_f_copy['shock'].cumsum()

        fig_shock = go.Figure()
        fig_shock.add_trace(go.Bar(x=eia_f_copy['release_date'], y=eia_f_copy['shock'], name="Weekly Shock", marker_color='blue'))
        fig_shock.add_trace(go.Scatter(x=eia_f_copy['release_date'], y=eia_f_copy['cum_shock'], name="Cumulative Shock", line=dict(color='yellow', width=3)))
        fig_shock.update_layout(title="Inventory Shock vs Cumulative Trend", template="plotly_dark", height=400)
        st.plotly_chart(fig_shock, use_container_width=True)
    else:
        st.warning("⏳ EIA data not available. Upload EIA inventory file to view S&D analysis.")

def render_quant_page(df):
    st.title("📈 Institutional Price Action")

    # --- Interactive Inputs ---
    st.sidebar.markdown("### ⚙️ Quant Settings")
    vwap_window = st.sidebar.number_input("VWAP Lookback (Days)", min_value=1, max_value=200, value=20)
    sr_window = st.sidebar.number_input("S/R Lookback (Days)", min_value=5, max_value=100, value=20)

    # Calculations
    df = detect_liquidity_sweeps_v2(df)
    df['vwap'] = calculate_vwap(df, window=vwap_window)
    sup, res = calculate_sr_levels(df, window=sr_window)

    # Plotting
    fig = go.Figure(data=[go.Candlestick(x=df['date'], open=df['open'], high=df['high'], low=df['low'], close=df['price'], name="WTI")])

    # VWAP
    fig.add_trace(go.Scatter(x=df['date'], y=df['vwap'], name=f"VWAP {vwap_window}d", line=dict(color='yellow', width=2)))

    # Support & Resistance
    fig.add_hline(y=res, line_color="red", line_dash="dash", annotation_text="Resistance")
    fig.add_hline(y=sup, line_color="green", line_dash="dash", annotation_text="Support")

    # Liquidity Sweeps
    sweeps = df[df['sweep'] == True]
    fig.add_trace(go.Scatter(x=sweeps['date'], y=sweeps['low'], mode='markers', marker=dict(color='cyan', size=12, symbol='triangle-up'), name="Liquidity Sweep"))

    fig.update_layout(template="plotly_dark", xaxis_rangeslider_visible=False, height=600)
    st.plotly_chart(fig, use_container_width=True)

    # --- Liquidity Sweeps Summary ---
    st.divider()
    st.subheader("💨 Liquidity Sweep Summary")
    if not sweeps.empty:
        st.info(f"**Found {len(sweeps)} liquidity sweep(s) in the displayed period**")
        sweep_summary = sweeps[['date', 'low', 'high', 'vol']].rename(columns={
            'date': 'Sweep Date', 'low': 'Sweep Low', 'high': 'Sweep High', 'vol': 'Volume'
        })
        st.dataframe(sweep_summary, use_container_width=True)
    else:
        st.warning("No liquidity sweeps detected in the current period")

    # --- Definitions & Legends ---
    st.divider()
    st.subheader("📚 Quantitative Glossary")
    with st.expander("View Metric Definitions"):
        st.markdown("""
        - **VWAP (Volume Weighted Average Price):** The average price a security has traded at throughout the day, based on both volume and price. It is used by institutions to determine the 'Fair Value' of an asset.
        - **Liquidity Sweep:** A price move that breaks a significant low or high to trigger stop-losses, followed by an immediate reversal. This indicates 'Institutional Absorption'.
        - **Support/Resistance:** The price levels where a coin/commodity historically has difficulty breaking through.
        - **Candlestick (OHLC):** Open, High, Low, and Close prices for the day.
        """)


def render_volume_profile_page(df):
    """
    Volume Profile Analysis Page - Leading S/R Detection
    Shows: POC, VAH/VAL, HVN, LVN, Volume Gaps, Trading Signals
    """
    st.title("📊 Volume Profile - Leading Support/Resistance")
    st.markdown("Institutional footprint analysis using volume-at-price. **Leading** indicators show where price will move, not where it was.")

    if df is None or df.empty or len(df) < 10:
        st.warning("⚠️ Insufficient data for Volume Profile analysis. Need at least 10 bars.")
        return

    # Sidebar: User Inputs
    st.sidebar.markdown("### ⚙️ Volume Profile Settings")
    lookback_bars = st.sidebar.number_input(
        "Lookback Period (bars)",
        min_value=5,
        max_value=200,
        value=20,
        step=5,
        help="Number of bars to use for volume profile calculation. More bars = more significant levels."
    )

    bin_count = st.sidebar.slider(
        "Price Bins",
        min_value=20,
        max_value=100,
        value=50,
        help="Number of price levels for volume histogram. Higher = more precision."
    )

    # Calculate Volume Profile
    vp_data = calculate_volume_profile_signals(df, window=lookback_bars, bins=bin_count)

    if vp_data.get('poc') == 0:
        st.error("Volume Profile calculation failed. Check data quality.")
        return

    # Current price
    current_price = df['price'].iloc[-1]
    latest_date = df['date'].iloc[-1] if 'date' in df.columns else "N/A"

    # Filter df to lookback period for chart (show 1.5x for context)
    df_chart = df.tail(int(lookback_bars * 1.5)).copy()

    st.info(f"**Latest Close:** ${current_price:.2f} | **Data through:** {latest_date} | **Lookback:** {lookback_bars} bars")

    st.divider()

    # Top Row: Key Levels
    col1, col2, col3, col4 = st.columns(4)

    with col1:

        if vp_data.get('poc_migration') == 'rising':
            st.metric(
                label = "POC (Point of Control)",
                value = f"${vp_data['poc']:.2f}",
                delta=f"{vp_data.get('poc_migration', 'rising').upper()}",
                delta_color="green", delta_arrow="up",
                help="Price level with highest volume - institutional fair value. POC is rising → price is being bought up"
                )            
        elif vp_data.get('poc_migration') == 'falling':
            st.metric(
                label = "POC (Point of Control)",
                value = f"${vp_data['poc']:.2f}",
                delta=f"{vp_data.get('poc_migration', 'falling').upper()}",
                delta_color="red", delta_arrow="down",
                help="Price level with highest volume - institutional fair value. POC is falling → price is being sold down"
            )
        else:
            st.metric(
                label = "POC (Point of Control)",
                value = f"${vp_data['poc']:.2f}",
                delta=f"{vp_data.get('poc_migration', 'stable').upper()}",
                delta_color="blue", delta_arrow="off",
                help="Price level with highest volume - institutional fair value. POC is stable → price is balanced"
            )

    with col2:

        st.metric(
            "VAH (Value Area High)",
            f"${vp_data['vah']:.2f}",
            delta="Resistance",
            delta_color="red", delta_arrow="down",
            help="Upper bound of 70% value area"
        )

    with col3:
        st.metric(
            "VAL (Value Area Low)",
            f"${vp_data['val']:.2f}",
            delta="Support",
            delta_color="green",delta_arrow="up",
            help="Lower bound of 70% value area"
        )

    with col4:
        position = vp_data.get('current_price_position', 'unknown')
        st.metric(
            "Price Position",
            position.replace('_', ' ').capitalize(),
            delta="Current",
            delta_color="blue", delta_arrow="off",
            help=f"Price relative to POC/Value Area"
        )

    st.divider()

    # Main Chart: Volume Profile + Price
    st.subheader("📈 Volume Profile Chart")

    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df_chart['date'] if 'date' in df_chart.columns else df_chart.index,
        open=df_chart['open'], high=df_chart['high'], low=df_chart['low'], close=df_chart['price'],
        name="Price",
        increasing_line_color='green',
        decreasing_line_color='red'
    ))

    # POC Line
    fig.add_hline(
        y=vp_data['poc'],
        line_color="yellow",
        line_width=2,
        annotation_text=f"POC ${vp_data['poc']:.2f}",
        annotation_position="right"
    )

    # VAH/VAL Lines
    fig.add_hline(
        y=vp_data['vah'],
        line_color="orange",
        line_dash="dash",
        annotation_text=f"VAH ${vp_data['vah']:.2f}",
        annotation_position="right"
    )
    fig.add_hline(
        y=vp_data['val'],
        line_color="orange",
        line_dash="dash",
        annotation_text=f"VAL ${vp_data['val']:.2f}",
        annotation_position="right"
    )

    # HVN Lines
    for i, hvn in enumerate(vp_data.get('hvn', [])[:5]):
        fig.add_hline(
            y=hvn,
            line_color="green",
            line_dash="dot",
            opacity=0.5,
            annotation_text=f"HVN{i+1}" if len(vp_data.get('hvn', [])) <= 5 else None
        )

    # LVN Lines
    for i, lvn in enumerate(vp_data.get('lvn', [])[:5]):
        fig.add_hline(
            y=lvn,
            line_color="red",
            line_dash="dot",
            opacity=0.5,
            annotation_text=f"LVN{i+1}" if len(vp_data.get('lvn', [])) <= 5 else None
        )

    fig.update_layout(
        template="plotly_dark",
        height=600,
        xaxis_rangeslider_visible=False,
        title=f"Volume Profile Analysis - {lookback_bars} bars"
    )

    st.plotly_chart(fig, use_container_width=True)

    # Volume Profile Histogram (Right Panel)
    st.subheader("📊 Volume-at-Price Distribution")

    if vp_data.get('price_bins') and vp_data.get('volume_bins'):
        vol_fig = go.Figure()

        vol_fig.add_trace(go.Bar(
            orientation='h',
            y=vp_data['price_bins'],
            x=vp_data['volume_bins'],
            name='Volume',
            marker_color='blue',
            opacity=0.6
        ))

        # Add POC marker
        vol_fig.add_hline(
            y=vp_data['poc'],
            line_color="yellow",
            line_width=3,
            annotation_text="POC"
        )

        # Add VAH/VAL
        vol_fig.add_hline(y=vp_data['vah'], line_color="orange", line_dash="dash", annotation_text="VAH")
        vol_fig.add_hline(y=vp_data['val'], line_color="orange", line_dash="dash", annotation_text="VAL")

        # Add current price
        vol_fig.add_hline(
            y=current_price,
            line_color="white",
            line_width=2,
            annotation_text="Current"
        )

        vol_fig.update_layout(
            template="plotly_dark",
            height=500,
            xaxis_title="Volume",
            yaxis_title="Price ($) ➡️",
            showlegend=False
        )

        st.plotly_chart(vol_fig, use_container_width=True)

    st.divider()

    # Trading Signals
    st.subheader("🎯 Volume Profile Signals")

    signals = generate_vp_signals(vp_data, current_price)

    if signals:
        for i, signal in enumerate(signals):
            color = "green" if signal['direction'] == 'bullish' else "red" if signal['direction'] == 'bearish' else "blue"
            icon = "📈" if signal['direction'] == 'bullish' else "📉" if signal['direction'] == 'bearish' else "⚠️"

            st.markdown(f"**{icon} Signal {i+1}: {signal['type'].upper()}**")
            col_sig1, col_sig2 = st.columns([3, 1])
            with col_sig1:
                st.markdown(f"{signal['description']}")
            with col_sig2:
                st.badge(signal['strength'].upper(), color=color)
            st.markdown("---")
    else:
        st.info("No strong signals at current price level. Market in balance.")

    st.divider()

    # Targets & Stop Loss Calculator
    st.subheader("🎯 Target & Stop Loss Calculator")

    target_col1, target_col2 = st.columns(2)

    with target_col1:
        trade_direction = st.selectbox(
            "Trade Direction",
            ["long", "short"],
            help="Select your intended trade direction"
        )

    with target_col2:
        entry_price = st.number_input(
            "Recommended Entry Price",
            min_value=0.0,
            value=current_price,
            step=0.01,
            help="Your planned entry price"
        )

    if st.button("Calculate Targets & Stop", type="primary"):
        ts_data = calculate_vp_targets_sl(vp_data, entry_price, trade_direction)

        if ts_data['targets']:
            st.success("✅ Calculated levels based on Volume Profile")

            t_col1, t_col2, t_col3 = st.columns(3)

            for target_name, target_price, target_reason in ts_data['targets']:
                if target_name == "T1":
                    t_col1.metric(
                        f"{target_name} - {target_reason}",
                        f"${target_price:.2f}",
                        delta=f"{((target_price - entry_price) / entry_price * 100):.2f}%" if trade_direction == 'long' else f"{((entry_price - target_price) / entry_price * 100):.2f}%",
                        delta_color="green"
                    )
                elif target_name == "T2":
                    t_col2.metric(
                        f"{target_name} - {target_reason}",
                        f"${target_price:.2f}",
                        delta=f"{((target_price - entry_price) / entry_price * 100):.2f}%" if trade_direction == 'long' else f"{((entry_price - target_price) / entry_price * 100):.2f}%",
                        delta_color="green"
                    )
                elif target_name == "T3":
                    t_col3.metric(
                        f"{target_name} - {target_reason}",
                        f"${target_price:.2f}",
                        delta=f"{((target_price - entry_price) / entry_price * 100):.2f}%" if trade_direction == 'long' else f"{((entry_price - target_price) / entry_price * 100):.2f}%",
                        delta_color="green"
                    )

            if ts_data['stop_loss'] > 0:
                ts_data_calculated = calculate_vp_targets_sl(vp_data, entry_price, trade_direction)

                st.metric(
                    "🛑 Stop Loss",
                    f"${ts_data_calculated['stop_loss']:.2f}",
                    delta=f"{((entry_price - ts_data_calculated['stop_loss']) / entry_price * 100):.2f}% risk" if trade_direction == 'long' else f"{((ts_data_calculated['stop_loss'] - entry_price) / entry_price * 100):.2f}% risk",
                    delta_color="red"
                )

                if ts_data_calculated.get('risk_reward', 0) > 0:
                    st.info(f"**Risk/Reward Ratio:** {ts_data_calculated['risk_reward']:.2f}")
        else:
            st.warning("Could not calculate targets. Insufficient Volume Profile data.")

    st.divider()

    # Educational Section
    with st.expander("📚 How to Read Volume Profile"):
        st.markdown("""
        ### Volume Profile Components

        | Component | What It Is | How To Trade |
        |-----------|------------|--------------|
        | **POC (Point of Control)** | Price level with highest traded volume | Magnet - price gravitates here. Support when above, resistance when below. |
        | **VAH/VAL (Value Area High/Low)** | 70% of volume transacted in this range | Mean reversion zones. Long at VAL, short at VAH. Breakout = momentum trade. |
        | **HVN (High Volume Nodes)** | Local volume peaks | Institutional footprint. Strong support/resistance. |
        | **LVN (Low Volume Nodes)** | Local volume troughs | Rejection zones. Price moves FAST through these. |
        | **Volume Gaps** | Near-zero volume between nodes | Unfinished business. Price fills gaps quickly. |

        ### Key Principles

        1. **Volume = Cause, Price = Effect** - Trade FROM volume levels, not TO them
        2. **POC Migration = Trend** - Rising POC = bullish, Falling POC = bearish
        3. **HVN = Support/Resistance** - Where big money transacted
        4. **LVN = Breakout Path** - Price accelerates through low-volume zones
        5. **Value Area = Fair Price** - 70% of market agrees this is fair value

        ### Leading vs Lagging

        This is **leading** analysis because:
        - Shows where institutions WILL move price (to high-volume nodes)
        - Not based on past price reactions (like traditional S/R)
        - Volume reveals intent before price movement
        """)
