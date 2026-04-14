# ui_components.py
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import duckdb, io
from config import DATA_SOURCES, PRIMARY_KEYS
from data_engine import validate_df, normalize_columns
from quant_engine import calculate_z_score, calculate_volume_profile, generate_detailed_force_matrix, detect_liquidity_sweeps_v2, calculate_inventory_shock, calculate_inv_momentum, calculate_rsi, calculate_vwap, calculate_sr_levels


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

    status_cols = st.columns(8)
    dataset_names = ["WTI OHLC", "Brent Spread", "COT", "RBOB", "HO", "EIA", "Cracks"]
    for idx, (name, avail_key) in enumerate(zip(dataset_names, ["wti", "brent_spr", "cot", "rbob", "ho", "eia", "cracks"])):
        with status_cols[idx]:
            if availability.get(avail_key, False):
                st.success(f"✅ {name}")
            else:
                st.warning(f"⏳ {name}")

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
                    "Vol Premium",
                    f"{vol_data['vol_premium']:.2f}",
                    delta=vol_data['signal'],
                    help="OVX minus RV — positive means options are expensive"
                )
            
            with vol_col4:
                if vol_data['signal'] == 'EXPENSIVE':
                    st.error(f"⚠️ {vol_data['signal']}\nSell vol edge")
                elif vol_data['signal'] == 'CHEAP':
                    st.success(f"✅ {vol_data['signal']}\nBuy vol edge")
                else:
                    st.info(f"🟩 {vol_data['signal']}\nNo vol edge")
            
            # Recommendation box
            st.info(f"**Strategy Recommendation:** {vol_data['recommendation']}")
            
            # Combined directional + vol signal
            st.write("### 🎯 Combined Signal (Direction + Volatility)")
            if total_score >= 6:
                if vol_data['vol_premium'] < -10:
                    st.success("🚀 **IDEAL SETUP:** Bullish Score + Cheap IV → Buy OTM calls for leverage at discount")
                elif vol_data['vol_premium'] > 15:
                    st.warning("⚡ **OPPORTUNE:** Bullish Score + Expensive IV → Use futures or deep ITM calls (avoid OTM — overpriced)")
                else:
                    st.info("📈 **BULLISH:** Balanced IV → Use any directional vehicle (futures, calls, spreads)")
            elif total_score < 3:
                if vol_data['vol_premium'] < -10:
                    st.success("⚠️ **HEDGED DOWNSIDE:** Bearish Score + Cheap IV → Buy OTM puts for protection at discount")
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
    st.info(f"📊 **Metrics calculated from last {weeks_lookback} weeks of data**")
    
    if availability['wti'] and wti_f is not None and len(wti_f) > 0:
        poc_data = calculate_volume_profile(wti_f, window=weeks_lookback)
        st.info(f"**Point of Control (POC): ${poc_data['poc']:.2f}**")
        st.info(f"**Value Area (High): ${poc_data['vah']:.2f}**")
        st.info(f"**Value Area (Low): ${poc_data['val']:.2f}**")
    else:
        st.warning("⏳ WTI data not available for Volume Profile calculation")

    m1, m2, m3, m4, m5 = st.columns(5)

    if availability['cracks']:
        m1.metric("Crack Spread", f"${metrics['current_crack']:.2f}", 
            delta="Expanding" if metrics['current_crack'] > metrics['crack_mean'] else "Contracting")
    else:
        m1.warning("No Crack Data")
    
    if availability['cot']:
        m2.metric("Squeeze Factor", f"{metrics['net_pos']}", 
            delta="Short Squeeze 📈" if metrics['net_pos'] < -10000 else "Long Squeeze 📉" if metrics['net_pos'] > 10000 else "Balanced")
    else:
        m2.warning("No COT Data")
    # Condition	Signal	Meaning
    # net_pos < -10000	Short Squeeze 📈 (Bullish)	Too many shorts trapped → forced to cover → price up
    # net_pos > 10000	Long Squeeze 📉 (Bearish)	Too many longs trapped → forced to liquidate → price down
    # -10000 ≤ net_pos ≤ 10000	Balanced (Neutral)	Positions are equilibrated, no extreme squeeze risk


    if availability['eia']:
        m3.metric("Supply-Demand Shock", f"{metrics['inv_shock']['abs_m']:.2f}M BBL", 
            delta="Bullish" if metrics['inv_shock']['value'] < 0 else "Bearish")
    else:
        m3.warning("No EIA Data")
#     Scenario	Value	Signal	Meaning
#     Actual < Forecast	Negative (e.g., -2M BBL)	Bullish 📈	More draw than expected = less supply in market
#     Actual > Forecast	Positive (e.g., +3M BBL)	Bearish 📉	More build than expected = more supply in market
#     Actual = Forecast	~0	Neutral	Expectations met, no surprise

    if availability['brent_spr']:
        m4.metric("Spread-Z Score", f"{metrics['z_score']:.2f}", 
            delta="Arb Bullish 📈" if metrics['z_score'] > 1.5 else "Arb Bearish 📉" if metrics['z_score'] < -1.5 else "Neutral")
    else:
        m4.warning("No Spread Data")
#   The Z-Score measures how far the Brent-WTI spread is from its historical average, in terms of standard deviations.
#   Z-Score	Interpretation	Signal	Trading Implication
#   z > 1.5	Spread is very high vs history	Bullish for WTI 📈	WTI is undervalued → Arb opportunity (buy WTI)
#   z < -1.5	Spread is very low vs history	Bearish for WTI 📉	WTI is overvalued → Arb opportunity (sell WTI)
#   -1.5 < z < 1.5	Spread is normal	Neutral	No strong arbitrage signal

    if availability['wti']:
        m5.metric("RSI (14)", f"{metrics['rsi'].iloc[-1]:.1f}", 
            delta="Overbought" if metrics['rsi'].iloc[-1] > 80 else "Oversold" if metrics['rsi'].iloc[-1] < 20 else "")
    else:
        m5.warning("No WTI Data")

    st.divider()

    # --- THE VERDICT SECTION ---
    color = "green" if verdict == "BULLISH" else "grey" if verdict == "NEUTRAL" else "red"
    st.markdown(f"<h2 style='text-align: center; color: {color};'>VERDICT: {verdict}</h2>", unsafe_allow_html=True)

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
