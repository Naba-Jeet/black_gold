# ui_components.py
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import duckdb, io
from config import DATA_SOURCES, PRIMARY_KEYS
from data_engine import validate_df, upsert_to_duckdb, clean_volume, normalize_columns
from quant_engine import calculate_z_score, generate_detailed_force_matrix, detect_liquidity_sweeps, calculate_inventory_shock, calculate_inv_momentum, calculate_rsi, calculate_vwap, calculate_sr_levels


# ==========================================
# DATA INGESTION PAGE (With Source Links)
# ==========================================
def render_ingestion_page():
    st.title("🏗️ Data Gatekeeper")
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



def render_terminal_page(wti, brent_spr, cot, rbob, ho, cracks, eia_stocks):
    st.title("🎯 Crude Flow Predictive Terminal")

    # 1. Core Calculations
    current_crack = cracks['crack_spread'].iloc[-1]
    crack_mean = cracks['crack_spread'].mean()
    net_pos = cot['m_money_positions_long_all'].iloc[-1] - cot['m_money_positions_short_all'].iloc[-1]

    inv_data = calculate_inventory_shock(eia_stocks)

    # If by some chance inv_data is a float (old version), convert it to the required dict
    if isinstance(inv_data, float):
        inv_data = {"value": inv_data, "unit": "BBL", "abs_m": inv_data / 1_000_000}

    inv_mom = calculate_inv_momentum(eia_stocks)
    rsi = calculate_rsi(wti)
    z_score = calculate_z_score(brent_spr)

    force_matrix, total_score, verdict = generate_detailed_force_matrix(
        current_crack, crack_mean, z_score, net_pos, inv_data['value'], inv_mom, rsi
    )

    # --- Top Metrics ---
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Crack Spread", f"${current_crack:.2f}", delta="Expanding" if current_crack > crack_mean else "Contracting")
    m2.metric("Squeeze Factor", f"{net_pos}", delta="Squeeze" if net_pos < -10000 else "")
    m3.metric("S&D Shock", f"{inv_data['abs_m']:.2f}M BBL", delta="Bullish" if inv_data['value'] < 0 else "Bearish")
    m4.metric("S-Z Score", f"{z_score:.2f}", delta="Arb Bullish" if z_score > 1.5 else "")
    m5.metric("RSI (14)", f"{rsi.iloc[-1]:.1f}", delta="Overbought" if rsi.iloc[-1] > 70 else "Oversold" if rsi.iloc[-1] < 30 else "")

    st.divider()

    # --- THE VERDICT SECTION ---
    color = "green" if verdict == "BULLISH" else "grey" if verdict == "NEUTRAL" else "red"
    st.markdown(f"<h1 style='text-align: center; color: {color};'>VERDICT: {verdict}</h1>", unsafe_allow_html=True)

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
        st.info("A score > 6 indicates high confluence of forced bullish factors.")

    # --- S&D ANALYSIS SECTION ---
    st.divider()
    st.subheader("📊 S&D Momentum Analysis")

    def clean_m(val): return float(val.replace('M','')) if isinstance(val,str) else float(val) if pd.notnull(val) else 0
    eia_stocks['shock'] = eia_stocks['actual'].apply(clean_m) - eia_stocks['forecast'].apply(clean_m)
    eia_stocks['cum_shock'] = eia_stocks['shock'].cumsum()

    fig_shock = go.Figure()
    fig_shock.add_trace(go.Bar(x=eia_stocks['release_date'], y=eia_stocks['shock'], name="Weekly Shock", marker_color='blue'))
    fig_shock.add_trace(go.Scatter(x=eia_stocks['release_date'], y=eia_stocks['cum_shock'], name="Cumulative Shock", line=dict(color='yellow', width=3)))
    fig_shock.update_layout(title="Inventory Shock vs Cumulative Trend", template="plotly_dark", height=400)
    st.plotly_chart(fig_shock, use_container_width=True)

def render_quant_page(df):
    st.title("📈 Institutional Price Action")

    # --- Interactive Inputs ---
    st.sidebar.markdown("### ⚙️ Quant Settings")
    vwap_window = st.sidebar.number_input("VWAP Lookback (Days)", min_value=1, max_value=200, value=20)
    sr_window = st.sidebar.number_input("S/R Lookback (Days)", min_value=5, max_value=100, value=20)

    # Calculations
    df = detect_liquidity_sweeps(df)
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